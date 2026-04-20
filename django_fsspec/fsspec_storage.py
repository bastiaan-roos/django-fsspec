import posixpath

from django.core.exceptions import ImproperlyConfigured
from django.core.files.storage import Storage
from django.utils.crypto import get_random_string

from .utils import build_virtual_hosted_url
from .utils import get_filesystem
from .utils import make_boto3_client_from_s3fs
from .utils import unwrap_s3_target


class FsspecStorage(Storage):
    """Django Storage implementation using fsspec.

    Any filesystem that fsspec supports — local, S3, GCS, Azure, memory,
    ftp, sftp, zip, ... — can be used as a Django storage backend. The two
    composed filesystems shipped in this package (``NestedFileSystem``,
    ``TransparentFileSystem``) plug straight into the same configuration.

    Example — single S3 bucket::

        STORAGES = {
            "default": {
                "BACKEND": "django_fsspec.FsspecStorage",
                "OPTIONS": {
                    "base_url": "https://cdn.example.com/",
                    "storage_config": {
                        "protocol": "s3",
                        "endpoint_url": "https://s3.eu-central-1.amazonaws.com",
                        "key": S3_KEY,
                        "secret": S3_SECRET,
                        "relative_to_path": "my-bucket",
                    },
                },
            },
        }

    Example — multi-bucket routing with ``NestedFileSystem``::

        STORAGES = {
            "default": {
                "BACKEND": "django_fsspec.FsspecStorage",
                "OPTIONS": {
                    "storage_config": {
                        "protocol": "nested",
                        "path_storage_configs": {
                            "upload": {
                                "protocol": "s3",
                                "endpoint_url": S3_ENDPOINT,
                                "key": S3_KEY, "secret": S3_SECRET,
                                "relative_to_path": "myapp-upload",
                            },
                            "video": {
                                "protocol": "s3",
                                "endpoint_url": S3_ENDPOINT,
                                "key": S3_KEY, "secret": S3_SECRET,
                                "relative_to_path": "myapp-video",
                            },
                            "default": {
                                "protocol": "file",
                                "auto_mkdir": True,
                            },
                        },
                    },
                },
            },
        }

    Usage::

        from django.core.files.storage import storages
        storage = storages["default"]
        storage.save("upload/foo.ribx", content)
        storage.exists("upload/foo.ribx")

        # Presigned download URL (1 hour TTL)
        url = storage.url_signed("upload/foo.ribx", expires=3600)
    """

    def __init__(self, **settings):
        settings_cp = settings.copy()
        self.location = settings_cp.pop("location", "")
        self.base_url = settings_cp.pop("base_url", None)
        self.file_permissions_mode = settings_cp.pop("file_permissions_mode", None)
        self.directory_permissions_mode = settings_cp.pop("directory_permissions_mode", None)
        self.allow_overwrite = settings_cp.pop("allow_overwrite", True)
        # Na een _save-write de opgeslagen checksum vergelijken met
        # `content.checksum` (indien gezet). Mismatch → object weer verwijderen
        # en IOError raisen. Default uit: kost een extra round-trip per save.
        self.verify_checksum = settings_cp.pop("verify_checksum", False)

        if "storage_config" not in settings_cp:
            raise ImproperlyConfigured("storage_config is required")
        storage_config = settings_cp.pop("storage_config")
        self.filesystem = get_filesystem(**storage_config)

        if settings_cp:
            raise ImproperlyConfigured(f"Unknown setting(s): {settings_cp.keys()}")

    def delete(self, name):
        return self.filesystem.rm(name)

    def exists(self, name):
        return self.filesystem.exists(name)

    def listdir(self, path):
        """Return ``(directories, files)`` tuple per Django's Storage contract.

        Parameters
        ----------
        path : str

        Returns
        -------
        tuple of (list of str, list of str)
        """
        details = self.filesystem.ls(path, detail=True)
        dirs = []
        files = []
        for item in details:
            # fsspec geeft ofwel dicts (detail=True) ofwel strings (detail=False)
            if isinstance(item, dict):
                full_name = item.get("name", "")
                kind = item.get("type", "file")
            else:
                full_name = item
                kind = "file"
            base_name = posixpath.basename(full_name.rstrip("/"))
            if not base_name:
                continue
            if kind == "directory":
                dirs.append(base_name)
            else:
                files.append(base_name)
        return dirs, files

    def ls(self, path):
        """List a directory with details (extra — not part of Django's Storage API)."""
        return self.filesystem.ls(path, detail=True)

    def _open(self, name, mode="rb"):
        return self.filesystem.open(name, mode)

    def path(self, name):
        # Django's contract: Storage.path() is alleen geldig voor lokale
        # filesystem-storages. Voor remote backends moet het NotImplementedError
        # raisen, zodat callers expliciet weten dat ze geen absoluut local pad
        # kunnen verwachten. Voorheen retourneerde dit `name` (de relatieve
        # string), wat tot silent footguns leidde in code die `os.path.isfile`
        # of `open()` op de uitkomst aanriep.
        raise NotImplementedError(
            "FsspecStorage does not support absolute local file paths. "
            "Use storage.open(name) or storage.url(name) instead."
        )

    def _save(self, name, content, max_length=None):
        # Zorg dat parent directory bestaat (relevant voor file:// backend en
        # geneste filesystems die geen impliciete makedirs doen).
        parent = posixpath.dirname(name)
        if parent and hasattr(self.filesystem, "makedirs"):
            try:
                self.filesystem.makedirs(parent, exist_ok=True)
            except (FileExistsError, NotImplementedError):
                pass

        if self.exists(name):
            if self.allow_overwrite:
                self.delete(name)
            else:
                # Niet overschrijven; Django's Storage.save() roept normaal
                # get_available_name() vóór _save aan, dus dit is een edge
                # case. We retourneren een alternatieve naam zodat Django de
                # juiste waarde krijgt te zien.
                from django.core.files.storage import Storage as _StorageBase
                base_name = _StorageBase.get_available_name(self, name, max_length=max_length)
                return self._save(base_name, content, max_length=max_length)

        # Streaming write: voor UploadedFile gebruiken we chunks() om niet de
        # hele file in memory te laden. Voor ContentFile / BytesIO valt het
        # terug op een enkele read().
        with self.filesystem.open(name, "wb") as f:
            if hasattr(content, "chunks"):
                for chunk in content.chunks():
                    f.write(chunk)
            else:
                # File-like zonder chunks(): kopieer in blokken van 4 MB
                while True:
                    block = content.read(4 * 1024 * 1024)
                    if not block:
                        break
                    f.write(block)

        if self.verify_checksum:
            self._verify_checksum_after_save(name, content)

        return name

    def _verify_checksum_after_save(self, name, content):
        # Caller zet source-checksum op `content.checksum` (bv. CRC-64NVME of
        # MD5 van de bron). Zonder checksum op content doen we niets — opt-in
        # per upload via het content-object.
        source_checksum = getattr(content, "checksum", None)
        if source_checksum is None:
            return
        stored_checksum = self.filesystem.checksum(name)
        if stored_checksum != source_checksum:
            self.filesystem.rm(name)
            raise IOError(
                f"Checksum mismatch after upload of {name}: "
                f"{stored_checksum!r} != {source_checksum!r}"
            )

    def size(self, name):
        return self.filesystem.size(name)

    def url(self, name):
        if not self.base_url:
            raise ValueError(
                "FsspecStorage instance has no base_url configured; "
                "set it via OPTIONS['base_url'] in STORAGES if you need url() support."
            )
        return self.base_url.rstrip("/") + "/" + name.lstrip("/")

    def url_direct(self, name):
        """Return the public, un-signed URL for an object.

        Parameters
        ----------
        name : str
            Name within this storage (including any NestedFileSystem prefix).

        Returns
        -------
        str
            Virtual-hosted-style URL — ``{scheme}://{bucket}.{endpoint}/{key}``.
            Only works when the bucket's ACL permits public read; otherwise
            S3 returns 403.

        Raises
        ------
        NotImplementedError
            When the underlying backend is not S3-compatible, or has no
            endpoint URL configured.
        """
        s3_fs, bucket, key = self._resolve_s3_target(name)
        return build_virtual_hosted_url(s3_fs, bucket, key)

    def url_signed(self, name, expires=3600, method="GET", response_headers=None):
        """Generate a presigned URL for temporary access to an S3 object.

        Parameters
        ----------
        name : str
            Name within this storage (including any NestedFileSystem prefix).
        expires : int, optional
            TTL in seconds. Default 3600.
        method : {'GET', 'PUT'}
            'GET' grants download access, 'PUT' grants upload access.
        response_headers : dict, optional
            Response headers S3 should inject on download (only valid for
            method='GET'), for instance
            ``{'ResponseContentDisposition': 'attachment; filename=foo.mp4'}``.

        Returns
        -------
        str
            Presigned URL that expires after `expires` seconds.

        Raises
        ------
        NotImplementedError
            When the underlying backend is not S3-compatible.
        ValueError
            When `method` is not one of ``'GET'`` or ``'PUT'``, or when
            `response_headers` is supplied together with ``method='PUT'``.
        """
        if method not in ("GET", "PUT"):
            raise ValueError(f"method must be 'GET' or 'PUT', got {method!r}")
        if method == "PUT" and response_headers:
            raise ValueError("response_headers is only valid for method='GET'")

        s3_fs, bucket, key = self._resolve_s3_target(name)

        # Fast path: GET zonder custom response-headers → s3fs kan het zelf
        # tekenen (wrap rond sync + boto3 onder water). Scheelt het aanmaken
        # van een extra sync client.
        if method == "GET" and not response_headers:
            return s3_fs.url(f"{bucket}/{key}", expires=expires)

        boto_client = make_boto3_client_from_s3fs(s3_fs)
        params = {"Bucket": bucket, "Key": key}
        if response_headers:
            params.update(response_headers)
        operation = "get_object" if method == "GET" else "put_object"
        return boto_client.generate_presigned_url(
            operation, Params=params, ExpiresIn=expires,
        )

    def _resolve_s3_target(self, name):
        """Resolve `name` via NestedFileSystem if present, else unwrap directly.

        Parameters
        ----------
        name : str

        Returns
        -------
        tuple of (s3fs.S3FileSystem, str, str)
        """
        resolver = getattr(self.filesystem, "resolve_s3_target", None)
        if resolver is not None:
            return resolver(name)
        return unwrap_s3_target(self.filesystem, name)

    def get_accessed_time(self, name):
        raise NotImplementedError

    def get_alternative_name(self, file_root, file_ext):
        """Append a short random suffix before the extension (Django contract)."""
        return "%s_%s%s" % (file_root, get_random_string(7), file_ext)

    def get_created_time(self, name):
        return self.filesystem.created(name)

    def get_modified_time(self, name):
        return self.filesystem.modified(name)
