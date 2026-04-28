import posixpath
import warnings

from django.core.exceptions import ImproperlyConfigured
from django.core.files.storage import Storage
from django.utils.crypto import get_random_string

from .nested_fs import NestedFileSystem
from .permissions import combine_on_collision
from .permissions import combine_permissions
from .permissions import normalize_on_collision
from .permissions import normalize_permissions
from .utils import build_virtual_hosted_url
from .utils import get_filesystem
from .utils import make_boto3_client_from_s3fs
from .utils import unwrap_s3_target

_UNSET = object()


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
        # After a _save write, compare the stored checksum with
        # `content.checksum` (if set). Mismatch → delete the object and
        # raise IOError. Default off: costs an extra round-trip per save.
        self.verify_checksum = settings_cp.pop("verify_checksum", False)

        self.permissions = normalize_permissions(settings_cp.pop("permissions", None))
        self.on_collision = self._resolve_on_collision_option(settings_cp)
        # Django's Storage.save() inspects this attr (>=5.1) to decide whether
        # to call get_available_name. Honor the on_collision intent: only
        # `rename` should let Django auto-rename.
        self.allow_overwrite = self.on_collision != "rename"

        # Permission modes are accepted for symmetry with FileSystemStorage
        # but not currently honored by any FsspecStorage backend.
        for unused in ("file_permissions_mode", "directory_permissions_mode"):
            if getattr(self, unused) is not None:
                warnings.warn(
                    f"FsspecStorage ignores {unused!r}; it is not currently implemented "
                    "for any fsspec backend and will be dropped in 0.2.0.",
                    DeprecationWarning,
                    stacklevel=2,
                )

        if "storage_config" not in settings_cp:
            raise ImproperlyConfigured("storage_config is required")
        # Copy so that mapping `location` into it does not mutate the
        # caller's STORAGES dict.
        storage_config = dict(settings_cp.pop("storage_config"))

        # Bridge Django's `location` into the underlying fsspec config.
        # Only meaningful for the local filesystem protocols; for any other
        # protocol the user must use storage_config['relative_to_path']
        # explicitly (`location` semantics differ per backend, so we refuse
        # to silently guess what the user meant).
        if self.location:
            protocol = storage_config.get("protocol")
            if protocol not in ("file", "local"):
                raise ImproperlyConfigured(
                    f"OPTIONS['location'] is only supported with storage_config "
                    f"protocol='file'/'local', got protocol={protocol!r}. "
                    "Use storage_config['relative_to_path'] instead."
                )
            if "relative_to_path" in storage_config:
                raise ImproperlyConfigured(
                    "Cannot set both OPTIONS['location'] and storage_config['relative_to_path']; pick one."
                )
            storage_config["relative_to_path"] = self.location

        if settings_cp:
            raise ImproperlyConfigured(f"Unknown setting(s): {list(settings_cp.keys())}")

        self.filesystem = get_filesystem(**storage_config)
        # Pre-compute effective (top-level ∧ sub-fs) permissions per nested
        # prefix so per-call lookups are a single dict access. For flat
        # storages there is no sub-fs to combine with — the cache stays empty.
        self._effective_perms_by_prefix: dict[str, tuple[dict, str]] = {}
        if isinstance(self.filesystem, NestedFileSystem):
            for prefix, sub_perms in self.filesystem.permissions.items():
                self._effective_perms_by_prefix[prefix] = (
                    combine_permissions(self.permissions, sub_perms),
                    combine_on_collision(self.on_collision, self.filesystem.on_collision[prefix]),
                )

    @staticmethod
    def _resolve_on_collision_option(settings_cp: dict) -> str:
        """Resolve ``on_collision`` from settings, honoring the deprecated alias.

        ``allow_overwrite`` is mapped onto ``on_collision`` for backwards
        compatibility; setting both at once is ambiguous and raises.
        """
        on_collision_raw = settings_cp.pop("on_collision", None)
        allow_overwrite_raw = settings_cp.pop("allow_overwrite", _UNSET)
        if allow_overwrite_raw is _UNSET:
            return normalize_on_collision(on_collision_raw)
        if on_collision_raw is not None:
            raise ImproperlyConfigured(
                "Cannot set both 'allow_overwrite' and 'on_collision'; "
                "use only 'on_collision' (allow_overwrite is deprecated)."
            )
        warnings.warn(
            "FsspecStorage option 'allow_overwrite' is deprecated and will be "
            "removed in 0.2.0; use on_collision='overwrite' (default) or 'rename' instead.",
            DeprecationWarning,
            stacklevel=3,
        )
        return normalize_on_collision("overwrite" if allow_overwrite_raw else "rename")

    def _resolve_effective(self, name: str) -> tuple[dict, str]:
        """Return effective ``(permissions, on_collision)`` for ``name``."""
        if not isinstance(self.filesystem, NestedFileSystem):
            return self.permissions, self.on_collision
        fs, root_path, _ = self.filesystem._get_filesystem(name)
        if fs is None:
            return self.permissions, self.on_collision
        key = root_path if root_path else "default"
        return self._effective_perms_by_prefix.get(key, (self.permissions, self.on_collision))

    def _check_permission(self, action: str, name: str) -> None:
        """Raise ``PermissionError`` when ``action`` is not allowed for ``name``.

        Parameters
        ----------
        action : {'read', 'write', 'delete'}
        name : str
        """
        perms, _ = self._resolve_effective(name)
        key = f"allow_{action}"
        if not perms[key]:
            raise PermissionError(f"FsspecStorage permission denied: {action} {name!r} ({key}=False)")

    def is_name_available(self, name, max_length=None):
        """Defer to the *effective* ``on_collision`` for ``name``.

        For ``"overwrite"`` and ``"raise"`` the caller's chosen name should
        hit ``_save`` verbatim so the collision policy can be enforced
        there. Only ``"rename"`` keeps Django's standard rename-if-taken
        behavior. The effective value is resolved per path so that a
        sub-filesystem inside a ``NestedFileSystem`` can pick a stricter
        policy than the top-level storage (most-restrictive-wins).
        """
        _, effective = self._resolve_effective(name)
        if effective == "rename":
            return super().is_name_available(name, max_length=max_length)
        if max_length and len(name) > max_length:
            return False
        return True

    def delete(self, name):
        self._check_permission("delete", name)
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
            # fsspec returns either dicts (detail=True) or strings (detail=False).
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
        if any(c in mode for c in ("w", "a", "x", "+")):
            self._check_permission("write", name)
        else:
            self._check_permission("read", name)
        return self.filesystem.open(name, mode)

    def path(self, name):
        # Django's contract: Storage.path() is only valid for local
        # filesystem storages. For remote backends it must raise
        # NotImplementedError so callers know explicitly that they cannot
        # expect an absolute local path. Previously this returned `name`
        # (the relative string), which led to silent footguns in code
        # calling `os.path.isfile` or `open()` on the result.
        raise NotImplementedError(
            "FsspecStorage does not support absolute local file paths. "
            "Use storage.open(name) or storage.url(name) instead."
        )

    def _save(self, name, content, max_length=None):
        self._check_permission("write", name)

        # Ensure the parent directory exists (relevant for the file://
        # backend and nested filesystems that do not implicitly makedirs).
        parent = posixpath.dirname(name)
        if parent and hasattr(self.filesystem, "makedirs"):
            try:
                self.filesystem.makedirs(parent, exist_ok=True)
            except (FileExistsError, NotImplementedError):
                pass

        if self.exists(name):
            redirected = self._handle_collision(name, content, max_length)
            if redirected is not None:
                return redirected

        self._stream_to_filesystem(name, content)

        if self.verify_checksum:
            self._verify_checksum_after_save(name, content)

        return name

    def _handle_collision(self, name, content, max_length):
        """Apply the effective ``on_collision`` policy when ``name`` exists.

        Returns the alternative name when the policy is ``"rename"`` (so the
        caller should return that instead), or ``None`` after handling
        ``"overwrite"`` (caller should proceed with the write). Raises for
        ``"raise"``.
        """
        _, on_collision = self._resolve_effective(name)
        if on_collision == "raise":
            raise PermissionError(f"FsspecStorage on_collision='raise': {name!r} already exists")
        if on_collision == "overwrite":
            # Internal cleanup; the caller's intent is "write", so this is
            # implementation detail and not gated by allow_delete.
            self.filesystem.rm(name)
            return None
        # "rename" — Django was supposed to call get_available_name first;
        # defensive fallback for direct calls bypassing Storage.save.
        alt_name = super().get_available_name(name, max_length=max_length)
        return self._save(alt_name, content, max_length=max_length)

    def _stream_to_filesystem(self, name, content):
        """Stream ``content`` into ``name`` on the underlying filesystem."""
        with self.filesystem.open(name, "wb") as f:
            if hasattr(content, "chunks"):
                # UploadedFile path — avoids loading the entire file into memory.
                for chunk in content.chunks():
                    f.write(chunk)
            else:
                # File-like without chunks() (ContentFile, BytesIO): copy in 4 MB blocks.
                while True:
                    block = content.read(4 * 1024 * 1024)
                    if not block:
                        break
                    f.write(block)

    def _verify_checksum_after_save(self, name, content):
        # The caller sets the source checksum on `content.checksum` (e.g.
        # CRC-64NVME or MD5 of the source). Without a checksum on content
        # we do nothing — opt-in per upload via the content object.
        source_checksum = getattr(content, "checksum", None)
        if source_checksum is None:
            return
        stored_checksum = self.filesystem.checksum(name)
        if stored_checksum != source_checksum:
            self.filesystem.rm(name)
            raise IOError(f"Checksum mismatch after upload of {name}: {stored_checksum!r} != {source_checksum!r}")

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
        self._check_permission("read", name)
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

        if method == "GET":
            self._check_permission("read", name)
        else:
            self._check_permission("write", name)
            # Presigned URLs are opaque after issuance, so we cannot enforce
            # on_collision once the client uses them. Refuse upfront for
            # 'raise' (the caller asked for strictness) and ask the user to
            # rename ahead of time for 'rename'.
            _, on_collision = self._resolve_effective(name)
            if on_collision != "overwrite" and self.exists(name):
                raise PermissionError(
                    f"FsspecStorage on_collision={on_collision!r}: cannot issue "
                    f"a presigned PUT for {name!r} because it already exists "
                    "(presigned URLs bypass server-side collision policy)."
                )

        s3_fs, bucket, key = self._resolve_s3_target(name)

        # Fast path: GET without custom response headers → s3fs can sign
        # it itself (wraps sync + boto3 under the hood). Saves creating an
        # extra sync client.
        if method == "GET" and not response_headers:
            return s3_fs.url(f"{bucket}/{key}", expires=expires)

        boto_client = make_boto3_client_from_s3fs(s3_fs)
        params = {"Bucket": bucket, "Key": key}
        if response_headers:
            params.update(response_headers)
        operation = "get_object" if method == "GET" else "put_object"
        return boto_client.generate_presigned_url(
            operation,
            Params=params,
            ExpiresIn=expires,
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
