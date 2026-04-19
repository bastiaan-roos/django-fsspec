import posixpath

from django.core.exceptions import ImproperlyConfigured
from django.core.files.storage import Storage
from django.utils.crypto import get_random_string

from .utils import get_filesystem


class FsspecStorage(Storage):
    """Django Storage implementation using fsspec

    settings:
    - fsspec
      could be a fsspec filesystem object or a dictionary with fsspec configuration and fs_type for type of filesystem

    example settings with nested filesystem:
    STORAGES = {
        "default": {
            BACKEND: "django_fsspec.storage.FsspecStorage",
            OPTIONS: {
                "storage_config": {
                    "protocol": "transparent",
                    "transparent_fs": {
                        fs: "file",
                        "path": "/path/to/files/transparent",
                    },
                    "underlying_fs": {
                        "fs_type": "nested",
                        "storage_config": {
                        "dir_a": {
                            fs: "s3",
                            "fsspec_config": {
                                "endpoint_url": env.get("S3_TEST_ENDPOINT_URL"),
                                "key": env.get("S3_TEST_ACCESS_KEY"),
                                "secret": env.get("S3_TEST_SECRET_KEY"),
                            },
                            "relative_to_path": env.get("S3_TEST_BUCKET_NAME"),
                        },
                        "dir_b": {
                            fs: "s3",
                            "fsspec_config": {
                                "endpoint_url": env.get("S3_TEST_ENDPOINT_URL"),
                                "key": env.get("S3_TEST_ACCESS_KEY"),
                                "secret": env.get("S3_TEST_SECRET_KEY"),
                            },
                            "relative_to_path": env.get("S3_TEST_BUCKET_NAME2"),
                        },
                        "default": {
                            "fs": "file",
                            "path": "/path/to/default/files",
                        },
                    }
                }
            }
        },
        "staticfiles": {
            "BACKEND": "django.contrib.staticfiles.storage.StaticFilesStorage",
        }
    }

    from django.core.files.storage import storages

    storage = storages["default"]
    storage.exists("path/to/file")
    storage.open("path/to/file")

    """

    def __init__(self, **settings):
        settings_cp = settings.copy()
        self.location = settings_cp.pop("location", "")
        self.base_url = settings_cp.pop("base_url", None)
        self.file_permissions_mode = settings_cp.pop("file_permissions_mode", None)
        self.directory_permissions_mode = settings_cp.pop("directory_permissions_mode", None)
        self.allow_overwrite = settings_cp.pop("allow_overwrite", True)

        if "storage_config" not in settings_cp:
            raise ImproperlyConfigured("storage_config is required")
        storage_config = settings_cp.pop("storage_config")
        self.filesystem = get_filesystem(**storage_config)

        # extra (not implemented yet)
        # self.allow_delete = settings.get('allow_delete', False)
        # self.allow_write = settings.get('allow_write', False)

        if settings_cp:
            raise ImproperlyConfigured(f"Unknown setting(s): {settings_cp.keys()}")

    def delete(self, name):
        return self.filesystem.rm(name)

    def exists(self, name):
        return self.filesystem.exists(name)

    def listdir(self, path):
        """Geef (directories, files) terug zoals Django's Storage.listdir() contract.

        Voorheen zat er dead code na een early `return` en de niet-bereikbare
        code refereerde naar een onbestaande variabele. Nu correct in lijn met
        de Django Storage API.
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
        """Extra: List a directory with details"""
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

        return name

    def size(self, name):
        return self.filesystem.size(name)

    def url(self, name):
        if not self.base_url:
            raise ValueError(
                "FsspecStorage instance has no base_url configured; "
                "set it via OPTIONS['base_url'] in STORAGES if you need url() support."
            )
        return self.base_url.rstrip("/") + "/" + name.lstrip("/")

    def get_accessed_time(self, name):
        raise NotImplementedError

    def get_alternative_name(self, file_root, file_ext):
        """Append een korte random suffix vóór de extensie (Django contract)."""
        return "%s_%s%s" % (file_root, get_random_string(7), file_ext)


    def get_created_time(self, name):
        return self.filesystem.created(name)

    def get_modified_time(self, name):
        return self.filesystem.modified(name)

    # optional:
    # def get_accessed_time(self, name):
    # def get_alternative_name(self,file_root, file_ext):
    # def get_created_time(self, name):
    # def get_modified_time(self, name):
    # def get_valid_name(self, name):
    # def generate_filename(self, filename):

    # extra:
    # def url_direct(self, name):
    # def url_signed(self, name, expires=None):
