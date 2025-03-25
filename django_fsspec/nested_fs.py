import json
from pathlib import Path

from fsspec import AbstractFileSystem
from fsspec import register_implementation

from .utils import get_filesystem


class NestedFileSystem(AbstractFileSystem):
    """A fsspec filesystem that maps paths to different filesystems based on the path prefix.

    Args:
        path_storage_configs (dict): dictionary with path as key and storage configuration as value.
            'default' path is the root path.
            the format of the storage configuration is the same as the one used in fsspec.filesystem, with additional
            settings for allow_overwrite, allow_delete, and allow_write.
            Dictionary includes the following:
            - fs or protocal: fsspec filesystem object / fsspec filesystem type
            - relative_to_path: path to use as base path for the filesystem (fs will be wrapped in a DirFileSystem)
            - nested_permissions (not implemented yet): dictionary with permissions for the nested filesystems
                - allow_write: allow writing files (default is True)
                - allow_overwrite: allow overwriting files (default is True)
                - allow_delete: allow deleting files (default is True)
            - **storage_options: are specific to the protocol being chosen, and are passed directly to the class.

    Example:

    fs_nested = NestedPathFileSystem(
        path_storage_configs={
            # root path is mapped to a local directory
            'default': {
                'protocol': 'file',
                'relative_to_path': '/tmp',
            },
            # directory 'a' is mapped to a local directory, read only
            'a': {
                'fs': fsspec.filesystem('file'),
                'nested_permissions': {
                    'allow_write': False
                    'allow_overwrite': False,
                    'allow_delete': False,
                }
            },
            # directory 'b' is mapped to a s3 bucket
            'b': {
                'fs': 's3',
                'endpoint_url': 'https://ams3.digitaloceanspaces.com',
                'access_key': 'my-access-key',
                'secret_key: "",
                'relative_to_path': 'my-bucket',  # use bucket name here
                },
            },
        }
    )
    """

    # todo: implement nested_permissions

    protocol = "nested"

    def __init__(self, path_storage_configs, **storage_options):
        super().__init__(**storage_options)
        self.file_systems = {}
        self.permissions = {}
        for path, storage_config_orig in path_storage_configs.items():
            storage_config = storage_config_orig.copy()
            permissions = storage_config.pop("nested_permissions", {})
            self.permissions[path] = {
                "allow_write": permissions.get("allow_write", True),
                "allow_overwrite": permissions.get("allow_overwrite", True),
                "allow_delete": permissions.get("allow_delete", True),
            }
            self.file_systems[path] = get_filesystem(**storage_config)
        # fsid is hash of the storage configurations
        self._fsid = hash(json.dumps(path_storage_configs, default=str))

    @property
    def fsid(self):
        return self._fsid

    def _get_filesystem(self, path: str | Path) -> (AbstractFileSystem, str, str):
        """Returns the nested filesystem and the nested path (path minus the root path).

        Args:
            path (str | Path): (relative) path to the file or directory
        """
        parts = str(path).split("/", 1)
        if len(parts) == 1:
            if path in self.file_systems:
                return self.file_systems[path], path, ""
            root_path = None
            nested_path = parts[0]
        else:
            root_path, nested_path = parts
        if root_path in self.file_systems:
            return self.file_systems[root_path], root_path, nested_path
        return self.file_systems["default"], "", path

    def mkdir(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        if fs is None:
            raise ValueError("Cannot create directory")
        return fs.mkdir(nested_path, *args, **kwargs)

    def makedirs(self, path, exist_ok=False):
        fs, root_path, nested_path = self._get_filesystem(path)
        if fs is None:
            raise ValueError("Cannot create directory")
        return fs.makedirs(nested_path, exist_ok=exist_ok)

    def rmdir(self, path, *args, **kwargs):
        if path == "":
            raise ValueError("Cannot remove root path")
        fs, root_path, nested_path = self._get_filesystem(path)
        if fs is None:
            raise ValueError("Cannot remove directory in root path")
        return fs.rmdir(nested_path, *args, **kwargs)

    def ls(self, path, detail=True, **kwargs):
        # no args!?
        if path == "":
            if detail:
                out = [
                    {"name": rpath, "size": None, "type": "directory"}
                    for rpath in self.file_systems.keys()
                    if rpath != "default"
                ]
            else:
                out = list(self.file_systems.keys())

            if "default" in self.file_systems:
                out += self.file_systems["default"].ls("", detail=detail, **kwargs)
            return out
        fs, root_path, nested_path = self._get_filesystem(path)
        out = fs.ls(nested_path, detail=detail, **kwargs)
        if detail:
            for item in out:
                item["name"] = f"{fs.root_path}/{item['name']}"
        else:
            out = [f"{fs.root_path}/{rpath}" for rpath in out]
        return out

    def walk(self, path, maxdepth=None, **kwargs):
        # todo: recursive including other fs

        # no args!?
        if path == "":
            if "default" in self.file_systems:
                for base_path, dirs, files in self.file_systems["default"].walk(
                    "", maxdepth=maxdepth, **kwargs
                ):
                    if base_path == "":
                        yield "", dirs + [
                            rpath
                            for rpath in self.file_systems.keys()
                            if rpath != "default"
                        ], files
                    else:
                        yield base_path, dirs, files
            for root_path, fs in self.file_systems.items():
                if root_path == "default":
                    continue
                for base_path, dirs, files in fs.ls(
                    "", maxdepth=maxdepth - 1, **kwargs
                ):
                    yield f"{root_path}/{base_path}", dirs, files

    # def find(self, path, **kwargs) uses isdir, info, walk, isfile
    # def du(self, path, **kwargs): uses isdir, info, walk, isfile
    # def glob(self, path, maxdepth=None, **kwargs): uses exists, info, find

    def exists(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        if fs is None:
            return False
        return fs.exists(nested_path, *args, **kwargs)

    def lexists(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        if fs is None:
            return False
        return fs.lexists(nested_path, *args, **kwargs)

    # def info(self, path, **kwargs): uses ls

    def checksum(self, path, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.checksum(nested_path, **kwargs)

    def size(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.size(nested_path, *args, **kwargs)

    # def sizes(self, paths, **kwargs): uses size

    def isdir(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.isdir(nested_path, *args, **kwargs)

    def isfile(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.isfile(nested_path, *args, **kwargs)

    def read_text(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.read_text(nested_path, *args, **kwargs)

    def write_text(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.write_text(nested_path, *args, **kwargs)

    def cat_file(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.cat_file(nested_path, *args, **kwargs)

    def pipe_file(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.pipe_file(nested_path, *args, **kwargs)

    def pipe(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.pipe(nested_path, *args, **kwargs)

    # def cat_ranges(self, paths, **kwargs): uses cat_file

    def cat(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.cat(nested_path, *args, **kwargs)

    def get_file(self, rpath, lpath, *args, **kwargs):
        # copy to local path
        fs, root_path, nested_path = self._get_filesystem(rpath)
        return fs.get_file(nested_path, lpath, *args, **kwargs)

    def get(self, rpath, lpath, *args, **kwargs):
        # copy to local path
        # todo: recursive=False?!
        if rpath == "":
            raise ValueError("Cannot get root path")
        fs, root_path, nested_path = self._get_filesystem(rpath)
        return fs.get(nested_path, lpath, *args, **kwargs)

    def put_file(self, lpath, rpath, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(rpath)
        return fs.put_file(lpath, nested_path, *args, **kwargs)

    def put(self, lpath, rpath, *args, **kwargs):
        # todo: recursive=False?
        fs, root_path, nested_path = self._get_filesystem(rpath)
        if fs is None:
            raise ValueError(f"Cannot put file to {rpath}")
        return fs.put(lpath, nested_path, *args, **kwargs)

    def head(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.head(nested_path, *args, **kwargs)

    def tail(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.tail(nested_path, *args, **kwargs)

    def cp_file(self, path1, path2, **kwargs):
        # todo: no args?
        fs1, nested_path1 = self._get_filesystem(path1)
        fs2, nested_path2 = self._get_filesystem(path2)
        if fs1 == fs2:
            return fs1.cp_file(nested_path1, nested_path2, **kwargs)
        else:
            return fs2.put_file(fs1.get_file(nested_path1), nested_path2, **kwargs)

    # def copy(self, path1, path2, **kwargs): uses cp_file, isdir, expand_path

    # def expand_path(self, path, recursive=False, maxdepth=None, **kwargs): uses glob, expand_path, exists

    def mv(self, path1, path2, **kwargs):
        # todo: no args?
        fs1, nested_path1 = self._get_filesystem(path1)
        fs2, nested_path2 = self._get_filesystem(path2)
        if fs1 == fs2:
            return fs1.mv(nested_path1, nested_path2, **kwargs)
        else:
            self.cp_file(path1, path2, **kwargs)
            return fs1.rm(nested_path1, **kwargs)

    def rm_file(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.rm_file(nested_path, *args, **kwargs)

    def rm(self, path, recursive=False, maxdepth=None):
        if recursive:
            # todo: if recursive, find other fs
            pass

        fs, root_path, nested_path = self._get_filesystem(path)
        if fs is None:
            raise ValueError("Cannot remove file or directory")
        if maxdepth is not None and root_path:
            # substract depth from maxdepth (depth of root path)
            maxdepth -= len(root_path.split("/"))

        return fs.rm(nested_path, recursive, maxdepth=maxdepth)

    def _open(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs._open(nested_path, *args, **kwargs)

    def open(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.open(nested_path, *args, **kwargs)

    def touch(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.touch(nested_path, *args, **kwargs)

    def ukey(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.ukey(nested_path, *args, **kwargs)

    def read_block(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.read_block(nested_path, *args, **kwargs)

    def clear_instance_cache(self):
        for fs in self.file_systems.values():
            fs.clear_instance_cache()

    def created(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.created(nested_path, *args, **kwargs)

    def modified(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        return fs.modified(nested_path, *args, **kwargs)


# Registreer het bestandssysteem
register_implementation("nested", NestedFileSystem)
