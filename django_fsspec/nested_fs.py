import json
import os
import tempfile
from typing import TYPE_CHECKING

from fsspec import AbstractFileSystem
from fsspec import register_implementation

from .utils import get_filesystem
from .utils import unwrap_s3_target

if TYPE_CHECKING:
    from pathlib import Path


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

    def _get_filesystem(self, path: "str | Path") -> tuple["AbstractFileSystem | None", str, str]:
        """Returns (filesystem, root_path, nested_path) for the given path.

        - `filesystem`: the sub-filesystem that handles this path, or `None`
          when there is no match and no `default` is configured.
        - `root_path`: the matching prefix (e.g. `"a"` for `"a/foo"`), or
          `""` when the default fs is used.
        - `nested_path`: the path relative to the chosen sub-filesystem.
        """
        path_str = str(path)
        parts = path_str.split("/", 1)
        if len(parts) == 1:
            # Single segment: first check whether it is itself a prefix
            # (e.g. "a" → fs["a"]).
            if path_str in self.file_systems:
                return self.file_systems[path_str], path_str, ""
            root_path = None
            nested_path = path_str
        else:
            root_path, nested_path = parts
            if root_path in self.file_systems:
                return self.file_systems[root_path], root_path, nested_path

        # No prefix match: fall back to the default fs if configured.
        if "default" in self.file_systems:
            return self.file_systems["default"], "", path_str
        return None, "", path_str

    def resolve_s3_target(self, path: str):
        """Resolve `path` to the underlying (S3FileSystem, bucket, key).

        Parameters
        ----------
        path : str
            Path in nested notation (e.g. ``'video/foo.mp4'``).

        Returns
        -------
        tuple of (s3fs.S3FileSystem, str, str)
            The underlying `s3fs.S3FileSystem`, the bucket name (taken from
            the sub-filesystem's `DirFileSystem` root), and the object key
            within that bucket.

        Raises
        ------
        FileNotFoundError
            When no sub-filesystem matches `path` and no `default` is
            configured.
        NotImplementedError
            When the matched sub-filesystem does not ultimately route to an
            `S3FileSystem` (e.g. the local ``default`` fallback).
        """
        fs, _root_path, nested_path = self._get_filesystem(path)
        if fs is None:
            raise FileNotFoundError(f"No sub-filesystem for path {path!r}")
        return unwrap_s3_target(fs, nested_path)

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
        if path == "":
            # Top-level: list every prefix (except "default") as a directory.
            if detail:
                out = [
                    {"name": rpath, "size": None, "type": "directory"}
                    for rpath in self.file_systems.keys()
                    if rpath != "default"
                ]
            else:
                out = [k for k in self.file_systems.keys() if k != "default"]

            # Append the contents of the default fs if one is configured.
            if "default" in self.file_systems:
                out = list(out) + list(self.file_systems["default"].ls("", detail=detail, **kwargs))
            return out

        fs, root_path, nested_path = self._get_filesystem(path)
        if fs is None:
            return []

        out = fs.ls(nested_path, detail=detail, **kwargs)

        # Prefix the names with root_path so callers see consistent paths.
        prefix = f"{root_path}/" if root_path else ""
        if detail:
            for item in out:
                # `item["name"]` is relative to the sub-fs; restore the full path.
                item_name = item.get("name", "")
                item["name"] = f"{prefix}{item_name}"
        else:
            out = [f"{prefix}{rpath}" for rpath in out]
        return out

    def walk(self, path, maxdepth=None, **kwargs):
        """Walk over all paths. For `path=""` iterates over every sub-fs."""
        if path == "":
            # Top-level: yield the prefixes as directories alongside whatever
            # the default fs produces.
            extra_prefixes = [k for k in self.file_systems.keys() if k != "default"]

            if "default" in self.file_systems:
                for base_path, dirs, files in self.file_systems["default"].walk("", maxdepth=maxdepth, **kwargs):
                    if base_path == "":
                        yield "", list(dirs) + extra_prefixes, files
                    else:
                        yield base_path, dirs, files
            else:
                yield "", extra_prefixes, []

            # Then walk per sub-fs (with adjusted depth so the root layer counts).
            sub_maxdepth = None if maxdepth is None else max(0, maxdepth - 1)
            for root_path, fs in self.file_systems.items():
                if root_path == "default":
                    continue
                for base_path, dirs, files in fs.walk("", maxdepth=sub_maxdepth, **kwargs):
                    full_base = f"{root_path}/{base_path}" if base_path else root_path
                    yield full_base, dirs, files
            return

        # Non-root: walk on the specific sub-fs.
        fs, root_path, nested_path = self._get_filesystem(path)
        if fs is None:
            return
        prefix = f"{root_path}/" if root_path else ""
        for base_path, dirs, files in fs.walk(nested_path, maxdepth=maxdepth, **kwargs):
            yield f"{prefix}{base_path}", dirs, files

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
        fs1, _root1, nested_path1 = self._get_filesystem(path1)
        fs2, _root2, nested_path2 = self._get_filesystem(path2)
        if fs1 is None or fs2 is None:
            raise FileNotFoundError(f"No backend filesystem for {path1} or {path2}")
        if fs1 is fs2:
            return fs1.cp_file(nested_path1, nested_path2, **kwargs)
        # Cross-filesystem copy: stream via a temporary local path.
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        try:
            fs1.get_file(nested_path1, tmp_path)
            return fs2.put_file(tmp_path, nested_path2, **kwargs)
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    # def copy(self, path1, path2, **kwargs): uses cp_file, isdir, expand_path

    # def expand_path(self, path, recursive=False, maxdepth=None, **kwargs): uses glob, expand_path, exists

    def mv(self, path1, path2, **kwargs):
        fs1, _root1, nested_path1 = self._get_filesystem(path1)
        fs2, _root2, nested_path2 = self._get_filesystem(path2)
        if fs1 is None or fs2 is None:
            raise FileNotFoundError(f"No backend filesystem for {path1} or {path2}")
        if fs1 is fs2:
            return fs1.mv(nested_path1, nested_path2, **kwargs)
        # Cross-filesystem move: cp dan rm
        self.cp_file(path1, path2, **kwargs)
        return fs1.rm(nested_path1)

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


# Register the filesystem
register_implementation("nested", NestedFileSystem)
