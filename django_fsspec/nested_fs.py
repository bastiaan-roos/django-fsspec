import json
import os
import tempfile
from typing import TYPE_CHECKING

from fsspec import AbstractFileSystem
from fsspec import register_implementation

from .permissions import DEFAULT_ON_COLLISION
from .permissions import DEFAULT_PERMISSIONS
from .permissions import normalize_on_collision
from .permissions import normalize_permissions
from .utils import get_filesystem
from .utils import unwrap_s3_target

if TYPE_CHECKING:
    from pathlib import Path


# Files larger than this threshold are typically uploaded to S3 as multipart
# objects, which makes the ETag a `"{md5}-{partcount}"` digest instead of the
# plain MD5 of the bytes. That digest is not portable across backends, so a
# raw checksum-vs-checksum comparison would always mismatch above this cut-off.
# We conservatively skip checksum verification for files this size or larger
# and rely on size-only verification (which is always meaningful).
#
# The 5 MiB value is the AWS default `multipart_threshold`; AWS CLI uses 16 MiB
# and some tools 8 MiB. Keep this as a single constant so future tuning is
# one edit.
_NON_MULTIPART_LIMIT = 5 * 1024 * 1024


def _compare_checksums_safe(fs1, path1: str, fs2, path2: str, *, size: int) -> bool:
    """Compare checksums of two paths across two filesystems with graceful skip.

    The compare is best-effort: filesystems that cannot produce a portable
    string checksum (local FS returns ``int(size+mtime)``; some backends
    raise ``NotImplementedError``) are skipped rather than treated as a
    failure, because a hard-fail would make this layer unusable for local
    development. Files at or above ``_NON_MULTIPART_LIMIT`` are also
    skipped: S3 multipart uploads change the ETag format
    (``"{md5}-{partcount}"``) and the comparison would always mismatch.

    Parameters
    ----------
    fs1, fs2 : fsspec.AbstractFileSystem
        Source and destination filesystems.
    path1, path2 : str
        Paths within the respective filesystems.
    size : int
        Size of the file in bytes — used to gate the multipart cut-off.

    Returns
    -------
    bool
        ``True`` when checksums matched OR the comparison was skipped.

    Raises
    ------
    IOError
        When both checksums are strings (i.e. comparable) but unequal.
        The destination is **not** removed here — the caller decides
        cleanup, because only the caller knows whether ``path2`` was
        freshly created by the current operation.
    """
    if size >= _NON_MULTIPART_LIMIT:
        # Multipart-uploaded objects use a different ETag formula on S3;
        # a portable comparison is not feasible, so size-check is the
        # strongest guarantee we can offer above this threshold.
        return True
    try:
        cs1 = fs1.checksum(path1)
        cs2 = fs2.checksum(path2)
    except NotImplementedError:
        # Some backends do not implement checksum() at all — that is fine,
        # we already verified size which catches the bulk of corruption.
        return True
    # Local FS returns int(size+mtime); two local FS roots always disagree
    # on mtime so this would be a guaranteed false-positive. Restrict the
    # comparison to portable string checksums.
    if not isinstance(cs1, str) or not isinstance(cs2, str):
        return True
    if cs1 != cs2:
        raise IOError(f"Checksum mismatch between {path1!r} and {path2!r}: {cs1!r} != {cs2!r}")
    return True


class NestedFileSystem(AbstractFileSystem):
    """A fsspec filesystem that maps paths to different filesystems based on the path prefix.

    Args:
        path_storage_configs (dict): dictionary with path as key and storage configuration as value.
            The special key ``'default'`` is the fallback for paths that do not match any prefix.
            Each value is itself a storage configuration:

            - ``fs`` or ``protocol``: fsspec filesystem object / protocol name
            - ``relative_to_path``: optional, wraps the fs in a DirFileSystem rooted at this path
            - ``permissions``: optional dict with ``allow_read`` / ``allow_write`` /
              ``allow_delete`` (default ``True`` each); enforced by the wrapping
              ``FsspecStorage`` (most-restrictive-wins against the top-level permissions)
            - ``on_collision``: optional, one of ``"overwrite"`` / ``"rename"`` /
              ``"raise"`` (default ``"overwrite"``)
            - any other keys: forwarded to ``fsspec.filesystem(protocol, ...)``.

    Example:

    fs_nested = NestedFileSystem(
        path_storage_configs={
            # root path is mapped to a local directory
            'default': {
                'protocol': 'file',
                'relative_to_path': '/tmp',
            },
            # directory 'a' is mapped to a local directory, read only
            'a': {
                'fs': fsspec.filesystem('file'),
                'permissions': {
                    'allow_write': False,
                    'allow_delete': False,
                },
                'on_collision': 'raise',
            },
            # directory 'b' is mapped to a s3 bucket
            'b': {
                'protocol': 's3',
                'endpoint_url': 'https://ams3.digitaloceanspaces.com',
                'key': 'my-access-key',
                'secret': '...',
                'relative_to_path': 'my-bucket',  # use bucket name here
            },
        }
    )
    """

    protocol = "nested"

    def __init__(self, path_storage_configs, **storage_options):
        super().__init__(**storage_options)
        self.file_systems = {}
        self.permissions = {}
        self.on_collision = {}
        for path, storage_config_orig in path_storage_configs.items():
            storage_config = dict(storage_config_orig)
            self.permissions[path] = normalize_permissions(storage_config.pop("permissions", None))
            self.on_collision[path] = normalize_on_collision(storage_config.pop("on_collision", None))
            self.file_systems[path] = get_filesystem(**storage_config)
        # fsid is hash of the storage configurations
        self._fsid = hash(json.dumps(path_storage_configs, default=str))

    def permissions_for(self, path: str) -> tuple[dict, str]:
        """Return ``(permissions, on_collision)`` for the sub-fs handling ``path``.

        Parameters
        ----------
        path : str
            Path in nested notation (e.g. ``'video/foo.mp4'``).

        Returns
        -------
        tuple of (dict, str)
            The sub-fs permissions dict and on_collision value, or the
            ``'default'`` entry when the path falls through, or all-``True``
            permissions and ``"overwrite"`` when no sub-fs matches at all.
        """
        fs, root_path, _ = self._get_filesystem(path)
        if fs is None:
            return dict(DEFAULT_PERMISSIONS), DEFAULT_ON_COLLISION
        key = root_path if root_path else "default"
        return self.permissions[key], self.on_collision[key]

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
            raise FileNotFoundError(f"No sub-filesystem matches {path!r} in nested config")
        return fs.mkdir(nested_path, *args, **kwargs)

    def makedirs(self, path, exist_ok=False):
        fs, root_path, nested_path = self._get_filesystem(path)
        if fs is None:
            raise FileNotFoundError(f"No sub-filesystem matches {path!r} in nested config")
        return fs.makedirs(nested_path, exist_ok=exist_ok)

    def rmdir(self, path, *args, **kwargs):
        if path == "":
            raise ValueError("Cannot remove root path of a NestedFileSystem")
        fs, root_path, nested_path = self._get_filesystem(path)
        if fs is None:
            raise FileNotFoundError(f"No sub-filesystem matches {path!r} in nested config")
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
            raise FileNotFoundError(f"No sub-filesystem matches {rpath!r} in nested config")
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
        # Recursive rm at the conceptual root must walk every sub-fs;
        # otherwise it would only clear the matched (or default) one and
        # silently leave every other sub-fs intact.
        if recursive and path == "":
            for sub_fs in self.file_systems.values():
                try:
                    sub_fs.rm("", recursive=True, maxdepth=maxdepth)
                except FileNotFoundError:
                    # An empty sub-fs is fine; treat as no-op for that fs.
                    continue
            return

        fs, root_path, nested_path = self._get_filesystem(path)
        if fs is None:
            raise FileNotFoundError(f"No sub-filesystem matches {path!r} in nested config")
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
