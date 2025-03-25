import shutil
import typing
from pathlib import Path

from fsspec import AbstractFileSystem
from fsspec import register_implementation

from .utils import get_filesystem


class ExistsReturn(object):
    def __init__(self, exists, where, modification, path=None):
        """
        :param exists: bool
        :param where: int (0: transparent_fs, 1: base_fs)
        :param modification: int (0: no modification, 1: deleted, 2: replaced)
        :param path: str (last existing path)
        """

        self.exists = exists
        self.where = where
        self.modification = modification
        self.path = path


class TransparentFileSystem(AbstractFileSystem):
    """A fsspec filesystem that put a 'transparent' layer on top of another filesystem. File writes and deletes are
    done on this transparent filesystem. Reads are done from the transparent filesystem if the file exists, otherwise
    from the underlying base filesystem.

    This filesystem is useful for testing purposes, where you want to test the behaviour of a filesystem without
    actually writing to it.

    Args:
        transparent_fs (AbstractFileSystem or storage_configuration): The transparent filesystem
        base_fs (AbstractFileSystem): The underlying filesystem

    Examples:


    fs_nested = TransparentFileSystem(
        transparent_fs=fsspec.filesystem("file", {"auto_mkdir": True})
        base_fs={
            "protocol": "dir",
            "path": "/tmp",
            "target_fs": "file",
            "target_options": {"auto_mkdir": True},
        );
    """

    """
    Implementation details:
    - for deleted file or directory, create a file or directory with the name <name>.deleted
    - for replaced (or recreated) directory, create a directory with the name <name>.replaced

    """
    # todo: implement deleted paths and files

    protocol = "transparent"

    def __init__(
        self,
        transparent_fs: typing.Dict | AbstractFileSystem,
        base_fs: typing.Dict | AbstractFileSystem,
        **storage_options,
    ):
        super().__init__(**storage_options)

        if isinstance(transparent_fs, dict):
            self.transparent_fs = get_filesystem(**transparent_fs)
        elif isinstance(transparent_fs, AbstractFileSystem):
            self.transparent_fs = transparent_fs
        else:
            raise ValueError(
                "transparent_fs must be a fsspec filesystem object or a dictionary with fsspec configuration"
            )

        if isinstance(base_fs, dict):
            self.base_fs = get_filesystem(**base_fs)
        elif isinstance(base_fs, AbstractFileSystem):
            self.base_fs = base_fs
        else:
            raise ValueError(
                "base_fs must be a fsspec filesystem object or a dictionary with fsspec configuration"
            )

    def _get_path_tree(self, path):
        split_path = path.split("/")
        paths = ["/".join(split_path[:i]) for i in range(1, len(split_path) + 1)]
        return paths

    def _check_exists_and_where(self, path) -> ExistsReturn:
        """
        Check if path exists in transparent_fs or base_fs and return where it exists
        :param path:
        :return: ExistsReturn

        """
        # check from root to leaf if path exists in transparent_fs
        check_base_fs = True
        modification = 0
        for pt in self._get_path_tree(path):
            if self.transparent_fs.exists(pt):
                if pt == path:
                    return ExistsReturn(True, 0, 0, pt)
            elif self.transparent_fs.exists(pt + ".deleted"):
                return ExistsReturn(False, 0, 1, pt)
            elif self.transparent_fs.exists(pt + ".replaced"):
                check_base_fs = False
                modification = 2

        if check_base_fs:
            return ExistsReturn(self.base_fs.exists(path), 1, modification, path)
        return ExistsReturn(False, 0, modification, path)

    def exists(self, path, *args, **kwargs):
        # check from root to leaf if path exists in transparent_fs
        out = self._check_exists_and_where(path)
        return out.exists

    def isdir(self, path, *args, **kwargs):
        out = self._check_exists_and_where(path)
        if not out.exists:
            return False
        if out.where == 0:
            return self.transparent_fs.isdir(path)
        return self.base_fs.isdir(path)

    def isfile(self, path, *args, **kwargs):
        out = self._check_exists_and_where(path)
        if not out.exists:
            return False
        if out.where == 0:
            return self.transparent_fs.isfile(path)
        return self.base_fs.isfile(path)

    def __pre_mkdir(self, path: str, exist_ok: typing.Any) -> None:
        out = self._check_exists_and_where(path)
        if out.exists:
            if exist_ok:
                return
            else:
                raise FileExistsError(
                    f"Cannot create directory {path} because it already exists"
                )
        # if directory or one of the parent directories in the path is deleted, then:
        # - create the directory in the transparent_fs
        # - remove the deleted flag directory
        # - create a directory with the name <name>.replaced
        if out.modification == 1:
            self.transparent_fs.makedirs(out.path, exist_ok=True)
            self.transparent_fs.rmdir(path + ".deleted")
            self.transparent_fs.makedirs(path + ".replaced", exist_ok=True)
        elif out.modification == 2:
            # directory is replaced. keep it like it is now
            pass
        else:
            # check in base directory if parent directory exists
            parent = str(Path(path).parent)
            if self.base_fs.isdir(parent):
                self.transparent_fs.mkdirs(parent)
        return

    def mkdir(self, path, *args, **kwargs):
        self.__pre_mkdir(path, False)
        return self.transparent_fs.mkdir(path, *args, **kwargs)

    def makedirs(self, path, exist_ok=False):
        self.__pre_mkdir(path, exist_ok)
        return self.transparent_fs.mkdirs(path, exist_ok)

    def rmdir(self, path):
        # for deleted files and directories, keep track of them by creating a file or directory
        # with the name <name>.deleted
        if self.base_fs.isdir(path):
            self.transparent_fs.makedirs(path + ".deleted", exist_ok=True)
        else:
            raise ValueError("Cannot remove directory")
        if self.transparent_fs.isdir(path):
            return self.transparent_fs.rmdir(path)

    def ls(self, path, detail=True, **kwargs):
        out = {
            item["name"]: item
            for item in self.base_fs.ls(path, detail=detail, **kwargs)
        }
        for item in self.transparent_fs.ls(path, detail=detail, **kwargs):
            if item.endswith(".deleted"):
                del out[item[:-8]]
            elif item.endswith(".replaced"):
                out[item[:-9]] = {
                    "name": item[:-9],
                    "type": "directory",
                    "size": 0,
                    "created": 0,
                    "modified": 0,
                }
            else:
                out[item] = item
        return out.values()

    def walk(self, path, maxdepth=None, **kwargs):  # noqa: C901, PLR0912
        # zip values of transparent_fs and base_fs
        # make one big dictionary
        out = {
            base_path: {"dirs": dirs, "files": files}
            for base_path, dirs, files in self.base_fs.walk(
                "", maxdepth=maxdepth, **kwargs
            )
        }
        # first loop and delete all paths that are deleted or replaced
        for base_path, dirs, files in self.transparent_fs.walk(
            "", maxdepth=maxdepth, **kwargs
        ):
            replaced = False
            if base_path.endswith(".deleted"):
                bp = base_path[:-8]
                # remove all from out starting with base_path
                out = [k for k in out if not k.startswith(bp)]
                replaced = True
            elif base_path.endswith(".replaced"):
                bp = base_path[:-9]
                # remove all from out starting with base_path
                out = [k for k in out if not k.startswith(bp)]

            if replaced:
                out[base_path] = {"dirs": dirs, "files": files}
            elif base_path in out:
                out_dirs = set(out[base_path]["dirs"])
                for d in dirs:
                    if d.endswith(".deleted"):
                        d_ = d[:-8]
                        out_dirs.remove(d_)
                    else:
                        out_dirs.add(d)
                out[base_path]["dirs"] = list(out_dirs)
                out_files = set(out[base_path]["files"])
                for f in files:
                    if f.endswith(".deleted"):
                        f_ = f[:-8]
                        out_files.remove(f_)
                    else:
                        out_files.add(f)
                out[base_path]["files"] = list(out_files)
            else:
                out[base_path] = {"dirs": dirs, "files": files}

        for base_path, item in out.items():
            yield base_path, item["dirs"], item["files"]

    # def find(self, path, **kwargs) uses isdir, info, walk, isfile
    # def du(self, path, **kwargs): uses isdir, info, walk, isfile
    # def glob(self, path, maxdepth=None, **kwargs): uses exists, info, find

    def __leading_fs(self, path) -> AbstractFileSystem:
        ex = self._check_exists_and_where(path)
        if not ex.exists:
            return self.transparent_fs
        if ex.where == 0:
            return self.transparent_fs
        return self.base_fs

    def lexists(self, path, *args, **kwargs):
        fs, root_path, nested_path = self._get_filesystem(path)
        if fs is None:
            return False
        return fs.lexists(nested_path, *args, **kwargs)

    # def info(self, path, **kwargs): uses ls

    def checksum(self, path):
        fs = self.__leading_fs(path)
        return fs.checksum(path)

    def size(self, path):
        fs = self.__leading_fs(path)
        return fs.size(path)

    # def sizes(self, paths, **kwargs): uses size

    def read_text(self, path, *args, **kwargs):
        fs = self.__leading_fs(path)
        return fs.read_text(path, *args, **kwargs)

    def write_text(self, path, *args, **kwargs):
        fs = self.__leading_fs(path)
        return fs.write_text(path, *args, **kwargs)

    def cat_file(self, path, *args, **kwargs):
        fs = self.__leading_fs(path)
        return fs.cat_file(path, *args, **kwargs)

    def pipe_file(self, path, *args, **kwargs):
        fs = self.__leading_fs(path)
        return fs.pipe_file(path, *args, **kwargs)

    def pipe(self, path, value=None, **kwargs):
        fs = self.__leading_fs(path)
        return fs.pipe(path, value, **kwargs)

    # def cat_ranges(self, paths, **kwargs): uses cat_file

    def cat(self, path, *args, **kwargs):
        fs = self.__leading_fs(path)
        return fs.cat(path, *args, **kwargs)

    def get_file(self, rpath, lpath, *args, **kwargs):
        # copy to local path
        fs = self.__leading_fs(rpath)
        return fs.get_file(rpath, lpath, *args, **kwargs)

    def get(self, rpath, lpath, *args, **kwargs):
        # copy to local path
        # todo: get is also for directories? --> rewrite with loop from both
        fs = self.__leading_fs(rpath)
        return fs.get(rpath, lpath, *args, **kwargs)

    def put_file(self, lpath, rpath, *args, **kwargs):
        # make directory to make sure all flags are correct
        self.mkdirs(str(Path(rpath).parent), exist_ok=True)
        return self.transparent_fs.put_file(lpath, rpath, *args, **kwargs)

    def put(self, lpath, rpath, *args, **kwargs):
        # make directory to make sure all flags are correct
        self.mkdirs(str(Path(rpath).parent), exist_ok=True)
        return self.transparent_fs.put(lpath, rpath, *args, **kwargs)

    def head(self, path, size=1024):
        fs = self.__leading_fs(path)
        return fs.head(path, size)

    def tail(self, path, *args, **kwargs):
        fs = self.__leading_fs(path)
        return fs.tail(path, *args, **kwargs)

    def cp_file(self, path1, path2, **kwargs):
        # todo: no args?
        fs = self.__leading_fs(path1)
        if fs == self.transparent_fs:
            return fs.cp_file(path1, path2, **kwargs)
        else:
            # pipe form one to another
            with (
                fs.open(path1, "rb") as f1,
                self.transparent_fs.open(path2, "wb") as f2,
            ):
                # shutil uses buffers
                shutil.copyfileobj(f1, f2)

    # def copy(self, path1, path2, **kwargs): uses cp_file, isdir, expand_path

    # def expand_path(self, path, recursive=False, maxdepth=None, **kwargs): uses glob, expand_path, exists

    def mv(self, path1, path2, **kwargs):
        fs = self.__leading_fs(path1)
        if fs == self.transparent_fs:
            return fs.mv(path1, path2, **kwargs)
        else:
            self.cp_file(path1, path2, **kwargs)
            self.rm_file(path1)

    def rm_file(self, path):
        fs = self.__leading_fs(path)
        if fs == self.transparent_fs:
            return self.transparent_fs.rm_file(path)
        else:
            # create a file with .deleted flag
            self.transparent_fs.touch(path + ".deleted")
            return True

    def rm(self, path, recursive=False, maxdepth=None):
        # todo: check with maxdepth
        if maxdepth is not None:
            raise NotImplementedError("maxdepth is not implemented yet")

        if not recursive:
            # check if directory or file exists
            if not self.exists(path):
                raise ValueError("Cannot remove file or directory")
            # check if directory is empty
            subdirs, files = self.ls(path)
            if subdirs or files:
                raise ValueError("Cannot remove directory. Directory is not empty")

        ex = self._check_exists_and_where(path)
        if not ex.exists:
            raise FileNotFoundError("Cannot remove file or directory")

        # remove .replaced if exists
        if self.transparent_fs.exists(path + ".replaced"):
            self.transparent_fs.rmdir(path + ".replaced")

        if ex.where == 0:
            # remove from directory
            self.transparent_fs.rm(path, recursive, maxdepth=maxdepth)
            # check if directory exists in base_fs
            if self.base_fs.exists(path):
                # create a file with .deleted flag
                self.transparent_fs.touch(path + ".deleted")
        else:
            # write .deleted flag
            self.transparent_fs.touch(path + ".deleted")
        return True

    def __get_fs_for_open(self, path, method):
        parent = str(Path(path).parent)
        if "w" in method:
            # make sure directory exists
            self.transparent_fs.mkdirs(parent, exist_ok=True)
            return self.transparent_fs
        elif "a" in method:
            # check if first copy is required
            if self.transparent_fs.exists(path + ".deleted"):
                self.transparent_fs.rm(path + ".deleted")
                return self.transparent_fs
            ex = self._check_exists_and_where(path)
            if ex.exists:
                # copy file to transparent_fs
                if ex.where == 1:
                    # copy file to transparent_fs
                    self.transparent_fs.mkdirs(parent, exist_ok=True)
                    self.cp_file(path, path)
            return self.transparent_fs
        elif "r" in method:
            # check if file exists in transparent_fs
            fs = self.__leading_fs(path)
            return fs

    def _open(self, path, method, *args, **kwargs):
        fs = self.__get_fs_for_open(path, method)
        return fs.open(path, method, *args, **kwargs)

    def open(self, path, method, *args, **kwargs):
        fs = self.__get_fs_for_open(path, method)
        return fs.open(path, method, *args, **kwargs)

    def touch(self, path, *args, **kwargs):
        self.mkdirs(str(Path(path).parent), exist_ok=True)
        return self.transparent_fs.touch(path, *args, **kwargs)

    def ukey(self, path: typing.Any) -> str:
        fs = self.__leading_fs(path)
        return fs.ukey(path)

    def read_block(self, path, *args, **kwargs):
        fs = self.__leading_fs(path)
        return fs.read_block(path, *args, **kwargs)

    def clear_instance_cache(self):
        self.transparent_fs.clear_instance_cache()
        self.base_fs.clear_instance_cache()

    def created(self, path):
        fs = self.__leading_fs(path)
        return fs.created(path)

    def modified(self, path):
        fs = self.__leading_fs(path)
        return fs.modified(path)


# Registreer het bestandssysteem
register_implementation("transparent", TransparentFileSystem)
