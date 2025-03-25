__version__ = "0.0.1a1"

from .fsspec_storage import FsspecStorage
from .nested_fs import NestedFileSystem
from .utils import get_filesystem

__all__ = [
    "FsspecStorage",
    "NestedFileSystem",
    "get_filesystem",
]
