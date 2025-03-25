import typing

# if typing.TYPE_CHECKING:
from pathlib import Path

import fsspec
from fsspec import AbstractFileSystem
from fsspec.implementations.dirfs import DirFileSystem


def get_filesystem(
    fs: AbstractFileSystem | None = None,
    protocol: str | None = None,
    relative_to_path: typing.Optional[str | Path] = None,
    **storage_config: typing.Any,
) -> fsspec.AbstractFileSystem:
    """Get a fsspec filesystem from settings

    :param fs: fsspec filesystem object
    :param protocol: protocol for fsspec filesystem
    :param relative_to_path: path to use as base path for the filesystem (fs will be wrapped in a DirFileSystem)
    :param storage_config: configuration for fsspec filesystem
    :return: fsspec filesystem object
    """
    if fs:
        if not isinstance(fs, AbstractFileSystem):
            raise ValueError("fs must be a fsspec filesystem object")
        fs_out = fs
    elif protocol:
        fs_out = fsspec.filesystem(protocol, **storage_config)
    else:
        raise ValueError("either fs or protocol must be provided")

    if relative_to_path is not None:
        fs_out = DirFileSystem(fs=fs_out, path=relative_to_path)
    return fs_out
