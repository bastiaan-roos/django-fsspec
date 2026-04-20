import typing
from pathlib import Path
from urllib.parse import urlparse

import boto3
import fsspec
from botocore.config import Config
from fsspec import AbstractFileSystem
from fsspec.implementations.dirfs import DirFileSystem

try:
    from s3fs import S3FileSystem
except ImportError:  # pragma: no cover — s3fs is an explicit install extra
    S3FileSystem = None


def get_filesystem(
    fs: AbstractFileSystem | None = None,
    protocol: str | None = None,
    relative_to_path: typing.Optional[str | Path] = None,
    **storage_config: typing.Any,
) -> fsspec.AbstractFileSystem:
    """Get a fsspec filesystem from settings.

    Parameters
    ----------
    fs : fsspec.AbstractFileSystem, optional
        Pre-built filesystem object.
    protocol : str, optional
        fsspec protocol name (e.g. 's3', 'file', 'nested').
    relative_to_path : str or Path, optional
        When set, wraps `fs` in a `DirFileSystem` with this as the root path.
    **storage_config : Any
        Extra configuration passed to ``fsspec.filesystem(protocol, ...)``.

    Returns
    -------
    fsspec.AbstractFileSystem

    Raises
    ------
    ValueError
        When neither `fs` nor `protocol` is supplied, or `fs` is not an
        ``AbstractFileSystem``.
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


def unwrap_s3_target(fs: AbstractFileSystem, path: str):
    """Unwrap a `DirFileSystem`-wrapped `S3FileSystem` to (s3_fs, bucket, key).

    Parameters
    ----------
    fs : fsspec.AbstractFileSystem
        Either a `DirFileSystem` whose inner filesystem is an `S3FileSystem`,
        or a bare `S3FileSystem`. In the latter case the first segment of
        `path` is treated as the bucket.
    path : str
        Path within `fs` (i.e. relative to the DirFileSystem root, or the
        full `bucket/key` when `fs` is a bare `S3FileSystem`).

    Returns
    -------
    tuple of (S3FileSystem, str, str)
        The underlying `s3fs.S3FileSystem`, the bucket name, and the
        object key.

    Raises
    ------
    NotImplementedError
        When `fs` does not route to an `S3FileSystem` (e.g. local fallback).
        The message names the concrete type so callers can give useful
        error messages.
    """
    if S3FileSystem is None:
        raise NotImplementedError(
            "s3fs is not installed; install django-fsspec[s3] for S3 support"
        )

    if isinstance(fs, DirFileSystem):
        bucket = str(fs.path)
        inner = fs.fs
        key = path
    else:
        inner = fs
        bucket, _, key = path.partition("/")
        if not bucket or not key:
            raise NotImplementedError(
                f"Bare S3FileSystem requires 'bucket/key' path, got {path!r}"
            )

    if not isinstance(inner, S3FileSystem):
        raise NotImplementedError(
            f"Sub-filesystem is {type(inner).__name__}, not S3FileSystem; "
            "signing and direct URLs require an S3 backend"
        )
    return inner, bucket, key


def make_boto3_client_from_s3fs(s3_fs):
    """Build a synchronous ``boto3.client('s3', ...)`` from an `S3FileSystem`.

    s3fs uses the async `aiobotocore`; `generate_presigned_url` requires a
    synchronous boto3 client. This helper reads credentials and endpoint
    configuration off the existing `s3fs.S3FileSystem` instance so callers
    do not need to re-specify them.

    Parameters
    ----------
    s3_fs : s3fs.S3FileSystem

    Returns
    -------
    botocore.client.S3
        Synchronous boto3 S3 client configured with the same credentials
        and endpoint as `s3_fs`.
    """
    kwargs = {}
    if getattr(s3_fs, "key", None):
        kwargs["aws_access_key_id"] = s3_fs.key
    if getattr(s3_fs, "secret", None):
        kwargs["aws_secret_access_key"] = s3_fs.secret
    if getattr(s3_fs, "token", None):
        kwargs["aws_session_token"] = s3_fs.token
    client_kwargs = getattr(s3_fs, "client_kwargs", None) or {}
    endpoint_url = client_kwargs.get("endpoint_url") or getattr(s3_fs, "endpoint_url", None)
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    region = client_kwargs.get("region_name")
    if region:
        kwargs["region_name"] = region
    # SigV4: AWS has deprecated SigV2 and S3-compatible providers
    # (notably DigitalOcean Spaces) reject SigV2 for PUT presigned URLs.
    # Forcing s3v4 keeps presigned URLs interoperable across providers.
    kwargs["config"] = Config(signature_version="s3v4")
    return boto3.client("s3", **kwargs)


def build_virtual_hosted_url(s3_fs, bucket: str, key: str) -> str:
    """Build a virtual-hosted-style S3 URL (no signature).

    Parameters
    ----------
    s3_fs : s3fs.S3FileSystem
    bucket : str
    key : str

    Returns
    -------
    str
        `{scheme}://{bucket}.{endpoint_host}/{key}`. Only usable for objects
        whose ACL permits public read; otherwise S3 returns 403.

    Raises
    ------
    NotImplementedError
        When no endpoint URL is configured on `s3_fs`.
    """
    client_kwargs = getattr(s3_fs, "client_kwargs", None) or {}
    endpoint_url = client_kwargs.get("endpoint_url") or getattr(s3_fs, "endpoint_url", None)
    if not endpoint_url:
        raise NotImplementedError(
            "S3FileSystem has no endpoint_url configured; cannot build direct URL"
        )
    parsed = urlparse(endpoint_url)
    return f"{parsed.scheme}://{bucket}.{parsed.netloc}/{key.lstrip('/')}"
