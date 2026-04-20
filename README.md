# django-fsspec

A Django Storage backend for [fsspec](https://filesystem-spec.readthedocs.io/).
With this package every filesystem that fsspec supports (local, S3, GCS,
Azure, memory, ...) can be used as a Django storage backend.

Alongside the storage implementation, two "composed" fsspec filesystems are
shipped as well:

- **`NestedFileSystem`** — routes paths to different sub-filesystems based
  on a prefix. Useful for sending e.g. `upload/` to bucket A and `video/`
  to bucket B.
- **`TransparentFileSystem`** — lays a writable layer on top of a
  (read-only) base filesystem. Reads go to the transparent layer first,
  then fall through to the base. Useful for local dev caches over a
  remote bucket or for test isolation.

## Installation

```bash
pip install django-fsspec          # core only
pip install django-fsspec[s3]      # with S3 support (pulls in s3fs)
```

## Quick start — local filesystem

```python
# settings.py
STORAGES = {
    "default": {
        "BACKEND": "django_fsspec.FsspecStorage",
        "OPTIONS": {
            "location": "/var/myapp/media",
            "base_url": "/media/",
            "storage_config": {
                "protocol": "file",
                "auto_mkdir": True,
            },
        },
    },
}
```

## Use case 1 — single S3 bucket

```python
STORAGES = {
    "default": {
        "BACKEND": "django_fsspec.FsspecStorage",
        "OPTIONS": {
            "base_url": "https://cdn.example.com/",
            "storage_config": {
                "protocol": "s3",
                "endpoint_url": "https://s3.eu-central-1.amazonaws.com",
                "key": os.environ["S3_KEY"],
                "secret": os.environ["S3_SECRET"],
                "relative_to_path": "my-bucket",  # bucket as virtual root
            },
        },
    },
}
```

Then in your Django code:

```python
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile

default_storage.save("hello.txt", ContentFile(b"hi"))
default_storage.exists("hello.txt")     # True
default_storage.size("hello.txt")        # 2
with default_storage.open("hello.txt") as f:
    print(f.read())

# Presigned download URL (1 hour TTL)
url = default_storage.url_signed("hello.txt", expires=3600)

# Presigned upload URL
upload_url = default_storage.url_signed("hello.txt", method="PUT", expires=600)
```

## Use case 2 — multi-bucket routing with `NestedFileSystem`

Suppose you have multiple buckets and want to route files based on a
prefix in the filename:

| prefix in `file.name` | bucket            |
| --------------------- | ----------------- |
| `upload/...`          | `myapp-upload`    |
| `video/...`           | `myapp-video`     |
| `archive/...`         | `myapp-archive`   |
| (rest)                | local fallback    |

```python
STORAGES = {
    "default": {
        "BACKEND": "django_fsspec.FsspecStorage",
        "OPTIONS": {
            "base_url": "/media/",
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
                    "archive": {
                        "protocol": "s3",
                        "endpoint_url": S3_ENDPOINT,
                        "key": S3_KEY, "secret": S3_SECRET,
                        "relative_to_path": "myapp-archive",
                    },
                    # Fallback for unmatched prefixes — send to local disk.
                    "default": {
                        "protocol": "file",
                        "auto_mkdir": True,
                    },
                },
            },
        },
    },
}
```

`upload/foo.ribx` → `myapp-upload` bucket; `video/intro.mp4` → `myapp-video`;
`other/file.txt` → local disk.

## Use case 3 — read-through cache with `TransparentFileSystem`

For local development: use a local writable layer on top of a remote
bucket. Reads go to the local cache first, and fall through to S3 on a
miss. Writes go to the local layer (the remote bucket stays unchanged).

```python
STORAGES = {
    "default": {
        "BACKEND": "django_fsspec.FsspecStorage",
        "OPTIONS": {
            "storage_config": {
                "protocol": "transparent",
                "transparent_fs": {
                    "protocol": "file",
                    "auto_mkdir": True,
                    "relative_to_path": "/tmp/dev-cache",
                },
                "base_fs": {
                    "protocol": "s3",
                    "endpoint_url": S3_ENDPOINT,
                    "key": S3_KEY, "secret": S3_SECRET,
                    "relative_to_path": "production-bucket",
                },
            },
        },
    },
}
```

## Presigned URLs and checksums

`FsspecStorage` exposes extras on top of Django's standard Storage API:

- `storage.url_signed(name, expires=3600, method="GET", response_headers=None)`
  generates a presigned S3 URL. Works through the
  `NestedFileSystem → DirFileSystem → S3FileSystem` stack. Pass
  `method="PUT"` for browser-direct uploads. `response_headers` lets
  callers inject headers such as `ResponseContentDisposition` for
  friendly filenames on download.
- `storage.url_direct(name)` returns a virtual-hosted-style URL without
  a signature — usable only for buckets with a public-read ACL.
- Pass `verify_checksum=True` in `OPTIONS` to have `_save()` compare
  `content.checksum` (e.g. CRC-64NVME or MD5) against the checksum S3
  reports back; mismatches delete the uploaded object and raise
  `IOError`.

## Important notes

- **`storage.path()` raises `NotImplementedError`** for remote backends —
  that is correct per Django's contract. Code that explicitly needs a
  local filesystem path must go through `storage.open()` or
  `storage.url()`.
- **`_save()` streams** via `content.chunks()` where possible, so large
  uploads (video, archive dumps) do not pressure memory.
- **Parent directories are created automatically** for backends that
  support `makedirs` (`protocol="file"` with `auto_mkdir=True`,
  `NestedFileSystem`, etc.).
- **Presigned URLs use SigV4.** AWS has deprecated SigV2, and
  S3-compatible providers such as DigitalOcean Spaces reject SigV2 for
  PUT presigned URLs. `django-fsspec` forces `signature_version="s3v4"`
  on the internal boto3 client.

## Development

### Install for local development

The recommended workflow uses [pixi](https://pixi.sh) — it provisions the
Python environment, installs the package editable with the `s3` extra,
and exposes every common action as a named task:

```bash
git clone https://github.com/bastiaan-roos/django-fsspec.git
cd django-fsspec
pixi install                              # provisions .pixi/envs/default
                                          # and installs the pre-commit git hook
```

Prefer a plain virtualenv? That works too:

```bash
python -m venv venv
source venv/bin/activate
pip install -e ".[s3]"
pip install pytest pytest-cov pytest-django ruff build twine python-dotenv tox
```

### Run the tests

Single Python/Django combination (fast):

```bash
pixi run tests                            # pytest --cov=django_fsspec
pixi run tests-verbose                    # adds -v
pixi run -- pytest tests/test_nested_fs.py -v
pixi run -- pytest -k presigned
```

Full matrix (every supported Python × Django combination, via tox):

```bash
pixi run tox                              # every env in tox.ini
pixi run -- tox -e py3.12-django5.1       # one env
pixi run -- tox -e ruff                   # the lint-only env
```

The S3 integration tests in `tests/test_with_s3.py` are skipped unless
the following environment variables are set (typically via a
`tests/.env` file that is **not** committed):

```
S3_TEST_ENDPOINT_URL=https://<region>.digitaloceanspaces.com
S3_TEST_ACCESS_KEY=...
S3_TEST_SECRET_KEY=...
S3_TEST_BUCKET_NAME=...
S3_TEST_BUCKET_NAME2=...
# (and the …3 variants for the second-endpoint tests)
```

### Style and lint

```bash
pixi run style                            # sort_imports + fmt
pixi run sort_imports                     # ruff check --select I --fix
pixi run fmt                              # ruff format
pixi run lint                             # ruff check ./django_fsspec
```

CI runs the `ruff` tox env on every push.

### Pre-commit hooks

A `.pre-commit-config.yaml` runs ruff (check + format), trailing-
whitespace, end-of-file, YAML/TOML validity and a large-file guard on
every commit. `pixi install` auto-wires the git hook via a
`postinstall` task; to run the full sweep manually:

```bash
pixi run pre-commit-run                   # run every hook on every file
pixi run pre-commit-install               # re-install the git hook
```

The hook runs inside the pre-commit-managed environment (not the pixi
env), so the ruff version is pinned via the `rev` in
`.pre-commit-config.yaml` — bump it with `pixi run -- pre-commit autoupdate`.

### Build an installable artifact

```bash
pixi run build                            # cleans dist/, builds wheel + sdist
pixi run build-check                      # twine check dist/*
```

## Publishing to PyPI

One-time setup:

1. Register an account on https://pypi.org and on
   https://test.pypi.org (separate accounts).
2. Enable 2FA on both.
3. Create an API token on each site (Account settings → API tokens,
   scope: "Entire account" for the first upload, later narrow to the
   project).

Test the release on Test PyPI first:

```bash
pixi run publish-test
# Username: __token__
# Password: <your Test PyPI token>

# In a clean venv, verify the install works:
pip install --index-url https://test.pypi.org/simple/ \
            --extra-index-url https://pypi.org/simple/ \
            django-fsspec
python -c "from django_fsspec import FsspecStorage; print('ok')"
```

Real release:

```bash
pixi run publish
# Username: __token__
# Password: <your PyPI token>
```

Confirm at https://pypi.org/project/django-fsspec/.

For subsequent releases, **Trusted Publishing** via GitHub Actions OIDC
is strongly recommended — no more tokens to rotate. See
<https://docs.pypi.org/trusted-publishers/> and add a
`.github/workflows/publish.yml` that runs on a release tag.

See `TODO.md` for the full pre-publish checklist (LICENSE, metadata,
name availability, classifiers).

## Status

Beta (`0.1.0b1`). `FsspecStorage`, `NestedFileSystem`, and
`TransparentFileSystem` are working for the commonly used read/write
paths and are test-covered against a real S3-compatible backend.
Presigned URLs (GET and PUT) and optional checksum verification ship
as of `0.1.0b1`. Some edge cases in `walk`, `get`, and `put`
recursion are still marked with TODOs in the source; contributions
welcome.
