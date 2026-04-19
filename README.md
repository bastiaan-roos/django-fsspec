# django-fsspec

Django Storage backend voor [fsspec](https://filesystem-spec.readthedocs.io/).
Met dit package kan elk filesystem dat fsspec ondersteunt (lokaal, S3, GCS,
Azure, Memory, ...) gebruikt worden als Django storage backend.

Naast de storage implementatie zitten er twee "samengestelde" fsspec
filesystems in:

- **`NestedFileSystem`** — routeert paden naar verschillende sub-filesystems
  op basis van een prefix. Handig om bv. `upload/` naar bucket A te sturen en
  `video/` naar bucket B.
- **`TransparentFileSystem`** — legt een writable laag over een (read-only)
  base filesystem. Reads gaan eerst naar de transparent laag, dan naar de
  base. Handig voor lokale dev caches over een remote bucket of voor
  test-isolatie.

## Installatie

```bash
pip install django-fsspec
```

## Quick start — lokaal filesystem

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
                "relative_to_path": "my-bucket",  # bucket als virtuele root
            },
        },
    },
}
```

Daarna in je Django code:

```python
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile

default_storage.save("hello.txt", ContentFile(b"hi"))
default_storage.exists("hello.txt")     # True
default_storage.size("hello.txt")        # 2
with default_storage.open("hello.txt") as f:
    print(f.read())
```

## Use case 2 — multi-bucket routing met `NestedFileSystem`

Stel je hebt meerdere buckets en wil files routeren op basis van een
prefix in de filename:

| `file.name` prefix | Bucket |
|---|---|
| `upload/...` | `myapp-upload` |
| `video/...` | `myapp-video` |
| `archive/...` | `myapp-archive` |
| (rest) | lokale fallback |

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
                    # Fallback voor unmatched prefixes — stuur naar lokale disk
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
`other/file.txt` → lokale disk.

## Use case 3 — read-through cache met `TransparentFileSystem`

Voor lokale dev: gebruik een lokale schrijfbare laag bovenop een remote
bucket. Reads gaan eerst naar de lokale cache, en bij een miss naar S3.
Writes gaan naar de lokale laag (de remote bucket blijft ongewijzigd).

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

## Belangrijke noten

- **`storage.path()` raised `NotImplementedError`** voor remote backends —
  dat is correct per Django's contract. Code die expliciet een lokaal
  filesystem pad nodig heeft moet via `storage.open()` of
  `storage.url()` werken.
- **`_save()` streamt** via `content.chunks()` waar mogelijk, dus grote
  uploads (video, archive dumps) belasten het geheugen niet.
- **Parent directories worden automatisch aangemaakt** voor backends die
  `makedirs` ondersteunen (`protocol="file"` met `auto_mkdir=True`,
  `NestedFileSystem`, etc.).

## Status

Dit is een vendored fork (zie `CHANGELOG.rst` voor de afwijkingen tov. de
upstream). De NestedFileSystem en TransparentFileSystem implementaties
zijn werkend voor de meest gebruikte read/write paths; sommige edge cases
in `walk`, `get`, `put` recursie zijn nog gemarkeerd met TODO's.
