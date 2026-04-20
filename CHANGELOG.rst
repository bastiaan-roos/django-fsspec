CHANGELOG
=========

0.1.0b1 (2026-04-20)
--------------------

First beta release. First PyPI release under
``bastiaan-roos/django-fsspec``.

- Presigned S3 URLs: ``FsspecStorage.url_signed(name, expires, method,
  response_headers)`` for GET/PUT, plus ``url_direct(name)`` for
  public-read buckets. SigV4 enforced for interop with DigitalOcean
  Spaces and other S3-compatible providers.
- ``NestedFileSystem.resolve_s3_target(path)`` unwraps a nested path to
  ``(S3FileSystem, bucket, key)``. ``TransparentFileSystem`` delegates
  signing to its ``base_fs``.
- Optional integrity check: ``FsspecStorage(verify_checksum=True)``
  compares ``content.checksum`` against the stored checksum after upload
  and removes the object on mismatch.
- ``s3fs`` and ``boto3`` moved to an optional ``[s3]`` extra
  (``pip install django-fsspec[s3]``).
- Bug fixes landed on ``FsspecStorage`` (``path``, ``url``, ``listdir``,
  streaming ``_save``, parent ``makedirs``, ``allow_overwrite``),
  ``NestedFileSystem`` (``_get_filesystem`` without default, ``cp_file``/
  ``mv`` tuple unpacking, ``walk`` / ``ls`` on sub-fs), and
  ``TransparentFileSystem.lexists``.
- Test suite runs against Python 3.11–3.13 × Django 5.0/5.1, covers the
  presigned-URL and resolver code paths against a real S3 backend, and
  switched to ``python-dotenv`` for env loading (no credentials in
  test code).
- Dev classifier bumped from Pre-Alpha to Beta.
