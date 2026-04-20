CHANGELOG
=========

Unreleased (0.0.1a2)
--------------------

New features
~~~~~~~~~~~~

- ``FsspecStorage.url_signed(name, expires=3600, method='GET',
  response_headers=None)`` generates S3 presigned URLs against the
  underlying backend through the ``NestedFileSystem`` /
  ``DirFileSystem`` wrappers. Both ``GET`` (download) and ``PUT``
  (upload) are supported; ``response_headers`` lets callers inject
  ``ResponseContentDisposition`` and friends on downloads.
- ``FsspecStorage.url_direct(name)`` returns the un-signed,
  virtual-hosted-style URL for buckets with public-read ACL.
- ``NestedFileSystem.resolve_s3_target(path)`` resolves a nested path
  to the underlying ``(S3FileSystem, bucket, key)`` tuple so signing
  helpers can reach boto3 directly. Delegates sensibly through
  ``TransparentFileSystem`` (signs against ``base_fs``).
- ``FsspecStorage`` accepts a ``verify_checksum`` OPTION; when set,
  ``_save`` compares ``content.checksum`` against the stored
  checksum after upload and removes the object on mismatch.
- New utility helpers in ``django_fsspec.utils``:
  ``unwrap_s3_target``, ``make_boto3_client_from_s3fs``,
  ``build_virtual_hosted_url``.

Tests (0.0.1a2)
~~~~~~~~~~~~~~~

- ``tests/test_with_s3.py`` now loads env vars via ``python-dotenv``
  (optional dependency) instead of the in-house ``VariabeleLoader``;
  no credentials live in the test code itself.
- Added 10 presigned-URL and resolver tests:
  ``test_resolve_s3_target_nested``, ``test_presigned_get_roundtrip``,
  ``test_presigned_get_response_headers``,
  ``test_presigned_put_upload``, ``test_presigned_expiry``,
  ``test_presigned_put_with_response_headers_raises``,
  ``test_presigned_invalid_method_raises``,
  ``test_resolve_s3_target_raises_for_local_fs``,
  ``test_resolve_s3_target_raises_for_default_local``,
  ``test_resolve_s3_target_no_match_raises_filenotfound``, plus
  two ``verify_checksum`` scenarios.

Docstrings
~~~~~~~~~~

- Rewrote the ``FsspecStorage`` class-level example docstring to valid
  Python (unquoted ``fs:`` keys and the missing ``secret_key`` closing
  quote are gone). Added NumPy-style docstrings for the new public
  methods.

Bug fixes (FsspecStorage)
~~~~~~~~~~~~~~~~~~~~~~~~~

- ``path()`` raised silently a relative string (the file name) instead of
  ``NotImplementedError``. Callers using ``os.path.isfile(field.path)`` or
  ``open(field.path)`` got a misleading value and failed deep down. Now
  raises ``NotImplementedError`` per Django's contract for non-local
  storage.
- ``url()`` crashed with ``TypeError`` when ``base_url`` was ``None``. Now
  raises a clear ``ValueError`` and joins the base URL safely (no double
  slashes).
- ``listdir()`` had unreachable dead code after an early ``return`` and
  referenced an undefined ``name`` variable. Rewritten to return the
  correct ``(directories, files)`` tuple per Django's contract, handling
  both dict and string output from fsspec.
- ``_save()``:

  - now creates parent directories via ``filesystem.makedirs(parent,
    exist_ok=True)`` so backends like ``protocol="file"`` don't fail on
    missing parents.
  - streams content via ``content.chunks()`` (or 4 MB blocks for
    file-likes without ``chunks()``) instead of loading the entire file
    into memory with ``content.read()``.
  - honors ``allow_overwrite=False`` properly by falling back to
    ``get_available_name()`` instead of silently overwriting.
- ``get_random_string`` is now imported from ``django.utils.crypto`` (uses
  the ``secrets`` module) instead of an in-house ``random.choice``
  implementation.

Bug fixes (NestedFileSystem)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``_get_filesystem()`` raised ``KeyError`` when no ``default`` entry was
  configured. Now returns ``(None, "", path)`` so callers can handle the
  unmatched case explicitly.
- ``cp_file()`` and ``mv()`` destructured the ``_get_filesystem()`` return
  value as a 2-tuple while the function returns a 3-tuple. Both crashed
  with ``ValueError: too many values to unpack`` on every call. Fixed and
  the cross-filesystem code path (``cp_file`` between two different
  sub-filesystems) now uses a temporary local file to stream between
  backends instead of the broken ``put_file(get_file(...))`` chain.
- ``walk()`` was broken in three ways: it called ``fs.ls(maxdepth=...)``
  (``ls`` does not accept ``maxdepth``), tried to destructure ``ls()``
  output as 3-tuples (it returns a flat list), and computed
  ``maxdepth - 1`` on a possibly-``None`` ``maxdepth``. Rewritten to use
  ``fs.walk(...)`` with a None-safe ``sub_maxdepth``.
- ``ls()`` referenced ``fs.root_path`` which does not exist on
  ``AbstractFileSystem`` — it crashed with ``AttributeError`` as soon as
  it was called on a sub-filesystem. Replaced with the local ``root_path``
  variable returned by ``_get_filesystem()``.

Bug fixes (TransparentFileSystem)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- ``lexists()`` called ``self._get_filesystem(path)``, a method that only
  exists on ``NestedFileSystem`` (copy-paste error). It crashed with
  ``AttributeError`` on every call. Replaced with the existing
  ``__leading_fs(path)`` helper, matching the other read paths in the
  same class.

Tests
~~~~~

- Added 9 regression tests covering each of the bugs above. The
  ``django-fsspec2`` test suite is now 19 tests (up from 10), all green.
