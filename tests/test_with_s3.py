import os
import time
import unittest
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import fsspec
import s3fs  # noqa: F401

import django_fsspec  # noqa: F401

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

# Laad .env uit de test-directory (optioneel) zodat lokaal ontwikkelen
# met een tests/.env bestand werkt. In CI mogen env-vars direct gezet zijn.
_ENV_PATH = Path(__file__).parent / ".env"
if load_dotenv is not None and _ENV_PATH.exists():
    load_dotenv(_ENV_PATH, override=False)


def _env(name: str) -> str | None:
    """Return env var `name` or None (no defaults — never hardcode creds)."""
    return os.environ.get(name)


class TestWithS3(unittest.TestCase):
    """Integration tests for django-fsspec against real S3-compatible storage.

    Requires the following environment variables (e.g. via a ``tests/.env``
    file that is *not* committed, or the CI secret store):

    - ``S3_TEST_ENDPOINT_URL``, ``S3_TEST_ACCESS_KEY``, ``S3_TEST_SECRET_KEY``
    - ``S3_TEST_BUCKET_NAME``, ``S3_TEST_BUCKET_NAME2``
    - ``S3_TEST_ENDPOINT_URL3``, ``S3_TEST_ACCESS_KEY3``,
      ``S3_TEST_SECRET_KEY3``, ``S3_TEST_BUCKET_NAME3``

    The test is skipped when any of those are unset.
    """

    def setUp(self):
        storage_config = {
            "endpoint_url": _env("S3_TEST_ENDPOINT_URL"),
            "key": _env("S3_TEST_ACCESS_KEY"),
            "secret": _env("S3_TEST_SECRET_KEY"),
            "bucket_name": _env("S3_TEST_BUCKET_NAME"),
            "bucket_name2": _env("S3_TEST_BUCKET_NAME2"),
            "endpoint_url3": _env("S3_TEST_ENDPOINT_URL3"),
            "key3": _env("S3_TEST_ACCESS_KEY3"),
            "secret3": _env("S3_TEST_SECRET_KEY3"),
            "bucket_name3": _env("S3_TEST_BUCKET_NAME3"),
        }

        if not all(storage_config.values()):
            self.skipTest("S3 credentials not set")

        test_data_dir = Path(Path(__file__).parent, "tmp")
        self.root_fs_default = Path(test_data_dir, "root_fs3")

        self.nested_mapping = {
            "a": {
                "protocol": "s3",
                "endpoint_url": _env("S3_TEST_ENDPOINT_URL"),
                "key": _env("S3_TEST_ACCESS_KEY"),
                "secret": _env("S3_TEST_SECRET_KEY"),
                "relative_to_path": _env("S3_TEST_BUCKET_NAME"),
            },
            "b": {
                "protocol": "s3",
                "endpoint_url": _env("S3_TEST_ENDPOINT_URL"),
                "key": _env("S3_TEST_ACCESS_KEY"),
                "secret": _env("S3_TEST_SECRET_KEY"),
                "relative_to_path": _env("S3_TEST_BUCKET_NAME2"),
            },
            "c": {
                "protocol": "s3",
                "endpoint_url": _env("S3_TEST_ENDPOINT_URL3"),
                "key": _env("S3_TEST_ACCESS_KEY3"),
                "secret": _env("S3_TEST_SECRET_KEY3"),
                "relative_to_path": _env("S3_TEST_BUCKET_NAME3"),
            },
            "default": {
                "protocol": "local",
                "auto_mkdir": True,
                "relative_to_path": self.root_fs_default,
            },
        }
        self.nested_fs = fsspec.filesystem(
            "nested", path_storage_configs=self.nested_mapping
        )

        import boto3

        self.boto12 = boto3.client(
            "s3",
            aws_access_key_id=_env("S3_TEST_ACCESS_KEY"),
            aws_secret_access_key=_env("S3_TEST_SECRET_KEY"),
            endpoint_url=_env("S3_TEST_ENDPOINT_URL"),
        )
        self.boto3 = boto3.client(
            "s3",
            aws_access_key_id=_env("S3_TEST_ACCESS_KEY3"),
            aws_secret_access_key=_env("S3_TEST_SECRET_KEY3"),
            endpoint_url=_env("S3_TEST_ENDPOINT_URL3"),
        )

    def tearDown(self):
        fs = self.nested_fs
        try:
            fs.rm("testdef.txt")
        except FileNotFoundError:
            pass

    def test_setup_with_s3(self):
        bucket_name = _env("S3_TEST_BUCKET_NAME")
        fs = fsspec.filesystem(
            protocol="s3",
            endpoint_url=_env("S3_TEST_ENDPOINT_URL"),
            key=_env("S3_TEST_ACCESS_KEY"),
            secret=_env("S3_TEST_SECRET_KEY"),
            client_kwargs={"region_name": bucket_name},
        )

        exists = fs.exists(f"{bucket_name}/test/test.txt")
        if exists:
            fs.rm_file(f"{bucket_name}/test/test.txt")
        exists = fs.exists(f"{bucket_name}/test/test.txt")
        self.assertFalse(exists)

        # test put file, read file, delete file
        content = "Hello, World!"
        with fs.open(f"{bucket_name}/test/test.txt", "wb") as f:
            f.write(bytes(content, "utf-8"))
        self.assertTrue(fs.exists(f"s3://{bucket_name}/test/test.txt"))
        with fs.open(f"{bucket_name}/test/test.txt", "r") as f:
            self.assertEqual(f.read(), content)

        items = fs.ls(f"{bucket_name}/test")
        self.assertEqual(len(items), 1)
        fs.expand_path(f"{bucket_name}/test/test.txt")
        fs.rm(f"{bucket_name}/test/", recursive=True)

        exists = fs.exists(f"{bucket_name}/test/test.txt")
        self.assertFalse(exists)

    def test_nested_write_read_delete(self):  # noqa: PLR0915
        fs = self.nested_fs

        with fs.open("a/sub/testa.txt", "w") as f:
            f.write("Hello, a")
        with fs.open("b/sub/testb.txt", "w") as f:
            f.write("Hello, b")
        with fs.open("c/sub/testc.txt", "w") as f:
            f.write("Hello, c")
        with fs.open("testdef.txt", "w") as f:
            f.write("Hello, def")

        self.assertTrue(fs.exists("a/sub/testa.txt"))
        self.assertTrue(fs.exists("b/sub/testb.txt"))
        self.assertTrue(fs.exists("c/sub/testc.txt"))
        self.assertTrue(fs.exists("testdef.txt"))
        # check if file exists with boto
        s3 = self.boto12
        response = s3.list_objects_v2(Bucket=_env("S3_TEST_BUCKET_NAME"))
        contents = response.get("Contents")
        self.assertTrue(
            [obj.get("Key") for obj in contents if "testa.txt" in obj.get("Key")]
        )

        response = s3.list_objects_v2(Bucket=_env("S3_TEST_BUCKET_NAME2"))
        contents = response.get("Contents")
        self.assertTrue(
            [obj.get("Key") for obj in contents if "testb.txt" in obj.get("Key")]
        )
        self.assertFalse(
            [obj.get("Key") for obj in contents if "testa.txt" in obj.get("Key")]
        )

        s3 = self.boto3
        response = s3.list_objects_v2(Bucket=_env("S3_TEST_BUCKET_NAME3"))
        contents = response.get("Contents")
        self.assertTrue(
            [obj.get("Key") for obj in contents if "testc.txt" in obj.get("Key")]
        )
        self.assertFalse(
            [obj.get("Key") for obj in contents if "testa.txt" in obj.get("Key")]
        )

        os.path.exists("testdef.txt")
        self.assertTrue(Path(self.root_fs_default, "testdef.txt").exists())

        # test read
        with fs.open("a/sub/testa.txt", "r") as f:
            self.assertEqual(f.read(), "Hello, a")
        with fs.open("b/sub/testb.txt", "r") as f:
            self.assertEqual(f.read(), "Hello, b")
        with fs.open("c/sub/testc.txt", "r") as f:
            self.assertEqual(f.read(), "Hello, c")
        with fs.open("testdef.txt", "r") as f:
            self.assertEqual(f.read(), "Hello, def")

        # test delete
        fs.rm("a/sub/", recursive=True)
        fs.rm("b/sub/", recursive=True)
        fs.rm("c/sub/", recursive=True)
        fs.rm("testdef.txt")

        self.assertFalse(fs.exists("a/sub/testa.txt"))
        self.assertFalse(fs.exists("b/sub/testb.txt"))
        self.assertFalse(fs.exists("c/sub/testc.txt"))
        self.assertFalse(fs.exists("testdef.txt"))
        # also check with boto
        s3 = self.boto12
        response = s3.list_objects_v2(Bucket=_env("S3_TEST_BUCKET_NAME"))
        contents = response.get("Contents")
        self.assertFalse(
            contents is not None
            and [obj.get("Key") for obj in contents if "testa.txt" in obj.get("Key")]
        )
        response = s3.list_objects_v2(Bucket=_env("S3_TEST_BUCKET_NAME2"))
        contents = response.get("Contents")
        self.assertFalse(
            contents is not None
            and [obj.get("Key") for obj in contents if "testb.txt" in obj.get("Key")]
        )
        s3 = self.boto3
        response = s3.list_objects_v2(Bucket=_env("S3_TEST_BUCKET_NAME3"))
        contents = response.get("Contents")
        self.assertFalse(
            contents is not None
            and [obj.get("Key") for obj in contents if "testc.txt" in obj.get("Key")]
        )
        self.assertFalse(Path(self.root_fs_default, "testdef.txt").exists())


class TestPresignedUrls(unittest.TestCase):
    """Integration tests for ``FsspecStorage.url_signed`` and ``url_direct``."""

    def setUp(self):
        required = [
            "S3_TEST_ENDPOINT_URL",
            "S3_TEST_ACCESS_KEY",
            "S3_TEST_SECRET_KEY",
            "S3_TEST_BUCKET_NAME",
            "S3_TEST_BUCKET_NAME2",
        ]
        if not all(_env(n) for n in required):
            self.skipTest("S3 credentials not set")

        from django_fsspec import FsspecStorage

        self.nested_config = {
            "protocol": "nested",
            "path_storage_configs": {
                "upload": {
                    "protocol": "s3",
                    "endpoint_url": _env("S3_TEST_ENDPOINT_URL"),
                    "key": _env("S3_TEST_ACCESS_KEY"),
                    "secret": _env("S3_TEST_SECRET_KEY"),
                    "relative_to_path": _env("S3_TEST_BUCKET_NAME"),
                },
                "video": {
                    "protocol": "s3",
                    "endpoint_url": _env("S3_TEST_ENDPOINT_URL"),
                    "key": _env("S3_TEST_ACCESS_KEY"),
                    "secret": _env("S3_TEST_SECRET_KEY"),
                    "relative_to_path": _env("S3_TEST_BUCKET_NAME2"),
                },
                "default": {
                    "protocol": "local",
                    "auto_mkdir": True,
                    "relative_to_path": str(Path(__file__).parent / "tmp_signed"),
                },
            },
        }
        self.storage = FsspecStorage(storage_config=self.nested_config)
        self._tmp_names = []

    def tearDown(self):
        for name in self._tmp_names:
            try:
                self.storage.delete(name)
            except FileNotFoundError:
                pass

    def _put(self, name, content: bytes):
        from django.core.files.base import ContentFile

        self.storage.save(name, ContentFile(content))
        self._tmp_names.append(name)

    def test_resolve_s3_target_nested(self):
        """resolve_s3_target should unwrap DirFileSystem → S3FileSystem + bucket."""
        import s3fs as _s3fs

        s3_fs, bucket, key = self.storage.filesystem.resolve_s3_target(
            "video/foo.mp4"
        )
        self.assertIsInstance(s3_fs, _s3fs.S3FileSystem)
        self.assertEqual(bucket, _env("S3_TEST_BUCKET_NAME2"))
        self.assertEqual(key, "foo.mp4")

    def test_resolve_s3_target_local_default_raises(self):
        """Local default fallback cannot be signed — must raise NotImplementedError."""
        with self.assertRaises(NotImplementedError):
            self.storage.filesystem.resolve_s3_target("unknownprefix/foo.txt")

    def test_presigned_get_roundtrip(self):
        """Write → url_signed(GET) → HTTP GET → bytes match."""
        name = "upload/signed_roundtrip.bin"
        payload = b"Hello from presigned URL!"
        self._put(name, payload)

        url = self.storage.url_signed(name, expires=60)
        self.assertIn("Signature=", url)

        with urlopen(url) as resp:  # noqa: S310 — presigned, trusted
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.read(), payload)

    def test_presigned_get_response_headers(self):
        """response_headers should propagate to the HTTP response."""
        name = "upload/signed_with_headers.bin"
        self._put(name, b"contents")

        url = self.storage.url_signed(
            name,
            expires=60,
            response_headers={
                "ResponseContentDisposition": 'attachment; filename="custom.bin"',
                "ResponseContentType": "application/x-drainworks-test",
            },
        )
        with urlopen(url) as resp:  # noqa: S310
            self.assertEqual(resp.status, 200)
            disp = resp.headers.get("Content-Disposition", "")
            self.assertIn("custom.bin", disp)
            self.assertEqual(
                resp.headers.get("Content-Type"),
                "application/x-drainworks-test",
            )

    def test_presigned_put_upload(self):
        """url_signed(method='PUT') allows upload via plain HTTP PUT."""
        name = "upload/signed_put.bin"
        payload = b"uploaded via presigned PUT"

        url = self.storage.url_signed(name, expires=60, method="PUT")
        req = Request(url, data=payload, method="PUT")  # noqa: S310
        with urlopen(req) as resp:  # noqa: S310
            self.assertIn(resp.status, (200, 204))

        # Register for teardown regardless of assertion outcome below.
        self._tmp_names.append(name)
        self.assertTrue(self.storage.exists(name))
        self.assertEqual(self.storage.size(name), len(payload))

    def test_presigned_put_with_response_headers_raises(self):
        """response_headers is a GET-only concept."""
        with self.assertRaises(ValueError):
            self.storage.url_signed(
                "upload/x.bin",
                method="PUT",
                response_headers={"ResponseContentType": "text/plain"},
            )

    def test_presigned_invalid_method_raises(self):
        with self.assertRaises(ValueError):
            self.storage.url_signed("upload/x.bin", method="DELETE")

    def test_presigned_expiry(self):
        """URL issued with expires=2 should 403 after the TTL passes."""
        name = "upload/expiring.bin"
        self._put(name, b"goes stale")

        url = self.storage.url_signed(name, expires=2)
        time.sleep(3)
        with self.assertRaises(HTTPError) as ctx:
            urlopen(url)  # noqa: S310
        self.assertEqual(ctx.exception.code, 403)


class TestChecksumVerification(unittest.TestCase):
    """Test the optional verify_checksum path on FsspecStorage._save."""

    def setUp(self):
        required = [
            "S3_TEST_ENDPOINT_URL",
            "S3_TEST_ACCESS_KEY",
            "S3_TEST_SECRET_KEY",
            "S3_TEST_BUCKET_NAME",
        ]
        if not all(_env(n) for n in required):
            self.skipTest("S3 credentials not set")

        from django_fsspec import FsspecStorage

        self.storage = FsspecStorage(
            verify_checksum=True,
            storage_config={
                "protocol": "s3",
                "endpoint_url": _env("S3_TEST_ENDPOINT_URL"),
                "key": _env("S3_TEST_ACCESS_KEY"),
                "secret": _env("S3_TEST_SECRET_KEY"),
                "relative_to_path": _env("S3_TEST_BUCKET_NAME"),
            },
        )
        self._tmp_names = []

    def tearDown(self):
        for name in self._tmp_names:
            try:
                self.storage.delete(name)
            except FileNotFoundError:
                pass

    def test_save_without_source_checksum_is_noop(self):
        """verify_checksum=True + no content.checksum → save succeeds, no raise."""
        from django.core.files.base import ContentFile

        name = "test/no_checksum.bin"
        self.storage.save(name, ContentFile(b"payload"))
        self._tmp_names.append(name)
        self.assertTrue(self.storage.exists(name))

    def test_save_with_mismatched_checksum_raises_and_cleans_up(self):
        """Deliberate checksum mismatch → IOError and object removed."""
        content = BytesIO(b"payload")
        content.checksum = "obviously-wrong-checksum"
        # Django calls chunks() on UploadedFile but falls back to .read()
        # on BytesIO; _save uses the 4 MB block loop path.

        name = "test/bad_checksum.bin"
        with self.assertRaises(IOError):
            self.storage._save(name, content)
        self.assertFalse(self.storage.exists(name))
