import asyncio
import os
import unittest
from pathlib import Path

import fsspec
import s3fs  # noqa

import django_fsspec  # noqa


class VariabeleLoader:
    """Laadt een variabele uit een .env-bestand in omgevingsvariabelen."""

    def __init__(self, relative_file=".env"):
        self.filepath = os.path.join(os.path.dirname(__file__), relative_file)
        self.variables = None

    def _load_env(self):
        if self.variables:
            return
        self.variables = {}

        if not os.path.exists(self.filepath):
            return  # Als het bestand niet bestaat, doe niets

        with open(self.filepath, "r") as f:
            for line_f in f:
                line = line_f.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue  # Negeer lege regels, commentaar en ongeldige regels

                key, value = line.split("=", maxsplit=1)
                key = key.strip()
                value = (
                    value.strip().strip("'").strip('"')
                )  # Verwijder eventuele aanhalingstekens
                self.variables[key] = value

    def get(self, variabele):
        if os.environ.get(variabele):
            return os.environ.get(variabele)
        if not self.variables:
            self._load_env()
        return self.variables.get(variabele)


# Laad de variabelen uit het .env-bestand
env = VariabeleLoader()


async def async_test():
    await asyncio.sleep(0.1)
    return "Resultaat"


class TestWithS3(unittest.TestCase):
    """Test django-fsspec with S3.
    requires the following environment variables in .env file (in this test directory) or in the environment:
    # one endpoint with 2 buckets:
    - S3_TEST_ENDPOINT_URL
    - S3_TEST_ACCESS_KEY
    - S3_TEST_SECRET_KEY
    - S3_TEST_BUCKET_NAME
    - S3_TEST_BUCKET_NAME2
    # other s3 endpoint
    - S3_TEST_ENDPOINT_URL3
    - S3_TEST_ACCESS_KEY3
    - S3_TEST_SECRET_KEY3
    - S3_TEST_BUCKET_NAME3
    Test will be skipped if these are not set.
    """

    def setUp(self):
        storage_config = {
            "endpoint_url": env.get("S3_TEST_ENDPOINT_URL"),
            "key": env.get("S3_TEST_ACCESS_KEY"),
            "secret": env.get("S3_TEST_SECRET_KEY"),
            "bucket_name": env.get("S3_TEST_BUCKET_NAME"),
            "bucket_name2": env.get("S3_TEST_BUCKET_NAME2"),
            "endpoint_url3": env.get("S3_TEST_ENDPOINT_URL3"),
            "key3": env.get("S3_TEST_ACCESS_KEY3"),
            "secret3": env.get("S3_TEST_SECRET_KEY3"),
            "bucket_name3": env.get("S3_TEST_BUCKET_NAME3"),
        }

        if not all(storage_config.values()):
            self.skipTest("S3 credentials not set")

        test_data_dir = Path(Path(__file__).parent, "tmp")
        # root_fs1 = Path(test_data_dir, 'root_fs1')
        self.root_fs_default = Path(test_data_dir, "root_fs3")

        nested_mapping = {
            "a": {
                "protocol": "s3",
                "endpoint_url": env.get("S3_TEST_ENDPOINT_URL"),
                "key": env.get("S3_TEST_ACCESS_KEY"),
                "secret": env.get("S3_TEST_SECRET_KEY"),
                "relative_to_path": env.get("S3_TEST_BUCKET_NAME"),
            },
            "b": {
                "protocol": "s3",
                "endpoint_url": env.get("S3_TEST_ENDPOINT_URL"),
                "key": env.get("S3_TEST_ACCESS_KEY"),
                "secret": env.get("S3_TEST_SECRET_KEY"),
                "relative_to_path": env.get("S3_TEST_BUCKET_NAME2"),
            },
            "c": {
                "protocol": "s3",
                "endpoint_url": env.get("S3_TEST_ENDPOINT_URL3"),
                "key": env.get("S3_TEST_ACCESS_KEY3"),
                "secret": env.get("S3_TEST_SECRET_KEY3"),
                "relative_to_path": env.get("S3_TEST_BUCKET_NAME3"),
            },
            "default": {
                "protocol": "local",
                "auto_mkdir": True,
                "relative_to_path": self.root_fs_default,
            },
        }
        self.nested_fs = fsspec.filesystem(
            "nested", path_storage_configs=nested_mapping
        )

        import boto3

        self.boto12 = boto3.client(
            "s3",
            aws_access_key_id=env.get("S3_TEST_ACCESS_KEY"),
            aws_secret_access_key=env.get("S3_TEST_SECRET_KEY"),
            endpoint_url=env.get("S3_TEST_ENDPOINT_URL"),
        )
        self.boto3 = boto3.client(
            "s3",
            aws_access_key_id=env.get("S3_TEST_ACCESS_KEY3"),
            aws_secret_access_key=env.get("S3_TEST_SECRET_KEY3"),
            endpoint_url=env.get("S3_TEST_ENDPOINT_URL3"),
        )

    def tearDown(self):
        fs = self.nested_fs
        # fs.rm("a/sub", recursive=True)
        # fs.rm("b/sub", recursive=True)
        # fs.rm("c/sub", recursive=True)
        try:
            fs.rm("testdef.txt")
        except FileNotFoundError:
            pass

    def test_setup_with_s3(self):
        """
        :return:
        """
        # # test async
        # resultaat = await test_async()
        # self.assertEqual(resultaat, "Resultaat")

        bucket_name = env.get("S3_TEST_BUCKET_NAME")
        fs = fsspec.filesystem(
            protocol="s3",
            endpoint_url=env.get("S3_TEST_ENDPOINT_URL"),
            key=env.get("S3_TEST_ACCESS_KEY"),
            secret=env.get("S3_TEST_SECRET_KEY"),
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

        # fs.rm_file(f"{bucket_name}/test/test.txt")
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
        response = s3.list_objects_v2(Bucket=env.get("S3_TEST_BUCKET_NAME"))
        contents = response.get("Contents")
        self.assertTrue(
            [obj.get("Key") for obj in contents if "testa.txt" in obj.get("Key")]
        )

        response = s3.list_objects_v2(Bucket=env.get("S3_TEST_BUCKET_NAME2"))
        contents = response.get("Contents")
        self.assertTrue(
            [obj.get("Key") for obj in contents if "testb.txt" in obj.get("Key")]
        )
        self.assertFalse(
            [obj.get("Key") for obj in contents if "testa.txt" in obj.get("Key")]
        )

        s3 = self.boto3
        response = s3.list_objects_v2(Bucket=env.get("S3_TEST_BUCKET_NAME3"))
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
        response = s3.list_objects_v2(Bucket=env.get("S3_TEST_BUCKET_NAME"))
        contents = response.get("Contents")
        self.assertFalse(
            contents is not None
            and [obj.get("Key") for obj in contents if "testa.txt" in obj.get("Key")]
        )
        response = s3.list_objects_v2(Bucket=env.get("S3_TEST_BUCKET_NAME2"))
        contents = response.get("Contents")
        self.assertFalse(
            contents is not None
            and [obj.get("Key") for obj in contents if "testb.txt" in obj.get("Key")]
        )
        s3 = self.boto3
        response = s3.list_objects_v2(Bucket=env.get("S3_TEST_BUCKET_NAME3"))
        contents = response.get("Contents")
        self.assertFalse(
            contents is not None
            and [obj.get("Key") for obj in contents if "testc.txt" in obj.get("Key")]
        )
        self.assertFalse(Path(self.root_fs_default, "testdef.txt").exists())
