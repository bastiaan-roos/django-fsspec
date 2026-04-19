import os
import shutil

from pathlib import Path

import django
from django.core.files.base import ContentFile
from django.core.files.storage import storages
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.management import call_command

from django.test import TestCase, Client
from django.conf import settings
from django.urls import reverse

test_data_dir = Path(Path(__file__).parent, "tmp")
# Zorg ervoor dat Django settings zijn geconfigureerd

settings.configure(
    DEBUG=True,
    INSTALLED_APPS=[
        "test_app",
    ],
    ROOT_URLCONF="test_app.urls",
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": ":memory:",
        }
    },
    STORAGES={
        "default": {
            "BACKEND": "django_fsspec.FsspecStorage",
            "OPTIONS": {
                "storage_config": {
                    "protocol": "dir",
                    "path": test_data_dir,
                    "target_protocol": "local",
                    "target_options": {
                        "auto_mkdir": True,  # make directories if they do not exist
                    }
                },
            },
        }
    },
)

django.setup()
call_command("migrate", "--run-syncdb", verbosity=0)


# test as django app# from django.core.files.storage import Storage


class TestFsspecStorage(TestCase):
    # settings_module = 'django-fsspec.tests.test_site.project.settings'
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Initialize Django


    def setUp(self):
        os.makedirs(test_data_dir, exist_ok=True)
        # os.makedirs(test_data_dir / "test", exist_ok=True)
        # os.makedirs(test_data_dir / "extra", exist_ok=True)

    def tearDown(self):
        # Clean up test data directory after each test
        shutil.rmtree(test_data_dir, ignore_errors=True)

    def test_functions_part_one(self):

        storage = storages["default"]
        with storage.open("test_file.txt", "wb") as f:
            f.write(b"test content")
        # test with storage functions
        self.assertTrue(storage.exists("test_file.txt"))
        self.assertEqual(storage.size("test_file.txt"), 12)
        self.assertEqual(storage.open("test_file.txt").read(), b"test content")
        self.assertEqual([item.get('name') for item in storage.listdir("")], ["test_file.txt"])
        # test with original functions
        file_path = test_data_dir / "test_file.txt"
        self.assertTrue(file_path.exists())
        self.assertEqual(file_path.stat().st_size, 12)
        with open(file_path, "rb") as f:
            self.assertEqual(f.read(), b"test content")
        self.assertEqual(list(test_data_dir.iterdir()), [file_path])

        storage.delete("test_file.txt")
        self.assertFalse(storage.exists("test_file.txt"))
        self.assertFalse(file_path.exists())

    def test_functions_part_two(self):
        """ functions save and get_alternative_name """

        storage = storages["default"]
        # test save
        name = storage.save("test_file.txt", ContentFile(b"test content"))
        self.assertTrue(storage.exists(name))
        self.assertEqual(storage.size(name), 12)
        self.assertEqual(storage.open(name).read(), b"test content")
        # test get_alternative_name
        name2 = storage.save("test_file.txt", ContentFile(b"test content"))
        self.assertTrue(storage.exists(name2))
        self.assertFalse(name == name2)

    def test_file_field(self):

        django.setup()
        from test_app.models import FieldTestModel

        test_model = FieldTestModel()
        test_model.file.save("test_file.txt", ContentFile(b"test content"))
        # make empty image of 600x800 pixels
        image = b"\x00" * (600 * 800 * 3)
        test_model.extra_file.save("test_file_extra.md", ContentFile(image))
        test_model.save()

        self.assertTrue(test_model.file.storage.exists("test/test_file.txt"))
        self.assertTrue(test_model.file.storage.exists("extra/test_file_extra.md"))
        self.assertEqual(test_model.file.size, 12)
        self.assertEqual(test_model.extra_file.size, 600 * 800 * 3)
        self.assertEqual(test_model.file.read(), b"test content")

    def test_multipart_upload(self):
        client = Client()
        url = reverse("test_app:file_upload")

        # get filecontent of file of 10MB
        file_content = b"test content" * 1000000

        uploaded_file = SimpleUploadedFile(
            "test.txt",
            file_content,
            content_type="text/plain"
        )

        data = {
            "name": "test_file.txt",
            "file": uploaded_file,
        }
        response = client.post(url, data, format="multipart")
        self.assertEqual(response.status_code, 200)

