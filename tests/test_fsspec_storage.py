import os
import shutil
import warnings
from pathlib import Path

import django
from django.core.exceptions import ImproperlyConfigured
from django.core.files.base import ContentFile
from django.core.files.storage import storages
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import Client
from django.test import TestCase
from django.urls import reverse

# Django settings are configured in tests/conftest.py.
test_data_dir = Path(Path(__file__).parent, "tmp")


class TestFsspecStorage(TestCase):
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
        dirs, files = storage.listdir("")
        self.assertEqual(files, ["test_file.txt"])
        self.assertEqual(dirs, [])
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
        """functions save and get_alternative_name"""

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

        uploaded_file = SimpleUploadedFile("test.txt", file_content, content_type="text/plain")

        data = {
            "name": "test_file.txt",
            "file": uploaded_file,
        }
        response = client.post(url, data, format="multipart")
        self.assertEqual(response.status_code, 200)


class TestOptionsValidation(TestCase):
    """Validate FsspecStorage option handling and per-protocol checks."""

    def test_location_maps_to_relative_to_path_for_file_protocol(self):
        """OPTIONS['location'] is forwarded to storage_config['relative_to_path']
        for the local filesystem protocol, and a save lands under that path."""
        from django_fsspec import FsspecStorage

        loc = test_data_dir / "loc_via_option"
        os.makedirs(loc, exist_ok=True)
        try:
            storage = FsspecStorage(
                location=str(loc),
                storage_config={"protocol": "file", "auto_mkdir": True},
            )
            storage.save("hello.txt", ContentFile(b"hi"))
            self.assertTrue((loc / "hello.txt").exists())
        finally:
            shutil.rmtree(loc, ignore_errors=True)

    def test_location_with_non_file_protocol_raises(self):
        """OPTIONS['location'] + non-file protocol → ImproperlyConfigured."""
        from django_fsspec import FsspecStorage

        with self.assertRaises(ImproperlyConfigured) as ctx:
            FsspecStorage(
                location="/tmp/whatever",
                storage_config={
                    "protocol": "s3",
                    "key": "x",
                    "secret": "y",
                    "relative_to_path": "my-bucket",
                },
            )
        self.assertIn("location", str(ctx.exception))

    def test_location_and_relative_to_path_conflict_raises(self):
        """OPTIONS['location'] together with storage_config['relative_to_path']
        on the same level is ambiguous → ImproperlyConfigured."""
        from django_fsspec import FsspecStorage

        with self.assertRaises(ImproperlyConfigured):
            FsspecStorage(
                location="/tmp/a",
                storage_config={
                    "protocol": "file",
                    "auto_mkdir": True,
                    "relative_to_path": "/tmp/b",
                },
            )

    def test_file_permissions_mode_emits_deprecation_warning(self):
        """file_permissions_mode is accepted but unused → DeprecationWarning."""
        from django_fsspec import FsspecStorage

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            FsspecStorage(
                file_permissions_mode=0o644,
                storage_config={"protocol": "file", "auto_mkdir": True},
            )
        messages = [str(w.message) for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertTrue(any("file_permissions_mode" in m for m in messages), messages)

    def test_directory_permissions_mode_emits_deprecation_warning(self):
        from django_fsspec import FsspecStorage

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            FsspecStorage(
                directory_permissions_mode=0o755,
                storage_config={"protocol": "file", "auto_mkdir": True},
            )
        messages = [str(w.message) for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertTrue(any("directory_permissions_mode" in m for m in messages), messages)

    def test_s3_protocol_requires_relative_to_path(self):
        """protocol='s3' without relative_to_path → ImproperlyConfigured."""
        from django_fsspec import FsspecStorage

        with self.assertRaises(ImproperlyConfigured) as ctx:
            FsspecStorage(
                storage_config={
                    "protocol": "s3",
                    "key": "x",
                    "secret": "y",
                },
            )
        self.assertIn("relative_to_path", str(ctx.exception))

    def test_nested_protocol_requires_path_storage_configs(self):
        from django_fsspec import FsspecStorage

        with self.assertRaises(ImproperlyConfigured) as ctx:
            FsspecStorage(storage_config={"protocol": "nested"})
        self.assertIn("path_storage_configs", str(ctx.exception))

    def test_transparent_protocol_requires_both_layers(self):
        from django_fsspec import FsspecStorage

        with self.assertRaises(ImproperlyConfigured) as ctx:
            FsspecStorage(storage_config={"protocol": "transparent"})
        msg = str(ctx.exception)
        self.assertIn("transparent_fs", msg)
        self.assertIn("base_fs", msg)

    def test_nested_validates_sub_configs_recursively(self):
        """A nested config with an S3 sub-fs lacking relative_to_path also
        raises — validation is applied recursively via get_filesystem."""
        from django_fsspec import FsspecStorage

        with self.assertRaises(ImproperlyConfigured) as ctx:
            FsspecStorage(
                storage_config={
                    "protocol": "nested",
                    "path_storage_configs": {
                        "upload": {
                            "protocol": "s3",
                            "key": "x",
                            "secret": "y",
                            # no relative_to_path → must raise
                        },
                    },
                },
            )
        self.assertIn("relative_to_path", str(ctx.exception))


class TestPermissionsAndCollision(TestCase):
    """Verify permissions, on_collision, and the AND-merge with sub-fs perms."""

    def setUp(self):
        self.tmp = test_data_dir / "perm_tests"
        os.makedirs(self.tmp, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _flat_storage(self, **opts):
        from django_fsspec import FsspecStorage

        return FsspecStorage(
            storage_config={
                "protocol": "file",
                "auto_mkdir": True,
                "relative_to_path": str(self.tmp),
            },
            **opts,
        )

    # --- top-level permissions -------------------------------------------------

    def test_allow_read_false_blocks_open(self):
        storage = self._flat_storage(permissions={"allow_read": False})
        (self.tmp / "x.txt").write_bytes(b"hi")
        with self.assertRaises(PermissionError) as ctx:
            storage.open("x.txt")
        self.assertIn("read", str(ctx.exception))

    def test_allow_write_false_blocks_save(self):
        storage = self._flat_storage(permissions={"allow_write": False})
        with self.assertRaises(PermissionError) as ctx:
            storage.save("x.txt", ContentFile(b"hi"))
        self.assertIn("write", str(ctx.exception))

    def test_allow_delete_false_blocks_delete(self):
        storage = self._flat_storage(permissions={"allow_delete": False})
        (self.tmp / "x.txt").write_bytes(b"hi")
        with self.assertRaises(PermissionError) as ctx:
            storage.delete("x.txt")
        self.assertIn("delete", str(ctx.exception))

    def test_unknown_permission_key_raises(self):
        with self.assertRaises(ImproperlyConfigured):
            self._flat_storage(permissions={"allow_chown": False})

    # --- on_collision ----------------------------------------------------------

    def test_on_collision_overwrite_replaces_content(self):
        storage = self._flat_storage()  # default = overwrite
        storage.save("x.txt", ContentFile(b"v1"))
        name2 = storage.save("x.txt", ContentFile(b"v2"))
        self.assertEqual(name2, "x.txt")
        self.assertEqual(storage.open("x.txt").read(), b"v2")

    def test_on_collision_rename_keeps_both(self):
        storage = self._flat_storage(on_collision="rename")
        storage.save("x.txt", ContentFile(b"v1"))
        name2 = storage.save("x.txt", ContentFile(b"v2"))
        self.assertNotEqual(name2, "x.txt")
        self.assertEqual(storage.open("x.txt").read(), b"v1")
        self.assertEqual(storage.open(name2).read(), b"v2")

    def test_on_collision_raise_blocks_overwrite(self):
        storage = self._flat_storage(on_collision="raise")
        storage.save("x.txt", ContentFile(b"v1"))
        with self.assertRaises(PermissionError) as ctx:
            storage.save("x.txt", ContentFile(b"v2"))
        self.assertIn("already exists", str(ctx.exception))

    def test_invalid_on_collision_raises(self):
        with self.assertRaises(ImproperlyConfigured):
            self._flat_storage(on_collision="ignore")

    # --- backwards-compat for allow_overwrite ----------------------------------

    def test_allow_overwrite_true_emits_deprecation_and_maps_to_overwrite(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            storage = self._flat_storage(allow_overwrite=True)
        self.assertEqual(storage.on_collision, "overwrite")
        self.assertTrue(
            any(issubclass(w.category, DeprecationWarning) and "allow_overwrite" in str(w.message) for w in caught)
        )

    def test_allow_overwrite_false_maps_to_rename(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            storage = self._flat_storage(allow_overwrite=False)
        self.assertEqual(storage.on_collision, "rename")

    def test_allow_overwrite_and_on_collision_together_raises(self):
        with self.assertRaises(ImproperlyConfigured):
            self._flat_storage(allow_overwrite=True, on_collision="overwrite")

    # --- nested: per-sub-fs permissions and most-restrictive-wins -------------

    def _nested_storage(self, top_permissions=None, top_collision=None, sub_permissions=None, sub_collision=None):
        from django_fsspec import FsspecStorage

        sub_a_root = self.tmp / "sub_a"
        sub_def_root = self.tmp / "sub_def"
        os.makedirs(sub_a_root, exist_ok=True)
        os.makedirs(sub_def_root, exist_ok=True)

        sub_a_cfg = {
            "protocol": "file",
            "auto_mkdir": True,
            "relative_to_path": str(sub_a_root),
        }
        if sub_permissions is not None:
            sub_a_cfg["permissions"] = sub_permissions
        if sub_collision is not None:
            sub_a_cfg["on_collision"] = sub_collision

        opts = {
            "storage_config": {
                "protocol": "nested",
                "path_storage_configs": {
                    "a": sub_a_cfg,
                    "default": {
                        "protocol": "file",
                        "auto_mkdir": True,
                        "relative_to_path": str(sub_def_root),
                    },
                },
            },
        }
        if top_permissions is not None:
            opts["permissions"] = top_permissions
        if top_collision is not None:
            opts["on_collision"] = top_collision
        return FsspecStorage(**opts)

    def test_nested_sub_permission_blocks_only_that_prefix(self):
        storage = self._nested_storage(sub_permissions={"allow_write": False})
        # Sub-fs 'a' is read-only.
        with self.assertRaises(PermissionError):
            storage.save("a/x.txt", ContentFile(b"hi"))
        # Default sub-fs is unaffected.
        name = storage.save("free.txt", ContentFile(b"hi"))
        self.assertEqual(name, "free.txt")

    def test_nested_top_permission_blocks_all(self):
        storage = self._nested_storage(top_permissions={"allow_write": False})
        with self.assertRaises(PermissionError):
            storage.save("a/x.txt", ContentFile(b"hi"))
        with self.assertRaises(PermissionError):
            storage.save("free.txt", ContentFile(b"hi"))

    def test_nested_most_restrictive_wins_for_collision(self):
        # Top says overwrite, sub says raise → effective = raise.
        storage = self._nested_storage(top_collision="overwrite", sub_collision="raise")
        storage.save("a/x.txt", ContentFile(b"v1"))
        with self.assertRaises(PermissionError) as ctx:
            storage.save("a/x.txt", ContentFile(b"v2"))
        self.assertIn("already exists", str(ctx.exception))
        # Default sub-fs inherits top's overwrite.
        storage.save("y.txt", ContentFile(b"v1"))
        storage.save("y.txt", ContentFile(b"v2"))  # no raise
        self.assertEqual(storage.open("y.txt").read(), b"v2")

    def test_nested_unknown_sub_permission_key_raises(self):
        with self.assertRaises(ImproperlyConfigured):
            self._nested_storage(sub_permissions={"allow_chown": False})

    def test_nested_most_restrictive_wins_overwrite_vs_rename(self):
        """top=overwrite, sub=rename -> effective=rename for that sub-fs."""
        storage = self._nested_storage(top_collision="overwrite", sub_collision="rename")
        storage.save("a/x.txt", ContentFile(b"v1"))
        # Sub-fs uses 'rename' on collision -> Django's get_available_name
        # finds an alternative name; both files coexist.
        name2 = storage.save("a/x.txt", ContentFile(b"v2"))
        self.assertNotEqual(name2, "a/x.txt")
        self.assertTrue(storage.exists("a/x.txt"))
        self.assertTrue(storage.exists(name2))


class TestStorageContractDetails(TestCase):
    """Direct unit tests for small contract surfaces (path/name/availability)."""

    def setUp(self):
        self.tmp = test_data_dir / "contract_tests"
        os.makedirs(self.tmp, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _storage(self, **opts):
        from django_fsspec import FsspecStorage

        return FsspecStorage(
            storage_config={
                "protocol": "file",
                "auto_mkdir": True,
                "relative_to_path": str(self.tmp),
            },
            **opts,
        )

    def test_path_raises_not_implemented(self):
        """`storage.path(name)` is contractually invalid for non-local backends."""
        storage = self._storage()
        with self.assertRaises(NotImplementedError):
            storage.path("anything.txt")

    def test_get_alternative_name_appends_random_suffix(self):
        storage = self._storage()
        alt = storage.get_alternative_name("foo", ".txt")
        self.assertTrue(alt.startswith("foo_"))
        self.assertTrue(alt.endswith(".txt"))
        # 7 random alphanumeric chars between "foo_" and ".txt"
        suffix_part = alt[len("foo_") : -len(".txt")]
        self.assertEqual(len(suffix_part), 7)
        self.assertTrue(suffix_part.isalnum())

    def test_is_name_available_overwrite_returns_true_even_when_exists(self):
        storage = self._storage(on_collision="overwrite")
        (self.tmp / "x.txt").write_bytes(b"hi")
        self.assertTrue(storage.is_name_available("x.txt"))

    def test_is_name_available_raise_returns_true_so_save_can_decide(self):
        storage = self._storage(on_collision="raise")
        (self.tmp / "x.txt").write_bytes(b"hi")
        self.assertTrue(storage.is_name_available("x.txt"))

    def test_is_name_available_rename_uses_django_default(self):
        storage = self._storage(on_collision="rename")
        (self.tmp / "x.txt").write_bytes(b"hi")
        # Existing name is not available -> rename loop kicks in.
        self.assertFalse(storage.is_name_available("x.txt"))
        # Free name remains available.
        self.assertTrue(storage.is_name_available("y.txt"))

    def test_is_name_available_max_length_enforced_for_overwrite(self):
        """Even when on_collision short-circuits the existence check, names
        that exceed `max_length` must still be reported unavailable so
        Django's save flow can truncate."""
        storage = self._storage(on_collision="overwrite")
        long_name = "a" * 50
        self.assertTrue(storage.is_name_available(long_name, max_length=100))
        self.assertFalse(storage.is_name_available(long_name, max_length=10))

    def test_allow_overwrite_is_derived_from_on_collision(self):
        """`storage.allow_overwrite` is read-only and derived from on_collision."""
        self.assertTrue(self._storage(on_collision="overwrite").allow_overwrite)
        self.assertTrue(self._storage(on_collision="raise").allow_overwrite)
        self.assertFalse(self._storage(on_collision="rename").allow_overwrite)


class TestUrlSignedCollisionPreCheck(TestCase):
    """Verify the on_collision pre-check on `url_signed(method='PUT')` fires
    before the S3 plumbing — so it can be tested without real S3 credentials.
    """

    def setUp(self):
        self.tmp = test_data_dir / "url_signed_tests"
        os.makedirs(self.tmp, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _storage(self, **opts):
        from django_fsspec import FsspecStorage

        return FsspecStorage(
            storage_config={
                "protocol": "file",
                "auto_mkdir": True,
                "relative_to_path": str(self.tmp),
            },
            **opts,
        )

    def test_put_with_on_collision_raise_blocks_when_target_exists(self):
        storage = self._storage(on_collision="raise")
        (self.tmp / "x.txt").write_bytes(b"hi")
        with self.assertRaises(PermissionError) as ctx:
            storage.url_signed("x.txt", method="PUT")
        msg = str(ctx.exception)
        self.assertIn("on_collision", msg)
        self.assertIn("'raise'", msg)

    def test_put_with_on_collision_rename_blocks_when_target_exists(self):
        """`rename` cannot be honored over presigned URLs (the URL is opaque)."""
        storage = self._storage(on_collision="rename")
        (self.tmp / "x.txt").write_bytes(b"hi")
        with self.assertRaises(PermissionError):
            storage.url_signed("x.txt", method="PUT")

    def test_put_allow_write_false_blocks_before_collision_check(self):
        storage = self._storage(permissions={"allow_write": False})
        with self.assertRaises(PermissionError) as ctx:
            storage.url_signed("anything.txt", method="PUT")
        self.assertIn("write", str(ctx.exception))


class TestVerifyChecksum(TestCase):
    """Unit-test the verify_checksum path without needing real S3."""

    def setUp(self):
        self.tmp = test_data_dir / "checksum_tests"
        os.makedirs(self.tmp, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _storage(self, **opts):
        from django_fsspec import FsspecStorage

        return FsspecStorage(
            verify_checksum=True,
            storage_config={
                "protocol": "file",
                "auto_mkdir": True,
                "relative_to_path": str(self.tmp),
            },
            **opts,
        )

    def test_save_without_source_checksum_is_noop(self):
        storage = self._storage()
        storage.save("x.txt", ContentFile(b"hi"))
        self.assertTrue(storage.exists("x.txt"))

    def test_save_with_matching_checksum_succeeds(self):
        storage = self._storage()
        # Force the underlying fs to report a known checksum.
        storage.filesystem.checksum = lambda name: "abc123"
        content = ContentFile(b"hi")
        content.checksum = "abc123"
        storage._save("x.txt", content)
        self.assertTrue(storage.exists("x.txt"))

    def test_save_with_mismatched_checksum_raises_and_cleans_up(self):
        storage = self._storage()
        storage.filesystem.checksum = lambda name: "actually-stored"
        content = ContentFile(b"hi")
        content.checksum = "expected-but-wrong"
        with self.assertRaises(IOError) as ctx:
            storage._save("x.txt", content)
        self.assertIn("Checksum mismatch", str(ctx.exception))
        # Cleanup: object must not be left behind.
        self.assertFalse(storage.exists("x.txt"))


class TestDeleteIdempotency(TestCase):
    """`storage.delete(name)` must align with Django's ``FileSystemStorage``
    contract: deleting a missing file is silent, not an error."""

    def setUp(self):
        self.tmp = test_data_dir / "delete_tests"
        os.makedirs(self.tmp, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _storage(self, **opts):
        from django_fsspec import FsspecStorage

        return FsspecStorage(
            storage_config={
                "protocol": "file",
                "auto_mkdir": True,
                "relative_to_path": str(self.tmp),
            },
            **opts,
        )

    def test_delete_missing_file_is_silent(self):
        storage = self._storage()
        # Must not raise; mirrors FileSystemStorage.delete behavior.
        self.assertIsNone(storage.delete("never_existed.txt"))

    def test_delete_existing_file_then_again_is_silent(self):
        storage = self._storage()
        storage.save("x.txt", ContentFile(b"hi"))
        storage.delete("x.txt")
        self.assertFalse(storage.exists("x.txt"))
        # Second delete must not raise.
        storage.delete("x.txt")

    def test_delete_still_honors_allow_delete(self):
        storage = self._storage(permissions={"allow_delete": False})
        with self.assertRaises(PermissionError):
            storage.delete("anything.txt")

    def test_delete_routes_dir_vs_file_via_isdir(self):
        """``delete()`` consults ``isdir()`` to decide whether to pass
        ``recursive=True`` to ``rm()``. Required for s3fs, which refuses
        to remove a "directory" (S3 prefix) without recursion.
        """
        from unittest.mock import MagicMock

        storage = self._storage()
        # Vervang de echte filesystem door een mock zodat we de calls kunnen tracken.
        fake_fs = MagicMock()
        storage.filesystem = fake_fs

        # File path (isdir=False) → rm zonder recursive.
        fake_fs.isdir.return_value = False
        storage.delete("ordinary.txt")
        fake_fs.rm.assert_called_once_with("ordinary.txt")

        fake_fs.reset_mock()

        # Directory path (isdir=True) → rm met recursive=True.
        fake_fs.isdir.return_value = True
        storage.delete("some/dir")
        fake_fs.rm.assert_called_once_with("some/dir", recursive=True)
