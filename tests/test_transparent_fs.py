import shutil
import unittest
from pathlib import Path

import fsspec

from django_fsspec.transparent_fs import TransparentFileSystem

test_data_dir = Path(Path(__file__).parent, "tmp")
tmp_get_dir = Path(test_data_dir, "..", "tmp_get").resolve()

root_base_fs = Path(test_data_dir, "root_base_fs")
root_transparent_fs = Path(test_data_dir, "root_transparent_fs")


class TestTransparentFS(unittest.TestCase):
    def setUp(self):
        # Ensure the test directories exist so DirFileSystem accepts them.
        root_base_fs.mkdir(parents=True, exist_ok=True)
        root_transparent_fs.mkdir(parents=True, exist_ok=True)
        self.fs = TransparentFileSystem(
            base_fs={
                "protocol": "file",
                "auto_mkdir": True,
                "relative_to_path": root_base_fs,
            },
            transparent_fs=fsspec.filesystem(
                protocol="dir",
                target_protocol="file",
                target_options={"auto_mkdir": True},
                path=root_transparent_fs,
            ),
        )

    def tearDown(self):
        shutil.rmtree(test_data_dir, ignore_errors=True)
        shutil.rmtree(tmp_get_dir, ignore_errors=True)

    def test_get_filesystem(self):
        base_fs = self.fs.base_fs
        self.assertEqual("dir", base_fs.protocol)

        transparent_fs = self.fs.transparent_fs
        self.assertEqual("dir", transparent_fs.protocol)

    def test_lexists_does_not_crash(self):
        """Regression: lexists() previously called `self._get_filesystem(path)`,
        which is a NestedFileSystem method that does not exist on
        TransparentFileSystem. That copy-paste bug crashed on AttributeError
        the moment lexists() was called.
        """
        # For a non-existing path: must return False, not crash.
        self.assertFalse(self.fs.lexists("does/not/exist.txt"))

        # Write a file and verify that lexists() returns True.
        with self.fs.open("present.txt", "w") as f:
            f.write("hi")
        self.assertTrue(self.fs.lexists("present.txt"))

    # --- ls -------------------------------------------------------------------

    def _seed(self):
        """Plant a file in each layer; return their bytes for assertions."""
        Path(root_base_fs, "b.txt").write_bytes(b"base")
        Path(root_transparent_fs, "t.txt").write_bytes(b"trans")
        return b"base", b"trans"

    def test_ls_detail_true_merges_both_layers(self):
        self._seed()
        names = sorted(e["name"] for e in self.fs.ls("", detail=True))
        self.assertEqual(names, ["b.txt", "t.txt"])
        for entry in self.fs.ls("", detail=True):
            self.assertEqual(entry["type"], "file")

    def test_ls_detail_false_returns_strings(self):
        self._seed()
        result = sorted(self.fs.ls("", detail=False))
        self.assertEqual(result, ["b.txt", "t.txt"])

    def test_ls_overlay_overrides_base(self):
        Path(root_base_fs, "shared.txt").write_bytes(b"base-version")
        Path(root_transparent_fs, "shared.txt").write_bytes(b"overlay-version")
        result = sorted(self.fs.ls("", detail=False))
        self.assertEqual(result, ["shared.txt"])
        self.assertEqual(self.fs.cat_file("shared.txt"), b"overlay-version")

    def test_ls_hides_deleted_tombstones(self):
        Path(root_base_fs, "gone.txt").write_bytes(b"x")
        Path(root_transparent_fs, "gone.txt.deleted").touch()
        names = list(self.fs.ls("", detail=False))
        self.assertEqual(names, [])

    def test_ls_does_not_emit_marker_files(self):
        Path(root_base_fs, "vanished").write_bytes(b"")
        Path(root_transparent_fs, "vanished.deleted").touch()
        Path(root_transparent_fs, "redone.replaced").mkdir()
        names = sorted(self.fs.ls("", detail=False))
        # Tombstones for "vanished" hide it; "redone" was replaced but has no
        # actual content yet so neither name should appear in the listing.
        self.assertEqual(names, [])

    # --- rm -------------------------------------------------------------------

    def test_rm_overlay_only_file(self):
        Path(root_transparent_fs, "x.txt").write_bytes(b"hi")
        self.fs.rm("x.txt")
        self.assertFalse(self.fs.exists("x.txt"))
        self.assertFalse(Path(root_transparent_fs, "x.txt").exists())

    def test_rm_base_only_file_leaves_tombstone(self):
        Path(root_base_fs, "x.txt").write_bytes(b"hi")
        self.fs.rm("x.txt")
        self.assertFalse(self.fs.exists("x.txt"))
        # Base file is left untouched (read-only contract); tombstone records
        # the deletion in the overlay.
        self.assertTrue(Path(root_base_fs, "x.txt").exists())
        self.assertTrue(Path(root_transparent_fs, "x.txt.deleted").exists())

    def test_rm_both_layers_records_tombstone(self):
        Path(root_base_fs, "x.txt").write_bytes(b"base")
        Path(root_transparent_fs, "x.txt").write_bytes(b"overlay")
        self.fs.rm("x.txt")
        self.assertFalse(self.fs.exists("x.txt"))
        self.assertFalse(Path(root_transparent_fs, "x.txt").exists())
        self.assertTrue(Path(root_transparent_fs, "x.txt.deleted").exists())

    def test_rm_missing_raises_filenotfound(self):
        with self.assertRaises(FileNotFoundError):
            self.fs.rm("does/not/exist.txt")

    def test_rm_non_empty_dir_without_recursive_raises(self):
        Path(root_base_fs, "subdir").mkdir()
        Path(root_base_fs, "subdir", "child.txt").write_bytes(b"x")
        with self.assertRaises(OSError):
            self.fs.rm("subdir")

    # --- write/read paths -----------------------------------------------------

    def test_open_write_lands_on_overlay_only(self):
        with self.fs.open("a.txt", "w") as f:
            f.write("hi")
        self.assertTrue(Path(root_transparent_fs, "a.txt").exists())
        self.assertFalse(Path(root_base_fs, "a.txt").exists())
        self.assertEqual(self.fs.cat_file("a.txt"), b"hi")

    def test_read_falls_through_to_base(self):
        Path(root_base_fs, "b.txt").write_bytes(b"from-base")
        self.assertEqual(self.fs.cat_file("b.txt"), b"from-base")

    def test_size_uses_active_layer(self):
        Path(root_base_fs, "b.txt").write_bytes(b"base")
        Path(root_transparent_fs, "b.txt").write_bytes(b"overlay-longer")
        self.assertEqual(self.fs.size("b.txt"), len(b"overlay-longer"))

    # --- contract edge cases --------------------------------------------------

    def test_ls_missing_path_raises(self):
        """fsspec contract: `ls` raises FileNotFoundError when neither
        layer has the path. Previously the missing-path errors were
        silently swallowed and an empty list was returned."""
        with self.assertRaises(FileNotFoundError):
            self.fs.ls("does/not/exist")

    def test_ls_succeeds_when_only_one_layer_has_the_path(self):
        Path(root_base_fs, "subdir").mkdir()
        Path(root_base_fs, "subdir", "b.txt").write_bytes(b"x")
        # Overlay does not have `subdir/`; ls must still return base content.
        result = sorted(self.fs.ls("subdir", detail=False))
        self.assertEqual(result, ["subdir/b.txt"])

    def test_rm_directory_when_overlay_holds_only_tombstones(self):
        """A directory whose entire content has been hidden via overlay
        tombstones should remove cleanly: the overlay-side bookkeeping
        files must be cleared and the directory must vanish from the
        merged view."""
        Path(root_base_fs, "ghost").mkdir()
        Path(root_base_fs, "ghost", "g1.txt").write_bytes(b"g1")
        Path(root_base_fs, "ghost", "g2.txt").write_bytes(b"g2")

        # Hide both files via overlay tombstones.
        self.fs.rm("ghost/g1.txt")
        self.fs.rm("ghost/g2.txt")
        # Merged view of ghost/ is empty.
        self.assertEqual(list(self.fs.ls("ghost", detail=False)), [])

        # Now rm the directory. Should not raise on the leftover overlay
        # bookkeeping (g1.txt.deleted, g2.txt.deleted).
        self.fs.rm("ghost")
        self.assertFalse(self.fs.exists("ghost"))
        # Overlay's stale tombstones are gone…
        self.assertFalse(Path(root_transparent_fs, "ghost").exists())
        # …but a new tombstone for the directory itself is recorded so the
        # base directory does not reappear in the merged view.
        self.assertTrue(Path(root_transparent_fs, "ghost.deleted").exists())
