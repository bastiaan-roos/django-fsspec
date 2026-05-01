import shutil
import unittest
from pathlib import Path

import fsspec

from django_fsspec.nested_fs import _NON_MULTIPART_LIMIT
from django_fsspec.nested_fs import NestedFileSystem
from django_fsspec.nested_fs import _compare_checksums_safe

test_data_dir = Path(Path(__file__).parent, "tmp")
tmp_get_dir = Path(test_data_dir, "..", "tmp_get").resolve()

root_fs1 = Path(test_data_dir, "root_fs1")
root_fs2 = Path(test_data_dir, "root_fs2")
root_fs_default = Path(test_data_dir, "root_fs3")

nested_mapping = {
    "a": {
        "protocol": "local",
        "auto_mkdir": True,
        "relative_to_path": root_fs1,
    },
    "b": {
        "protocol": "local",
        "auto_mkdir": True,
        "relative_to_path": root_fs2,
    },
    "default": {
        "protocol": "local",
        "auto_mkdir": True,
        "relative_to_path": root_fs_default,
    },
}

test_file_root_fs1 = "a/test1.txt"
test_file_root_fs1_subdir = "a/subdir/test2.txt"
test_file_root_fs2 = "b/test3.txt"
test_file_root_fs2_subdir = "b/subdir/test4.txt"
test_file_root_fs_default = "test5.txt"
test_file_root_fs_default_subdir = "subdir/test6.txt"


def get_testfile_content(file_path):
    return f"content of {file_path}"


class TestNextedPathFileSystem(unittest.TestCase):
    def tearDown(self):
        shutil.rmtree(test_data_dir, ignore_errors=True)
        shutil.rmtree(tmp_get_dir, ignore_errors=True)

    def test_using_fsspec_filesystem(self):
        fs = fsspec.filesystem("nested", path_storage_configs=nested_mapping)
        self.assertIsInstance(fs, NestedFileSystem)
        fs1, rootpath, path = fs._get_filesystem("a/something/extra")
        self.assertEqual("dir", fs1.protocol)

    def test_get_filesystem(self):
        fs = NestedFileSystem(nested_mapping)

        self.assertEqual("nested", fs.protocol)

        # get filesystem for root_fs1
        fs1, rootpath, path = fs._get_filesystem("a/something/extra")
        self.assertEqual("dir", fs1.protocol)
        self.assertEqual("a", rootpath)
        self.assertEqual("something/extra", path)

        fs1, rootpath, path = fs._get_filesystem("a")
        self.assertEqual("a", rootpath)
        self.assertEqual("", path)

        # get filesystem for root_fs2
        fs2, rootpath, path = fs._get_filesystem("b/something/extra")
        self.assertEqual("dir", fs2.protocol)
        self.assertEqual("b", rootpath)
        self.assertEqual("something/extra", path)

        # get filesystem for root_fs_default
        fs_default, rootpath, path = fs._get_filesystem("c/something/extra")
        self.assertEqual("dir", fs_default.protocol)
        self.assertEqual("", rootpath)
        self.assertEqual("c/something/extra", path)

    def test_open_write(self):
        fs = NestedFileSystem(nested_mapping)

        # write to root_fs1
        content = get_testfile_content(test_file_root_fs1)
        with fs.open(test_file_root_fs1, "w") as f:
            f.write(content)

        path = Path(root_fs1, (Path(test_file_root_fs1).relative_to("a")))
        self.assertTrue(path.exists())
        with open(path, "r") as f:
            self.assertEqual(f.read(), content)

        # write to root_fs2
        content = get_testfile_content(test_file_root_fs2)
        with fs.open(test_file_root_fs2, "w") as f:
            f.write(content)

        path = Path(root_fs2, (Path(test_file_root_fs2).relative_to("b")))
        self.assertTrue(path.exists())
        with open(path, "r") as f:
            self.assertEqual(f.read(), content)

        # write to root_fs_default
        content = get_testfile_content(test_file_root_fs_default)
        with fs.open(test_file_root_fs_default, "w") as f:
            f.write(content)

        path = Path(root_fs_default, test_file_root_fs_default)
        self.assertTrue(path.exists())
        with open(path, "r") as f:
            self.assertEqual(f.read(), content)

    def test_open_write_subdir(self):
        fs = NestedFileSystem(nested_mapping)

        # write to root_fs1 subdir
        content = get_testfile_content(test_file_root_fs1_subdir)
        with fs.open(test_file_root_fs1_subdir, "w") as f:
            f.write(content)

        path = Path(root_fs1, (Path(test_file_root_fs1_subdir).relative_to("a")))
        self.assertTrue(path.exists())
        with open(path, "r") as f:
            self.assertEqual(f.read(), content)

        # write to root_default subdir
        content = get_testfile_content(test_file_root_fs_default_subdir)
        with fs.open(test_file_root_fs_default_subdir, "w") as f:
            f.write(content)

        path = Path(root_fs_default, (Path(test_file_root_fs_default_subdir).relative_to("")))
        self.assertTrue(path.exists())
        with open(path, "r") as f:
            self.assertEqual(f.read(), content)

    def test_mkdir_and_mkdirs(self):
        fs = NestedFileSystem(nested_mapping)

        # create directory in root_fs1
        fs.mkdir("a/new_dir")
        path = Path(root_fs1, "new_dir")
        self.assertTrue(path.exists())

        # create directory in root_fs_default
        fs.mkdir("new_dir")
        path = Path(root_fs_default, "new_dir")
        self.assertTrue(path.exists())

        # create path that is in nested_mapping
        with self.assertRaises(FileExistsError):
            fs.mkdir("a")

        # created nested not existing
        fs.mkdir("a/nested/not/existing/yet")
        path = Path(root_fs1, "nested/not/existing/yet")
        self.assertTrue(path.exists())

        with self.assertRaises(FileExistsError):
            fs.mkdir("a/nested/not/existing/yet")

        # with makedirs
        with self.assertRaises(FileExistsError):
            fs.makedirs("a/nested/not/existing/yet", exist_ok=False)

        # without exception
        fs.makedirs("a/nested/not/existing/yet", exist_ok=True)

        # check if it is created
        fs.makedirs("a/nested/not/existing/yet2", exist_ok=True)
        path = Path(root_fs1, "nested/not/existing/yet2")
        self.assertTrue(path.exists())

    def test_read_file_functions(self):
        """
        test 'read' functions on filesystem:
        - exists
        - lexists
        - checksum
        - ukey
        - size
        - isdir
        - isfile
        - read_text
        - cat_file
        - cat
        - get_file
        - head
        - tail
        - read_block
        - created
        - modified
        # - pipe_file
        # - pipe?

        - ls
        - glob
        - walk

        :return:
        """

        # make content of 1000 characters
        content = "abcdefghij" * 1000

        # file paths
        file_paths = [
            ("a/test1.txt", f"{root_fs1}/test1.txt"),
            ("a/subdir/test2.txt", f"{root_fs1}/subdir/test2.txt"),
            ("b/test3.txt", f"{root_fs2}/test3.txt"),
            ("b/subdir/test4.txt", f"{root_fs2}/subdir/test4.txt"),
            ("test5.txt", f"{root_fs_default}/test5.txt"),
            ("subdir/test6.txt", f"{root_fs_default}/subdir/test6.txt"),
        ]
        # create files
        fs = NestedFileSystem(nested_mapping)
        for file_path, _ in file_paths:
            with fs.open(file_path, "w") as f:
                f.write(content)

        # start testing
        for file_path, abs_local_path in file_paths:
            # exists
            self.assertTrue(fs.exists(file_path))
            # lexists
            self.assertTrue(fs.lexists(file_path))
            # checksum
            self.assertIsNotNone(fs.checksum(file_path))
            # ukey
            self.assertIsNotNone(fs.ukey(file_path))
            # size
            self.assertEqual(len(content), 10000)
            # isdir
            self.assertFalse(fs.isdir(file_path))
            self.assertTrue(Path(file_path).parent)
            # isfile
            self.assertTrue(fs.isfile(file_path))
            # read_text
            self.assertEqual(fs.read_text(file_path), content)
            # cat_file
            self.assertEqual(fs.cat_file(file_path), bytes(content, "utf-8"))
            # cat
            self.assertEqual(fs.cat(file_path), bytes(content, "utf-8"))
            # get_file
            out = Path(tmp_get_dir, Path(file_path).name)
            fs.get_file(file_path, out)
            self.assertTrue(out.exists())
            with open(out, "r") as f:
                self.assertEqual(content, f.read())
            # head
            self.assertEqual(bytes(content[:1024], "utf-8"), fs.head(file_path, 1024))
            # tail
            self.assertEqual(bytes(content[-1024:], "utf-8"), fs.tail(file_path, 1024))
            # read_block
            self.assertEqual(bytes(content[4:9], "utf-8"), fs.read_block(file_path, 4, 5))
            # created — 1 ms tolerance: the datetime round-trip through UTC
            # loses sub-ms precision on some filesystems.
            self.assertAlmostEqual(
                Path(abs_local_path).stat().st_ctime,
                fs.created(file_path).timestamp(),
                places=3,
            )
            # modified — same precision caveat as `created` above.
            self.assertAlmostEqual(
                Path(abs_local_path).stat().st_mtime,
                fs.modified(file_path).timestamp(),
                places=3,
            )

        # todo:
        # test get on directory
        # pipe?
        # pipe_file?

    # def test_write_file_functions(self):
    #     """
    #     test 'write/ delete' functions on filesystem:
    #     - touch
    #     - rmdir
    #     - write_text
    #     - put_file
    #     - cp_file
    #     - mv
    #     - rem_file
    #     - rm
    #
    #     :return:
    #     """
    #     pass

    def test_get_filesystem_no_default_returns_none(self):
        """Zonder default entry retourneert _get_filesystem (None, "", path) i.p.v. KeyError."""
        mapping_no_default = {k: v for k, v in nested_mapping.items() if k != "default"}
        fs = NestedFileSystem(mapping_no_default)

        result = fs._get_filesystem("c/something/extra")
        self.assertIsNone(result[0])
        self.assertEqual("", result[1])
        self.assertEqual("c/something/extra", result[2])

    def test_cp_file_same_fs(self):
        """cp_file binnen één sub-fs (regressietest voor 2-vs-3-tuple unpack bug)."""
        fs = NestedFileSystem(nested_mapping)
        with fs.open("a/source.txt", "w") as f:
            f.write("hallo wereld")

        # Previously this crashed with `ValueError: too many values to unpack`.
        fs.cp_file("a/source.txt", "a/dest.txt")

        self.assertTrue(fs.exists("a/dest.txt"))
        self.assertEqual("hallo wereld", fs.read_text("a/dest.txt"))

    def test_cp_file_cross_fs(self):
        """cp_file tussen twee verschillende sub-fs'en."""
        fs = NestedFileSystem(nested_mapping)
        with fs.open("a/source.txt", "w") as f:
            f.write("cross fs content")

        fs.cp_file("a/source.txt", "b/dest.txt")

        self.assertTrue(fs.exists("a/source.txt"))  # origineel intact
        self.assertTrue(fs.exists("b/dest.txt"))
        self.assertEqual("cross fs content", fs.read_text("b/dest.txt"))

    def test_mv_same_fs(self):
        """mv binnen één sub-fs."""
        fs = NestedFileSystem(nested_mapping)
        with fs.open("a/source.txt", "w") as f:
            f.write("verplaatsen")

        fs.mv("a/source.txt", "a/dest.txt")

        self.assertFalse(fs.exists("a/source.txt"))
        self.assertTrue(fs.exists("a/dest.txt"))
        self.assertEqual("verplaatsen", fs.read_text("a/dest.txt"))

    def test_mv_cross_fs(self):
        """mv tussen twee verschillende sub-fs'en."""
        fs = NestedFileSystem(nested_mapping)
        with fs.open("a/source.txt", "w") as f:
            f.write("cross fs mv")

        fs.mv("a/source.txt", "b/dest.txt")

        self.assertFalse(fs.exists("a/source.txt"))
        self.assertTrue(fs.exists("b/dest.txt"))
        self.assertEqual("cross fs mv", fs.read_text("b/dest.txt"))

    def test_ls_sub_fs_uses_correct_root_path(self):
        """ls() on a sub-fs prefixes names with the right root_path (bug: used fs.root_path).

        Previously the code referenced `fs.root_path` (which did not exist),
        so ls() crashed with AttributeError the moment it was called on a
        sub-fs.
        """
        fs = NestedFileSystem(nested_mapping)
        with fs.open("a/test1.txt", "w") as f:
            f.write("x")
        with fs.open("a/test2.txt", "w") as f:
            f.write("y")

        # Must not crash.
        names = fs.ls("a", detail=False)
        self.assertEqual(2, len(names))
        # Both entries get an "a/" prefix.
        for name in names:
            self.assertTrue(name.startswith("a/"), f"Expected 'a/' prefix, got: {name}")

    def test_walk_top_level_doesnt_crash_without_maxdepth(self):
        """walk() with maxdepth=None must not crash on None - 1 (old bug)."""
        fs = NestedFileSystem(nested_mapping)
        with fs.open("a/test.txt", "w") as f:
            f.write("x")

        # Must not raise TypeError on None - 1.
        results = list(fs.walk(""))
        self.assertGreater(len(results), 0)

    def test_walk_specific_subfs(self):
        """walk() on a specific sub-fs path."""
        fs = NestedFileSystem(nested_mapping)
        with fs.open("a/dir1/file1.txt", "w") as f:
            f.write("x")
        with fs.open("a/dir1/file2.txt", "w") as f:
            f.write("y")

        results = list(fs.walk("a"))
        # Collect every file path.
        all_files = set()
        for base, _dirs, files in results:
            for f in files:
                full = f"{base}/{f}" if base else f
                all_files.add(full)

        self.assertIn("a/dir1/file1.txt", all_files)
        self.assertIn("a/dir1/file2.txt", all_files)

    def test_resolve_s3_target_raises_for_local_fs(self):
        """resolve_s3_target on non-S3 sub-fs must raise NotImplementedError."""
        fs = NestedFileSystem(nested_mapping)
        # Prefix 'a' routes to the local DirFileSystem
        with self.assertRaises(NotImplementedError) as ctx:
            fs.resolve_s3_target("a/foo.txt")
        self.assertIn("S3FileSystem", str(ctx.exception))

    def test_resolve_s3_target_raises_for_default_local(self):
        """Default local fallback must raise NotImplementedError too."""
        fs = NestedFileSystem(nested_mapping)
        with self.assertRaises(NotImplementedError):
            fs.resolve_s3_target("foo.txt")

    def test_resolve_s3_target_no_match_raises_filenotfound(self):
        """Path with unknown prefix and no `default` must raise FileNotFoundError."""
        mapping_no_default = {
            "only_a": {
                "protocol": "local",
                "auto_mkdir": True,
                "relative_to_path": root_fs1,
            },
        }
        fs = NestedFileSystem(mapping_no_default)
        with self.assertRaises(FileNotFoundError):
            fs.resolve_s3_target("unknown/foo.txt")

    def test_unmatched_path_without_default_raises_filenotfound(self):
        """rm/mkdir/etc on an unmatched path raise FileNotFoundError with
        the path included — previously they raised generic ValueErrors."""
        mapping_no_default = {
            "only_a": {
                "protocol": "local",
                "auto_mkdir": True,
                "relative_to_path": root_fs1,
            },
        }
        fs = NestedFileSystem(mapping_no_default)
        for op, args in [
            ("rm", ("unmatched/foo.txt",)),
            ("mkdir", ("unmatched/foo",)),
            ("makedirs", ("unmatched/foo",)),
            ("rmdir", ("unmatched/foo",)),
            ("put", ("/tmp/whatever", "unmatched/foo.txt")),
        ]:
            with self.assertRaises(FileNotFoundError) as ctx:
                getattr(fs, op)(*args)
            msg = str(ctx.exception)
            self.assertIn("unmatched", msg, f"{op}: {msg!r} should mention path")

    def test_recursive_rm_at_root_walks_every_subfs(self):
        """`rm("", recursive=True)` clears every sub-fs, not just the
        matched/default one."""
        fs = fsspec.filesystem("nested", path_storage_configs=nested_mapping)
        # Drop a file in each sub-fs and the default.
        for sub in ("a/foo.txt", "b/bar.txt", "stray.txt"):
            with fs.open(sub, "w") as f:
                f.write("x")
        # Sanity: all three exist.
        self.assertTrue(fs.exists("a/foo.txt"))
        self.assertTrue(fs.exists("b/bar.txt"))
        self.assertTrue(fs.exists("stray.txt"))

        fs.rm("", recursive=True)

        # All three sub-fs's must now be empty.
        self.assertFalse(fs.exists("a/foo.txt"))
        self.assertFalse(fs.exists("b/bar.txt"))
        self.assertFalse(fs.exists("stray.txt"))


class TestCompareChecksumsSafe(unittest.TestCase):
    """Verifies the graceful-skip semantics of the checksum compare helper.

    The helper must:
    - return ``True`` when both checksums are equal strings,
    - raise ``IOError`` when both are strings but unequal,
    - return ``True`` (skip) when either fs returns a non-string checksum
      (e.g. local FS returns an int of size+mtime that always mismatches),
    - return ``True`` (skip) when either ``checksum()`` raises
      ``NotImplementedError``,
    - return ``True`` (skip) when ``size`` is at or above
      ``_NON_MULTIPART_LIMIT`` (multipart ETag uncertainty).
    """

    def _fake_fs(self, checksum_value):
        """Return an object exposing a ``checksum(path)`` method."""

        class _FakeFs:
            def checksum(self, path):
                if isinstance(checksum_value, type) and issubclass(checksum_value, BaseException):
                    raise checksum_value("nope")
                return checksum_value

        return _FakeFs()

    def test_matching_string_checksums(self):
        result = _compare_checksums_safe(self._fake_fs("abc"), "src", self._fake_fs("abc"), "dst", size=1024)
        self.assertTrue(result)

    def test_mismatched_string_checksums_raises(self):
        with self.assertRaises(IOError) as ctx:
            _compare_checksums_safe(self._fake_fs("abc"), "src", self._fake_fs("xyz"), "dst", size=1024)
        msg = str(ctx.exception)
        self.assertIn("Checksum mismatch", msg)
        self.assertIn("src", msg)
        self.assertIn("dst", msg)

    def test_int_checksum_skips(self):
        # Local FS returns int(size+mtime). isinstance(str) gating must skip.
        result = _compare_checksums_safe(self._fake_fs(123), "src", self._fake_fs("abc"), "dst", size=1024)
        self.assertTrue(result)

    def test_notimplementederror_skips(self):
        result = _compare_checksums_safe(
            self._fake_fs(NotImplementedError), "src", self._fake_fs("abc"), "dst", size=1024
        )
        self.assertTrue(result)

    def test_size_at_multipart_limit_skips(self):
        # File exactly at threshold: skip (defensive — could be multipart).
        result = _compare_checksums_safe(
            self._fake_fs("abc"), "src", self._fake_fs("xyz"), "dst", size=_NON_MULTIPART_LIMIT
        )
        self.assertTrue(result)

    def test_size_above_multipart_limit_skips(self):
        result = _compare_checksums_safe(
            self._fake_fs("abc"), "src", self._fake_fs("xyz"), "dst", size=_NON_MULTIPART_LIMIT * 2
        )
        self.assertTrue(result)


class TestCpFileCrossFsVerification(unittest.TestCase):
    """Cross-fs cp_file must verify that the destination size matches the
    source. On mismatch the destination is removed; the source is left
    intact. Same-fs cp_file delegates to the sub-fs and is out of scope here.
    """

    def setUp(self):
        # Two separate local roots so cp_file takes the cross-fs branch.
        self.fs = NestedFileSystem(nested_mapping)
        with self.fs.open("a/source.txt", "w") as f:
            f.write("0123456789" * 100)  # 1000 bytes

    def tearDown(self):
        shutil.rmtree(test_data_dir, ignore_errors=True)

    def test_size_match_copies_successfully(self):
        """Happy path: sizes match → copy succeeds, both files present."""
        self.fs.cp_file("a/source.txt", "b/dest.txt")
        self.assertTrue(self.fs.exists("a/source.txt"))
        self.assertTrue(self.fs.exists("b/dest.txt"))
        self.assertEqual(self.fs.size("a/source.txt"), self.fs.size("b/dest.txt"))

    def test_size_mismatch_raises_and_removes_destination(self):
        """Mismatch: destination is removed, source is preserved, IOError raised."""
        # Get the destination sub-fs and monkey-patch its size() to lie
        # about the written bytes — simulates a partial put.
        fs2, _root, _ = self.fs._get_filesystem("b/dest.txt")
        original_size = fs2.size

        def _lying_size(path, *args, **kwargs):
            return original_size(path, *args, **kwargs) - 1

        fs2.size = _lying_size
        try:
            with self.assertRaises(IOError) as ctx:
                self.fs.cp_file("a/source.txt", "b/dest.txt")
            self.assertIn("size", str(ctx.exception).lower())
        finally:
            fs2.size = original_size

        # Source intact, destination removed (per "delete the suspect, keep
        # the original" cleanup philosophy).
        self.assertTrue(self.fs.exists("a/source.txt"))
        self.assertFalse(self.fs.exists("b/dest.txt"))

    def test_destination_remove_failure_is_swallowed(self):
        """When the cleanup rm itself fails, the original IOError still surfaces.

        Use case: destination fs lost connectivity or permission was revoked
        between put_file and rm. We must not mask the verification error
        with the cleanup error.
        """
        fs2, _root, _ = self.fs._get_filesystem("b/dest.txt")
        original_size = fs2.size
        fs2.size = lambda path, *a, **k: original_size(path, *a, **k) - 1

        original_rm = fs2.rm

        def _broken_rm(*a, **k):
            raise PermissionError("cleanup denied")

        fs2.rm = _broken_rm
        try:
            with self.assertRaises(IOError):
                self.fs.cp_file("a/source.txt", "b/dest.txt")
        finally:
            fs2.size = original_size
            fs2.rm = original_rm

    def test_same_fs_cp_file_unchanged(self):
        """Same-fs path delegates to the sub-fs and bypasses verification.

        Regression guard: we must not introduce verification overhead on
        the same-fs branch, because that breaks single-fs unit tests that
        do not configure size() to be reliable.
        """
        self.fs.cp_file("a/source.txt", "a/dest.txt")
        self.assertTrue(self.fs.exists("a/dest.txt"))

    def test_verify_checksum_true_with_string_match_passes(self):
        """End-to-end: when both fs return matching string checksums, it succeeds."""
        fs1, _, _ = self.fs._get_filesystem("a/source.txt")
        fs2, _, _ = self.fs._get_filesystem("b/dest.txt")
        # Force a portable string checksum on both sides.
        fs1.checksum = lambda path, **k: "deadbeef"
        fs2.checksum = lambda path, **k: "deadbeef"

        self.fs.cp_file("a/source.txt", "b/dest.txt", verify_checksum=True)
        self.assertTrue(self.fs.exists("b/dest.txt"))

    def test_verify_checksum_true_with_string_mismatch_raises(self):
        """End-to-end: differing string checksums raise and remove dest."""
        fs1, _, _ = self.fs._get_filesystem("a/source.txt")
        fs2, _, _ = self.fs._get_filesystem("b/dest.txt")
        fs1.checksum = lambda path, **k: "deadbeef"
        fs2.checksum = lambda path, **k: "cafef00d"

        with self.assertRaises(IOError) as ctx:
            self.fs.cp_file("a/source.txt", "b/dest.txt", verify_checksum=True)
        self.assertIn("Checksum mismatch", str(ctx.exception))
        # Destination removed, source intact.
        self.assertFalse(self.fs.exists("b/dest.txt"))
        self.assertTrue(self.fs.exists("a/source.txt"))

    def test_verify_checksum_true_with_int_checksums_skips(self):
        """Default local-FS behavior (int checksum) must not break verify_checksum.

        Reason: the local FS returns ``int(size+mtime)`` which is guaranteed
        to differ between two roots. A naive comparison would make
        ``verify_checksum=True`` unusable for local development; the
        graceful-skip path in ``_compare_checksums_safe`` filters out
        non-string checksums.
        """
        # No monkey-patch: real local FS returns int. Should still succeed.
        self.fs.cp_file("a/source.txt", "b/dest.txt", verify_checksum=True)
        self.assertTrue(self.fs.exists("b/dest.txt"))

    def test_verify_checksum_default_off_does_not_call_checksum(self):
        """Without verify_checksum=True the helper must not be invoked.

        Guards against a regression where someone refactors and accidentally
        wires the checksum compare in unconditionally — we already had a
        bug like that in the FsspecStorage layer, document the contract.
        """
        fs1, _, _ = self.fs._get_filesystem("a/source.txt")
        calls = []
        original = fs1.checksum
        fs1.checksum = lambda path, **k: calls.append(path) or original(path, **k)
        try:
            self.fs.cp_file("a/source.txt", "b/dest.txt")  # default off
        finally:
            fs1.checksum = original
        self.assertEqual([], calls, "checksum() must not be called when verify_checksum=False")


class TestMvCrossFsVerification(unittest.TestCase):
    """Cross-fs mv must not delete the source until the destination is
    confirmed present. The previous implementation called ``rm`` on the
    source unconditionally after ``cp_file`` — a silently-failing copy
    would have caused data loss.
    """

    def setUp(self):
        self.fs = NestedFileSystem(nested_mapping)
        with self.fs.open("a/source.txt", "w") as f:
            f.write("inhoud om te verplaatsen")

    def tearDown(self):
        shutil.rmtree(test_data_dir, ignore_errors=True)

    def test_happy_path_moves(self):
        """Sanity: a normal cross-fs mv still works."""
        self.fs.mv("a/source.txt", "b/dest.txt")
        self.assertFalse(self.fs.exists("a/source.txt"))
        self.assertTrue(self.fs.exists("b/dest.txt"))

    def test_destination_missing_after_copy_preserves_source(self):
        """If cp_file silently produced no destination, mv must not rm source.

        Simulated by monkey-patching the destination sub-fs ``exists`` to
        return False even after a successful put. In real life this would
        be e.g. a backend that swallows write errors; we want belt-and-
        braces against that.
        """
        fs2, _root, _ = self.fs._get_filesystem("b/dest.txt")
        original_exists = fs2.exists

        def _lying_exists(path, *args, **kwargs):
            # Lie about the freshly-written file only; let real lookups pass.
            if path.endswith("dest.txt"):
                return False
            return original_exists(path, *args, **kwargs)

        fs2.exists = _lying_exists
        try:
            with self.assertRaises(IOError) as ctx:
                self.fs.mv("a/source.txt", "b/dest.txt")
            self.assertIn("destination", str(ctx.exception).lower())
        finally:
            fs2.exists = original_exists

        # Source must still be there — the operation failed before rm.
        self.assertTrue(self.fs.exists("a/source.txt"))

    def test_cp_file_failure_preserves_source(self):
        """If cp_file itself raises (e.g. size-check fails), mv must not rm source.

        Regression for the original bug: previously rm ran unconditionally
        after cp_file, even if cp_file later started raising on mismatch.
        """
        fs2, _root, _ = self.fs._get_filesystem("b/dest.txt")
        original_size = fs2.size
        fs2.size = lambda path, *a, **k: 0  # always lie → size mismatch

        try:
            with self.assertRaises(IOError):
                self.fs.mv("a/source.txt", "b/dest.txt")
        finally:
            fs2.size = original_size

        self.assertTrue(self.fs.exists("a/source.txt"))
        self.assertFalse(self.fs.exists("b/dest.txt"))
