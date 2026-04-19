import shutil
import unittest
from pathlib import Path

import fsspec

from django_fsspec.nested_fs import NestedFileSystem

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

        path = Path(
            root_fs_default, (Path(test_file_root_fs_default_subdir).relative_to(""))
        )
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
            self.assertEqual(
                bytes(content[4:9], "utf-8"), fs.read_block(file_path, 4, 5)
            )
            # created
            self.assertAlmostEqual(
                Path(abs_local_path).stat().st_ctime,
                fs.created(file_path).timestamp(),
                4,
            )
            # modified
            self.assertAlmostEqual(
                Path(abs_local_path).stat().st_mtime,
                fs.modified(file_path).timestamp(),
                4,
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

        # Voorheen crashte dit op `ValueError: too many values to unpack`
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
        """ls() op een sub-fs prefixed names met de juiste root_path (Bug: gebruikte fs.root_path).

        Voorheen verwees de code naar `fs.root_path` (bestond niet), waardoor
        ls() crashte op AttributeError zodra je 'm op een sub-fs aanriep.
        """
        fs = NestedFileSystem(nested_mapping)
        with fs.open("a/test1.txt", "w") as f:
            f.write("x")
        with fs.open("a/test2.txt", "w") as f:
            f.write("y")

        # Mag niet crashen
        names = fs.ls("a", detail=False)
        self.assertEqual(2, len(names))
        # Beide entries krijgen een "a/" prefix
        for name in names:
            self.assertTrue(name.startswith("a/"), f"Expected 'a/' prefix, got: {name}")

    def test_walk_top_level_doesnt_crash_without_maxdepth(self):
        """walk() met maxdepth=None mag niet crashen op None - 1 (oude bug)."""
        fs = NestedFileSystem(nested_mapping)
        with fs.open("a/test.txt", "w") as f:
            f.write("x")

        # Mag niet TypeError op None - 1 geven
        results = list(fs.walk(""))
        self.assertGreater(len(results), 0)

    def test_walk_specific_subfs(self):
        """walk() op een specifieke sub-fs path."""
        fs = NestedFileSystem(nested_mapping)
        with fs.open("a/dir1/file1.txt", "w") as f:
            f.write("x")
        with fs.open("a/dir1/file2.txt", "w") as f:
            f.write("y")

        results = list(fs.walk("a"))
        # Verzamel alle file paden
        all_files = set()
        for base, dirs, files in results:
            for f in files:
                full = f"{base}/{f}" if base else f
                all_files.add(full)

        self.assertIn("a/dir1/file1.txt", all_files)
        self.assertIn("a/dir1/file2.txt", all_files)
