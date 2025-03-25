import unittest
from pathlib import Path

import fsspec

from django_fsspec.utils import get_filesystem

tmp_test_path = Path(Path(__file__).parent, "tmp", "test_media")


class TestGetFilesystem(unittest.TestCase):
    def test_with_fs_as_param(self):
        fs = fsspec.filesystem("file")
        self.assertEqual(("file", "local"), fs.protocol)
        fs_out = get_filesystem(fs)
        self.assertEqual(fs_out, fs)

    def test_with_fs_type_and_config(self):
        fs_out = get_filesystem(protocol="local", auto_mkdir=True)
        self.assertEqual(("file", "local"), fs_out.protocol)

    def test_with_relative_path(self):
        fs_out = get_filesystem(
            protocol="local",
            auto_mkdir=True,
            relative_to_path=tmp_test_path,
        )
        self.assertEqual("dir", fs_out.protocol)
