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

    def test_get_filesystem(self):
        base_fs = self.fs.base_fs
        self.assertEqual("dir", base_fs.protocol)

        transparent_fs = self.fs.transparent_fs
        self.assertEqual("dir", transparent_fs.protocol)

    # todo: add more tests
