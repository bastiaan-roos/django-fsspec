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
        # Zorg dat de test directories bestaan zodat de DirFileSystem ze accepteert
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
        """Regressie: lexists() riep voorheen `self._get_filesystem(path)` aan,
        wat een NestedFileSystem methode is die niet op TransparentFileSystem
        bestaat. Dat was een copy-paste bug die meteen crashte op AttributeError.
        """
        # Voor een non-existing path: moet False retourneren, niet crashen
        self.assertFalse(self.fs.lexists("does/not/exist.txt"))

        # Schrijf een file en verifieer dat lexists() True retourneert
        with self.fs.open("present.txt", "w") as f:
            f.write("hi")
        self.assertTrue(self.fs.lexists("present.txt"))
