import json
import sys
import tempfile
import unittest
from pathlib import Path

OPENDATAHARVEST_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(OPENDATAHARVEST_ROOT))

from gbl_to_aardvark import has_aardvark_metadata
from gbl_to_aardvark import has_legacy_metadata


class GblToAardvarkTest(unittest.TestCase):
    def test_detects_legacy_geoblacklight_by_content_not_filename(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir)
            legacy_path = repo_path / "metadata-1.0" / "record-123.json"
            legacy_path.parent.mkdir(parents=True)
            legacy_path.write_text(
                json.dumps({"id": "record-123", "geoblacklight_version": "1.0"}),
                encoding="utf8",
            )

            self.assertTrue(has_legacy_metadata(legacy_path.parent))

    def test_detects_aardvark_metadata_by_content_not_filename(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            repo_path = Path(tmpdir)
            aardvark_path = repo_path / "aardvark" / "record-123.json"
            aardvark_path.parent.mkdir(parents=True)
            aardvark_path.write_text(
                json.dumps({"id": "record-123", "gbl_mdVersion_s": "Aardvark"}),
                encoding="utf8",
            )

            self.assertTrue(has_aardvark_metadata(repo_path))


if __name__ == "__main__":
    unittest.main()
