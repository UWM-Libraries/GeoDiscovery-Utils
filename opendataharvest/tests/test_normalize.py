import subprocess
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

OPENDATAHARVEST_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(OPENDATAHARVEST_ROOT))

from classify import ResourceClassifier
from normalize import ResourceClassificationNormalizer
from normalize import ResourceValueNormalizer
from normalize import TitleTransliterationNormalizer


class TitleTransliterationNormalizerTest(unittest.TestCase):
    def setUp(self):
        TitleTransliterationNormalizer._cache = {}
        TitleTransliterationNormalizer._disabled = False
        TitleTransliterationNormalizer._transient_failures = 0

    def test_transient_uconv_failure_does_not_disable_later_transliteration(self):
        title = "北京市城区街道图"

        with patch(
            "normalize.subprocess.run",
            side_effect=[
                subprocess.SubprocessError("temporary uconv failure"),
                SimpleNamespace(stdout="Bei Jing Shi Cheng Qu Jie Dao Tu\n"),
            ],
        ):
            # A transient subprocess failure should not disable future attempts.
            self.assertIsNone(TitleTransliterationNormalizer.transliterate(title))
            self.assertFalse(TitleTransliterationNormalizer._disabled)

            self.assertEqual(
                TitleTransliterationNormalizer.transliterate(title),
                "Bei Jing Shi Cheng Qu Jie Dao Tu",
            )

    def test_latin_leading_titles_after_punctuation_do_not_shell_out(self):
        with patch("normalize.subprocess.run") as mock_run:
            self.assertIsNone(
                TitleTransliterationNormalizer.transliterate(
                    "!Alabama county boundaries"
                )
            )
            self.assertIsNone(
                TitleTransliterationNormalizer.transliterate("  Éire county map")
            )

        mock_run.assert_not_called()


class ResourceValueNormalizerTest(unittest.TestCase):
    def test_resource_types_are_trimmed_and_deduplicated(self):
        record = {
            "gbl_resourceType_sm": ["Index maps ", " Geological Maps", "Index maps"]
        }

        changed = ResourceValueNormalizer.normalize(record)

        self.assertTrue(changed)
        self.assertEqual(
            record["gbl_resourceType_sm"], ["Index maps", "Geological Maps"]
        )


class ResourceClassificationNormalizerTest(unittest.TestCase):
    def test_explicit_website_class_does_not_require_a_resource_type(self):
        record = {
            "id": "VilasCounty-ed0f4064bfd34596abcd655452b1cd73",
            "dct_title_s": "Vilas County application",
            "dct_format_s": None,
            "gbl_resourceClass_sm": ["Websites"],
            "gbl_resourceType_sm": [],
        }

        changed = ResourceClassificationNormalizer.normalize(record)

        self.assertFalse(changed)
        self.assertEqual(record["gbl_resourceClass_sm"], ["Websites"])
        self.assertEqual(record["gbl_resourceType_sm"], [])

    def test_explicit_collection_class_does_not_require_a_resource_type(self):
        record = {
            "id": "agsl-opendata-harvest",
            "dct_title_s": "AGSL Wisconsin Open Data Harvest",
            "gbl_resourceClass_sm": ["Collections"],
        }

        changed = ResourceClassificationNormalizer.normalize(record)

        self.assertFalse(changed)
        self.assertEqual(record["gbl_resourceClass_sm"], ["Collections"])


class ResourceClassifierTest(unittest.TestCase):
    def test_planning_commission_in_title_is_not_a_map_signal(self):
        record = {
            "id": "CARPC-8a4d495a845e4c01934aea0855ada047",
            "dct_title_s": (
                "Capital Area Regional Planning Commission - Heritage Oak Project"
            ),
            "dct_description_sm": [],
            "dct_subject_sm": [],
            "dct_format_s": None,
            "gbl_resourceClass_sm": [],
            "gbl_resourceType_sm": [],
        }

        resource_class, _resource_type = (
            ResourceClassifier.determine_resource_class_and_type(record)
        )

        self.assertNotIn("Maps", resource_class)


if __name__ == "__main__":
    unittest.main()
