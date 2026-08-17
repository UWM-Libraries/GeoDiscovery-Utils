import sys
import unittest
from pathlib import Path

OPENDATAHARVEST_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(OPENDATAHARVEST_ROOT))

from DCAT_Harvester import Aardvark
from DCAT_Harvester import AardvarkDataProcessor
from DCAT_Harvester import DESCRIPTION
from DCAT_Harvester import RESOURCECLASS
from DCAT_Harvester import Site
from DCAT_Harvester import contains_unresolved_template


class UnresolvedTemplateTest(unittest.TestCase):
    def test_detects_unresolved_arcgis_template(self):
        self.assertTrue(contains_unresolved_template("{{modified:toISO}}"))
        self.assertTrue(contains_unresolved_template("Prefix {{name}}"))
        self.assertFalse(contains_unresolved_template("2024-01-08T16:41:03.000Z"))
        self.assertFalse(contains_unresolved_template(None))

    def test_templated_date_is_treated_as_missing(self):
        record = Aardvark.__new__(Aardvark)
        record.id = "Example-placeholder"

        with self.assertNoLogs(level="WARNING"):
            self.assertIsNone(
                record._parse_index_year("{{modified:toISO}}", "modified")
            )
            self.assertIsNone(
                AardvarkDataProcessor.issue_date_parser({"issued": "{{created:toISO}}"})
            )

    def test_record_uses_safe_fallbacks_for_templated_metadata(self):
        website = Site(
            "Example",
            {
                "CreatedBy": "Example Agency",
                "Spatial": ["Wisconsin", "United States"],
                "DefaultBbox": "Wisconsin",
            },
            {},
            [],
            [],
            [],
        )
        dataset = {
            "title": "{{name}}",
            "identifier": (
                "https://www.arcgis.com/home/item.html"
                "?id=6ee1cc1bf02b4b1bbe60e0c57513b02a"
            ),
            "description": "{{description}}",
            "publisher": {"name": "{{source}}"},
            "issued": "2024-01-08T16:41:03.000Z",
            "modified": "{{modified:toISO}}",
            "keyword": ["Township", "PLSS"],
            "landingPage": "https://example.com/datasets/example",
            "spatial": "-90.0, 44.0, -89.0, 43.0",
            "distribution": [],
        }

        extracted = AardvarkDataProcessor.extract_data(dataset)
        self.assertEqual(extracted["creator"], [])
        self.assertEqual(extracted["publisher"], {})

        record = Aardvark(dataset, website)

        self.assertEqual(record.dct_title_s, "Example Agency - Untitled Dataset")
        self.assertEqual(record.dct_creator_sm, [])
        self.assertEqual(record.dct_publisher_sm, ["Example Agency"])
        self.assertEqual(record.dct_description_sm, [DESCRIPTION])
        self.assertEqual(record.dct_issued_s, "2024-01-08")
        self.assertEqual(record.dct_temporal_sm, ["Issued 2024"])
        self.assertEqual(record.gbl_indexYear_im, [2024])

    def test_untitled_dataset_fallback_logs_record_identifier(self):
        for title in (None, "", "{{name}}"):
            with self.subTest(title=title):
                dataset = {
                    "identifier": "https://www.arcgis.com/home/item.html?id=example",
                    "title": title,
                    "description": "",
                }

                with self.assertLogs(level="WARNING") as captured:
                    extracted = AardvarkDataProcessor.extract_data(dataset)

                self.assertEqual(extracted["title"], "Untitled Dataset")
                self.assertIn(
                    'Assigned "Untitled Dataset" to '
                    "https://www.arcgis.com/home/item.html?id=example",
                    captured.output[0],
                )


class SpatialProcessingTest(unittest.TestCase):
    def test_valid_spatial_data_does_not_require_a_default_bbox(self):
        missing_default_bbox = {
            "envelope": None,
            "west": None,
            "east": None,
            "north": None,
            "south": None,
        }

        result = AardvarkDataProcessor.process_dcat_spatial(
            "-89.3219,43.6477,-87.7342,44.8530",
            missing_default_bbox,
        )

        self.assertEqual(result, "ENVELOPE(-89.3219,-87.7342,44.853,43.6477)")


class ResourceClassificationTest(unittest.TestCase):
    def test_imagery_classification_does_not_leak_to_later_records(self):
        original_resource_class = list(RESOURCECLASS)

        imagery = AardvarkDataProcessor.process_dataset_class_type_and_format(
            {"title": "County aerial photography", "distribution": []}
        )
        dataset = AardvarkDataProcessor.process_dataset_class_type_and_format(
            {"title": "County road centerlines", "distribution": []}
        )

        self.assertIn("Imagery", imagery["gbl_resourceClass_sm"])
        self.assertNotIn("Imagery", dataset["gbl_resourceClass_sm"])
        self.assertEqual(RESOURCECLASS, original_resource_class)


class SkipListTest(unittest.TestCase):
    def setUp(self):
        self.dataset = {
            "identifier": (
                "https://www.arcgis.com/home/item.html"
                "?id=5fddf40eccdd4de3acf119c2510036b4&sublayer=10"
            )
        }

    def website_with_skiplist(self, skiplist):
        return Site("VilasCounty", {}, {}, skiplist, [], [])

    def test_composite_uuid_skips_only_the_matching_sublayer(self):
        record = Aardvark.__new__(Aardvark)
        website = self.website_with_skiplist(["5fddf40eccdd4de3acf119c2510036b410"])

        self.assertFalse(record._process_id(self.dataset, website))

        other_sublayer = {
            "identifier": self.dataset["identifier"].replace(
                "sublayer=10", "sublayer=12"
            )
        }
        self.assertTrue(record._process_id(other_sublayer, website))

    def test_base_uuid_skips_all_sublayers(self):
        record = Aardvark.__new__(Aardvark)
        website = self.website_with_skiplist(["5fddf40eccdd4de3acf119c2510036b4"])

        self.assertFalse(record._process_id(self.dataset, website))


if __name__ == "__main__":
    unittest.main()
