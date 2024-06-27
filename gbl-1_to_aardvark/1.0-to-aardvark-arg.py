import json
import csv
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import argparse


class LoggerConfig:
    @staticmethod
    def configure_logging(logfile: str) -> None:
        logging.basicConfig(
            filename=logfile,
            filemode="a",
            level=logging.WARNING,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )


class SchemaUpdater:
    RESOURCE_CLASS_DEFAULT = "Other"
    PLACE_DEFAULT = None
    CROSSWALK_PATH = Path("/home/srappel/GeoDiscovery-Utils/gbl-1_to_aardvark/crosswalk.csv")

    def __init__(self):
        self.crosswalk = self.load_crosswalk(self.CROSSWALK_PATH)

    @staticmethod
    def load_crosswalk(crosswalk_path: Path) -> Dict[str, str]:
        """Load crosswalk CSV into a dictionary."""
        crosswalk = {}
        with open(crosswalk_path, encoding="utf8") as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for old, new in reader:
                crosswalk[old] = new
        return crosswalk

    def update_all_schemas(self, dir_old_schema: Path, dir_new_schema: Path) -> None:
        """Update schemas for all JSON files in the directory."""
        dir_new_schema.mkdir(parents=True, exist_ok=True)
        files = self.list_all_json_files(dir_old_schema)
        for file in files:
            logging.info(f"Processing {file} ...")
            self.update_schema(file, dir_new_schema)

    @staticmethod
    def list_all_json_files(rootdir: Path) -> List[Path]:
        """List all JSON files in a directory, excluding 'layers.json'."""
        return [path for path in rootdir.rglob("*.json") if path.name != "layers.json"]

    def update_schema(self, filepath: Path, dir_new_schema: Path) -> None:
        """Update the schema of a single JSON file."""
        try:
            with open(filepath, encoding="utf8") as fr:
                data = json.load(fr)

            if not isinstance(data, dict):
                return

            for old_schema, new_schema in self.crosswalk.items():
                if old_schema in data:
                    data[new_schema] = data.pop(old_schema)

            data["gbl_mdVersion_s"] = "Aardvark"
            data.pop("geoblacklight_version", None)

            self.check_required(data)
            self.remove_deprecated(data)
            self.fix_stanford_place_issue(data)
            data = self.string2array(data)

            new_filepath = dir_new_schema / (filepath.name if filepath.name != "geoblacklight.json" else f"{data['id']}.json")
            with open(new_filepath, "w", encoding="utf8") as fw:
                json.dump(data, fw, indent=2)
        except Exception as e:
            logging.error(f"Failed to update schema for {filepath.name}: {e}")

    def check_required(self, data_dict: Dict) -> None:
        """Check for required fields and handle missing ones."""
        requirements = [
            "dct_publisher_sm",
            "dct_spatial_sm",
            "gbl_mdVersion_s",
            "dct_title_s",
            "gbl_resourceClass_sm",
            "id",
            "gbl_mdModified_dt",
            "gbl_resourceType_sm",
        ]

        for req in requirements:
            if req not in data_dict:
                logging.warning(f"Requirement {req} is not present...")
                self.handle_missing_field(data_dict, req)

    def handle_missing_field(self, data_dict: Dict, field: str) -> None:
        """Handle missing required fields with default values or logic."""
        if field == "gbl_resourceClass_sm":
            data_dict["gbl_resourceClass_sm"] = self.determine_resource_class(data_dict)
        elif field == "dct_spatial_sm":
            data_dict["dct_spatial_sm"] = [self.PLACE_DEFAULT] if self.PLACE_DEFAULT else []
        elif field == "gbl_mdModified_dt":
            data_dict["gbl_mdModified_dt"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        elif field == "dct_publisher_sm":
            data_dict["dct_publisher_sm"] = data_dict.get("dct_creator_sm", [])
        elif field == "gbl_resourceType_sm":
            data_dict["gbl_resourceType_sm"] = self.determine_resource_type(data_dict)

    @staticmethod
    def determine_resource_class(data_dict: Dict) -> List[str]:
        """Determine the resource class based on the data dictionary."""
        format = data_dict.get("dct_format_s", "")
        if format in ["Shapefile", "ArcGrid", "GeoDatabase"]:
            return ["Datasets"]
        elif format == "GeoTIFF":
            description = data_dict.get("dct_description_sm", "")
            return ["Maps"] if "georeferenced" in description else ["Datasets"]
        else:
            return [SchemaUpdater.RESOURCE_CLASS_DEFAULT]

    @staticmethod
    def determine_resource_type(data_dict: Dict) -> List[str]:
        """Determine the resource type based on the data dictionary."""
        if "stanford-ch237ht4777" in data_dict.get("dct_source_sm", "") or data_dict.get("id") == "stanford-ch237ht4777":
            return ["Index maps"]
        return []

    @staticmethod
    def remove_deprecated(data_dict: Dict) -> None:
        """Remove deprecated fields from the data dictionary."""
        deprecated_fields = [
            "dc_type_s",
            "layer_geom_type_s",
            "dct_isPartOf_sm",
            "uw_supplemental_s",
            "uw_notice_s",
            "uuid",
        ]
        for field in deprecated_fields:
            data_dict.pop(field, None)

    @staticmethod
    def fix_stanford_place_issue(data_dict: Dict) -> None:
        """Fix specific place issues related to Stanford."""
        spatial = data_dict.get("dct_spatial_sm", [])
        if "Wisconsin" in spatial and "United States" in spatial:
            data_dict["dct_spatial_sm"] = ["United States"]

    @staticmethod
    def string2array(data_dict: Dict) -> Dict:
        """Convert certain string fields to array if they should be lists."""
        for key in data_dict.keys():
            suffix = key.split("_")[-1]
            if suffix in ["sm", "im"] and not isinstance(data_dict[key], list):
                data_dict[key] = [data_dict[key]]
        return data_dict


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update metadata schema from GBL 1.0 to Aardvark.")
    parser.add_argument("dir_old_schema", type=Path, help="Directory of JSON files in the old schema")
    parser.add_argument("dir_new_schema", type=Path, help="Directory for the new schema JSON files")

    args = parser.parse_args()

    LoggerConfig.configure_logging("gbl-1_to_aardvark/log/gbl-1_to_aardvark.log")
    schema_updater = SchemaUpdater()
    schema_updater.update_all_schemas(args.dir_old_schema, args.dir_new_schema)
