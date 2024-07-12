import json
import csv
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import argparse


class LoggerConfig:
    @staticmethod
    def configure_logging(logfile: str) -> None:
        os.makedirs(os.path.dirname(logfile), exist_ok=True)
        logging.basicConfig(
            filename=logfile,
            filemode="a",
            level=logging.ERROR,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )


class SchemaUpdater:
    def __init__(
        self,
        overwrite_values: Optional[Dict[str, str]] = None,
        resource_class_default: str = None,
        resource_type_default: str = None,
        place_default: Optional[str] = None,
    ):
        self.RESOURCE_CLASS_DEFAULT = resource_class_default
        self.RESOURCE_TYPE_DEFAULT = resource_type_default
        self.PLACE_DEFAULT = place_default
        try:
            self.crosswalk = self.load_crosswalk(self.CROSSWALK_PATH)
        except Exception as e:
            logging.critical(f"Failed to load crosswalk: {e}")
            self.crosswalk = {}
        self.overwrite_values = overwrite_values if overwrite_values else {}

    CROSSWALK_PATH = Path("lib/opendataharvest/gbl-1_to_aardvark/crosswalk.csv")

    @staticmethod
    def load_crosswalk(crosswalk_path: Path) -> Dict[str, str]:
        """Load crosswalk CSV into a dictionary."""
        crosswalk = {}
        try:
            with open(crosswalk_path, encoding="utf8") as f:
                reader = csv.reader(f)
                next(reader)  # Skip header
                for old, new in reader:
                    crosswalk[old] = new
        except FileNotFoundError:
            logging.critical(f"Crosswalk file not found: {crosswalk_path}")
        except Exception as e:
            logging.critical(f"Error loading crosswalk: {e}")
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

            # Handle class and type
            (
                data["gbl_resourceClass_sm"],
                data["gbl_resourceType_sm"],
            ) = self.determine_resource_class_and_type(data)

            # Overwrite specified values
            for key, value in self.overwrite_values.items():
                data[key] = value

            # Run all the static functions:
            data = self.string2array(data)
            self.check_required(data)
            self.add_restricted_display_notes(data)
            self.fix_stanford_place_issue(data)
            self.fix_wisco_provider_issue(data)
            self.remove_deprecated(data)

            new_filepath = dir_new_schema / (
                filepath.name
                if filepath.name != "geoblacklight.json"
                else f"{data['id']}.json"
            )
            with open(new_filepath, "w", encoding="utf8") as fw:
                json.dump(data, fw, indent=2)
        except FileNotFoundError:
            logging.error(f"File not found: {filepath}")
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON in file: {filepath}")
        except Exception as e:
            logging.error(f"Failed to update schema for {filepath.name}: {e}")

    @staticmethod
    def add_restricted_display_notes(data_dict: Dict) -> None:
        """Add a restricted display note if dct_accessRights_s is 'Restricted'."""
        if data_dict.get("dct_accessRights_s") == "Restricted":
            note = "Warning: This dataset is restricted and you may not be able to access the resource. Contact the dataset provider or the AGSL for assistance."
            display_notes = data_dict.get("gbl_displayNote_sm")

            if display_notes is None:
                data_dict["gbl_displayNote_sm"] = [note]
            elif isinstance(display_notes, list):
                if note not in display_notes:
                    display_notes.append(note)
            else:
                data_dict["gbl_displayNote_sm"] = [display_notes, note]

    def check_required(self, data_dict: Dict) -> None:
        """Check for required fields and handle missing ones."""
        requirements = [
            "dct_publisher_sm",
            "dct_spatial_sm",
            "gbl_mdVersion_s",
            "dct_title_s",
            "id",
            "gbl_mdModified_dt",
            "gbl_resourceClass_sm",
        ]

        for req in requirements:
            value = data_dict.get(req)
            if not value or (isinstance(value, list) and not any(value)):
                logging.warning(
                    f"Requirement {req} is either missing or contains empty values..."
                )
                self.handle_missing_field(data_dict, req)

    def handle_missing_field(self, data_dict: Dict, field: str) -> None:
        """Handle missing required fields with default values or logic."""
        assert field not in ["gbl_mdVersion_s", "gbl_resourceClass_sm", "id"]

        if field == "dct_spatial_sm":
            data_dict["dct_spatial_sm"] = (
                [self.PLACE_DEFAULT] if self.PLACE_DEFAULT else []
            )
        elif field == "gbl_mdModified_dt":
            data_dict["gbl_mdModified_dt"] = datetime.now(datetime.UTC).strftime(
                f"%Y-%m-%dT%H:%M:%SZ"
            )
        elif field == "dct_publisher_sm":
            data_dict["dct_publisher_sm"] = data_dict.get("dct_creator_sm", [])

    @staticmethod
    def determine_resource_class_and_type(
        data_dict: Dict,
    ) -> Tuple[List[str], List[str]]:
        """Determine the resource class based on the data dictionary."""

        def append_if_not_exists(lst, item):
            if item not in lst:
                lst.append(item)

        logging.debug("Determining resource class and type for data: %s", data_dict)

        # Assign the main return variables if they exist already, if both exist, return them as is.
        gbl_resourceClass_sm = data_dict.get("gbl_resourceClass_sm") or []
        gbl_resourceType_sm = data_dict.get("gbl_resourceType_sm") or []

        logging.debug("Initial resource class: %s", gbl_resourceClass_sm)
        logging.debug("Initial resource type: %s", gbl_resourceType_sm)

        if gbl_resourceClass_sm and gbl_resourceType_sm:
            logging.debug("Resource class and type already determined.")
            return gbl_resourceClass_sm, gbl_resourceType_sm

        # Grab some more info as text right away
        dct_title_s = str(data_dict.get("dct_title_s", ""))
        dct_format_s = str(data_dict.get("dct_format_s", ""))
        dct_description_sm = str(data_dict.get("dct_description_sm", ""))
        dct_subject_sm = str(data_dict.get("dct_subject_sm", ""))
        dct_publisher_sm = str(data_dict.get("dct_publisher_sm", ""))
        id = str(data_dict.get("id", ""))
        dct_references_s = str(data_dict.get("dct_references_s", ""))
        dct_source_sm = str(data_dict.get("dct_source_sm", ""))

        # Open Index Maps
        if (
            ("openindexmaps" in dct_references_s.lower())
            or (id == "stanford-ch237ht4777")
            or ("ch237ht4777" in dct_source_sm.lower())
        ):
            logging.debug("OpenIndexMap detected, setting resource class and type.")
            gbl_resourceClass_sm = ["Maps"]
            gbl_resourceType_sm = ["Index maps"]
            if "aerial" in dct_description_sm.lower():
                gbl_resourceClass_sm = ["Imagery"]
            return gbl_resourceClass_sm, gbl_resourceType_sm

        logging.info("Class and Type determination using keywords...")
        logging.debug("id: %s", id)
        logging.debug("Title: %s", dct_title_s)
        logging.debug("Format: %s", dct_format_s)
        logging.debug("Description: %s", dct_description_sm)
        logging.debug("Subject: %s", dct_subject_sm)
        logging.debug("Publisher: %s", dct_publisher_sm)
        logging.debug("References: %s", dct_references_s)

        if "aerial photo" in dct_title_s.lower():
            logging.debug("Aerial photogrpahy detected")
            gbl_resourceType_sm = ["Aerial photographs"]
            gbl_resourceClass_sm = ["Imagery"]
            return gbl_resourceClass_sm, gbl_resourceType_sm

        if "sanborn" in dct_publisher_sm.lower():
            logging.debug("Sanborn map detected, setting resource class and type.")
            append_if_not_exists(gbl_resourceClass_sm, "Maps")
            append_if_not_exists(gbl_resourceType_sm, "Fire insurance maps")
            return gbl_resourceClass_sm, gbl_resourceType_sm

        if "topographical map" in dct_title_s.lower():
            logging.debug(
                "topographical map detected, setting resource class and type."
            )
            append_if_not_exists(gbl_resourceClass_sm, "Maps")
            append_if_not_exists(gbl_resourceType_sm, "Topographic maps")
            return gbl_resourceClass_sm, gbl_resourceType_sm

        if "aeronautical" in dct_title_s.lower():
            logging.debug(
                "Aeronautical charts detected, setting resource class and type."
            )
            append_if_not_exists(gbl_resourceClass_sm, "Maps")
            append_if_not_exists(gbl_resourceType_sm, "Aeronautical charts")
            return gbl_resourceClass_sm, gbl_resourceType_sm

        if "iiif" in dct_references_s.lower():
            logging.debug("IIIF Map detected, setting resource class and type.")
            append_if_not_exists(gbl_resourceClass_sm, "Maps")
            if ("aerial photo" in dct_title_s.lower()) or (
                "aerial photo" in dct_description_sm.lower()
            ):
                logging.debug("IIIF Aerial Photography Detected")
                append_if_not_exists(gbl_resourceType_sm, "Aerial photographs")
            else:
                logging.debug("IIIF Map Detected")
                append_if_not_exists(gbl_resourceType_sm, "Digital maps")
            return gbl_resourceClass_sm, gbl_resourceType_sm

        if dct_format_s in ["GeoTIFF", "TIFF"]:
            if (
                "relief" in dct_description_sm.lower()
                or "map" in dct_description_sm.lower()
                or "maps" in dct_subject_sm.lower()
                or "plan" in dct_title_s.lower()
                or "map" in dct_title_s.lower()
                or "topographic" in dct_title_s.lower()
            ):
                logging.debug(
                    "GeoTIFF or TIFF format with map-related description or subject detected."
                )
                append_if_not_exists(gbl_resourceClass_sm, "Maps")
                append_if_not_exists(gbl_resourceType_sm, "Digital maps")
                return gbl_resourceClass_sm, gbl_resourceType_sm

            if "aerial photo" in dct_title_s.lower():
                logging.debug("Aerial Photogrpahy Detected")
                append_if_not_exists(gbl_resourceType_sm, "Aerial photographs")
                gbl_resourceClass_sm = ["Imagery"]

            logging.debug(
                "GeoTIFF or TIFF format detected, setting resource class to Datasets."
            )
            gbl_resourceClass_sm = ["Datasets"]
            return gbl_resourceClass_sm, gbl_resourceType_sm

        if (
            dct_format_s
            in [
                "Shapefile",
                "ArcGrid",
                "GeoDatabase",
                "Geodatabase",
                "Arc/Info Binary Grid",
            ]
            or "csdgm" in dct_references_s
            or "ArcGIS#" in dct_references_s
        ):
            logging.debug("Setting resource class to Datasets based on format.")
            gbl_resourceClass_sm = ["Datasets"]
            if "aerial photo" in dct_title_s.lower():
                logging.debug("Aerial photogrpahy detected")
                gbl_resourceType_sm = ["Aerial photographs"]
                gbl_resourceClass_sm = ["Imagery"]
            return gbl_resourceClass_sm, gbl_resourceType_sm

        if dct_format_s == "":
            if (
                "relief" in dct_description_sm.lower()
                or "map" in dct_description_sm.lower()
                or "maps" in dct_subject_sm.lower()
            ):
                logging.debug(
                    "Empty format with map-related description or subject detected."
                )
                append_if_not_exists(gbl_resourceClass_sm, "Maps")
                return gbl_resourceClass_sm, gbl_resourceType_sm
            else:
                logging.debug("Empty format, setting resource class to Other.")
                gbl_resourceClass_sm = ["Other"]
                return gbl_resourceClass_sm, gbl_resourceType_sm

        if (
            dct_format_s == "ArcGRID"
            or dct_format_s == "IMG"
            or "DEM" in dct_description_sm
            or "DSM" in dct_description_sm
            or "digital elevation model" in dct_description_sm
            or "digital terrain model" in dct_description_sm
            or "digital surface model" in dct_description_sm
            or "arc-second" in dct_description_sm
            or "raster dataset" in dct_description_sm.lower()
        ):
            logging.debug("Elevation or other non-Imagery Raster Detected.")
            append_if_not_exists(gbl_resourceClass_sm, "Datasets")
            append_if_not_exists(gbl_resourceType_sm, "Raster data")
            return gbl_resourceClass_sm, gbl_resourceType_sm

        if (
            "relief" in dct_description_sm.lower()
            or "map" in dct_description_sm.lower()
            or "maps" in dct_subject_sm.lower()
        ):
            logging.debug("Map-related description or subject detected.")
            gbl_resourceClass_sm = ["Maps"]
            return gbl_resourceClass_sm, gbl_resourceType_sm

        # If all else fails:
        logging.debug("Setting default resource class and type.")
        gbl_resourceClass_sm = [SchemaUpdater.RESOURCE_CLASS_DEFAULT]
        gbl_resourceType_sm = [SchemaUpdater.RESOURCE_TYPE_DEFAULT]
        return gbl_resourceClass_sm, gbl_resourceType_sm

    def handle_class_and_type(self, data_dict: Dict) -> None:
        (
            data_dict["gbl_resourceClass_sm"],
            data_dict["gbl_resourceType_sm"],
        ) = self.determine_resource_class_and_type(data_dict)

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
            "stanford_rights_metadata_s",
            "stanford_use_and_reproduction_s",
            "stanford_copyright_s",
        ]
        for field in deprecated_fields:
            if field in data_dict:
                logging.debug(f"Removing deprecated field: {field}")
                data_dict.pop(field, None)

    @staticmethod
    def fix_stanford_place_issue(data_dict: Dict) -> None:
        """Fix specific place issues related to Stanford."""
        spatial = data_dict.get("dct_spatial_sm", [])
        if "Wisconsin" in spatial and "New Mexico" in spatial:
            data_dict["dct_spatial_sm"] = ["United States"]

    @staticmethod
    def fix_wisco_provider_issue(data_dict: Dict) -> None:
        """Fix specific provider issues related to edu.wisc."""
        provider = data_dict.get("schema_provider_s", "")
        wisco_providers = [
            "UW-Madison Robinson Map Library",
            "WisconsinView",
            "UW Digital Collections Center",
            "Wisconsin State Cartographer's Office",
        ]
        if provider in wisco_providers:
            logging.debug(f"Wisco provider identified: {provider}")
            data_dict["schema_provider_s"] = ["University of Wisconsin-Madison"]
            data_dict["dct_description_sm"].insert(
                0, f"Resource provided by {provider}."
            )
            description = str(data_dict["dct_description_sm"])
            logging.debug(f"Wisco Description now reads: {description}")

    @staticmethod
    def string2array(data_dict: Dict) -> Dict:
        """Convert certain string fields to array if they should be lists."""
        for key in data_dict.keys():
            suffix = key.split("_")[-1]
            if suffix in ["sm", "im"] and not isinstance(data_dict[key], list):
                data_dict[key] = [data_dict[key]]
        return data_dict


if __name__ == "__main__":
    # fmt: off
    parser = argparse.ArgumentParser(description="Update metadata schema from GBL 1.0 to Aardvark.")
    parser.add_argument("dir_old_schema", type=Path, help="Directory of JSON files in the old schema")
    parser.add_argument("dir_new_schema", type=Path, help="Directory for the new schema JSON files")

    # Optional arguments for overwriting values
    parser.add_argument("--dct_publisher_sm", type=str, help="Overwrite dct_publisher_sm")
    parser.add_argument("--dct_spatial_sm", type=str, help="Overwrite dct_spatial_sm")
    parser.add_argument("--gbl_resourceClass_sm", type=str, help="Overwrite gbl_resourceClass_sm")
    parser.add_argument("--gbl_resourceType_sm", type=str, help="Overwrite gbl_resourceType_sm")
    parser.add_argument("--id", type=str, help="Overwrite id")
    parser.add_argument("--gbl_mdModified_dt", type=str, help="Overwrite gbl_mdModified_dt")
    parser.add_argument("--schema_provider_s", type=str, help="Overwrite schema_provider_s")
    parser.add_argument("--gbl_displayNote_sm", type=str, help="Overwrite gbl_displayNote_sm")

    # Optional arguments for setting default values
    parser.add_argument("--resource_class_default", type=str, help="Set default value for resource class")
    parser.add_argument("--resource_type_default", type=str, help="Set default value for resource type")
    parser.add_argument("--place_default", type=str, help="Set default value for place")

    args = parser.parse_args()

    LoggerConfig.configure_logging("log/gbl-1_to_aardvark.log")

    logging.debug(f"Parsed Arguments: {vars(args)}")

    overwrite_values = {
        k: v
        for k, v in vars(args).items()
        if v is not None and k not in ["dir_old_schema", "dir_new_schema", "resource_class_default", "place_default"]
    }

    logging.info(f"Initializing SchemaUpdater with PLACE_DEFAULT: {args.place_default}")

    schema_updater = SchemaUpdater(overwrite_values, args.resource_class_default, args.resource_type_default, args.place_default)
    schema_updater.update_all_schemas(args.dir_old_schema, args.dir_new_schema)
