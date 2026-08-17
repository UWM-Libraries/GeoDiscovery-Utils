import json
import csv
import os
import logging
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from typing import Dict, List, Optional, Tuple
import argparse
import yaml
from classify import ResourceClassifier
from normalize import MetadataNormalizer

CONFIG_DIR = Path(__file__).resolve().parent

# Load configuration from YAML file
with open(CONFIG_DIR / "config.yaml", "r") as file:
    config = yaml.safe_load(file)


class LoggerConfig:
    @staticmethod
    def configure_logging() -> None:
        logfile = config["logging"]["logfile"]
        if not os.path.isabs(logfile):
            logfile = str((CONFIG_DIR / logfile).resolve())
        level = getattr(logging, config["logging"]["level"].upper(), logging.ERROR)
        os.makedirs(os.path.dirname(logfile), exist_ok=True)
        logging.basicConfig(
            filename=logfile,
            filemode="a",
            level=level,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )


def write_json_atomically(path: Path, data: Dict) -> None:
    """Write JSON without leaving empty or truncated destination files."""
    path.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf8",
        dir=path.parent,
        delete=False,
    ) as tmp_file:
        tmp_path = Path(tmp_file.name)
        json.dump(data, tmp_file, indent=2)
        tmp_file.write("\n")
        tmp_file.flush()
        os.fsync(tmp_file.fileno())

    try:
        with open(tmp_path, encoding="utf8") as check_file:
            json.load(check_file)
        if tmp_path.stat().st_size == 0:
            raise ValueError(f"Refusing to replace {path} with an empty JSON file.")
        os.replace(tmp_path, path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


class SchemaUpdater:
    CROSSWALK_PATH = (CONFIG_DIR / config["paths"]["crosswalk"]).resolve()

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
        for file in self.list_all_json_files(dir_old_schema):
            logging.info(f"Processing {file} ...")
            self.update_schema(file, dir_new_schema)

    @staticmethod
    def list_all_json_files(rootdir: Path):
        """Yield JSON files lazily so large trees do not get materialized in memory."""
        for path in rootdir.rglob("*.json"):
            if path.name != "layers.json":
                yield path

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

            # Run normalization and cleanup after conversion.
            data = self.string2array(data)
            self.check_required(data)
            self.apply_normalizations(data)
            self.remove_deprecated(data)

            new_filepath = dir_new_schema / (
                filepath.name
                if filepath.name != "geoblacklight.json"
                else f"{data['id']}.json"
            )
            write_json_atomically(new_filepath, data)
        except FileNotFoundError:
            logging.error(f"File not found: {filepath}")
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON in file: {filepath}")
        except Exception as e:
            logging.error(f"Failed to update schema for {filepath.name}: {e}")

    def check_required(self, data_dict: Dict) -> None:
        """Check for required fields and handle missing ones."""
        requirements = config["requirements"]["check_required"]
        record_id = data_dict.get("id", "<missing id>")

        for req in requirements:
            value = data_dict.get(req)
            if not value or (isinstance(value, list) and not any(value)):
                # Missing fields are common in legacy metadata and are often repairable here.
                logging.info(
                    f"Record {record_id}: requirement {req} is either missing or contains empty values."
                )
                self.handle_missing_field(data_dict, req)
                repaired = data_dict.get(req)
                if not repaired or (isinstance(repaired, list) and not any(repaired)):
                    logging.warning(
                        f"Record {record_id}: requirement {req} is still missing after normalization."
                    )

    def handle_missing_field(self, data_dict: Dict, field: str) -> None:
        """Handle missing required fields with default values or logic."""
        assert field not in ["gbl_mdVersion_s", "gbl_resourceClass_sm", "id"]

        if field == "dct_spatial_sm":
            data_dict["dct_spatial_sm"] = (
                [self.PLACE_DEFAULT] if self.PLACE_DEFAULT else []
            )
        elif field == "gbl_mdModified_dt":
            data_dict["gbl_mdModified_dt"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        elif field == "dct_publisher_sm":
            data_dict["dct_publisher_sm"] = data_dict.get("dct_creator_sm", [])

    def determine_resource_class_and_type(
        self,
        data_dict: Dict,
    ) -> Tuple[List[str], List[str]]:
        """Determine the resource class based on the data dictionary."""
        return ResourceClassifier.determine_resource_class_and_type(
            data_dict,
            resource_class_default=self.RESOURCE_CLASS_DEFAULT,
            resource_type_default=self.RESOURCE_TYPE_DEFAULT,
        )

    def handle_class_and_type(self, data_dict: Dict) -> None:
        (
            data_dict["gbl_resourceClass_sm"],
            data_dict["gbl_resourceType_sm"],
        ) = self.determine_resource_class_and_type(data_dict)

    def apply_normalizations(self, data_dict: Dict) -> None:
        MetadataNormalizer.normalize_document(data_dict)

    def remove_deprecated(self, data_dict: Dict) -> None:
        """Remove deprecated fields from the data dictionary."""
        deprecated_fields = config["deprecated_fields"]["remove_deprecated"]
        for field in deprecated_fields:
            if field in data_dict:
                logging.debug(f"Removing deprecated field: {field}")
                data_dict.pop(field, None)

    def string2array(self, data_dict: Dict) -> Dict:
        """Convert certain string fields to array if they should be lists."""
        for key in data_dict.keys():
            suffix = key.split("_")[-1]
            if suffix in config["string2array_suffixes"] and not isinstance(
                data_dict[key], list
            ):
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

    LoggerConfig.configure_logging()

    logging.debug(f"Parsed Arguments: {vars(args)}")

    overwrite_values = {
        k: v
        for k, v in vars(args).items()
        if v is not None and k not in ["dir_old_schema", "dir_new_schema", "resource_class_default", "place_default"]
    }

    logging.debug(f"Initializing SchemaUpdater with PLACE_DEFAULT: {args.place_default}")

    schema_updater = SchemaUpdater(
        overwrite_values,
        args.resource_class_default,
        args.resource_type_default,
        args.place_default,
    )
    schema_updater.update_all_schemas(args.dir_old_schema, args.dir_new_schema)
    logging.info(f"Conversion complete for {args.dir_old_schema}")
