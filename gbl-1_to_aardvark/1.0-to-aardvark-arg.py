import json
import csv
import os
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List
import argparse

logfile = f"gbl-1_to_aardvark/log/gbl-1_to_aardvark.log"

# Configure the logging module
logging.basicConfig(
    filename=logfile,
    filemode="a",
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# Default values
RESOURCE_CLASS_DEFAULT = "Other"
PLACE_DEFAULT = None


def load_crosswalk(crosswalk_path: Path) -> Dict[str, str]:
    crosswalk = {}
    with open(crosswalk_path, encoding="utf8") as f:
        reader = csv.reader(f)
        fields = next(reader)
        for record in reader:
            old, new = record[0], record[1]
            crosswalk[old] = new
    return crosswalk


crosswalk = load_crosswalk(
    Path(r"/home/srappel/GeoDiscovery-Utils/gbl-1_to_aardvark/crosswalk.csv")
)


def string2array(data_dict: Dict) -> Dict:
    for key in data_dict.keys():
        suffix = key.split("_")[-1]
        if suffix in ["sm", "im"] and not isinstance(data_dict[key], list):
            data_dict[key] = [data_dict[key]]
    return data_dict


def check_required(data_dict: Dict) -> None:
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
            handle_missing_field(data_dict, req)


def handle_missing_field(data_dict: Dict, field: str) -> None:
    if field == "gbl_resourceClass_sm":
        if "dct_format_s" in data_dict:
            format = data_dict["dct_format_s"]
            if format in ["Shapefile", "ArcGrid", "GeoDatabase"]:
                data_dict["gbl_resourceClass_sm"] = "Datasets"
            elif format == "GeoTIFF":
                if "georeferenced" in data_dict.get("dct_description_sm", ""):
                    data_dict["gbl_resourceClass_sm"] = "Maps"
                else:
                    data_dict["gbl_resourceClass_sm"] = "Datasets"
        else:
            data_dict["gbl_resourceClass_sm"] = [RESOURCE_CLASS_DEFAULT]
    elif field == "dct_spatial_sm":
        if PLACE_DEFAULT:
            data_dict["dct_spatial_sm"] = [PLACE_DEFAULT]
            logging.info(
                f"Replaced dct_spatial_sm:Null with dct_spatial_sm:{PLACE_DEFAULT}"
            )
        else:
            logging.warning(f"There is no dct_spatial_sm")
    elif field == "gbl_mdModified_dt":
        data_dict["gbl_mdModified_dt"] = datetime.utcnow().strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    elif field == "dct_publisher_sm":
        if "dct_creator_sm" in data_dict and data_dict["dct_creator_sm"]:
            data_dict["dct_publisher_sm"] = data_dict["dct_creator_sm"]
            logging.info(
                f"Replaced dct_publisher_sm:Null with dct_publisher_sm:{data_dict['dct_creator_sm']}"
            )
        else:
            logging.warning(f"No Publisher or Creator information found.")
    elif field == "gbl_resourceType_sm":
        if "dct_source_sm" in data_dict:
            if (
                "stanford-ch237ht4777" in data_dict["dct_source_sm"]
                or data_dict["id"] == "stanford-ch237ht4777"
            ):
                data_dict["gbl_resourceType_sm"] = "Index maps"
                data_dict["gbl_resourceClass_sm"] = "Maps"


def remove_deprecated(data_dict: Dict) -> None:
    deprecated = [
        "dc_type_s",
        "layer_geom_type_s",
        "dct_isPartOf_sm",
        "uw_supplemental_s",
        "uw_notice_s",
        "uuid",
    ]
    for field in deprecated:
        if field in data_dict:
            data_dict.pop(field)
            logging.info(f"Removed the deprecated {field} field.")


def stanford_place(data_dict: Dict) -> None:
    spatial = data_dict.get("dct_spatial_sm", [])
    if len(spatial) > 1 and "Wisconsin" in spatial and "United States" in spatial:
        logging.info("Fixing the Wisconsin issue in US coverage")
        data_dict["dct_spatial_sm"] = ["United States"]


def schema_update(filepath: Path, dir_new_schema: Path) -> None:
    try:
        with open(filepath, encoding="utf8") as fr:
            data = json.load(fr)

        if not isinstance(data, dict):
            return

        for old_schema, new_schema in crosswalk.items():
            if old_schema in data:
                data[new_schema] = data.pop(old_schema)

        data["gbl_mdVersion_s"] = "Aardvark"

        data.pop("geoblacklight_version", None)

        check_required(data)
        remove_deprecated(data)
        if "dct_spatial_sm" in data:
            stanford_place(data)

        data = string2array(data)

        filepath_updated = (
            dir_new_schema / filepath.name
            if filepath.name != "geoblacklight.json"
            else dir_new_schema / f"{data['id']}.json"
        )
        with open(filepath_updated, "w") as fw:
            json.dump(data, fw, indent=2)
    except Exception as e:
        logging.error(f"Failed to update schema for {filepath.name}: {e}")


def list_all_json(rootdir: Path) -> List[Path]:
    rootdir = Path(rootdir)
    return [
        path for path in sorted(rootdir.rglob("*.json")) if path.name != "layers.json"
    ]


def main_function(dir_old_schema: Path, dir_new_schema: Path) -> None:
    if not dir_new_schema.exists():
        dir_new_schema.mkdir()

    files = list_all_json(dir_old_schema)
    for file in files:
        logging.info(f"Executing {file} ...")
        schema_update(file, dir_new_schema)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update metadata schema from GBL 1.0 to Aardvark."
    )
    parser.add_argument(
        "dir_old_schema", type=Path, help="Directory of JSON files in the old schema"
    )
    parser.add_argument(
        "dir_new_schema", type=Path, help="Directory for the new schema JSON files"
    )

    args = parser.parse_args()

    main_function(args.dir_old_schema, args.dir_new_schema)
