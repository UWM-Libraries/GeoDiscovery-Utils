"""
DCAT_Harvester.py
Author: Stephen Appel
Created: May 14, 2024
Version: 0.1
Dependencies: requests, yaml, and dateutil are not part of the standard library.
Credit: UW-Madison - State Cartographer's Office for some code. Some code refactored and edited by CoPilot.
Description: This script is used to harvest open data from data portals who
expose a DCAT JSON. It reads configuration options from a YAML file, including
output directory, default bounding box, which portals to scan (catalog), maximum
retry attempts, and sleep time for requests.
A Site object is created for each website in the defined catalog. Datasets not
in the skip list for the Site will be looped over and a JSON File generated for
each. The Aardvark class is dictionary-like and defines the structure of a
single dataset description. We dump the Aardvark object to JSON when
crosswalking is complete and write it to a file. A timestamped log file is
created on each run and contains verbose output for debugging and for maintaining
the config.yaml file such as datasets to add to the skip list, etc.
Code is formatted according to PEP8 using Black.
Care is taken to use functionality from the Python standard library.
AI was utilized in authoring this script.
"""

import csv
import json
import logging
import os
import re
import shutil
import sys
import time
import uuid
import html
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from typing import List

import requests
import yaml
from dateutil import parser
import jsonschema
from jsonschema import validate

CONFIG_DIR = Path(__file__).resolve().parent
config_file = CONFIG_DIR / "config.yaml"

try:
    with open(config_file, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
except FileNotFoundError:
    print(f"Config file {config_file} not found")
    sys.exit()

try:
    CONFIG = config.get("CONFIG")
    env_ogm_path = os.getenv("OGM_PATH")
    if os.getenv("RAILS_ENV") == "production" and not env_ogm_path:
        raise ValueError("OGM_PATH must be set in production")
    OGM_PATH = (
        Path(env_ogm_path)
        if env_ogm_path
        else (CONFIG_DIR / config["paths"]["ogm_path"]).resolve()
    )
    outputdir_config = Path(CONFIG.get("OUTPUTDIR", "opendataharvest"))
    OUTPUTDIR = (
        outputdir_config
        if outputdir_config.is_absolute()
        else OGM_PATH / outputdir_config
    )
    COLLECTION_RECORD = (CONFIG_DIR / CONFIG.get("COLLECTION_RECORD")).resolve()
    LOGFILE = Path(config["logging"]["logfile"])
    if not LOGFILE.is_absolute():
        LOGFILE = (CONFIG_DIR / LOGFILE).resolve()
    LOGLEVEL = getattr(logging, config["logging"]["level"].upper(), logging.ERROR)
    DEFAULTBBOX = (CONFIG_DIR / CONFIG.get("DEFAULTBBOX")).resolve()
    CATALOG_KEY = CONFIG.get("CATALOG", "TestSites")
    CATALOG = config.get(CATALOG_KEY, None)
    MAXRETRY = CONFIG.get("MAXRETRY", 5)
    SLEEPTIME = CONFIG.get("SLEEPTIME", 1)

    # Default Values
    default_config = config.get("DEFAULT", {})
    MEMBEROF = default_config.get("MEMBEROF", [])
    RESOURCECLASS = default_config.get("RESOURCECLASS", [])
    ACCESSRIGHTS = default_config.get("ACCESSRIGHTS")  # This is a single string value
    MDVERSION = default_config.get("MDVERSION")  # This is a single string value
    LANG = default_config.get("LANG", [])
    PROVIDER = default_config.get("PROVIDER")  # This is a single string value
    SUPPRESSED = default_config.get("SUPPRESSED")  # This is a boolean value
    RIGHTS = default_config.get("RIGHTS", [])
    RESOURCETYPE = default_config.get("RESOURCETYPE", [])
    FORMAT = default_config.get("FORMAT")
    DESCRIPTION = default_config.get("DESCRIPTION")
    DISPLAYNOTE = default_config.get("DISPLAYNOTE")

    ## Get the JSON schema:
    SCHEMA = CONFIG.get("SCHEMA")

except (AttributeError, ValueError) as e:
    print(f"Unable to read all configuration values from {config_file}")
    print(e)
    sys.exit()

# Configure the logging module
logging.basicConfig(
    filename=str(LOGFILE), filemode="a", level=LOGLEVEL, format="%(message)s"
)

dt = datetime.now().strftime(r"%Y-%m-%d %H:%M:%S")
logging.info(f"DCAT harvest started at {dt}")


def contains_unresolved_template(value) -> bool:
    """Return whether a string contains an unresolved ArcGIS template value."""
    return isinstance(value, str) and re.search(r"\{\{[^{}]+\}\}", value) is not None


def ensure_collection_record(output_dir: Path):
    """Copy the committed collection-level record into the harvest output."""
    if not COLLECTION_RECORD.is_file():
        logging.warning(f"Collection record not found at {COLLECTION_RECORD}")
        return

    destination = output_dir / COLLECTION_RECORD.name
    try:
        shutil.copy2(COLLECTION_RECORD, destination)
    except Exception as e:
        logging.warning(f"Unable to copy collection record to {destination}: {e}")


def clear_output_directory(output_dir: Path):
    """Remove old JSON harvest output so each run produces a clean set."""
    for path in output_dir.glob("*.json"):
        try:
            path.unlink()
        except Exception as e:
            logging.warning(f"Unable to remove {path}: {e}")


class Site:
    """
    A class to represent a Site.

    Attributes
    ----------
    site_name : str
        The name of the site.
    site_details : dict
        The details of the site.
    site_json : dict
        The JSON data of the site.
    site_skiplist : set
        The set of UUIDs to skip.
    site_applist : set
        The set of UUIDs for applications.

    Methods
    -------
    __getitem__(self, key):
        Gets the attribute of the object using the key.
    __setitem__(self, key, value):
        Sets the attribute of the object using the key and value.
    """

    def __init__(
        self,
        site_name: str,
        site_details: dict,
        site_json: dict,
        site_skiplist: list,
        site_applist: list,
        site_maplist: list,
    ):
        """
        Constructs all the necessary attributes for the Site object.

        Parameters
        ----------
            site_name : str
                The name of the site.
            site_details : dict
                The details of the site.
            site_json : dict
                The JSON data of the site.
            site_skiplist : list
                The list of UUIDs to skip.
            site_applist : list
                The list of UUIDs for applications.
        """
        self.site_name = site_name
        self.site_details = site_details
        self.site_json = site_json
        self.site_skiplist = set(site_skiplist)
        self.site_applist = set(site_applist)
        self.site_maplist = set(site_maplist)

    def __getitem__(self, key):
        """
        Gets the attribute of the object using the key.

        Parameters
        ----------
            key : str
                The key to the attribute.

        Returns
        -------
            The value of the attribute.
        """
        return getattr(self, key)

    def __setitem__(self, key, value):
        """
        Sets the attribute of the object using the key and value.

        Parameters
        ----------
            key : str
                The key to the attribute.
            value : str
                The value to set the attribute to.
        """
        setattr(self, key, value)


def get_site_data(site: str, details: dict) -> dict:
    """Fetch the site data with retries."""
    for i in range(MAXRETRY):
        try:
            response = requests.get(details["SiteURL"], timeout=3)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.MissingSchema:
            logging.info(f"Trying SiteURL for {site} as a local filepath.")
            response_file = json.load(open(Path(details["SiteURL"]), "r"))
            return response_file
        except json.JSONDecodeError:
            logging.warning(f"The content from {site} is not a valid JSON document.")
            return None
        except (requests.HTTPError, requests.exceptions.Timeout) as e:
            logging.debug(
                f"Received bad response from {site}. Retrying after {SLEEPTIME} seconds."
            )
            time.sleep(SLEEPTIME)
            if i == (MAXRETRY - 1):
                logging.warning(
                    f"Failed to connect to {site} after {MAXRETRY + 1} attempts."
                )
                logging.warning(str(e))
                return None


def get_uuid_list(details: dict, key: str) -> List[str]:
    """Extract UUIDs from details."""
    uuid_list = []
    if key in details:
        for item in details[key]:
            uuid_list.append(item["UUID"])
    return uuid_list


def harvest_sites() -> list:
    """Main function to harvest sites."""
    site_list = []
    for site, details in CATALOG.items():
        site_json = get_site_data(site, details)
        if site_json is None:
            continue
        site_skiplist = get_uuid_list(details, "SkipList")
        site_applist = get_uuid_list(details, "AppList")
        site_maplist = get_uuid_list(details, "MapList")
        current_Site = Site(
            details["SiteName"],
            details,
            site_json,
            site_skiplist,
            site_applist,
            site_maplist,
        )
        site_list.append(current_Site)
    return site_list


class AardvarkDataProcessor:
    @staticmethod
    def extract_data(dataset_dict):
        # Extract data from dataset_dict
        identifier = dataset_dict["identifier"]
        title = dataset_dict.get("title")
        if not title or contains_unresolved_template(title):
            logging.warning(
                f'Assigned "Untitled Dataset" to {identifier}: '
                f"title is missing or unresolved ({title!r})."
            )
            title = "Untitled Dataset"
        description = re.sub("<[^<]+?>", "", dataset_dict.get("description", []))
        publisher = (
            dataset_dict.get("publisher", {})
            if isinstance(dataset_dict.get("publisher"), dict)
            else {}
        )
        publisher_name = publisher.get("name")
        if contains_unresolved_template(publisher_name):
            publisher = {}
            publisher_name = None
        creator = [publisher_name] if publisher_name else []
        issued = dataset_dict.get("issued", "")
        modified = dataset_dict.get("modified", "")
        keyword = dataset_dict.get("keyword", [])
        spatial = dataset_dict.get("spatial", None)
        distribution = dataset_dict.get("distribution", None)
        landingPage = dataset_dict.get("landingPage", "")

        return {
            "title": title,
            "identifier": identifier,
            "description": description,
            "creator": creator,
            "issued": issued,
            "modified": modified,
            "keyword": keyword,
            "spatial": spatial,
            "distribution": distribution,
            "publisher": publisher,
            "landingPage": landingPage,
        }

    @staticmethod
    def extract_id_sublayer(identifier):
        id_pattern = r"id=([a-zA-Z0-9]+)"
        sublayer_pattern = r"sublayer=(\d+)"

        id_match = re.search(id_pattern, identifier)
        sublayer_match = re.search(sublayer_pattern, identifier)

        id_value = id_match.group(1) if id_match else None
        sublayer_value = sublayer_match.group(1) if sublayer_match else None

        if id_value is None:
            logging.warning(f"No id was extracted from: {identifier}")
            id_value = uuid.uuid4()
            logging.info(f"Assigned new UUID: {id_value}")

        return id_value, sublayer_value

    @staticmethod
    def default_bbox(website):
        if "DefaultBbox" not in website.site_details:
            return {
                "envelope": None,
                "west": None,
                "east": None,
                "north": None,
                "south": None,
            }

        defaultBox = website.site_details["DefaultBbox"]
        with open(DEFAULTBBOX) as default_csv:
            bboxreader = csv.DictReader(default_csv)
            for row in bboxreader:
                if row["name"] == defaultBox:
                    west = row["west"]
                    east = row["east"]
                    north = row["north"]
                    south = row["south"]
                    envelope = f"ENVELOPE({west},{east},{north},{south})"
                    return {
                        "envelope": envelope,
                        "west": float(west),
                        "east": float(east),
                        "north": float(north),
                        "south": float(south),
                    }

        return {
            "envelope": None,
            "west": None,
            "east": None,
            "north": None,
            "south": None,
        }

    @staticmethod
    def process_dcat_spatial(spatial_string, defaultBbox):
        def is_in_range(value, range_min, range_max):
            return range_min <= value <= range_max

        # Extract coordinates
        pattern = r"(-?\d+\.\d+)"
        matches = re.findall(pattern, spatial_string)

        if len(matches) != 4:
            raise ValueError(f"Non-conforming spatial bounding box:\n{spatial_string}")

        # Convert to floats and validate coordinates
        coordinates = [float(coord) for coord in matches]
        longitudes = coordinates[::2]
        latitudes = coordinates[1::2]

        if not all(is_in_range(lon, -180, 180) for lon in longitudes):
            raise ValueError(
                f"Longitude coordinates must be between -180 and 180:\n{spatial_string}"
            )

        if not all(is_in_range(lat, -90, 90) for lat in latitudes):
            raise ValueError(
                f"Latitude coordinates must be between -90 and 90:\n{spatial_string}"
            )

        # Ensure North is greater than South and East is greater than West
        coordinates[1], coordinates[3] = sorted(latitudes, reverse=True)
        coordinates[0], coordinates[2] = sorted(longitudes)

        # Ensure West and East OR North and South are not the same:
        if (coordinates[1] == coordinates[3]) or (coordinates[0] == coordinates[2]):
            raise ValueError(
                f"The bounding box has matching NS or EW coordinates:\n{spatial_string}"
            )

        # Check whether it is within the site's default bbox plus a 1 degree
        # buffer when a usable default bbox is configured.
        has_default_bbox = all(
            defaultBbox.get(key) is not None
            for key in ("west", "east", "north", "south")
        )
        if has_default_bbox and any(
            [
                coordinates[0] < defaultBbox["west"] - 1.0,
                coordinates[2] > defaultBbox["east"] + 1.0,
                coordinates[1] > defaultBbox["north"] + 1.0,
                coordinates[3] < defaultBbox["south"] - 1.0,
            ]
        ):
            raise ValueError(
                f"Bounding box falls outside of default bounding box:\n{spatial_string}"
            )

        # Convert to ENVELOPE format
        envelope = f"ENVELOPE({coordinates[0]},{coordinates[2]},{coordinates[1]},{coordinates[3]})"

        return envelope

    @staticmethod
    def getURL(distribution):
        url = distribution.get("accessURL", None)
        if url is None:
            logging.debug("Distribution has no accessURL; falling back to downloadURL.")
            url = distribution.get("downloadURL", None)
        return quote(url, safe=":/?=")

    @staticmethod
    def process_distribution(distribution):
        url = AardvarkDataProcessor.getURL(distribution)
        if "format" not in distribution or url == "invalid":
            return None

        format_to_reference = {
            "ArcGIS GeoServices REST API": {
                "FeatureServer": "urn:x-esri:serviceType:ArcGIS#FeatureLayer",
                "ImageServer": "urn:x-esri:serviceType:ArcGIS#ImageMapLayer",
                "MapServer": "urn:x-esri:serviceType:ArcGIS#DynamicMapLayer",
            },
            "ZIP": "http://schema.org/downloadUrl",
        }

        format_references = format_to_reference.get(distribution["format"], {})
        if isinstance(format_references, dict):
            for key, value in format_references.items():
                if key in url:
                    return {value: url}
        else:
            return {format_references: url}

        return None

    @staticmethod
    def process_dataset_class_type_and_format(dataset):
        aerial_keywords = ["aerial", "air photo", "ortho", "mrsid", "sid image"]

        result = {
            "dct_format_s": None,
            "gbl_resourceClass_sm": list(RESOURCECLASS),
            "gbl_resourceType_sm": list(RESOURCETYPE),
        }

        shapefile_found = False
        for distribution in dataset.get("distribution", []):
            if distribution.get("title") == "Shapefile":
                result["dct_format_s"] = "Shapefile"
                result["gbl_resourceClass_sm"] = ["Datasets"]
                result["gbl_resourceType_sm"] = ["Digital maps"]
                shapefile_found = True

        if not shapefile_found:
            title = dataset.get("title", "").lower()
            logging.debug(
                f"Processing title: {title} ({dataset.get('identifier', 'no id')})"
            )
            matched_keywords = [
                keyword for keyword in aerial_keywords if keyword in title
            ]
            is_aerial = bool(matched_keywords)
            logging.debug(
                f"Keywords matched: {is_aerial}, matched keywords: {matched_keywords}"
            )

            if is_aerial:
                result["gbl_resourceClass_sm"].append("Imagery")
                result["gbl_resourceType_sm"] = ["Aerial photographs"]

        result["gbl_resourceClass_sm"] = list(set(result["gbl_resourceClass_sm"]))
        result["gbl_resourceType_sm"] = list(set(result["gbl_resourceType_sm"]))
        logging.debug(result)
        return result

    @staticmethod
    def issue_date_parser(dataset_dict):
        dt_string = dataset_dict["issued"]
        if not dt_string or contains_unresolved_template(dt_string):
            return None

        try:
            parsed_date = parser.parse(dt_string)
            dct_issued_s = parsed_date.strftime(r"%Y-%m-%d")
        except Exception as e:
            logging.warning(f'Unable to parse the year from: "{dt_string}". Error: {e}')
            dct_issued_s = dt_string

        return dct_issued_s

    @staticmethod
    def load_schema():
        try:
            response = requests.get(SCHEMA, timeout=10)
            schema = json.loads(response.text)
            return schema
        except requests.exceptions.ReadTimeout as e:
            logging.error("Failed to fetch schema from GitHub!")
            sys.exit()

    @staticmethod
    def validate_json(json_data, schema):
        try:
            validate(instance=json_data, schema=schema)
        except jsonschema.exceptions.ValidationError as err:
            return False, err
        return True, None


class InitializationError(Exception):
    pass


class Aardvark:
    """
    A class to represent a single dataset as an OGM Aardvark record
    """

    def __init__(self, dataset_dict, website):
        process_id_result = self._process_id(dataset_dict, website)
        if process_id_result is False:
            raise InitializationError("Initialization failed: dataset in skiplist")
        self._initialize_default_field_values()
        extracted_dataset_dict = AardvarkDataProcessor.extract_data(dataset_dict)
        self._process_extracted_dataset_dict(extracted_dataset_dict, website)

    def _initialize_default_field_values(self):
        self.pcdm_memberOf_sm = MEMBEROF
        self.gbl_resourceClass_sm = RESOURCECLASS
        self.dct_accessRights_s = ACCESSRIGHTS
        self.gbl_mdVersion_s = MDVERSION
        self.dct_language_sm = LANG
        self.schema_provider_s = PROVIDER
        self.gbl_suppressed_b = SUPPRESSED
        self.dct_rights_sm = RIGHTS
        self.gbl_displayNote_sm = [DISPLAYNOTE] if DISPLAYNOTE else []

    def _process_id(self, dataset_dict, website):
        uuid, sublayer = AardvarkDataProcessor.extract_id_sublayer(
            dataset_dict["identifier"]
        )
        record_uuid = f"{uuid}{sublayer if sublayer else ''}"
        self.id = f"{website.site_name}-{record_uuid}"
        self.uuid = uuid

        if not self.id:
            logging.warning("ID is required.")
            return False

        # Stop processing if in skiplist
        skiplist_match = next(
            (
                candidate
                for candidate in (self.uuid, record_uuid)
                if candidate in website.site_skiplist
            ),
            None,
        )
        if skiplist_match:
            logging.debug(f"{skiplist_match} is on the skiplist.")
            return False

        self.dct_identifier_sm = [dataset_dict["identifier"]]
        return True

    def _process_extracted_dataset_dict(self, dataset_dict, website):
        self.dct_spatial_sm = website.site_details["Spatial"]

        prefix = website.site_details["CreatedBy"]
        title = prefix + " - " + dataset_dict["title"]
        self.dct_title_s = title

        self.gbl_mdModified_dt = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        description = dataset_dict.get("description", "No description provided.")
        if not contains_unresolved_template(description):
            cleaned_description = re.sub("<[^<]+?>", "", description)
            unescaped_description = html.unescape(cleaned_description)
            self.dct_description_sm = [DESCRIPTION, unescaped_description]
        else:
            self.dct_description_sm = [DESCRIPTION]

        publisher_name = (
            dataset_dict.get("publisher", {}).get("name")
            if isinstance(dataset_dict.get("publisher"), dict)
            else None
        )
        if contains_unresolved_template(publisher_name):
            publisher_name = None

        self.dct_creator_sm = [publisher_name] if publisher_name else []
        self.dct_publisher_sm = (
            [publisher_name] if publisher_name else [website.site_details["CreatedBy"]]
        )

        # dct_issued_s
        self.dct_issued_s = AardvarkDataProcessor.issue_date_parser(dataset_dict)

        self._process_spatial(dataset_dict, website)

        # dcat_keyword_sm (string multiple!)
        self.dcat_keyword_sm = dataset_dict["keyword"]

        self._process_distributions(dataset_dict)

        self._process_temporal_coverage(dataset_dict)

        # License and Rights
        rights = self.dct_rights_sm
        if dataset_dict.get("license"):
            rights.append(re.sub("<[^<]+?>", "", dataset_dict.get("license")))
        self.dct_rights_sm = rights

        # Replace gbl_resourceClass_sm for web applications/websites
        if (not self.uuid in website.site_applist) and (
            not self.uuid in website.site_maplist
        ):
            result = AardvarkDataProcessor.process_dataset_class_type_and_format(
                dataset_dict
            )
            self.dct_format_s = result["dct_format_s"]
            self.gbl_resourceClass_sm = result["gbl_resourceClass_sm"]
            self.gbl_resourceType_sm = result["gbl_resourceType_sm"]
        else:
            if self.uuid in website.site_applist:
                logging.debug(
                    f"UUID {self.uuid} is in site_applist; setting gbl_resourceClass_sm to ['Websites']."
                )
                self.gbl_resourceClass_sm = ["Websites"]
                self.dct_format_s = None
                self.gbl_resourceType_sm = None
            elif self.uuid in website.site_maplist:
                logging.debug(
                    f"UUID {self.uuid} is in site_maplist; setting gbl_resourceClass_sm to ['Maps']."
                )
                self.gbl_resourceClass_sm = ["Maps"]
                self.dct_format_s = None
                self.gbl_resourceType_sm = ["Digital maps"]

    def _process_spatial(self, dataset_dict, website):
        if "spatial" not in dataset_dict:
            logging.warning(f"No spatial information found for: {self.id}")
            return

        defaultBbox = AardvarkDataProcessor.default_bbox(website)

        try:
            processed_spatial = AardvarkDataProcessor.process_dcat_spatial(
                dataset_dict["spatial"], defaultBbox
            )
            self.locn_geometry = self.dcat_bbox = processed_spatial
        except ValueError as e:
            logging.warning(
                f"There was a problem interpreting the bbox information for: {self.id}\n"
                f"\t - at {dataset_dict['landingPage']}\n"
                f"\t Warning: {e}"
            )
            if defaultBbox is not None:
                self.locn_geometry = self.dcat_bbox = defaultBbox["envelope"]
                logging.debug("Using default envelope for the website.")
            else:
                logging.warning(f"No default bounding box set for {website}")

    def _process_distributions(self, dataset_dict):
        if "distribution" not in dataset_dict:
            return

        references = {"http://schema.org/url": dataset_dict["landingPage"]}
        for distribution in dataset_dict["distribution"]:
            reference = AardvarkDataProcessor.process_distribution(distribution)
            if reference is not None:
                references.update(reference)

        self.dct_references_s = json.dumps(references).replace(" ", "")

    def _parse_index_year(self, value, field_name):
        if not value or contains_unresolved_template(value):
            return None

        try:
            index_date = parser.parse(value)
            return int(index_date.year)
        except ImportError:
            try:
                return int(str(value)[:4])
            except (TypeError, ValueError) as exc:
                logging.warning(
                    f'Unable to derive {field_name} year from "{value}" for {self.id}: {exc}'
                )
                return None
        except Exception as exc:
            logging.warning(
                f'Unable to parse {field_name} year from "{value}" for {self.id}: {exc}'
            )
            return None

    def _process_temporal_coverage(self, dataset_dict):
        if "modified" in dataset_dict:
            index_year = self._parse_index_year(dataset_dict["modified"], "modified")
            if index_year is not None:
                self.gbl_indexYear_im = [index_year]
                self.dct_temporal_sm = [f"Modified {index_year}"]
            else:
                self.gbl_indexYear_im = []
                self.dct_temporal_sm = []
        else:
            self.gbl_indexYear_im = []
            self.dct_temporal_sm = []

        if "issued" in dataset_dict:
            index_year = self._parse_index_year(dataset_dict["issued"], "issued")
            if index_year is not None:
                self.gbl_indexYear_im.append(index_year)
                if self.dct_temporal_sm:
                    self.dct_temporal_sm[0] = f"Issued {index_year}"
                else:
                    self.dct_temporal_sm = [f"Issued {index_year}"]

    def to_dict(self):
        """
        Serialize the object to a dictionary, excluding None or empty values.
        """
        # List all the attributes that you want to include in the JSON output.
        attributes = [
            "id",
            "dct_title_s",
            "dct_creator_sm",
            "dct_publisher_sm",
            "dct_identifier_sm",
            "dct_rights_sm",
            "pcdm_memberOf_sm",
            "gbl_resourceClass_sm",
            "dct_accessRights_s",
            "gbl_mdModified_dt",
            "gbl_mdVersion_s",
            "dct_language_sm",
            "schema_provider_s",
            "gbl_suppressed_b",
            "gbl_displayNote_sm",
            "dct_spatial_sm",
            "dct_description_sm",
            "dct_issued_s",
            "dcat_keyword_sm",
            "dct_references_s",
            "dct_format_s",
            "gbl_resourceType_sm",
            "locn_geometry",
            "dct_temporal_sm",
            "gbl_indexYear_im",
        ]
        # Build the dictionary with attribute names and their values if they are not None or empty.
        return {
            attr: getattr(self, attr)
            for attr in attributes
            if hasattr(self, attr) and getattr(self, attr)
        }

    def __str__(self):
        # Use the to_dict method to get the dictionary representation of the object.
        obj_dict = self.to_dict()
        # Format the dictionary into a string for printing.
        return "\n".join(f"{key}: {value}" for key, value in obj_dict.items())

    def toJSON(self):
        aardvark_dict = self.to_dict()  # Use the new to_dict method
        json_dump = json.dumps(aardvark_dict)
        schema = AardvarkDataProcessor.load_schema()
        is_valid, error = AardvarkDataProcessor.validate_json(aardvark_dict, schema)
        if is_valid:
            return json_dump
        else:
            logging.warning(f"Failed JSON Validation:\n{error}")
            logging.debug(str(json_dump))
            return None

    def is_valid(self):
        json_dump = self.toJSON()  # Call toJSON as a method
        if json_dump is None:
            return False, "JSON serialization failed."

        json_object = json.loads(
            json_dump
        )  # Parse the JSON string back into a dictionary
        schema = AardvarkDataProcessor.load_schema()
        is_valid, error = AardvarkDataProcessor.validate_json(json_object, schema)
        if is_valid:
            return True, None
        else:
            return False, error


# Main Function
def main():
    list_of_sites = harvest_sites()

    # Create output dir if it doesn't exist:
    if not OUTPUTDIR.is_dir():
        try:
            logging.info(f"Creating output directory {str(OUTPUTDIR)}")
            OUTPUTDIR.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logging.warning("Unable to create output directory")
            return

    clear_output_directory(OUTPUTDIR)
    ensure_collection_record(OUTPUTDIR)

    for website in list_of_sites:
        new_aardvark_objects = []
        for dataset in website.site_json["dataset"]:
            try:
                new_aardvark_object = Aardvark(dataset, website)
                new_aardvark_objects.append(new_aardvark_object)
                newfile = f"{new_aardvark_object.id}.json"
                newfilePath = OUTPUTDIR / newfile
                with open(newfilePath, "w", encoding="utf-8") as f:
                    f.write(new_aardvark_object.toJSON())
            except InitializationError as e:
                logging.debug(str(e))


if __name__ == "__main__":
    dt = datetime.now().strftime(r"%Y-%m-%d %H:%M:%S")
    try:
        main()
        logging.info(f"DCAT harvest finished at {dt}")
    except Exception as e:
        logging.error(str(e))
        logging.warning(f"DCAT harvest finished with errors at {dt}")
