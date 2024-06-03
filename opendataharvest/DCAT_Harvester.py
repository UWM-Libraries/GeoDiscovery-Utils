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
A Site object is crated for each website in the defined catalog. Datasets not 
in the skip list for the Site will be looped over and a JSON File generated for
each. The Aardvark class is dictionary-like and defines the structure of a 
single dataset description. We  dump the Aardvark object to JSON when 
crosswalking is completeo and write it to a file. A timestamped log file is 
created on each run and contains verbose output for debugging and for maintaining
the config.yaml file such as datasets to add to the skip list, etc.
Code is formated according to PEP8 using Black.
Care is taken to use functionality from the Python standard library.
AI was utilized in authoring this script.
"""

import csv
import json
import logging
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote
from typing import List

import requests
import yaml
from dateutil import parser
import jsonschema
from jsonschema import validate

config_file = r"opendataharvest/config.yaml"

try:
    with open(config_file, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)
except FileNotFoundError:
    print(f"Config file {config_file} not found")
    sys.exit()

try:
    CONFIG = config.get("CONFIG")
    OUTPUTDIR = Path(CONFIG.get("OUTPUTDIR"))
    LOGDIR = Path(CONFIG.get("LOGDIR"))
    DEFAULTBBOX = Path(CONFIG.get("DEFAULTBBOX"))
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

    ## Get the JSON schema:
    SCHEMA = CONFIG.get("SCHEMA")

except AttributeError as e:
    print(f"Unable to read all configuration values from {config_file}")
    print(e)
    sys.exit()

dt = str(datetime.now().strftime(r"%Y%m%d%H%M%S"))
logfile_name = f"_{dt}.log"
LOGFILE = LOGDIR / logfile_name

# Configure the logging module
logging.basicConfig(
    filename=LOGFILE, filemode="w", level=logging.INFO, format="%(message)s"
)

# # Add a console handler for debug messages
# console_handler = logging.StreamHandler()
# console_handler.setLevel(logging.DEBUG)
# console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
# console_handler.setFormatter(console_formatter)
# logging.getLogger().addHandler(console_handler)

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
            logging.warning(f"trying SitURL for {site} as a filepath")
            response_file = json.load(open(Path(details["SiteURL"]), "r"))
            return response_file
        except json.JSONDecodeError:
            logging.warning(f"The content from {site} is not a valid JSON document.")
            return None
        except (requests.HTTPError, requests.exceptions.Timeout) as e:
            logging.info(
                f"Received bad response from {site}. Retrying after {SLEEPTIME} seconds..."
            )
            time.sleep(SLEEPTIME)
            if i == (MAXRETRY - 1):
                logging.warning(
                    f"Failed to connect to {site} after {MAXRETRY + 1} attempts."
                )
                error = str(e) + "\n"
                logging.warning(error)
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
            details["SiteName"], details, site_json, site_skiplist, site_applist, site_maplist
        )
        site_list.append(current_Site)
    return site_list


class AardvarkDataProcessor:
    @staticmethod
    def extract_data(dataset_dict):
        # Extract data from dataset_dict
        title = dataset_dict.get("title", "Untitled Dataset")
        identifier = dataset_dict["identifier"]
        description = re.sub("<[^<]+?>", "", dataset_dict.get("description", []))
        creator = (
            [dataset_dict["publisher"]["name"]] if "publisher" in dataset_dict else []
        )
        issued = dataset_dict.get("issued", "")
        modified = dataset_dict.get("modified", "")
        keyword = dataset_dict.get("keyword", [])
        spatial = dataset_dict.get("spatial", None)
        distribution = dataset_dict.get("distribution", None)
        publisher = dataset_dict.get("publisher", [])
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
    def process_dcat_spatial(spatial_string):
        def is_in_range(value, range_min, range_max):
            return range_min <= value <= range_max

        # Extract coordinates
        pattern = r"(-?\d+\.\d+)"
        matches = re.findall(pattern, spatial_string)

        if len(matches) != 4:
            raise ValueError("Non-conforming spatial bounding box")

        # Convert to floats and validate coordinates
        coordinates = [float(coord) for coord in matches]
        longitudes = coordinates[::2]
        latitudes = coordinates[1::2]

        if not all(is_in_range(lon, -180, 180) for lon in longitudes):
            raise ValueError("Longitude coordinates must be between -180 and 180")

        if not all(is_in_range(lat, -90, 90) for lat in latitudes):
            raise ValueError("Latitude coordinates must be between -90 and 90")

        # Ensure North is greater than South and East is greater than West
        coordinates[1], coordinates[3] = sorted(latitudes, reverse=True)
        coordinates[0], coordinates[2] = sorted(longitudes)

        # Convert to ENVELOPE format
        envelope = f"ENVELOPE({coordinates[0]},{coordinates[2]},{coordinates[1]},{coordinates[3]})"

        return envelope

    @staticmethod
    def defaultBbox(website):
        envelope = None
        if "DefaultBbox" in website.site_details:
            default_bbox = website.site_details["DefaultBbox"]
            with open(DEFAULTBBOX) as default_csv:
                bboxreader = csv.DictReader(default_csv)
                for row in bboxreader:
                    if row["name"] == default_bbox:
                        envelope = f"ENVELOPE({row['west']},{row['east']},{row['north']},{row['south']})"
        return envelope

    @staticmethod
    def getURL(distribution):
        url = distribution.get("accessURL", None)
        if url is None:
            logging.info("There is no accessURL, looking for downloadURL instead.")
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
            "gbl_resourceClass_sm": RESOURCECLASS,
            "gbl_resourceType_sm": RESOURCETYPE,
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
            logging.info(f"Processing title: {title} ({dataset.get('identifier', 'no id')})\n")
            matched_keywords = [keyword for keyword in aerial_keywords if keyword in title]
            is_aerial = bool(matched_keywords)
            logging.info(f"Keywords matched: {is_aerial}, Matched keywords: {matched_keywords}\n")

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


class Aardvark:
    """
    A class to represent a single dataset as an OGM Aardvark record
    """

    def __init__(self, dataset_dict, website):
        process_id_result = self._process_id(dataset_dict, website)
        if not process_id_result:  # Dataset is in the skiplist
            return
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

    def _process_id(self, dataset_dict, website):
        uuid, sublayer = AardvarkDataProcessor.extract_id_sublayer(
            dataset_dict["identifier"]
        )
        self.id = f"{website.site_name}-{uuid}{sublayer if sublayer else ''}"
        self.uuid = uuid

        if not self.id:
            logging.warning("ID is required.")
            return None

        # Stop processing if in skiplist
        if self.uuid in website.site_skiplist:
            logging.info(f"{self.uuid} is on the skiplist...\n")
            return None

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
        if "{{description}}" not in description:
            cleaned_description = re.sub("<[^<]+?>", "", description)
            self.dct_description_sm = [cleaned_description, DESCRIPTION]
        else:
            self.dct_description_sm = [DESCRIPTION]
        

        self.dct_creator_sm = (
            [dataset_dict["publisher"]["name"]] if "publisher" in dataset_dict else []
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
        if (not self.uuid in website.site_applist) and (not self.uuid in website.site_maplist):
            result = AardvarkDataProcessor.process_dataset_class_type_and_format(
                dataset_dict
            )
            self.dct_format_s = result["dct_format_s"]
            self.gbl_resourceClass_sm = result["gbl_resourceClass_sm"]
            self.gbl_resourceType_sm = result["gbl_resourceType_sm"]
        else:
            if self.uuid in website.site_applist:
                logging.info(
                    f"UUID {self.uuid} is in site_applist, setting gbl_resourceClass_sm to ['Websites']\n"
                )
                self.gbl_resourceClass_sm = ["Websites"]
                self.dct_format_s = None
                self.gbl_resourceType_sm = None
            elif self.uuid in website.site_maplist:
                logging.info(
                    f"UUID {self.uuid} is in site_maplist, setting gbl_resourceClass_sm to ['Maps']\n"
                )
                self.gbl_resourceClass_sm = ["Maps"]
                self.dct_format_s = None
                self.gbl_resourceType_sm = ["Digital maps"]
            

    def _process_spatial(self, dataset_dict, website):
        if "spatial" not in dataset_dict:
            logging.warning(f"No spatial information found for: {self.id}")
            return

        try:
            processed_spatial = AardvarkDataProcessor.process_dcat_spatial(
                dataset_dict["spatial"]
            )
            self.locn_geometry = self.dcat_bbox = processed_spatial
        except ValueError as e:
            logging.warning(
                f"There was a problem interpreting the bbox information for: {self.id}\n"
                f"\t - at {dataset_dict['landingPage']}\n"
                f"\t Error: {e}\n"
            )
            default_bbox = AardvarkDataProcessor.defaultBbox(website)
            if default_bbox is not None:
                self.locn_geometry = self.dcat_bbox = default_bbox
                logging.warning("Using default envelope for the website.\n")
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

    def _process_temporal_coverage(self, dataset_dict):
        if "modified" in dataset_dict:
            try:
                index_date = parser.parse(dataset_dict["modified"])
                index_year = int(index_date.year)
            except ImportError:
                index_year = int(dataset_dict["modified"][:4])
            except Exception as e:
                logging.error(f"An error occurred: {e}")

            self.gbl_indexYear_im = [index_year]
            self.dct_temporal_sm = [f"Modified {index_year}"]
        else:
            self.gbl_indexYear_im = []

        if "issued" in dataset_dict:
            try:
                index_date = parser.parse(dataset_dict["issued"])
                index_year = int(index_date.year)
            except ImportError:
                index_year = int(dataset_dict["issued"][:4])
            except Exception as e:
                logging.error("Problem processing the issued date.")

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
            "dct_identifier_sm",
            "dct_rights_sm",
            "gbl_resourceClass_sm",
            "dct_accessRights_s",
            "gbl_mdModified_dt",
            "gbl_mdVersion_s",
            "dct_language_sm",
            "schema_provider_s",
            "gbl_suppressed_b",
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
            logging.info(str(json_dump))
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

    for website in list_of_sites:
        new_aardvark_objects = [
            Aardvark(dataset, website) for dataset in website.site_json["dataset"]
        ]
        for new_aardvark_object in new_aardvark_objects:
            if new_aardvark_object.uuid not in website.site_skiplist:
                newfile = f"{new_aardvark_object.id}.json"
                newfilePath = OUTPUTDIR / newfile
                json_data = new_aardvark_object.toJSON()
                if not json_data is None:
                    with open(newfilePath, "w", encoding="utf-8") as f:
                        f.write(new_aardvark_object.toJSON())
                else:
                    logging.warning(f"{str(newfilePath)} not written...")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(str(e))
