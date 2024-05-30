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
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
except FileNotFoundError:
    print(f"Config file {config_file} not found")
    sys.exit()

try:
    CONFIG = config.get("CONFIG")
    OUTPUTDIR = Path(CONFIG.get("OUTPUTDIR"))
    DEFAULTBBOX = Path(CONFIG.get("DEFAULTBBOX"))
    CATALOG_KEY = CONFIG.get("CATALOG", "TestSites")
    CATALOG = config.get(CATALOG_KEY, None)
    MAXRETRY = CONFIG.get("MAXRETRY", 5)
    SLEEPTIME = CONFIG.get("SLEEPTIME", 1)

    ## Get the JSON schema:
    SCHEMA = CONFIG.get("SCHEMA")

except AttributeError as e:
    print(f"Unable to read all configuration values from {config_file}")
    print(e)
    sys.exit()

dt = str(datetime.now().strftime(r"%Y-%m-%d-%H:%M:%S"))
logfile_name = f"_{dt}.log"
LOGFILE = OUTPUTDIR / logfile_name

# Configure the logging module
logging.basicConfig(
    filename=LOGFILE, filemode="w", level=logging.INFO, format="%(message)s"
)


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
        current_Site = Site(
            details["SiteName"], details, site_json, site_skiplist, site_applist
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
    def extract_id_sublayer(url):
        id_pattern = r"id=([a-zA-Z0-9]+)"
        sublayer_pattern = r"sublayer=(\d+)"

        id_match = re.search(id_pattern, url)
        sublayer_match = re.search(sublayer_pattern, url)

        id_value = id_match.group(1) if id_match else None
        sublayer_value = sublayer_match.group(1) if sublayer_match else None

        if id_value is None:
            logging.warning(f"No id was extracted from the url: {url}")

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
        coordinates[1], coordinates[3] = sorted(latitudes)
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
        url = distribution.get("accessURL", distribution.get("downloadURL", "invalid"))
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
    def format_fetcher(dataset_dict):
        dct_format_s = None
        gbl_resourceType_sm = None
        gbl_resourceClass_sm = ["Datasets"]

        for distribution in dataset_dict["distribution"]:
            if distribution["title"] == "Shapefile":
                dct_format_s = "Shapefile"
            elif "Aerial" in dataset_dict.get("title", "") or any(
                keyword in dataset_dict.get("keyword", [])
                for keyword in ["Aerial", "aerial", "imagery"]
            ):
                gbl_resourceType_sm = "Aerial photographs"
                dct_format_s = "Raster data"
                gbl_resourceClass_sm.append("Imagery")

        return dct_format_s, gbl_resourceType_sm, gbl_resourceClass_sm
    
    @staticmethod
    def load_schema():
        response = requests.get(SCHEMA, timeout=3)
        schema = json.loads(response.text)
        return schema
    
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
        self._initialize_required_fields()
        dataset_dict = AardvarkDataProcessor.extract_data(dataset_dict)
        self._process_id(dataset_dict, website)
        self._process_dataset_dict(dataset_dict, website)

    def _initialize_required_fields(self):
        self.pcdm_memberOf_sm = ["AGSLOpenDataHarvest"]
        self.gbl_resourceClass_sm = ["Datasets"]
        self.dct_accessRights_s = "public"
        self.gbl_mdVersion_s = "Aardvark"
        self.dct_language_sm = ["English"]
        self.schema_provider_s = "American Geographical Society Library – UWM Libraries"
        self.gbl_suppressed_b = False
        self.dct_rights_sm = [
            "Although this data is being distributed by the American Geographical Society Library at the University of Wisconsin-Milwaukee Libraries, no warranty expressed or implied is made by the University as to the accuracy of the data and related materials. The act of distribution shall not constitute any such warranty, and no responsibility is assumed by the University in the use of this data, or related materials."
        ]

    def _process_id(self, dataset_dict, website):
        uuid, sublayer = AardvarkDataProcessor.extract_id_sublayer(
            dataset_dict["identifier"]
        )
        self.id = f"{website.site_name}-{uuid}{sublayer if sublayer else ''}"
        self.uuid = uuid

        if not self.id:
            logging.warning("ID is required.")

        # Stop processing if in skiplist
        if self.uuid in website.site_skiplist:
            logging.info(f"{self.uuid} is on the skiplist...\n")
            return

        self.dct_identifier_sm = dataset_dict["identifier"]

    def _process_dataset_dict(self, dataset_dict, website):
        self.dct_spatial_sm = website.site_details["Spatial"]

        prefix = website.site_details["CreatedBy"]
        title = prefix + " - " + dataset_dict["title"]
        self.dct_title_s = title

        self.gbl_mdModified_dt = datetime.now(timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        self.dct_description_sm = [
            re.sub("<[^<]+?>", "", dataset_dict.get("description", []))
        ]
        self.dct_description_sm.append(
            f"This dataset was automatically cataloged from the creator's Open Data Portal. In some cases, publication year and bounding coordinates shown here may be incorrect. Additional download formats may be available on the author's website. Please check the 'More details at' link for additional information."
        )

        self.dct_creator_sm = (
            [dataset_dict["publisher"]["name"]] if "publisher" in dataset_dict else []
        )

        # dct_issued_s
        self.dct_issued_s = dataset_dict["issued"]

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

        # Format dct_format_s
        self.dct_format_s, self.gbl_resourceType_sm, self.gbl_resourceClass_sm = (
            AardvarkDataProcessor.format_fetcher(dataset_dict)
        )

        # Replace gbl_resourceClass_sm for web applications/websites
        if self.uuid in website.site_applist:
            self.gbl_resourceClass_sm = ["Websites"]

    def _process_spatial(self, dataset_dict, website):
        if "spatial" in dataset_dict:
            try:
                self.locn_geometry = self.dcat_bbox = (
                    AardvarkDataProcessor.process_dcat_spatial(dataset_dict["spatial"])
                )
            except ValueError as e:
                logging.warning(
                    f"There was a problem interpreting the bbox information for: {self.id}\n\t - at {dataset_dict['landingPage']}\n\t Error: {e}\n"
                )
                try:
                    self.locn_geometry = self.dcat_bbox = (
                        AardvarkDataProcessor.defaultBbox(website)
                    )
                    logging.warning(f"Using default envelope for the website.\n")
                except UnboundLocalError as e:
                    logging.error(f"{e}\n")
                    self.locn_geometry = self.dcat_bbox = None

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
                print(f"An error occurred: {e}")

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

    def __str__(self):
        return f"""
        dcat_bbox: {self.dcat_bbox}
        dcat_keyword_sm: {self.dcat_keyword_sm}
        dct_accessRights_s: {self.dct_accessRights_s}
        dct_creator_sm: {self.dct_creator_sm}
        dct_description_sm: {self.dct_description_sm}
        dct_format_s: {self.dct_format_s}
        dct_identifier_sm: {self.dct_identifier_sm}
        dct_issued_s: {self.dct_issued_s}
        dct_language_sm: {self.dct_language_sm}
        dct_references_s: {self.dct_references_s}
        dct_rights_sm: {self.dct_rights_sm}
        dct_spatial_sm: {self.dct_spatial_sm}
        dct_temporal_sm: {self.dct_temporal_sm}
        dct_title_s: {self.dct_title_s}
        gbl_indexYear_im: {self.gbl_indexYear_im}
        gbl_mdModified_dt: {self.gbl_mdModified_dt}
        gbl_mdVersion_s: {self.gbl_mdVersion_s}
        gbl_resourceClass_sm: {self.gbl_resourceClass_sm}
        gbl_resourceType_sm: {self.gbl_resourceType_sm}
        gbl_suppressed_b: {self.gbl_suppressed_b}
        id: {self.id}
        locn_geometry: {self.locn_geometry}
        pcdm_memberOf_sm: {self.pcdm_memberOf_sm}
        schema_provider_s: {self.schema_provider_s}
        uuid: {self.uuid}
        """


    def toJSON(self):
        aardvark_dict = vars(self)
        aardvark_dict.pop(
            "uuid", None
        )  # Removes uuid if it exists, does nothing otherwise
        json_dump = json.dumps(aardvark_dict)
        schema = AardvarkDataProcessor.load_schema()
        is_valid, error = AardvarkDataProcessor.validate_json(json_dump, schema)
        if is_valid:
            return json_dump
        else:
            logging.warning(f"Failed JSON Validation")
            return
    
    def is_valid(self):
        json_object = self.toJSON
        
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
                with open(newfilePath, "w") as f:
                    json_data = new_aardvark_object.toJSON()
                    if not json_data is None:
                        f.write(new_aardvark_object.toJSON())
                    else:
                        logging.warning(f"{str(newfilePath)} not written...")


if __name__ == "__main__":
    main()
