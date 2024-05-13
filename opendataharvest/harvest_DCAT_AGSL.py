import csv
import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from pprint import pp
from urllib.parse import quote

# Third party imports
import requests
import yaml
from dateutil import parser


OpenDataSites = yaml.safe_load(
    open(
        r"C:\Users\srappel\Documents\GitHub\GeoDiscovery-Utils\opendataharvest\OpenDataSites.yaml",
        "r",
    )
)
OUTPUTDIR = Path(
    r"C:\Users\srappel\Documents\GitHub\GeoDiscovery-Utils\opendataharvest\output_md"
)
assert OUTPUTDIR.is_dir()

DEFAULTBBOX = Path(
    r"C:\Users\srappel\Documents\GitHub\GeoDiscovery-Utils\opendataharvest\Wisconsin-Counties-CSV.csv"
)


CATALOG = OpenDataSites["ArcGIS_Sites"]
assert isinstance(CATALOG, dict)

MAXRETRY = 1
SLEEPTIME = 1

LOGFILE = OUTPUTDIR / "_logfile.txt"
logw = open(LOGFILE, "w")
logw.write("")
logw.close()


def logg(string):
    string = str(string)
    log = open(LOGFILE, "a")
    log.write(string)
    log.write("\n")
    print(string, "\n")
    log.close()


class Site:
    def __init__(
        self,
        site_name: str,
        site_details: dict,
        site_json: dict,
        site_skiplist: list,
        site_applist: list,
    ):
        self.site_name = site_name
        self.site_details = site_details
        self.site_json = site_json
        self.site_skiplist = site_skiplist
        self.site_applist = site_applist

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)


def harvest_sites() -> list:
    site_list = []  # list of Site objects
    for site, details in CATALOG.items():
        for i in range(MAXRETRY):  # Retry up to 5 times
            try:
                response = requests.get(details["SiteURL"], timeout=3)
                response.raise_for_status()
                site_json = response.json()
                site_skiplist = []
                if "SkipList" in details:
                    for skip in details["SkipList"]:
                        site_skiplist.append(skip["UUID"])
                site_applist = []
                if "AppList" in details:
                    for app in details["AppList"]:
                        site_applist.append(app["UUID"])
                current_Site = Site(
                    details["SiteName"], details, site_json, site_skiplist, site_applist
                )
                site_list.append(current_Site)
                break  # If the request is successful, break the retry loop
            except json.JSONDecodeError:
                logg(f"The content from {site} is not a valid JSON document.")
                break  # If the content is not valid JSON, break the retry loop
            except (requests.HTTPError, requests.exceptions.Timeout) as e:
                logg(
                    f"Received bad response from {site}. Retrying after {SLEEPTIME} seconds..."
                )
                time.sleep(SLEEPTIME)  # Wait for 1 second before retrying
                if i == (MAXRETRY - 1):  # If this was the last retry
                    logg(f"Failed to connect to {site} after {MAXRETRY + 1} attempts.")
                    logg(str(e))
    return site_list


list_of_sites = harvest_sites()


def extract_id_sublayer(url):
    id_pattern = r"id=([a-zA-Z0-9]+)"
    sublayer_pattern = r"sublayer=(\d+)"

    id_match = re.search(id_pattern, url)
    sublayer_match = re.search(sublayer_pattern, url)

    id_value = id_match.group(1) if id_match else None
    sublayer_value = sublayer_match.group(1) if sublayer_match else None

    return id_value, sublayer_value


class Aardvark:
    def __init__(self, dataset_dict, website):
        # Required fields
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

        assert (
            "title" in dataset_dict and "identifier" in dataset_dict
        ), "Dataset missing title or identifier"

        # From YAML:
        uuid, sublayer = extract_id_sublayer(dataset_dict["identifier"])
        self.id = f"{website.site_name}-{uuid}{sublayer if sublayer else ''}"
        self.uuid = uuid

        assert isinstance(self.id, str) and len(self.id) > 0, "id is required"

        # Stop processing if in skiplist
        if self.uuid in website.site_skiplist:
            logg(f"{self.uuid} is on the skiplist...\n")
            return

        # dct_identifier_sm
        self.dct_identifier_sm = dataset_dict["identifier"]

        # Process spatial bounding box
        self.dct_spatial_sm = website.site_details["Spatial"]

        # dct_title_s (REQUIRED)
        prefix = website.site_details["CreatedBy"]
        title = prefix + " - " + dataset_dict.get("title", "Untitled Dataset")

        self.dct_title_s = title

        # gbl_mdModified_dt (Required)
        self.gbl_mdModified_dt = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        assert (
            isinstance(self.gbl_mdModified_dt, str) and len(self.gbl_mdModified_dt) > 0
        ), "mdModified is required"

        # id (Required)
        uuid, sublayer = extract_id_sublayer(dataset_dict["identifier"])
        self.id = (
            website.site_name + "-" + uuid + (sublayer if not sublayer is None else "")
        )

        # dct_description_sm
        self.dct_description_sm = [
            re.sub("<[^<]+?>", "", dataset_dict.get("description", []))
        ]
        self.dct_description_sm.append(
            f"This dataset was automatically cataloged from the author's Open Data Portal. In some cases, publication year and bounding coordinates shown here may be incorrect. Additional download formats may be available on the author's website. Please check the 'More details at' link for additional information."
        )

        # dct_creator_sm
        self.dct_creator_sm = (
            [dataset_dict["publisher"]["name"]] if "publisher" in dataset_dict else []
        )

        # dct_issued_s
        self.dct_issued_s = dataset_dict.get("issued", "")

        # locn_geometry & dcat_bbox
        def process_dcat_spatial(spatial_string):
            # Extract coordinates

            pattern = r"(-?\d+\.\d+)"
            matches = re.findall(pattern, spatial_string)

            if len(matches) != 4:
                raise ValueError(f"Non-conforming spatial bounding box in {self.uuid}")

            # Convert to floats
            coordinates = [float(coord) for coord in matches]

            #  ENVELOPE(W=0,E=2,N=1,S=3)

            # error cases:
            # North is less than South:
            if coordinates[1] < coordinates[3]:
                coordinates[1], coordinates[3] = coordinates[3], coordinates[1]

            # East is less than West:
            if coordinates[2] < coordinates[0]:
                coordinates[2], coordinates[0] = coordinates[0], coordinates[2]

            # West or East is greater than 180 or lower than -180
            if not (-180 <= coordinates[0] <= 180) or not (
                -180 <= coordinates[2] <= 180
            ):
                raise ValueError("CoordinateError:East-West")

            if not (90 >= coordinates[1]) or not (-90 <= coordinates[3]):
                raise ValueError("CoordinateError:North-South")

            # Convert to ENVELOPE format
            envelope = f"ENVELOPE({coordinates[0]},{coordinates[2]},{coordinates[1]},{coordinates[3]})"

            return envelope

        def defaultBbox():
            if "DefaultBbox" in website.site_details:
                default_bbox = website.site_details["DefaultBbox"]
                default_csv = open(DEFAULTBBOX)
                bboxreader = csv.DictReader(default_csv)
                for row in bboxreader:
                    if row["name"] == default_bbox:
                        envelope = f"ENVELOPE({row['west']},{row['east']},{row['north']},{row['south']})"
            return envelope

        if "spatial" in dataset_dict:
            try:
                self.locn_geometry = self.dcat_bbox = process_dcat_spatial(
                    dataset_dict["spatial"]
                )
            except ValueError as e:
                logg(
                    f"There was a problem interpreting the bbox information for: {self.id}\n\t - at {dataset_dict['landingPage']}\n\t Error: {e}\n"
                )
                try:
                    self.locn_geometry = self.dcat_bbox = defaultBbox()
                    logg(f"Using default envelope for the website.")
                except UnboundLocalError as e:
                    logg(f"{e}\n")
                    self.locn_geometry = self.dcat_bbox = None

        # dcat_keyword_sm (string multiple!)
        self.dcat_keyword_sm = dataset_dict.get("keyword", [])

        # dct_references_s

        def getURL(refs):
            url = refs.get("accessURL", refs.get("downloadURL", "invalid"))
            return quote(url, safe=":/?=")

        if "distribution" in dataset_dict:
            references = {"http://schema.org/url": dataset_dict["landingPage"]}
            for dist in dataset_dict["distribution"]:
                url = getURL(dist)
                if "format" in dist and url != "invalid":
                    if dist["format"] == "ArcGIS GeoServices REST API":
                        if "FeatureServer" in url:
                            references[
                                "urn:x-esri:serviceType:ArcGIS#FeatureLayer"
                            ] = url
                        elif "ImageServer" in url:
                            references[
                                "urn:x-esri:serviceType:ArcGIS#ImageMapLayer"
                            ] = url
                        elif "MapServer" in url:
                            references[
                                "urn:x-esri:serviceType:ArcGIS#DynamicMapLayer"
                            ] = url
                    elif dist["format"] == "ZIP":
                        references["http://schema.org/downloadUrl"] = url
            self.dct_references_s = json.dumps(references).replace(" ", "")

        # index year and temporal coverage
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
                print(f"An error occurred: {e}")

            self.gbl_indexYear_im.append(index_year)
            if self.dct_temporal_sm:
                self.dct_temporal_sm.append(f"Issued {index_year}")
            else:
                self.dct_temporal_sm = [f"Issued {index_year}"]

        # License and Rights
        rights = self.dct_rights_sm
        if dataset_dict.get("license"):
            rights.append(re.sub("<[^<]+?>", "", dataset_dict.get("license")))
        self.dct_rights_sm = rights

        # Format dct_format_s
        # TODO: this is only catching shapefile right now.
        def format_fetcher():
            for distribution in dataset_dict["distribution"]:
                if distribution["title"] == "Shapefile":
                    self.dct_format_s = "Shapefile"
                    return

            if (
                "Aerial" in dataset_dict.get("title")
                or "aerial" in dataset_dict.get("keyword")
                or "imagery" in dataset_dict.get("keyword")
            ):
                self.gbl_resourceType_sm = "Aerial photographs"
                self.dct_format_s = "Raster data"
                self.gbl_resourceClass_sm.append("Imagery")
                return

        format_fetcher()

        # Replace gbl_resourceClass_sm for web applications/websites
        if self.uuid in website.site_applist:
            self.gbl_resourceClass_sm = "Websites"

    def __str__(self):
        return f"""
        Title: {self.dct_title_s}
        Id: {self.id}
        Index Year: {self.gbl_indexYear_im}
        Metadata Modified: {self.gbl_mdModified_dt}
        Spatial: {self.dct_spatial_sm}
        Description: {self.dct_description_sm}
        Creator: {self.dct_creator_sm}
        Issued: {self.dct_issued_s}
        Spatial bbox: {self.locn_geometry}
        References: {self.dct_references_s}
        """

    def toJSON(self):
        # Remove UUID field
        aardvark_dict = vars(self)
        assert isinstance(aardvark_dict, dict)
        del aardvark_dict["uuid"]
        return json.dumps(aardvark_dict)


for website in list_of_sites:
    for dataset in website.site_json["dataset"]:
        new_aardvark_object = Aardvark(dataset, website)
        if not new_aardvark_object.uuid in website.site_skiplist:
            # print(new_aardvark_object.toJSON())
            newfile = new_aardvark_object.id + ".json"
            newfilePath = OUTPUTDIR / newfile
            f = open(newfilePath, "w")
            # print(f'Writing {newfilePath}')
            f.write(new_aardvark_object.toJSON())
            f.close()
