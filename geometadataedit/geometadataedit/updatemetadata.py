"""
Tools for updating AGSL metadata for GeoDiscovery
"""

import arcpy
import requests
import re
import zipfile
import os

import xml.etree.ElementTree as ET

from arcpy import metadata as md
from datetime import datetime
from pathlib import Path
from enum import Enum

from dotenv import load_dotenv

load_dotenv()

# .env settings
APPLICATION_URL = os.getenv("APPLICATION_URL")
FILE_SERVER_URL = os.getenv("FILE_SERVER_URL")
REDIRECT_URL = os.getenv("REDIRECT_URL")
NOID_URL = os.getenv("NOID_URL")
FILE_SERVER_PATH = os.getenv("FILE_SERVER_PATH")

ARK_REGEX = r"(\d{5})\/(\w{11})"

SEARCH_STRING_DICT = {
    "altTitle": ".//idCitation/resAltTitle",
    "rights": ".//othConsts",
    "identCode": ".//citId/identCode",
    "citationIdentifier": ".dataIdInfo/idCitation",
    "metadataFileID": ".//mdFileID",
    "datasetURI": ".//dataSetURI",
    "contact": ".//rpCntInfo/cntHours/../..",  # code smell. This is only finding contacts that have hours listed.
    "contactDisplayName": "./displayName",
    "contactHours": ".//cntHours",
    "timeRangeBegin": ".//tmBegin",
    "timeRangeEnd": ".//tmEnd",
    "timeInstantExtent": ".//tmPosition",
}


class Dataset:
    def __init__(self, providedPath):
        self.path = Path(providedPath)
        if not self.path.is_dir():
            raise Exception("Provided path is not a directory.")
            return

        self.data, self.datatype = self.fetch_dataset_from_directory()
        if self.datatype == 0:
            raise Exception("Not able to fetch the data from the directory provided")
            return
        elif self.datatype == 5:
            raise Exception("There are multiple data types in the directory provided")
            return

        self.metadata: AGSLMetadata = AGSLMetadata(self.get_dataset_metadata())

    def fetch_dataset_from_directory(self) -> tuple[Path, int]:
        def test_dataset_type(rootdir) -> int:
            """Test Dataset Type
            - Returns an integer for type:
                - 0: Error
                - 1: Shapefile
                - 2: FileGeodatabase
                - 3: ArcGRID Raster
                - 4: TIFF
                - 5: Other/Mutliple
            """
            rootdir = Path(rootdir)

            gdb_count = 0
            shp_count = 0
            raster_count = 0
            tiff_count = 0

            for path in Path(rootdir).rglob("*"):
                if path.suffix.lower() == ".gdb":
                    gdb_count += 1
                elif path.suffix.lower() == ".shp":
                    shp_count += 1
                elif path.suffix.lower() == ".adf":
                    raster_count += 1
                elif path.suffix.lower() == ".tif":
                    tiff_count += 1
                else:
                    continue

            print(f"Shapefile count: {shp_count}")
            print(f"GDB count: {gdb_count}")
            print(f"Raster count: {raster_count}")
            print(f"TIFF count: {tiff_count}")

            if (
                gdb_count == 0
                and shp_count == 0
                and raster_count == 0
                and tiff_count == 0
            ):
                return 0
            elif (
                gdb_count == 0
                and shp_count == 1
                and raster_count == 0
                and tiff_count == 0
            ):
                return 1
            elif (
                gdb_count == 1
                and shp_count == 0
                and raster_count == 0
                and tiff_count == 0
            ):
                return 2
            elif (
                gdb_count == 0
                and shp_count == 0
                and raster_count >= 1
                and tiff_count == 0
            ):
                # Note that there might be more than one .adf file!
                return 3
            elif (
                gdb_count == 0
                and shp_count == 0
                and raster_count == 0
                and tiff_count == 1
            ):
                return 4
            elif (gdb_count + shp_count + raster_count + tiff_count) > 1:
                return 5
            else:
                return 0

        dataset_type = test_dataset_type(self.path)

        if dataset_type != 0:  # 0 would mean there is an error
            if dataset_type == 1:  # Shapefile
                dataset = next(self.path.rglob("*.shp"))
            elif dataset_type == 2:  # FileGeodatabase
                geodatabase = next(
                    self.path.rglob("*.gdb")
                )  # Path Representation of the geodatabase
                arcpy.env.workspace = str(
                    geodatabase
                )  # This can't be a Path, it has to be a path as string.
                feature_dataset_list = arcpy.ListDatasets("*", "feature")
                if not len(feature_dataset_list) > 1:
                    dataset = Path(geodatabase) / feature_dataset_list[0]
                else:
                    return
            elif dataset_type == 3:  # Raster Dataset... ArcGrid only for now.
                arcpy.env.workspace = str(
                    self.path
                )  # This can't be a Path, it has to be a path as string.
                raster_dataset_list = arcpy.ListRasters("*")
                if not len(raster_dataset_list) > 1:
                    dataset = self.path / raster_dataset_list[0]
                else:
                    return
            elif dataset_type == 4:  # Tiff dataset
                arcpy.env.workspace = str(
                    self.path
                )  # This can't be a Path, it has to be a path as string.
                tiff_dataset_list = arcpy.ListRasters("*")
                if not len(tiff_dataset_list) > 1:
                    dataset = self.path / tiff_dataset_list[0]
                else:
                    raise Exception("Too many tiff files!")
                    return
            elif dataset_type == 5:
                raise Exception("There are multiple datasets in the directory")
                return
        else:
            raise Exception("No data found in the provided dataset path. (1)")
            return

        if md.Metadata(dataset).__class__ == arcpy.metadata.Metadata:
            return Path(dataset), dataset_type
        else:
            raise Exception("No data found in the provided dataset path. (2)")
            return

    def get_dataset_metadata(self) -> tuple[str, md.Metadata, ET.Element]:
        dataset_Metadata_object = md.Metadata(self.data)

        if (
            dataset_Metadata_object.isReadOnly is None
        ):  # This means that nothing was passed
            print("A blank metadata object was created")
            return
        elif (
            dataset_Metadata_object.isReadOnly is True
        ):  # This means that a URI was passed, but it isn't a valid XML document
            print("Not a valid URI")
            return
        else:
            dataset_root_Element = ET.fromstring(dataset_Metadata_object.xml)
            return (
                dataset_Metadata_object.xml,
                dataset_Metadata_object,
                dataset_root_Element,
            )

    def tree(self):
        print(f"+ {self.path}")
        for path in sorted(self.path.rglob("*")):
            depth = len(path.relative_to(self.path).parts)
            spacer = "    " * depth
            print(f"{spacer}+ {path.name}")

    def ingest(self) -> tuple[Path, Path, Path]:
        file_server_path = Path(FILE_SERVER_PATH)
        fileserver_dir = (
            file_server_path
            / self.metadata.rights
            / self.metadata.identifier.assignedName
        )
        fileserver_dir.mkdir()

        if fileserver_dir.exists() == False:
            raise Exception("Unable to make the directory on the fileserver.")
            return

        self.fileserver_dir = fileserver_dir
        zipPath = fileserver_dir / f"{self.metadata.altTitle}.zip"
        self.fileserver_zip = zipPath
        with zipfile.ZipFile(zipPath, mode="w") as archive:
            for member in self.path.rglob("*"):
                try:
                    archive.write(member, member.relative_to(self.path))
                except Exception as error:
                    print(
                        f"Warning: There was a problem adding {member.name} to the new zipfile"
                    )
                    print(error)
                    print()
                    continue
        archive.close()

        newzipfile = zipfile.ZipFile(zipPath)
        print(f"\nContents of deliverable zipfile `{str(zipPath)}`")
        newzipfile.printdir()
        newzipfile.close()

        if zipPath.exists() == False:
            raise Exception("Unable to create the zip file.")

        # Copy the ISO metadata to the metadata directory:
        ISO_Metadata = self.path / f"{self.metadata.altTitle}_ISO.xml"

        if ISO_Metadata.exists():
            ISO_Metadata_text = ISO_Metadata.read_text()
        else:
            raise Exception("ISO Metadata does not exist!")
            return

        file_server_path = Path(FILE_SERVER_PATH)
        Fileserver_ISO_Metadata = (
            file_server_path
            / "metadata"
            / f"{self.metadata.identifier.assignedName}_ISO.xml"
        )
        Fileserver_ISO_Metadata.touch()
        Fileserver_ISO_Metadata.write_text(ISO_Metadata_text)

        if Fileserver_ISO_Metadata.exists() == False:
            raise Exception("The ISO metadata was not created on the server")
            return

        print(Fileserver_ISO_Metadata)

        self.fileserver_metadata = Fileserver_ISO_Metadata
        print(Fileserver_ISO_Metadata.absolute)
        print("\n")
        print("Does the metdata exist on the fileserver?")
        result = Fileserver_ISO_Metadata.exists()
        print(result)

        return self.fileserver_dir, self.fileserver_zip, self.fileserver_metadata


class AGSLMetadata:
    def __init__(self, dataset_metadata_tuple):
        self.xml_text: str = dataset_metadata_tuple[0]
        self.md_object: md.Metadata = dataset_metadata_tuple[1]
        self.rootElement: ET.Element = dataset_metadata_tuple[2]
        self.altTitle: str = self.get_alt_title()
        self.rights: str = self.rights_test()
        self.identifier: Identifier = Identifier()

    def save(self):
        self.md_object.xml = ET.tostring(self.rootElement)
        self.md_object.save()
        self.xml_text = self.md_object.xml
        self.rootElement: ET.Element = ET.fromstring(self.xml_text)
        self.altTitle: str = self.get_alt_title()

    def get_alt_title(self) -> str:
        rootElement = self.rootElement
        altTitle_Element = rootElement.find(SEARCH_STRING_DICT["altTitle"])
        if not altTitle_Element is None:
            return altTitle_Element.text
        else:
            raise Exception("Failed to get alt title element text.")

    def rights_test(self) -> str:
        rights_list = self.rootElement.findall(
            SEARCH_STRING_DICT["rights"]
        )  # Returns a list
        if len(rights_list) == 0:
            return "public"
        if "restricted" in rights_list[0].text.lower():
            return "restricted-uw-system"
        else:
            return "public"

    def get_existing_identifier_or_mint(self):  # Returns self.identifier

        rootElement = self.rootElement

        def check_bind(check_id):
            print(NOID_URL)
            get_request = requests.get(NOID_URL + f"+get+{check_id}")

            if get_request.status_code != 200:
                raise Exception("No response from the Noid admin.")
                return

            if "no elements bound under" in get_request.text:
                return False
            elif get_request.text == "":
                return False
            else:  # The identifier is bound
                return True

        def new_identifier():  # Returns a new Identifier object that has been minted
            new_identifier = Identifier()
            new_identifier.mint()  # This returns a response code, but we don't need it here.
            return new_identifier

        if (
            len(rootElement.findall(SEARCH_STRING_DICT["metadataFileID"])) > 0
        ):  # There is a MDFILEID
            regex = re.compile(ARK_REGEX)
            regex_result = regex.search(
                rootElement.find(SEARCH_STRING_DICT["metadataFileID"]).text
            )

            if not regex_result is None:  # The regex DID find an arkid
                existing_identifier = Identifier()
                existing_identifier.arkid = regex_result[0]

                if check_bind(existing_identifier.arkid) == True:  # The arkid is bound
                    # These only need to be set if we're using an already-bound arkid
                    existing_identifier.nameAuthorityNumber = regex_result[1]
                    existing_identifier.assignedName = regex_result[2]

                    self.identifier = existing_identifier
                else:  # the arkid is not bound
                    self.identifier = new_identifier()
            else:  # the identifier is not an arkid
                self.identifier = new_identifier()
        else:  # There is not an MDFILEID
            self.identifier = new_identifier()

        return self.identifier

    def write_identifiers(self, identifer) -> None:
        # Check to make sure identifier is an instance of Identifier:
        if not isinstance(identifer, Identifier):
            raise Exception(
                "identifier passed to write_identifiers() is not an instance of Identifier."
            )
            return

        self.identifier: Identifier = identifer

        # Generate the text strings
        ark_URI: str = REDIRECT_URL + "/ark:/" + self.identifier.arkid
        download_URI: str = f"{FILE_SERVER_URL}{self.rights}/{self.identifier.assignedName}/{self.altTitle}.zip"

        def check_if_existing_identifier(root, find_string) -> bool:
            if len(root.findall(find_string)) > 0:
                return True
            else:
                return False

        if check_if_existing_identifier(
            self.rootElement, SEARCH_STRING_DICT["identCode"]
        ):
            identCode_Element = self.rootElement.find(SEARCH_STRING_DICT["identCode"])
        else:
            dataset_idCitation_Element = self.rootElement.find(
                SEARCH_STRING_DICT["citationIdentifier"]
            )
            citId_Element = ET.SubElement(dataset_idCitation_Element, "citId", xmls="")
            identCode_Element = ET.SubElement(citId_Element, "identCode")

        identCode_Element.text = ark_URI

        # Write the Metadata File ID Code:
        if check_if_existing_identifier(
            self.rootElement, SEARCH_STRING_DICT["metadataFileID"]
        ):
            dataset_mdFileID_Element = self.rootElement.find(
                SEARCH_STRING_DICT["metadataFileID"]
            )
        else:
            dataset_mdFileID_Element = ET.SubElement(self.rootElement, "mdFileID")

        dataset_mdFileID_Element.text = f"ark:/{self.identifier.arkid}"

        # Write the Dataset URI:
        if check_if_existing_identifier(
            self.rootElement, SEARCH_STRING_DICT["datasetURI"]
        ):
            dataset_dataSetURI_Element = self.rootElement.find(
                SEARCH_STRING_DICT["datasetURI"]
            )
        else:
            dataset_dataSetURI_Element = ET.SubElement(self.rootElement, "dataSetURI")

        dataset_dataSetURI_Element.text = download_URI

        self.save()

        return

    def update_agsl_hours(self) -> None:
        # Find all the AGSL contacts
        contact_list = self.rootElement.findall(
            ".//rpCntInfo/cntHours/../.."
        )  # Returns a list

        if len(contact_list) < 1:
            print("No contacts found!")
            return rootElement
        else:
            print(f"{len(contact_list)} contacts found.")

        for contact in contact_list:
            org = contact.find("./displayName")

            if not org is None:
                org_text = org.text
            else:
                print("no org text!")
                return

            if "American Geographical" in org_text:
                hours_Element = contact.find(".//cntHours")
                hours_Element.text = "Monday – Friday: 9:00am – 4:30pm"
                print(
                    f"Updated {contact.tag}/rpCntInfo/cntHours.text to {hours_Element.text}"
                )

        self.save()

        return

    def dual_metadata_export(
        self, md_outputdir=None, md_filename=None
    ) -> tuple[Path, Path]:
        dataset_path = Path(self.md_object.uri)

        if md_outputdir is None:
            if dataset_path.parent.suffix == ".gdb":
                parent = dataset_path.parent.parent
            elif dataset_path.parent.is_dir() is False:
                print(
                    "updateMetadta.py > Dataset > dual_metadta_export() -> Parent is not a dir!"
                )
                return
            else:
                parent = dataset_path.parent
        else:
            if Path(md_outputdir).is_dir():
                parent = Path(md_outputdir)
            else:
                print(
                    "updateMetadta.py > Dataset > dual_metadta_export() -> specified output directory is not a valid directory!"
                )
                return

        if md_filename is None:  # Default
            output_ISO_Path = parent / f"{dataset_path.stem}_ISO.xml"
            output_FGDC_Path = parent / f"{dataset_path.stem}_FGDC.xml"
        else:
            output_ISO_Path = parent / f"{md_filename}_ISO.xml"
            output_FGDC_Path = parent / f"{md_filename}_FGDC.xml"

        self.md_object.exportMetadata(
            output_ISO_Path,
            "ISO19139_GML32",
            metadata_removal_option="REMOVE_ALL_SENSITIVE_INFO",
        )
        self.md_object.exportMetadata(
            output_FGDC_Path,
            "FGDC_CSDGM",
            metadata_removal_option="REMOVE_ALL_SENSITIVE_INFO",
        )

        return output_ISO_Path, output_FGDC_Path

    def bind(self, purge=False) -> requests.models.Response:

        binder = NOID_URL + "-"

        def create_bind_params(metadata) -> dict:
            root_Element = ET.fromstring(metadata.xml_text)

            download_URI = root_Element.find(SEARCH_STRING_DICT["datasetURI"]).text

            metadata_URL = (
                f"{FILE_SERVER_URL}metadata/{self.identifier.assignedName}_ISO.xml"
            )

            application_URL = f"{APPLICATION_URL}catalog/ark:-{self.identifier.arkid.replace('/','-')}"

            try:
                tmBegin = root_Element.find(".//tmBegin").text
                tmEnd = root_Element.find(".//tmEnd").text
                date_when = f"{tmBegin}/{tmEnd}"
            except:
                date_when = root_Element.find(".//tmPosition").text

            time_now = datetime.now().replace(microsecond=0).isoformat()

            parameter_dictionary = {
                "who": f"{metadata.md_object.credits}",
                "what": f"{metadata.md_object.title}",
                "when": f"{date_when}",
                "where": f"{application_URL}",
                "meta-who": "University of Wisconsin-Milwaukee Libraries",
                "meta-when": f"{time_now}",
                "meta-uri": f"{metadata_URL}",
                "rights": f"{self.rights}",
                "download": f"{download_URI}",
            }
            return parameter_dictionary

        def construct_bind_request(arkid, bind_params) -> str:
            param_string = ""
            if purge == False:
                for key, value in bind_params.items():
                    param_string = (
                        param_string + f'bind set {arkid} {key} "{value}"' + "\n"
                    )
            else:
                print("### PURGE PURGE PURGE ###")
                for key, value in bind_params.items():
                    param_string = param_string + f"bind purge {arkid} {key}" + "\n"

            print(param_string)
            return param_string

        bind_params = create_bind_params(self)
        bind_params_commands = construct_bind_request(
            self.identifier.arkid, bind_params
        )

        try:
            r = requests.post(binder, data=bind_params_commands)
        except:
            print(
                "There was a connection error! Check the URL you used to bind the id!"
            )
            return
        if r.status_code != 200:
            print("There was a non-200 status code from the binder: " + r.status_code)
            return
        else:
            return r


class Identifier:

    arkid: str
    nameAuthorityNumber: str
    assignedName: str

    def mint(self) -> requests.models.Response:

        minter = NOID_URL + "mint+1"

        try:
            mint_request = requests.get(minter)
        except Exception as ex:
            print(ex)
            return

        if mint_request.status_code != 200:
            raise Exception(f"mint request status code = {mint_request.status_code}")
            return mint_request
        else:
            regex = re.compile(ARK_REGEX)
            regex_result = regex.search(mint_request.text)
            if not regex_result is None:
                self.arkid = regex_result[0]
                self.nameAuthorityNumber = regex_result[1]
                self.assignedName = regex_result[2]
                print(f"mint request status code = {mint_request.status_code}")
                return mint_request
            else:
                raise Exception("Failed to mint an arkid!")
                print(f"mint request status code = {mint_request.status_code}")
                return mint_request


def main() -> None:
    """Main function."""

    print(f"Application URL: {APPLICATION_URL}")
    print(f"File Server URL: {FILE_SERVER_URL}")
    print(f"Redirect URL: {REDIRECT_URL}")
    print(f"NOID URL: {NOID_URL}")
    print(f"File Server Path: {FILE_SERVER_PATH}")

    # Test creating the Dataset object:
    dataset = Dataset(
        r"S:\GeoBlacklight\project-files\Test_Fixture\MilwaukeeCounty_MCTSStops_2018"
    )
    print(f"The class of dataset is {dataset.__class__}")
    print(f"\nThe dataset path is: {dataset.path}")
    print(f"The dataset within the path is: {dataset.data}\n")

    # Test creating the Metadata object:
    dataset_metadata = dataset.metadata
    print(f"The class of dataset_metadata is {dataset_metadata.__class__}")
    print(f"The dataset's alt title is {dataset_metadata.altTitle}")
    print(f"The rights string for the dataset is: {dataset.metadata.rights}\n")

    # Test fetching existing identifiers:
    existing_identifier = dataset_metadata.get_existing_identifier()
    print(f"The class of existing_identifier is {existing_identifier.__class__}")
    print(f"The existing identifier's Arkid is {existing_identifier.arkid}")

    # Test creating and writing the identifiers:
    dataset_metadata.write_identifiers(existing_identifier)
    print(
        f"The Metadata File ID is: {ET.fromstring(dataset_metadata.xml_text).find(SEARCH_STRING_DICT['metadataFileID']).text}"
    )
    print(
        f"The Citation ID is: {ET.fromstring(dataset_metadata.xml_text).find(SEARCH_STRING_DICT['identCode']).text}"
    )
    print(
        f"The Dataset URI is: {ET.fromstring(dataset_metadata.xml_text).find(SEARCH_STRING_DICT['datasetURI']).text}\n"
    )

    # Test updating agsl hours:
    dataset_metadata.update_agsl_hours()
    print("\n")

    # Test Dual Metadata Export:
    (
        ISO_dir,
        FGDC_dir,
    ) = (
        dataset_metadata.dual_metadata_export()
    )  # returns the 2 paths of the exported md
    print("Result of Dual Metadata Export:")
    print(ISO_dir)
    print(FGDC_dir)
    print("\n")

    # Test the binder:
    print(
        f"The ancitipated NOID URL is: {NOID_URL}get+{dataset_metadata.identifier.arkid}"
        + "\n"
    )

    print("The bind request sent to NOID:")
    r = AGSLMetadata.bind(dataset_metadata, "restricted-uw-system")

    print(f"Bind request status code: {r.status_code}\n")

    # Test the zipfile builder:
    dataset.tree()
    ingestPath = dataset.ingest()

    # Success!
    print("Success!")


if __name__ == "__main__":
    main()
