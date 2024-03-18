# This python file will loop through a directory of OGM Aardvark metadata
# and check with NOID if the Ark ID is properly bound and redirected.

import json
import requests
import re

from pathlib import Path

AARDVARK_DIR = "uwm_fixture/Aardvark"

NOID_PROD = r"https://digilib-admin.uwm.edu/noidu_gmgs"
NOID_DEV = r"https://digilib-dev.uwm.edu/noidu_gmgs"

# Loop through the files in the dir
dir = Path(AARDVARK_DIR)
assert dir.is_dir()


def searchForID(text) -> list:
    # Will match the whole ID
    # Group 1 will be the *Name Assigning Authority Number*
    # Group 2 will be the *Assigned Name*
    arkregex = re.compile(r"(\d{5})\/(\w{11})")
    arkid = arkregex.search(text)
    return arkid


def listMetadata(dir) -> list[str]:
    dir = Path(dir)  # ensure is Path object
    mdlist = []
    for mdfilePath in dir.rglob("*"):
        with open(mdfilePath, "r", encoding="utf8") as mdfile:
            md = json.load(mdfile)

            assert md.__class__ == dict

            mdlist.append(searchForID(md["dct_identifier_sm"][0])[0])

    return mdlist


# Fetch the current NOID data for the given ID
def NOIDfetch(arkid) -> str:
    # Example: https://digilib-admin.uwm.edu/noidu_gmgs?fetch+77981/gmgs0c4sj3x
    fetchURL = NOID_PROD + "?fetch+" + arkid
    fetch_r = requests.get(fetchURL)

    # TODO: Handle a non 200 status code
    assert fetch_r.status_code == 200

    print(fetch_r.text)


# fetch_result = NOIDfetch(searchForID(test_text)[0])

# Until Karl is able to get a cert for the digitlib-admin domain, we can use this return text for testing:
fetch_result = """
id:    77981/gmgs0c4sj3x hold
Circ:  i|20230925154243|@10.62.92.248 apache/apache|157
download: https://geodata.uwm.edu/public/gmgs0c4sj3x/ClarkCounty_CensusBlocks_2002.zip
meta-uri: https://geodata.uwm.edu/metadata/gmgs0c4sj3x_ISO.xml
meta-when: 2023-09-25T15:42:43
meta-who: University of Wisconsin-Milwaukee Libraries
rights: public
what: Census Blocks Clark County, Wisconsin 2002
when: 2002-01-01T00:00:00
where: https://geodiscovery.uwm.edu/catalog/ark:-77981-gmgs0c4sj3x
who: Legislative Technology Services Bureau
"""

if __name__ == "__main__":
    print(listMetadata(AARDVARK_DIR))
