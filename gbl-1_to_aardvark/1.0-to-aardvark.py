import json
import csv
import os

from datetime import datetime
from pathlib import Path

# Manual changes before run

# Path to crosswalk definition
dir_crosswalk = Path('crosswalk.csv')
# add directory of JSON files in the 1.0 schema
dir_old_schema = Path(r"S:\GeoBlacklight\project-files\opengeometadata_GBL-1\opengeometadata\edu.princeton")
#add directory for new JSON files in the Aardvark schema
dir_new_schema = Path('aardvark/princeton/')

#Default values
RESOURCE_CLASS_DEFAULT = "Maps"
PLACE_DEFAULT = ""

# Load the crosswalk.csv and make it a dictionary
crosswalk = {}
with open(dir_crosswalk, encoding='utf8') as f:
    reader = csv.reader(f)
    fields = next(reader)
    for record in reader:
        old = record[0]
        new = record[1]
        crosswalk[old] = new

# Function to convert fields that ends with '_sm' to an array
# SRA - Moved this to before the other function because it calls this function
def string2array(data_dict):
    for key in data_dict.keys():
        suffix = key.split('_')[-1]
        if suffix == 'sm' or suffix == 'im':
            val = data_dict[key]
            if type(val) != list:
                data_dict[key] = [val]
    return data_dict

def check_required(data_dict):
    # Check if required fields are present
    requirements = ["dct_publisher_sm", "dct_spatial_sm", "gbl_mdVersion_s", "dct_title_s", "gbl_resourceClass_sm", "id", "gbl_mdModified_dt"]
    
    for req in requirements:
        if req not in data_dict:
            print (f"Requirement {req} is not present...")
            
            if req == "gbl_resourceClass_sm":
                if "dc_type_s" not in data_dict:
                    print(f"No dc_type_s information found.")
                    data_dict["gbl_resourceClass_sm"] = [RESOURCE_CLASS_DEFAULT]
                elif data_dict["dc_type_s"] == "Dataset":
                    data_dict["gbl_resourceClass_sm"] = ["Datasets"]
                    print(f"Replaced dc_type_s:Dataset with gbl_resourceClass_sm:Datasets")
                elif data_dict["dc_type_s"] == "Image":
                    data_dict["gbl_resourceClass_sm"] = ["Imagery"]
                    print(f"Replaced dc_type_s:Dataset with gbl_resourceClass_sm:Imagery")
                else:
                    data_dict["gbl_resourceClass_sm"] = [RESOURCE_CLASS_DEFAULT] # change if needed!
                    print(f"Replaced dc_type_s:Null with gbl_resourceClass_sm:{RESOURCE_CLASS_DEFAULT}")
                    
            elif req == "dct_spatial_sm":
                data_dict["dct_spatial_sm"] = [PLACE_DEFAULT]
                print(f"Replaced dct_spatial_sm:Null with dct_spatial_sm:{PLACE_DEFAULT}")
                
            elif req == "gbl_mdModified_dt":
                data_dict["gbl_mdModified_dt"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
                
            elif req == "dct_publisher_sm":
                print(f"No Publisher information found.")
                if "dct_creator_sm" not in data_dict:
                    print(f"No Creator information found.")
                elif data_dict["dct_creator_sm"] != "":
                    data_dict["dct_publisher_sm"] = data_dict["dct_creator_sm"]
                    print(f"Replaced dct_publisher_sm:Null with dct_publisher_sm:{data_dict['dct_creator_sm']}")
                else:
                    continue
    return

def remove_deprecated(data_dict):
    # Remove the deprecated fields from the output Aardvark
    deprecated = ["dc_type_s", "layer_geom_type_s", "dct_isPartOf_sm", "uw_supplemental_s", "uw_notice_s", "uuid"]
    for field in deprecated:
        if field in data_dict:
            data_dict.pop(field)
            print(f"Removed the deprecated {field} field.")
    
    return

# Function to update the metadata schema
def schema_update(filepath):
    # Open the JSON file with schema GBL 1.0
    with open(filepath, encoding='utf8') as fr:
        # Load its content and make a new dictionary
        data = json.load(fr)

        if data.__class__ != dict:
            return

        # Loop over crosswalk to change dictionary keys
        for old_schema, new_schema in crosswalk.items():
            if old_schema in data:
                data[new_schema] = data.pop(old_schema)
        
        # Change the metadata type:
        data["gbl_mdVersion_s"] = "Aardvark"

        # Remove geoblacklight_version
        if "geoblacklight_version" in data:
            data.pop("geoblacklight_version")
        
        # Check for required fields:
        check_required(data)
        
        # Remove deprecated fields
        remove_deprecated(data)
        
    # check for multi-valued fields - if so, convert its value to an array
    data = string2array(data)

    # Write updated JSON to a new folder
    if filepath.name != "geoblacklight.json":
        filepath_updated = dir_new_schema / filepath.name
    else:
        id_filepath = Path(f"{data['id']}.json")
        filepath_updated = dir_new_schema / id_filepath

    with open(filepath_updated, 'w') as fw:
        j = json.dumps(data, indent=2)
        fw.write(j)


# Collect all JSON files in a list
# Iterate the list to update metadata schema
def list_all_json(rootdir): # -> list[pathlib.Path]
    '''List All .json files in a given dir'''

    rootdir = Path(rootdir)
    files = []
    for path in sorted(rootdir.rglob("*.json")):
        # skip layers.json
        if path.name == 'layers.json':
            print(f"Skipped {path.name}")
            continue
        else:
            files.append(Path(path))

    return files

# Main function:
def main_function():
    if not dir_new_schema.exists():
        dir_new_schema.mkdir()

    files = list_all_json(dir_old_schema)
    for file in files:
        print(f'Executing {file} ...')
        schema_update(file)

if __name__ == '__main__':
    main_function()
