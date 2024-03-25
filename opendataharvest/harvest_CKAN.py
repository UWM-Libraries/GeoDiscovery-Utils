# /api/3/action/
# Use package_list to get a list of all the ids
# Get a JSON representation of a dataset with package_show?id=`id`
# simple URL access, e.g. https://data.milwaukee.gov/dataset/2022-partisan-primary
# where 2022-partisan-primary is the "id" from the package_list

#!/usr/bin/env python
import requests
import json
import pprint

# Make the HTTP request.
packge_list_response = requests.get(
    "https://data.milwaukee.gov/api/3/action/package_search?fq=groups:maps"
)
assert packge_list_response.status_code == 200

# Use the json module to load CKAN's response into a dictionary.
response_dict = json.loads(packge_list_response.content)

# Check the contents of the response.
assert response_dict["success"] is True
result = response_dict["result"]
dataset_list = []
for dataset in result["results"]:
    dataset_list.append(dataset["name"])

print(dataset_list)

### Let's assume we will be working off a dataset list defined in the YAML
### for now, we can use dataset_list

for dataset in dataset_list:
    # make an api call for that record:
    dataset_response = requests.get(
        f"https://data.milwaukee.gov/api/3/action/package_show?id={dataset}"
    )
    assert dataset_response.status_code == 200
    print(f"{dataset}:")

    dataset_dict = json.loads(dataset_response.content)

    for resource in dataset_dict["result"]["resources"]:  # -> dict

        pprint.pprint(resource["format"])
