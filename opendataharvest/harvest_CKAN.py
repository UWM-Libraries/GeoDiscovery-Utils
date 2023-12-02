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
packge_list_response = requests.get('https://data.milwaukee.gov/api/3/action/package_list')
assert packge_list_response.status_code == 200

# Use the json module to load CKAN's response into a dictionary.
response_dict = json.loads(packge_list_response.content)

# Check the contents of the response.
assert response_dict['success'] is True
result: list = response_dict['result']
pprint.pprint(result)

