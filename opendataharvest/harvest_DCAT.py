import yaml
import json
import requests

x = yaml.safe_load(open("opendataharvest/OpenDateSites.yaml", "r"))

# print(x["Sites"].keys())

catalog_list = []

for site in x["TestSites"].keys():
    r = requests.get(x["TestSites"][site]["SiteURL"])
    if r.status_code != 200:
        break
    else:
        try:
            catalog_json = json.loads(r.content)
        except JSONDecodeError:
            print(f"The content from {site} is not a valid JSON document.")
            continue

        catalog_list.append(catalog_json)
        print(len(catalog_list))
        
for catalog in catalog_list:
    for dataset in catalog["dataset"]:
        print(dataset["title"])
        
           

