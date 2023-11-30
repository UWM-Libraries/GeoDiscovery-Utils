import yaml
import json
import requests

x = yaml.safe_load(open("opendataharvest/OpenDateSites.yaml", "r"))

# print(x["Sites"].keys())

for site in x["Sites"].keys():
    r = requests.get(x["Sites"][site]["SiteURL"])
    if r.status_code != 200:
        break
    else:
        catalog_json =json.loads(r.content)
        print(catalog_json)
        exit
