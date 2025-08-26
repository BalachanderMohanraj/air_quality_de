import requests
import yaml
import time
import pandas as pd
import json
import csv
from pprint import pprint
from dotenv import load_dotenv
import os
load_dotenv()  
API_KEY = os.getenv("OPENAQ_API_KEY")
if not API_KEY:
    raise ValueError("Missing OPENAQ_API_KEY in .env file")
headers = {"X-API-Key": API_KEY}
BASE_URL = "https://api.openaq.org/v3/locations"
params = {
    "country": "IN",
    "limit": 1000,    
    "sort": "desc",
    "order_by": "id",
    "page" : 1
}
all_results = []
while True:
    print(f"Fetching page {params['page']}...")
    response = requests.get(BASE_URL, params=params, headers=headers)
    if response.status_code != 200:
        print(f"Error: {response.status_code}")
        break
    data = response.json()
    results = data.get("results", [])
    if not results:
        print("No more results. Done.")
        break
    all_results.extend(results)
    if len(results) < params["limit"]:
        print("Last page reached.")
        break
    params["page"] += 1
    time.sleep(0.5)  
print(f"Total locations fetched: {len(all_results)}")
df = pd.DataFrame(all_results)
if response.status_code != 200:
    try:
        error_msg = response.json().get("message", "")
    except Exception:
        error_msg = response.text
    raise Exception(f"API Error {response.status_code}: {error_msg}")
data = response.json()
file_path_d1 = r'G:\air-quality-project\air_quality_de\data\Raw\sample_data.json'
with open(file_path_d1,'w+') as file:
    json.dump(data, file ,indent=4, sort_keys=True)
print("Data written to file")
with open(file_path_d1,'r') as file:
    data = json.load(file)
dict_keys_lst = list(data.keys())
results = data[dict_keys_lst[1]]
res_results = results
def flatten_json(res_results, parent_key='', sep='.'):
    """Recursively flatten JSON"""
    items = {}
    if isinstance(res_results, dict):
        for key, value in res_results.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            items.update(flatten_json(value, new_key, sep=sep))
    elif isinstance(res_results, list):
        for i, value in enumerate(res_results):
            new_key = f"{parent_key}[{i}]"
            items.update(flatten_json(value, new_key, sep=sep))
        if not res_results:
            items[parent_key] = None 
    else:
        items[parent_key] = res_results
    return items
all_flat = [flatten_json(record) for record in data["results"]]
fieldnames = sorted(set(k for d in all_flat for k in d.keys()))
profiled_data_csv = r'G:\air-quality-project\air_quality_de\data\Raw\locations.csv'
with open(profiled_data_csv, "w", newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for row in all_flat:
        writer.writerow(row)
print("Flattened data written to profiled_data.csv")