import os
import json
import requests
from datetime import date, timedelta
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

load_dotenv()  # load environment variables from .env file
# ---- config (read secrets from environment, never hardcode) ----
API_KEY = os.environ["SIMPLER_API_KEY"]              # set this in your shell
STORAGE_ACCOUNT = os.environ["ADLS_ACCOUNT_NAME"]    # your storage account name
STORAGE_KEY = os.environ["ADLS_ACCOUNT_KEY"]         # storage account access key
FILESYSTEM = "bronze"                                # your container / filesystem

BASE = "https://api.simpler.grants.gov"
headers = {"X-API-Key": API_KEY, "Content-Type": "application/json"}

# ---- 1. fetch the data ----
since = (date.today() - timedelta(days=1)).isoformat()   # yesterday

payload = {
    "filters": {
        "opportunity_status": {"one_of": ["posted"]},
        "post_date": {"start_date": since}
    },
    "pagination": {
        "page_offset": 1,
        "page_size": 100,
        "sort_order": [{"order_by": "post_date", "sort_direction": "descending"}]
    }
}

resp = requests.post(f"{BASE}/v1/opportunities/search", headers=headers, json=payload)
resp.raise_for_status()                              # stop early if the call failed
print("Status:", resp.status_code)

data = resp.json()
records = data.get("data", [])
print(f"Opportunities posted since {since}: {len(records)}\n")

# ---- 2. print to terminal so you can view the data ----
for i, opp in enumerate(records, start=1):
    print("=" * 80)
    print(f"OPPORTUNITY {i} of {len(records)}")
    print("=" * 80)
    print(json.dumps(opp, indent=2, default=str))
    print()

# ---- 3. save the RAW json to ADLS Gen2 (partitioned by ingest date) ----
ingest_date = date.today().isoformat()
# NDJSON: one opportunity per line — no indent, no outer [ ], no commas between objects
raw_bytes = "\n".join(json.dumps(opp, default=str) for opp in records).encode("utf-8")
file_path = f"grants_gov/ingest_date={ingest_date}/opportunities.json"

service = DataLakeServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
    credential=STORAGE_KEY,
)
file_system = service.get_file_system_client(FILESYSTEM)

file_client = file_system.get_file_client(file_path)
file_client.upload_data(raw_bytes, overwrite=True)   # overwrite = idempotent re-runs

print(f"\n✅ Saved {len(records)} records to: {FILESYSTEM}/{file_path}")