import requests
import json
from datetime import datetime
from azure.storage.filedatalake import DataLakeServiceClient


# =============================
# Grants.gov API
# =============================

SEARCH_URL = "https://api.grants.gov/v1/api/search2"

rows = 100
start_record = 0

all_opportunities = []


while True:

    payload = {
        "rows": rows,
        "startRecordNum": start_record,
        "keyword": "",
        "oppStatuses": "posted|forecasted",
        "agencies": "",
        "fundingCategories": "",
        "eligibilities": "",
        "aln": ""
    }

    response = requests.post(
        SEARCH_URL,
        json=payload,
        timeout=30
    )

    response.raise_for_status()

    data = response.json()

    opportunities = data["data"]["oppHits"]

    all_opportunities.extend(opportunities)

    total_results = data["data"]["hitCount"]

    print(
        f"Downloaded {len(all_opportunities)} / {total_results}"
    )


    if len(all_opportunities) >= total_results:
        break


    start_record += rows



print(
    f"Total collected: {len(all_opportunities)}"
)



# =============================
# Azure ADLS Gen2
# =============================

storage_account_name = "grantproject"
storage_account_key = ""
container_name = "bronze"


today = datetime.now().strftime("%Y-%m-%d")

directory_name = (
    f"bronze/grants/ingestion_date={today}"
)

file_name = "all_grants.json"



# =============================
# Connect to ADLS
# =============================

service_client = DataLakeServiceClient(
    account_url=f"https://{storage_account_name}.dfs.core.windows.net",
    credential=storage_account_key
)


file_system_client = service_client.get_file_system_client(
    container_name
)



# =============================
# Create ADLS directory
# =============================

directory_client = file_system_client.get_directory_client(
    directory_name
)

try:
    directory_client.create_directory()
except Exception:
    pass



# =============================
# Upload directly from memory
# =============================

file_client = directory_client.get_file_client(
    file_name
)


json_data = json.dumps(
    all_opportunities,
    indent=4
)


file_client.upload_data(
    json_data,
    overwrite=True
)


print("Successfully uploaded directly to ADLS")