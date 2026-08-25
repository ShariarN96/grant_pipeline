"""Ingest posted grant opportunities from simpler.grants.gov into the ADLS bronze layer."""

import json
import os
from dataclasses import dataclass
from datetime import date, timedelta

import requests
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

BASE_URL = "https://api.simpler.grants.gov"
FILESYSTEM = "bronze"
REQUEST_TIMEOUT = 30


@dataclass(frozen=True)
class IngestConfig:
    api_key: str
    storage_account: str
    storage_key: str

    @classmethod
    def from_env(cls) -> "IngestConfig":
        load_dotenv()
        return cls(
            api_key=os.environ["SIMPLER_API_KEY"],
            storage_account=os.environ["ADLS_ACCOUNT_NAME"],
            storage_key=os.environ["ADLS_ACCOUNT_KEY"],
        )


def fetch_opportunities(api_key: str, since: str) -> list[dict]:
    """Fetch posted grant opportunities with a post date on/after `since` (YYYY-MM-DD)."""
    payload = {
        "filters": {
            "opportunity_status": {"one_of": ["posted"]},
            "post_date": {"start_date": since},
        },
        "pagination": {
            "page_offset": 1,
            "page_size": 100,
            "sort_order": [{"order_by": "post_date", "sort_direction": "descending"}],
        },
    }
    response = requests.post(
        f"{BASE_URL}/v1/opportunities/search",
        headers={"X-API-Key": api_key, "Content-Type": "application/json"},
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    print("Status:", response.status_code)

    records = response.json().get("data", [])
    print(f"Opportunities posted since {since}: {len(records)}\n")

    for i, opp in enumerate(records, start=1):
        print("=" * 80)
        print(f"OPPORTUNITY {i} of {len(records)}")
        print("=" * 80)
        print(json.dumps(opp, indent=2, default=str))
        print()

    return records


def upload_opportunities(
    records: list[dict], storage_account: str, storage_key: str, ingest_date: str
) -> str:
    """Write opportunities as NDJSON to ADLS, partitioned by ingest date. Returns the blob path."""
    file_path = f"grants_gov/ingest_date={ingest_date}/opportunities.json"
    raw_bytes = "\n".join(json.dumps(record, default=str) for record in records).encode("utf-8")

    service = DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=storage_key,
    )
    file_client = service.get_file_system_client(FILESYSTEM).get_file_client(file_path)
    file_client.upload_data(raw_bytes, overwrite=True)  # overwrite = idempotent re-runs
    print(f"\n✅ Saved {len(records)} records to: {FILESYSTEM}/{file_path}")
    return file_path


def ingest_opportunities(days_back: int = 1) -> int:
    """Fetch recent opportunities and persist them to ADLS. Entry point for DAG tasks."""
    config = IngestConfig.from_env()
    since = (date.today() - timedelta(days=days_back)).isoformat()
    ingest_date = date.today().isoformat()

    records = fetch_opportunities(config.api_key, since)
    upload_opportunities(records, config.storage_account, config.storage_key, ingest_date)
    return len(records)


if __name__ == "__main__":
    ingest_opportunities()