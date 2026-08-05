"""
See what USAspending data looks like.

Run:
    pip install requests
    python explore_usaspending.py

No API key / auth needed.
"""

import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://api.usaspending.gov"


def make_session():
    """Session with retry/backoff so a transient 5xx doesn't kill the run."""
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.0,  # 1s, 2s, 4s, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def section(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def get_last_updated(session):
    """The watermark field - this is what enables incremental loading."""
    section("1) /awards/last_updated/  (your incremental watermark)")
    r = session.get(f"{BASE}/api/v2/awards/last_updated/", timeout=30)
    r.raise_for_status()
    print(json.dumps(r.json(), indent=2))


def get_award_sample(session, limit=5):
    """
    Main search endpoint. award_type_codes 02-05 = assistance (grant-like):
      02 block grant | 03 formula grant | 04 project grant | 05 cooperative agreement
    (Use A/B/C/D for contracts instead.)
    """
    payload = {
        "filters": {
            "award_type_codes": ["02", "03", "04", "05"],
            "time_period": [{"start_date": "2024-10-01", "end_date": "2025-09-30"}],
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Award Amount",
            "Awarding Agency",
            "Awarding Sub Agency",
            "Start Date",
            "End Date",
            "Assistance Listings",  # the CFDA linkage
        ],
        "page": 1,
        "limit": limit,
        "sort": "Award Amount",
        "order": "desc",
    }

    r = session.post(
        f"{BASE}/api/v2/search/spending_by_award/", json=payload, timeout=60
    )
    r.raise_for_status()
    data = r.json()
    results = data.get("results", [])

    # --- top-level shape ---
    section("2) Response shape")
    print("Top-level keys:", list(data.keys()))
    print("page_metadata:", json.dumps(data.get("page_metadata", {}), indent=2))
    print(f"results: {len(results)} records")

    if not results:
        print("\nNo results - try widening the time_period or changing award types.")
        return data

    # --- one full record ---
    section("3) First record, in full")
    print(json.dumps(results[0], indent=2))

    # --- every field + its python type ---
    section("4) Fields present (name -> type -> sample value)")
    for k, v in results[0].items():
        sample = str(v)
        if len(sample) > 50:
            sample = sample[:47] + "..."
        print(f"  {k:<28} {type(v).__name__:<8} {sample}")

    # --- compact overview of several ---
    section(f"5) Compact view of {len(results)} records")
    for i, rec in enumerate(results, 1):
        name = str(rec.get("Recipient Name"))[:32]
        amt = rec.get("Award Amount")
        agency = str(rec.get("Awarding Agency"))[:28]
        print(f"  {i}. {name:<34} ${amt:<15} {agency}")

    # --- save raw for later poking ---
    with open("usaspending_sample.json", "w") as f:
        json.dump(data, f, indent=2)
    section("Saved")
    print("Full response written to usaspending_sample.json")
    return data


if __name__ == "__main__":
    s = make_session()
    get_last_updated(s)
    get_award_sample(s)