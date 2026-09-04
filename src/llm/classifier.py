"""
Classify grant opportunities into research domains using the OpenAI API,
reading from silver_grant and writing results to enrichment_grant_category.

Categories (controlled vocabulary):
  - "Arts, Law and Social Sciences"  (arts, law, social sciences, business,
                                       architecture, humanities)
  - "STEM"
  - "Health Sciences"
  - "Multidisciplinary"              (falls within two or more of the above)

SETUP:
    pip install openai snowflake-connector-python python-dotenv
    # .env must contain OPENAI_API_KEY and your Snowflake creds (reuse existing)

RUN:
    python classify_grants.py            # classify all not-yet-classified grants
    python classify_grants.py --all      # re-classify everything (rebuild table)
    python classify_grants.py --limit 20 # test on first 20 only
"""

import os
import sys
import argparse

from dotenv import load_dotenv
from openai import OpenAI
import snowflake.connector


# ---- controlled vocabulary -------------------------------------------------
CATEGORIES = [
    "Arts, Law and Social Sciences",
    "Science, Technology, Engineering, Mathematics (STEM)",
    "Health Sciences",
    "Multidisciplinary",
]
load_dotenv()

MODEL = "gpt-4o-mini"          # cheap + good enough for classification
BATCH_WRITE_SIZE = 50          # flush results to Snowflake every N rows


PROMPT = """You are classifying a U.S. federal grant opportunity into exactly ONE \
research domain. Choose from this list ONLY:

1. "Arts, Law and Social Sciences" — arts, law, social sciences, business, \
architecture, humanities.
2. "Science, Technology, Engineering, Mathematics (STEM)" — science, technology, engineering, mathematics (physical sciences, \
computing, engineering, energy, environment, agriculture, etc.).
3. "Health Sciences" — medicine, clinical research, public health, biomedical \
research, nursing, disease-specific research.
4. "Multidisciplinary" — use ONLY if the opportunity clearly spans TWO OR MORE \
of the three domains above.

Base your decision ONLY on the information provided below. Do not use outside \
knowledge. If it does not clearly fit one domain and is not clearly \
multidisciplinary, pick the single best-fitting domain.

Agency-assigned category: {funding_category}
Title: {title}
Summary: {summary}

Respond with ONLY the exact category name from the list, nothing else."""


def get_conn():
    """Snowflake connection from env vars (reuse your existing pipeline creds)."""
    load_dotenv()
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),   # whichever auth you use
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "GRANTS"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def ensure_table(cur):
    """Create the enrichment table if it doesn't exist."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS silver.enrichment_grant_category (
            opportunity_id   STRING,
            research_domain  STRING,
            enriched_at      TIMESTAMP_NTZ
        )
    """)


def fetch_rows(cur, classify_all: bool, limit: int):
    """Get opportunities to classify.

    Default: only those not yet in the enrichment table (incremental).
    --all: every row (rebuild).
    """
    base = """
        SELECT s.opportunity_id, s.opportunity_title,
               s.summary_description, s.funding_category
        FROM silver.silver_grant s
    """
    if not classify_all:
        base += """
        LEFT JOIN silver.enrichment_grant_category e
            ON s.opportunity_id = e.opportunity_id
        WHERE e.opportunity_id IS NULL
        """
    if limit:
        base += f" LIMIT {limit}"
    cur.execute(base)
    cols = [c[0].lower() for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def classify(client, row):
    """Classify one grant. Returns (label, prompt_tokens, completion_tokens)."""
    prompt = PROMPT.format(
        funding_category=row.get("funding_category") or "(none)",
        title=row.get("opportunity_title") or "(no title)",
        summary=(row.get("summary_description") or "(no summary)")[:2000],  # cap length
    )
    try:
        resp = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=15,
        )
        label = resp.choices[0].message.content.strip().strip('"')
        # validate against controlled vocabulary; reject anything off-list
        if label not in CATEGORIES:
            match = next((c for c in CATEGORIES if c.lower() in label.lower()), None)
            label = match or "Multidisciplinary"
        return label, resp.usage.prompt_tokens, resp.usage.completion_tokens
    except Exception as e:
        print(f"    [error {row['opportunity_id']}: {e}]", file=sys.stderr)
        return None, 0, 0     # None -> not written, retried next run


def write_batch(cur, batch):
    """Insert a batch of (opportunity_id, research_domain) into the table."""
    if not batch:
        return
    cur.executemany(
        """INSERT INTO silver.enrichment_grant_category
           (opportunity_id, research_domain, enriched_at)
           VALUES (%s, %s, CURRENT_TIMESTAMP())""",
        batch,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true",
                    help="re-classify every row (default: only new ones)")
    ap.add_argument("--limit", type=int, default=0, help="classify only first N")
    args = ap.parse_args()

    if not os.environ.get("OPENAI_API_KEY"):
        sys.exit("ERROR: set OPENAI_API_KEY in your environment/.env")
    client = OpenAI()

    conn = get_conn()
    cur = conn.cursor()
    ensure_table(cur)

    # if --all, clear the table first so we rebuild cleanly
    if args.all:
        cur.execute("TRUNCATE TABLE silver.enrichment_grant_category")
        conn.commit()

    rows = fetch_rows(cur, classify_all=args.all, limit=args.limit)
    print(f"To classify: {len(rows)} grants")
    if not rows:
        print("Nothing to do.")
        cur.close(); conn.close()
        return

    batch, done = [], 0
    tok_in = tok_out = 0
    counts = {c: 0 for c in CATEGORIES}

    for i, row in enumerate(rows, 1):
        label, pin, pout = classify(client, row)
        tok_in += pin; tok_out += pout
        if label is None:
            continue                      # failed -> skip, retried next run
        counts[label] += 1
        batch.append((row["opportunity_id"], label))
        done += 1

        if len(batch) >= BATCH_WRITE_SIZE:
            write_batch(cur, batch); conn.commit()
            print(f"  written {done}/{len(rows)}")
            batch = []

    write_batch(cur, batch); conn.commit()     # final flush

    print("\n================ SUMMARY ================")
    print(f"  Classified:   {done}/{len(rows)}")
    for c in CATEGORIES:
        print(f"    {c:<32} {counts[c]}")
    print(f"  Tokens:       in={tok_in}  out={tok_out}")
    print("=========================================")

    cur.close(); conn.close()


if __name__ == "__main__":
    main()
    
    
    
def run_classification(classify_all=False, limit=0):
    """Callable entry point for Airflow (no argparse)."""
    load_dotenv()
    if not os.environ.get("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY not set")
    client = OpenAI()
    conn = get_conn()
    cur = conn.cursor()
    ensure_table(cur)
    if classify_all:
        cur.execute("TRUNCATE TABLE silver.enrichment_grant_category")
        conn.commit()

    rows = fetch_rows(cur, classify_all=classify_all, limit=limit)
    if not rows:
        cur.close(); conn.close()
        return 0

    batch, done = [], 0
    for row in rows:
        label, _, _ = classify(client, row)
        if label is None:
            continue
        batch.append((row["opportunity_id"], label))
        done += 1
        if len(batch) >= BATCH_WRITE_SIZE:
            write_batch(cur, batch); conn.commit(); batch = []
    write_batch(cur, batch); conn.commit()
    cur.close(); conn.close()
    return done