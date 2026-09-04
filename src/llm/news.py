"""
Grant opportunity digest generator (styled).

Deterministic SQL selects the opportunities; the LLM writes only grounded
one-line summaries; Python wraps everything in a styled HTML template.

SETUP:
    pip install openai snowflake-connector-python python-dotenv
    # .env: OPENAI_API_KEY + Snowflake creds

RUN:
    uv run newsletter.py --days-back 1 --dry-run > digest.html   # view in browser
    uv run newsletter.py --days-back 7 --dry-run                 # print to terminal
    uv run newsletter.py --category "Health Sciences" --dry-run
"""

import os
import sys
import json
import argparse
from datetime import date
from dotenv import load_dotenv
from openai import OpenAI
import snowflake.connector

load_dotenv()

MODEL = "gpt-4o-mini"
PRICE_IN = 0.15 / 1_000_000
PRICE_OUT = 0.60 / 1_000_000


# ============================================================
# Snowflake connection
# ============================================================
def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "GRANTS"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


# ============================================================
# 1. Deterministic selection (SQL decides what's in the digest)
# ============================================================
def fetch_opportunities(cur, days_back: int, category: str):
    """Opportunities POSTED within the last `days_back` days (newly added)."""
    sql = f"""
        SELECT
            o.opportunity_id,
            o.opportunity_title,
            o.opportunity_url,
            a.agency_name,
            o.research_domain,
            f.post_date,
            f.close_date,
            f.award_ceiling,
            o.summary_description
        FROM gold.fact f
        JOIN gold.dim_opps o
            ON f.opportunity_id = o.opportunity_id
        LEFT JOIN gold.dim_agency a
            ON f.agency_code = a.agency_code
        WHERE o.opportunity_status = 'posted'
          AND o.dbt_valid_to = '9999-12-31'
          AND f.post_date >= DATEADD('day', -{days_back}, CURRENT_DATE())
    """
    if category:
        sql += " AND o.research_domain = %s"
        cur.execute(sql, (category,))
    else:
        cur.execute(sql)

    cols = [c[0].lower() for c in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    rows.sort(key=lambda r: (r["close_date"] or date.max,
                             -(r["post_date"] or date.min).toordinal()))
    return rows


# ============================================================
# 2. LLM writes ONLY grounded one-line summaries
# ============================================================
SUMMARY_PROMPT = """You are writing one-sentence plain-English summaries of U.S. \
federal grant opportunities. For each opportunity below, write ONE short, clear \
sentence describing what it funds. Use ONLY the information given -- do not invent \
anything. Return a JSON object mapping each id to its one-sentence summary, e.g.
{{"abc123": "Funds research on ...", "def456": "Supports development of ..."}}

Opportunities:
{data}

Return ONLY the JSON object, nothing else:"""


def generate_blurbs(client, rows):
    data = "\n".join(
        f'- id: {r["opportunity_id"]}\n'
        f'  title: {r["opportunity_title"]}\n'
        f'  summary: {(r.get("summary_description") or "")[:400]}'
        for r in rows
    )
    resp = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": SUMMARY_PROMPT.format(data=data)}],
        temperature=0.3,
        max_tokens=2000,
    )
    raw = resp.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1].replace("json", "", 1).strip()
    try:
        blurbs = json.loads(raw)
    except Exception:
        blurbs = {}
    return blurbs, resp.usage.prompt_tokens, resp.usage.completion_tokens


# ============================================================
# 3. Python renders the styled HTML (design lives here, not the LLM)
# ============================================================
HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
  body {{
    margin: 0; padding: 0; background: #f4f5f7;
    font-family: -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    color: #1a1a2e; line-height: 1.5;
  }}
  .wrap {{ max-width: 680px; margin: 0 auto; padding: 24px 16px; }}
  .header {{
    background: linear-gradient(135deg, #1e3a5f 0%, #2d6a9f 100%);
    color: #fff; border-radius: 12px; padding: 28px; margin-bottom: 20px;
  }}
  .header h1 {{ margin: 0 0 6px; font-size: 22px; font-weight: 700; }}
  .header p {{ margin: 0; opacity: .85; font-size: 14px; }}
  .domain {{
    font-size: 13px; font-weight: 700; text-transform: uppercase;
    letter-spacing: .5px; color: #2d6a9f; margin: 26px 0 12px;
    padding-bottom: 6px; border-bottom: 2px solid #e3e8ef;
  }}
  .card {{
    background: #fff; border: 1px solid #e3e8ef; border-radius: 10px;
    padding: 16px 18px; margin-bottom: 12px;
  }}
  .card a.title {{
    color: #1e3a5f; font-weight: 600; font-size: 16px;
    text-decoration: none; display: inline-block; margin-bottom: 6px;
  }}
  .card a.title:hover {{ text-decoration: underline; }}
  .card .summary {{ font-size: 14px; color: #3a3a4a; margin: 4px 0 10px; }}
  .meta {{ display: flex; flex-wrap: wrap; gap: 8px; font-size: 12px; }}
  .tag {{
    background: #eef3f8; color: #2d6a9f; border-radius: 20px;
    padding: 3px 10px; font-weight: 500;
  }}
  .tag.deadline {{ background: #fdecec; color: #c0392b; }}
  .tag.award {{ background: #eafaf1; color: #1e8449; }}
  .footer {{
    text-align: center; color: #9aa0aa; font-size: 12px;
    margin-top: 28px; padding-top: 16px; border-top: 1px solid #e3e8ef;
  }}
</style>
</head>
<body>
  <div class="wrap">
    <div class="header">
      <h1>Federal Grant Opportunities Digest</h1>
      <p>{count} opportunities &middot; {today}</p>
    </div>
    {body}
    <div class="footer">
      Generated automatically from the grants data pipeline.<br>
      Data source: simpler.grants.gov
    </div>
  </div>
</body>
</html>"""


def _fmt_award(v):
    return f"${int(v):,}" if v else None


def render_digest_html(rows, blurbs):
    groups = {}
    for r in rows:
        groups.setdefault(r.get("research_domain") or "Uncategorized", []).append(r)

    body_parts = []
    for domain, items in groups.items():
        body_parts.append(f'<div class="domain">{domain}</div>')
        for r in items:
            title = r["opportunity_title"]
            url = r.get("opportunity_url") or "#"
            agency = r.get("agency_name") or "N/A"
            close = r.get("close_date")
            award = _fmt_award(r.get("award_ceiling"))
            blurb = blurbs.get(r["opportunity_id"], "")

            tags = [f'<span class="tag">{agency}</span>']
            if close:
                tags.append(f'<span class="tag deadline">Closes {close}</span>')
            if award:
                tags.append(f'<span class="tag award">Up to {award}</span>')

            body_parts.append(
                f'<div class="card">'
                f'<a class="title" href="{url}">{title}</a>'
                f'<div class="summary">{blurb}</div>'
                f'<div class="meta">{"".join(tags)}</div>'
                f'</div>'
            )

    return HTML_TEMPLATE.format(
        count=len(rows), today=date.today().strftime("%B %d, %Y"),
        body="".join(body_parts),
    )


# ============================================================
# 4. Optional email
# ============================================================
def send_email(html, subject):
    host = os.environ.get("SMTP_HOST")
    if not host:
        print("[no SMTP_HOST set - skipping email; use --dry-run to preview]", file=sys.stderr)
        return
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = os.environ["SMTP_FROM"]
    msg["To"] = os.environ["SMTP_TO"]
    msg.attach(MIMEText(html, "html"))
    with smtplib.SMTP(host, int(os.environ.get("SMTP_PORT", 587))) as server:
        server.starttls()
        server.login(os.environ["SMTP_USER"], os.environ["SMTP_PASSWORD"])
        server.send_message(msg)
    print(f"Digest emailed to {os.environ['SMTP_TO']}", file=sys.stderr)


# ============================================================
# main
# ============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days-back", type=int, default=7, help="posted within N days")
    ap.add_argument("--category", default=None, help="filter to one research_domain")
    ap.add_argument("--dry-run", action="store_true", help="print HTML, don't email")
    args = ap.parse_args()

    if not os.environ.get("OPENAI_API_KEY"):
        sys.exit("ERROR: set OPENAI_API_KEY")
    client = OpenAI()

    conn = get_conn()
    cur = conn.cursor()
    rows = fetch_opportunities(cur, args.days_back, args.category)
    cur.close(); conn.close()

    print(f"Selected {len(rows)} opportunities.", file=sys.stderr)
    if not rows:
        print("Nothing to send.", file=sys.stderr)
        return

    blurbs, tin, tout = generate_blurbs(client, rows)
    html = render_digest_html(rows, blurbs)

    subject = f"Grant Opportunities Digest - {date.today().isoformat()} ({len(rows)} new)"

    if args.dry_run:
        print(html)   # goes to stdout so you can pipe to a file
    else:
        send_email(html, subject)


if __name__ == "__main__":
    main()
    
    
    
    
def generate_digest(days_back=1, category=None):
    if not os.environ.get("OPENAI_API_KEY"):
        raise ValueError("OPENAI_API_KEY is not set")

    client = OpenAI()

    conn = get_conn()
    cur = conn.cursor()

    try:
        rows = fetch_opportunities(
            cur,
            days_back=days_back,
            category=category,
        )
    finally:
        cur.close()
        conn.close()

    print(f"Selected {len(rows)} opportunities.")

    if not rows:
        print("Nothing to send.")
        return None

    blurbs, tin, tout = generate_blurbs(client, rows)

    html = render_digest_html(rows, blurbs)

    subject = (
        f"Grant Opportunities Digest - "
        f"{date.today().isoformat()} ({len(rows)} new)"
    )

    # Save HTML file
    output_dir = "/opt/airflow/digests"
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(
        output_dir,
        f"digest_{date.today().isoformat()}.html",
    )

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"HTML digest saved to: {output_file}")

    return {
        "html": html,
        "subject": subject,
        "count": len(rows),
        "input_tokens": tin,
        "output_tokens": tout,
        "file_path": output_file,
    }