"""
Grant opportunity digest generator.

Queries the Gold layer for relevant opportunities (deterministic SQL), then uses
an LLM to write a readable digest grouped by research domain. The LLM only does
LANGUAGE (summarize/format the rows it is given) -- it never selects what is
relevant (SQL does that) and never invents grants (everything is grounded in the
query results). Output is an HTML digest, optionally emailed.

SETUP:
    pip install openai snowflake-connector-python python-dotenv
    # .env: OPENAI_API_KEY + Snowflake creds (reuse existing)

RUN:
    python send_digest.py                 # last 7 days posted OR closing in 30 days
    python send_digest.py --days-back 3   # opportunities posted in last 3 days
    python send_digest.py --closing 14    # + those closing within 14 days
    python send_digest.py --category STEM # only one research domain
    python send_digest.py --dry-run       # print HTML, don't email
"""

import os
import sys
import argparse
from datetime import date

from dotenv import load_dotenv
from openai import OpenAI
import snowflake.connector

load_dotenv()

MODEL = "gpt-4o-mini"
PRICE_IN = 0.15 / 1_000_000
PRICE_OUT = 0.60 / 1_000_000


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


def fetch_opportunities(cur, days_back: int, closing_days: int, category: str):
    """Select opportunities POSTED within the last `days_back` days (newly added)."""
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
          AND o.dbt_valid_to = '9999-12-31'                      -- current snapshot version only
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


def build_grounded_text(rows: list) -> str:
    """Turn the SQL rows into a compact, grounded text block for the LLM.

    We hand the LLM ONLY these facts. It must summarize from this, nothing else.
    """
    lines = []
    for r in rows:
        ceiling = f"${int(r['award_ceiling']):,}" if r.get("award_ceiling") else "not specified"
        summary = (r.get("summary_description") or "")[:400]
        lines.append(
            f"- id: {r['opportunity_id']}\n"
            f"  title: {r['opportunity_title']}\n"
            f"  agency: {r.get('agency_name') or 'N/A'}\n"
            f"  domain: {r.get('research_domain') or 'Uncategorized'}\n"
            f"  posted: {r.get('post_date')}\n"
            f"  closes: {r.get('close_date') or 'N/A'}\n"
            f"  max_award: {ceiling}\n"
            f"  url: {r.get('opportunity_url') or 'N/A'}\n"
            f"  summary: {summary}\n"
        )
    return "\n".join(lines)


DIGEST_PROMPT = """You are writing a weekly digest of U.S. federal grant \
opportunities for researchers. Below is a list of opportunities with their real \
details. Write a clean, scannable HTML digest.

STRICT RULES:
- Use ONLY the opportunities and facts provided below. Do NOT invent any grant, \
number, date, agency, or URL. If a field says N/A, omit it gracefully.
- Group the opportunities by their "domain" (research_domain) with a heading for each.
- For each opportunity: show the title as a link to its url, then a one-line plain \
summary, the agency, the close date, and the max award if specified.
- Keep each summary to ONE short sentence in plain English.
- At the top, write a one-sentence intro noting how many opportunities and the date.
- Output valid HTML only (use <h2>, <h3>, <ul>, <li>, <a>). No markdown.

Today's date: {today}
Number of opportunities: {count}

OPPORTUNITIES:
{data}

Write the HTML digest now:"""


def generate_digest(client, rows: list):
    data = build_grounded_text(rows)
    prompt = DIGEST_PROMPT.format(today=date.today().isoformat(), count=len(rows), data=data)
    resp = client.chat.completions.create(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3,
        max_tokens=2000,
    )
    html = resp.choices[0].message.content.strip()
    if html.startswith("```"):
        html = html.split("```")[1].replace("html", "", 1).strip()
    return html, resp.usage.prompt_tokens, resp.usage.completion_tokens


def send_email(html: str, subject: str):
    """Send the digest via SMTP. Configure via env vars; skipped if not set."""
    host = os.environ.get("SMTP_HOST")
    if not host:
        print("[no SMTP_HOST set — skipping email; use --dry-run to preview]")
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
    print(f"✅ Digest emailed to {os.environ['SMTP_TO']}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days-back", type=int, default=7, help="posted within N days")
    ap.add_argument("--closing", type=int, default=30, help="closing within N days")
    ap.add_argument("--category", default=None, help="filter to one research_domain")
    ap.add_argument("--dry-run", action="store_true", help="print HTML, don't email")
    args = ap.parse_args()

    if not os.environ.get("OPENAI_API_KEY"):
        sys.exit("ERROR: set OPENAI_API_KEY")
    client = OpenAI()

    conn = get_conn()
    cur = conn.cursor()
    rows = fetch_opportunities(cur, args.days_back, args.closing, args.category)
    cur.close(); conn.close()

    print(f"Selected {len(rows)} opportunities.")
    if not rows:
        print("Nothing to send.")
        return

    html, tin, tout = generate_digest(client, rows)
    cost = tin * PRICE_IN + tout * PRICE_OUT
    print(f"Digest generated. Tokens in={tin} out={tout} (~${cost:.4f})")

    subject = f"Grant Opportunities Digest — {date.today().isoformat()} ({len(rows)} new/closing)"

    if args.dry_run:
        print("\n----- DIGEST HTML (dry run) -----\n")
        print(html)
    else:
        send_email(html, subject)


if __name__ == "__main__":
    main()