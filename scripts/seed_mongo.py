"""
seed_mongo.py
─────────────
Reads cases from the SQL database via the REST API (POST /mongo/cases)
and populates the MongoDB collection.  Then runs the three required
queries and prints results so they can be included in the report.

Usage:
    python scripts/seed_mongo.py [--api http://localhost:8000]
"""
import sys
import json
import argparse
import sqlite3
from pathlib import Path
from datetime import datetime

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = PROJECT_ROOT / "data" / "lca.db"


def fetch_cases_from_sql(per_period: int = 50):
    """Read a balanced sample across all periods from SQLite."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Get all distinct period_ids
    cur.execute("SELECT period_id FROM periods ORDER BY fiscal_year, quarter")
    period_ids = [r[0] for r in cur.fetchall()]

    rows = []
    for pid in period_ids:
        cur.execute("""
            SELECT
                c.case_id,
                p.fiscal_year, p.quarter, p.start_date, p.end_date,
                e.employer_name, e.employer_location, e.employer_country,
                c.soc_title, c.visa_class, c.job_title,
                c.full_time_position, c.worksite,
                c.wage, c.unit_of_pay, c.case_status, c.created_at
            FROM cases c
            JOIN periods  p ON c.period_id  = p.period_id
            JOIN employers e ON c.employer_id = e.employer_id
            WHERE c.period_id = ?
            LIMIT ?
        """, (pid, per_period))
        rows.extend([dict(r) for r in cur.fetchall()])

    conn.close()
    return rows


def row_to_mongo_doc(row: dict) -> dict:
    return {
        "case_id":   row["case_id"],
        "period": {
            "fiscal_year": row["fiscal_year"],
            "quarter":     row["quarter"],
            "start_date":  row["start_date"],
            "end_date":    row["end_date"],
        },
        "employer": {
            "name":     row["employer_name"],
            "location": row["employer_location"],
            "country":  row["employer_country"],
        },
        "soc_title":         row["soc_title"],
        "visa_class":        row["visa_class"],
        "job_title":         row["job_title"],
        "full_time_position":row["full_time_position"],
        "worksite":          row["worksite"],
        "wage":              row["wage"],
        "unit_of_pay":       row["unit_of_pay"],
        "case_status":       row["case_status"],
        "created_at":        row["created_at"],
    }


def seed(api_base: str, per_period: int = 50):
    print(f"Fetching up to {per_period} cases per period from SQL …")
    rows = fetch_cases_from_sql(per_period)
    print(f"  Got {len(rows)} rows. Posting to {api_base}/mongo/cases …")

    ok = skip = err = 0
    for row in rows:
        doc = row_to_mongo_doc(row)
        try:
            r = requests.post(f"{api_base}/mongo/cases", json=doc, timeout=10)
            if r.status_code in (200, 201):
                ok += 1
            elif r.status_code == 409:
                skip += 1
            else:
                err += 1
        except Exception as e:
            err += 1
            print(f"  Error on case {row['case_id']}: {e}")
    print(f"  Inserted {ok}, skipped {skip}, errors {err}.\n")


def run_queries(api_base: str):
    print("=" * 60)
    print("MongoDB Query Results")
    print("=" * 60)

    # Query 1 – Latest record
    print("\n[Query 1] Latest record")
    r = requests.get(f"{api_base}/mongo/cases/ts/latest", timeout=10)
    doc = r.json()
    print(json.dumps(doc, indent=2))

    # Query 2 – Date range (FY 2021 Q1–Q4)
    print("\n[Query 2] Records in date range 2020-10-01 → 2021-09-30")
    r = requests.get(
        f"{api_base}/mongo/cases/ts/date_range",
        params={"start_date": "2020-10-01", "end_date": "2021-09-30"},
        timeout=10,
    )
    docs = r.json()
    print(f"  Returned {len(docs)} records. First two:")
    for d in docs[:2]:
        print(json.dumps(d, indent=2))

    # Query 3 – Get by case_id (use the latest case_id from Query 1)
    cid = doc.get("case_id", 1)
    print(f"\n[Query 3] Get by case_id={cid}")
    r = requests.get(f"{api_base}/mongo/cases/{cid}", timeout=10)
    print(json.dumps(r.json(), indent=2))

    print("\n" + "=" * 60)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api", default="http://localhost:8000",
                        help="Base URL of the running FastAPI server")
    parser.add_argument("--per-period", type=int, default=50,
                        help="Cases to seed per time period (default 50 × 15 periods = 750)")
    args = parser.parse_args()

    try:
        requests.get(args.api, timeout=5)
    except Exception:
        print(f"ERROR: API not reachable at {args.api}. Make sure it is running.")
        sys.exit(1)

    seed(args.api, args.per_period)
    run_queries(args.api)


if __name__ == "__main__":
    main()
