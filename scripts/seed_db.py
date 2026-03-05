# Seed SQL DB from processed CSV so API and prediction script have data.
# Samples rows from every (fiscal_year, quarter) pair so the DB has full
# time-series coverage across FY 2017-2022.
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from api.database_sql import init_db, SessionLocal, Period, Employer, Case

ROWS_PER_PERIOD = 500   # ~500 cases per quarter → ~7500 total rows

def quarter_to_dates(fiscal_year, quarter):
    y = int(fiscal_year)
    q = str(quarter).upper()
    if q == "Q1": return f"{y-1}-10-01", f"{y-1}-12-31"
    if q == "Q2": return f"{y}-01-01", f"{y}-03-31"
    if q == "Q3": return f"{y}-04-01", f"{y}-06-30"
    if q == "Q4": return f"{y}-07-01", f"{y}-09-30"
    return f"{y}-01-01", f"{y}-03-31"


def main():
    import pandas as pd
    import sqlite3

    data_path = PROJECT_ROOT / "data" / "processed" / "lca_unified.csv"
    if not data_path.exists():
        print("Run preprocess.py first.")
        return

    # ── Drop and recreate tables so we start clean ─────────────────────────
    db_path = PROJECT_ROOT / "data" / "lca.db"
    if db_path.exists():
        db_path.unlink()
        print("Dropped old lca.db")

    init_db()
    db = SessionLocal()

    # ── Load and sample across all periods ──────────────────────────────────
    print("Reading CSV (this may take a moment)…")
    df = pd.read_csv(
        data_path,
        low_memory=False,
        usecols=["fiscal_year","quarter","wage","employer_name",
                 "employer_location","employer_country","soc_title",
                 "visa_class","job_title","full_time_position",
                 "worksite","unit_of_pay","case_status"],
    )
    df = df[df["wage"].notna() & (df["wage"] > 0)].copy()

    # Sample up to ROWS_PER_PERIOD rows from each (fiscal_year, quarter)
    # Use concat approach to avoid pandas 2.x groupby.apply column-drop issue
    chunks = []
    for (fy, q), grp in df.groupby(["fiscal_year", "quarter"]):
        chunks.append(grp.sample(min(len(grp), ROWS_PER_PERIOD), random_state=42))
    sampled = pd.concat(chunks, ignore_index=True)
    n_periods = sampled[["fiscal_year", "quarter"]].drop_duplicates().shape[0]
    print(f"Sampled {len(sampled)} rows across {n_periods} periods")

    period_keys  = {}
    employer_keys = {}

    for _, row in sampled.iterrows():
        fy, q = row["fiscal_year"], row["quarter"]
        key_p = (fy, q)
        if key_p not in period_keys:
            start, end = quarter_to_dates(fy, q)
            p = Period(
                fiscal_year=int(fy),
                quarter=str(q),
                start_date=start,
                end_date=end,
            )
            db.add(p)
            db.flush()
            period_keys[key_p] = p.period_id

        emp_key = (
            str(row.get("employer_name", "") or "")[:255],
            str(row.get("employer_location", "") or "")[:255],
        )
        if emp_key not in employer_keys:
            e = Employer(
                employer_name=emp_key[0],
                employer_location=emp_key[1],
                employer_country=str(row.get("employer_country", "") or "")[:100],
            )
            db.add(e)
            db.flush()
            employer_keys[emp_key] = e.employer_id

        c = Case(
            period_id=period_keys[key_p],
            employer_id=employer_keys[emp_key],
            soc_title=str(row.get("soc_title", "") or "")[:255],
            visa_class=str(row.get("visa_class", "") or "")[:50],
            job_title=str(row.get("job_title", "") or "")[:255],
            full_time_position=str(row.get("full_time_position", "Y") or "Y")[:1],
            worksite=str(row.get("worksite", "") or "")[:255],
            wage=float(row["wage"]),
            unit_of_pay=str(row.get("unit_of_pay", "Year") or "Year")[:10],
            case_status=str(row.get("case_status", "") or "")[:50],
        )
        db.add(c)

    db.commit()
    print(f"Done. Seeded {len(sampled)} cases, "
          f"{len(period_keys)} periods, "
          f"{len(employer_keys)} employers.")
    db.close()


if __name__ == "__main__":
    main()
