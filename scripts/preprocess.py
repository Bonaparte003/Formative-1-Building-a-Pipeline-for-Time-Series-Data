# Task 1: Preprocessing - unify LCA CSVs, add fiscal_year/quarter.
import re
import pandas as pd
import numpy as np
from pathlib import Path

# Setting the project's directories
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_DIR = PROJECT_ROOT / "archive"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
WAGE_COLUMN_ALIASES = ["Prevailing_Wage", "Wage_Rate_Of_Pay"]


# Function to load and unify csv
def load_and_unify_csv(path, fiscal_year):
    df = pd.read_csv(path, low_memory=False)
    wage_col = next((c for c in WAGE_COLUMN_ALIASES if c in df.columns), None)
    df["wage"] = pd.to_numeric(df[wage_col], errors="coerce") if wage_col else np.nan
    if "Quarter" in df.columns:
        df["quarter"] = df["Quarter"].astype(str).str.upper().str.strip().replace({"NAN": "Q1", "NONE": "Q1"})
    else:
        df["quarter"] = "Q1"
    df["fiscal_year"] = fiscal_year
    return df


def standardize_columns(df):
    column_map = {"Employer_Name": "employer_name", "SOC_Title": "soc_title", "Job_Title": "job_title",
                  "Full_Time_Position": "full_time_position", "Worksite": "worksite", "Unit_Of_Pay": "unit_of_pay",
                  "Employer_Location": "employer_location", "Employer_Country": "employer_country",
                  "Case_Status": "case_status", "Visa_Class": "visa_class"}
    out = df[["fiscal_year", "quarter", "wage"]].copy()
    for old, new in column_map.items():
        out[new] = df[old].astype(str).str.strip() if old in df.columns else ""
    return out


def add_period_dates(df):
    def quarter_to_dates(row):
        y, q = int(row["fiscal_year"]), str(row["quarter"]).upper()
        if q == "Q1": return f"{y-1}-10-01", f"{y-1}-12-31"
        if q == "Q2": return f"{y}-01-01", f"{y}-03-31"
        if q == "Q3": return f"{y}-04-01", f"{y}-06-30"
        if q == "Q4": return f"{y}-07-01", f"{y}-09-30"
        return f"{y}-01-01", f"{y}-03-31"
    df[["start_date", "end_date"]] = df.apply(lambda r: pd.Series(quarter_to_dates(r)), axis=1)
    df["period"] = df["fiscal_year"].astype(str) + "-" + df["quarter"]
    return df


def handle_missing_values(df):
    # flag rows where wage was missing before filling
    df["wage_missing"] = df["wage"].isna()

    # normalize text columns
    for col in ["employer_name", "soc_title", "case_status", "visa_class"]:
        if col in df.columns:
            df[col] = df[col].replace("", "Unknown").replace("nan", "Unknown")

    # fill missing wages with the mean
    mean_wage = df["wage"].mean(skipna=True)
    if pd.isna(mean_wage):
        mean_wage = 0.0
    df["wage"] = df["wage"].fillna(mean_wage)

    # indicate which rows were filled
    df["wage_filled_with_mean"] = df["wage_missing"]

    return df


def main():
    print("================== Starting Preprocessing ===================")
    dfs = []
    for f in sorted(ARCHIVE_DIR.glob("LCA_FY_*.csv")):
        m = re.search(r"LCA_FY_(\d{4})\.csv", f.name)
        if m:
            dfs.append(load_and_unify_csv(f, int(m.group(1))))
    raw = pd.concat(dfs, ignore_index=True)
    unified = standardize_columns(raw)
    unified = add_period_dates(unified)
    unified = handle_missing_values(unified)
    output_path = OUTPUT_DIR / "lca_unified.csv"
    unified.to_csv(output_path, index=False)
    print(f"Saved {len(unified)} rows to {output_path}")
    return unified


if __name__ == "__main__":
    main()
