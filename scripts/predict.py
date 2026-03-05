# Task 4: Fetch from API, preprocess, load model, predict.
import os
import sys
import json
import requests
import pandas as pd
import numpy as np
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MODEL_DIR = PROJECT_ROOT / "models"
API_BASE = os.getenv("API_BASE", "http://localhost:8000")


def load_model_and_config():
    import joblib
    model = joblib.load(MODEL_DIR / "model.joblib")
    scaler = joblib.load(MODEL_DIR / "scaler.joblib")
    config = joblib.load(MODEL_DIR / "config.joblib")
    return model, scaler, config


def fetch_latest_from_api():
    r = requests.get(f"{API_BASE}/sql/cases/ts/latest", timeout=5)
    r.raise_for_status()
    return r.json()


def fetch_date_range_from_api(start_date, end_date):
    r = requests.get(
        f"{API_BASE}/sql/cases/ts/date_range",
        params={"start_date": start_date, "end_date": end_date, "limit": 100},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()


def preprocess_for_prediction(record_or_list, config):
    """Same pipeline as Task 1: build feature vector from record(s)."""
    feature_cols = config["feature_cols"]
    use_scale = config.get("use_scale", True)
    if isinstance(record_or_list, dict):
        records = [record_or_list]
    else:
        records = record_or_list
    # Build minimal row: we need fiscal_year, lags, ma4, visa_class_enc, case_status_enc.
    # From API we get period_id, wage, case_status; we may not have lags. Use 0 for missing.
    rows = []
    for rec in records:
        wage = rec.get("wage")
        if wage is None:
            wage = 0
        # Default feature values when we only have one record from API
        row = {
            "fiscal_year": 2022,
            "median_wage_lag1": wage,
            "median_wage_lag2": wage,
            "median_wage_ma4": wage,
            "visa_class_enc": 0,
            "case_status_enc": 1 if (rec.get("case_status") or "").lower() == "certified" else 0,
        }
        rows.append(row)
    X = pd.DataFrame(rows)[feature_cols].fillna(0)
    return X, use_scale


def main():
    if not (MODEL_DIR / "model.joblib").exists():
        print("Run train_model.py first to create model.joblib", file=sys.stderr)
        sys.exit(1)
    model, scaler, config = load_model_and_config()
    # Fetch latest record from API
    try:
        latest = fetch_latest_from_api()
    except Exception as e:
        print("API not available, using dummy record:", e)
        latest = {"case_id": 0, "wage": 90000, "case_status": "Certified", "period_id": 1}
    X, use_scale = preprocess_for_prediction(latest, config)
    if use_scale:
        X = scaler.transform(X)
    pred = model.predict(X)
    result = {"case_id": latest.get("case_id"), "input_wage": latest.get("wage"), "predicted_wage": float(pred[0])}
    print(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    main()
