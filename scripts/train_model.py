# Task 1.C: Train a model for wage prediction with hyperparameter tuning.
# Produces experiment table (>=2 experiments) and saves best model.
import pandas as pd
import numpy as np
import json
from pathlib import Path
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import joblib

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = PROJECT_ROOT / "data" / "processed" / "lca_unified.csv"
MODEL_DIR = PROJECT_ROOT / "models"
EXP_DIR = PROJECT_ROOT / "outputs" / "experiments"
MODEL_DIR.mkdir(parents=True, exist_ok=True)
EXP_DIR.mkdir(parents=True, exist_ok=True)


def load_and_prepare():
    df = pd.read_csv(DATA_PATH)
    df = df[df["wage"].notna() & (df["wage"] > 0) & (df["unit_of_pay"] == "Year")].copy()
    df["period_ord"] = (
        df["fiscal_year"].astype(str) + "-" + df["quarter"]
    )
    # Lagged and moving average features (time-series)
    agg = df.groupby("period_ord", as_index=False).agg(
        median_wage=("wage", "median"),
        count=("wage", "count"),
    )
    for lag in [1, 2]:
        agg[f"median_wage_lag{lag}"] = agg["median_wage"].shift(lag)
    agg["median_wage_ma4"] = agg["median_wage"].rolling(4, min_periods=1).mean()
    agg = agg.dropna()
    # Merge back to get features per row (we predict wage at row level with period features)
    df = df.merge(
        agg[["period_ord", "median_wage_lag1", "median_wage_lag2", "median_wage_ma4"]],
        on="period_ord",
        how="left",
    )
    df["fiscal_year"] = df["fiscal_year"].astype(float)
    for col in ["visa_class", "case_status"]:
        le = LabelEncoder()
        df[col + "_enc"] = le.fit_transform(df[col].astype(str))
    return df, agg


def run_experiment(name, model, X_train, X_test, y_train, y_test):
    model.fit(X_train, y_train)
    pred = model.predict(X_test)
    return {
        "experiment": name,
        "rmse": np.sqrt(mean_squared_error(y_test, pred)),
        "mae": mean_absolute_error(y_test, pred),
        "r2": r2_score(y_test, pred),
    }


def main():
    df, _ = load_and_prepare()
    feature_cols = [
        "fiscal_year",
        "median_wage_lag1",
        "median_wage_lag2",
        "median_wage_ma4",
        "visa_class_enc",
        "case_status_enc",
    ]
    X = df[feature_cols].fillna(0)
    y = df["wage"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_test_s = scaler.transform(X_test)

    experiments = []

    # Experiment 1: Ridge regression
    exp1 = run_experiment(
        "Ridge (alpha=1.0)",
        Ridge(alpha=1.0),
        X_train_s, X_test_s, y_train, y_test,
    )
    experiments.append(exp1)

    # Experiment 2: Ridge with different alpha
    exp2 = run_experiment(
        "Ridge (alpha=10.0)",
        Ridge(alpha=10.0),
        X_train_s, X_test_s, y_train, y_test,
    )
    experiments.append(exp2)

    # Experiment 3: Random Forest
    exp3 = run_experiment(
        "RandomForest (n_estimators=100)",
        RandomForestRegressor(n_estimators=100, max_depth=12, random_state=42),
        X_train, X_test, y_train, y_test,
    )
    experiments.append(exp3)

    # Experiment 4: Random Forest tuned
    exp4 = run_experiment(
        "RandomForest (n_estimators=200, max_depth=15)",
        RandomForestRegressor(n_estimators=200, max_depth=15, random_state=42),
        X_train, X_test, y_train, y_test,
    )
    experiments.append(exp4)

    exp_df = pd.DataFrame(experiments)
    exp_df.to_csv(EXP_DIR / "experiment_table.csv", index=False)
    print(exp_df.to_string())

    best_idx = exp_df["rmse"].idxmin()
    best_name = exp_df.loc[best_idx, "experiment"]
    if "Ridge" in best_name:
        best_model = Ridge(alpha=10.0)
        best_model.fit(X_train_s, y_train)
        joblib.dump(best_model, MODEL_DIR / "model.joblib")
        joblib.dump(scaler, MODEL_DIR / "scaler.joblib")
        joblib.dump(
            {"feature_cols": feature_cols, "use_scale": True},
            MODEL_DIR / "config.joblib",
        )
    else:
        best_model = RandomForestRegressor(n_estimators=200, max_depth=15, random_state=42)
        best_model.fit(X_train, y_train)
        joblib.dump(best_model, MODEL_DIR / "model.joblib")
        joblib.dump(scaler, MODEL_DIR / "scaler.joblib")
        joblib.dump(
            {"feature_cols": feature_cols, "use_scale": False},
            MODEL_DIR / "config.joblib",
        )

    with open(MODEL_DIR / "config.json", "w") as f:
        json.dump(
            {
                "feature_cols": feature_cols,
                "experiment_table": exp_df.to_dict(orient="records"),
                "best_experiment": best_name,
            },
            f,
            indent=2,
        )
    print(f"Best model: {best_name} saved to {MODEL_DIR}")


if __name__ == "__main__":
    main()
