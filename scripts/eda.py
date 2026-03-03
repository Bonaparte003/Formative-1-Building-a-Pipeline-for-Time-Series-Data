# Task 1.B: EDA and 5 analytical questions (2+ with lag and moving average).
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = PROJECT_ROOT / "data" / "processed" / "lca_unified.csv"
FIG_DIR = PROJECT_ROOT / "outputs" / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)


def load_agg():
    df = pd.read_csv(DATA_PATH)
    df = df[df["wage"].notna() & (df["wage"] > 0)].copy()
    df_y = df[df["unit_of_pay"] == "Year"].copy()
    df_y["wage"] = pd.to_numeric(df_y["wage"], errors="coerce")
    df_y = df_y[df_y["wage"] > 0]
    return df, df_y


def period_order(df):
    return df.sort_values(["fiscal_year", "quarter"])


def q1_trend(df_y):
    p = period_order(df_y)
    agg = p.groupby(["fiscal_year", "quarter"], as_index=False).agg(
        median_wage=("wage", "median"), mean_wage=("wage", "mean"), count=("wage", "count")
    )
    agg["period"] = agg["fiscal_year"].astype(str) + "-" + agg["quarter"]
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(agg["period"], agg["median_wage"], marker="o", label="Median wage")
    ax.plot(agg["period"], agg["mean_wage"], marker="s", alpha=0.7, label="Mean wage")
    step = max(1, len(agg) // 15)
    ax.set_xticks(range(0, len(agg), step))
    ax.set_xticklabels(agg["period"].iloc[::step], rotation=45)
    ax.legend()
    ax.set_title("Q1: Wage trend over fiscal quarters")
    ax.set_ylabel("Wage (USD)")
    fig.tight_layout()
    fig.savefig(FIG_DIR / "q1_trend.png", dpi=100)
    plt.close()
    return agg


def q2_correlation(df_y):
    top_visa = df_y["visa_class"].value_counts().head(5).index.tolist()
    sub = df_y[df_y["visa_class"].isin(top_visa)]
    p = period_order(sub)
    agg = p.groupby(["fiscal_year", "quarter", "visa_class"], as_index=False).agg(
        median_wage=("wage", "median"), count=("wage", "count")
    )
    agg["period"] = agg["fiscal_year"].astype(str) + "-" + agg["quarter"]
    periods = agg["period"].unique()
    fig, ax = plt.subplots(figsize=(12, 4))
    for v in top_visa:
        d = agg[agg["visa_class"] == v]
        ax.plot(d["period"], d["median_wage"], marker="o", label=v, alpha=0.8)
    step = max(1, len(periods) // 12)
    ax.set_xticks(range(0, len(periods), step))
    ax.set_xticklabels(periods[::step], rotation=45)
    ax.legend()
    ax.set_title("Q2: Median wage by visa class over time")
    ax.set_ylabel("Median wage (USD)")
    fig.tight_layout()
    fig.savefig(FIG_DIR / "q2_correlation_visa.png", dpi=100)
    plt.close()
    return agg


def q3_lagged(agg_quarter):
    d = agg_quarter.sort_values(["fiscal_year", "quarter"]).reset_index(drop=True)
    for lag in [1, 2, 4]:
        d[f"median_wage_lag{lag}"] = d["median_wage"].shift(lag)
    d = d.dropna()
    fig, axes = plt.subplots(1, 3, figsize=(14, 4))
    for i, lag in enumerate([1, 2, 4]):
        col = f"median_wage_lag{lag}"
        axes[i].scatter(d[col], d["median_wage"], alpha=0.5)
        axes[i].set_xlabel(f"Median wage (lag {lag} quarter)")
        axes[i].set_ylabel("Median wage (current)")
        axes[i].set_title(f"Q3: Lag-{lag} effect")
    fig.suptitle("Lagged wage: current quarter vs previous quarters")
    fig.tight_layout()
    fig.savefig(FIG_DIR / "q3_lagged.png", dpi=100)
    plt.close()
    return d


def q4_moving_average(agg_quarter):
    d = agg_quarter.sort_values(["fiscal_year", "quarter"]).reset_index(drop=True)
    d["count_ma4"] = d["count"].rolling(4, min_periods=1).mean()
    d["median_wage_next"] = d["median_wage"].shift(-1)
    d = d.dropna(subset=["median_wage_next"])
    fig, ax1 = plt.subplots(figsize=(12, 4))
    ax1.plot(d["period"], d["count_ma4"], color="tab:blue", label="Cert count (4-Q MA)")
    ax1.set_ylabel("Count (4-quarter MA)")
    ax2 = ax1.twinx()
    ax2.plot(d["period"], d["median_wage_next"], color="tab:orange", alpha=0.8, label="Next Q median wage")
    ax2.set_ylabel("Next quarter median wage (USD)")
    ax1.legend(loc="upper left")
    ax2.legend(loc="upper right")
    ax1.set_title("Q4: 4-quarter MA of certification count vs next quarter wage")
    step = max(1, len(d) // 12)
    ax1.set_xticks(range(0, len(d), step))
    ax1.set_xticklabels(d["period"].iloc[::step], rotation=45)
    fig.tight_layout()
    fig.savefig(FIG_DIR / "q4_moving_average.png", dpi=100)
    plt.close()
    return d


def q5_seasonality(df_y):
    p = period_order(df_y)
    agg = p.groupby(["fiscal_year", "quarter"], as_index=False).agg(
        median_wage=("wage", "median"), count=("wage", "count")
    )
    by_q = agg.groupby("quarter").agg(median_wage=("median_wage", "median"), count=("count", "sum"))
    for q in ["Q1", "Q2", "Q3", "Q4"]:
        if q not in by_q.index:
            by_q.loc[q] = [np.nan, 0]
    by_q = by_q.reindex(["Q1", "Q2", "Q3", "Q4"])
    fig, ax = plt.subplots(figsize=(6, 4))
    by_q["median_wage"].plot(kind="bar", ax=ax)
    ax.set_xticklabels(by_q.index, rotation=0)
    ax.set_ylabel("Median wage (USD)")
    ax.set_title("Q5: Seasonality - median wage by quarter of year")
    fig.tight_layout()
    fig.savefig(FIG_DIR / "q5_seasonality.png", dpi=100)
    plt.close()
    return by_q


def main():
    df, df_y = load_agg()
    print("Time range:", df_y["fiscal_year"].min(), "-", df_y["fiscal_year"].max())
    print("Missing wage (dropped for wage analysis):", df["wage"].isna().sum())
    agg_quarter = df_y.groupby(["fiscal_year", "quarter"], as_index=False).agg(
        median_wage=("wage", "median"), mean_wage=("wage", "mean"), count=("wage", "count")
    )
    agg_quarter["period"] = agg_quarter["fiscal_year"].astype(str) + "-" + agg_quarter["quarter"]
    q1_trend(df_y)
    q2_correlation(df_y)
    q3_lagged(agg_quarter)
    q4_moving_average(agg_quarter)
    q5_seasonality(df_y)
    print("Figures saved to", FIG_DIR)


if __name__ == "__main__":
    main()
