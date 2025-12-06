from pathlib import Path

import pandas as pd
from sklearn.ensemble import IsolationForest


DATA_DIR = Path(__file__).resolve().parent.parent / "data"
TRANSFERS_CSV = DATA_DIR / "transfers.csv"
ANOMALIES_CSV = DATA_DIR / "anomalies.csv"


def load_data() -> pd.DataFrame:
    if not TRANSFERS_CSV.exists():
        raise FileNotFoundError(f"No transfers.csv found at {TRANSFERS_CSV}")

    df = pd.read_csv(TRANSFERS_CSV)

    # Basic sanity filters: drop weird/zero durations
    df = df[df["duration"] > 0].copy()
    df["throughput_bytes_per_sec"] = df["throughput_bytes_per_sec"].fillna(0)

    return df


def run_isolation_forest(df: pd.DataFrame) -> pd.DataFrame:
    # Features for anomaly detection
    feature_cols = ["bytes", "duration", "throughput_bytes_per_sec"]

    X = df[feature_cols].values

    # IsolationForest: unsupervised anomaly detection
    model = IsolationForest(
        n_estimators=100,
        contamination=0.05,  # ~5% of points considered anomalies
        random_state=42,
    )

    model.fit(X)

    # Predictions: -1 = anomaly, 1 = normal
    df["anomaly_label"] = model.predict(X)
    # Decision function: lower = more abnormal
    df["anomaly_score"] = model.decision_function(X)

    return df


def save_anomalies(df: pd.DataFrame) -> None:
    anomalies = df[df["anomaly_label"] == -1].copy()
    anomalies.to_csv(ANOMALIES_CSV, index=False)
    print(f"Saved {len(anomalies)} anomalies to {ANOMALIES_CSV}")


def main() -> None:
    print(f"Loading data from {TRANSFERS_CSV} ...")
    df = load_data()
    print(f"Loaded {len(df)} records")

    df_with_scores = run_isolation_forest(df)

    # Show top 10 most anomalous by score (ascending)
    top_anomalies = df_with_scores.sort_values("anomaly_score").head(10)
    print("\nTop 10 most anomalous transfers:")
    print(top_anomalies[[
        "timestamp_iso",
        "bytes",
        "duration",
        "throughput_bytes_per_sec",
        "anomaly_score",
    ]])

    save_anomalies(df_with_scores)


if __name__ == "__main__":
    main()