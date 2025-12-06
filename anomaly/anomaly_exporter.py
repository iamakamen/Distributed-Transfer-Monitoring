import time
from pathlib import Path

import pandas as pd
from prometheus_client import start_http_server, Gauge
from sklearn.ensemble import IsolationForest


DATA_DIR = Path(__file__).resolve().parent.parent / "data"
TRANSFERS_CSV = DATA_DIR / "transfers.csv"

# Prometheus metrics
ANOMALY_COUNT = Gauge(
    "dtms_anomaly_count",
    "Number of anomalous transfers detected in the dataset",
)

ANOMALY_RATIO = Gauge(
    "dtms_anomaly_ratio",
    "Fraction of transfers detected as anomalous",
)

ANOMALY_SCORE_MIN = Gauge(
    "dtms_anomaly_score_min",
    "Minimum (most anomalous) IsolationForest score in the dataset",
)


def load_data():
    if not TRANSFERS_CSV.exists():
        print(f"[ANOMALY_EXPORTER] No transfers.csv found at {TRANSFERS_CSV}")
        return None

    df = pd.read_csv(TRANSFERS_CSV)

    # Basic filtering
    df = df[df["duration"] > 0].copy()
    if df.empty:
        print("[ANOMALY_EXPORTER] No valid rows after filtering")
        return None

    df["throughput_bytes_per_sec"] = df["throughput_bytes_per_sec"].fillna(0)
    return df


def compute_anomalies(df):
    feature_cols = ["bytes", "duration", "throughput_bytes_per_sec"]
    X = df[feature_cols].values

    model = IsolationForest(
        n_estimators=100, contamination=0.05, random_state=42
    )
    model.fit(X)

    labels = model.predict(X)  # -1 anomaly, 1 normal
    scores = model.decision_function(X)

    df["anomaly_label"] = labels
    df["anomaly_score"] = scores

    return df


def update_metrics():
    df = load_data()
    if df is None:
        # Set zeros if we have no data
        ANOMALY_COUNT.set(0)
        ANOMALY_RATIO.set(0)
        ANOMALY_SCORE_MIN.set(0)
        return

    df = compute_anomalies(df)

    total = len(df)
    anomalies = (df["anomaly_label"] == -1).sum()

    ratio = anomalies / total if total > 0 else 0.0
    min_score = df["anomaly_score"].min() if not df.empty else 0.0

    ANOMALY_COUNT.set(float(anomalies))
    ANOMALY_RATIO.set(float(ratio))
    ANOMALY_SCORE_MIN.set(float(min_score))

    print(
        f"[ANOMALY_EXPORTER] total={total} anomalies={anomalies} "
        f"ratio={ratio:.3f} min_score={min_score:.4f}"
    )


def main():
    port = 8001
    print(f"[ANOMALY_EXPORTER] Starting on 0.0.0.0:{port} ...")
    start_http_server(port)

    # Periodically recompute metrics
    while True:
        update_metrics()
        time.sleep(30)  # every 30 seconds


if __name__ == "__main__":
    main()