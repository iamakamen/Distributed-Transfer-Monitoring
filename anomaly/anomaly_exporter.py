import time
import sys
from pathlib import Path

import pandas as pd
from prometheus_client import start_http_server, Gauge
from sklearn.ensemble import IsolationForest

# Unbuffered stdout for Docker logging
sys.stdout.reconfigure(line_buffering=True)


DATA_DIR = Path(__file__).resolve().parent.parent / "data"
TRANSFERS_CSV = DATA_DIR / "transfers.csv"
ANOMALIES_CSV = DATA_DIR / "anomalies.csv"

# Prometheus metrics (now per site)
ANOMALY_COUNT = Gauge(
    "dtms_anomaly_count",
    "Number of anomalous transfers detected in the dataset per site",
    ["site"],
)

ANOMALY_RATIO = Gauge(
    "dtms_anomaly_ratio",
    "Fraction of transfers detected as anomalous per site",
    ["site"],
)

ANOMALY_SCORE_MIN = Gauge(
    "dtms_anomaly_score_min",
    "Minimum (most anomalous) IsolationForest score in the dataset per site",
    ["site"],
)


def load_data():
    if not TRANSFERS_CSV.exists():
        print(f"[ANOMALY_EXPORTER] No transfers.csv found at {TRANSFERS_CSV}")
        return None

    try:
        # on_bad_lines='skip' is the correct parameter for pandas 2.x
        df = pd.read_csv(TRANSFERS_CSV, on_bad_lines='skip')
        if df.empty:
            print("[ANOMALY_EXPORTER] CSV is empty after skipping bad lines")
            return None
    except Exception as e:
        print(f"[ANOMALY_EXPORTER] Error reading CSV: {e}")
        return None

    # Basic filtering
    df = df[df["duration"] > 0].copy()
    if df.empty:
        print("[ANOMALY_EXPORTER] No valid rows after filtering")
        return None

    df["throughput_bytes_per_sec"] = df["throughput_bytes_per_sec"].fillna(0)

    # Ensure we have a site column (for multi-site metrics)
    if "site" not in df.columns:
        df["site"] = "UNKNOWN"

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
        # No data yet; do not update metrics
        print("[ANOMALY_EXPORTER] No data available; metrics not updated.")
        return

    df = compute_anomalies(df)

    # Save anomalies to CSV for correlation job
    anomalies_df = df[df["anomaly_label"] == -1].copy()
    if not anomalies_df.empty:
        # Ensure we have the columns needed by correlation job
        if "timestamp_unix" not in anomalies_df.columns and "timestamp" in anomalies_df.columns:
            anomalies_df["timestamp_unix"] = anomalies_df["timestamp"]
        ANOMALIES_CSV.parent.mkdir(parents=True, exist_ok=True)
        anomalies_df.to_csv(ANOMALIES_CSV, index=False)
        print(f"[ANOMALY_EXPORTER] Saved {len(anomalies_df)} anomalies to {ANOMALIES_CSV}")

    # Compute metrics per site
    for site, site_df in df.groupby("site"):
        total = len(site_df)
        anomalies = (site_df["anomaly_label"] == -1).sum()

        ratio = anomalies / total if total > 0 else 0.0
        min_score = site_df["anomaly_score"].min() if not site_df.empty else 0.0

        ANOMALY_COUNT.labels(site=site).set(float(anomalies))
        ANOMALY_RATIO.labels(site=site).set(float(ratio))
        ANOMALY_SCORE_MIN.labels(site=site).set(float(min_score))

        print(
            f"[ANOMALY_EXPORTER] site={site} total={total} anomalies={anomalies} "
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
