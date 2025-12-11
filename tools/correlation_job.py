#!/usr/bin/env python3
"""
Cron job: compute correlation between anomaly counts and throughput
and push a single metric to Pushgateway.

- Reads from /app/data/parquet/site_aggregates/ if exists (Parquet), else uses transfers.csv
- Reads anomalies.csv (from anomaly/isolation runner) if exists
- Computes Pearson correlation between per-minute anomaly_count and mean_throughput across sites
- Pushes metric dtms_corr_anomaly_throughput (float) to Pushgateway job=correlation
"""
import os
import time
import math
import requests
import pandas as pd
from pathlib import Path
from scipy.stats import pearsonr

DATA_DIR = Path("/app/data")
PARQUET_DIR = DATA_DIR / "parquet" / "site_aggregates"
TRANSFERS_CSV = DATA_DIR / "transfers.csv"
ANOMALIES_CSV = DATA_DIR / "anomalies.csv"

PUSHGATEWAY = os.environ.get("PUSHGATEWAY_URL", "http://pushgateway:9091")
JOB_NAME = os.environ.get("PUSHGATEWAY_JOB", "dtms_correlation")
BUCKET = os.environ.get("PUSHGATEWAY_INSTANCE", "default")

def load_site_timeseries(minutes=30):
    """Return DataFrame with columns ['minute','site','avg_throughput','anomaly_count'] aggregated per-minute."""
    now = time.time()
    since = now - (minutes * 60)
    dfs = []
    if PARQUET_DIR.exists():
        # read parquet files (assume they have columns: timestamp_unix, site, throughput)
        try:
            df = pd.read_parquet(str(PARQUET_DIR))
        except Exception:
            # try reading multiple parquet files
            files = list(PARQUET_DIR.glob("**/*.parquet"))
            if not files:
                df = pd.DataFrame()
            else:
                df = pd.concat([pd.read_parquet(str(f)) for f in files], ignore_index=True)
    elif TRANSFERS_CSV.exists():
        try:
            df = pd.read_csv(TRANSFERS_CSV, on_bad_lines='skip')
            if df.empty:
                print("[CORRELATION] CSV is empty after skipping bad lines")
                df = pd.DataFrame()
        except Exception as e:
            print(f"[CORRELATION] Error reading CSV: {e}")
            df = pd.DataFrame()
    else:
        df = pd.DataFrame()

    # normalize timestamp column
    if df.empty:
        return pd.DataFrame()

    if "timestamp_unix" not in df.columns and "timestamp" in df.columns:
        df["timestamp_unix"] = df["timestamp"]

    df = df.dropna(subset=["timestamp_unix"])
    df = df[df["timestamp_unix"] >= since].copy()
    if df.empty:
        return pd.DataFrame()

    # ensure site column
    if "site" not in df.columns:
        df["site"] = "UNKNOWN"

    # throughput: prefer throughput_bytes_per_sec, else compute bytes/duration
    if "throughput_bytes_per_sec" not in df.columns:
        df["throughput_bytes_per_sec"] = df.apply(lambda r: r["bytes"]/r["duration"] if r.get("duration",0)>0 else 0, axis=1)

    # round down to minute
    df["minute"] = (df["timestamp_unix"] // 60).astype(int)
    agg = df.groupby(["minute","site"]).agg(
        avg_throughput=("throughput_bytes_per_sec","mean"),
        transfers_count=("bytes","count")
    ).reset_index()
    return agg

def load_anomalies(minutes=30):
    now = time.time()
    since = now - (minutes * 60)
    if not ANOMALIES_CSV.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(ANOMALIES_CSV, on_bad_lines='skip')
        if df.empty:
            print("[CORRELATION] Anomalies CSV is empty after skipping bad lines")
            return pd.DataFrame()
    except Exception as e:
        print(f"[CORRELATION] Error reading anomalies CSV: {e}")
        return pd.DataFrame()
    if "timestamp_unix" not in df.columns and "timestamp" in df.columns:
        df["timestamp_unix"] = df["timestamp"]
    df = df[df["timestamp_unix"] >= since].copy()
    if "site" not in df.columns:
        df["site"] = "UNKNOWN"
    df["minute"] = (df["timestamp_unix"] // 60).astype(int)
    agg = df.groupby(["minute","site"]).size().reset_index(name="anomaly_count")
    return agg

def compute_global_correlation(minutes=180):
    thr = load_site_timeseries(minutes)
    ann = load_anomalies(minutes)
    if thr.empty or ann.empty:
        return None
    merged = thr.merge(ann, on=["minute","site"], how="outer").fillna(0)
    # for global correlation across all minutes+sites:
    try:
        corr, pval = pearsonr(merged["anomaly_count"], merged["avg_throughput"])
    except Exception:
        return None
    return float(corr)

def push_to_pushgateway(value):
    # metric name dtms_corr_anomaly_throughput
    metric_name = "dtms_corr_anomaly_throughput"
    payload = f"# TYPE {metric_name} gauge\n{metric_name} {value}\n"
    url = f"{PUSHGATEWAY}/metrics/job/{JOB_NAME}/instance/{BUCKET}"
    try:
        resp = requests.put(url, data=payload, timeout=10)
        resp.raise_for_status()
        print(f"Pushed {value} to {url} (status {resp.status_code})")
    except Exception as e:
        print("Push failed:", e)

def main():
    # Widen lookback to 180 minutes so we have enough overlapping transfer and anomaly data
    corr = compute_global_correlation(minutes=180)
    if corr is None or math.isnan(corr):
        print("No correlation computed (not enough data).")
        return
    print("Computed correlation:", corr)
    push_to_pushgateway(corr)

if __name__ == "__main__":
    main()
