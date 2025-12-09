from pathlib import Path
from typing import List, Dict

import os
import re
import requests
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware

# Enable CORS for external CDN resources
app = FastAPI(
    title="DTMS Monitoring API",
    description="REST API exposing DTMS monitoring and anomaly information per site.",
    version="0.1.0",
    docs_url="/docs",
    openapi_url="/openapi.json",
    redoc_url="/redoc",
)

# Add CORS middleware to allow Swagger UI to load from CDN
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------------
# Paths (inside container)
# -----------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
TRANSFERS_CSV = DATA_DIR / "transfers.csv"

PARQUET_DIR = BASE_DIR / "sample_data" / "parquet" / "site_aggregates"

ANOMALY_METRICS_URL = os.getenv(
    "ANOMALY_METRICS_URL",
    "http://anomaly:8001/metrics",
)
def load_sites_from_transfers() -> List[str]:
    if not TRANSFERS_CSV.exists():
        return []

    df = pd.read_csv(TRANSFERS_CSV)
    if "site" not in df.columns:
        return []

    sites = sorted(df["site"].dropna().unique().tolist())
    return sites


def load_aggregates_from_parquet() -> List[Dict]:
    if not PARQUET_DIR.exists():
        raise FileNotFoundError(f"No Parquet directory found at {PARQUET_DIR}")

    # Read all Parquet files under site_aggregates
    df = pd.read_parquet(PARQUET_DIR.as_posix())

    # If no src_site column, bail out
    if "src_site" not in df.columns:
        raise ValueError("Parquet data does not contain 'src_site' column.")

    # Group per src_site and compute averages of the aggregate metrics themselves
    grouped = (
        df.groupby("src_site")
        .agg(
            avg_bytes=("avg_bytes", "mean"),
            avg_latency_ms=("avg_latency", "mean"),
        )
        .reset_index()
    )

    records = grouped.to_dict(orient="records")
    return records


def load_anomalies_from_metrics() -> List[Dict]:
    """
    Scrape the anomaly exporter's /metrics endpoint and extract
    dtms_anomaly_* metrics per site.
    """
    try:
        resp = requests.get(ANOMALY_METRICS_URL, timeout=5)
        resp.raise_for_status()
    except Exception as e:
        raise RuntimeError(f"Failed to fetch anomaly metrics: {e}")

    text = resp.text

    # Patterns for our three metrics
    patterns = {
        "count": re.compile(r'^dtms_anomaly_count\{site="(?P<site>[^"]+)"\}\s+(?P<value>[-0-9\.eE]+)$'),
        "ratio": re.compile(r'^dtms_anomaly_ratio\{site="(?P<site>[^"]+)"\}\s+(?P<value>[-0-9\.eE]+)$'),
        "score_min": re.compile(r'^dtms_anomaly_score_min\{site="(?P<site>[^"]+)"\}\s+(?P<value>[-0-9\.eE]+)$'),
    }

    result: Dict[str, Dict[str, float]] = {}

    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#") or not line:
            continue

        for key, pattern in patterns.items():
            m = pattern.match(line)
            if m:
                site = m.group("site")
                value = float(m.group("value"))
                if site not in result:
                    result[site] = {}
                result[site][key] = value

    # Convert to list of dicts
    anomalies = []
    for site, vals in result.items():
        anomalies.append(
            {
                "site": site,
                "anomaly_count": vals.get("count", 0.0),
                "anomaly_ratio": vals.get("ratio", 0.0),
                "anomaly_score_min": vals.get("score_min", 0.0),
            }
        )

    return anomalies


# -----------------------------
# API Endpoints
# -----------------------------
@app.get("/health")
def health():
    return {"status": "ok", "service": "dtms-api"}


@app.get("/sites")
def get_sites():
    sites = load_sites_from_transfers()
    return {"sites": sites}


@app.get("/aggregates")
def get_aggregates():
    try:
        records = load_aggregates_from_parquet()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load aggregates: {e}")

    return {"aggregates": records}


@app.get("/anomalies")
def get_anomalies():
    try:
        anomalies = load_anomalies_from_metrics()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load anomalies: {e}")

    return {"anomalies": anomalies}
