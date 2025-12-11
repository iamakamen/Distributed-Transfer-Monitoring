import csv
from datetime import datetime, timezone

import os
import time
from pathlib import Path

from prometheus_client import start_http_server, Counter, Histogram
from simulator.transfer_simulator import simulate_single_transfer


# ----------------------------------------
# NEW: Site awareness via environment variable
# ----------------------------------------
SITE_NAME = os.getenv("SITE_NAME", "SITE_A")
print(f"[EXPORTER] Running in site: {SITE_NAME}")


# Prometheus metrics WITH site label
TRANSFER_BYTES = Counter(
    "dtms_transfer_bytes_total",
    "Total bytes transferred by the simulator",
    ["site"],
)

TRANSFER_DURATION = Histogram(
    "dtms_transfer_duration_seconds",
    "Histogram of transfer durations in seconds",
    ["site"],
)

TRANSFER_FAILURES = Counter(
    "dtms_transfer_failures_total",
    "Total number of failed transfers",
    ["site"],
)


DATA_DIR = Path(__file__).resolve().parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)
TRANSFERS_CSV = DATA_DIR / "transfers.csv"


def append_transfer_to_csv(metrics: dict) -> None:
    """
    Append a single transfer record to a CSV file.
    Creates the file with header if it does not exist yet.
    """
    file_exists = TRANSFERS_CSV.exists()

    with open(TRANSFERS_CSV, "a", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "timestamp_iso",
                "timestamp_unix",
                "bytes",
                "duration",
                "throughput_bytes_per_sec",
                "status",
                "site",  # NEW column
            ],
        )
        if not file_exists:
            writer.writeheader()

        duration = metrics["duration"]
        bytes_ = metrics["bytes"]
        throughput = bytes_ / duration if duration > 0 else 0.0

        writer.writerow(
            {
                "timestamp_iso": datetime.fromtimestamp(
                    metrics["timestamp"], tz=timezone.utc
                ).isoformat().replace("+00:00", "Z"),
                "timestamp_unix": metrics["timestamp"],
                "bytes": bytes_,
                "duration": duration,
                "throughput_bytes_per_sec": throughput,
                "status": metrics["status"],
                "site": SITE_NAME,  # NEW column value
            }
        )


def run_exporter() -> None:
    base_dir = Path(__file__).resolve().parent.parent
    simulator_dir = base_dir / "simulator"
    input_dir = simulator_dir / "input"
    output_dir = simulator_dir / "output"

    input_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)

    dummy_file = input_dir / "dummy.data"

    # Create dummy file if not present
    if not dummy_file.exists():
        with open(dummy_file, "wb") as f:
            f.write(os.urandom(1024 * 1024))  # 1 MB

    print("[EXPORTER] Starting transfer loop...")
    while True:
        try:
            timestamp = int(time.time())
            dest_file = output_dir / f"dummy_{timestamp}.data"

            metrics = simulate_single_transfer(dummy_file, dest_file)
            duration = metrics["duration"]

            # USE labelling
            TRANSFER_BYTES.labels(site=SITE_NAME).inc(metrics["bytes"])
            TRANSFER_DURATION.labels(site=SITE_NAME).observe(duration)

            append_transfer_to_csv(metrics)

            print(
                f"[TRANSFER][{SITE_NAME}] bytes={metrics['bytes']} duration={duration:.4f}s"
            )
        except Exception as e:
            print(f"[ERROR][{SITE_NAME}] Transfer failed: {e}")
            TRANSFER_FAILURES.labels(site=SITE_NAME).inc()

        time.sleep(2)


def main() -> None:
    port = 8000
    print(f"[EXPORTER] Starting HTTP metrics server on 0.0.0.0:{port} ...")
    # Default binds to 0.0.0.0, so Codespaces can port-forward it
    start_http_server(port)

    run_exporter()


if __name__ == "__main__":
    main()
