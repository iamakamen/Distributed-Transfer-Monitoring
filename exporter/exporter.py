import os
import time
from pathlib import Path

from prometheus_client import start_http_server, Counter, Histogram
from simulator.transfer_simulator import simulate_single_transfer


# Prometheus metrics
TRANSFER_BYTES = Counter(
    "dtms_transfer_bytes_total",
    "Total bytes transferred by the simulator",
)

TRANSFER_DURATION = Histogram(
    "dtms_transfer_duration_seconds",
    "Histogram of transfer durations in seconds",
)

TRANSFER_FAILURES = Counter(
    "dtms_transfer_failures_total",
    "Total number of failed transfers",
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

            TRANSFER_BYTES.inc(metrics["bytes"])
            TRANSFER_DURATION.observe(duration)

            print(
                f"[TRANSFER] bytes={metrics['bytes']} duration={duration:.4f}s"
            )
        except Exception as e:
            print(f"[ERROR] Transfer failed: {e}")
            TRANSFER_FAILURES.inc()

        time.sleep(2)


def main() -> None:
    port = 8000
    print(f"[EXPORTER] Starting HTTP metrics server on 0.0.0.0:{port} ...")
    # Default binds to 0.0.0.0, so Codespaces can port-forward it
    start_http_server(port)

    run_exporter()


if __name__ == "__main__":
    main()