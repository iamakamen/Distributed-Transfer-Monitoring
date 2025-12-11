import os
import time
import shutil
from pathlib import Path
from typing import Dict, Any


def simulate_single_transfer(src_file: Path, dest_file: Path) -> Dict[str, Any]:
    """
    Simulate a single file transfer by copying the file and measuring
    time + bytes transferred.

    Returns a dictionary with metrics.
    """
    start_time = time.time()

    # Ensure destination directory exists
    dest_file.parent.mkdir(parents=True, exist_ok=True)

    # Copy the file
    shutil.copy2(src_file, dest_file)

    end_time = time.time()
    duration = end_time - start_time
    bytes_transferred = dest_file.stat().st_size

    return {
        "src": str(src_file),
        "dest": str(dest_file),
        "bytes": bytes_transferred,
        "duration": duration,
        "status": "success",
        "timestamp": start_time,
    }


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    input_dir = base_dir / "input"
    output_dir = base_dir / "output"

    # Create input/output directories
    input_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)

    dummy_file = input_dir / "dummy.data"

    # If the file does not exist, create it with some bytes
    if not dummy_file.exists():
        with open(dummy_file, "wb") as f:
            # 1 MB of random data
            f.write(os.urandom(1024 * 1024))

    # Simulate a few transfers
    for i in range(5):
        dest_file = output_dir / f"dummy_copy_{i}.data"
        metrics = simulate_single_transfer(dummy_file, dest_file)

        print(
            f"[TRANSFER] src={metrics['src']} dest={metrics['dest']} "
            f"bytes={metrics['bytes']} duration={metrics['duration']:.4f}s"
        )
        time.sleep(1)


if __name__ == "__main__":
    main()
