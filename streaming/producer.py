import json
import os
import time
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError


# -------------------------------------------------
# Multi-site configuration
# -------------------------------------------------
SITES = ["SITE_A", "SITE_B", "SITE_C"]


def random_site_pair():
    """Pick two different sites: (src_site, dst_site)."""
    src_site, dst_site = random.sample(SITES, 2)
    return src_site, dst_site


def create_producer(bootstrap_servers: str) -> KafkaProducer:
    """Retry until Kafka is available."""
    while True:
        try:
            print(f"[producer] Trying to connect to Kafka at {bootstrap_servers} ...")
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                request_timeout_ms=10000,
                retries=3,
            )
            print("[producer] Connected to Kafka.")
            return producer
        except (NoBrokersAvailable, KafkaError) as e:
            print(f"[producer] Kafka connection failed: {e}. Retrying in 5s...")
            time.sleep(5)
        except Exception as e:
            print(f"[producer] Unexpected error during connection: {type(e).__name__}: {e}")
            time.sleep(5)


def main():
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
    print(f"[producer] Using bootstrap servers: {bootstrap_servers}")

    producer = create_producer(bootstrap_servers)

    while True:
        try:
            # Pick a random source and destination site
            src_site, dst_site = random_site_pair()

            # Simulate transfer characteristics
            bytes_ = random.randint(50_000, 5_000_000)  # 50 KB to ~5 MB
            latency_ms = round(random.uniform(5, 250), 2)  # 5–250 ms
            duration_sec = latency_ms / 1000.0

            # Mostly OK, sometimes FAIL
            status = "OK" if random.random() < 0.95 else "FAILED"

            record = {
                "event_type": "data_transfer",
                "src_site": src_site,
                "dst_site": dst_site,
                "reporting_site": src_site,
                "protocol": "file_transfer",
                "bytes": bytes_,
                "duration": duration_sec,
                "latency_ms": latency_ms,
                "status": status,
                "timestamp": time.time(),
            }

            future = producer.send("dtms_transfers", record)
            # Wait for the send to complete with a timeout
            future.get(timeout=5)
            print("[producer] Sent:", record)
        except KafkaError as e:
            print(f"[producer] Failed to send message: {e}")
            time.sleep(1)
        except Exception as e:
            print(f"[producer] Unexpected error: {type(e).__name__}: {e}")
            time.sleep(1)

        time.sleep(1)


if __name__ == "__main__":
    main()
