import json
import os
import time
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError


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
            record = {
                "timestamp": time.time(),
                "bytes": random.randint(5_000, 50_000),
                "latency_ms": round(random.uniform(5, 250), 2),
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