"""
Kafka producer for the RetailRocket e-commerce dataset.

Reads the real RetailRocket events.csv and streams each row to the
Kafka topic `events`, simulating real-time user activity on an
e-commerce site.

Dataset:
    https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset

Schema (RetailRocket events.csv):
    timestamp, visitorid, event, itemid, transactionid
"""

import csv
import json
import os
import sys
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "events")
DATA_FILE = os.getenv("DATA_FILE", "/data/events.csv")
EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "20"))


def pick_data_file() -> str:
    """Return the path to the real RetailRocket events.csv. Fail loud if missing."""
    if os.path.exists(DATA_FILE):
        print(f"[producer] Using RetailRocket dataset: {DATA_FILE}", flush=True)
        return DATA_FILE
    print(
        "[producer] ERROR: events.csv not found.\n"
        f"  Expected at: {DATA_FILE}\n"
        "  Download from https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset\n"
        "  and place events.csv inside the ./data folder before running.",
        file=sys.stderr,
        flush=True,
    )
    sys.exit(1)


def connect_producer(retries: int = 30, delay: int = 5) -> KafkaProducer:
    """Wait for Kafka to come up, then return a connected producer."""
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda v: str(v).encode("utf-8") if v is not None else None,
                acks="all",
                linger_ms=50,
            )
            print(f"[producer] Connected to Kafka at {KAFKA_BOOTSTRAP}", flush=True)
            return producer
        except NoBrokersAvailable:
            print(
                f"[producer] Kafka not ready (attempt {attempt}/{retries}), "
                f"retrying in {delay}s...",
                flush=True,
            )
            time.sleep(delay)
    print("[producer] Could not connect to Kafka, giving up.", file=sys.stderr, flush=True)
    sys.exit(1)


def row_to_event(row: dict) -> dict:
    """Normalize a CSV row into the JSON event we publish to Kafka."""
    try:
        ts_ms = int(row["timestamp"])
    except (KeyError, ValueError, TypeError):
        ts_ms = int(time.time() * 1000)

    return {
        "timestamp": ts_ms,
        "event_time": datetime.utcfromtimestamp(ts_ms / 1000.0).isoformat(),
        "visitorid": row.get("visitorid", ""),
        "event": row.get("event", ""),
        "itemid": row.get("itemid", ""),
        "transactionid": row.get("transactionid", "") or None,
        "ingest_time": datetime.utcnow().isoformat(),
    }


def main() -> None:
    data_path = pick_data_file()
    producer = connect_producer()

    sleep_seconds = 1.0 / EVENTS_PER_SECOND if EVENTS_PER_SECOND > 0 else 0
    total_sent = 0

    print(
        f"[producer] Streaming {data_path} -> topic '{KAFKA_TOPIC}' "
        f"at ~{EVENTS_PER_SECOND} events/sec",
        flush=True,
    )

    while True:
        with open(data_path, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                event = row_to_event(row)
                producer.send(
                    KAFKA_TOPIC,
                    key=event["visitorid"],
                    value=event,
                )
                total_sent += 1
                if total_sent % 500 == 0:
                    producer.flush()
                    print(
                        f"[producer] sent {total_sent} events "
                        f"(latest: {event['event']} item={event['itemid']})",
                        flush=True,
                    )
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

        producer.flush()
        print(
            f"[producer] End of file reached, looping again to keep the "
            f"stream alive (total sent: {total_sent})",
            flush=True,
        )
        time.sleep(2)


if __name__ == "__main__":
    main()
