"""
producer.py — simulates a live e-commerce clickstream and publishes each
event to the "clickstream" topic on Redpanda (Kafka-compatible broker).

Run this alongside consumer.py to see events flow end-to-end:

    python producer/producer.py
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC = "clickstream"

EVENT_TYPES = ["view", "click", "add_to_cart", "purchase"]
# Weighted so it behaves like a real funnel: lots of views, few purchases.
EVENT_WEIGHTS = [0.55, 0.25, 0.13, 0.07]

PRODUCTS = [f"PROD-{n:03d}" for n in range(1, 21)]
USERS = [f"user-{n:03d}" for n in range(1, 51)]


def build_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(USERS),
        "product_id": random.choice(PRODUCTS),
        "event_type": random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # user_id as the key means all of one user's events land on the same
        # partition — useful if you ever need per-user ordering guarantees.
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
    )

    print(f"Producing events to '{TOPIC}' on {BROKER}. Ctrl+C to stop.\n")

    try:
        while True:
            event = build_event()
            producer.send(TOPIC, key=event["user_id"], value=event)
            print(f"  -> sent {event['event_type']:<12} {event['product_id']} ({event['user_id']})")
            time.sleep(random.uniform(0.2, 1.2))
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
