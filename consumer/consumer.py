"""
consumer.py — reads events from the "clickstream" topic and aggregates
them in a rolling (tumbling) time window, printing a live summary.

This is the same idea as "events per minute" dashboards you'd see in a
real analytics pipeline — just scaled down to a shorter window so it's
demoable in real time instead of making you wait 60 seconds per update.

Run this alongside producer.py:

    python consumer/consumer.py
"""

import json
import time
from collections import Counter, defaultdict

from kafka import KafkaConsumer

BROKER = "localhost:9092"
TOPIC = "clickstream"
WINDOW_SECONDS = 10  # tumbling window size — "events per 10s" instead of "per minute"


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        auto_offset_reset="latest",
        # Named consumer group — if you ran two of these, Kafka/Redpanda
        # would split the topic's partitions between them automatically.
        group_id="clickstream-aggregator",
    )

    print(f"Consuming from '{TOPIC}' on {BROKER}, aggregating every {WINDOW_SECONDS}s. Ctrl+C to stop.\n")

    event_counts = Counter()
    product_counts = defaultdict(Counter)
    window_start = time.time()
    total_events = 0

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)

            for partition_records in records.values():
                for record in partition_records:
                    event = record.value
                    event_counts[event["event_type"]] += 1
                    product_counts[event["event_type"]][event["product_id"]] += 1
                    total_events += 1

            if time.time() - window_start >= WINDOW_SECONDS:
                print_window_summary(event_counts, product_counts, total_events)
                event_counts.clear()
                product_counts.clear()
                window_start = time.time()

    except KeyboardInterrupt:
        print("\nStopping consumer.")
    finally:
        consumer.close()


def print_window_summary(event_counts: Counter, product_counts: dict, running_total: int) -> None:
    print(f"--- window summary ({sum(event_counts.values())} events, {running_total} total so far) ---")
    if not event_counts:
        print("  (no events this window)")
    for event_type, count in event_counts.most_common():
        top_product, top_count = product_counts[event_type].most_common(1)[0]
        print(f"  {event_type:<12} {count:>4}   top product: {top_product} ({top_count}x)")
    print()


if __name__ == "__main__":
    main()
