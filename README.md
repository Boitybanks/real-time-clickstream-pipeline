# 📡 Real-Time Clickstream Pipeline

A real-time streaming data pipeline that simulates an e-commerce clickstream and aggregates it live, using Redpanda (a Kafka-API-compatible broker) as the message backbone.

Built for the WeThinkCode_ Data Engineering elective — demonstrates event streaming, consumer groups, and windowed aggregation: the same core concepts behind production systems like fraud detection, live dashboards, and real-time recommendation engines.

---

## 🧠 Architecture

```
 ┌──────────────┐        topic: clickstream        ┌──────────────┐
 │  producer.py │ ───────────────────────────────▶ │  consumer.py │
 │              │   events keyed by user_id         │              │
 │ simulates    │                                    │ aggregates   │
 │ view/click/  │        ┌──────────────┐            │ in 10s       │
 │ cart/purchase│───────▶│   Redpanda   │───────────▶│ tumbling     │
 │ events       │        │   (broker)   │            │ windows      │
 └──────────────┘        └──────────────┘            └──────────────┘
                                 │
                                 ▼
                        Redpanda Console
                       (localhost:8080)
                    — watch topics/partitions
                       and messages live
```

**Why Redpanda instead of raw Kafka?** Same wire protocol, same concepts (topics, partitions, consumer groups) — but it's one Docker container instead of a Kafka + Zookeeper cluster. The engineering concepts you're demonstrating are identical either way.

**Analogy worth knowing for a demo:** Kafka/Redpanda is a **postal sorting facility**. Producers drop letters (events) into labeled bins (topics). Consumers pick up mail from whichever bin they're subscribed to, at their own pace — the producer never waits on the consumer. That decoupling is the entire point of event streaming.

---

## 🚀 Getting Started

### Prerequisites
- Docker + Docker Compose
- Python 3.10+

### 1. Start the broker

```bash
docker compose up -d
```

Give it ~10 seconds to come up. Check the Redpanda Console at **http://localhost:8080** — you should see it connected with no topics yet (the producer creates the topic on first send).

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the consumer first, then the producer (separate terminals)

```bash
# Terminal 1
python consumer/consumer.py

# Terminal 2
python producer/producer.py
```

You'll see the producer logging each event it sends, and every 10 seconds the consumer will print a window summary: event counts by type, plus the top product per type in that window.

---

## 📊 What's actually happening

- **Producer**: generates a realistic funnel — mostly `view` events, fewer `click`, fewer still `add_to_cart`, fewest `purchase` — and publishes each as JSON, keyed by `user_id`.
- **Keying by `user_id`**: guarantees all of one user's events land on the same partition, so if you needed strict per-user ordering, you'd have it for free.
- **Consumer group** (`clickstream-aggregator`): if you ran a second consumer instance with the same group ID, Redpanda would automatically split the topic's partitions between them — that's how this scales horizontally.
- **Tumbling window**: the consumer buffers counts in memory and flushes/prints every `WINDOW_SECONDS` (10s here, scaled down from the more realistic "per minute" so the demo doesn't sit in silence for a full minute).

---

## 🎯 Talking points for your demo

**"Partition = parallelism."** If asked how this scales: more partitions means more consumers can read in parallel — like adding more till operators at checkout. This demo runs a single partition; in production you'd size partition count to your expected consumer parallelism.

**What breaks it:**
- If the consumer goes down, events keep queuing in the topic (nothing is lost) — it just catches up when it comes back, up to Redpanda's retention window.
- If the producer floods faster than the consumer can process, lag builds up on the partition. You'd monitor **consumer lag** as the key health metric in production.

**How you'd scale it 10x:** more partitions + more consumer instances in the same group, and move the in-memory window aggregation to a proper stream-processing layer (Kafka Streams, Flink, or Spark Structured Streaming) instead of a hand-rolled Python loop.

**What it costs to run:** locally, nothing — it's two containers. In production, a managed Kafka/Redpanda cluster (e.g. Redpanda Cloud, Confluent Cloud, AWS MSK) bills per broker-hour plus data transfer; the cost driver is retention period and throughput, not the number of topics.

---

## 📁 Project Structure

```
streaming-pipeline/
├── docker-compose.yml     # Redpanda broker + web console
├── requirements.txt
├── producer/
│   └── producer.py        # simulates clickstream events
└── consumer/
    └── consumer.py        # consumes + windowed aggregation
```

---

## 🛠️ Built With

- Redpanda (Kafka-API-compatible broker)
- Python 3 + `kafka-python`
- Docker Compose

---

## 👤 Author

**Boitumelo Mbhele**
GitHub: [@Boitybanks](https://github.com/Boitybanks)
