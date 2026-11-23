# Kafka Order Pipeline — Avro, Retries and a Dead Letter Queue
# EG/2021/4588 - Jayaweera J. A. P. V.
# EC8202 Big Data Analytics - Assignment Chapter 3


A Kafka producer/consumer pair that moves `Order` messages through Avro
serialisation, computes a **running average of prices** in real time, **retries**
transient failures with exponential backoff, and parks permanently failed
messages in a **Dead Letter Queue**.

Written in **Python 3.11+** using `confluent-kafka`, with a one-command Docker
Compose cluster (Kafka in KRaft mode + Confluent Schema Registry + Kafka UI).

---

## 1. Architecture

```
                          ┌──────────────────────┐
                          │   Schema Registry    │
                          │  subject:            │
                          │  orders-value  (v1)  │
                          └───▲──────────────▲───┘
         register schema      │              │   fetch schema by id
                              │              │
   ┌────────────────┐   ┌─────┴──────┐   ┌───┴──────────────┐
   │  producer.py   │──▶│   orders   │──▶│   consumer.py    │
   │                │   │ (3 parts)  │   │                  │
   │ • random price │   └────────────┘   │ • deserialise    │
   │ • Avro encode  │                    │ • validate       │
   │ • idempotent   │                    │ • running avg ───┼──▶ live stats
   └────────────────┘                    │ • retry ×3       │
                                         │ • manual commit  │
                                         └────────┬─────────┘
                                                  │ terminal failure
                                                  ▼
                                         ┌──────────────────┐
                                         │   orders.DLQ     │
                                         │ original payload │◀── dlq_consumer.py
                                         │ + error headers  │
                                         └──────────────────┘
```

**Message on the wire.** Avro payloads use the Confluent wire format: a magic
byte, a 4-byte schema id, then the binary Avro body. The schema itself is never
shipped per message — the consumer resolves it from the registry by id and
caches it. A 3-field order costs roughly 20–30 bytes instead of the ~350 bytes
the equivalent JSON would take.

---

## 2. Repository layout

```
.
├── docker-compose.yml          Kafka (KRaft) + Schema Registry + Kafka UI
├── Makefile                    make up / produce / consume / dlq / test
├── requirements.txt
├── schemas/
│   └── order.avsc              the Order schema from the assignment brief
├── src/
│   ├── config.py               env-driven settings shared by all entry points
│   ├── producer.py             Avro producer + fault injection switches
│   ├── consumer.py             aggregation + retry + DLQ routing
│   └── dlq_consumer.py         DLQ inspector for the demo
├── tests/
│   └── test_pipeline.py        17 broker-free unit tests
├── docs/
│   └── DEMO.md                 step-by-step live demonstration script
└── .github/workflows/ci.yml    lint, schema validation, tests on every push
```

---

## 3. The schema (`schemas/order.avsc`)

| Field     | Type   | Description                             |
|-----------|--------|-----------------------------------------|
| `orderId` | string | Unique identifier, e.g. `"1001"`        |
| `product` | string | Purchased item, e.g. `"Item1"`          |
| `price`   | float  | Randomised by the producer              |

Registered automatically under the subject `orders-value` the first time the
producer runs. Confirm it with:

```bash
curl -s http://localhost:8081/subjects/orders-value/versions/1 | python -m json.tool
```

---

## 4. Quick start

**Prerequisites:** Docker + Docker Compose, Python 3.11+, and ~2 GB free RAM.

```bash
git clone <your-repo-url> && cd kafka-order-pipeline

python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt

make up          # start the cluster and create the 'orders' + 'orders.DLQ' topics
```

Then, in two terminals:

```bash
# terminal A — consumer with a 30% simulated failure rate
make consume-faulty

# terminal B — 25 orders, 25% of them invalid, plus 2 poison pills
make produce-bad
```

And inspect what failed:

```bash
make dlq
```

Tear down with `make down` (or `make clean` to drop volumes too).

`make help` lists every target.

---

## 5. What the consumer actually does

### 5.1 Running average (real-time aggregation)

The average is maintained **incrementally** — a count and a sum, updated per
message — rather than by buffering prices and recomputing. Memory stays constant
no matter how long the stream runs, which matters for an unbounded topic. Both
an overall average and a per-product average are tracked, and each processed
message prints the updated figures:

```
10:42:07 INFO     | OK   orderId=1003  product=Item4   price=  310.45 | n=3    avg=228.71 | Item4 avg=310.45
```

On shutdown (Ctrl-C) a summary table is printed, and with `--stats-file` the
final numbers are also written as JSON for inclusion in a report.

### 5.2 Failure taxonomy

Not every failure deserves a retry. Retrying a deterministic failure just burns
time and delays the whole partition, so failures are split three ways:

| Failure | Example | Handling |
|---|---|---|
| **Deserialisation** | payload isn't valid Avro (poison pill) | straight to DLQ — bytes will never decode |
| **Permanent** (`PermanentError`) | empty product, price ≤ 0 | straight to DLQ — deterministic, retrying changes nothing |
| **Transient** (`TransientError`) | downstream service unavailable | retry with backoff, then DLQ |

The poison-pill case is the important one: without it, a single malformed
message sits at the head of the partition and the consumer crash-loops on it
forever, blocking every message behind it.

### 5.3 Retry with exponential backoff

Up to `MAX_RETRIES` (default 3) retries, so 4 attempts total. Delays follow
`base × 2^(n-1)` capped at `RETRY_MAX_DELAY`, plus up to 30% jitter:

```
attempt 1 fails → wait ~0.5s
attempt 2 fails → wait ~1.0s
attempt 3 fails → wait ~2.0s
attempt 4 fails → DLQ
```

Jitter matters when several consumer instances hit the same downstream outage —
without it they all retry in lockstep and hammer the recovering service in
synchronised waves.

Retries run **in-line on the poll thread**, which means the partition is
naturally paused while a message is being retried and **ordering within the
partition is preserved**. The worst-case stall (~3.5s) sits far below
`max.poll.interval.ms` (300s), so the broker never mistakes a retrying consumer
for a dead one and triggers a rebalance.

### 5.4 Dead Letter Queue

Failed messages go to `orders.DLQ` with the **original bytes untouched**, so a
replay tool can resubmit them to `orders` once the root cause is fixed. All
diagnostics travel as Kafka headers instead of being baked into the payload,
which keeps the DLQ readable without a second schema:

| Header | Contents |
|---|---|
| `dlq.reason` | `validation` / `retries-exhausted` / `deserialization` |
| `dlq.error` | the exception message |
| `dlq.attempts` | how many times processing was attempted |
| `dlq.failed.at` | UTC timestamp |
| `dlq.origin.topic` / `.partition` / `.offset` | exact source coordinates |

`src/dlq_consumer.py` prints all of this, so the demo can show *why* each
message failed:

```
#2  orders.DLQ[0]@1
  key       : 1007
  reason    : retries-exhausted
  error     : downstream pricing-service unavailable (simulated)
  attempts  : 4
  origin    : orders[2]@4
  payload   : {'orderId': '1007', 'product': 'Item2', 'price': 88.2}
```

### 5.5 Delivery guarantees

**At-least-once**, by construction:

- `enable.auto.commit=False` — the consumer commits offsets itself, never on a timer.
- An offset is committed only once the message reaches a **terminal state**: processed successfully, or acknowledged by the DLQ.
- The DLQ write is flushed **before** the source offset is committed. Committing first would lose the message if the process died in between.
- The producer runs with `acks=all` and `enable.idempotence=true`, so a broker-side retry can't silently duplicate or reorder a record.

The trade-off: a crash between "processed" and "commit" replays the message, so
downstream effects should be idempotent. Exactly-once would require Kafka
transactions tying the DLQ write and the offset commit into one atomic unit —
deliberately out of scope here, and noted in §8.

---

## 6. Configuration

Every setting is an environment variable with a sensible default (`src/config.py`):

| Variable | Default | Purpose |
|---|---|---|
| `BOOTSTRAP_SERVERS` | `localhost:9092` | broker list |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | registry endpoint |
| `ORDERS_TOPIC` | `orders` | main topic |
| `DLQ_TOPIC` | `orders.DLQ` | dead letter topic |
| `CONSUMER_GROUP` | `order-processor` | consumer group id |
| `MAX_RETRIES` | `3` | retries before dead-lettering |
| `RETRY_BASE_DELAY` | `0.5` | first backoff, seconds |
| `RETRY_MAX_DELAY` | `8.0` | backoff ceiling, seconds |

CLI flags:

```bash
python src/producer.py --count 50 --interval 0.3 --bad-rate 0.2 --poison 2
python src/consumer.py --fault-rate 0.3 --stats-file stats.json
python src/dlq_consumer.py --follow
```

---

## 7. Tests

17 unit tests run without a broker — the Kafka client is only touched at the
edges, so aggregation, validation, the retry ladder and DLQ routing are all
testable in isolation:

```bash
pip install pytest
make test
```

They cover the Avro round-trip, incremental averaging, each validation rule,
backoff growth and capping, and all four message outcomes (success, validation
→ DLQ, retries exhausted → DLQ, poison pill → DLQ), plus the case where a
transient failure recovers mid-ladder and is *not* dead-lettered.

CI runs lint, schema validation and the suite on every push.

---

## 8. Known limits and what I'd do next

- **At-least-once, not exactly-once.** Making the DLQ write and the offset commit atomic needs Kafka transactions.
- **In-line retries block the partition.** Fine at this scale; a high-throughput system would use tiered retry topics (`orders.retry.5s`, `orders.retry.30s`) so slow messages don't hold up fast ones.
- **The aggregate lives in consumer memory.** It resets when the process restarts. Kafka Streams with a state store, or an external store, would make it durable and queryable.
- **Single-node cluster, replication factor 1.** Correct for a laptop demo, unacceptable in production.
- **No replay tooling yet.** The DLQ preserves original payloads specifically so a reprocessing job can be added.

---

