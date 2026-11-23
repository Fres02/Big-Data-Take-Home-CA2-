# Live Demonstration Script

Roughly 8–10 minutes. Run `make up` and `make reset` **before** the session
starts so no one watches Docker pull images.

Terminal layout:

| | |
|---|---|
| **A** | consumer |
| **B** | producer |
| **C** | DLQ inspector / curl |

Optionally open Kafka UI at <http://localhost:8080> as a fourth surface.

---

## Step 0 — Setup (before the demo, off camera)

```bash
make up      # cluster + topics
make reset   # wipe topics for a clean run
pytest -q    # confirm 17 tests pass
```

---

## Step 1 — Show the schema (~1 min)

```bash
cat schemas/order.avsc
```

Say: three fields, exactly as specified in the brief. The schema is registered
once with Schema Registry; messages carry only a 4-byte schema id, not the
schema itself.

---

## Step 2 — Happy path (~2 min)

**Terminal A:**
```bash
make consume
```

**Terminal B:**
```bash
make produce
```

Point at the consumer output as it scrolls:

```
OK   orderId=1001  product=Item3   price=  147.02 | n=1    avg=147.02 | Item3 avg=147.02
OK   orderId=1002  product=Item1   price=  310.45 | n=2    avg=228.74 | Item1 avg=310.45
```

Say: `avg` updates on every message — count and sum only, no buffering, so
memory is constant regardless of how long the stream runs. Per-product averages
are tracked alongside the overall one.

**Terminal C** — prove the schema got registered:
```bash
curl -s http://localhost:8081/subjects
curl -s http://localhost:8081/subjects/orders-value/versions/1 | python -m json.tool
```

Stop the consumer with Ctrl-C to show the summary table.

---

## Step 3 — Retries (~2 min)

**Terminal A:**
```bash
make consume-faulty     # 30% simulated transient failures
```

**Terminal B:**
```bash
make produce
```

Look for the retry ladder:

```
RETRY orderId=1014 attempt 1/4 failed (downstream pricing-service unavailable) - backing off 0.58s
RETRY orderId=1014 attempt 2/4 failed (downstream pricing-service unavailable) - backing off 1.13s
OK   orderId=1014  product=Item2  price=   88.20 | n=9  avg=201.33 | Item2 avg=152.60
```

Say: exponential backoff with jitter. The jitter stops multiple consumers from
retrying in lockstep and stampeding a downstream that's trying to recover. Most
transient failures clear within the ladder and never reach the DLQ.

---

## Step 4 — DLQ (~3 min)

Leave the faulty consumer running in **A**.

**Terminal B:**
```bash
make produce-bad     # 25% rule violations + 2 poison pills
```

Three distinct failure paths appear in **A**:

```
DLQ  <- orders[1]@12  reason=validation        attempts=1  product name is empty
DLQ  <- orders[0]@7   reason=retries-exhausted attempts=4  downstream pricing-service unavailable
DLQ  <- orders[2]@9   reason=deserialization   attempts=1  SerializationError: unknown magic byte
```

Call out the `attempts` column — validation failures are dead-lettered
**immediately**, because an empty product name will still be empty on the fourth
try. Only transient failures earn retries.

**Terminal C:**
```bash
make dlq
```

Walk through one entry: the original payload is preserved byte-for-byte and the
diagnosis lives in headers, so a replay job could resubmit it to `orders` once
the root cause is fixed.

Then the key point about the poison pill: that message was never valid Avro.
Without a DLQ it would sit at the head of its partition and the consumer would
crash-loop on it forever, blocking every message behind it. Routing it aside
keeps the pipeline moving.

---

## Step 5 — Delivery guarantees (~1 min)

Show these four lines in `src/consumer.py`:

1. `"enable.auto.commit": False`
2. `consumer.commit(message=msg, asynchronous=False)` — after a terminal state only
3. `self._producer.flush(10)` inside `DeadLetterQueue.send` — DLQ write lands before the commit
4. `"enable.idempotence": True` on the producer

Say: at-least-once. Order of operations is the whole guarantee — committing the
offset before the DLQ write is acknowledged would lose the message on a crash.

---

## Step 6 — Recovery (~1 min, optional)

Kill the consumer mid-stream with Ctrl-C while the producer is still running,
then restart it:

```bash
make consume
```

It resumes from the last committed offset. Nothing is lost; at worst an
in-flight message is reprocessed, which is the at-least-once trade-off.

---

## Likely questions

**Why not retry the poison pill?**
Deserialisation is deterministic. Bytes that aren't valid Avro on attempt 1
won't be on attempt 4 — retrying only delays the partition.

**Why in-line retries rather than a retry topic?**
In-line keeps ordering within the partition and is far simpler. The cost is that
one slow message blocks its partition. At higher throughput I'd move to tiered
retry topics (`orders.retry.5s`, `orders.retry.30s`) so retries happen off the
main path.

**Why is the DLQ payload raw bytes instead of a decoded record?**
So it can be replayed verbatim. Re-encoding risks lossy round-trips, and for
poison pills there's nothing to decode in the first place.

**What happens if the DLQ write itself fails?**
`flush()` blocks and the delivery callback logs at CRITICAL. Since the offset is
not committed, the message is redelivered on restart rather than lost.

**Why manual commits?**
Auto-commit fires on a timer and can commit an offset for a message that is
still being retried — the definition of silent data loss.

**Could you do exactly-once?**
Yes, with Kafka transactions binding the DLQ produce and the offset commit into
one atomic operation. It was out of scope here and is listed under known limits.

---

## Recording checklist

- [ ] Terminal font large enough to read on a projector
- [ ] `make reset` run beforehand so offsets and topics are clean
- [ ] All three terminals visible at once, or switched deliberately
- [ ] Screenshots saved to `docs/screenshots/` and committed
- [ ] Repo pushed, with CI green on `main`
