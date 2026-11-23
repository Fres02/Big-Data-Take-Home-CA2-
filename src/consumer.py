#!/usr/bin/env python3
"""Order consumer: Avro deserialisation + running average + retry + DLQ.

Processing contract
-------------------
Offsets are committed manually and only after a message reaches a terminal
state (processed, or parked in the DLQ). Combined with `enable.auto.commit=False`
this gives at-least-once delivery: a crash mid-retry replays the message rather
than silently dropping it.

Failure taxonomy
----------------
  DeserialisationError  -> DLQ immediately. Retrying cannot fix malformed bytes;
                           a poison pill would otherwise block the partition
                           forever.
  PermanentError        -> DLQ immediately. Business-rule violations (empty
                           product, non-positive price) are deterministic.
  TransientError        -> retry up to MAX_RETRIES with exponential backoff and
                           jitter, then DLQ. Models a flaky downstream (pricing
                           service, database) that may recover on its own.

Because retries happen in-line on the poll thread, the partition is naturally
paused while a message is being retried, so ordering within the partition is
preserved. Total worst-case stall is bounded by the backoff schedule, which is
kept well under `max.poll.interval.ms`.

Aggregation
-----------
A running average is maintained incrementally (count + sum) rather than by
buffering prices, so memory stays constant regardless of stream length. Both an
overall average and a per-product average are tracked.

Examples
--------
    python src/consumer.py
    python src/consumer.py --fault-rate 0.3     # inject transient failures
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import config

LOG = logging.getLogger("consumer")

_running = True


class TransientError(Exception):
    """Failure that may succeed on a later attempt."""


class PermanentError(Exception):
    """Failure that will never succeed, no matter how many times it is retried."""


# --------------------------------------------------------------------------- #
# Aggregation
# --------------------------------------------------------------------------- #
@dataclass
class RunningAverage:
    count: int = 0
    total: float = 0.0
    minimum: float = float("inf")
    maximum: float = float("-inf")

    def add(self, value: float) -> None:
        self.count += 1
        self.total += value
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)

    @property
    def average(self) -> float:
        return self.total / self.count if self.count else 0.0


@dataclass
class Aggregator:
    overall: RunningAverage = field(default_factory=RunningAverage)
    by_product: dict[str, RunningAverage] = field(default_factory=dict)

    def add(self, product: str, price: float) -> None:
        self.overall.add(price)
        self.by_product.setdefault(product, RunningAverage()).add(price)

    def summary(self) -> str:
        lines = [
            "",
            "=" * 62,
            "AGGREGATION SUMMARY",
            "=" * 62,
            f"  messages processed : {self.overall.count}",
            f"  running average    : {self.overall.average:.2f}",
        ]
        if self.overall.count:
            lines.append(f"  min / max price    : {self.overall.minimum:.2f} / {self.overall.maximum:.2f}")
        lines.append("-" * 62)
        lines.append(f"  {'product':<12}{'count':>8}{'average':>12}{'total':>12}")
        for product, stats in sorted(self.by_product.items()):
            lines.append(
                f"  {product:<12}{stats.count:>8}{stats.average:>12.2f}{stats.total:>12.2f}"
            )
        lines.append("=" * 62)
        return "\n".join(lines)


# --------------------------------------------------------------------------- #
# Business logic
# --------------------------------------------------------------------------- #
def process_order(order: dict, fault_rate: float) -> None:
    """Validate and 'handle' one order.

    Raises PermanentError for bad data and TransientError for simulated
    downstream outages.
    """
    product = order.get("product")
    price = order.get("price")

    if not product or not str(product).strip():
        raise PermanentError("product name is empty")
    if price is None:
        raise PermanentError("price is missing")
    if price <= 0:
        raise PermanentError(f"price must be positive, got {price}")

    # Stand-in for a real downstream call (enrichment service, database write).
    if fault_rate and random.random() < fault_rate:
        raise TransientError("downstream pricing-service unavailable (simulated)")


def backoff_delay(attempt: int) -> float:
    """Exponential backoff with jitter, capped at RETRY_MAX_DELAY."""
    raw = min(config.RETRY_BASE_DELAY * (2 ** (attempt - 1)), config.RETRY_MAX_DELAY)
    jitter = raw * config.RETRY_JITTER * random.random()
    return raw + jitter


# --------------------------------------------------------------------------- #
# Dead Letter Queue
# --------------------------------------------------------------------------- #
class DeadLetterQueue:
    """Publishes failed records to the DLQ topic, original bytes untouched.

    The payload is forwarded verbatim so a replay tool can re-submit it to the
    main topic once the underlying problem is fixed. All diagnostics travel as
    Kafka headers, which keeps the DLQ readable by any consumer without needing
    a second schema.
    """

    def __init__(self, producer: Producer, topic: str) -> None:
        self._producer = producer
        self._topic = topic
        self.count = 0

    def send(self, msg, reason: str, error: str, attempts: int) -> None:
        headers = [
            ("dlq.reason", reason),
            ("dlq.error", error[:900]),
            ("dlq.attempts", str(attempts)),
            ("dlq.failed.at", datetime.now(timezone.utc).isoformat()),
            ("dlq.origin.topic", msg.topic()),
            ("dlq.origin.partition", str(msg.partition())),
            ("dlq.origin.offset", str(msg.offset())),
        ]
        self._producer.produce(
            topic=self._topic,
            key=msg.key(),
            value=msg.value(),
            headers=headers,
            on_delivery=self._on_delivery,
        )
        # Block until the DLQ write is acknowledged. Committing the source
        # offset before this lands would lose the message on a crash.
        self._producer.flush(10)
        self.count += 1
        LOG.error(
            "DLQ  <- %s[%d]@%d  reason=%s attempts=%d  %s",
            msg.topic(), msg.partition(), msg.offset(), reason, attempts, error,
        )

    @staticmethod
    def _on_delivery(err, msg) -> None:
        if err is not None:
            LOG.critical("DLQ WRITE FAILED (%s) - message may be lost", err)


# --------------------------------------------------------------------------- #
# Main loop
# --------------------------------------------------------------------------- #
def handle_message(msg, deserializer, ctx, aggregator, dlq, fault_rate) -> None:
    # 1. Deserialise. Malformed bytes are terminal.
    try:
        order = deserializer(msg.value(), ctx)
        if order is None:
            raise ValueError("tombstone / empty payload")
    except Exception as exc:  # noqa: BLE001 - any decode failure is terminal
        dlq.send(msg, reason="deserialization", error=f"{type(exc).__name__}: {exc}", attempts=1)
        return

    # 2. Process, with retries for transient failures only.
    attempts = 0
    total_attempts = config.MAX_RETRIES + 1

    while attempts < total_attempts:
        attempts += 1
        try:
            process_order(order, fault_rate)
            aggregator.add(order["product"], float(order["price"]))
            LOG.info(
                "OK   orderId=%-6s product=%-7s price=%8.2f | n=%-4d avg=%.2f | %s avg=%.2f",
                order["orderId"], order["product"], order["price"],
                aggregator.overall.count, aggregator.overall.average,
                order["product"], aggregator.by_product[order["product"]].average,
            )
            return

        except PermanentError as exc:
            dlq.send(msg, reason="validation", error=str(exc), attempts=attempts)
            return

        except TransientError as exc:
            if attempts >= total_attempts:
                dlq.send(msg, reason="retries-exhausted", error=str(exc), attempts=attempts)
                return
            delay = backoff_delay(attempts)
            LOG.warning(
                "RETRY orderId=%s attempt %d/%d failed (%s) - backing off %.2fs",
                order.get("orderId"), attempts, total_attempts, exc, delay,
            )
            time.sleep(delay)


def run(args: argparse.Namespace) -> int:
    registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(
        schema_registry_client=registry,
        schema_str=config.load_schema(),
        from_dict=lambda payload, ctx: payload,
    )

    consumer = Consumer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "group.id": config.CONSUMER_GROUP,
            "auto.offset.reset": config.AUTO_OFFSET_RESET,
            # Manual commits are what make at-least-once semantics possible.
            "enable.auto.commit": False,
            # Leave headroom for the worst-case retry backoff chain.
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 45000,
        }
    )
    dlq_producer = Producer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "client.id": "order-dlq-producer",
            "acks": "all",
            "enable.idempotence": True,
        }
    )

    aggregator = Aggregator()
    dlq = DeadLetterQueue(dlq_producer, config.DLQ_TOPIC)
    ctx = SerializationContext(config.ORDERS_TOPIC, MessageField.VALUE)

    consumer.subscribe([config.ORDERS_TOPIC])
    LOG.info("consuming '%s' as group '%s' (%s)", config.ORDERS_TOPIC, config.CONSUMER_GROUP, config.describe())
    LOG.info("retry policy: %d retries, base %.1fs, cap %.1fs | fault-rate %.2f",
             config.MAX_RETRIES, config.RETRY_BASE_DELAY, config.RETRY_MAX_DELAY, args.fault_rate)

    try:
        while _running:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            handle_message(msg, deserializer, ctx, aggregator, dlq, args.fault_rate)

            # Terminal state reached (processed or DLQ'd) -> safe to commit.
            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        LOG.warning("interrupted")
    finally:
        dlq_producer.flush(10)
        consumer.close()
        print(aggregator.summary())
        print(f"  messages sent to DLQ : {dlq.count}")
        if args.stats_file:
            payload = {
                "processed": aggregator.overall.count,
                "running_average": round(aggregator.overall.average, 4),
                "dead_lettered": dlq.count,
                "by_product": {
                    p: {"count": s.count, "average": round(s.average, 4)}
                    for p, s in aggregator.by_product.items()
                },
            }
            with open(args.stats_file, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, indent=2)
            LOG.info("stats written to %s", args.stats_file)

    return 0


def _stop(signum, frame) -> None:  # noqa: ARG001
    global _running
    _running = False


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume Avro orders with retry + DLQ.")
    parser.add_argument(
        "--fault-rate",
        type=float,
        default=0.0,
        help="probability (0..1) of a simulated transient downstream failure",
    )
    parser.add_argument("--stats-file", help="write final aggregation stats to this JSON file")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
