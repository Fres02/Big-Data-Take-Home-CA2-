#!/usr/bin/env python3
"""Order producer.

Publishes Avro-serialised `Order` records to the `orders` topic. The schema is
registered with Confluent Schema Registry on first use, so every message on the
wire carries a 5-byte wire-format header (magic byte + schema id) instead of the
full schema.

Two fault-injection switches exist purely so the consumer's error handling can
be demonstrated live:

  --bad-rate   fraction of messages that are structurally valid Avro but fail
               business validation (empty product name, non-positive price).
               These are *permanent* failures: the consumer sends them straight
               to the DLQ without retrying.

  --poison     append N messages that are not Avro at all. These fail at
               deserialisation time and prove the consumer can survive a poison
               pill instead of crash-looping on it.

Examples
--------
    python src/producer.py --count 50 --interval 0.3
    python src/producer.py --count 30 --bad-rate 0.2 --poison 2
"""

from __future__ import annotations

import argparse
import logging
import random
import sys
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

import config

LOG = logging.getLogger("producer")

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def build_serializers() -> tuple[AvroSerializer, StringSerializer]:
    registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(
        schema_registry_client=registry,
        schema_str=config.load_schema(),
        to_dict=lambda order, ctx: order,
        conf={"auto.register.schemas": True},
    )
    return avro_serializer, StringSerializer("utf_8")


def make_order(order_id: int, bad_rate: float) -> dict:
    """Return one order dict. With probability `bad_rate`, return an invalid one."""
    if random.random() < bad_rate:
        # Valid against the Avro schema, invalid against business rules.
        if random.random() < 0.5:
            return {
                "orderId": str(order_id),
                "product": "",              # missing product name
                "price": round(random.uniform(5, 500), 2),
            }
        return {
            "orderId": str(order_id),
            "product": random.choice(PRODUCTS),
            "price": -abs(round(random.uniform(1, 99), 2)),  # negative price
        }

    return {
        "orderId": str(order_id),
        "product": random.choice(PRODUCTS),
        "price": round(random.uniform(5, 500), 2),
    }


def delivery_report(err, msg) -> None:
    if err is not None:
        LOG.error("delivery failed: %s", err)
        return
    LOG.info(
        "delivered key=%s -> %s[%d]@%d",
        msg.key().decode("utf-8") if msg.key() else None,
        msg.topic(),
        msg.partition(),
        msg.offset(),
    )


def run(args: argparse.Namespace) -> int:
    avro_serializer, key_serializer = build_serializers()
    producer = Producer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "client.id": "order-producer",
            # Durability: wait for all in-sync replicas, retry internally, and
            # keep ordering intact even while retries are in flight.
            "acks": "all",
            "enable.idempotence": True,
            "retries": 5,
            "linger.ms": 10,
        }
    )

    ctx = SerializationContext(config.ORDERS_TOPIC, MessageField.VALUE)
    order_id = args.start_id
    sent = 0

    LOG.info("producing %d orders to '%s' (%s)", args.count, config.ORDERS_TOPIC, config.describe())

    try:
        for _ in range(args.count):
            order = make_order(order_id, args.bad_rate)
            producer.produce(
                topic=config.ORDERS_TOPIC,
                key=key_serializer(order["orderId"]),
                value=avro_serializer(order, ctx),
                on_delivery=delivery_report,
            )
            LOG.info("queued  %s", order)
            producer.poll(0)
            sent += 1
            order_id += 1
            if args.interval:
                time.sleep(args.interval)

        for i in range(args.poison):
            # Raw bytes, no Avro wire header -> consumer deserialisation fails.
            producer.produce(
                topic=config.ORDERS_TOPIC,
                key=key_serializer(f"poison-{i + 1}"),
                value=b"this is not avro",
                on_delivery=delivery_report,
            )
            LOG.warning("queued  POISON PILL #%d (non-Avro payload)", i + 1)
            producer.poll(0)
            sent += 1

    except KeyboardInterrupt:
        LOG.warning("interrupted, flushing what is already queued")
    finally:
        remaining = producer.flush(15)
        if remaining:
            LOG.error("%d message(s) still undelivered after flush", remaining)
            return 1

    LOG.info("done: %d message(s) delivered", sent)
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Produce Avro order messages to Kafka.")
    parser.add_argument("--count", type=int, default=20, help="number of orders to send")
    parser.add_argument("--start-id", type=int, default=1001, help="first orderId")
    parser.add_argument("--interval", type=float, default=0.5, help="seconds between messages")
    parser.add_argument(
        "--bad-rate",
        type=float,
        default=0.0,
        help="fraction (0..1) of orders that violate business rules",
    )
    parser.add_argument(
        "--poison",
        type=int,
        default=0,
        help="number of non-Avro poison-pill messages to append",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
