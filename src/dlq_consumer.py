#!/usr/bin/env python3
"""DLQ inspector.

Reads the dead letter topic from the beginning and prints each parked message
with its diagnostic headers. Use this during the live demo to show *why* each
message failed, not just that it did.

Payloads are forwarded to the DLQ as the original bytes, so this tool tries an
Avro decode first and falls back to a raw preview for poison pills that were
never valid Avro in the first place.

    python src/dlq_consumer.py
    python src/dlq_consumer.py --follow      # keep tailing instead of exiting
"""

from __future__ import annotations

import argparse
import logging
import sys

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

import config

LOG = logging.getLogger("dlq")


def decode_payload(raw: bytes | None, deserializer, ctx) -> str:
    if raw is None:
        return "<null>"
    try:
        return repr(deserializer(raw, ctx))
    except Exception:  # noqa: BLE001 - poison pills are expected here
        preview = raw[:80]
        return f"<undecodable, {len(raw)} bytes> {preview!r}"


def run(args: argparse.Namespace) -> int:
    registry = SchemaRegistryClient({"url": config.SCHEMA_REGISTRY_URL})
    deserializer = AvroDeserializer(
        schema_registry_client=registry,
        schema_str=config.load_schema(),
        from_dict=lambda payload, ctx: payload,
    )
    # The DLQ carries the original topic's payloads, so decode with that context.
    ctx = SerializationContext(config.ORDERS_TOPIC, MessageField.VALUE)

    consumer = Consumer(
        {
            "bootstrap.servers": config.BOOTSTRAP_SERVERS,
            "group.id": args.group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([config.DLQ_TOPIC])
    print(f"Reading '{config.DLQ_TOPIC}' from the beginning. Ctrl-C to stop.\n")

    seen = 0
    idle_polls = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                idle_polls += 1
                if not args.follow and idle_polls >= args.idle_polls:
                    break
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            idle_polls = 0
            seen += 1
            headers = {k: (v.decode("utf-8", "replace") if isinstance(v, bytes) else v)
                       for k, v in (msg.headers() or [])}

            print("-" * 72)
            print(f"#{seen}  {config.DLQ_TOPIC}[{msg.partition()}]@{msg.offset()}")
            print(f"  key       : {msg.key().decode('utf-8') if msg.key() else None}")
            print(f"  reason    : {headers.get('dlq.reason', '?')}")
            print(f"  error     : {headers.get('dlq.error', '?')}")
            print(f"  attempts  : {headers.get('dlq.attempts', '?')}")
            print(f"  failed at : {headers.get('dlq.failed.at', '?')}")
            print(
                "  origin    : "
                f"{headers.get('dlq.origin.topic', '?')}"
                f"[{headers.get('dlq.origin.partition', '?')}]"
                f"@{headers.get('dlq.origin.offset', '?')}"
            )
            print(f"  payload   : {decode_payload(msg.value(), deserializer, ctx)}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print("-" * 72)
    print(f"{seen} message(s) in the dead letter queue.")
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect the dead letter queue.")
    parser.add_argument("--group", default="dlq-inspector", help="consumer group id")
    parser.add_argument("--follow", action="store_true", help="keep tailing the DLQ")
    parser.add_argument(
        "--idle-polls",
        type=int,
        default=3,
        help="empty polls before exiting when not following",
    )
    parser.add_argument("--log-level", default="WARNING")
    return parser.parse_args(argv)


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=args.log_level.upper(), format="%(levelname)s | %(message)s")
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
