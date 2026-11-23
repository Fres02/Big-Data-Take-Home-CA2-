"""Central configuration.

Every value can be overridden with an environment variable, so the same code
runs against a local Docker Compose cluster or a remote broker without edits.
"""

from __future__ import annotations

import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_PATH = Path(os.getenv("SCHEMA_PATH", REPO_ROOT / "schemas" / "order.avsc"))

# --- Cluster endpoints -------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# --- Topics ------------------------------------------------------------------
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "orders.DLQ")

# --- Consumer ----------------------------------------------------------------
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "order-processor")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "earliest")

# --- Retry policy ------------------------------------------------------------
# Total attempts = 1 initial try + MAX_RETRIES retries.
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_BASE_DELAY = float(os.getenv("RETRY_BASE_DELAY", "0.5"))  # seconds
RETRY_MAX_DELAY = float(os.getenv("RETRY_MAX_DELAY", "8.0"))    # seconds
RETRY_JITTER = float(os.getenv("RETRY_JITTER", "0.3"))          # 0..1 fraction


def load_schema() -> str:
    """Return the raw Avro schema text used by both serializer and deserializer."""
    return SCHEMA_PATH.read_text(encoding="utf-8")


def describe() -> str:
    return (
        f"bootstrap={BOOTSTRAP_SERVERS} registry={SCHEMA_REGISTRY_URL} "
        f"topic={ORDERS_TOPIC} dlq={DLQ_TOPIC}"
    )
