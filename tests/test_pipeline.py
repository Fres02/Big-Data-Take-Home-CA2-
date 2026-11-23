"""Unit tests that run without a broker.

The Kafka client is only touched at the edges (poll / produce / commit), so the
interesting parts - aggregation, validation, the retry ladder and DLQ routing -
can be tested with a fake message and a fake DLQ.

    pip install pytest && pytest -q
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import fastavro
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import config  # noqa: E402
import consumer as c  # noqa: E402
import producer as p  # noqa: E402


class FakeMsg:
    def __init__(self, value=b"payload", key=b"1001"):
        self._v, self._k = value, key

    def value(self):
        return self._v

    def key(self):
        return self._k

    def topic(self):
        return "orders"

    def partition(self):
        return 0

    def offset(self):
        return 7


class FakeDLQ:
    def __init__(self):
        self.items = []
        self.count = 0

    def send(self, msg, reason, error, attempts):
        self.count += 1
        self.items.append({"reason": reason, "attempts": attempts, "error": error})


@pytest.fixture(autouse=True)
def fast_backoff(monkeypatch):
    """Keep the retry ladder instant during tests."""
    monkeypatch.setattr(config, "RETRY_BASE_DELAY", 0.001)
    monkeypatch.setattr(config, "RETRY_MAX_DELAY", 0.002)


# --------------------------------------------------------------------------- #
# Schema
# --------------------------------------------------------------------------- #
def test_schema_is_valid_and_round_trips():
    import io

    schema = json.loads(config.load_schema())
    parsed = fastavro.parse_schema(schema)
    order = {"orderId": "1001", "product": "Item1", "price": 42.5}

    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, order)
    buf.seek(0)
    assert fastavro.schemaless_reader(buf, parsed) == order


# --------------------------------------------------------------------------- #
# Aggregation
# --------------------------------------------------------------------------- #
def test_running_average_is_incremental():
    agg = c.Aggregator()
    for product, price in [("Item1", 10.0), ("Item1", 20.0), ("Item2", 5.0)]:
        agg.add(product, price)

    assert agg.overall.count == 3
    assert agg.overall.average == pytest.approx(35 / 3)
    assert agg.by_product["Item1"].average == 15.0
    assert agg.overall.minimum == 5.0
    assert agg.overall.maximum == 20.0


def test_empty_aggregator_does_not_divide_by_zero():
    assert c.Aggregator().overall.average == 0.0


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "order",
    [
        {"orderId": "1", "product": "", "price": 5.0},
        {"orderId": "2", "product": "   ", "price": 5.0},
        {"orderId": "3", "product": "Item1", "price": -3.0},
        {"orderId": "4", "product": "Item1", "price": 0.0},
    ],
)
def test_invalid_orders_are_permanent_failures(order):
    with pytest.raises(c.PermanentError):
        c.process_order(order, fault_rate=0.0)


def test_valid_order_passes():
    c.process_order({"orderId": "5", "product": "Item1", "price": 9.0}, fault_rate=0.0)


def test_fault_injection_raises_transient():
    with pytest.raises(c.TransientError):
        c.process_order({"orderId": "6", "product": "Item1", "price": 9.0}, fault_rate=1.0)


# --------------------------------------------------------------------------- #
# Backoff
# --------------------------------------------------------------------------- #
def test_backoff_grows_and_stays_capped(monkeypatch):
    monkeypatch.setattr(config, "RETRY_BASE_DELAY", 0.5)
    monkeypatch.setattr(config, "RETRY_MAX_DELAY", 8.0)
    ceiling = config.RETRY_MAX_DELAY * (1 + config.RETRY_JITTER)
    delays = [c.backoff_delay(i) for i in range(1, 9)]

    assert all(d <= ceiling for d in delays)
    assert delays[0] < delays[3]


# --------------------------------------------------------------------------- #
# End-to-end message outcomes
# --------------------------------------------------------------------------- #
def test_happy_path_updates_aggregate_and_skips_dlq():
    agg, dlq = c.Aggregator(), FakeDLQ()
    c.handle_message(
        FakeMsg(),
        lambda v, ctx: {"orderId": "1001", "product": "Item1", "price": 50.0},
        None, agg, dlq, 0.0,
    )
    assert agg.overall.count == 1
    assert dlq.count == 0


def test_validation_failure_goes_straight_to_dlq_without_retrying():
    agg, dlq = c.Aggregator(), FakeDLQ()
    c.handle_message(
        FakeMsg(),
        lambda v, ctx: {"orderId": "1002", "product": "", "price": 50.0},
        None, agg, dlq, 0.0,
    )
    assert agg.overall.count == 0
    assert dlq.items[0]["reason"] == "validation"
    assert dlq.items[0]["attempts"] == 1  # no wasted retries


def test_transient_failure_retries_then_dead_letters():
    agg, dlq = c.Aggregator(), FakeDLQ()
    c.handle_message(
        FakeMsg(),
        lambda v, ctx: {"orderId": "1003", "product": "Item2", "price": 10.0},
        None, agg, dlq, 1.0,  # always fails
    )
    assert dlq.items[0]["reason"] == "retries-exhausted"
    assert dlq.items[0]["attempts"] == config.MAX_RETRIES + 1


def test_transient_failure_that_recovers_is_not_dead_lettered(monkeypatch):
    agg, dlq = c.Aggregator(), FakeDLQ()
    calls = {"n": 0}

    def flaky(order, fault_rate):
        calls["n"] += 1
        if calls["n"] < 3:
            raise c.TransientError("still down")

    monkeypatch.setattr(c, "process_order", flaky)
    c.handle_message(
        FakeMsg(),
        lambda v, ctx: {"orderId": "1004", "product": "Item3", "price": 30.0},
        None, agg, dlq, 0.0,
    )
    assert calls["n"] == 3
    assert agg.overall.count == 1
    assert dlq.count == 0


def test_poison_pill_is_dead_lettered_not_retried():
    agg, dlq = c.Aggregator(), FakeDLQ()

    def boom(value, ctx):
        raise ValueError("unknown magic byte")

    c.handle_message(FakeMsg(b"this is not avro"), boom, None, agg, dlq, 0.0)
    assert dlq.items[0]["reason"] == "deserialization"
    assert agg.overall.count == 0


# --------------------------------------------------------------------------- #
# Producer
# --------------------------------------------------------------------------- #
def test_producer_generates_valid_orders_by_default():
    orders = [p.make_order(1001 + i, bad_rate=0.0) for i in range(100)]
    assert all(o["product"] and o["price"] > 0 for o in orders)


def test_producer_bad_rate_generates_rule_violations():
    orders = [p.make_order(1001 + i, bad_rate=1.0) for i in range(100)]
    assert all(o["product"] == "" or o["price"] < 0 for o in orders)
