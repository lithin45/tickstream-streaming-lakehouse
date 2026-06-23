"""Unit tests for the live-producer reconnect loop (no broker, no socket).

Drives ``run_producer`` with a fake ExchangeClient + fake Producer so the resilience logic
— clean-end vs error-end reconnect, backoff application, max_reconnects ceiling, max_messages
stop, and per-message normalize-error isolation — is actually exercised. These guard the two
high-severity reconnect bugs the live producer is otherwise one bad frame / graceful close away
from.
"""

from __future__ import annotations

import asyncio

import pytest

from tickstream.config import get_settings
from tickstream.producer import service

# --- fakes ---


class FakeClient:
    name = "coinbase"

    def __init__(self, payloads, raise_exc=None):
        self._payloads = payloads
        self._raise = raise_exc

    def describe(self) -> str:
        return "fake"

    async def stream(self):
        for item in self._payloads:
            yield item
        if self._raise is not None:
            raise self._raise


class FakeProducer:
    def __init__(self):
        self.produced = []
        self.flushes = 0

    def produce(self, *, topic, value, key=None, on_delivery=None):
        self.produced.append((topic, key, value))

    def poll(self, _timeout):
        pass

    def flush(self, timeout=None):
        self.flushes += 1
        return 0


def _trade_payload(trade_id: str, *, product_id: str | None = "BTC-USD"):
    trade = {
        "trade_id": trade_id,
        "price": "100.0",
        "size": "1.0",
        "time": "2026-01-01T00:00:00.000000Z",
        "side": "BUY",
    }
    if product_id is not None:
        trade["product_id"] = product_id  # omit -> normalize raises KeyError
    return (
        "market_trades",
        {
            "channel": "market_trades",
            "timestamp": "2026-01-01T00:00:00.000000Z",
            "events": [{"type": "update", "trades": [trade]}],
        },
    )


@pytest.fixture
def patched(monkeypatch):
    """Patch out the broker/network seams; return (producer, sleeps, set_clients)."""
    producer = FakeProducer()
    sleeps: list[float] = []

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(service, "build_producer", lambda *a, **k: producer)
    monkeypatch.setattr(service, "ensure_topics", lambda *a, **k: None)
    monkeypatch.setattr(service.asyncio, "sleep", fake_sleep)

    def set_clients(clients):
        it = iter(clients)
        monkeypatch.setattr(service, "build_client", lambda *a, **k: next(it))

    return producer, sleeps, set_clients


def run_producer(**kw):
    return service.run_producer(get_settings(), **kw)


def test_max_messages_stops_and_flushes(patched) -> None:
    producer, _sleeps, set_clients = patched
    set_clients([FakeClient([_trade_payload("1"), _trade_payload("2"), _trade_payload("3")])])

    published = asyncio.run(run_producer(max_messages=2))
    assert published == 2
    assert len(producer.produced) == 2
    assert producer.flushes >= 1


def test_malformed_message_is_quarantined_not_fatal(patched) -> None:
    producer, _sleeps, set_clients = patched
    # First message is missing product_id -> quarantined per-record (not published, not fatal).
    set_clients([FakeClient([_trade_payload("bad", product_id=None), _trade_payload("ok")])])

    published = asyncio.run(run_producer(max_messages=1))
    assert published == 1
    raw = [p for p in producer.produced if "raw" in p[0]]
    quarantine = [p for p in producer.produced if "quarantine" in p[0]]
    assert len(raw) == 1  # only the valid trade reached a raw topic
    assert len(quarantine) == 1  # the malformed one was quarantined


def test_clean_end_applies_backoff_and_respects_max_reconnects(patched) -> None:
    _producer, sleeps, set_clients = patched
    # Every connection ends cleanly with zero messages (graceful-close storm).
    set_clients([FakeClient([]) for _ in range(10)])

    published = asyncio.run(run_producer(max_reconnects=2))
    assert published == 0
    # The bug was: clean end re-looped with NO sleep. Now each clean end backs off.
    assert len(sleeps) == 2  # reconnects 1 and 2 sleep; the 3rd exceeds the ceiling
    assert all(0.0 <= d <= 30.0 for d in sleeps)


def test_error_end_flushes_and_reconnects(patched) -> None:
    producer, sleeps, set_clients = patched
    set_clients(
        [
            FakeClient([_trade_payload("1")], raise_exc=OSError("boom")),
            FakeClient([], raise_exc=OSError("boom")),
        ]
    )

    published = asyncio.run(run_producer(max_reconnects=1))
    assert published == 1
    assert producer.flushes >= 1  # flushed on disconnect
    assert len(sleeps) >= 1  # backoff applied before reconnect
