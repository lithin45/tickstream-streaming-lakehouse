"""Unit tests for the normalized MarketEvent schema (no broker required)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from tickstream.producer.demo import make_demo_events
from tickstream.schema import EventType, MarketEvent, Side

_T = datetime(2026, 1, 1, tzinfo=UTC)


def test_roundtrip_json_bytes_is_lossless() -> None:
    event = MarketEvent(
        exchange="demo",
        symbol="BTC-USD",
        event_type=EventType.TRADE,
        price=42000.5,
        size=0.1,
        side=Side.BUY,
        trade_id="t-1",
        ts_event=_T,
        ts_ingest=_T,
    )
    restored = MarketEvent.from_json_bytes(event.to_json_bytes())
    assert restored == event


def test_demo_events_all_serializable() -> None:
    for event in make_demo_events():
        assert MarketEvent.from_json_bytes(event.to_json_bytes()) == event


def test_price_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        MarketEvent(
            exchange="demo",
            symbol="BTC-USD",
            event_type=EventType.TRADE,
            price=-1.0,
            size=0.1,
            ts_event=_T,
            ts_ingest=_T,
        )


def test_size_must_be_non_negative() -> None:
    with pytest.raises(ValidationError):
        MarketEvent(
            exchange="demo",
            symbol="BTC-USD",
            event_type=EventType.TRADE,
            price=1.0,
            size=-0.1,
            ts_event=_T,
            ts_ingest=_T,
        )


def test_crossed_book_rejected() -> None:
    with pytest.raises(ValidationError):
        MarketEvent(
            exchange="demo",
            symbol="BTC-USD",
            event_type=EventType.TICKER,
            best_bid=100.0,
            best_ask=99.0,
            ts_event=_T,
            ts_ingest=_T,
        )


def test_spread_computed() -> None:
    event = MarketEvent(
        exchange="demo",
        symbol="BTC-USD",
        event_type=EventType.TICKER,
        best_bid=100.0,
        best_ask=101.0,
        ts_event=_T,
        ts_ingest=_T,
    )
    assert event.spread == pytest.approx(1.0)


def test_key_is_symbol_bytes() -> None:
    event = make_demo_events()[0]
    assert event.key() == b"BTC-USD"
