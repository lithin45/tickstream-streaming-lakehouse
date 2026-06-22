"""Unit tests for raw-message normalization (pure, no socket)."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tickstream.config import get_settings
from tickstream.producer.normalize import (
    build_symbol_map,
    normalize_binance_us,
    normalize_coinbase,
    parse_ts,
)
from tickstream.schema import EventType, Side

_TS_INGEST = datetime(2026, 1, 1, tzinfo=UTC)
_BINANCE_MAP = {"BTCUSD": "BTC-USD", "ETHUSD": "ETH-USD", "SOLUSD": "SOL-USD"}


# --- timestamp parsing ---
def test_parse_ts_truncates_nanoseconds() -> None:
    # 9 fractional digits (Coinbase) would overflow datetime; must truncate to micros.
    dt = parse_ts("2026-06-22T22:14:35.153209871Z")
    assert dt.tzinfo is UTC
    assert dt.microsecond == 153209


def test_parse_ts_handles_short_fractions_and_z() -> None:
    dt = parse_ts("2026-01-01T00:00:00.5Z")
    assert dt.microsecond == 500000
    assert dt.tzinfo is UTC


# --- Coinbase ---
def test_normalize_coinbase_market_trades() -> None:
    payload = {
        "channel": "market_trades",
        "timestamp": "2026-01-01T00:00:00.123456789Z",
        "sequence_num": 1,
        "events": [
            {
                "type": "update",
                "trades": [
                    {
                        "product_id": "BTC-USD",
                        "trade_id": "111",
                        "price": "42000.50",
                        "size": "0.01",
                        "time": "2026-01-01T00:00:00.500000Z",
                        "side": "BUY",
                    },
                    {
                        "product_id": "ETH-USD",
                        "trade_id": "112",
                        "price": "2200.00",
                        "size": "1.5",
                        "time": "2026-01-01T00:00:01.000000Z",
                        "side": "SELL",
                    },
                ],
            }
        ],
    }
    events = normalize_coinbase(payload, ts_ingest=_TS_INGEST)
    assert len(events) == 2
    btc, eth = events
    assert (btc.symbol, btc.event_type, btc.side) == ("BTC-USD", EventType.TRADE, Side.BUY)
    assert btc.price == 42000.5 and btc.size == 0.01 and btc.trade_id == "111"
    assert btc.ts_event == datetime(2026, 1, 1, 0, 0, 0, 500000, tzinfo=UTC)
    assert btc.ts_ingest == _TS_INGEST
    assert eth.side == Side.SELL and eth.symbol == "ETH-USD"


def test_normalize_coinbase_ticker_has_book_and_spread() -> None:
    payload = {
        "channel": "ticker",
        "timestamp": "2026-01-01T00:00:02.123456789Z",
        "sequence_num": 2,
        "events": [
            {
                "type": "update",
                "tickers": [
                    {
                        "type": "ticker",
                        "product_id": "BTC-USD",
                        "price": "42000.55",
                        "best_bid": "42000.50",
                        "best_ask": "42000.60",
                        "best_bid_quantity": "0.5",
                        "best_ask_quantity": "0.7",
                    }
                ],
            }
        ],
    }
    (event,) = normalize_coinbase(payload, ts_ingest=_TS_INGEST)
    assert event.event_type == EventType.TICKER
    assert event.best_bid == 42000.5 and event.best_ask == 42000.6
    assert event.best_bid_size == 0.5 and event.best_ask_size == 0.7
    assert event.spread == pytest.approx(0.1)
    # event time comes from the envelope (nanoseconds truncated)
    assert event.ts_event.microsecond == 123456


# --- Coinbase snapshot handling ---
def test_coinbase_market_trades_snapshot_is_skipped() -> None:
    payload = {
        "channel": "market_trades",
        "timestamp": "2026-01-01T00:00:00.000000Z",
        "events": [
            {
                "type": "snapshot",  # backfill -> must be dropped
                "trades": [
                    {
                        "product_id": "BTC-USD",
                        "trade_id": "old",
                        "price": "1.0",
                        "size": "1.0",
                        "time": "2026-01-01T00:00:00.000000Z",
                        "side": "BUY",
                    }
                ],
            },
            {
                "type": "update",  # live -> kept
                "trades": [
                    {
                        "product_id": "BTC-USD",
                        "trade_id": "new",
                        "price": "2.0",
                        "size": "1.0",
                        "time": "2026-01-01T00:00:01.000000Z",
                        "side": "SELL",
                    }
                ],
            },
        ],
    }
    events = normalize_coinbase(payload, ts_ingest=_TS_INGEST)
    assert [e.trade_id for e in events] == ["new"]


# --- Binance.US ---
@pytest.mark.parametrize(
    ("maker", "expected_side"),
    [(True, Side.SELL), (False, Side.BUY)],  # m=True -> buyer maker -> aggressor sells
)
def test_normalize_binance_trade_side_from_maker_flag(maker: bool, expected_side: Side) -> None:
    payload = {
        "stream": "btcusd@trade",
        "data": {
            "e": "trade",
            "E": 1735689600000,
            "s": "BTCUSD",
            "t": 555,
            "p": "42000.50",
            "q": "0.02",
            "T": 1735689600000,
            "m": maker,
        },
    }
    (event,) = normalize_binance_us(payload, ts_ingest=_TS_INGEST, symbol_map=_BINANCE_MAP)
    assert event.symbol == "BTC-USD"
    assert event.side == expected_side
    assert event.price == 42000.5 and event.size == 0.02 and event.trade_id == "555"
    assert event.ts_event == datetime(2025, 1, 1, tzinfo=UTC)


def test_normalize_binance_unmapped_symbol_is_dropped() -> None:
    payload = {
        "stream": "dogeusd@trade",
        "data": {"e": "trade", "s": "DOGEUSD", "t": 1, "p": "0.1", "q": "1", "T": 1735689600000},
    }
    # DOGEUSD is not in the configured symbol map -> dropped (no raw, non-canonical symbol).
    assert normalize_binance_us(payload, ts_ingest=_TS_INGEST, symbol_map=_BINANCE_MAP) == []


def test_build_symbol_map_direction() -> None:
    settings = get_settings()
    # Binance: native concat -> canonical dash.
    assert build_symbol_map(settings, "binance_us") == {
        "BTCUSD": "BTC-USD",
        "ETHUSD": "ETH-USD",
        "SOLUSD": "SOL-USD",
    }
    # Coinbase: already canonical (identity).
    assert build_symbol_map(settings, "coinbase") == {
        "BTC-USD": "BTC-USD",
        "ETH-USD": "ETH-USD",
        "SOL-USD": "SOL-USD",
    }


def test_normalize_binance_book_ticker() -> None:
    payload = {
        "stream": "ethusd@bookTicker",
        "data": {"u": 400, "s": "ETHUSD", "b": "2200.00", "B": "1.2", "a": "2200.50", "A": "0.8"},
    }
    (event,) = normalize_binance_us(payload, ts_ingest=_TS_INGEST, symbol_map=_BINANCE_MAP)
    assert event.event_type == EventType.TICKER
    assert event.symbol == "ETH-USD"
    assert event.best_bid == 2200.0 and event.best_ask == 2200.5
    assert event.best_bid_size == 1.2 and event.best_ask_size == 0.8
    assert event.spread == pytest.approx(0.5)
    # bookTicker carries no event time -> falls back to ingest time
    assert event.ts_event == _TS_INGEST
