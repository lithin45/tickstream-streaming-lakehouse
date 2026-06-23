"""Exact unit tests for the windowed-metrics reference (no broker)."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import pytest

from tickstream.processing.metrics import (
    closed_windows,
    compute_windows,
    max_event_ts_by_symbol,
    window_bounds,
)
from tickstream.schema import EventType, MarketEvent, Side

_T0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)


def _trade(symbol: str, price: float, size: float, offset_s: float) -> MarketEvent:
    ts = _T0 + timedelta(seconds=offset_s)
    return MarketEvent(
        exchange="t",
        symbol=symbol,
        event_type=EventType.TRADE,
        price=price,
        size=size,
        side=Side.BUY,
        ts_event=ts,
        ts_ingest=ts,
    )


def _ticker(symbol: str, bid: float, ask: float, offset_s: float) -> MarketEvent:
    ts = _T0 + timedelta(seconds=offset_s)
    return MarketEvent(
        exchange="t",
        symbol=symbol,
        event_type=EventType.TICKER,
        best_bid=bid,
        best_ask=ask,
        ts_event=ts,
        ts_ingest=ts,
    )


def test_window_bounds_are_epoch_aligned() -> None:
    start, end = window_bounds(_T0 + timedelta(seconds=75), 60)
    assert start == _T0 + timedelta(seconds=60)
    assert end == _T0 + timedelta(seconds=120)


def test_vwap_volume_count_single_window() -> None:
    events = [_trade("BTC-USD", 100.0, 1.0, 1), _trade("BTC-USD", 200.0, 3.0, 30)]
    (m,) = compute_windows(events, 60, "1m")
    assert m.symbol == "BTC-USD"
    assert m.trade_count == 2
    assert m.trade_volume == pytest.approx(4.0)
    # VWAP = (100*1 + 200*3) / (1+3) = 700/4
    assert m.vwap == pytest.approx(175.0)
    assert m.window_start == _T0 and m.window_end == _T0 + timedelta(seconds=60)


def test_window_boundary_splits_into_two() -> None:
    events = [_trade("BTC-USD", 100.0, 1.0, 30), _trade("BTC-USD", 200.0, 1.0, 90)]
    metrics = compute_windows(events, 60, "1m")
    assert len(metrics) == 2
    assert [m.window_start for m in metrics] == [_T0, _T0 + timedelta(seconds=60)]
    assert metrics[0].vwap == pytest.approx(100.0)
    assert metrics[1].vwap == pytest.approx(200.0)


def test_out_of_order_is_independent_of_arrival_order() -> None:
    events = [_trade("BTC-USD", 100.0 + i, 1.0, i * 7) for i in range(20)]
    ordered = compute_windows(events, 60, "1m")
    shuffled = list(events)
    random.Random(123).shuffle(shuffled)
    assert compute_windows(shuffled, 60, "1m") == ordered


def test_ticker_spread_and_mid() -> None:
    events = [_ticker("ETH-USD", 100.0, 102.0, 1), _ticker("ETH-USD", 200.0, 204.0, 2)]
    (m,) = compute_windows(events, 60, "1m")
    assert m.ticker_count == 2
    assert m.avg_spread == pytest.approx((2.0 + 4.0) / 2)
    assert m.avg_mid == pytest.approx((101.0 + 202.0) / 2)
    assert m.vwap is None and m.trade_count == 0


def test_trades_and_tickers_combine_in_one_window() -> None:
    events = [_trade("BTC-USD", 100.0, 2.0, 1), _ticker("BTC-USD", 99.0, 101.0, 2)]
    (m,) = compute_windows(events, 60, "1m")
    assert m.trade_count == 1 and m.ticker_count == 1
    assert m.vwap == pytest.approx(100.0)
    assert m.avg_spread == pytest.approx(2.0)
    assert m.event_count == 2


def test_closed_windows_grace_boundary() -> None:
    # Window [0, 60); a window closes once the watermark reaches end + grace (>=).
    metrics = compute_windows([_trade("BTC-USD", 100.0, 1.0, 10)], 60, "1m")
    assert len(metrics) == 1
    wm_at = {"BTC-USD": _T0 + timedelta(seconds=65)}  # exactly end(60)+grace(5)
    wm_below = {"BTC-USD": _T0 + timedelta(seconds=64.999)}
    assert len(closed_windows(metrics, wm_at, grace_seconds=5)) == 1
    assert closed_windows(metrics, wm_below, grace_seconds=5) == []


def test_closed_windows_excludes_trailing_open_window() -> None:
    # Three consecutive 1m windows for one symbol; the last is still open.
    events = [_trade("BTC-USD", 100.0, 1.0, s) for s in (10, 70, 130)]
    metrics = compute_windows(events, 60, "1m")
    assert len(metrics) == 3
    max_ts = max_event_ts_by_symbol(events)
    closed = closed_windows(metrics, max_ts)
    # Window containing the max ts (130s -> [120,180)) is open; first two are closed.
    assert [m.window_start for m in closed] == [_T0, _T0 + timedelta(seconds=60)]


def test_fixture_window_totals_match(fixture_events) -> None:
    metrics = compute_windows(fixture_events, 60, "1m")
    total_trades = sum(1 for e in fixture_events if e.event_type == EventType.TRADE.value)
    total_volume = sum(e.size for e in fixture_events if e.event_type == EventType.TRADE.value)
    assert sum(m.trade_count for m in metrics) == total_trades
    assert sum(m.trade_volume for m in metrics) == pytest.approx(total_volume)
    # All three symbols appear and windows are non-empty.
    assert {m.symbol for m in metrics} == {"BTC-USD", "ETH-USD", "SOL-USD"}
    assert len(metrics) > 3
