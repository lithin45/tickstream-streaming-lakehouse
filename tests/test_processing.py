"""Phase 3 acceptance: the Quix Streams processor's windows match the reference exactly.

Requires a broker (`make up`) + the processing extra. Each run uses ISOLATED unique topics so
accumulated replay runs can't perturb the counts. The streamed, closed windows are compared
field-for-field against the pure :mod:`tickstream.processing.metrics` reference.
"""

from __future__ import annotations

import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta

import pytest

from tests._helpers import drain_topic, isolated_settings
from tickstream.config import Settings
from tickstream.processing.app import DEFAULT_GRACE_SECONDS, run_processor
from tickstream.processing.metrics import (
    WINDOW_SIZES,
    closed_windows,
    compute_windows,
    max_event_ts_by_symbol,
)
from tickstream.producer.publisher import publish_events
from tickstream.producer.replay import replay
from tickstream.schema import EventType, MarketEvent, Side

pytestmark = [pytest.mark.integration, pytest.mark.slow]

# A minute/5-min-aligned base so synthetic event offsets land on clean window boundaries.
_BASE = datetime(2026, 3, 1, 0, 0, 0, tzinfo=UTC)


def _trade(symbol: str, price: float, size: float, offset_s: float) -> MarketEvent:
    ts = _BASE + timedelta(seconds=offset_s)
    return MarketEvent(
        exchange="syn",
        symbol=symbol,
        event_type=EventType.TRADE,
        price=price,
        size=size,
        side=Side.BUY,
        ts_event=ts,
        ts_ingest=ts,
    )


def test_streamed_windows_match_reference(broker: Settings, fixture_events, tmp_path) -> None:
    settings = isolated_settings(broker)

    # 1) replay the fixture into isolated raw topics
    replay(settings)

    # 2) run the Quix Streams processor over them, bounded
    run_processor(
        settings,
        consumer_group=f"proc-{uuid.uuid4().hex[:8]}",
        state_dir=str(tmp_path / "state"),
        bounded_timeout=20.0,
    )

    # 3) collect emitted window records
    recs = drain_topic(settings, settings.topics.metrics_windowed)
    assert recs, "processor emitted no windows"

    # Each closed window must be emitted exactly once (guards .current()/double-emit regressions
    # and at-least-once duplicates that a dict-collapse would otherwise hide).
    dupes = [
        k
        for k, n in Counter(
            (r["symbol"], r["window_size"], r["window_start"], r["source"]) for r in recs
        ).items()
        if n > 1
    ]
    assert not dupes, f"duplicate window records emitted: {dupes}"

    trades = [e for e in fixture_events if e.event_type == EventType.TRADE.value]
    tickers = [e for e in fixture_events if e.event_type == EventType.TICKER.value]
    assert trades and tickers, "fixture must contain both trades and tickers"
    trade_wm = max_event_ts_by_symbol(trades)
    ticker_wm = max_event_ts_by_symbol(tickers)

    emitted_trade_windows = 0
    emitted_ticker_windows = 0
    for label, secs in WINDOW_SIZES.items():
        # --- trade windows: VWAP / volume / count ---
        ref = closed_windows(
            compute_windows(trades, secs, label), trade_wm, grace_seconds=DEFAULT_GRACE_SECONDS
        )
        emitted = {
            (r["symbol"], r["window_start"]): r
            for r in recs
            if r["window_size"] == label and r["source"] == "trades"
        }
        assert set(emitted) == {(w.symbol, w.window_start.isoformat()) for w in ref}
        for w in ref:
            r = emitted[(w.symbol, w.window_start.isoformat())]
            assert r["trade_count"] == w.trade_count
            assert r["trade_volume"] == pytest.approx(w.trade_volume, rel=1e-9)
            assert r["vwap"] == pytest.approx(w.vwap, rel=1e-9)
        emitted_trade_windows += len(emitted)

        # --- ticker windows: avg spread / mid ---
        ref_k = closed_windows(
            compute_windows(tickers, secs, label), ticker_wm, grace_seconds=DEFAULT_GRACE_SECONDS
        )
        emitted_k = {
            (r["symbol"], r["window_start"]): r
            for r in recs
            if r["window_size"] == label and r["source"] == "ticker"
        }
        assert set(emitted_k) == {(w.symbol, w.window_start.isoformat()) for w in ref_k}
        for w in ref_k:
            r = emitted_k[(w.symbol, w.window_start.isoformat())]
            assert r["ticker_count"] == w.ticker_count
            assert r["avg_spread"] == pytest.approx(w.avg_spread, rel=1e-9)
            assert r["avg_mid"] == pytest.approx(w.avg_mid, rel=1e-9)
        emitted_ticker_windows += len(emitted_k)

    # Sanity: the pipeline actually produced closed trade AND ticker windows (no vacuous pass).
    assert emitted_trade_windows > 0
    assert emitted_ticker_windows > 0


def _run_synthetic(
    broker: Settings, events: list[MarketEvent], tmp_path, *, grace: int
) -> list[dict]:
    """Publish synthetic trades to an isolated topic, run the processor, return emitted windows."""
    settings = isolated_settings(broker)
    publish_events(settings, events, topic=settings.topics.trades_raw)
    run_processor(
        settings,
        consumer_group=f"syn-{uuid.uuid4().hex[:8]}",
        state_dir=str(tmp_path / "state"),
        bounded_timeout=10.0,
        grace_seconds=grace,
    )
    return drain_topic(settings, settings.topics.metrics_windowed)


def test_late_record_past_grace_is_dropped(broker: Settings, tmp_path) -> None:
    """A trade landing in an already-expired window is dropped, not folded into it."""
    events = [
        _trade("BTC-USD", 100.0, 1.0, 10),  # window [0,60)
        _trade("BTC-USD", 100.0, 1.0, 20),  # window [0,60)
        _trade("BTC-USD", 200.0, 1.0, 70),  # window [60,120) -> advances watermark
        _trade("BTC-USD", 200.0, 1.0, 80),  # watermark now 80; [0,60) expired (80-2-60=18>=0)
        _trade("BTC-USD", 999.0, 5.0, 30),  # LATE: falls in expired [0,60) -> must be dropped
    ]
    recs = _run_synthetic(broker, events, tmp_path, grace=2)
    first = [
        r for r in recs if r["window_size"] == "1m" and r["window_start"].endswith("00:00:00+00:00")
    ]
    assert len(first) == 1
    # Only the two in-window trades counted; the late t=30 straggler did NOT inflate it.
    assert first[0]["trade_count"] == 2
    assert first[0]["vwap"] == pytest.approx(100.0)


def test_per_symbol_watermark_closing(broker: Settings, tmp_path) -> None:
    """Windows close on the PER-SYMBOL watermark: a quiet symbol's window stays open."""
    events = [
        # SOL only trades early, then goes quiet -> its [0,60) window never closes (per-key).
        _trade("SOL-USD", 50.0, 1.0, 10),
        _trade("SOL-USD", 50.0, 1.0, 20),
        # BTC keeps advancing across many windows (global time goes way past SOL's window end).
        _trade("BTC-USD", 100.0, 1.0, 10),
        _trade("BTC-USD", 100.0, 1.0, 70),
        _trade("BTC-USD", 100.0, 1.0, 130),
        _trade("BTC-USD", 100.0, 1.0, 190),
    ]
    recs = _run_synthetic(broker, events, tmp_path, grace=2)
    sol = [r for r in recs if r["symbol"] == "SOL-USD"]
    btc = [r for r in recs if r["symbol"] == "BTC-USD"]
    # Under per-symbol (key) closing SOL emits nothing; a global watermark would have closed it.
    assert sol == []
    assert btc, "BTC's closed windows should still emit"
