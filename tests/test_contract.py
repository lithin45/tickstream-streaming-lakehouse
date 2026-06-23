"""Unit tests for the data contract + quarantine split (no broker)."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd

from tickstream.producer.demo import make_demo_events
from tickstream.quality.contract import apply_contract, count_out_of_order, events_to_frame
from tickstream.quality.sla import SLAReport

_T = datetime(2026, 1, 1, tzinfo=UTC)
_FLOATS = ["price", "size", "best_bid", "best_ask", "best_bid_size", "best_ask_size"]


def _frame(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for col in _FLOATS:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    for col in ("ts_event", "ts_ingest"):
        df[col] = pd.to_datetime(df[col], utc=True)
    return df


def _row(**over) -> dict:
    base = {
        "exchange": "x",
        "symbol": "BTC-USD",
        "event_type": "trade",
        "price": 100.0,
        "size": 1.0,
        "best_bid": None,
        "best_ask": None,
        "best_bid_size": None,
        "best_ask_size": None,
        "ts_event": _T,
        "ts_ingest": _T,
    }
    base.update(over)
    return base


def test_contract_passes_clean_events() -> None:
    result = apply_contract(events_to_frame(make_demo_events()))
    assert result.ok
    assert result.n_quarantined == 0
    assert result.n_valid == len(make_demo_events())
    assert result.reasons == []  # no swallowed (unindexed) dtype failures


def test_contract_accepts_boundary_valid_rows() -> None:
    df = _frame(
        [
            _row(size=0.0),  # size == 0 is allowed (ge 0)
            _row(
                event_type="ticker", price=None, size=None, best_bid=100.0, best_ask=100.0
            ),  # spread 0
            _row(event_type="ticker", price=None, size=None),  # ticker with null price/size
        ]
    )
    result = apply_contract(df)
    assert result.n_quarantined == 0
    assert result.n_valid == 3


def test_contract_each_rule_fires() -> None:
    df = _frame(
        [
            _row(size=-1.0),  # size >= 0
            _row(best_bid=-1.0),  # bid >= 0
            _row(ts_event=None),  # non-null key
            _row(exchange=None),  # non-null
            _row(event_type="heartbeat"),  # event_type membership
            _row(event_type="trade", price=None, size=None),  # trade must have price+size
        ]
    )
    result = apply_contract(df)
    assert result.n_quarantined == 6
    joined = " | ".join(result.quarantined["quarantine_reason"].tolist())
    assert "greater_than_or_equal_to(0)" in joined  # size / bid
    assert "not_nullable" in joined  # ts_event / exchange
    assert "isin" in joined  # event_type membership
    assert "trade_has_price_and_size" in joined


def test_contract_quarantines_violations() -> None:
    df = _frame(
        [
            _row(),  # valid
            _row(price=-5.0),  # price <= 0
            _row(symbol=None),  # null key
            _row(symbol="DOGE-USD"),  # unknown symbol
            _row(
                event_type="ticker", price=None, size=None, best_bid=101.0, best_ask=100.0
            ),  # crossed
        ]
    )
    result = apply_contract(df)
    assert result.n_valid == 1
    assert result.n_quarantined == 4
    assert "quarantine_reason" in result.quarantined.columns
    # Every quarantined row carries a non-empty reason.
    assert all(result.quarantined["quarantine_reason"].str.len() > 0)


def _report(**over) -> SLAReport:
    base = {
        "latency_p95_s": 10.0,
        "windows_produced": 27,
        "windows_expected": 27,
        "completeness_pct": 100.0,
        "completeness_by_symbol": {"BTC-USD": 100.0},
        "gold_violations": 0,
    }
    base.update(over)
    return SLAReport(**base)


def test_sla_report_gate_trips_on_each_breach() -> None:
    assert _report().passed
    assert not _report(latency_p95_s=61.0).passed  # latency breach
    assert not _report(completeness_pct=98.0).passed  # completeness breach
    assert not _report(gold_violations=1).passed  # violation breach


def test_count_out_of_order() -> None:
    df = _frame(
        [
            _row(symbol="BTC-USD", ts_event=_T),
            _row(symbol="BTC-USD", ts_event=datetime(2026, 1, 1, 0, 0, 5, tzinfo=UTC)),
            _row(symbol="BTC-USD", ts_event=datetime(2026, 1, 1, 0, 0, 3, tzinfo=UTC)),  # late
            _row(symbol="ETH-USD", ts_event=_T),  # different symbol, in order
        ]
    )
    assert count_out_of_order(df) == 1
