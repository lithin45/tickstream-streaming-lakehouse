"""Phase 4 acceptance: dbt builds the marts, gold matches the oracle, Iceberg time-travel works.

Integration test (needs a broker): replays the fixture into an isolated topic set + lake, runs
write_bronze, then build_marts (dbt build = run + 13 tests, then the gold Iceberg materialization).
Gold values are checked against the pure windowing oracle, and a prior snapshot is read back.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from tests._helpers import isolated_settings
from tickstream.config import Settings
from tickstream.lake.bronze import bronze_root, write_bronze
from tickstream.lake.iceberg import gold_snapshots, read_gold
from tickstream.lake.marts import build_marts
from tickstream.processing.metrics import WINDOW_SIZES, compute_windows
from tickstream.producer.replay import replay
from tickstream.schema import EventType

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_TS = datetime(2026, 1, 1, tzinfo=UTC)


def _tmp_lake_settings(broker: Settings, tmp_path) -> Settings:
    runtime = broker.runtime.model_copy(
        update={"lake_root": tmp_path / "lake", "warehouse_root": tmp_path / "wh"}
    )
    return isolated_settings(broker).model_copy(update={"runtime": runtime})


def test_marts_build_and_gold_matches_oracle(broker: Settings, fixture_events, tmp_path) -> None:
    settings = _tmp_lake_settings(broker, tmp_path)

    # replay -> bronze (into the isolated tmp lake), then dbt build + Iceberg materialization.
    replay(settings)
    write_bronze(settings, root=bronze_root(settings), group_id="marts-bronze", timeout=20.0)
    result = build_marts(settings)  # raises if dbt build / any of the 13 dbt tests fail
    assert result.gold_rows > 0

    gold = read_gold(settings).to_pylist()
    gmap = {
        (r["symbol"], r["window_size"], r["window_start"].astimezone(UTC).isoformat()): r
        for r in gold
    }

    trades = [e for e in fixture_events if e.event_type == EventType.TRADE.value]
    tickers = [e for e in fixture_events if e.event_type == EventType.TICKER.value]

    oracle_keys: set[tuple[str, str, str]] = set()
    checked = 0
    for label, secs in WINDOW_SIZES.items():
        for w in compute_windows(trades, secs, label):
            key = (w.symbol, label, w.window_start.isoformat())
            oracle_keys.add(key)
            g = gmap[key]
            assert g["trade_count"] == w.trade_count
            assert g["trade_volume"] == pytest.approx(w.trade_volume, rel=1e-9)
            assert g["vwap"] == pytest.approx(w.vwap, rel=1e-9)
            checked += 1
        for w in compute_windows(tickers, secs, label):
            key = (w.symbol, label, w.window_start.isoformat())
            oracle_keys.add(key)
            g = gmap[key]
            assert g["ticker_count"] == w.ticker_count
            assert g["avg_spread"] == pytest.approx(w.avg_spread, rel=1e-9)
    assert checked > 0
    # Bijection: gold has exactly the oracle's windows — no missing and no spurious rows.
    assert set(gmap.keys()) == oracle_keys


def test_iceberg_time_travel_returns_prior_snapshot(broker: Settings, tmp_path) -> None:
    settings = _tmp_lake_settings(broker, tmp_path)
    replay(settings)
    write_bronze(settings, root=bronze_root(settings), group_id="tt-bronze", timeout=20.0)
    result = build_marts(settings)

    snaps = gold_snapshots(settings)
    assert len(snaps) == 2  # first append = 1m windows, second = 5m windows

    current = read_gold(settings)
    first = read_gold(settings, snapshot_id=snaps[0])
    # The first snapshot predates the 5m append, so it has strictly fewer rows.
    assert first.num_rows < current.num_rows
    assert current.num_rows == result.gold_rows
    assert set(first.column("window_size").to_pylist()) == {"1m"}
    assert set(current.column("window_size").to_pylist()) == {"1m", "5m"}
