"""Phase 3: windows Parquet sink — schema, types, partitions (the Phase-4 gold input).

Reads the landed dataset back and asserts the column types, so the contract dbt/DuckDB rely on
in Phase 4 is pinned. Includes a single-source run, which is exactly where unschema'd Arrow
inference would degrade all-None metric columns to `null` type.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest

from tests._helpers import isolated_settings
from tickstream.config import Settings
from tickstream.lake.windows import land_windows, read_windows
from tickstream.processing.app import run_processor
from tickstream.producer.publisher import publish_events
from tickstream.producer.replay import replay
from tickstream.schema import EventType, MarketEvent, Side

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_TS = pa.timestamp("us", tz="UTC")
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


def _process_and_land(settings: Settings, tmp_path, *, grace: int = 5):
    run_processor(
        settings,
        consumer_group=f"proc-{uuid.uuid4().hex[:8]}",
        state_dir=str(tmp_path / "state"),
        bounded_timeout=20.0,
        grace_seconds=grace,
    )
    root = tmp_path / "windows"
    n = land_windows(settings, root=root, group_id=f"win-{uuid.uuid4().hex[:8]}", clear=True)
    return n, read_windows(settings, root=root)


def test_windows_dataset_types_and_partitions(broker: Settings, tmp_path) -> None:
    settings = isolated_settings(broker)
    replay(settings)
    n, table = _process_and_land(settings, tmp_path)
    assert n > 0

    # Window bounds are real timestamps; metrics are numeric (never null type).
    assert table.schema.field("window_start").type == _TS
    assert table.schema.field("window_end").type == _TS
    for col in ("vwap", "trade_volume", "avg_spread", "avg_mid"):
        assert table.schema.field(col).type == pa.float64()
    for col in ("trade_count", "ticker_count", "event_count"):
        assert table.schema.field(col).type == pa.int64()

    assert set(table.column("window_size").to_pylist()) <= {"1m", "5m"}
    assert set(table.column("source").to_pylist()) == {"trades", "ticker"}


def test_single_source_windows_keep_numeric_types(broker: Settings, tmp_path) -> None:
    # Trades-only run -> avg_spread/avg_mid are all-None but MUST stay float64, not null type
    # (otherwise a DuckDB/dbt read across partitions fails to cast).
    settings = isolated_settings(broker)
    events = [_trade("BTC-USD", 100.0, 1.0, s) for s in (10, 20, 70, 80, 130)]
    publish_events(settings, events, topic=settings.topics.trades_raw)
    n, table = _process_and_land(settings, tmp_path, grace=2)
    assert n > 0
    assert set(table.column("source").to_pylist()) == {"trades"}
    assert table.schema.field("avg_spread").type == pa.float64()
    assert table.schema.field("avg_mid").type == pa.float64()
    assert table.schema.field("vwap").type == pa.float64()
