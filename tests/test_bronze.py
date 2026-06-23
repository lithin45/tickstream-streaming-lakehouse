"""Phase 3: bronze Parquet sink lands raw events (broker integration)."""

from __future__ import annotations

import pytest

from tests._helpers import isolated_settings
from tickstream.config import Settings
from tickstream.lake.bronze import read_bronze, write_bronze
from tickstream.producer.replay import replay

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def test_bronze_lands_replayed_events(broker: Settings, tmp_path) -> None:
    settings = isolated_settings(broker)
    summary = replay(settings)

    root = tmp_path / "bronze"
    rows = write_bronze(settings, root=root, group_id="bronze-test", timeout=20.0)
    assert rows == summary.events

    table = read_bronze(settings, root=root)
    assert table.num_rows == summary.events
    # Hive-partitioned by event_type + symbol.
    assert set(table.column("event_type").to_pylist()) <= {"trade", "ticker"}
    assert set(table.column("symbol").to_pylist()) == {"BTC-USD", "ETH-USD", "SOL-USD"}
    # Core normalized columns survive the round-trip.
    for col in ("price", "size", "ts_event", "exchange"):
        assert col in table.column_names
