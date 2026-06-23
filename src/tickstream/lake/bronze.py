"""Bronze layer — raw normalized events landed as partitioned Parquet.

A sink that drains ``trades.raw`` + ``ticker.raw`` and writes the normalized records to
``lake_data/bronze/`` partitioned by ``event_type`` and ``symbol``. This is the bottom of the
medallion: Phase 4's dbt ``silver`` model reads this Parquet via DuckDB.
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from tickstream.config import Settings, get_settings
from tickstream.kafka_utils import build_consumer
from tickstream.logging import get_logger
from tickstream.schema import MarketEvent

log = get_logger("lake.bronze")

_PARTITION_COLS = ["event_type", "symbol"]
_TS = pa.timestamp("us", tz="UTC")

# Explicit schema so column types are stable regardless of which event types a drain contains.
# (Without this, an all-None column — e.g. best_bid in a trades-only drain — infers as Arrow
# `null` and a later DuckDB/dbt read across partitions fails to cast null<->double.)
BRONZE_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("symbol", pa.string()),
        ("event_type", pa.string()),
        ("price", pa.float64()),
        ("size", pa.float64()),
        ("side", pa.string()),
        ("trade_id", pa.string()),
        ("best_bid", pa.float64()),
        ("best_ask", pa.float64()),
        ("best_bid_size", pa.float64()),
        ("best_ask_size", pa.float64()),
        ("ts_event", _TS),
        ("ts_ingest", _TS),
    ]
)


def bronze_root(settings: Settings) -> Path:
    return Path(settings.runtime.lake_root) / "bronze"


def write_bronze(
    settings: Settings | None = None,
    *,
    root: Path | str | None = None,
    group_id: str = "tickstream-bronze",
    timeout: float = 30.0,
    idle: float = 3.0,
    max_messages: int | None = None,
    clear: bool = True,
) -> int:
    """Drain the raw topics into bronze Parquet. Returns the number of rows written.

    ``clear`` wipes the bronze dir first so a replay reproduces it deterministically.
    """
    settings = settings or get_settings()
    dest = Path(root) if root is not None else bronze_root(settings)

    consumer = build_consumer(
        settings, group_id=group_id, auto_offset_reset="earliest", enable_auto_commit=False
    )
    consumer.subscribe([settings.topics.trades_raw, settings.topics.ticker_raw])

    rows: list[dict] = []
    deadline = time.monotonic() + timeout
    idle_until = time.monotonic() + idle
    try:
        while time.monotonic() < deadline:
            if max_messages is not None and len(rows) >= max_messages:
                break
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                if time.monotonic() >= idle_until:
                    break  # caught up: no new messages for `idle` seconds
                continue
            rows.append(MarketEvent.from_json_bytes(msg.value()).model_dump())
            idle_until = time.monotonic() + idle
    finally:
        consumer.close()

    if not rows:
        log.warning("bronze_empty", topics=[settings.topics.trades_raw, settings.topics.ticker_raw])
        return 0

    if clear and dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA)
    pq.write_to_dataset(
        table,
        root_path=str(dest),
        partition_cols=_PARTITION_COLS,
        existing_data_behavior="overwrite_or_ignore",
    )
    log.info("bronze_written", rows=len(rows), root=str(dest))
    return len(rows)


def read_bronze(settings: Settings | None = None, *, root: Path | str | None = None) -> pa.Table:
    """Read the whole bronze dataset back as an Arrow table (used by tests/Phase 4)."""
    import pyarrow.dataset as ds

    settings = settings or get_settings()
    dest = Path(root) if root is not None else bronze_root(settings)
    return ds.dataset(str(dest), format="parquet", partitioning="hive").to_table()
