"""Land streamed window metrics (``metrics.windowed``) to Parquet for the lakehouse.

The Quix processor emits closed windows to the ``metrics.windowed`` topic; this sink drains
them to ``lake_data/windows/`` partitioned by window size and source. Phase 4's gold Iceberg
mart is built from this dataset.
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
from tickstream.producer.normalize import parse_ts

log = get_logger("lake.windows")

_PARTITION_COLS = ["window_size", "source"]
_TS = pa.timestamp("us", tz="UTC")

# Explicit schema so metric columns stay double/int64 even when a drain has only one source
# (e.g. a trades-only run leaves avg_spread all-None, which would otherwise infer as null type
# and break a DuckDB/dbt read across partitions).
WINDOWS_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("window_size", pa.string()),
        ("window_start", _TS),
        ("window_end", _TS),
        ("source", pa.string()),
        ("vwap", pa.float64()),
        ("trade_volume", pa.float64()),
        ("trade_count", pa.int64()),
        ("avg_spread", pa.float64()),
        ("avg_mid", pa.float64()),
        ("ticker_count", pa.int64()),
        ("event_count", pa.int64()),
    ]
)


def windows_root(settings: Settings) -> Path:
    return Path(settings.runtime.lake_root) / "windows"


def land_windows(
    settings: Settings | None = None,
    *,
    root: Path | str | None = None,
    group_id: str = "tickstream-windows-sink",
    timeout: float = 30.0,
    idle: float = 3.0,
    clear: bool = True,
) -> int:
    """Drain ``metrics.windowed`` into Parquet. Returns the number of window rows written."""
    settings = settings or get_settings()
    dest = Path(root) if root is not None else windows_root(settings)

    consumer = build_consumer(
        settings, group_id=group_id, auto_offset_reset="earliest", enable_auto_commit=False
    )
    consumer.subscribe([settings.topics.metrics_windowed])

    import orjson

    rows: list[dict] = []
    deadline = time.monotonic() + timeout
    idle_until = time.monotonic() + idle
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                if time.monotonic() >= idle_until:
                    break
                continue
            idle_until = time.monotonic() + idle
            rec = orjson.loads(msg.value())
            # Store window bounds as real timestamps for DuckDB/dbt.
            rec["window_start"] = parse_ts(rec["window_start"])
            rec["window_end"] = parse_ts(rec["window_end"])
            rows.append(rec)
    finally:
        consumer.close()

    if not rows:
        log.warning("windows_empty", topic=settings.topics.metrics_windowed)
        return 0

    if clear and dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True, exist_ok=True)

    table = pa.Table.from_pylist(rows, schema=WINDOWS_SCHEMA)
    pq.write_to_dataset(
        table,
        root_path=str(dest),
        partition_cols=_PARTITION_COLS,
        existing_data_behavior="overwrite_or_ignore",
    )
    log.info("windows_landed", rows=len(rows), root=str(dest))
    return len(rows)


def read_windows(settings: Settings | None = None, *, root: Path | str | None = None) -> pa.Table:
    """Read the whole windows dataset back as an Arrow table (used by tests/Phase 4)."""
    import pyarrow.dataset as ds

    settings = settings or get_settings()
    dest = Path(root) if root is not None else windows_root(settings)
    return ds.dataset(str(dest), format="parquet", partitioning="hive").to_table()
