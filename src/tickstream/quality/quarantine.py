"""Quarantine path — contract-violating records are routed here, never to the raw topics.

A record that fails normalization (a contract violation at the ingest boundary) is published to
the ``contracts.quarantine`` topic with its reason and raw payload, so it can never flow into
bronze/silver/gold. ``land_quarantine`` drains that topic to a Parquet table for inspection and
the "0 violations reached gold" SLA.
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

import orjson
import pyarrow as pa
import pyarrow.parquet as pq

from tickstream.config import Settings, get_settings
from tickstream.logging import get_logger
from tickstream.utils import utcnow

log = get_logger("quality.quarantine")

QUARANTINE_SCHEMA = pa.schema(
    [
        ("exchange", pa.string()),
        ("seq", pa.int64()),
        ("reason", pa.string()),
        ("payload", pa.string()),
        ("ts_quarantined", pa.timestamp("us", tz="UTC")),
    ]
)


def quarantine_message(*, exchange: str, seq: int, reason: str, payload: dict) -> bytes:
    """Build a quarantine record value for the ``contracts.quarantine`` topic."""
    return orjson.dumps(
        {
            "exchange": exchange,
            "seq": seq,
            "reason": reason,
            "payload": payload,
            "ts_quarantined": utcnow().isoformat(),
        }
    )


def quarantine_root(settings: Settings) -> Path:
    return Path(settings.runtime.lake_root) / "quarantine"


def land_quarantine(
    settings: Settings | None = None,
    *,
    root: Path | str | None = None,
    group_id: str = "tickstream-quarantine-sink",
    timeout: float = 15.0,
    idle: float = 3.0,
    clear: bool = True,
) -> int:
    """Drain the quarantine topic into a Parquet table. Returns the number of rows written."""
    from tickstream.kafka_utils import build_consumer
    from tickstream.producer.normalize import parse_ts

    settings = settings or get_settings()
    dest = Path(root) if root is not None else quarantine_root(settings)

    consumer = build_consumer(
        settings, group_id=group_id, auto_offset_reset="earliest", enable_auto_commit=False
    )
    consumer.subscribe([settings.topics.quarantine])
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
            rows.append(
                {
                    "exchange": rec.get("exchange"),
                    "seq": rec.get("seq"),
                    "reason": rec.get("reason"),
                    "payload": orjson.dumps(rec.get("payload")).decode(),
                    "ts_quarantined": parse_ts(rec["ts_quarantined"]),
                }
            )
    finally:
        consumer.close()

    if clear and dest.exists():
        shutil.rmtree(dest)
    if not rows:
        log.info("quarantine_empty", topic=settings.topics.quarantine)
        return 0

    dest.mkdir(parents=True, exist_ok=True)
    pq.write_to_dataset(pa.Table.from_pylist(rows, schema=QUARANTINE_SCHEMA), root_path=str(dest))
    log.info("quarantine_landed", rows=len(rows), root=str(dest))
    return len(rows)


def read_quarantine(
    settings: Settings | None = None, *, root: Path | str | None = None
) -> pa.Table:
    """Read the landed quarantine table back (used by tests)."""
    import pyarrow.dataset as ds

    settings = settings or get_settings()
    dest = Path(root) if root is not None else quarantine_root(settings)
    return ds.dataset(str(dest), format="parquet").to_table()
