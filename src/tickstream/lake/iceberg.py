"""Gold layer — Apache Iceberg tables via pyiceberg (local SQLite catalog + Parquet storage).

The dbt gold mart is materialized into an Iceberg table so it gets schema evolution, snapshots,
and **time travel**. We deliberately write the mart in two appends (1-minute windows, then
5-minute windows) so there are two snapshots to demonstrate a time-travel query.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog.sql import SqlCatalog

from tickstream.config import Settings, get_settings
from tickstream.logging import get_logger

log = get_logger("lake.iceberg")

NAMESPACE = "gold"
TABLE = "gold.window_metrics"


def _paths(settings: Settings) -> tuple[Path, Path]:
    wh = Path(settings.runtime.warehouse_root)
    return wh / "catalog.db", wh / "iceberg"


def gold_catalog(settings: Settings | None = None) -> SqlCatalog:
    settings = settings or get_settings()
    catalog_db, data_dir = _paths(settings)
    catalog_db.parent.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        "tickstream",
        uri=f"sqlite:///{catalog_db}",
        warehouse=f"file://{data_dir}",
    )


def materialize_gold(
    gold: pa.Table, settings: Settings | None = None
) -> tuple[int | None, int | None]:
    """(Re)create the gold Iceberg table and append it as two snapshots (1m, then 5m).

    Returns ``(snapshot_after_1m, snapshot_after_5m)`` snapshot ids for time-travel.
    """
    settings = settings or get_settings()
    catalog = gold_catalog(settings)
    catalog.create_namespace_if_not_exists(NAMESPACE)

    # Deterministic rebuild: drop the catalog row AND purge the on-disk files. drop_table only
    # removes the SQLite row; without the rmtree, each rerun would leave an orphaned table
    # generation in the same directory, and a version-guessing reader could later pick a stale one.
    if catalog.table_exists(TABLE):
        catalog.drop_table(TABLE)
    shutil.rmtree(gold_table_location(settings), ignore_errors=True)
    table = catalog.create_table(TABLE, schema=gold.schema)

    first = gold.filter(pc.equal(gold["window_size"], "1m"))
    rest = gold.filter(pc.not_equal(gold["window_size"], "1m"))

    snap_1m = None
    if first.num_rows:
        table.append(first)
        snap_1m = table.current_snapshot().snapshot_id
    snap_5m = snap_1m
    if rest.num_rows:
        table.append(rest)
        snap_5m = table.current_snapshot().snapshot_id

    log.info(
        "gold_iceberg_materialized",
        rows=gold.num_rows,
        snapshot_1m=snap_1m,
        snapshot_5m=snap_5m,
    )
    return snap_1m, snap_5m


def read_gold(settings: Settings | None = None, *, snapshot_id: int | None = None) -> pa.Table:
    """Read the gold Iceberg table — the current state, or a prior snapshot (time travel)."""
    settings = settings or get_settings()
    table = gold_catalog(settings).load_table(TABLE)
    scan = table.scan(snapshot_id=snapshot_id) if snapshot_id is not None else table.scan()
    return scan.to_arrow()


def gold_snapshots(settings: Settings | None = None) -> list[int]:
    """Snapshot ids in chronological order (for time-travel demos/tests)."""
    settings = settings or get_settings()
    table = gold_catalog(settings).load_table(TABLE)
    return [s.snapshot_id for s in table.snapshots()]


def gold_table_location(settings: Settings | None = None) -> str:
    """Filesystem path of the Iceberg table directory."""
    settings = settings or get_settings()
    _catalog_db, data_dir = _paths(settings)
    return str(data_dir / "gold.db" / "window_metrics")


def gold_metadata_location(settings: Settings | None = None) -> str:
    """Filesystem path of the catalog's CURRENT metadata.json (for DuckDB ``iceberg_scan``).

    Pointing DuckDB at the exact metadata file the catalog records avoids version-guessing,
    so DuckDB always reads the same generation pyiceberg considers current.
    """
    settings = settings or get_settings()
    location = gold_catalog(settings).load_table(TABLE).metadata_location
    return location.removeprefix("file://")
