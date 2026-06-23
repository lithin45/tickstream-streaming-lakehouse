"""Build the silver/gold marts: run dbt-duckdb, then land gold in Apache Iceberg.

dbt-duckdb reads bronze Parquet and builds the silver views + gold table (SQL window
aggregations). We then read the gold table out of the DuckDB warehouse and materialize it as an
Iceberg table (with snapshots for time travel). This is the SQL-forward half of the lakehouse.
"""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel

from tickstream.config import REPO_ROOT, Settings, get_settings
from tickstream.lake.iceberg import materialize_gold
from tickstream.logging import get_logger

log = get_logger("lake.marts")

DBT_DIR = REPO_ROOT / "dbt"


class MartsResult(BaseModel):
    gold_rows: int
    snapshot_1m: int | None
    snapshot_5m: int | None


def duckdb_path(settings: Settings) -> Path:
    return Path(settings.runtime.warehouse_root) / "tickstream.duckdb"


def run_dbt(settings: Settings, *, command: str = "build") -> None:
    """Invoke dbt-duckdb programmatically (no shell/PATH dependency). Raises on failure."""
    from dbt.cli.main import dbtRunner

    db = duckdb_path(settings)
    db.parent.mkdir(parents=True, exist_ok=True)
    lake_root = Path(settings.runtime.lake_root).resolve()
    os.environ["TICKSTREAM_DUCKDB"] = str(db)

    args = [
        command,
        "--project-dir",
        str(DBT_DIR),
        "--profiles-dir",
        str(DBT_DIR),
        "--vars",
        f"lake_root: {lake_root}",
    ]
    result = dbtRunner().invoke(args)
    if not result.success:
        raise RuntimeError(f"dbt {command} failed: {getattr(result, 'exception', None)}")
    log.info("dbt_ok", command=command)


def read_gold_arrow(settings: Settings):
    """Read the dbt gold table out of the DuckDB warehouse as an Arrow table (UTC)."""
    import duckdb

    # Not read_only: dbt-duckdb may still hold a read-write connection to the same file in this
    # process, and DuckDB requires same-process connections to share access configuration.
    con = duckdb.connect(str(duckdb_path(settings)))
    try:
        con.execute("SET TimeZone='UTC'")
        return con.execute("SELECT * FROM gold_window_metrics").to_arrow_table()
    finally:
        con.close()


def build_marts(settings: Settings | None = None, *, run_tests: bool = True) -> MartsResult:
    """Run dbt (build = run + test) and materialize the gold Iceberg table. Returns counts."""
    settings = settings or get_settings()
    bronze = Path(settings.runtime.lake_root) / "bronze"
    if next(bronze.rglob("*.parquet"), None) is None:
        raise RuntimeError(f"no bronze Parquet under {bronze} — run `make replay` first")
    run_dbt(settings, command="build" if run_tests else "run")
    gold = read_gold_arrow(settings)
    snap_1m, snap_5m = materialize_gold(gold, settings)
    result = MartsResult(gold_rows=gold.num_rows, snapshot_1m=snap_1m, snapshot_5m=snap_5m)
    log.info("marts_built", **result.model_dump())
    return result
