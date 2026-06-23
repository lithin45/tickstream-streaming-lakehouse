"""DuckDB SQL over the gold Iceberg tables — analytical queries + time travel.

Demonstrates the SQL-forward query layer: DuckDB reads the gold Iceberg table directly via
``iceberg_scan`` (window functions, aggregations), and pyiceberg powers point-in-time
(time-travel) reads of prior snapshots.
"""

from __future__ import annotations

from typing import Any

import duckdb

from tickstream.config import Settings, get_settings
from tickstream.lake.iceberg import gold_metadata_location, gold_snapshots, read_gold

# A window-function query: 1-minute VWAP with a 3-window trailing moving average per symbol.
VWAP_MOVING_AVG_SQL = """
SELECT
    symbol,
    window_start,
    vwap,
    avg(vwap) OVER (
        PARTITION BY symbol
        ORDER BY window_start
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS vwap_ma3,
    trade_volume,
    trade_count
FROM gold
WHERE window_size = '1m' AND trade_count > 0
ORDER BY symbol, window_start
"""

SYMBOL_SUMMARY_SQL = """
SELECT
    symbol,
    count(*) AS windows,
    sum(trade_volume) AS total_volume,
    round(avg(avg_spread), 6) AS mean_spread
FROM gold
WHERE window_size = '1m'
GROUP BY symbol
ORDER BY symbol
"""


def gold_connection(settings: Settings | None = None) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with the gold Iceberg table exposed as a view ``gold``."""
    settings = settings or get_settings()
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("SET TimeZone='UTC';")
    # Point at the catalog's current metadata.json so DuckDB and pyiceberg always agree (no
    # version-guessing, which could otherwise pick an orphaned/stale generation).
    metadata = gold_metadata_location(settings)
    con.execute(f"CREATE VIEW gold AS SELECT * FROM iceberg_scan('{metadata}')")
    return con


def query_gold(sql: str, settings: Settings | None = None) -> list[dict[str, Any]]:
    """Run SQL against the gold Iceberg table (``gold`` view). Returns a list of row dicts."""
    con = gold_connection(settings)
    try:
        rel = con.execute(sql)
        cols = [c[0] for c in rel.description]
        return [dict(zip(cols, row, strict=True)) for row in rel.fetchall()]
    finally:
        con.close()


def vwap_moving_average(settings: Settings | None = None) -> list[dict[str, Any]]:
    return query_gold(VWAP_MOVING_AVG_SQL, settings)


def symbol_summary(settings: Settings | None = None) -> list[dict[str, Any]]:
    return query_gold(SYMBOL_SUMMARY_SQL, settings)


def time_travel(settings: Settings | None = None) -> dict[str, Any]:
    """Compare the current gold table to its first snapshot (Iceberg time travel)."""
    settings = settings or get_settings()
    snaps = gold_snapshots(settings)
    current = read_gold(settings)
    result: dict[str, Any] = {
        "snapshots": snaps,
        "current_rows": current.num_rows,
    }
    if snaps:
        first = read_gold(settings, snapshot_id=snaps[0])
        result["first_snapshot_id"] = snaps[0]
        result["first_snapshot_rows"] = first.num_rows
    return result
