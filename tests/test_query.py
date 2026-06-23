"""Phase 4: the DuckDB SQL query layer over gold Iceberg, incl. the rerun/staleness guard.

Covers what test_marts.py (pyiceberg-only) does not: the iceberg_scan-based query path, and the
case the high-severity review flagged — a second build_marts into the same warehouse must NOT
leave the DuckDB reader serving the prior run's (orphaned) Iceberg generation.
"""

from __future__ import annotations

import uuid

import pytest

from tests._helpers import isolated_settings
from tickstream.config import Settings
from tickstream.lake.bronze import bronze_root, write_bronze
from tickstream.lake.iceberg import read_gold
from tickstream.lake.marts import build_marts
from tickstream.producer.replay import replay
from tickstream.query.duck import query_gold, symbol_summary, vwap_moving_average

pytestmark = [pytest.mark.integration, pytest.mark.slow]


def _tmp_base(broker: Settings, tmp_path) -> Settings:
    runtime = broker.runtime.model_copy(
        update={"lake_root": tmp_path / "lake", "warehouse_root": tmp_path / "wh"}
    )
    return broker.model_copy(update={"runtime": runtime})


def _run(base: Settings, *, limit: int | None = None):
    """Replay (optionally limited) into a fresh topic set, write bronze, build the marts."""
    run = isolated_settings(base)
    replay(run, limit=limit)
    write_bronze(run, root=bronze_root(run), group_id=f"q-{uuid.uuid4().hex[:8]}", timeout=20.0)
    return build_marts(base)


def test_duckdb_query_layer_matches_gold(broker: Settings, tmp_path) -> None:
    base = _tmp_base(broker, tmp_path)
    result = _run(base)

    # The iceberg_scan `gold` view sees exactly the catalog-current rows.
    n = query_gold("SELECT count(*) AS n FROM gold", base)[0]["n"]
    assert n == read_gold(base).num_rows == result.gold_rows

    assert len(symbol_summary(base)) == 3  # three symbols
    ma = vwap_moving_average(base)
    assert ma, "moving-average query returned no rows"
    # The first window per symbol has ma3 == its own vwap (trailing 2-preceding window).
    first_per_symbol: dict[str, dict] = {}
    for row in ma:
        first_per_symbol.setdefault(row["symbol"], row)
    for row in first_per_symbol.values():
        assert row["vwap_ma3"] == pytest.approx(row["vwap"])


def test_query_reads_catalog_current_after_rerun(broker: Settings, tmp_path) -> None:
    base = _tmp_base(broker, tmp_path)

    full = _run(base)  # run 1: whole fixture
    rerun = _run(base, limit=300)  # run 2: far fewer messages -> fewer gold windows

    assert rerun.gold_rows < full.gold_rows, "run 2 should genuinely differ from run 1"
    # pyiceberg and DuckDB must both serve the SECOND run, not the stale first generation.
    assert read_gold(base).num_rows == rerun.gold_rows
    duck_rows = query_gold("SELECT count(*) AS n FROM gold", base)[0]["n"]
    assert duck_rows == rerun.gold_rows
