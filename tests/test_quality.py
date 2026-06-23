"""Phase 5 acceptance: quarantine routing + the SLA assertions (broker integration)."""

from __future__ import annotations

import pytest

from tests._helpers import drain_topic, isolated_settings
from tickstream.config import REPO_ROOT, Settings
from tickstream.kafka_utils import end_offset_total
from tickstream.pipeline import run_pipeline
from tickstream.producer.replay import replay
from tickstream.quality.quarantine import QUARANTINE_SCHEMA, land_quarantine, read_quarantine

pytestmark = [pytest.mark.integration, pytest.mark.slow]

_VIOLATIONS_FIXTURE = REPO_ROOT / "fixtures" / "contract_violations.jsonl"


def test_replay_quarantines_contract_violations(broker: Settings) -> None:
    settings = isolated_settings(broker)
    summary = replay(settings, fixture=_VIOLATIONS_FIXTURE)

    # 1 valid trade is published; the 3 violating records are quarantined, not published.
    assert summary.events == 1
    assert summary.quarantined == 3

    quarantine = drain_topic(settings, settings.topics.quarantine)
    assert len(quarantine) == 3
    # Each distinct violation is classified with its own reason (not a loose OR).
    reasons = " ".join(q["reason"] for q in quarantine)
    assert "price must be > 0" in reasons  # negative-price trade
    assert "product_id" in reasons  # missing-key trade
    assert "crossed book" in reasons  # crossed ticker

    # Only the single valid event reached the raw topics.
    raw = [settings.topics.trades_raw, settings.topics.ticker_raw]
    assert end_offset_total(settings, raw) == 1


def test_land_quarantine_writes_parquet(broker: Settings, tmp_path) -> None:
    settings = isolated_settings(broker)
    replay(settings, fixture=_VIOLATIONS_FIXTURE)

    n = land_quarantine(settings, root=tmp_path / "q", group_id="q-test")
    assert n == 3
    table = read_quarantine(settings, root=tmp_path / "q")
    assert table.num_rows == 3
    assert table.schema.names == QUARANTINE_SCHEMA.names
    # Payloads round-trip back to the original raw sub-records.
    import orjson

    payloads = [orjson.loads(p) for p in table.column("payload").to_pylist()]
    assert all(isinstance(p, dict) for p in payloads)


def _tmp_runtime(broker: Settings, tmp_path) -> Settings:
    runtime = broker.runtime.model_copy(
        update={"lake_root": tmp_path / "lake", "warehouse_root": tmp_path / "wh"}
    )
    return broker.model_copy(update={"runtime": runtime})


def test_slas_met_over_replay(broker: Settings, tmp_path) -> None:
    # Full medallion over the clean committed fixture, into an isolated tmp lake/warehouse.
    result = run_pipeline(_tmp_runtime(broker, tmp_path))

    # Anchor counts to the known clean fixture so a truncated drain can't pass green.
    assert result.replayed_events == 7005
    assert result.bronze_rows == 7005
    assert result.window_rows == 42
    assert result.gold_rows == 27

    assert result.quarantined == 0  # the committed fixture is contract-clean
    assert result.gold_violations == 0  # 0 contract violations reached gold
    assert result.completeness_pct >= 99.0  # >= 99% of expected windows produced (per symbol)
    assert result.latency_p95_s < 60.0  # e2e ingest->gold p95 < 60s
    assert result.sla_passed
