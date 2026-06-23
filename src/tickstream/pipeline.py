"""Offline pipeline orchestration — `make replay` reproduces the medallion end-to-end.

Runs the recorded fixture through the whole stack with no network and deterministically:

    fixture -> replay (raw topics) -> bronze Parquet
                                   -> Quix windows -> metrics.windowed -> windows Parquet

It uses ISOLATED per-run topics so repeated runs can't accumulate, and writes the lake outputs
to fixed locations (cleared each run). Phase 4 extends this with the dbt silver/gold marts.
"""

from __future__ import annotations

import shutil
import uuid
from pathlib import Path

from pydantic import BaseModel

from tickstream.config import Settings, Topics, get_settings
from tickstream.lake.bronze import bronze_root, write_bronze
from tickstream.lake.windows import land_windows, windows_root
from tickstream.logging import get_logger
from tickstream.processing.app import run_processor
from tickstream.producer.replay import DEFAULT_FIXTURE, replay

log = get_logger("pipeline")


class PipelineResult(BaseModel):
    replayed_events: int
    bronze_rows: int
    window_rows: int


def _isolated(settings: Settings, uid: str) -> Settings:
    """A settings copy with unique topic names so reruns don't accumulate."""
    topics = Topics(
        trades_raw=f"trades.raw.{uid}",
        ticker_raw=f"ticker.raw.{uid}",
        metrics_windowed=f"metrics.windowed.{uid}",
        quarantine=f"contracts.quarantine.{uid}",
    )
    return settings.model_copy(update={"topics": topics})


def run_pipeline(
    settings: Settings | None = None,
    *,
    fixture: Path | str = DEFAULT_FIXTURE,
    bounded_timeout: float = 15.0,
    state_dir: str | None = None,
) -> PipelineResult:
    """Replay the fixture through raw -> bronze -> windows, writing the lake outputs."""
    settings = settings or get_settings()
    uid = uuid.uuid4().hex[:8]
    run = _isolated(settings, uid)
    auto_state = state_dir is None
    if state_dir is None:
        state_dir = str(Path(settings.runtime.lake_root) / "processor-state" / uid)

    log.info("pipeline_start", run=uid, fixture=str(fixture))
    summary = replay(run, fixture=fixture)
    bronze_rows = write_bronze(
        run, root=bronze_root(settings), group_id=f"bronze-{uid}", clear=True
    )
    try:
        run_processor(
            run,
            consumer_group=f"pipeline-proc-{uid}",
            state_dir=state_dir,
            bounded_timeout=bounded_timeout,
        )
        window_rows = land_windows(
            run, root=windows_root(settings), group_id=f"windows-{uid}", clear=True
        )
    finally:
        # The per-run RocksDB store is only needed during the bounded run; reclaim it so
        # lake_data/processor-state doesn't grow unbounded over repeated `make replay`.
        if auto_state:
            shutil.rmtree(state_dir, ignore_errors=True)

    result = PipelineResult(
        replayed_events=summary.events, bronze_rows=bronze_rows, window_rows=window_rows
    )
    log.info("pipeline_complete", **result.model_dump())
    return result
