"""Shared pytest fixtures.

Integration tests are gated on a reachable Redpanda broker. If ``KAFKA_BOOTSTRAP_SERVERS``
points at a broker that does not answer, those tests are *skipped* with a clear message
rather than failing — but in CI / ``make test`` the broker is always up, so they run.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

from tickstream.config import REPO_ROOT, Settings, get_settings
from tickstream.kafka_utils import wait_for_broker


@pytest.fixture(scope="session")
def settings() -> Settings:
    return get_settings()


@pytest.fixture(scope="session", autouse=True)
def _cleanup_ephemeral_topics():
    """Best-effort: delete the per-run isolated test topics at session end, so a long-lived
    local broker doesn't accumulate thousands of them and hit its partition/memory limit.
    (CI starts a fresh broker each run, so this is purely a local-dev convenience.)"""
    yield
    try:
        from tickstream.kafka_utils import admin_client

        admin = admin_client(get_settings().kafka)
        prefixes = (
            "trades.raw.",
            "ticker.raw.",
            "metrics.windowed.",
            "contracts.quarantine.",
            "test.roundtrip.",
            "demo.roundtrip.",
        )
        doomed = [t for t in admin.list_topics(timeout=5).topics if t.startswith(prefixes)]
        if doomed:
            admin.delete_topics(doomed)
    except Exception:
        pass


@pytest.fixture(scope="session")
def broker(settings: Settings) -> Settings:
    """Provide a reachable broker, else skip — but *fail* if TICKSTREAM_REQUIRE_BROKER is set.

    Local dev skips integration tests when no broker is up. CI sets TICKSTREAM_REQUIRE_BROKER=1
    so a broker that fails to come up becomes a hard failure instead of a silent green build
    where the headline acceptance test never ran.
    """
    if wait_for_broker(settings, timeout=15.0):
        return settings
    msg = (
        f"no Redpanda broker reachable at {settings.kafka.bootstrap_servers} "
        "(start it with `make up`)"
    )
    if os.getenv("TICKSTREAM_REQUIRE_BROKER"):
        pytest.fail(msg)
    pytest.skip(msg)


@pytest.fixture
def unique_topic() -> str:
    """A throwaway topic name unique to one test, so 'earliest' reads exactly what we wrote."""
    return f"test.roundtrip.{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="session")
def fixture_path() -> Path:
    """Path to the committed recorded-stream fixture used by replay/normalization tests."""
    return REPO_ROOT / "fixtures" / "recorded_stream.jsonl"


@pytest.fixture(scope="session")
def fixture_events(fixture_path: Path):
    """The committed fixture normalized to MarketEvents (deterministic, fixed ts_ingest)."""
    from datetime import UTC, datetime

    from tickstream.producer.normalize import build_symbol_map, normalize
    from tickstream.producer.recording import read_fixture

    settings = get_settings()
    ts = datetime(2026, 1, 1, tzinfo=UTC)
    maps: dict[str, dict[str, str]] = {}
    events = []
    for rec in read_fixture(fixture_path):
        if rec.exchange not in maps:
            maps[rec.exchange] = build_symbol_map(settings, rec.exchange)
        events.extend(
            normalize(rec.exchange, rec.payload, ts_ingest=ts, symbol_map=maps[rec.exchange])
        )
    return events
