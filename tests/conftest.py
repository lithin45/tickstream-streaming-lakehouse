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
