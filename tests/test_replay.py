"""Phase 2 acceptance: replay publishes the expected message count to Redpanda.

Requires a broker (`make up`). Counts are derived independently from the fixture and verified
both producer-side (the replay summary) and broker-side (high-watermark offset delta), so the
assertion is robust to messages accumulated by prior replay runs.
"""

from __future__ import annotations

import itertools
from datetime import UTC, datetime
from pathlib import Path

import pytest

from tickstream.config import Settings
from tickstream.kafka_utils import end_offset_total, ensure_topics
from tickstream.producer.normalize import build_symbol_map, normalize
from tickstream.producer.recording import read_fixture
from tickstream.producer.replay import replay
from tickstream.schema import EventType

pytestmark = pytest.mark.integration

_TS = datetime(2026, 1, 1, tzinfo=UTC)


def _expected_counts(
    settings: Settings, fixture: Path, limit: int | None = None
) -> tuple[int, int, int]:
    """Independently count (events, trades, tickers) by normalizing the fixture directly."""
    symbol_maps: dict[str, dict[str, str]] = {}
    trades = tickers = 0
    records = read_fixture(fixture)
    if limit is not None:
        records = itertools.islice(records, limit)
    for rec in records:
        if rec.exchange not in symbol_maps:
            symbol_maps[rec.exchange] = build_symbol_map(settings, rec.exchange)
        for event in normalize(
            rec.exchange, rec.payload, ts_ingest=_TS, symbol_map=symbol_maps[rec.exchange]
        ):
            if event.event_type == EventType.TICKER.value:
                tickers += 1
            else:
                trades += 1
    return trades + tickers, trades, tickers


def test_replay_publishes_expected_count(broker: Settings, fixture_path: Path) -> None:
    exp_events, exp_trades, exp_tickers = _expected_counts(broker, fixture_path)
    assert exp_events > 0

    raw_topics = [broker.topics.trades_raw, broker.topics.ticker_raw]
    ensure_topics(broker, raw_topics)
    before = end_offset_total(broker, raw_topics)

    summary = replay(broker, fixture=fixture_path)

    # Producer-side counts match the independent normalization counts.
    assert summary.events == exp_events
    assert summary.trades == exp_trades
    assert summary.tickers == exp_tickers
    assert summary.skipped == 0  # the committed fixture is clean

    # Broker-side: exactly that many new messages actually landed across the raw topics.
    after = end_offset_total(broker, raw_topics)
    assert after - before == exp_events


def test_replay_limit_is_respected(broker: Settings, fixture_path: Path) -> None:
    exp_events, _, _ = _expected_counts(broker, fixture_path, limit=10)
    summary = replay(broker, fixture=fixture_path, limit=10)
    assert summary.messages == 10
    # Exact: the first 10 source messages expand to exactly this many events.
    assert summary.events == exp_events
