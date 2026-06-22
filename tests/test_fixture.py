"""The committed fixture parses and normalizes to the schema (deterministic, no broker).

This is the offline backbone of the project: every downstream test and `make replay` runs
off this file, so it must parse cleanly and satisfy the data contract.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from tickstream.config import get_settings
from tickstream.producer.normalize import build_symbol_map, normalize
from tickstream.producer.recording import read_fixture
from tickstream.schema import MarketEvent

_TS = datetime(2026, 1, 1, tzinfo=UTC)


def _normalize_fixture(path: Path, ts_ingest: datetime = _TS) -> list[MarketEvent]:
    settings = get_settings()
    symbol_maps: dict[str, dict[str, str]] = {}
    events: list[MarketEvent] = []
    for rec in read_fixture(path):
        if rec.exchange not in symbol_maps:
            symbol_maps[rec.exchange] = build_symbol_map(settings, rec.exchange)
        events.extend(
            normalize(
                rec.exchange, rec.payload, ts_ingest=ts_ingest, symbol_map=symbol_maps[rec.exchange]
            )
        )
    return events


def test_fixture_exists_and_is_nonempty(fixture_path: Path) -> None:
    assert fixture_path.exists(), "run `make record` to capture fixtures/recorded_stream.jsonl"
    assert next(read_fixture(fixture_path), None) is not None


def test_fixture_normalizes_to_valid_market_events(fixture_path: Path) -> None:
    events = _normalize_fixture(fixture_path)
    # Each raw message expands to >= 1 events; trade snapshots carry many trades.
    assert len(events) > 0
    symbols = set()
    for e in events:
        assert isinstance(e, MarketEvent)
        if e.price is not None:
            assert e.price > 0
        if e.size is not None:
            assert e.size >= 0
        assert e.ts_event.tzinfo is not None  # event time is tz-aware (windowing key)
        symbols.add(e.symbol)
    # The recorded symbol set is the configured one.
    assert symbols <= set(get_settings().source.symbols)


def test_normalization_is_deterministic_except_ingest_stamp(fixture_path: Path) -> None:
    """Only ts_ingest varies per run; ordering and every other field are deterministic."""
    run_a = _normalize_fixture(fixture_path, ts_ingest=_TS)
    run_b = _normalize_fixture(fixture_path, ts_ingest=_TS + timedelta(hours=1))
    assert len(run_a) == len(run_b)
    for a, b in zip(run_a, run_b, strict=True):
        assert a.model_dump(exclude={"ts_ingest"}) == b.model_dump(exclude={"ts_ingest"})
        assert a.ts_ingest != b.ts_ingest
