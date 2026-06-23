"""`make replay` — feed a recorded fixture through Redpanda, deterministically and offline.

Reads the committed JSONL fixture, normalizes each raw message with the SAME code the live
producer uses, and publishes the resulting events to ``trades.raw`` / ``ticker.raw``. No
network, no keys — this is what makes the whole pipeline reproducible for a reviewer.

Replay is a *gate*: it raises on a malformed record or on incomplete delivery so the CLI /
compose / CI replay step exits non-zero instead of silently succeeding on a partial run.
"""

from __future__ import annotations

import time
from pathlib import Path

from pydantic import BaseModel

from tickstream.config import REPO_ROOT, Settings, get_settings
from tickstream.kafka_utils import DeliveryCounter, build_producer, ensure_topics
from tickstream.logging import get_logger
from tickstream.producer.normalize import build_symbol_map, normalize_safe
from tickstream.producer.publisher import topic_for_event
from tickstream.producer.recording import read_fixture
from tickstream.quality.quarantine import quarantine_message
from tickstream.schema import EventType
from tickstream.utils import utcnow

log = get_logger("replay")

DEFAULT_FIXTURE = REPO_ROOT / "fixtures" / "recorded_stream.jsonl"


class ReplaySummary(BaseModel):
    """Outcome of a replay run."""

    messages: int
    events: int
    trades: int
    tickers: int
    quarantined: int
    by_symbol: dict[str, int]


def replay(
    settings: Settings | None = None,
    *,
    fixture: Path | str = DEFAULT_FIXTURE,
    speed: float = 0.0,
    limit: int | None = None,
) -> ReplaySummary:
    """Replay ``fixture`` into the broker. Returns a :class:`ReplaySummary`.

    ``speed`` paces playback against the recorded inter-message gaps: 0 = as fast as possible
    (default; used by tests/CI), 1 = original real-time, 2 = 2x, etc. ``limit`` caps the number
    of source messages processed. Raises ``RuntimeError`` if any event fails to be delivered.
    """
    settings = settings or get_settings()
    fixture = Path(fixture)
    if not fixture.exists():
        raise FileNotFoundError(f"fixture not found: {fixture} (run `make record` first)")

    ensure_topics(
        settings,
        [settings.topics.trades_raw, settings.topics.ticker_raw, settings.topics.quarantine],
    )
    producer = build_producer(settings)
    counter = DeliveryCounter()
    symbol_maps: dict[str, dict[str, str]] = {}

    messages = trades = tickers = quarantined = 0
    by_symbol: dict[str, int] = {}
    prev_recorded: float | None = None

    for rec in read_fixture(fixture):
        if limit is not None and messages >= limit:
            break
        messages += 1

        # Optional real-time-ish pacing based on recording timestamps.
        if speed > 0:
            cur = rec.ts_recorded.timestamp()
            if prev_recorded is not None:
                gap = (cur - prev_recorded) / speed
                if gap > 0:
                    time.sleep(min(gap, 5.0))
            prev_recorded = cur

        if rec.exchange not in symbol_maps:
            symbol_maps[rec.exchange] = build_symbol_map(settings, rec.exchange)

        events, rejects = normalize_safe(
            rec.exchange,
            rec.payload,
            ts_ingest=utcnow(),
            symbol_map=symbol_maps[rec.exchange],
        )
        # Each contract-violating sub-record is quarantined individually (never published to
        # raw), so a single bad trade does not drop its valid siblings in the same message.
        for reason, sub in rejects:
            quarantined += 1
            producer.produce(
                topic=settings.topics.quarantine,
                value=quarantine_message(
                    exchange=rec.exchange, seq=rec.seq, reason=reason, payload=sub
                ),
                on_delivery=counter,
            )
            producer.poll(0)
            log.warning("quarantined", seq=rec.seq, reason=reason, quarantined=quarantined)

        for event in events:
            producer.produce(
                topic=topic_for_event(settings, event),
                key=event.key(),
                value=event.to_json_bytes(),
                on_delivery=counter,
            )
            producer.poll(0)
            if event.event_type == EventType.TICKER.value:
                tickers += 1
            else:
                trades += 1
            by_symbol[event.symbol] = by_symbol.get(event.symbol, 0) + 1

    remaining = producer.flush(timeout=30.0)
    produced = trades + tickers
    if remaining > 0 or counter.failed > 0:
        raise RuntimeError(
            f"replay delivery incomplete: {remaining} still queued, "
            f"{counter.failed} failed, {counter.delivered}/{produced} delivered"
        )

    summary = ReplaySummary(
        messages=messages,
        events=produced,
        trades=trades,
        tickers=tickers,
        quarantined=quarantined,
        by_symbol=by_symbol,
    )
    log.info("replay_complete", **summary.model_dump())
    return summary
