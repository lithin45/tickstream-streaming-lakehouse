"""Publish normalized :class:`MarketEvent` records to Redpanda."""

from __future__ import annotations

from collections.abc import Iterable

from tickstream.config import Settings
from tickstream.kafka_utils import build_producer, delivery_report, ensure_topics
from tickstream.logging import get_logger
from tickstream.schema import EventType, MarketEvent

log = get_logger("producer")


def topic_for_event(settings: Settings, event: MarketEvent) -> str:
    """Route an event to its raw topic by event type."""
    if event.event_type == EventType.TICKER.value:
        return settings.topics.ticker_raw
    return settings.topics.trades_raw


def publish_events(
    settings: Settings,
    events: Iterable[MarketEvent],
    *,
    topic: str | None = None,
    ensure: bool = True,
    flush: bool = True,
) -> int:
    """Publish events to Redpanda. Returns the number of events produced.

    Parameters
    ----------
    topic:
        Force a single destination topic. If ``None``, each event is routed by type
        (trades -> ``trades.raw``, ticker -> ``ticker.raw``).
    ensure:
        Create destination topics first if missing.
    flush:
        Block until all messages are acknowledged before returning.
    """
    events = list(events)
    if ensure:
        targets = {topic} if topic else {topic_for_event(settings, e) for e in events}
        ensure_topics(settings, targets)

    producer = build_producer(settings)
    count = 0
    for event in events:
        dest = topic or topic_for_event(settings, event)
        producer.produce(
            topic=dest,
            key=event.key(),
            value=event.to_json_bytes(),
            on_delivery=delivery_report,
        )
        # Serve delivery callbacks; avoids the local queue filling on large batches.
        producer.poll(0)
        count += 1

    if flush:
        remaining = producer.flush(timeout=30.0)
        if remaining > 0:
            raise RuntimeError(f"publish flush incomplete: {remaining} messages undelivered")
    log.info("published", count=count, topic=topic or "by-type")
    return count
