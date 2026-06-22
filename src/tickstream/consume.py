"""Consume normalized events back off Redpanda (used by the demo CLI and tests)."""

from __future__ import annotations

import time

from tickstream.config import Settings
from tickstream.kafka_utils import build_consumer
from tickstream.logging import get_logger
from tickstream.schema import MarketEvent

log = get_logger("consumer")


def consume_events(
    settings: Settings,
    topics: list[str],
    *,
    group_id: str,
    max_messages: int,
    timeout: float = 20.0,
    auto_offset_reset: str = "earliest",
) -> list[MarketEvent]:
    """Consume up to ``max_messages`` events from ``topics`` (or until ``timeout``).

    Returns the decoded :class:`MarketEvent` list. Used by the broker round-trip test.
    """
    consumer = build_consumer(
        settings,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,
    )
    consumer.subscribe(topics)
    out: list[MarketEvent] = []
    deadline = time.monotonic() + timeout
    try:
        while len(out) < max_messages and time.monotonic() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning("consume_error", error=str(msg.error()))
                continue
            out.append(MarketEvent.from_json_bytes(msg.value()))
    finally:
        consumer.close()
    log.info("consumed", count=len(out), topics=topics)
    return out
