"""Shared helpers for integration tests (topic isolation, draining)."""

from __future__ import annotations

import time
import uuid

import orjson

from tickstream.config import Settings, Topics
from tickstream.kafka_utils import build_consumer


def isolated_settings(settings: Settings) -> Settings:
    """A settings copy with unique topic names so concurrent/repeat runs don't accumulate."""
    uid = uuid.uuid4().hex[:8]
    topics = Topics(
        trades_raw=f"trades.raw.{uid}",
        ticker_raw=f"ticker.raw.{uid}",
        metrics_windowed=f"metrics.windowed.{uid}",
        quarantine=f"contracts.quarantine.{uid}",
    )
    return settings.model_copy(update={"topics": topics})


def drain_topic(settings: Settings, topic: str, *, timeout: float = 15.0) -> list[dict]:
    """Consume a topic from earliest until idle; return decoded JSON values."""
    consumer = build_consumer(
        settings,
        group_id=f"read-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
    )
    consumer.subscribe([topic])
    out: list[dict] = []
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            out.append(orjson.loads(msg.value()))
    finally:
        consumer.close()
    return out
