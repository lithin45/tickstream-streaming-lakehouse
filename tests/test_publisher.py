"""Unit tests for producer routing (no broker required).

Guards the by-type routing path used by the live demo (`publish_events(..., topic=None)`),
including the ``event.event_type == EventType.TICKER.value`` comparison that only works
because the schema uses ``use_enum_values=True``.
"""

from __future__ import annotations

from tickstream.config import get_settings
from tickstream.producer.demo import make_demo_events
from tickstream.producer.publisher import topic_for_event
from tickstream.schema import EventType


def test_topic_routing_by_event_type() -> None:
    settings = get_settings()
    trade = next(e for e in make_demo_events() if e.event_type == EventType.TRADE)
    ticker = next(e for e in make_demo_events() if e.event_type == EventType.TICKER)

    assert topic_for_event(settings, trade) == settings.topics.trades_raw
    assert topic_for_event(settings, ticker) == settings.topics.ticker_raw
