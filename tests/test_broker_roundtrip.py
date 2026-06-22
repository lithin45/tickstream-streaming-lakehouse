"""Phase 1 acceptance: a message published to Redpanda is read back intact.

Requires a running broker (``make up``). In CI the broker is started before pytest.
"""

from __future__ import annotations

import pytest

from tickstream.config import Settings
from tickstream.consume import consume_events
from tickstream.kafka_utils import ensure_topics, wait_for_broker
from tickstream.producer.demo import make_demo_events
from tickstream.producer.publisher import publish_events

pytestmark = pytest.mark.integration


def test_broker_healthcheck(broker: Settings) -> None:
    """The broker answers metadata requests (mirror of the Docker healthcheck)."""
    assert wait_for_broker(broker, timeout=10.0) is True


def test_topic_roundtrip(broker: Settings, unique_topic: str) -> None:
    """Publish the demo events to a fresh topic and read them all back, intact + in order."""
    events = make_demo_events()
    ensure_topics(broker, [unique_topic])

    produced = publish_events(broker, events, topic=unique_topic)
    assert produced == len(events)

    consumed = consume_events(
        broker,
        [unique_topic],
        group_id="test-roundtrip",
        max_messages=len(events),
        timeout=30.0,
    )

    assert len(consumed) == len(events)
    # Single partition + symbol key => exact ordering and content preserved.
    assert consumed == events
