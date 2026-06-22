"""Producer layer: exchange WebSocket client + Redpanda publishing.

Phase 1 ships a minimal hand-crafted publisher (:mod:`tickstream.producer.demo`).
Phase 2 adds the real WebSocket client and the record/replay harness.
"""

from tickstream.producer.publisher import publish_events

__all__ = ["publish_events"]
