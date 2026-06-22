"""Hand-crafted demo messages for the Phase 1 broker round-trip.

Deterministic so the round-trip test can assert exact content. Phase 2 replaces this
with a real WebSocket producer + recorded-fixture replay.
"""

from __future__ import annotations

from datetime import UTC, datetime

from tickstream.config import Settings, get_settings
from tickstream.logging import get_logger
from tickstream.producer.publisher import publish_events
from tickstream.schema import EventType, MarketEvent, Side

log = get_logger("producer.demo")

# A fixed epoch so demo events are byte-for-byte reproducible.
_T0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=UTC)


def make_demo_events() -> list[MarketEvent]:
    """Return a small, deterministic set of normalized events (trades + ticker)."""
    events: list[MarketEvent] = [
        MarketEvent(
            exchange="demo",
            symbol="BTC-USD",
            event_type=EventType.TRADE,
            price=42000.50,
            size=0.10,
            side=Side.BUY,
            trade_id="t-1",
            ts_event=_T0,
            ts_ingest=_T0,
        ),
        MarketEvent(
            exchange="demo",
            symbol="ETH-USD",
            event_type=EventType.TRADE,
            price=2200.25,
            size=1.50,
            side=Side.SELL,
            trade_id="t-2",
            ts_event=_T0,
            ts_ingest=_T0,
        ),
        MarketEvent(
            exchange="demo",
            symbol="SOL-USD",
            event_type=EventType.TRADE,
            price=98.75,
            size=12.0,
            side=Side.BUY,
            trade_id="t-3",
            ts_event=_T0,
            ts_ingest=_T0,
        ),
        MarketEvent(
            exchange="demo",
            symbol="BTC-USD",
            event_type=EventType.TICKER,
            best_bid=41999.0,
            best_ask=42001.0,
            best_bid_size=0.5,
            best_ask_size=0.7,
            ts_event=_T0,
            ts_ingest=_T0,
        ),
    ]
    return events


def publish_demo(settings: Settings | None = None) -> int:
    """Publish the demo events to their raw topics. Returns the count produced."""
    settings = settings or get_settings()
    events = make_demo_events()
    return publish_events(settings, events)


if __name__ == "__main__":  # pragma: no cover
    from tickstream.logging import configure_logging

    configure_logging()
    n = publish_demo()
    log.info("demo_published", count=n)
