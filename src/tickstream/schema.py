"""The common normalized market-event schema.

Every exchange message is normalized to a :class:`MarketEvent` before it is published
to Redpanda. This is the contract boundary between the messy exchange feed and the rest
of the pipeline — the data-quality suite (Phase 5) enforces a stricter version of these
same rules and quarantines violations.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any

import orjson
from pydantic import BaseModel, ConfigDict, Field, model_validator


class EventType(StrEnum):
    """Logical event type carried on the normalized record."""

    TRADE = "trade"
    TICKER = "ticker"


class Side(StrEnum):
    BUY = "buy"
    SELL = "sell"


class MarketEvent(BaseModel):
    """A single normalized market event (trade or ticker tick).

    The core fields ``{exchange, symbol, event_type, price, size, side, ts_event,
    ts_ingest}`` are shared by all event types. Ticker-only fields (best bid/ask) are
    optional so one schema covers both channels.
    """

    model_config = ConfigDict(extra="forbid", use_enum_values=True)

    exchange: str
    symbol: str  # canonical form, e.g. "BTC-USD"
    event_type: EventType

    # Trade fields.
    price: float | None = None
    size: float | None = None
    side: Side | None = None
    trade_id: str | None = None

    # Ticker fields (best bid/ask).
    best_bid: float | None = None
    best_ask: float | None = None
    best_bid_size: float | None = None
    best_ask_size: float | None = None

    # Timestamps (timezone-aware UTC).
    ts_event: datetime = Field(..., description="Exchange event time.")
    ts_ingest: datetime = Field(..., description="Time the producer received the message.")

    @model_validator(mode="after")
    def _basic_contract(self) -> MarketEvent:
        """Seed of the data contract: prices positive, sizes non-negative, bid<=ask.

        Phase 5 enforces the full contract with Great Expectations and routes violations
        to a quarantine topic; this catch is a cheap first line of defense at construction.
        """
        if self.price is not None and self.price <= 0:
            raise ValueError(f"price must be > 0, got {self.price}")
        for name in ("size", "best_bid", "best_ask", "best_bid_size", "best_ask_size"):
            val = getattr(self, name)
            if val is not None and val < 0:
                raise ValueError(f"{name} must be >= 0, got {val}")
        if (
            self.best_bid is not None
            and self.best_ask is not None
            and self.best_ask < self.best_bid
        ):
            raise ValueError(
                f"crossed book: best_ask ({self.best_ask}) < best_bid ({self.best_bid})"
            )
        return self

    @property
    def spread(self) -> float | None:
        """Bid/ask spread, when both sides are present."""
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None

    # ----- serialization helpers (Kafka value <-> model) -----

    def to_json_bytes(self) -> bytes:
        """Serialize to compact JSON bytes for a Kafka message value."""
        return orjson.dumps(self.model_dump(mode="json"))

    @classmethod
    def from_json_bytes(cls, data: bytes | str) -> MarketEvent:
        """Deserialize from a Kafka message value."""
        return cls.model_validate_json(data)

    def key(self) -> bytes:
        """Kafka partition key: symbol (keeps a symbol's events on one partition/order)."""
        return self.symbol.encode()


def loads_event(data: bytes | str) -> MarketEvent:
    """Module-level convenience wrapper around :meth:`MarketEvent.from_json_bytes`."""
    return MarketEvent.from_json_bytes(data)


def parse_raw_json(data: bytes | str) -> dict[str, Any]:
    """Parse arbitrary raw JSON (used by the producer before normalization)."""
    return orjson.loads(data)
