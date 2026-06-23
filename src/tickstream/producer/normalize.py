"""Pure normalization: raw exchange messages -> list[MarketEvent].

This is the deterministic, socket-free core of the producer. Both the live producer and the
replayer call these functions; they are unit-tested against recorded samples. ``ts_ingest`` is
supplied by the caller (wall-clock at publish time for live/replay; a fixed value in tests).
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

from pydantic import ValidationError

from tickstream.schema import EventType, MarketEvent, Side

# Exceptions a single malformed/contract-violating raw message can raise during
# normalization. Callers (live producer, replay) catch these per-message to drop the bad
# record without aborting the whole stream.
NORMALIZE_ERRORS = (ValidationError, ValueError, KeyError, TypeError)

# ---------------------------------------------------------------------------
# Timestamp parsing
# ---------------------------------------------------------------------------
_FRAC = re.compile(r"\.(\d+)")


def parse_ts(value: str) -> datetime:
    """Parse an exchange ISO-8601 timestamp into an aware UTC datetime.

    Tolerant of a trailing ``Z`` and of sub-microsecond precision (e.g. Coinbase emits
    nanoseconds, which ``datetime`` can't hold) by truncating fractional seconds to 6 digits.
    """
    s = value.strip().replace("Z", "+00:00")
    s = _FRAC.sub(lambda m: "." + m.group(1)[:6], s)
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _ts_from_millis(ms: int | str) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000.0, tz=UTC)


_SIDE = {"BUY": Side.BUY, "SELL": Side.SELL}

# A quarantined sub-record: (reason, the raw sub-record that failed the contract).
Reject = tuple[str, dict]


def _resolve_symbol(raw: str, symbol_map: dict[str, str] | None) -> str | None:
    """Map a raw symbol to its canonical form. ``None`` => not a configured symbol (drop)."""
    if symbol_map is None:
        return raw  # no filtering (unit tests); accept as-is
    return symbol_map.get(raw)


# ---------------------------------------------------------------------------
# Coinbase Advanced Trade
# ---------------------------------------------------------------------------
def normalize_coinbase(
    payload: dict, *, ts_ingest: datetime, symbol_map: dict[str, str] | None = None
) -> tuple[list[MarketEvent], list[Reject]]:
    """Normalize a Coinbase ``market_trades``/``ticker`` envelope, PER record.

    Returns ``(events, rejects)``. Each trade/tick is handled independently, so one
    contract-violating record is rejected (quarantined upstream) without dropping its valid
    siblings in the same message. Non-configured symbols are silently dropped (not subscribed).
    """
    channel = payload.get("channel")
    events: list[MarketEvent] = []
    rejects: list[Reject] = []

    if channel == "market_trades":
        for ev in payload.get("events", []):
            # A "snapshot" is a backfill of historical trades sent on (re)subscribe — skip it;
            # only "update" events are genuinely new market activity.
            if ev.get("type") == "snapshot":
                continue
            for trade in ev.get("trades", []):
                try:
                    symbol = _resolve_symbol(trade["product_id"], symbol_map)
                    if symbol is None:
                        continue
                    events.append(
                        MarketEvent(
                            exchange="coinbase",
                            symbol=symbol,
                            event_type=EventType.TRADE,
                            price=float(trade["price"]),
                            size=float(trade["size"]),
                            side=_SIDE.get(str(trade.get("side", "")).upper()),
                            trade_id=str(trade["trade_id"]) if trade.get("trade_id") else None,
                            ts_event=parse_ts(trade["time"]),
                            ts_ingest=ts_ingest,
                        )
                    )
                except NORMALIZE_ERRORS as exc:
                    rejects.append((str(exc), trade))

    elif channel == "ticker":
        try:
            ts_event = parse_ts(payload["timestamp"])  # per-tick time not provided; use envelope
        except NORMALIZE_ERRORS as exc:
            return events, [(f"bad envelope timestamp: {exc}", payload)]
        for ev in payload.get("events", []):
            for tick in ev.get("tickers", []):
                try:
                    symbol = _resolve_symbol(tick["product_id"], symbol_map)
                    if symbol is None:
                        continue
                    events.append(
                        MarketEvent(
                            exchange="coinbase",
                            symbol=symbol,
                            event_type=EventType.TICKER,
                            price=_opt_float(tick.get("price")),
                            best_bid=_opt_float(tick.get("best_bid")),
                            best_ask=_opt_float(tick.get("best_ask")),
                            best_bid_size=_opt_float(tick.get("best_bid_quantity")),
                            best_ask_size=_opt_float(tick.get("best_ask_quantity")),
                            ts_event=ts_event,
                            ts_ingest=ts_ingest,
                        )
                    )
                except NORMALIZE_ERRORS as exc:
                    rejects.append((str(exc), tick))

    return events, rejects


# ---------------------------------------------------------------------------
# Binance.US (combined stream: {"stream": "...", "data": {...}})
# ---------------------------------------------------------------------------
def normalize_binance_us(
    payload: dict, *, ts_ingest: datetime, symbol_map: dict[str, str]
) -> tuple[list[MarketEvent], list[Reject]]:
    """Normalize a Binance.US ``trade``/``bookTicker`` message. Returns ``(events, rejects)``."""
    data = payload.get("data", payload)
    raw_symbol = data.get("s")
    symbol = symbol_map.get(raw_symbol) if raw_symbol is not None else None
    if symbol is None:
        return [], []  # not a configured symbol -> drop

    try:
        event_type_field = data.get("e")
        if event_type_field == "trade" or {"p", "q", "t", "T"} <= data.keys():
            is_buyer_maker = bool(data.get("m"))
            event = MarketEvent(
                exchange="binance_us",
                symbol=symbol,
                event_type=EventType.TRADE,
                price=float(data["p"]),
                size=float(data["q"]),
                side=Side.SELL if is_buyer_maker else Side.BUY,  # m=True => taker sold
                trade_id=str(data["t"]) if data.get("t") is not None else None,
                ts_event=_ts_from_millis(data["T"]),
                ts_ingest=ts_ingest,
            )
            return [event], []
        if {"b", "a"} <= data.keys():
            event = MarketEvent(
                exchange="binance_us",
                symbol=symbol,
                event_type=EventType.TICKER,
                best_bid=_opt_float(data.get("b")),
                best_ask=_opt_float(data.get("a")),
                best_bid_size=_opt_float(data.get("B")),
                best_ask_size=_opt_float(data.get("A")),
                ts_event=ts_ingest,  # bookTicker carries no event time
                ts_ingest=ts_ingest,
            )
            return [event], []
    except NORMALIZE_ERRORS as exc:
        return [], [(str(exc), data)]

    return [], []


def _opt_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)  # type: ignore[arg-type]


def normalize_safe(
    exchange: str,
    payload: dict,
    *,
    ts_ingest: datetime,
    symbol_map: dict[str, str] | None = None,
) -> tuple[list[MarketEvent], list[Reject]]:
    """Dispatch to the right exchange normalizer, returning ``(events, rejects)``."""
    if exchange == "coinbase":
        return normalize_coinbase(payload, ts_ingest=ts_ingest, symbol_map=symbol_map)
    if exchange == "binance_us":
        return normalize_binance_us(payload, ts_ingest=ts_ingest, symbol_map=symbol_map or {})
    raise ValueError(f"no normalizer for exchange '{exchange}'")


def normalize(
    exchange: str,
    payload: dict,
    *,
    ts_ingest: datetime,
    symbol_map: dict[str, str] | None = None,
) -> list[MarketEvent]:
    """Dispatch and return only the valid events (rejects dropped). Used by the oracle/tests."""
    return normalize_safe(exchange, payload, ts_ingest=ts_ingest, symbol_map=symbol_map)[0]


def build_symbol_map(settings, exchange: str) -> dict[str, str]:
    """Map an exchange's native symbol back to the canonical form (e.g. BTCUSD -> BTC-USD)."""
    profile = settings.source.exchanges[exchange]
    return {profile.exchange_symbol(sym): sym for sym in settings.source.symbols}
