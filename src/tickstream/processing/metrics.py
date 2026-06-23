"""Pure windowed microstructure metrics — the reference implementation.

This module is the deterministic, socket-free definition of *what the windows should be*.
``processing/app.py`` computes the same metrics as a genuine Quix Streams tumbling-window
pipeline; this module is its unit-test oracle and the source of the metric definitions.

A tumbling window of width ``W`` is epoch-aligned: an event at time ``t`` falls in the window
``[floor(t/W)*W, floor(t/W)*W + W)``. Bucketing is by EVENT time, so the result is independent
of the order events arrive in (out-of-order data lands in the correct window).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime

from pydantic import BaseModel

from tickstream.schema import EventType, MarketEvent

# Canonical window sizes for the project.
WINDOW_SIZES: dict[str, int] = {"1m": 60, "5m": 300}


class WindowMetrics(BaseModel):
    """Microstructure metrics for one (symbol, window-size, window) bucket."""

    symbol: str
    window_size: str
    window_start: datetime
    window_end: datetime

    # Trade-derived (from trades.raw).
    vwap: float | None
    trade_volume: float
    trade_count: int

    # Ticker-derived (from ticker.raw).
    avg_spread: float | None
    avg_mid: float | None
    ticker_count: int

    event_count: int

    def key(self) -> tuple[str, str, datetime]:
        return (self.symbol, self.window_size, self.window_start)


def window_bounds(ts_event: datetime, window_seconds: int) -> tuple[datetime, datetime]:
    """Return the epoch-aligned ``[start, end)`` tumbling window containing ``ts_event``."""
    epoch = ts_event.timestamp()
    start = (int(epoch) // window_seconds) * window_seconds
    return (
        datetime.fromtimestamp(start, tz=UTC),
        datetime.fromtimestamp(start + window_seconds, tz=UTC),
    )


class _Acc:
    """Mutable per-window accumulator."""

    __slots__ = ("mid_sum", "pv", "spread_sum", "tickers", "trades", "vol")

    def __init__(self) -> None:
        self.pv = 0.0  # sum(price * size)
        self.vol = 0.0  # sum(size)
        self.trades = 0
        self.spread_sum = 0.0
        self.mid_sum = 0.0
        self.tickers = 0


def compute_windows(
    events: list[MarketEvent], window_seconds: int, window_label: str
) -> list[WindowMetrics]:
    """Aggregate events into tumbling-window metrics, sorted by (symbol, window_start).

    VWAP = sum(price*size)/sum(size) over trades; avg_spread/avg_mid are means over tickers.
    """
    acc: dict[tuple[str, datetime], _Acc] = defaultdict(_Acc)

    for ev in events:
        start, _end = window_bounds(ev.ts_event, window_seconds)
        a = acc[(ev.symbol, start)]
        if ev.event_type == EventType.TRADE.value and ev.price is not None and ev.size is not None:
            a.pv += ev.price * ev.size
            a.vol += ev.size
            a.trades += 1
        elif (
            ev.event_type == EventType.TICKER.value
            and ev.best_bid is not None
            and ev.best_ask is not None
        ):
            a.spread_sum += ev.best_ask - ev.best_bid
            a.mid_sum += (ev.best_ask + ev.best_bid) / 2.0
            a.tickers += 1

    out: list[WindowMetrics] = []
    for (symbol, start), a in acc.items():
        _s, end = window_bounds(start, window_seconds)
        out.append(
            WindowMetrics(
                symbol=symbol,
                window_size=window_label,
                window_start=start,
                window_end=end,
                vwap=(a.pv / a.vol) if a.vol > 0 else None,
                trade_volume=a.vol,
                trade_count=a.trades,
                avg_spread=(a.spread_sum / a.tickers) if a.tickers > 0 else None,
                avg_mid=(a.mid_sum / a.tickers) if a.tickers > 0 else None,
                ticker_count=a.tickers,
                event_count=a.trades + a.tickers,
            )
        )
    out.sort(key=lambda m: (m.symbol, m.window_start))
    return out


def max_event_ts_by_symbol(events: list[MarketEvent]) -> dict[str, datetime]:
    """Latest event time seen per symbol (the per-key watermark a streaming engine reaches)."""
    out: dict[str, datetime] = {}
    for ev in events:
        cur = out.get(ev.symbol)
        if cur is None or ev.ts_event > cur:
            out[ev.symbol] = ev.ts_event
    return out


def closed_windows(
    metrics: list[WindowMetrics],
    max_ts_by_symbol: dict[str, datetime],
    grace_seconds: int = 0,
) -> list[WindowMetrics]:
    """Subset of ``metrics`` whose window has closed under the given per-symbol watermark.

    A tumbling window ``[start, end)`` closes once the watermark for its symbol reaches
    ``end + grace`` — i.e. a later event exists. The trailing (still-open) window per symbol
    is therefore excluded; this is exactly the set a streaming engine emits via ``.final()``.
    """
    out = []
    for m in metrics:
        wm = max_ts_by_symbol.get(m.symbol)
        if wm is None:
            continue
        if wm.timestamp() >= m.window_end.timestamp() + grace_seconds:
            out.append(m)
    return out
