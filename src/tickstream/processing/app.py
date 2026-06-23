"""Quix Streams stream processor — tumbling-window microstructure metrics.

Consumes ``trades.raw`` and ``ticker.raw``, computes 1-min and 5-min event-time tumbling
windows per symbol (windowing is keyed by the message key = symbol), and emits the closed
windows to ``metrics.windowed``:

* from trades: VWAP = sum(price*size)/sum(size), trade volume, trade count;
* from ticker: average bid/ask spread and mid.

Event time comes from each record's ``ts_event`` (a ``timestamp_extractor``), so windows are
assigned by when a trade happened, not when it was consumed. ``grace_ms`` tolerates
out-of-order/late data; records later than the grace are handled by the ``on_late`` hook.

The aggregation matches :mod:`tickstream.processing.metrics` exactly, which is its test oracle.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from quixstreams import Application
from quixstreams.dataframe.windows import aggregations as agg

from tickstream.config import Settings, get_settings
from tickstream.logging import get_logger
from tickstream.processing.metrics import WINDOW_SIZES
from tickstream.producer.normalize import parse_ts

log = get_logger("processor")

DEFAULT_GRACE_SECONDS = 5


def _extract_ts(value: dict, _headers: object, _timestamp: int, _ts_type: object) -> int:
    """Event-time extractor: ts_event (ISO string) -> epoch milliseconds."""
    return int(parse_ts(value["ts_event"]).timestamp() * 1000)


def _ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC).isoformat()


def _symbol_of(key: object) -> str:
    return key.decode() if isinstance(key, bytes | bytearray) else str(key)


def _on_late(
    _value: object,
    _key: object,
    timestamp_ms: int,
    _late_by_ms: int,
    start: int,
    end: int,
    _name: str,
    _topic: str,
    _partition: int,
    _offset: int,
) -> bool:
    """Late-record hook for an already-closed window.

    The record is dropped by Quix unconditionally (it cannot be folded into a closed window
    via this hook); we emit our own structured ``late_record`` warning and return ``False`` only
    to suppress Quix's duplicate built-in late-record log line.
    """
    log.warning(
        "late_record",
        event_ts=_ms_to_iso(timestamp_ms),
        window=f"[{_ms_to_iso(start)},{_ms_to_iso(end)})",
    )
    return False


def _finalize_trade_window(label: str):
    def _fin(value: dict, key: object, _ts: int, _headers: object) -> dict:
        volume = value["volume"]
        return {
            "symbol": _symbol_of(key),
            "window_size": label,
            "window_start": _ms_to_iso(value["start"]),
            "window_end": _ms_to_iso(value["end"]),
            "source": "trades",
            "vwap": (value["pv"] / volume) if volume > 0 else None,
            "trade_volume": volume,
            "trade_count": value["count"],
            "avg_spread": None,
            "avg_mid": None,
            "ticker_count": 0,
            "event_count": value["count"],
        }

    return _fin


def _finalize_ticker_window(label: str):
    def _fin(value: dict, key: object, _ts: int, _headers: object) -> dict:
        return {
            "symbol": _symbol_of(key),
            "window_size": label,
            "window_start": _ms_to_iso(value["start"]),
            "window_end": _ms_to_iso(value["end"]),
            "source": "ticker",
            "vwap": None,
            "trade_volume": 0.0,
            "trade_count": 0,
            "avg_spread": value["spread"],
            "avg_mid": value["mid"],
            "ticker_count": value["count"],
            "event_count": value["count"],
        }

    return _fin


def build_application(
    settings: Settings,
    *,
    consumer_group: str,
    state_dir: str,
    auto_offset_reset: str = "earliest",
    use_changelog_topics: bool = False,
) -> Application:
    return Application(
        broker_address=settings.kafka.bootstrap_servers,
        consumer_group=consumer_group,
        auto_offset_reset=auto_offset_reset,
        state_dir=state_dir,
        use_changelog_topics=use_changelog_topics,
        consumer_extra_config={"broker.address.family": settings.kafka.broker_address_family},
        producer_extra_config={"broker.address.family": settings.kafka.broker_address_family},
    )


def build_pipeline(
    app: Application,
    settings: Settings,
    *,
    window_sizes: dict[str, int] = WINDOW_SIZES,
    grace_seconds: int = DEFAULT_GRACE_SECONDS,
) -> None:
    """Register the trades + ticker windowed aggregations on ``app``."""
    metrics_topic = app.topic(settings.topics.metrics_windowed, value_serializer="json")
    grace = timedelta(seconds=grace_seconds)

    # --- trades: VWAP / volume / count ---
    trades_topic = app.topic(
        settings.topics.trades_raw, value_deserializer="json", timestamp_extractor=_extract_ts
    )
    sdf_t = app.dataframe(trades_topic)
    sdf_t["pv"] = sdf_t["price"] * sdf_t["size"]
    for label, secs in window_sizes.items():
        win = (
            sdf_t.tumbling_window(timedelta(seconds=secs), grace_ms=grace, on_late=_on_late)
            .agg(pv=agg.Sum("pv"), volume=agg.Sum("size"), count=agg.Count())
            # closing_strategy="key": a window closes only when a later message *for the same
            # symbol* advances time — the per-symbol watermark the reference models. Pinned
            # explicitly so a future Quix default change can't silently alter which windows emit.
            .final(closing_strategy="key")
        )
        win.apply(_finalize_trade_window(label), metadata=True).to_topic(metrics_topic)

    # --- ticker: avg spread / mid ---
    ticker_topic = app.topic(
        settings.topics.ticker_raw, value_deserializer="json", timestamp_extractor=_extract_ts
    )
    sdf_k = app.dataframe(ticker_topic)
    sdf_k["spread"] = sdf_k["best_ask"] - sdf_k["best_bid"]
    sdf_k["mid"] = (sdf_k["best_ask"] + sdf_k["best_bid"]) / 2.0
    for label, secs in window_sizes.items():
        win = (
            sdf_k.tumbling_window(timedelta(seconds=secs), grace_ms=grace, on_late=_on_late)
            .agg(spread=agg.Mean("spread"), mid=agg.Mean("mid"), count=agg.Count())
            # closing_strategy="key": a window closes only when a later message *for the same
            # symbol* advances time — the per-symbol watermark the reference models. Pinned
            # explicitly so a future Quix default change can't silently alter which windows emit.
            .final(closing_strategy="key")
        )
        win.apply(_finalize_ticker_window(label), metadata=True).to_topic(metrics_topic)


def run_processor(
    settings: Settings | None = None,
    *,
    consumer_group: str = "tickstream-processor",
    state_dir: str | None = None,
    bounded_timeout: float | None = None,
    window_sizes: dict[str, int] = WINDOW_SIZES,
    grace_seconds: int = DEFAULT_GRACE_SECONDS,
) -> None:
    """Run the processor. ``bounded_timeout`` (seconds) makes it stop when idle (replay/tests);
    ``None`` runs it live forever."""
    settings = settings or get_settings()
    if state_dir is None:
        state_dir = str(Path(settings.runtime.lake_root) / "processor-state")
    Path(state_dir).mkdir(parents=True, exist_ok=True)

    app = build_application(settings, consumer_group=consumer_group, state_dir=state_dir)
    build_pipeline(app, settings, window_sizes=window_sizes, grace_seconds=grace_seconds)
    log.info("processor_starting", bounded_timeout=bounded_timeout, state_dir=state_dir)
    app.run(timeout=bounded_timeout or 0.0)
