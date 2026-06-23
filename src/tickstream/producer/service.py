"""Live producer service: exchange WebSocket -> normalize -> Redpanda.

Long-running. Connects to the configured exchange (Coinbase, Binance.US fallback),
normalizes each message, and publishes to the raw topics. Resilience:

* transport errors AND graceful server closes both go through the same reconnect path,
  with jittered exponential backoff and a `max_reconnects` ceiling;
* the backoff is only reset after a *productive* connection (one that delivered messages),
  so a server that keeps closing immediately backs off instead of hot-looping;
* a single malformed/contract-violating message is logged + counted and skipped, never
  crashing the stream;
* SIGINT/SIGTERM trigger a graceful flush of buffered events before exit.

Used by `tickstream produce` / the producer container; NOT used by tests (which use the
offline replay path) other than the reconnect-loop unit tests in tests/test_service.py.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal

import websockets

from tickstream.config import Settings, get_settings
from tickstream.kafka_utils import build_producer, delivery_report, ensure_topics
from tickstream.logging import get_logger
from tickstream.producer.backoff import backoff_delays
from tickstream.producer.exchanges import build_client
from tickstream.producer.normalize import build_symbol_map, normalize_safe
from tickstream.producer.publisher import topic_for_event
from tickstream.quality.quarantine import quarantine_message
from tickstream.utils import utcnow

log = get_logger("producer.service")

_BACKOFF_BASE = 0.5
_BACKOFF_CAP = 30.0


async def run_producer(
    settings: Settings | None = None,
    *,
    exchange: str | None = None,
    max_messages: int | None = None,
    max_reconnects: int | None = None,
) -> int:
    """Stream live data to Redpanda until stopped (or limits hit). Returns events published."""
    settings = settings or get_settings()
    exchange = exchange or settings.source.exchange
    symbol_map = build_symbol_map(settings, exchange)

    ensure_topics(
        settings,
        [settings.topics.trades_raw, settings.topics.ticker_raw, settings.topics.quarantine],
    )
    producer = build_producer(settings)

    delays = backoff_delays(base=_BACKOFF_BASE, cap=_BACKOFF_CAP)
    published = 0
    quarantined = 0
    reconnects = 0

    try:
        while True:
            client = build_client(settings, exchange)
            log.info("connecting", source=client.describe())
            produced_this_conn = 0
            try:
                async for _channel, payload in client.stream():
                    events, rejects = normalize_safe(
                        exchange, payload, ts_ingest=utcnow(), symbol_map=symbol_map
                    )
                    for reason, sub in rejects:
                        quarantined += 1
                        producer.produce(
                            topic=settings.topics.quarantine,
                            value=quarantine_message(
                                exchange=exchange, seq=0, reason=reason, payload=sub
                            ),
                            on_delivery=delivery_report,
                        )
                        producer.poll(0)
                    for event in events:
                        producer.produce(
                            topic=topic_for_event(settings, event),
                            key=event.key(),
                            value=event.to_json_bytes(),
                            on_delivery=delivery_report,
                        )
                        producer.poll(0)
                        published += 1
                        produced_this_conn += 1
                    if max_messages and published >= max_messages:
                        producer.flush(timeout=10.0)
                        log.info(
                            "max_messages_reached", published=published, quarantined=quarantined
                        )
                        return published
            except (websockets.WebSocketException, OSError) as exc:
                producer.flush(timeout=5.0)
                log.warning("disconnected", error=str(exc))
            else:
                log.info("stream_ended")  # graceful server close

            # Shared reconnect path for BOTH transport errors and clean stream ends.
            reconnects += 1
            if max_reconnects is not None and reconnects > max_reconnects:
                log.error("max_reconnects_exceeded", reconnects=reconnects)
                return published
            if produced_this_conn > 0:
                # Healthy connection -> reset the backoff schedule.
                delays = backoff_delays(base=_BACKOFF_BASE, cap=_BACKOFF_CAP)
            delay = next(delays)
            log.warning("reconnecting", backoff_s=round(delay, 2), reconnects=reconnects)
            await asyncio.sleep(delay)
    finally:
        # Flush buffered events on any exit (return, cancellation, or signal-driven shutdown).
        producer.flush(timeout=10.0)


def run_producer_blocking(
    settings: Settings | None = None,
    *,
    exchange: str | None = None,
    max_messages: int | None = None,
    max_reconnects: int | None = None,
) -> int:
    """Run :func:`run_producer` to completion, flushing on SIGINT/SIGTERM (container stop)."""

    async def _runner() -> int:
        loop = asyncio.get_running_loop()
        task = asyncio.ensure_future(
            run_producer(
                settings,
                exchange=exchange,
                max_messages=max_messages,
                max_reconnects=max_reconnects,
            )
        )
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError):  # not supported on Windows
                loop.add_signal_handler(sig, task.cancel)
        try:
            return await task
        except asyncio.CancelledError:
            log.info("shutdown_signal_received")  # run_producer's finally already flushed
            return 0

    return asyncio.run(_runner())


def main() -> None:  # pragma: no cover - entrypoint for `tickstream produce`
    from tickstream.logging import configure_logging

    settings = get_settings()
    configure_logging(level=settings.runtime.log_level, json=settings.runtime.log_json)
    run_producer_blocking(settings)
