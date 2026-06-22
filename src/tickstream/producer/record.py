"""`make record` — capture a short live stream to a committed JSONL fixture.

Connects to the live exchange WebSocket for a bounded window (seconds and/or max messages),
captures the RAW messages, and writes them to ``fixtures/recorded_stream.jsonl``. This is the
ONLY component that touches the live socket; everything downstream (replay, tests) runs off
the committed fixture. Falls back from the configured exchange to the other if it can't
connect (e.g. Coinbase geo-restricted -> Binance.US).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path

from tickstream.config import Settings, get_settings
from tickstream.logging import get_logger
from tickstream.producer.exchanges import build_client
from tickstream.producer.recording import RecordedMessage, write_fixture

log = get_logger("record")

DEFAULT_FIXTURE = "fixtures/recorded_stream.jsonl"


async def _record_once(
    settings: Settings,
    exchange: str,
    *,
    seconds: float,
    max_messages: int | None,
) -> list[RecordedMessage]:
    """Record one exchange until the time/message budget is hit. Raises on connect failure."""
    client = build_client(settings, exchange)
    log.info("recording", source=client.describe(), seconds=seconds, max_messages=max_messages)
    captured: list[RecordedMessage] = []
    try:
        async with asyncio.timeout(seconds):
            async for channel, payload in client.stream():
                captured.append(
                    RecordedMessage(
                        seq=len(captured),
                        ts_recorded=datetime.now(UTC),
                        exchange=exchange,
                        channel=channel,
                        payload=payload,
                    )
                )
                if max_messages and len(captured) >= max_messages:
                    break
    except TimeoutError:
        pass  # window elapsed — normal stop
    log.info("recorded", exchange=exchange, count=len(captured))
    return captured


def record(
    settings: Settings | None = None,
    *,
    exchange: str | None = None,
    seconds: float = 20.0,
    max_messages: int | None = None,
    out_path: Path | str = DEFAULT_FIXTURE,
    fallback: bool = True,
) -> tuple[str, int]:
    """Record a fixture. Returns ``(exchange_used, message_count)``.

    Tries the configured (or given) exchange first; on connection failure or an empty
    capture, falls back to the other configured exchange when ``fallback`` is True.
    """
    settings = settings or get_settings()
    primary = exchange or settings.source.exchange
    candidates = [primary]
    if fallback:
        candidates += [e for e in settings.source.exchanges if e != primary]

    last_error: Exception | None = None
    for name in candidates:
        try:
            messages = asyncio.run(
                _record_once(settings, name, seconds=seconds, max_messages=max_messages)
            )
        except Exception as exc:  # connection/subscribe failure -> try next candidate
            log.warning("record_failed", exchange=name, error=str(exc))
            last_error = exc
            continue
        if messages:
            n = write_fixture(out_path, messages)
            log.info("fixture_written", path=str(out_path), exchange=name, count=n)
            return name, n
        log.warning("record_empty", exchange=name)

    if last_error is not None:
        raise last_error
    raise RuntimeError("recording produced no messages from any configured exchange")
