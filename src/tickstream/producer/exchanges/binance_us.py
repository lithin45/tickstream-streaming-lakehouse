"""Binance.US combined-stream WebSocket client (public market data, no API key).

US-region fallback for Coinbase. Uses the combined-stream endpoint so a single connection
carries all symbol/channel streams; each message is wrapped as ``{"stream", "data"}``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import orjson
import websockets

from tickstream.logging import get_logger

log = get_logger("exchange.binance_us")


class BinanceUSClient:
    name = "binance_us"

    def __init__(self, ws_url: str, streams: list[str]) -> None:
        # ws_url is the combined-stream base, e.g. wss://stream.binance.us:9443/stream
        self.streams = streams
        sep = "&" if "?" in ws_url else "?"
        self.url = f"{ws_url}{sep}streams={'/'.join(streams)}"

    def describe(self) -> str:
        return f"binance_us streams={self.streams}"

    async def stream(self) -> AsyncIterator[tuple[str | None, dict]]:
        async with websockets.connect(self.url, ping_interval=20, max_size=None) as ws:
            log.info("connected", streams=self.streams)
            async for raw in ws:
                msg = orjson.loads(raw)
                stream = msg.get("stream", "")
                # stream looks like "btcusd@trade" -> channel "trade".
                channel = stream.split("@", 1)[1] if "@" in stream else None
                yield channel, msg
