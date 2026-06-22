"""Coinbase Advanced Trade WebSocket client (public market data, no API key).

Subscribes to the ``market_trades`` and ``ticker`` channels for the configured products.
Public channels need no auth (only ``level2``/``user`` do). Control messages are throttled
to respect the ~8 msg/s subscription limit.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import orjson
import websockets

from tickstream.logging import get_logger

log = get_logger("exchange.coinbase")

# Non-market control frames we don't forward downstream.
_CONTROL_CHANNELS = {"subscriptions"}


class CoinbaseClient:
    name = "coinbase"

    def __init__(self, ws_url: str, product_ids: list[str], channels: list[str]) -> None:
        self.ws_url = ws_url
        self.product_ids = product_ids
        self.channels = channels

    def describe(self) -> str:
        return f"coinbase {self.ws_url} products={self.product_ids} channels={self.channels}"

    async def stream(self) -> AsyncIterator[tuple[str | None, dict]]:
        async with websockets.connect(self.ws_url, ping_interval=20, max_size=None) as ws:
            # One subscribe per channel; small gap to respect the control rate limit.
            for channel in self.channels:
                sub = {
                    "type": "subscribe",
                    "product_ids": self.product_ids,
                    "channel": channel,
                }
                await ws.send(orjson.dumps(sub).decode())
                await asyncio.sleep(0.2)
            log.info("subscribed", products=self.product_ids, channels=self.channels)

            async for raw in ws:
                msg = orjson.loads(raw)
                channel = msg.get("channel")
                if channel in _CONTROL_CHANNELS:
                    continue
                yield channel, msg
