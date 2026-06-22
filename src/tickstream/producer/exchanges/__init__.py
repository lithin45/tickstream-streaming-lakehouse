"""Exchange WebSocket clients.

Each client connects to a public market-data WebSocket, subscribes to the configured
channels, and yields ``(channel, raw_payload)`` tuples. Transport only — normalization
lives in :mod:`tickstream.producer.normalize` so it can be unit-tested without a socket.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol, runtime_checkable

from tickstream.config import Settings
from tickstream.producer.exchanges.binance_us import BinanceUSClient
from tickstream.producer.exchanges.coinbase import CoinbaseClient


@runtime_checkable
class ExchangeClient(Protocol):
    """Common interface for an exchange market-data client."""

    name: str

    def describe(self) -> str: ...

    def stream(self) -> AsyncIterator[tuple[str | None, dict]]:
        """Async-iterate ``(channel, raw_payload)`` for one connection (raises on disconnect)."""
        ...


def build_client(settings: Settings, exchange: str | None = None) -> ExchangeClient:
    """Construct the exchange client for ``exchange`` (defaults to the configured one)."""
    source = settings.source
    name = exchange or source.exchange
    if name not in source.exchanges:
        raise ValueError(f"unknown exchange '{name}' (have: {sorted(source.exchanges)})")
    profile = source.exchanges[name]
    exchange_channels = [profile.exchange_channel(c) for c in source.channels]

    if name == "coinbase":
        return CoinbaseClient(
            ws_url=profile.ws_url,
            product_ids=list(source.symbols),
            channels=exchange_channels,
        )
    if name == "binance_us":
        # Build combined-stream names: btcusd@trade, btcusd@bookTicker, ...
        streams = [
            f"{profile.exchange_symbol(sym).lower()}@{ch}"
            for sym in source.symbols
            for ch in exchange_channels
        ]
        return BinanceUSClient(ws_url=profile.ws_url, streams=streams)

    raise ValueError(f"no client implementation for exchange '{name}'")


__all__ = ["BinanceUSClient", "CoinbaseClient", "ExchangeClient", "build_client"]
