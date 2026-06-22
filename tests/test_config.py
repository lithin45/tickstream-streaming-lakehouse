"""Unit tests for configuration loading (no broker required)."""

from __future__ import annotations

from tickstream.config import Topics, load_source_config


def test_topics_defaults() -> None:
    topics = Topics()
    assert topics.trades_raw == "trades.raw"
    assert topics.ticker_raw == "ticker.raw"
    assert topics.metrics_windowed == "metrics.windowed"
    assert topics.quarantine == "contracts.quarantine"
    assert set(topics.all()) == {
        "trades.raw",
        "ticker.raw",
        "metrics.windowed",
        "contracts.quarantine",
    }


def test_source_config_loads_default() -> None:
    cfg = load_source_config()
    assert cfg.exchange in cfg.exchanges
    assert "BTC-USD" in cfg.symbols
    assert cfg.channels  # non-empty


def test_exchange_profile_symbol_styles() -> None:
    cfg = load_source_config()
    coinbase = cfg.exchanges["coinbase"]
    binance = cfg.exchanges["binance_us"]
    assert coinbase.exchange_symbol("BTC-USD") == "BTC-USD"
    assert binance.exchange_symbol("BTC-USD") == "BTCUSD"
    assert coinbase.exchange_channel("trades") == "market_trades"
    assert binance.exchange_channel("ticker") == "bookTicker"


def test_profile_property_resolves_active_exchange() -> None:
    cfg = load_source_config()
    assert cfg.profile is cfg.exchanges[cfg.exchange]
