"""Configuration for TickStream.

Two layers:

* :class:`KafkaSettings` / :class:`RuntimeSettings` — environment-driven settings
  (12-factor style) read from ``KAFKA_*`` / ``TICKSTREAM_*`` env vars. These are the
  knobs that differ between "running on the host" and "running inside a container".
* :class:`SourceConfig` — the exchange/symbols/channels declaration, loaded from
  ``config/source.yaml`` so a reviewer can switch exchanges without touching code.

The default ``bootstrap_servers`` is ``localhost:19092`` (the Redpanda *external*
listener used by host processes and tests). In-container services override it to
``redpanda:9092`` via the ``KAFKA_BOOTSTRAP_SERVERS`` env var in docker-compose.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

# Repo root resolved relative to this file: src/tickstream/config.py -> repo root.
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE_CONFIG = REPO_ROOT / "config" / "source.yaml"


class Topics(BaseModel):
    """Canonical Redpanda topic names used across the pipeline."""

    trades_raw: str = "trades.raw"
    ticker_raw: str = "ticker.raw"
    metrics_windowed: str = "metrics.windowed"
    quarantine: str = "contracts.quarantine"

    def all(self) -> list[str]:
        return [self.trades_raw, self.ticker_raw, self.metrics_windowed, self.quarantine]


class KafkaSettings(BaseSettings):
    """Connection settings for the Redpanda (Kafka API) broker."""

    model_config = SettingsConfigDict(env_prefix="KAFKA_", env_file=".env", extra="ignore")

    bootstrap_servers: str = "localhost:19092"
    client_id: str = "tickstream"
    # Pin to IPv4 so "localhost" doesn't waste a connect attempt on an unbound ::1.
    broker_address_family: str = "v4"
    # Topic auto-creation defaults (single-node dev cluster).
    default_partitions: int = 1
    default_replication: int = 1


class RuntimeSettings(BaseSettings):
    """Pipeline runtime knobs (env prefix ``TICKSTREAM_``)."""

    model_config = SettingsConfigDict(env_prefix="TICKSTREAM_", env_file=".env", extra="ignore")

    source_config_path: Path = DEFAULT_SOURCE_CONFIG
    log_level: str = "INFO"
    log_json: bool = True
    # Local lakehouse storage root (bronze/silver/windows Parquet).
    lake_root: Path = REPO_ROOT / "lake_data"
    # Warehouse root (DuckDB file + Iceberg catalog/data for the gold layer).
    warehouse_root: Path = REPO_ROOT / "warehouse"


class ExchangeProfile(BaseModel):
    """Per-exchange connection details from source.yaml."""

    ws_url: str
    channel_map: dict[str, str] = Field(default_factory=dict)
    symbol_style: str = "dash"  # "dash" -> BTC-USD, "concat" -> BTCUSD

    def exchange_symbol(self, symbol: str) -> str:
        """Render a canonical ``BTC-USD`` symbol in this exchange's native style."""
        if self.symbol_style == "concat":
            return symbol.replace("-", "")
        return symbol

    def exchange_channel(self, channel: str) -> str:
        """Map a logical channel (``trades``) to the exchange's native name."""
        return self.channel_map.get(channel, channel)


class SourceConfig(BaseModel):
    """Parsed ``config/source.yaml``."""

    exchange: str
    symbols: list[str]
    channels: list[str]
    exchanges: dict[str, ExchangeProfile]

    @field_validator("symbols", "channels")
    @classmethod
    def _non_empty(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("must be non-empty")
        return v

    @property
    def profile(self) -> ExchangeProfile:
        if self.exchange not in self.exchanges:
            raise ValueError(
                f"exchange '{self.exchange}' not found in source.yaml "
                f"(have: {sorted(self.exchanges)})"
            )
        return self.exchanges[self.exchange]


def load_source_config(path: Path | str | None = None) -> SourceConfig:
    """Load and validate ``config/source.yaml``."""
    cfg_path = Path(path) if path is not None else RuntimeSettings().source_config_path
    if not cfg_path.exists():
        raise FileNotFoundError(f"source config not found: {cfg_path}")
    raw = yaml.safe_load(cfg_path.read_text())
    return SourceConfig.model_validate(raw)


class Settings(BaseModel):
    """Aggregate settings object passed around the pipeline."""

    kafka: KafkaSettings
    runtime: RuntimeSettings
    topics: Topics
    source: SourceConfig


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Build the aggregate :class:`Settings` (cached). Env vars take effect at import time."""
    runtime = RuntimeSettings()
    return Settings(
        kafka=KafkaSettings(),
        runtime=runtime,
        topics=Topics(),
        source=load_source_config(runtime.source_config_path),
    )
