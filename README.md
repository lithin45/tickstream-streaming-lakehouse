# TickStream

**A real-time streaming lakehouse for crypto market microstructure — runs end-to-end on your laptop with one command, no API keys, no cloud.**

TickStream ingests a live crypto market-data WebSocket, computes rolling microstructure
analytics with windowed stream processing, enforces data contracts (quarantining bad
records), and lands query-ready lakehouse tables — Redpanda → Quix Streams → bronze/silver
Parquet → gold Apache Iceberg → DuckDB / Streamlit.

> **Status:** under active construction. **Phase 1 (scaffold + broker) is complete** — see
> [Build phases](#build-phases). A recorded-fixture replay harness (Phase 2) makes the whole
> pipeline reproducible offline with no network.

---

## Architecture

```mermaid
flowchart LR
    subgraph EX["Exchange (public WS, no key)"]
        CB["Coinbase Advanced Trade<br/>· Binance.US fallback"]
    end

    subgraph ING["Ingestion"]
        PROD["Producer<br/>normalize → MarketEvent"]
        REC[("fixtures/<br/>recorded_stream.jsonl")]
    end

    subgraph BROKER["Redpanda (Kafka API)"]
        T1["trades.raw"]
        T2["ticker.raw"]
        T3["metrics.windowed"]
        QT["contracts.quarantine"]
    end

    subgraph SP["Stream processing (Quix Streams)"]
        WIN["tumbling windows 1m/5m<br/>VWAP · spread · volume · count<br/>watermarks + late data"]
        DQ["data contracts<br/>(Great Expectations)"]
    end

    subgraph LAKE["Lakehouse (medallion)"]
        BRONZE[("bronze · Parquet<br/>raw normalized")]
        SILVER[("silver · Parquet<br/>clean / dedup — dbt")]
        GOLD[("gold · Apache Iceberg<br/>windowed marts — dbt")]
    end

    subgraph QUERY["Query + UI"]
        DUCK["DuckDB SQL<br/>+ Iceberg time-travel"]
        DASH["Streamlit dashboard"]
    end

    CB -->|live| PROD
    CB -.record.-> REC
    REC -.replay.-> PROD
    PROD --> T1 & T2
    T1 --> WIN
    T2 --> WIN
    WIN --> T3
    WIN --> BRONZE
    DQ -->|violations| QT
    BRONZE --> SILVER --> GOLD
    T3 --> GOLD
    GOLD --> DUCK --> DASH
```

**Medallion layers:** _bronze_ = raw normalized events (Parquet) · _silver_ = cleaned,
typed, deduplicated (dbt model) · _gold_ = windowed microstructure marts as Iceberg tables
(schema evolution + time travel).

---

## Why this stack (tradeoffs)

| Choice | Over | Why |
| --- | --- | --- |
| **Redpanda** | Kafka, Kinesis | Single container, no JVM/Zookeeper, Kafka-API compatible — real streaming that runs on a laptop. |
| **Quix Streams** | Flink, Faust | Python-native Pandas-like `StreamingDataFrame`, native Redpanda pairing; Faust is unmaintained. |
| **Apache Iceberg** | plain Parquet | Schema evolution, snapshots/time-travel, compaction for the gold marts. |
| **DuckDB + dbt** | Spark | Embedded SQL engine + SQL marts; zero infra, fast analytics over the lake. |

_(Expanded in the Phase 6 README.)_

---

## Quick start

```bash
make up             # start Redpanda (+ Console at http://localhost:8080), wait until healthy
make test           # run the full test suite against the live broker
make demo           # host round-trip: publish hand-crafted events and read them back (exact)
make demo-container # same round-trip, built + run inside Docker (proves the image)
make down           # stop the stack
```

Requirements: Docker + `docker compose`, and [`uv`](https://github.com/astral-sh/uv).
The project pins **Python 3.11** (managed by uv). Ports used: Redpanda `19092`,
Console `8080`, dashboard `8502` (later). No ports clash with common local services.

### Useful commands

```bash
make install      # uv sync (core + dev deps)
make test-unit    # unit tests only — no broker required
make lint         # ruff check
make format       # ruff format
tickstream --help # CLI: health · topics-create · produce-demo · demo
```

---

## Data source & licensing

Public WebSocket feeds only — **Coinbase Advanced Trade** (default) with a **Binance.US**
fallback, selectable in [`config/source.yaml`](config/source.yaml). No API keys. Symbols are
throttled to `BTC-USD, ETH-USD, SOL-USD`. The raw feed is **not redistributed** — only small
sample fixtures are committed, for offline tests and replay.

---

## Build phases

| Phase | Scope | State |
| --- | --- | --- |
| 1 | Scaffold + broker: uv project, src layout, Redpanda compose (healthy), Makefile, CI, broker round-trip test | ✅ done |
| 2 | Real WebSocket producer + `make record` / `make replay` harness | ⬜ |
| 3 | Quix Streams tumbling-window microstructure metrics (VWAP/spread/volume/count) | ⬜ |
| 4 | dbt-duckdb silver/gold marts + Iceberg time-travel | ⬜ |
| 5 | Data contracts (Great Expectations) + quarantine + SLA assertions | ⬜ |
| 6 | Streamlit dashboard + polished README/diagram | ⬜ |

---

## Project layout

```
src/tickstream/
  config.py        # env + source.yaml settings (pydantic)
  logging.py       # structlog JSON logging
  schema.py        # normalized MarketEvent contract
  kafka_utils.py   # producer/consumer factories, topic admin, health
  consume.py       # read events back off Redpanda
  cli.py           # `tickstream` CLI (typer)
  producer/        # publishing + (Phase 2) WebSocket client & replay
  processing/      # (Phase 3) Quix Streams windowing
  lake/            # (Phase 4) bronze/silver/gold + Iceberg
  quality/         # (Phase 5) contracts + quarantine + SLAs
  query/ ui/       # (Phase 4/6) DuckDB queries + Streamlit dashboard
config/source.yaml # exchange / symbols / channels
docker-compose.yml # Redpanda + Console (+ later services)
Makefile           # up / down / test / demo / record / replay / lint / format
tests/             # pytest (unit + broker integration)
```

## License

MIT. Market data belongs to the respective exchanges and is not redistributed.
