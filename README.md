# TickStream

**A real-time streaming lakehouse for crypto market microstructure — runs end-to-end on your laptop with one command, no API keys, no cloud.**

TickStream ingests a live crypto market-data WebSocket, computes rolling microstructure
analytics with windowed stream processing, enforces data contracts (quarantining bad
records), and lands query-ready lakehouse tables — Redpanda → Quix Streams → bronze/silver
Parquet → gold Apache Iceberg → DuckDB / Streamlit.

> **Status:** under active construction. **Phases 1–5 are complete** — see
> [Build phases](#build-phases). A recorded-fixture **replay harness** makes the whole
> pipeline reproducible offline with no network: `make replay` feeds a committed sample of
> real Coinbase market data through Redpanda → bronze Parquet **and** **Quix Streams
> tumbling-window analytics** → **dbt-built silver/gold marts** → **Apache Iceberg** gold tables
> queried with **DuckDB SQL** (incl. time travel), with **data contracts + a quarantine path**
> and **SLAs asserted against the replay** — deterministically.

**SLA result (latest `make replay`):** end-to-end ingest→gold latency **p95 30.8 s** (< 60 s) ·
**100%** of expected windows produced (≥ 99%) · **0** contract violations reached gold → **PASS**.

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
        DQ["data contracts<br/>(Pandera)"]
    end

    subgraph LAKE["Lakehouse (medallion)"]
        BRONZE[("bronze · Parquet<br/>raw normalized")]
        WINDOWS[("windows · Parquet<br/>tumbling metrics")]
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
    T1 --> BRONZE
    T2 --> BRONZE
    WIN --> T3
    DQ -->|violations| QT
    BRONZE --> SILVER --> GOLD
    T3 --> WINDOWS --> GOLD
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
make replay         # OFFLINE medallion: replay -> Quix windows -> bronze -> dbt silver/gold -> Iceberg
make query          # DuckDB SQL over the gold Iceberg table + an Iceberg time-travel query
make process        # run the live Quix Streams windowed processor
make demo           # host round-trip: publish hand-crafted events and read them back (exact)
make down           # stop the stack
```

### Record & replay harness

The pipeline is developed and tested against a **committed fixture of real Coinbase data**, so
tests and demos never depend on a live socket:

```bash
make replay   # offline: normalize fixtures/recorded_stream.jsonl -> Redpanda (no network/keys)
make record   # online: capture a fresh ~60s live sample to the fixture (the only socket use)
make produce  # online: stream live exchange data into Redpanda continuously (Ctrl-C to stop)
```

`record` is the *only* component that touches the exchange WebSocket; everything downstream
runs off the fixture. Normalization (raw exchange JSON → the `MarketEvent` contract) is a pure,
unit-tested function shared by the live producer and the replayer, so replay exercises the exact
same code path as production. Switch exchanges (Coinbase ↔ Binance.US) in
[`config/source.yaml`](config/source.yaml).

### Stream processing (windowing)

The processor ([`processing/app.py`](src/tickstream/processing/app.py)) is a **Quix Streams**
`StreamingDataFrame` pipeline that consumes `trades.raw` / `ticker.raw` and computes **tumbling
windows** per symbol, emitting closed windows to `metrics.windowed`:

- **1-minute and 5-minute** windows, **keyed by symbol** (windowing is per message key).
- **Event-time** based: a `timestamp_extractor` reads each record's `ts_event`, so a trade is
  bucketed by *when it happened*, not when it was consumed — out-of-order data lands in the
  correct window.
- **Watermarks + late data:** a `grace_ms` period tolerates out-of-order arrivals; records later
  than the grace are routed to an `on_late` hook (logged + dropped, not silently merged).
- **Metrics:** VWAP = Σ(price·size)/Σ(size), trade volume, trade count (from trades); average
  bid/ask spread and mid (from ticker).

The windowing math has a pure, socket-free twin in
[`processing/metrics.py`](src/tickstream/processing/metrics.py) that serves as its **exact test
oracle**: the integration test replays the fixture, runs the real Quix processor, and asserts the
streamed closed windows match the reference **field-for-field**. `make replay` then lands the raw
events to **bronze Parquet** and the windows to a Parquet dataset for the lakehouse.

### Lakehouse marts & SQL (medallion: bronze → silver → gold)

The SQL-forward half of the project. `make replay` continues past the lake sinks into the marts:

- **silver** — [dbt-duckdb](dbt/models/silver/) views over bronze Parquet: typed, validity-filtered,
  and **trade_id-deduplicated**.
- **gold** — [`gold_window_metrics`](dbt/models/gold/gold_window_metrics.sql): a dbt model that
  **re-aggregates silver into 1m/5m windows in pure SQL** (epoch-aligned `GROUP BY`, VWAP =
  `SUM(price*size)/SUM(size)`, trades `FULL OUTER JOIN` ticker). The SQL `window_start` macro matches
  the streaming oracle exactly, so **the batch SQL marts agree with the Quix streaming windows
  window-for-window** (asserted in tests).
- **Apache Iceberg** — the gold mart is materialized into an Iceberg table via
  [pyiceberg](src/tickstream/lake/iceberg.py) (local SQLite catalog + Parquet), written as two
  snapshots so **time travel** is demonstrable.
- **DuckDB SQL** — [`query/duck.py`](src/tickstream/query/duck.py) runs analytical SQL over the gold
  Iceberg table (e.g. a **windowed moving-average** of VWAP via `OVER (...)`) and reads a prior
  snapshot. `make query` shows both.

**Why Iceberg:** schema evolution, snapshots/**time travel** (query the table *as of* a prior build),
and compaction — the table format earns its place for the gold marts where history and evolution
matter, while bronze/silver stay plain Parquet.

```sql
-- `gold` is a DuckDB view over the gold Iceberg table's current snapshot (iceberg_scan).
-- 1-minute VWAP with a 3-window trailing moving average:
SELECT symbol, window_start, vwap,
       avg(vwap) OVER (PARTITION BY symbol ORDER BY window_start
                       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS vwap_ma3
FROM gold
WHERE window_size = '1m' ORDER BY symbol, window_start;
```

### Data contracts & SLAs

A formal, executable **data contract** ([quality/contract.py](src/tickstream/quality/contract.py),
[Pandera](https://pandera.readthedocs.io/)) defines what a valid normalized record is — schema +
types, `price > 0`, `size >= 0`, spread ≥ 0, non-null `symbol`/`ts_event`, known symbols. Records
that fail it are split off to a **quarantine path**:

- **Streaming guard:** a record that can't be normalized (a contract violation at the ingest
  boundary) is routed to the `contracts.quarantine` topic with its reason — **never** to the raw
  topics, so it cannot flow into bronze → silver → gold. `make replay` lands quarantined records to
  a Parquet table. (`make contracts` validates the landed bronze and reports the count.)
- Ordering (monotonic `ts_event` per symbol) is **flagged and counted**, not dropped.

Three **SLAs are measured and asserted against the replay** ([quality/sla.py](src/tickstream/quality/sla.py),
[tests/test_quality.py](tests/test_quality.py)):

| SLA | Target | Latest replay |
| --- | --- | --- |
| end-to-end latency (ingest → gold), p95 | < 60 s | **30.8 s** ✅ |
| expected windows produced per symbol | ≥ 99% | **100%** ✅ |
| contract violations reaching gold | 0 | **0** ✅ |

> **Library note:** the spec offered "Great Expectations **or Pandera if simpler**" — I chose
> **Pandera** for its clean row-level quarantine split (failing rows by index), which makes the
> "valid vs quarantined" boundary explicit and easy to test.

Requirements: Docker + `docker compose`, and [`uv`](https://github.com/astral-sh/uv).
The project pins **Python 3.11** (managed by uv). Ports used: Redpanda `19092`,
Console `8080`, dashboard `8502` (later). No ports clash with common local services.

### Useful commands

```bash
make install      # uv sync (core + dev deps)
make test-unit    # unit tests only — no broker required
make lint         # ruff check
make format       # ruff format
tickstream --help # CLI: health · topics-create · demo · record · replay · produce · process ·
                  #      bronze · build-marts · query · contracts · pipeline
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
| 2 | Real WebSocket producer (Coinbase + Binance.US fallback, reconnect/backoff) + `make record` / `make replay` harness | ✅ done |
| 3 | Quix Streams tumbling-window microstructure metrics (VWAP/spread/volume/count) on event time, with watermarks + late-data handling; bronze Parquet | ✅ done |
| 4 | dbt-duckdb silver/gold marts (SQL windowing) + Apache Iceberg gold tables + DuckDB SQL + time-travel | ✅ done |
| 5 | Data contracts (Pandera) + quarantine path + SLA assertions (p95 latency, % windows, 0 violations to gold) | ✅ done |
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
  producer/        # WebSocket clients, normalize, record, replay, live service
    exchanges/     #   Coinbase + Binance.US WS clients
    normalize.py   #   pure raw->MarketEvent normalization (unit-tested)
    record.py      #   capture live -> fixture   replay.py # fixture -> Redpanda
    service.py     #   live producer w/ reconnect+backoff
  processing/      # Quix Streams windowing (app.py) + pure metrics oracle (metrics.py)
  lake/            # bronze.py / windows.py Parquet sinks; marts.py (dbt) + iceberg.py (gold)
  query/           # duck.py — DuckDB SQL over gold Iceberg + time travel
  quality/         # contract.py (Pandera) + quarantine.py + sla.py
  pipeline.py      # `tickstream pipeline` — replay -> bronze -> windows -> marts -> contracts/SLA
dbt/               # dbt-duckdb project: silver views + gold_window_metrics (SQL) + tests
  quality/         # (Phase 5) contracts + quarantine + SLAs
  query/ ui/       # (Phase 4/6) DuckDB queries + Streamlit dashboard
config/source.yaml # exchange / symbols / channels
docker-compose.yml # Redpanda + Console (+ later services)
Makefile           # up / down / test / demo / record / replay / lint / format
tests/             # pytest (unit + broker integration)
```

## License

MIT. Market data belongs to the respective exchanges and is not redistributed.
