# Fixtures

Recorded market-data samples used for **deterministic, offline tests and replay**.

- `make record` (Phase 2) connects to the live exchange WebSocket for a short window and
  writes the raw message stream here as `recorded_stream.jsonl`.
- `make replay` (Phase 2+) feeds a committed fixture through Redpanda → the stream
  processor → the lake → DuckDB, reproducing the entire pipeline with **no network**.

Only small samples are committed. The raw exchange feed is **not redistributed**.
