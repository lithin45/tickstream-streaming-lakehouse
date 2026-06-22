"""Stream-processing layer (Quix Streams).

Phase 3: consumes ``trades.raw`` and computes 1-/5-min tumbling-window microstructure
metrics (VWAP, spread, volume, trade-count) per symbol, with watermarks/late-data
handling, emitting to ``metrics.windowed`` and bronze Parquet.
"""
