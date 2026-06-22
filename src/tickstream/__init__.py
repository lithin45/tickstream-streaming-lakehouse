"""TickStream — a real-time streaming lakehouse for crypto market microstructure.

Medallion architecture: live exchange WebSocket -> Redpanda -> Quix Streams windowing
-> bronze/silver Parquet -> gold Apache Iceberg -> DuckDB / Streamlit.
"""

__version__ = "0.1.0"
