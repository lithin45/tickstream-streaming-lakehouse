"""Lakehouse layer (medallion).

Bronze/silver are plain Parquet; gold is Apache Iceberg via pyiceberg with a local
SQLite catalog. Phase 4 builds silver/gold marts with dbt-duckdb and demonstrates an
Iceberg time-travel query.
"""
