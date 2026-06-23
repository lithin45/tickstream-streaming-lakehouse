"""Data-quality / contracts layer.

A Pandera contract enforcing schema + value contracts with a quarantine path for violations,
plus SLA measurement (p95 latency, % windows produced per symbol, 0 violations reaching gold)
asserted against the replay.
"""
