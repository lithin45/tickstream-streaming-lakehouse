"""Data-quality / contracts layer.

Phase 5: Great Expectations suite enforcing schema + value contracts with a quarantine
path for violations, plus SLA measurement (p95 latency, % windows produced, 0 violations
reaching gold) asserted against the replay.
"""
