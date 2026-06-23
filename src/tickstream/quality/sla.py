"""SLA measurement, asserted against the replay.

Three SLAs from the spec:

* **end-to-end latency p95 < 60 s** (ingest -> gold): for each landed bronze record, the time
  from its ``ts_ingest`` (set when the producer/replay published it) to the moment gold was built;
* **>= 99% of expected windows produced PER SYMBOL**: gold windows vs the windows the bronze
  event-time span implies (the worst-performing symbol must still clear the bar);
* **0 contract violations reaching gold**: the gold marts themselves satisfy their invariants
  (non-negative spread, positive VWAP where there are trades, consistent event_count).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime

import numpy as np
import pandas as pd
from pydantic import BaseModel

from tickstream.config import Settings, get_settings
from tickstream.lake.bronze import read_bronze
from tickstream.lake.iceberg import read_gold
from tickstream.processing.metrics import WINDOW_SIZES

LATENCY_TARGET_S = 60.0
COMPLETENESS_TARGET_PCT = 99.0


class SLAReport(BaseModel):
    latency_p95_s: float
    latency_target_s: float = LATENCY_TARGET_S
    windows_produced: int
    windows_expected: int
    completeness_pct: float  # the WORST symbol's completeness
    completeness_by_symbol: dict[str, float]
    completeness_target_pct: float = COMPLETENESS_TARGET_PCT
    gold_violations: int

    @property
    def latency_ok(self) -> bool:
        return self.latency_p95_s < self.latency_target_s

    @property
    def completeness_ok(self) -> bool:
        return self.completeness_pct >= self.completeness_target_pct

    @property
    def violations_ok(self) -> bool:
        return self.gold_violations == 0

    @property
    def passed(self) -> bool:
        return self.latency_ok and self.completeness_ok and self.violations_ok


def _silver_valid(bronze: pd.DataFrame) -> pd.DataFrame:
    """Rows that survive silver's filters (so the baseline matches gold's population)."""
    is_trade = bronze["event_type"] == "trade"
    is_ticker = bronze["event_type"] == "ticker"
    trade_ok = is_trade & bronze["trade_id"].notna() & (bronze["price"] > 0) & (bronze["size"] >= 0)
    ticker_ok = (
        is_ticker
        & bronze["best_bid"].notna()
        & bronze["best_ask"].notna()
        & (bronze["best_ask"] >= bronze["best_bid"])
    )
    return bronze[trade_ok | ticker_ok]


def _expected_windows_by_symbol(bronze: pd.DataFrame) -> dict[str, int]:
    """Distinct (symbol, size, window_start) buckets implied by the silver-valid bronze span."""
    valid = _silver_valid(bronze)
    out: dict[str, int] = defaultdict(int)
    if valid.empty:
        return dict(out)
    epoch = (
        (valid["ts_event"] - pd.Timestamp("1970-01-01", tz="UTC"))
        .dt.total_seconds()
        .astype("int64")
    )
    for secs in WINDOW_SIZES.values():
        bucket = (epoch // secs) * secs
        distinct = pd.DataFrame(
            {"symbol": valid["symbol"].to_numpy(), "b": bucket.to_numpy()}
        ).drop_duplicates()
        for sym, n in distinct.groupby("symbol").size().items():
            out[str(sym)] += int(n)
    return dict(out)


def _gold_violations(gold: pd.DataFrame) -> int:
    """Count gold rows that breach the contract invariants (a true check on the gold output)."""
    if gold.empty:
        return 0
    bad_spread = (gold["ticker_count"] > 0) & (gold["avg_spread"] < 0)
    bad_vwap = (gold["trade_count"] > 0) & (gold["vwap"].isna() | (gold["vwap"] <= 0))
    bad_count = gold["event_count"] != (gold["trade_count"] + gold["ticker_count"])
    return int((bad_spread | bad_vwap | bad_count).sum())


def measure_sla(settings: Settings | None = None, *, gold_built_at: datetime) -> SLAReport:
    """Compute the three SLAs from the landed lake. ``gold_built_at`` anchors the latency clock."""
    settings = settings or get_settings()
    bronze = read_bronze(settings).to_pandas()
    gold = read_gold(settings).to_pandas()

    # --- latency p95 (ingest -> gold) ---
    built = pd.Timestamp(gold_built_at).tz_convert("UTC")
    latencies = (built - bronze["ts_ingest"]).dt.total_seconds().to_numpy()
    latency_p95 = float(np.percentile(latencies, 95)) if len(latencies) else 0.0

    # --- per-symbol completeness; the SLA is gated on the worst symbol ---
    expected_by_symbol = _expected_windows_by_symbol(bronze)
    produced_by_symbol = gold.groupby("symbol").size().to_dict()
    by_symbol = {
        sym: round(100.0 * produced_by_symbol.get(sym, 0) / exp, 2)
        for sym, exp in expected_by_symbol.items()
    }
    completeness = min(by_symbol.values()) if by_symbol else 100.0

    return SLAReport(
        latency_p95_s=round(latency_p95, 3),
        windows_produced=len(gold),
        windows_expected=sum(expected_by_symbol.values()),
        completeness_pct=completeness,
        completeness_by_symbol=by_symbol,
        gold_violations=_gold_violations(gold),
    )
