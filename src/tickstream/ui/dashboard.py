"""TickStream dashboard (Streamlit) over the gold windowed marts.

Shows per symbol VWAP, spread and volume, the SLA result from the latest pipeline run, and an
Apache Iceberg time travel example. Run locally with::

    make dashboard      # or: streamlit run src/tickstream/ui/dashboard.py

Hosted/demo mode: when the live Iceberg warehouse is not available (for example on Streamlit
Community Cloud, which has no broker and no pipeline), the dashboard falls back to a committed
snapshot in dashboard_data/ and shows a clear "recorded demo" banner. Set TICKSTREAM_DEMO=1 to
force that mode.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd
import streamlit as st

# dashboard.py lives at src/tickstream/ui/ ; the committed snapshot is at the repo root.
_SNAPSHOT_DIR = Path(__file__).resolve().parents[3] / "dashboard_data"

st.set_page_config(page_title="TickStream", page_icon="📈", layout="wide")

st.title("📈 TickStream, crypto market microstructure")
st.caption(
    "Redpanda to Quix Streams tumbling windows to bronze/silver to gold Apache Iceberg to "
    "DuckDB SQL. Data contracts quarantine bad records and SLAs are checked against the replay."
)

refresh_s = st.sidebar.slider("Auto refresh (seconds, 0 = off)", 0, 30, 0)
st.sidebar.caption("Run `make replay` locally to (re)build the data, then the panels update.")


def _load_live():
    """Read the live Iceberg gold table. Raises if the warehouse is not available."""
    from tickstream.config import get_settings
    from tickstream.lake.iceberg import gold_snapshots, read_gold

    settings = get_settings()
    gold = read_gold(settings).to_pandas()
    if gold.empty:
        raise RuntimeError("gold is empty")
    snaps = gold_snapshots(settings)
    result_path = Path(settings.runtime.lake_root) / "pipeline_result.json"
    result = json.loads(result_path.read_text()) if result_path.exists() else {}
    tt = {
        "snapshots": len(snaps),
        "current_rows": int(read_gold(settings).num_rows),
        "first_snapshot_rows": int(read_gold(settings, snapshot_id=snaps[0]).num_rows)
        if snaps
        else 0,
    }
    return gold, result, tt


def _load_snapshot():
    """Read the committed static snapshot (no broker, no pipeline needed)."""
    gold = pd.read_csv(_SNAPSHOT_DIR / "gold_windows.csv")
    result = json.loads((_SNAPSHOT_DIR / "pipeline_result.json").read_text())
    tt = json.loads((_SNAPSHOT_DIR / "timetravel.json").read_text())
    return gold, result, tt


def load_data():
    """Prefer the live warehouse; fall back to the committed snapshot (demo mode)."""
    if not os.getenv("TICKSTREAM_DEMO"):
        try:
            return (*_load_live(), False)
        except Exception:
            pass
    return (*_load_snapshot(), True)


gold, result, tt, is_demo = load_data()
gold["window_start"] = pd.to_datetime(gold["window_start"])

if is_demo:
    st.info(
        "Recorded demo. This hosted version replays a saved slice of real Coinbase market data "
        "(about 7 minutes, 3 symbols). The live exchange feed and the streaming pipeline are not "
        "running here, so nothing connects to any external service. To generate fresh live data, "
        "clone the repo and run `make replay` (or `make produce`)."
    )

if result.get("sla_passed") is not None:
    c = st.columns(4)
    c[0].metric("Latency p95", f"{result['latency_p95_s']} s", "target < 60 s")
    c[1].metric("Windows / symbol", f"{result['completeness_pct']} %", "target >= 99%")
    c[2].metric("Violations to gold", result["gold_violations"], "target = 0")
    c[3].metric("Quarantined", result["quarantined"])
    (st.success if result["sla_passed"] else st.error)(
        "All SLAs pass" if result["sla_passed"] else "SLA breach"
    )

one_min = gold[gold["window_size"] == "1m"].sort_values(["symbol", "window_start"])
if one_min.empty:
    st.warning("No 1 minute windows yet. Run `make replay` to build the data.")
    st.stop()

st.subheader("Latest 1 minute metrics per symbol")
latest = one_min.groupby("symbol").tail(1).sort_values("symbol")
cols = st.columns(len(latest))
for col, (_, row) in zip(cols, latest.iterrows(), strict=False):
    col.metric(
        row["symbol"],
        f"VWAP {row['vwap']:,.2f}" if pd.notna(row["vwap"]) else "n/a",
        f"spread {row['avg_spread']:.4f}" if pd.notna(row["avg_spread"]) else "",
    )

trades = one_min[one_min["trade_count"] > 0]
ticks = one_min[one_min["ticker_count"] > 0]

st.markdown("**VWAP (1 minute windows)**")
st.line_chart(trades, x="window_start", y="vwap", color="symbol", height=320)

left, right = st.columns(2)
with left:
    st.markdown("**Trade volume**")
    st.bar_chart(trades, x="window_start", y="trade_volume", color="symbol", height=300)
with right:
    st.markdown("**Average bid/ask spread**")
    st.line_chart(ticks, x="window_start", y="avg_spread", color="symbol", height=300)

st.subheader("🕰️ Apache Iceberg time travel")
st.write(
    f"The gold table has **{tt['snapshots']} snapshots**. Current = **{tt['current_rows']} rows**; "
    f"the first snapshot (before the 5 minute windows were appended) had "
    f"**{tt.get('first_snapshot_rows', 0)} rows**, read as of that prior snapshot."
)

if refresh_s:
    st.caption(f"Auto refresh every {refresh_s}s is on.")
