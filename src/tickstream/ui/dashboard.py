"""TickStream dashboard (Streamlit) — live microstructure metrics over the gold Iceberg marts.

Shows per-symbol VWAP / spread / volume from the gold windowed marts, the SLA result from the
latest pipeline run, and an Apache Iceberg **time-travel** example. Run with::

    make dashboard      # or: streamlit run src/tickstream/ui/dashboard.py

Populate the lake first with ``make replay`` (or ``make produce`` + ``make process`` for live).
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from tickstream.config import get_settings

st.set_page_config(page_title="TickStream", page_icon="📈", layout="wide")
settings = get_settings()

st.title("📈 TickStream — crypto market microstructure")
st.caption(
    "Redpanda → Quix Streams tumbling windows → bronze/silver → **gold Apache Iceberg** → "
    "DuckDB SQL. Data contracts quarantine bad records; SLAs are asserted against the replay."
)

refresh_s = st.sidebar.slider("Auto-refresh (seconds, 0 = off)", 0, 30, 0)
st.sidebar.caption("Run `make replay` to (re)populate the lake, then the panels update on refresh.")


def _sla_panel() -> None:
    result_path = Path(settings.runtime.lake_root) / "pipeline_result.json"
    if not result_path.exists():
        return
    r = json.loads(result_path.read_text())
    st.subheader("SLA — last pipeline run")
    c = st.columns(4)
    c[0].metric(
        "Latency p95", f"{r['latency_p95_s']} s", "≤ 60 s" if r["latency_p95_s"] < 60 else "BREACH"
    )
    c[1].metric(
        "Windows / symbol",
        f"{r['completeness_pct']} %",
        "≥ 99%" if r["completeness_pct"] >= 99 else "BREACH",
    )
    c[2].metric(
        "Violations → gold", r["gold_violations"], "= 0" if r["gold_violations"] == 0 else "BREACH"
    )
    c[3].metric("Quarantined", r["quarantined"])
    (st.success if r["sla_passed"] else st.error)(
        "✅ All SLAs PASS" if r["sla_passed"] else "❌ SLA breach"
    )


@st.fragment(run_every=refresh_s if refresh_s else None)
def _metrics_panel() -> None:
    from tickstream.lake.iceberg import read_gold
    from tickstream.query.duck import time_travel

    try:
        gold = read_gold(settings).to_pandas()
    except Exception:
        st.warning("No gold data yet — run `make replay` to populate the lakehouse.")
        return
    if gold.empty:
        st.warning("Gold table is empty — run `make replay`.")
        return

    gold = gold.sort_values("window_start")
    one_min = gold[gold["window_size"] == "1m"]
    if one_min.empty:
        st.warning("No 1-minute windows yet — run `make replay`.")
        return

    st.subheader("Latest 1-minute metrics per symbol")
    latest = one_min.groupby("symbol").tail(1)
    cols = st.columns(len(latest))
    for col, (_, row) in zip(cols, latest.iterrows(), strict=False):
        # vwap/avg_spread are NaN (not None) for ticker-only / trade-only windows.
        col.metric(
            row["symbol"],
            f"VWAP {row['vwap']:,.2f}" if pd.notna(row["vwap"]) else "—",
            f"spread {row['avg_spread']:.4f}" if pd.notna(row["avg_spread"]) else "",
        )

    trades = one_min[one_min["trade_count"] > 0]
    ticks = one_min[one_min["ticker_count"] > 0]
    st.plotly_chart(
        px.line(trades, x="window_start", y="vwap", color="symbol", title="VWAP (1-min windows)"),
        use_container_width=True,
    )
    left, right = st.columns(2)
    left.plotly_chart(
        px.bar(trades, x="window_start", y="trade_volume", color="symbol", title="Trade volume"),
        use_container_width=True,
    )
    right.plotly_chart(
        px.line(
            ticks, x="window_start", y="avg_spread", color="symbol", title="Avg bid/ask spread"
        ),
        use_container_width=True,
    )

    tt = time_travel(settings)
    st.subheader("🕰️ Apache Iceberg time-travel")
    st.write(
        f"The gold table has **{len(tt['snapshots'])} snapshots**. "
        f"Current = **{tt['current_rows']} rows**; the first snapshot (before the 5-minute "
        f"windows were appended) had **{tt.get('first_snapshot_rows', 0)} rows** — "
        "queried *as of* that prior snapshot via pyiceberg."
    )


_sla_panel()
_metrics_panel()
