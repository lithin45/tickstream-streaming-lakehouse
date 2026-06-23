"""The data contract (Pandera) + row-level quarantine split.

This is the formal, executable definition of a valid normalized record:

* **schema**: required fields present, correct types;
* **validity**: price > 0, size >= 0, spread (best_ask - best_bid) >= 0;
* **keys**: non-null symbol and ts_event;
* **membership**: symbol is one of the configured symbols, event_type in {trade, ticker}.

``apply_contract`` validates a DataFrame and splits it into the rows that pass and the rows
that fail (with reasons) — the latter are the quarantine set. Ordering (monotonic ts_event per
symbol) is *flagged and counted* separately via :func:`count_out_of_order` rather than
quarantined, per the spec ("handle/flag out-of-order").
"""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera import Check, Column
from pydantic import BaseModel

from tickstream.config import Settings, get_settings
from tickstream.schema import MarketEvent

# Columns the contract operates on (a normalized record).
_FLOAT_COLS = ["price", "size", "best_bid", "best_ask", "best_bid_size", "best_ask_size"]


class ContractResult(BaseModel):
    """Outcome of validating a batch of records against the contract."""

    model_config = {"arbitrary_types_allowed": True}

    valid: pd.DataFrame
    quarantined: pd.DataFrame
    n_valid: int
    n_quarantined: int
    reasons: list[str]

    @property
    def ok(self) -> bool:
        return self.n_quarantined == 0


def _spread_non_negative(df: pd.DataFrame) -> pd.Series:
    """best_ask >= best_bid wherever both are present (else pass)."""
    both = df["best_bid"].notna() & df["best_ask"].notna()
    return (~both) | (df["best_ask"] >= df["best_bid"])


def _trade_has_price_and_size(df: pd.DataFrame) -> pd.Series:
    """A trade must carry a positive price and a non-negative size."""
    is_trade = df["event_type"] == "trade"
    return (~is_trade) | (df["price"].notna() & df["size"].notna())


def build_contract(symbols: list[str]) -> pa.DataFrameSchema:
    """Build the Pandera contract schema, parameterized by the allowed symbol set."""
    return pa.DataFrameSchema(
        {
            "exchange": Column(str, nullable=False),
            "symbol": Column(str, Check.isin(symbols), nullable=False),
            "event_type": Column(str, Check.isin(["trade", "ticker"]), nullable=False),
            "price": Column(float, Check.gt(0), nullable=True),
            "size": Column(float, Check.ge(0), nullable=True),
            "best_bid": Column(float, Check.ge(0), nullable=True),
            "best_ask": Column(float, Check.ge(0), nullable=True),
            "best_bid_size": Column(float, Check.ge(0), nullable=True),
            "best_ask_size": Column(float, Check.ge(0), nullable=True),
            # Coerce ONLY the timestamps (us<->ns resolution) so the dtype check actually fires
            # on real data instead of failing as a swallowed unindexed schema error. NOT
            # coerced at schema level, so a None in a string key still trips not_nullable.
            "ts_event": Column("datetime64[ns, UTC]", nullable=False, coerce=True),
            "ts_ingest": Column("datetime64[ns, UTC]", nullable=False, coerce=True),
        },
        checks=[
            Check(_spread_non_negative, name="spread_non_negative"),
            Check(_trade_has_price_and_size, name="trade_has_price_and_size"),
        ],
        strict=False,  # extra columns (side, trade_id) are allowed
        coerce=False,
    )


def events_to_frame(events: list[MarketEvent]) -> pd.DataFrame:
    """Build a contract-shaped DataFrame (correct dtypes) from normalized events."""
    df = pd.DataFrame([e.model_dump() for e in events])
    for col in _FLOAT_COLS:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    for col in ("ts_event", "ts_ingest"):
        df[col] = pd.to_datetime(df[col], utc=True)
    return df


def apply_contract(df: pd.DataFrame, settings: Settings | None = None) -> ContractResult:
    """Validate ``df`` against the contract, splitting valid rows from quarantined rows."""
    settings = settings or get_settings()
    schema = build_contract(list(settings.source.symbols))
    # Positional index so the split aligns with Pandera's positional failure_cases["index"]
    # (and is safe against any caller passing a duplicate-label/filtered frame).
    df = df.reset_index(drop=True)
    try:
        schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        failures = exc.failure_cases.dropna(subset=["index"])
        # Per-row reason: the distinct failed checks for that row.
        reason_by_index = (
            failures.groupby("index")["check"]
            .agg(lambda s: ", ".join(sorted(set(s.astype(str)))))
            .to_dict()
        )
        bad_index = set(reason_by_index)
        quarantined = df.loc[df.index.isin(bad_index)].copy()
        quarantined["quarantine_reason"] = [reason_by_index[i] for i in quarantined.index]
        valid = df.loc[~df.index.isin(bad_index)]
        return ContractResult(
            valid=valid,
            quarantined=quarantined,
            n_valid=len(valid),
            n_quarantined=len(quarantined),
            reasons=sorted({str(c) for c in failures["check"]}),
        )
    return ContractResult(
        valid=df, quarantined=df.iloc[0:0], n_valid=len(df), n_quarantined=0, reasons=[]
    )


def count_out_of_order(df: pd.DataFrame) -> int:
    """Count records whose ts_event is earlier than a prior record for the same symbol."""
    if df.empty:
        return 0
    out = 0
    for _symbol, group in df.groupby("symbol", sort=False):
        prev = None
        for ts in group["ts_event"]:
            if prev is not None and ts < prev:
                out += 1
            else:
                prev = ts
    return out
