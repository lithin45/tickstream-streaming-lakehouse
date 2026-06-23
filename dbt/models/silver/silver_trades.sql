-- Silver trades: typed, validity-filtered, and deduplicated by trade_id.
-- Source = bronze Parquet (raw normalized trade events), read straight from the lake.

with src as (
    select *
    from read_parquet(
        '{{ var("lake_root") }}/bronze/**/*.parquet',
        hive_partitioning = true
    )
    where event_type = 'trade'
      and price > 0
      and size >= 0
      and trade_id is not null  -- dedup is keyed on trade_id; drop the rare id-less trade
),

deduped as (
    select
        *,
        row_number() over (
            partition by symbol, trade_id
            -- deterministic tie-break so the surviving row is stable across runs
            order by ts_event, ts_ingest, price, size
        ) as _rn
    from src
)

select
    exchange,
    symbol,
    trade_id,
    price,
    size,
    side,
    ts_event,
    ts_ingest
from deduped
where _rn = 1
