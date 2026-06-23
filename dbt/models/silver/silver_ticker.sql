-- Silver ticker: typed, validity-filtered (no crossed books), with spread + mid derived.
-- Source = bronze Parquet (raw normalized ticker events).

with src as (
    select *
    from read_parquet(
        '{{ var("lake_root") }}/bronze/**/*.parquet',
        hive_partitioning = true
    )
    where event_type = 'ticker'
      and best_bid is not null
      and best_ask is not null
      and best_ask >= best_bid
)

select
    exchange,
    symbol,
    best_bid,
    best_ask,
    best_bid_size,
    best_ask_size,
    (best_ask - best_bid) as spread,
    (best_ask + best_bid) / 2.0 as mid,
    ts_event,
    ts_ingest
from src
