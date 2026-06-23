-- Gold: windowed microstructure marts (the analytical SQL showcase).
-- Re-aggregates silver into 1-minute and 5-minute tumbling windows per symbol, then joins the
-- trade-side metrics (VWAP/volume/count) with the ticker-side metrics (avg spread/mid). The
-- epoch-aligned window_start macro matches the streaming oracle exactly, so this batch SQL mart
-- and the Quix Streams windows agree window-for-window.

{% set sizes = [{"label": "1m", "secs": 60}, {"label": "5m", "secs": 300}] %}

with trade_windows as (
    {% for s in sizes %}
    select
        symbol,
        '{{ s.label }}' as window_size,
        {{ window_start("ts_event", s.secs) }} as window_start,
        sum(price * size) / nullif(sum(size), 0) as vwap,
        sum(size) as trade_volume,
        count(*) as trade_count
    from {{ ref("silver_trades") }}
    group by 1, 2, 3
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

ticker_windows as (
    {% for s in sizes %}
    select
        symbol,
        '{{ s.label }}' as window_size,
        {{ window_start("ts_event", s.secs) }} as window_start,
        avg(spread) as avg_spread,
        avg(mid) as avg_mid,
        count(*) as ticker_count
    from {{ ref("silver_ticker") }}
    group by 1, 2, 3
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

joined as (
    select
        coalesce(t.symbol, k.symbol) as symbol,
        coalesce(t.window_size, k.window_size) as window_size,
        coalesce(t.window_start, k.window_start) as window_start,
        t.vwap,
        coalesce(t.trade_volume, 0) as trade_volume,
        coalesce(t.trade_count, 0) as trade_count,
        k.avg_spread,
        k.avg_mid,
        coalesce(k.ticker_count, 0) as ticker_count
    from trade_windows as t
    full outer join ticker_windows as k
        on t.symbol = k.symbol
        and t.window_size = k.window_size
        and t.window_start = k.window_start
)

select
    symbol,
    window_size,
    window_start,
    window_start + (
        case window_size when '1m' then interval '60 seconds' else interval '300 seconds' end
    ) as window_end,
    vwap,
    trade_volume,
    trade_count,
    avg_spread,
    avg_mid,
    ticker_count,
    trade_count + ticker_count as event_count,
    -- epoch (not cast-to-varchar, which renders in the session timezone) keeps the key stable.
    symbol || '|' || window_size || '|' || cast(epoch(window_start) as bigint) as window_key
from joined
