-- Independently re-derive the trade-side metrics from silver and flag any window where gold
-- disagrees. Catches a VWAP-formula change, a FULL-OUTER-JOIN fan-out (inflated trade_count /
-- volume), or a grain bug — none of which the structural tests would notice. Returns offending
-- rows (0 = pass).

{% set sizes = [{"label": "1m", "secs": 60}, {"label": "5m", "secs": 300}] %}

with recomputed as (
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
)

select g.symbol, g.window_size, g.window_start
from {{ ref("gold_window_metrics") }} as g
inner join recomputed as r
    on g.symbol = r.symbol
    and g.window_size = r.window_size
    and g.window_start = r.window_start
where g.trade_count != r.trade_count
    or abs(g.trade_volume - r.trade_volume) > 1e-9
    or abs(coalesce(g.vwap, 0) - coalesce(r.vwap, 0)) > 1e-6
