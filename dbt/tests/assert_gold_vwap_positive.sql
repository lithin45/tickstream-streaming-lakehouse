-- A window with trades must have a positive VWAP. Returns offending rows (0 = pass).
select symbol, window_size, window_start, vwap, trade_count
from {{ ref("gold_window_metrics") }}
where trade_count > 0
  and (vwap is null or vwap <= 0)
