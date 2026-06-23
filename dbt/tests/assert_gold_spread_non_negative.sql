-- A window with ticker data must have a non-negative average spread. Returns offending rows.
select symbol, window_size, window_start, avg_spread, ticker_count
from {{ ref("gold_window_metrics") }}
where ticker_count > 0
  and avg_spread < 0
