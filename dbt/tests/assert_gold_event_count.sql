-- event_count must equal trade_count + ticker_count. Returns offending rows.
select symbol, window_size, window_start, event_count, trade_count, ticker_count
from {{ ref("gold_window_metrics") }}
where event_count != trade_count + ticker_count
