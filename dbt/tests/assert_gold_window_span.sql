-- The window must span exactly its size (60s for 1m, 300s for 5m). Returns offending rows.
select symbol, window_size, window_start, window_end
from {{ ref("gold_window_metrics") }}
where epoch(window_end) - epoch(window_start)
    != case window_size when '1m' then 60 when '5m' then 300 end
