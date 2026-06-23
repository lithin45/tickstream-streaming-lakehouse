-- silver_trades is contractually deduplicated by (symbol, trade_id). Returns any duplicate
-- groups (0 = pass) — guards the dedup CTE against silent regression.
select symbol, trade_id, count(*) as n
from {{ ref("silver_trades") }}
group by symbol, trade_id
having count(*) > 1
