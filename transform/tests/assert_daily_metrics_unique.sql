-- Exactly one row per day and agent.
select opened_date, agent, count(*) as n
from {{ ref('agg_daily_metrics') }}
group by all
having count(*) > 1
