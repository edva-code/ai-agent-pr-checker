select opened_date, agent, share_of_day
from {{ ref('agg_daily_metrics') }}
where share_of_day not between 0 and 1
