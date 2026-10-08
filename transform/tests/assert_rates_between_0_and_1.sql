select opened_date, agent, merge_rate, share_of_day
from {{ ref('agg_daily_metrics') }}
where merge_rate not between 0 and 1 or share_of_day not between 0 and 1
