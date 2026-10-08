-- Daily roll-up of capture_health, so daily metrics can say how much to trust a day.
select
    cast(hour as date) as day,
    count(*) filter (where has_file) as hours_present,
    count(*) filter (where capture_flag = 'low') as hours_low,
    case
        when count(*) filter (where has_file) < 24 then 'partial'
        when count(*) filter (where capture_flag = 'low') > 6 then 'low'
        else 'ok'
    end as capture_quality
from {{ ref('capture_health') }}
group by 1
