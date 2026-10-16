-- Daily roll-up of capture_health, so daily metrics can say how much to trust a day.
-- capture_warning lists every rule that fired; the day's numbers are still shown.
with hours as (
    select
        health.*,
        monthly.volume_low
    from {{ ref('capture_health') }} as health
    left join {{ ref('capture_monthly_volume') }} as monthly
        on health.source = monthly.source
        and date_trunc('month', health.hour) = monthly.month
),

daily as (
    select
        cast(hour as date) as day,
        count(*) filter (where has_file) as hours_present,
        count(*) filter (where capture_flag = 'low') as hours_low,
        sum(events_seen) as events_seen,
        sum(pr_events) as pr_events,
        coalesce(bool_or(volume_low), false) as volume_low
    from hours
    group by 1
),

rules as (
    select
        daily.*,
        case
            when hours_present < 24 then 'partial'
            when hours_low > 6 then 'low'
            else 'ok'
        end as capture_quality,
        round(pr_events / events_seen, 4) as pr_share,
        coalesce(pr_events / events_seen < 0.02, false) as pr_share_low
    from daily
)

select
    *,
    nullif(concat_ws('; ',
        case when volume_low then 'event volume below 75% of the previous 3 good months' end,
        case when pr_share_low then 'PR events under 2% of all events' end
    ), '') as capture_warning
from rules
