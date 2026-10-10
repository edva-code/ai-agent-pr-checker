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

prs as (
    select
        -- Count by the hour file the event came from, so numerator and denominator share files
        cast(source_hour as date) as day,
        count(*) filter (where event_type = 'PullRequestEvent') as pr_events,
        count(*) filter (where action = 'opened') as prs_opened,
        count(*) filter (where action = 'merged') as prs_merged
    from {{ ref('stg_pr_events') }}
    group by 1
),

daily as (
    select
        cast(hour as date) as day,
        count(*) filter (where has_file) as hours_present,
        count(*) filter (where capture_flag = 'low') as hours_low,
        sum(events_seen) as events_seen,
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
        round(prs.pr_events / daily.events_seen, 4) as pr_share,
        coalesce(prs.pr_events / daily.events_seen < 0.02, false) as pr_share_low,
        coalesce(prs.prs_opened > 0 and prs.prs_merged = 0, false) as no_merges
    from daily
    left join prs using (day)
)

select
    *,
    nullif(concat_ws('; ',
        case when volume_low then 'event volume below 75% of the previous 3 good months' end,
        case when pr_share_low then 'PR events under 2% of all events' end,
        case when no_merges then 'PRs opened but no merges captured' end
    ), '') as capture_warning
from rules
