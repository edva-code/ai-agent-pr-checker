-- Daily totals per agent, by the day the PR was opened (UTC).
-- Recent days look worse: their PRs have had less time to be merged.
with daily as (
    select
        cast(opened_at as date) as opened_date,
        agent,
        count(*) as prs_opened,
        count(*) filter (where status = 'merged') as prs_merged,
        count(*) filter (where status = 'closed') as prs_closed_unmerged,
        median(hours_to_merge) as median_hours_to_merge
    from {{ ref('fct_prs') }}
    group by 1, 2
)

select
    opened_date,
    agent,
    prs_opened,
    prs_merged,
    prs_closed_unmerged,
    round(prs_merged / prs_opened, 3) as merge_rate,
    round(median_hours_to_merge, 2) as median_hours_to_merge,
    round(prs_opened / sum(prs_opened) over (partition by opened_date), 3) as share_of_day,
    capture.capture_quality
from daily
left join {{ ref('agg_daily_capture') }} as capture on daily.opened_date = capture.day
