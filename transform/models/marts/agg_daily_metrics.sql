-- Daily totals per agent, by the day the PR was opened (UTC).
with daily as (
    select
        cast(opened_at as date) as opened_date,
        agent,
        count(*) as prs_opened
    from {{ ref('fct_prs') }}
    group by 1, 2
)

select
    opened_date,
    agent,
    prs_opened,
    round(prs_opened / sum(prs_opened) over (partition by opened_date), 3) as share_of_day,
    capture.capture_quality
from daily
left join {{ ref('agg_daily_capture') }} as capture on daily.opened_date = capture.day
