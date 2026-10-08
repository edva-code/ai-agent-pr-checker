-- One row per PR opened inside the loaded window.
with lifecycle as (
    select * from {{ ref('int_pr_lifecycle') }}
),

detection as (
    select * from {{ ref('int_agent_detection') }}
)

select
    lifecycle.pr_id,
    lifecycle.repo_name,
    lifecycle.pr_number,
    lifecycle.head_ref,
    lifecycle.base_ref,
    lifecycle.opened_by,
    detection.agent,
    detection.matched_by,
    detection.is_agent,
    lifecycle.is_fork,
    lifecycle.opened_at,
    lifecycle.merged_at,
    lifecycle.closed_at,
    case
        when lifecycle.merged_at is not null then 'merged'
        when lifecycle.closed_at is not null then 'closed'
        else 'open'
    end as status,
    date_diff('minute', lifecycle.opened_at, lifecycle.merged_at) / 60.0 as hours_to_merge,
    lifecycle.reopen_count,
    lifecycle.review_count,
    lifecycle.approval_count
from lifecycle
inner join detection on lifecycle.pr_id = detection.pr_id
where lifecycle.opened_at is not null
