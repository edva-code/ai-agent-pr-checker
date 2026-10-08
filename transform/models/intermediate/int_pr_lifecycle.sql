-- One row per pull request: what we saw happen to it inside the loaded window.
-- PRs opened before the window have opened_at = null and are left out of the marts.
with events as (
    select * from {{ ref('stg_pr_events') }}
),

opened as (
    select pr_id, actor_login as opened_by
    from events
    where event_type = 'PullRequestEvent' and action = 'opened'
    qualify row_number() over (partition by pr_id order by created_at) = 1
)

select
    events.pr_id,
    any_value(events.repo_name) as repo_name,
    any_value(events.pr_number) as pr_number,
    any_value(events.head_ref) as head_ref,
    any_value(events.base_ref) as base_ref,
    any_value(events.head_repo_id) is distinct from any_value(events.base_repo_id) as is_fork,
    any_value(opened.opened_by) as opened_by,
    min(events.created_at) filter (where action = 'opened') as opened_at,
    min(events.created_at) filter (where action = 'merged') as merged_at,
    min(events.created_at) filter (
        where event_type = 'PullRequestEvent' and action = 'closed'
    ) as closed_at,
    count(*) filter (where action = 'reopened') as reopen_count,
    count(*) filter (
        where event_type = 'PullRequestReviewEvent' and action = 'created'
    ) as review_count,
    count(*) filter (where review_state = 'approved') as approval_count,
    min(events.created_at) as first_seen_at,
    max(events.created_at) as last_seen_at
from events
left join opened on events.pr_id = opened.pr_id
group by events.pr_id
