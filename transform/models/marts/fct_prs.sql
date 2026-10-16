-- One row per PR opened inside the loaded window.
with openings as (
    select * from {{ ref('stg_pr_events') }}
),

detection as (
    select * from {{ ref('int_agent_detection') }}
)

select
    openings.pr_id,
    openings.repo_name,
    openings.pr_number,
    openings.head_ref,
    openings.base_ref,
    openings.actor_login as opened_by,
    detection.agent,
    detection.matched_by,
    detection.is_agent,
    openings.head_repo_id is distinct from openings.base_repo_id as is_fork,
    openings.created_at as opened_at
from openings
inner join detection on openings.pr_id = detection.pr_id
