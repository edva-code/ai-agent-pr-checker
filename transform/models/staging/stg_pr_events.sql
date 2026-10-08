-- One row per event. A re-ingested hour could repeat events, so keep the first copy.
with source as (
    select * from {{ source('gharchive', 'events') }}
),

deduped as (
    select
        *,
        row_number() over (partition by event_id order by source_hour) as copy_number
    from source
)

select
    event_id,
    event_type,
    action,
    created_at,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    pr_id,
    pr_number,
    head_ref,
    base_ref,
    head_repo_id,
    base_repo_id,
    review_state,
    source_hour
from deduped
where copy_number = 1
