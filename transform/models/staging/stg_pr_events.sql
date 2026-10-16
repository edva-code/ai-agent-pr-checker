-- One row per PR: its opening event. Upstream sometimes repeats events and a few PRs are opened
-- more than once, so keep the earliest opening.
-- Incremental: each build reads only hours that are new or re-downloaded since they were loaded.
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='pr_id',
    pre_hook="{% if is_incremental() %}delete from {{ this }} where source_hour in (select source_hour from ({{ unloaded_event_files(this) }})){% endif %}"
) }}

-- depends_on: {{ source('gharchive', 'events') }}
{% if is_incremental() %}
    {% set paths = run_query(unloaded_event_files(this)).columns[0].values() | list if execute else [] %}
{% else %}
    {% set paths = ['data/events/*/*.parquet'] %}
{% endif %}

with new_events as (
    {% if paths %}
    select
        event_id,
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
        events.source_hour,
        files.last_modified as loaded_at
    from read_parquet({{ paths }}, filename = true) as events
    join ({{ event_files() }}) as files using (filename)
    where event_type = 'PullRequestEvent' and action = 'opened'
    {% else %}
    select * from {{ this }} where false
    {% endif %}
),

source as (
    select * from new_events
    {% if is_incremental() %}
    union all
    -- Openings already loaded for these PRs, so an earlier one wins across batches
    select * from {{ this }} where pr_id in (select pr_id from new_events)
    {% endif %}
)

select * from source
qualify row_number() over (partition by pr_id order by created_at, source_hour, event_id) = 1
