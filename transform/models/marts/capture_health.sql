-- One row per hour in the loaded window, flagging hours that are missing or whose
-- PR volume is far from the trailing week. GH Archive capture varies a lot upstream.
with stats as (
    select * from {{ ref('stg_ingest_stats') }}
),

spine as (
    select unnest(generate_series(min(hour), max(hour), interval 1 hour)) as hour
    from stats
),

hours as (
    select
        spine.hour,
        stats.hour is not null as has_file,
        stats.events_seen,
        stats.pr_events,
        stats.malformed
    from spine
    left join stats using (hour)
),

with_baseline as (
    select
        *,
        -- Median ignores missing hours (null pr_events), so a gap doesn't drag the baseline down
        median(pr_events) over (
            order by hour rows between 168 preceding and 1 preceding
        ) as trailing_week_median
    from hours
)

select
    *,
    case
        when not has_file then 'missing'
        when pr_events < 0.5 * trailing_week_median then 'low'
        when pr_events > 2 * trailing_week_median then 'high'
        else 'ok'
    end as capture_flag
from with_baseline
