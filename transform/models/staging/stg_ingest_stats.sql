-- One row per hour tracker-ingest processed. A re-ingest overwrites the file, so hours are unique.
select
    cast(hour as timestamptz) as hour,
    lines as events_seen,
    kept as pr_events,
    malformed,
    source,
    cast(ingested_at as timestamptz) as ingested_at
from {{ source('gharchive', 'ingest_stats') }}
