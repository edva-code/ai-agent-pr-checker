-- More than 0.1% unparseable PR lines in an hour means the source format changed.
select hour, malformed, events_seen
from {{ ref('stg_ingest_stats') }}
where malformed > 0.001 * events_seen
