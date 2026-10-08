-- Hours with less than half or more than double the trailing week's median PR events.
-- Warn only: the variance is upstream in GH Archive, not a bug in our pipeline.
{{ config(severity='warn') }}
select hour, pr_events, trailing_week_median, capture_flag
from {{ ref('capture_health') }}
where capture_flag in ('low', 'high')
