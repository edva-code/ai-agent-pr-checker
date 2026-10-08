-- A gap of a few hours is a warning (rerun the ingest); over a day of gaps fails the build.
{{ config(warn_if='>0', error_if='>24') }}
select hour from {{ ref('capture_health') }} where capture_flag = 'missing'
