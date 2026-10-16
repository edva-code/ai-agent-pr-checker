{# One row per downloaded events file: its hour and when it was last written. #}
{% macro event_files() %}
    select
        filename,
        strptime(regexp_extract(filename, 'date=([0-9-]+/[0-9]{2})', 1), '%Y-%m-%d/%H')::timestamptz as source_hour,
        last_modified
    from read_blob('data/events/*/*.parquet')
{% endmacro %}

{# Files that are new, or re-downloaded since their hour was loaded into the model. #}
{% macro unloaded_event_files(model) %}
    select files.filename, files.source_hour
    from ({{ event_files() }}) as files
    left join (
        select source_hour, max(loaded_at) as loaded_at from {{ model }} group by source_hour
    ) as loaded using (source_hour)
    where loaded.loaded_at is null or files.last_modified > loaded.loaded_at
{% endmacro %}
