-- A PR can't be merged before it was opened. Rows returned = failures.
select pr_id, opened_at, merged_at
from {{ ref('fct_prs') }}
where merged_at < opened_at
