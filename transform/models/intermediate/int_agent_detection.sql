-- Match each PR against agent_rules. starts_with() rather than a regex: DuckDB's `~`
-- must match the whole string, which silently matched nothing in our first query.
-- Bot accounts (priority 1) win over branch prefixes (priority 2).
with prs as (
    select pr_id, head_ref, actor_login as opened_by from {{ ref('stg_pr_events') }}
),

rules as (
    select * from {{ ref('agent_rules') }}
),

matches as (
    select
        prs.pr_id,
        rules.agent,
        rules.match_type
    from prs
    inner join rules
        on (rules.match_type = 'branch_prefix' and starts_with(prs.head_ref, rules.pattern))
        or (rules.match_type = 'actor_login' and prs.opened_by = rules.pattern)
    qualify row_number() over (partition by prs.pr_id order by rules.priority, rules.agent) = 1
)

select
    prs.pr_id,
    coalesce(matches.agent, 'human') as agent,
    matches.match_type as matched_by,
    matches.agent is not null as is_agent
from prs
left join matches on prs.pr_id = matches.pr_id
