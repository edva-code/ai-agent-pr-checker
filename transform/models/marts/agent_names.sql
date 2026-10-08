-- Every label fct_prs.agent may take: the agents in agent_rules, plus 'human'.
select distinct agent from {{ ref('agent_rules') }}
union
select 'human'
