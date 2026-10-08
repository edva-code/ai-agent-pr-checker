# AI Agent PR Tracker

How much of open source is now written by AI coding agents — and does their work get merged?

This pipeline processes every public pull request on GitHub (via [GH Archive](https://www.gharchive.org/))
and detects PRs opened by AI coding agents such as Claude, Codex, Cursor, Copilot, Jules and Devin.

> **First look (2026-10-05 → 06, 33.5k new PRs):** ≈14.5% were opened by detected AI agents —
> Claude 8.3%, Codex 4.1%, Cursor 1.5%, Devin 0.3%, Copilot 0.2%, ChatGPT 0.1%.

## Architecture

```
GH Archive (hourly .json.gz, ~15 MB)
  → ingest: stream + filter PR events, raw never stored       [Python]
  → data/events/date=YYYY-MM-DD/HH.parquet (~100 KB/hour)     [Parquet]
  → staging → agent detection → PR lifecycle → daily metrics  [dbt + DuckDB]
  → hourly schedule + backfills                               [Dagster]        (planned)
  → dashboard                                                 [Streamlit]      (planned)
```

## How agents are detected

GitHub's public event stream no longer includes PR titles, descriptions or authors, so detection
relies on signals that remain:

- **Branch name prefixes** agents create by default: `claude/…`, `codex/…`, `cursor/…`, `copilot/…`
- **Bot accounts**: e.g. `devin-ai-integration[bot]`, `Copilot`

**Limitations:** a human can name a branch `claude/x`, and agents used with custom branch names leave
no trace. Reported figures are therefore *detected* AI-agent PRs and should be read as a lower bound.

## Quickstart

Requires [uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run tracker-ingest 2026-10-06T14                       # one hour
uv run tracker-ingest 2026-10-06T00 --end 2026-10-07T00   # one day
uv run pytest
```

Query the output directly with DuckDB:

```sql
SELECT split_part(head_ref, '/', 1) AS prefix, count(*) AS prs
FROM read_parquet('data/events/*/*.parquet', hive_partitioning = true)
WHERE event_type = 'PullRequestEvent' AND action = 'opened'
GROUP BY 1 ORDER BY 2 DESC LIMIT 15;
```

## Transformations (dbt + DuckDB)

```
source: data/events/*/*.parquet
  └─ stg_pr_events          view   typed + deduplicated events (GH Archive can repeat an event)
       └─ int_pr_lifecycle  view   one row per PR: opened / merged / closed, reviews, approvals
            └─ int_agent_detection  view   PR → agent, using rules from seeds/agent_rules.csv
                 └─ fct_prs           table  one row per PR opened inside the loaded window
                      └─ agg_daily_metrics  table  per day × agent: PRs, merge rate, median time to merge, share
```

Detection rules live in a seed (`transform/seeds/agent_rules.csv`), so adding a new agent is a
one-line CSV change, not a code change. Bot accounts take priority over branch prefixes.

```bash
uv run dbt build --project-dir transform --profiles-dir transform   # run from repo root
```

`dbt build` runs seeds, models and 25 tests (uniqueness, not-null, accepted values,
relationships, plus custom tests such as "no PR merged before it was opened").
The warehouse is `data/warehouse.duckdb`; all timestamps are bucketed in UTC.

### Data caveats

- **Right-censoring:** recent PRs have had less time to be merged, so merge rates for the last
  days are understated. PRs opened before the loaded window are excluded.
- **GH Archive volume change (2026-10-06 ≈17:00 UTC):** from this hour the archive stops
  containing push/create/delete events (~85k → ~7k events/hour), and the number of captured
  PR events roughly doubles. Absolute PR counts before and after are not comparable;
  agent *share* is the more robust metric.

## Roadmap

- [x] Hourly ingestion to partitioned Parquet (idempotent, atomic writes, retries)
- [x] dbt models: staging, agent detection, PR lifecycle (opened → merged/closed)
- [ ] Data quality tests and freshness checks
- [ ] Dagster schedule and backfills
- [ ] Streamlit dashboard
- [ ] Docker Compose and GitHub Actions CI
