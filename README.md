# AI Agent PR Tracker

How much of open source is now written by AI coding agents — and does their work get merged?

This pipeline processes every public pull request on GitHub (via [GH Archive](https://www.gharchive.org/))
and detects PRs opened by AI coding agents such as Claude, Codex, Cursor, Copilot, Jules and Devin.

> **First look (one hour, 2026-10-06 14:00 UTC):** 80 of 529 newly opened PRs (≈15%) came from
> branches created by AI agents.

## Architecture

```
GH Archive (hourly .json.gz, ~15 MB)
  → ingest: stream + filter PR events, raw never stored       [Python]
  → data/events/date=YYYY-MM-DD/HH.parquet (~100 KB/hour)     [Parquet]
  → staging → agent detection → PR lifecycle → daily metrics  [dbt + DuckDB]   (planned)
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

## Roadmap

- [x] Hourly ingestion to partitioned Parquet (idempotent, atomic writes, retries)
- [ ] dbt models: staging, agent detection, PR lifecycle (opened → merged/closed)
- [ ] Data quality tests and freshness checks
- [ ] Dagster schedule and backfills
- [ ] Streamlit dashboard
- [ ] Docker Compose and GitHub Actions CI
