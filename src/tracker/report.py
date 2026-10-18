"""Write the README charts, stats block and the interactive GitHub Pages page.

Run from the repo root after a dbt build (not during one: DuckDB lock):
    uv run python -m tracker.report
"""

import json
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd

WAREHOUSE = "data/warehouse.duckdb"
DOCS = Path("docs")
README = Path("README.md")
START, END = "<!-- stats:start -->", "<!-- stats:end -->"
# Charts only: before this, AI share is ~0 and squashes the interesting part
CHART_START = "2025-04-01"

# GitHub Primer colours, so the charts sit naturally in a README in either theme
GITHUB = {
    "dark": {"bg": "#0d1117", "text": "#e6edf3", "muted": "#9198a1", "grid": "#30363d"},
    "light": {"bg": "#ffffff", "text": "#1f2328", "muted": "#59636e", "grid": "#d1d9e0"},
}
FONT = "-apple-system, Segoe UI, Noto Sans, Helvetica, Arial, sans-serif"
ACCENT = "#2f81f7"
AGENT_COLORS = {
    "codex": "#2f81f7",
    "claude": "#f0883e",
    "copilot": "#a371f7",
    "cursor": "#3fb950",
    "devin": "#db61a2",
    "chatgpt": "#39c5cf",
    "jules": "#d29922",
}
MONTH_AXIS = alt.Axis(format="%b %Y", labelAngle=0)
MONTH = alt.X("month:T", title=None, axis=MONTH_AXIS)

# Page follows the browser theme; vega-embed has no automatic theme switch
PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AI agent PRs</title>
<script src="https://cdn.jsdelivr.net/npm/vega@{vega}"></script>
<script src="https://cdn.jsdelivr.net/npm/vega-lite@{vegalite}"></script>
<script src="https://cdn.jsdelivr.net/npm/vega-embed@{embed}"></script>
<style>
  body { margin: 0; padding: 24px 16px; background: #ffffff; }
  @media (prefers-color-scheme: dark) { body { background: #0d1117; } }
</style>
</head>
<body>
<div id="vis"></div>
<script>
  const specs = {specs};
  const dark = matchMedia("(prefers-color-scheme: dark)").matches;
  vegaEmbed("#vis", specs[dark ? "dark" : "light"], { actions: false });
</script>
</body>
</html>
"""

# Whole months only: a half-loaded last month would look like a collapse in the counts
QUERY = """
with full_months as (
    select date_trunc('month', opened_date) as month
    from agg_daily_metrics
    group by 1
    having count(distinct opened_date) >= 28
)
select date_trunc('month', opened_date) as month, agent, sum(prs_opened) as prs
from agg_daily_metrics
where date_trunc('month', opened_date) in (select month from full_months)
group by 1, 2
"""


def load() -> pd.DataFrame:
    with duckdb.connect(WAREHOUSE, read_only=True) as con:
        return con.sql(QUERY).df()


def charts(monthly: pd.DataFrame) -> tuple[alt.LayerChart, pd.DataFrame, list[str]]:
    totals = monthly.groupby("month").prs.sum()
    # Every month x agent, filled with 0: missing rows would draw a line straight over the gap
    ai_prs = (
        monthly[monthly.agent != "human"]
        .pivot_table(index="month", columns="agent", values="prs", aggfunc="sum", fill_value=0)
        .reindex(totals.index, fill_value=0)
    )
    share = (ai_prs.sum(axis=1) / totals).rename("ai_share").reset_index()
    ai = ai_prs.stack().rename("prs").reset_index()
    ai["share"] = ai.prs / ai.month.map(totals)
    order = list(ai_prs.sum().sort_values(ascending=False).index)

    base = alt.Chart(share, title="AI share of PRs opened, per month").encode(
        x=MONTH,
        y=alt.Y("ai_share:Q", title="AI share", axis=alt.Axis(format="%")),
        tooltip=[
            alt.Tooltip("month:T", format="%b %Y"),
            alt.Tooltip("ai_share:Q", format=".2%"),
        ],
    )
    share_chart = (
        base.mark_area(color=ACCENT, opacity=0.2)
        + base.mark_line(color=ACCENT, strokeWidth=2.5)
        + base.mark_circle(color=ACCENT, size=40)
    ).properties(width=720, height=220)
    return share_chart, ai, order


def agent_chart(ai: pd.DataFrame, order: list[str], x: alt.X = MONTH) -> alt.FacetChart:
    """One small chart per agent, each with its own y scale, so small agents stay readable."""
    base = alt.Chart(ai).encode(
        x=x,
        y=alt.Y("share:Q", title=None, axis=alt.Axis(format="~%", tickCount=3)),
        color=alt.Color(
            "agent:N",
            scale=alt.Scale(domain=list(AGENT_COLORS), range=list(AGENT_COLORS.values())),
            legend=None,
        ),
        tooltip=[
            alt.Tooltip("month:T", format="%b %Y"),
            "agent:N",
            alt.Tooltip("share:Q", format=".2%", title="share of all PRs"),
            alt.Tooltip("prs:Q", format=","),
        ],
    )
    return (
        (base.mark_area(opacity=0.25) + base.mark_line(strokeWidth=2))
        .properties(width=330, height=110)
        .facet(
            facet=alt.Facet("agent:N", sort=order, title=None),
            columns=2,
            title="Share of all PRs opened, per AI agent (each on its own scale)",
        )
        .resolve_scale(y="independent")
    )


def stats_block(monthly: pd.DataFrame) -> str:
    total = int(monthly.prs.sum())
    per_agent = monthly[monthly.agent != "human"].groupby("agent").prs.sum()
    ai = int(per_agent.sum())
    rows = [
        f"| {agent} | {int(prs):,} | {prs / total:.2%} |"
        for agent, prs in per_agent.sort_values(ascending=False).items()
    ]
    months = monthly.month
    return "\n".join(
        [
            f"**{months.min():%b %Y} – {months.max():%b %Y}:** {total:,} PRs opened, "
            f"{ai:,} (**{ai / total:.1%}**) by detected AI agents.",
            "",
            "| Agent | PRs opened | Share of all PRs |",
            "|---|---:|---:|",
            *rows,
        ]
    )


def replace_block(text: str, block: str) -> str:
    """Swap only the text between the markers, so hand-written README text survives reruns."""
    head, sep, rest = text.partition(START)
    _, sep2, tail = rest.partition(END)
    if not (sep and sep2):
        raise ValueError(f"README needs {START} and {END} markers")
    return f"{head}{START}\n{block}\n{END}{tail}"


def themed(chart: alt.TopLevelMixin, theme: str) -> alt.TopLevelMixin:
    c = GITHUB[theme]
    axis = dict(labelColor=c["muted"], titleColor=c["muted"], gridColor=c["grid"])
    return (
        chart.configure(background=c["bg"], font=FONT)
        .configure_view(stroke=None)
        .configure_axis(**axis, domainColor=c["grid"], tickColor=c["grid"])
        .configure_title(color=c["text"], anchor="start", fontSize=15, offset=12)
        .configure_header(labelColor=c["text"], labelFontSize=13, labelFontWeight="bold")
    )


def main() -> None:
    monthly = load()
    share_chart, ai, order = charts(monthly[monthly.month >= CHART_START])
    (DOCS / "img").mkdir(parents=True, exist_ok=True)
    # README picks the file matching the reader's GitHub theme via <picture>
    for theme in GITHUB:
        themed(share_chart, theme).save(DOCS / "img" / f"ai_share-{theme}.svg")
        themed(agent_chart(ai, order), theme).save(DOCS / "img" / f"per_agent-{theme}.svg")

    # Interactive: drag on the share chart to zoom every agent chart
    brush = alt.selection_interval(encodings=["x"])
    zoomed = alt.X("month:T", title=None, axis=MONTH_AXIS, scale={"domain": brush})
    page = alt.vconcat(share_chart.add_params(brush), agent_chart(ai, order, zoomed)).properties(
        title="AI agent pull requests on public GitHub"
    )
    specs = {theme: themed(page, theme).to_dict() for theme in GITHUB}
    (DOCS / "index.html").write_text(
        PAGE.replace("{specs}", json.dumps(specs))
        .replace("{vega}", alt.VEGA_VERSION)
        .replace("{vegalite}", alt.VEGALITE_VERSION)
        .replace("{embed}", alt.VEGAEMBED_VERSION)
    )

    README.write_text(replace_block(README.read_text(), stats_block(monthly)))


if __name__ == "__main__":
    main()
