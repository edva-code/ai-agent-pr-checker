# AI vs human PRs. Run from the repo root: uv run streamlit run src/tracker/dashboard.py
# Do not run while dbt builds: DuckDB allows one writer or many readers, not both.
import altair as alt
import duckdb
import streamlit as st

WAREHOUSE = "data/warehouse.duckdb"

QUERY = """
select
    metrics.opened_date as day,
    sum(prs_opened) filter (where agent <> 'human') as ai_prs,
    sum(prs_opened) filter (where agent = 'human') as human_prs,
    any_value(capture.capture_warning) as warning
from agg_daily_metrics as metrics
left join agg_daily_capture as capture on capture.day = metrics.opened_date
group by 1
order by 1
"""


@st.cache_data
def load_daily():
    # Close right away so an open dashboard does not hold the lock dbt needs
    with duckdb.connect(WAREHOUSE, read_only=True) as con:
        return con.sql(QUERY).df()


daily = load_daily()
daily["ai_share"] = daily.ai_prs / (daily.ai_prs + daily.human_prs)
daily["warned"] = daily.warning.notna()

st.title("AI vs human pull requests")
ai, human = int(daily.ai_prs.sum()), int(daily.human_prs.sum())
cols = st.columns(3)
cols[0].metric("Opened by AI agents", f"{ai:,}")
cols[1].metric("Opened by humans", f"{human:,}")
cols[2].metric("AI share", f"{ai / (ai + human):.1%}")

st.subheader("Daily AI share")
st.caption(
    "Share rather than counts: raw counts jump with how much the archive captured. "
    "Orange days have a capture warning; hover for the reason."
)
base = alt.Chart(daily).encode(
    x=alt.X("day:T", title=None),
    y=alt.Y("ai_share:Q", title="AI share of PRs opened", axis=alt.Axis(format="%")),
)
points = base.mark_circle(size=60).encode(
    color=alt.Color(
        "warned:N",
        scale=alt.Scale(domain=[False, True], range=["#4c78a8", "#f58518"]),
        legend=alt.Legend(title="Capture warning"),
    ),
    tooltip=[
        alt.Tooltip("day:T"),
        alt.Tooltip("ai_share:Q", format=".1%"),
        alt.Tooltip("ai_prs:Q", format=","),
        alt.Tooltip("human_prs:Q", format=","),
        alt.Tooltip("warning:N"),
    ],
)
st.altair_chart(base.mark_line(color="#4c78a8") + points, width="stretch")
