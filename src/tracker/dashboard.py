# AI vs human PRs. Run from the repo root: uv run streamlit run src/tracker/dashboard.py
# Do not run while dbt builds: DuckDB allows one writer or many readers, not both.
import altair as alt
import duckdb
import pandas as pd
import streamlit as st

WAREHOUSE = "data/warehouse.duckdb"
PERIODS = {"Day": "D", "Week": "W", "Month": "M"}

QUERY = """
select
    metrics.opened_date as day,
    metrics.agent,
    metrics.prs_opened as prs,
    capture.capture_warning as warning
from agg_daily_metrics as metrics
left join agg_daily_capture as capture on capture.day = metrics.opened_date
"""


@st.cache_data
def load_daily():
    # Close right away so an open dashboard does not hold the lock dbt needs
    with duckdb.connect(WAREHOUSE, read_only=True) as con:
        daily = con.sql(QUERY).df()
    daily["day"] = pd.to_datetime(daily.day)
    return daily


def by_period(df, granularity):
    return df.assign(period=df.day.dt.to_period(PERIODS[granularity]).dt.start_time)


daily = load_daily()
ai_agents = sorted(daily.loc[daily.agent != "human", "agent"].unique())

st.set_page_config(page_title="AI vs human PRs", layout="wide")
with st.sidebar:
    picked = st.date_input(
        "Date range",
        (daily.day.min(), daily.day.max()),
        min_value=daily.day.min(),
        max_value=daily.day.max(),
    )
    agents = st.multiselect("AI agents", ai_agents, default=ai_agents)
    granularity = st.radio("Group by", list(PERIODS), index=2, horizontal=True)

if len(picked) != 2:
    st.stop()  # the picker returns one date while the user is still choosing the range
if not agents:
    st.info("Pick at least one AI agent.")
    st.stop()
start, end = pd.Timestamp(picked[0]), pd.Timestamp(picked[1])

in_range = daily[daily.day.between(start, end)]
selected = in_range[in_range.agent.isin(agents)]

# Share = selected agents / ALL PRs, summed per period before dividing so busy days weigh more
totals = by_period(in_range, granularity).groupby("period").prs.sum()
trend = pd.DataFrame(
    {
        "all_prs": totals,
        "ai_prs": by_period(selected, granularity).groupby("period").prs.sum(),
    }
).fillna({"ai_prs": 0})
trend["ai_share"] = trend.ai_prs / trend.all_prs
trend["warning"] = in_range.groupby("day").warning.first() if granularity == "Day" else None
trend = trend.rename_axis("period").reset_index()

st.title("AI vs human pull requests")
overview, by_agent, yoy = st.tabs(["Overview", "Agents", "Year over year"])

with overview:
    ai, human = int(selected.prs.sum()), int(in_range.loc[in_range.agent == "human", "prs"].sum())
    cols = st.columns(3)
    cols[0].metric("Opened by selected AI agents", f"{ai:,}")
    cols[1].metric("Opened by humans", f"{human:,}")
    cols[2].metric("AI share of all PRs", f"{ai / in_range.prs.sum():.1%}")

    st.subheader(f"AI share per {granularity.lower()}")
    st.caption(
        "Share rather than counts: raw counts jump with how much the archive captured. "
        "In the Day view, orange days have a capture warning; hover for the reason."
    )
    base = alt.Chart(trend).encode(
        x=alt.X("period:T", title=None),
        y=alt.Y("ai_share:Q", title="AI share of PRs opened", axis=alt.Axis(format="%")),
    )
    tooltip = [
        alt.Tooltip("period:T"),
        alt.Tooltip("ai_share:Q", format=".2%"),
        alt.Tooltip("ai_prs:Q", format=","),
        alt.Tooltip("all_prs:Q", format=","),
    ]
    if granularity == "Day":
        points = (
            base.mark_circle(size=30)
            .encode(
                color=alt.Color(
                    "warned:N",
                    scale=alt.Scale(domain=[False, True], range=["#4c78a8", "#f58518"]),
                    legend=alt.Legend(title="Capture warning"),
                ),
                tooltip=[*tooltip, alt.Tooltip("warning:N")],
            )
            .transform_calculate(warned="datum.warning != null")
        )
    else:
        points = base.mark_circle(size=40, color="#4c78a8").encode(tooltip=tooltip)
    st.altair_chart(base.mark_line(color="#4c78a8") + points, width="stretch")

with by_agent:
    st.subheader(f"PRs opened per agent per {granularity.lower()}")
    per_agent = (
        by_period(selected, granularity).groupby(["period", "agent"]).prs.sum().reset_index()
    )
    st.altair_chart(
        alt.Chart(per_agent)
        .mark_area()
        .encode(
            x=alt.X("period:T", title=None),
            y=alt.Y("prs:Q", stack=True, title="PRs opened"),
            color=alt.Color("agent:N", title="Agent"),
            tooltip=["period:T", "agent:N", alt.Tooltip("prs:Q", format=",")],
        ),
        width="stretch",
    )

    st.subheader("Leaderboard")
    span = end - start + pd.Timedelta(days=1)
    prev_start, prev_end = start - span, start - pd.Timedelta(days=1)
    st.caption(
        f"Change compares with the {span.days} days just before the range "
        f"({prev_start.date()} to {prev_end.date()}); empty when there is no data."
    )
    before = daily[daily.day.between(prev_start, prev_end) & daily.agent.isin(agents)]
    current = selected.groupby("agent").prs.sum()
    previous = before.groupby("agent").prs.sum().reindex(current.index)
    board = pd.DataFrame(
        {
            "PRs opened": current,
            "Share of AI PRs (%)": current / current.sum() * 100,
            "Change vs previous (%)": (current / previous - 1) * 100,
        }
    ).sort_values("PRs opened", ascending=False)
    st.dataframe(
        board,
        column_config={
            "PRs opened": st.column_config.NumberColumn(format="localized"),
            "Share of AI PRs (%)": st.column_config.NumberColumn(format="%.1f%%"),
            "Change vs previous (%)": st.column_config.NumberColumn(format="%+.0f%%"),
        },
    )

    st.subheader("When each agent first appeared")
    st.caption("Over all loaded data, ignoring the date range.")
    seen = daily[(daily.prs > 0) & daily.agent.isin(agents)].groupby("agent").day.min()
    st.altair_chart(
        alt.Chart(seen.rename("first_seen").reset_index())
        .mark_circle(size=120)
        .encode(
            x=alt.X("first_seen:T", title="First PR seen"),
            y=alt.Y("agent:N", sort="x", title=None),
            tooltip=["agent:N", alt.Tooltip("first_seen:T")],
        ),
        width="stretch",
    )

with yoy:
    st.subheader("AI share by month, one line per year")
    months = in_range.assign(year=in_range.day.dt.year, month=in_range.day.dt.strftime("%b"))
    monthly = months.groupby(["year", "month"]).prs.sum().to_frame("all_prs")
    monthly["ai_prs"] = months[months.agent.isin(agents)].groupby(["year", "month"]).prs.sum()
    monthly["ai_share"] = monthly.ai_prs.fillna(0) / monthly.all_prs
    st.altair_chart(
        alt.Chart(monthly.reset_index())
        .mark_line(point=True)
        .encode(
            x=alt.X(
                "month:O",
                sort=list(pd.date_range("2000-01", periods=12, freq="MS").strftime("%b")),
                title=None,
            ),
            y=alt.Y("ai_share:Q", title="AI share of PRs opened", axis=alt.Axis(format="%")),
            color=alt.Color("year:N", title="Year"),
            tooltip=[
                "year:N",
                "month:N",
                alt.Tooltip("ai_share:Q", format=".2%"),
                alt.Tooltip("ai_prs:Q", format=","),
            ],
        ),
        width="stretch",
    )
