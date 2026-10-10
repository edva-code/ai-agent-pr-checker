# One row per source and month: average events per captured hour, compared with the
# average of that source's previous unflagged months. A loop, because each month's
# baseline depends on which earlier months were flagged.
import pyarrow as pa

BASELINE_MONTHS = 3
LOW_RATIO = 0.75


def flag_months(rows):
    """rows: (source, month, avg_events) tuples sorted by source, month."""
    out, good, current = [], [], None
    for source, month, avg_events in rows:
        if source != current:
            # A new source is a new series; never compare across sources
            good, current = [], source
        recent = good[-BASELINE_MONTHS:]
        baseline = sum(recent) / len(recent) if recent else None
        volume_low = baseline is not None and avg_events < LOW_RATIO * baseline
        if not volume_low:
            good.append(avg_events)
        out.append(
            {
                "source": source,
                "month": month,
                "avg_events": avg_events,
                "baseline": baseline,
                "volume_low": volume_low,
            }
        )
    return out


def model(dbt, session):
    rows = (
        dbt.ref("capture_health")
        .filter("has_file")
        .aggregate(
            "source, date_trunc('month', hour) as month, avg(events_seen) as avg_events",
            "source, month",
        )
        .order("source, month")
        .fetchall()
    )
    return pa.Table.from_pylist(flag_months(rows))
