"""Download GH Archive hours, keep only pull request events, write them as Parquet.

Each GH Archive hour is ~15 MB gzipped, but pull request events are ~1.5% of it.
The file is streamed and filtered on the fly, so the raw data never touches disk:

    data.gharchive.org/2026-10-06-14.json.gz
        -> data/events/date=2026-10-06/14.parquet   (~100 KB)

Usage:
    tracker-ingest 2026-10-06T14                       # one hour
    tracker-ingest 2026-10-01T00 --end 2026-10-02T00   # a range (end exclusive)
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import time
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import IO, Any

import pyarrow as pa
import pyarrow.parquet as pq
import requests

log = logging.getLogger("tracker.ingest")

GHARCHIVE_URL = "https://data.gharchive.org/{date}-{hour}.json.gz"
KEEP_TYPES = frozenset({"PullRequestEvent", "PullRequestReviewEvent"})
DEFAULT_OUT_DIR = Path("data/events")

SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_type", pa.string()),
        ("action", pa.string()),
        ("created_at", pa.timestamp("ms", tz="UTC")),
        ("actor_id", pa.int64()),
        ("actor_login", pa.string()),
        ("repo_id", pa.int64()),
        ("repo_name", pa.string()),
        ("pr_id", pa.int64()),
        ("pr_number", pa.int64()),
        ("head_ref", pa.string()),
        ("base_ref", pa.string()),
        ("head_repo_id", pa.int64()),
        ("base_repo_id", pa.int64()),
        ("review_state", pa.string()),
        ("source_hour", pa.timestamp("ms", tz="UTC")),
    ]
)


@dataclass
class HourStats:
    hour: datetime
    lines: int = 0
    kept: int = 0
    malformed: int = 0
    skipped: bool = False


def hour_url(hour: datetime) -> str:
    # GH Archive hours are NOT zero-padded: 2026-10-06-9.json.gz, not -09
    return GHARCHIVE_URL.format(date=hour.strftime("%Y-%m-%d"), hour=hour.hour)


def output_path(hour: datetime, out_dir: Path) -> Path:
    return out_dir / f"date={hour:%Y-%m-%d}" / f"{hour:%H}.parquet"


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def flatten(event: dict[str, Any], source_hour: datetime) -> dict[str, Any]:
    """Turn one raw PR event into a flat row matching SCHEMA.

    Since 2025 GitHub strips most PR details (title, body, author) from public
    events, so the branch name (head_ref) is our main signal for agent detection.
    """
    payload = event.get("payload") or {}
    pr = payload.get("pull_request") or {}
    head = pr.get("head") or {}
    base = pr.get("base") or {}
    actor = event.get("actor") or {}
    repo = event.get("repo") or {}
    review = payload.get("review") or {}

    return {
        "event_id": event["id"],
        "event_type": event["type"],
        "action": payload.get("action"),
        "created_at": parse_ts(event["created_at"]),
        "actor_id": actor.get("id"),
        "actor_login": actor.get("login"),
        "repo_id": repo.get("id"),
        "repo_name": repo.get("name"),
        "pr_id": pr.get("id"),
        "pr_number": pr.get("number") or payload.get("number"),
        "head_ref": head.get("ref"),
        "base_ref": base.get("ref"),
        "head_repo_id": (head.get("repo") or {}).get("id"),
        "base_repo_id": (base.get("repo") or {}).get("id"),
        "review_state": review.get("state"),
        "source_hour": source_hour,
    }


def filter_events(
    lines: Iterable[bytes], source_hour: datetime, stats: HourStats
) -> Iterator[dict[str, Any]]:
    for line in lines:
        stats.lines += 1
        # Cheap substring check before json.loads: skips ~98% of lines without parsing
        if b'"type":"PullRequest' not in line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            stats.malformed += 1
            continue
        if event.get("type") not in KEEP_TYPES:
            continue
        stats.kept += 1
        yield flatten(event, source_hour)


def write_parquet(rows: list[dict[str, Any]], path: Path) -> None:
    """Write atomically: a crash mid-write never leaves a half-written file behind."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".parquet.tmp")
    # Empty hours still get a file, so "this hour was processed" is visible on disk
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), tmp, compression="zstd")
    tmp.replace(path)


def open_hour(hour: datetime, session: requests.Session, retries: int = 3) -> IO[bytes]:
    url = hour_url(hour)
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, stream=True, timeout=60)
            if resp.status_code == 404:
                raise FileNotFoundError(f"{url} not published yet")
            resp.raise_for_status()
            resp.raw.decode_content = False  # we gunzip ourselves
            return gzip.GzipFile(fileobj=resp.raw)
        except requests.RequestException as exc:
            if attempt == retries:
                raise
            wait = 2**attempt
            log.warning("attempt %d for %s failed (%s), retrying in %ds", attempt, url, exc, wait)
            time.sleep(wait)
    raise AssertionError("unreachable")


def ingest_hour(
    hour: datetime,
    out_dir: Path = DEFAULT_OUT_DIR,
    force: bool = False,
    session: requests.Session | None = None,
) -> HourStats:
    stats = HourStats(hour=hour)
    path = output_path(hour, out_dir)
    if path.exists() and not force:
        stats.skipped = True
        return stats

    session = session or requests.Session()
    with open_hour(hour, session) as stream:
        rows = list(filter_events(stream, hour, stats))
    write_parquet(rows, path)
    return stats


def hours_between(start: datetime, end: datetime) -> Iterator[datetime]:
    hour = start
    while hour < end:
        yield hour
        hour += timedelta(hours=1)


def parse_hour(value: str) -> datetime:
    """Accept '2026-10-06T14' or '2026-10-06T14:00'."""
    if len(value) == 13:
        value += ":00"
    return datetime.fromisoformat(value).replace(minute=0, second=0, microsecond=0, tzinfo=UTC)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("start", type=parse_hour, help="first hour, e.g. 2026-10-06T14")
    parser.add_argument(
        "--end", type=parse_hour, help="stop before this hour (default: start + 1h)"
    )
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--force", action="store_true", help="re-ingest hours that already exist")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    end = args.end or args.start + timedelta(hours=1)
    session = requests.Session()

    for hour in hours_between(args.start, end):
        started = time.monotonic()
        try:
            stats = ingest_hour(hour, args.out_dir, args.force, session)
        except FileNotFoundError as exc:
            log.warning("%s", exc)
            continue
        if stats.skipped:
            log.info("%s already ingested, skipping", f"{hour:%Y-%m-%d %H}h")
            continue
        log.info(
            "%s: kept %d of %d events (%d malformed) in %.1fs",
            f"{hour:%Y-%m-%d %H}h",
            stats.kept,
            stats.lines,
            stats.malformed,
            time.monotonic() - started,
        )


if __name__ == "__main__":
    main()
