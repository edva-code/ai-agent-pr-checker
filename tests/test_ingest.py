import gzip
import io
import json
from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest

from tracker import ingest

HOUR = datetime(2026, 10, 6, 14, tzinfo=UTC)


def pr_event(event_id: str, action: str = "opened", head_ref: str = "claude/fix-bug") -> dict:
    return {
        "id": event_id,
        "type": "PullRequestEvent",
        "actor": {"id": 1, "login": "alice"},
        "repo": {"id": 10, "name": "alice/repo"},
        "payload": {
            "action": action,
            "number": 7,
            "pull_request": {
                "id": 99,
                "number": 7,
                "head": {"ref": head_ref, "repo": {"id": 10}},
                "base": {"ref": "main", "repo": {"id": 10}},
            },
        },
        "created_at": "2026-10-06T14:05:00Z",
    }


def raw_lines(*events: dict) -> list[bytes]:
    # GH Archive writes compact JSON, one event per line
    return [json.dumps(e, separators=(",", ":")).encode() + b"\n" for e in events]


def test_hour_url_is_not_zero_padded():
    assert ingest.hour_url(datetime(2026, 1, 2, 9, tzinfo=UTC)).endswith("2026-01-02-9.json.gz")
    assert ingest.hour_url(HOUR).endswith("2026-10-06-14.json.gz")


def test_filter_keeps_only_pr_events_and_counts_malformed():
    push = {"id": "2", "type": "PushEvent", "payload": {}, "created_at": "2026-10-06T14:00:00Z"}
    lines = raw_lines(pr_event("1"), push) + [b'{"type":"PullRequestEvent", broken\n']
    stats = ingest.HourStats(hour=HOUR)

    rows = list(ingest.filter_events(lines, HOUR, stats))

    assert [r["event_id"] for r in rows] == ["1"]
    assert (stats.lines, stats.kept, stats.malformed) == (3, 1, 1)


def test_flatten_extracts_branch_and_ids():
    row = ingest.flatten(pr_event("1", head_ref="codex/add-tests"), HOUR)

    assert row["head_ref"] == "codex/add-tests"
    assert row["pr_id"] == 99
    assert row["created_at"] == datetime(2026, 10, 6, 14, 5, tzinfo=UTC)


def test_flatten_tolerates_missing_fields():
    row = ingest.flatten(
        {"id": "3", "type": "PullRequestEvent", "created_at": "2026-10-06T14:00:00Z"}, HOUR
    )
    assert row["pr_id"] is None and row["head_ref"] is None


class FakeResponse:
    def __init__(self, body: bytes, status: int = 200):
        self.raw = io.BytesIO(body)
        self.status_code = status

    def raise_for_status(self):
        pass


class FakeSession:
    def __init__(self, body: bytes, status: int = 200):
        self.body, self.status, self.calls = body, status, 0

    def get(self, url, **kwargs):
        self.calls += 1
        return FakeResponse(self.body, self.status)


def test_ingest_hour_writes_parquet_and_is_idempotent(tmp_path):
    session = FakeSession(
        gzip.compress(b"".join(raw_lines(pr_event("1"), pr_event("2", "merged"))))
    )

    stats = ingest.ingest_hour(HOUR, tmp_path, session=session)
    again = ingest.ingest_hour(HOUR, tmp_path, session=session)

    table = pq.read_table(tmp_path / "date=2026-10-06" / "14.parquet")
    assert table.schema.equals(ingest.SCHEMA)
    assert table.column("action").to_pylist() == ["opened", "merged"]
    assert stats.kept == 2
    assert again.skipped and session.calls == 1


def test_ingest_hour_writes_empty_file_for_hour_without_prs(tmp_path):
    ingest.ingest_hour(HOUR, tmp_path, session=FakeSession(gzip.compress(b"")))

    assert pq.read_table(tmp_path / "date=2026-10-06" / "14.parquet").num_rows == 0


def test_missing_hour_raises_file_not_found(tmp_path):
    with pytest.raises(FileNotFoundError):
        ingest.ingest_hour(HOUR, tmp_path, session=FakeSession(b"", status=404))
