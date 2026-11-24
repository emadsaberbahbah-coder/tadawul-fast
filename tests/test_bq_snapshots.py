"""tests/test_bq_snapshots.py - scripts/bq_snapshots.py 1.0.0 (the heavy views computed once after each collector)."""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from scripts import bq_snapshots as bs


class Resp:
    def __init__(self, body, status=200):
        self._body, self.status_code = body, status

    def json(self):
        return self._body


class Session:
    """A query job per snapshot: DONE on the first poll, or an error for the table named in `fail`."""

    def __init__(self, fail=None, http=200):
        self.jobs, self.polls, self.fail, self.http = [], 0, fail, http

    def post(self, url, json=None, timeout=None):
        assert url == "https://bigquery.googleapis.com/bigquery/v2/projects/proj-1/jobs"
        self.jobs.append(json)
        if self.http != 200:
            return Resp({}, self.http)
        return Resp({"status": {"state": "RUNNING"}})

    def get(self, url, timeout=None):
        self.polls += 1
        table = self.jobs[-1]["configuration"]["query"]["destinationTable"]["tableId"]
        if table == self.fail:
            return Resp({"status": {"state": "DONE", "errorResult": {"message": "boom"}}})
        return Resp({"status": {"state": "DONE"}, "statistics": {"query": {"totalBytesProcessed": "250000000"}}})


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.delenv("TFB_BQ_SNAPSHOTS", raising=False)
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-1")
    monkeypatch.setenv("GITHUB_RUN_ID", "99")


def test_each_snapshot_is_one_query_job_replacing_its_table(capsys):
    sess = Session()
    assert bs.run_once(False, session=sess, sleep=lambda s: None) == 0
    assert [j["configuration"]["query"]["destinationTable"]["tableId"] for j in sess.jobs] == list(bs.SNAPSHOTS)
    q = sess.jobs[0]["configuration"]["query"]
    assert q["query"] == "SELECT *, CURRENT_TIMESTAMP() AS snapshot_at FROM `proj-1.tfb_archive.v_universe_latest`"
    assert q["writeDisposition"] == "WRITE_TRUNCATE" and q["createDisposition"] == "CREATE_IF_NEEDED"
    assert q["useLegacySql"] is False and q["clustering"] == {"fields": ["symbol"]}
    assert sess.jobs[0]["jobReference"]["jobId"].startswith("tfb_snap_snap_universe_latest_99_")
    out = capsys.readouterr().out
    assert "refreshed snap_universe_latest from v_universe_latest (0.25 GB processed)" in out
    assert "done: 3 of 3 snapshot(s) refreshed" in out


def test_a_failed_snapshot_leaves_the_others_and_marks_the_run(capsys):
    sess = Session(fail="snap_news_understanding")
    assert bs.run_once(False, session=sess, sleep=lambda s: None) == 1
    out = capsys.readouterr().out
    assert "ERROR: snap_news_understanding was not refreshed" in out and "done: 2 of 3 snapshot(s) refreshed" in out
    assert bs.run_once(False, session=Session(http=403), sleep=lambda s: None) == 1


def test_only_dry_run_and_switch(monkeypatch, capsys):
    sess = Session()
    assert bs.run_once(False, session=sess, only=["v_news_trend_learning"], sleep=lambda s: None) == 0
    assert [j["configuration"]["query"]["destinationTable"]["tableId"] for j in sess.jobs] == ["snap_news_trend_learning"]
    assert bs.run_once(True, session=Session()) == 0 and "dry run: would refresh" in capsys.readouterr().out
    monkeypatch.setenv("TFB_BQ_SNAPSHOTS", "off")
    sess = Session()
    assert bs.run_once(False, session=sess) == 0 and sess.jobs == []


@pytest.mark.parametrize("rows,error,expected", [
    ([{"age_min": "90"}], None, "snap_universe_latest"),
    ([{"age_min": str(30 * 60)}], None, "snap_universe_latest"),
    ([{"age_min": str(30 * 60 + 1)}], None, "v_universe_latest"),
    ([{"age_min": None}], None, "v_universe_latest"),
    ([], None, "v_universe_latest"),
    (None, "Not found: Table snap_universe_latest", "v_universe_latest"),
    (None, "HTTP 403", "v_universe_latest")])
def test_source_for_reads_a_fresh_snapshot_else_the_view(rows, error, expected):
    seen, notes = [], []

    def query(session, project, location, sql):
        seen.append(sql)
        if error:
            raise RuntimeError(error)
        return rows

    got = bs.source_for("v_universe_latest", None, "proj-1", "US", "proj-1.tfb_archive", query, note=notes.append)
    assert got == expected
    assert seen == ["SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(snapshot_at), MINUTE) AS age_min FROM "
                    "`proj-1.tfb_archive.snap_universe_latest`"]
    assert bool(notes) == (expected == "v_universe_latest")
    assert bs.source_for("v_other", None, "p", "US", "p.d", query) == "v_other" and len(seen) == 1   # no snapshot


def test_registry_views_exist_and_source_is_ascii():
    files = {p.name.split("_", 1)[1][:-4] for p in Path("sql/bigquery").glob("*.sql")}
    assert {v for v, _ in bs.SNAPSHOTS.values()} <= files
    assert set(bs.SNAPSHOTS) == {"snap_" + v[2:] for v, _ in bs.SNAPSHOTS.values()}
    src = Path(bs.__file__).read_text(encoding="utf-8")
    assert src.isascii() and bs.__version__ == "1.0.0" and "v1.0.0 (2026-11-24)" in src


def test_workflow_follows_the_collectors():
    wf = yaml.safe_load(Path(".github/workflows/bq_snapshots.yml").read_text(encoding="utf-8"))
    on = wf[True] if True in wf else wf["on"]
    assert on["workflow_run"]["workflows"] == ["🗄️ BigQuery Archive", "🔬 BigQuery Symbol Deep Data",
                                               "🌍 BigQuery Market Series", "🗄️ BigQuery Looker Views"]
    assert "schedule" not in on and set(on["workflow_dispatch"]["inputs"]) == {"write", "only"}
    job = wf["jobs"]["snapshots"]
    assert job["if"].startswith("github.event_name == 'workflow_dispatch' || github.event.workflow_run.conclusion == 'success'")
    assert job["concurrency"] == {"group": "tfb-bq-snapshots", "cancel-in-progress": False}
    env = job["steps"][-1]["env"]
    assert env["TFB_BQ_SNAPSHOTS"] == "${{ vars.TFB_BQ_SNAPSHOTS || 'on' }}" and "EODHD_API_KEY" not in env
    assert wf["permissions"] == {"contents": "read"}
