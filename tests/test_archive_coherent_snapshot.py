"""Actual capture/publisher with offline Sheets/BQ transport boundaries.

Reproduces the legacy partial-tab publication, tests capture-bound audits,
atomic SQL scope, immutable history, and jobs.get recovery after a committed
request loses its response. No credentials, providers or live BQ writes.
"""
import copy
import datetime as dt
import json
import ast
import itertools
from types import SimpleNamespace
from pathlib import Path
from urllib.parse import unquote

import pytest
import yaml

from scripts import archive_coherent_snapshot as co
from scripts import archive_to_bigquery as arc
from tests.test_bq_archive import FakeSession as LegacySession, Resp, SHEETS, PROJECT, DATASET, SID

SOURCE = "90001"
NOW = dt.datetime(2026, 11, 3, 0, 10, tzinfo=dt.timezone.utc)


def provenance(source=SOURCE, conclusion="failure"):
    return {"run": {"id": int(source), "path": co.release.SOURCE_WORKFLOW_PATH,
                    "head_branch": "main", "event": "schedule", "status": "completed",
                    "conclusion": conclusion, "run_attempt": 1, "head_sha": "3" * 40},
            "jobs": [{"name": co.release.FINAL_JOB_NAME, "status": "completed", "conclusion": "failure"}]}


def accepted(source=SOURCE):
    return {"run_id": source, "exit_code": 0, "audit_exit_codes": [0, 0], "blockers": [],
            "policy": {"expected_source_run": source},
            "audits": {"coverage": {"summary": {"exit_code": 0}},
                       "decision": {"summary": {"exit_code": 0}, "executable": True,
                                    "findings": [], "top10_run_riyadh": "2026-11-03 03:00:00",
                                    "portfolio_run_riyadh": "2026-11-03 03:00:00"}}}


def transport_admission(result, source, grids):
    """Transport tests stub the admission boundary; real-auditor tests do not."""
    good = result["audit_exit_codes"] == [0, 0]
    return {"schema": "synthetic-transport-admission", "accepted": good,
            "reason_codes": [] if good else ["SOURCE_CONTRACT_FAILED"],
            "display": {"trading_permission_granted": False}, "trading_permission_granted": False}


class Session:
    def __init__(self, grids=None):
        self.grids = grids or {tab: [["Symbol", "Value"], ["ABC.US", 10]] for tab in co.CAPTURE_TABS}
        self.calls, self.tables, self.jobs, self.uploads = [], {}, {}, {}
        self.fail_tab = self.change_tab = None
        self.get_counts = {}
        self.lose_load = self.lose_commit = self.fail_commit = self.collide_recovery = False
        self.history, self.receipts, self.production = {}, {}, {"prior-day": "untouched", "today": "last-good"}

    def get(self, url, **kwargs):
        self.calls.append(("GET", url, kwargs))
        if url.startswith(arc.SHEETS_API):
            tab = unquote(url.split("/values/")[1].split("?")[0]).strip("'")
            if tab == self.fail_tab:
                return Resp(503)
            self.get_counts[tab] = self.get_counts.get(tab, 0) + 1
            grid = copy.deepcopy(self.grids[tab])
            if tab == self.change_tab and self.get_counts[tab] > 1:
                grid[1][1] += 1
            return Resp(200, {"values": grid} if grid != [] else {})
        if "/tables/" in url:
            table = url.rsplit("/", 1)[1]
            return Resp(200, self.tables[table]) if table in self.tables else Resp(404)
        if "/jobs/" in url:
            job = url.split("/jobs/")[1].split("?")[0]
            return Resp(200, self.jobs[job]) if job in self.jobs else Resp(404)
        if "/queries/" in url:
            job = url.split("/queries/")[1].split("?")[0]
            if job.startswith("tfb_refresh_attempt_check_"):
                attempt = job.split("tfb_refresh_attempt_check_", 1)[1]
                rows = self.uploads.get("tfb_refresh_attempt_" + attempt, [])
                keys = ("attempt_id", "attempted_at", "state", "source_run_id", "capture_id", "reason", "diagnostic_json")
                return Resp(200, {"rows": [{"f": [{"v": row[key]} for key in keys]} for row in rows]})
            capture = job.split("tfb_capture_verify_", 1)[1].split("_", 1)[0]
            receipt = self.receipts.get(capture)
            current = max(self.receipts, key=lambda key: (
                int(self.receipts[key]["source_run_id"]), self.receipts[key]["source_attempt"],
                self.receipts[key]["captured_at"])) if self.receipts else None
            return Resp(200, {"rows": [{"f": [{"v": json.dumps(receipt)},
                                                  {"v": str(len(self.history[capture]) - 1)},
                                                  {"v": current}]}]} if receipt else {})
        raise AssertionError(url)

    def post(self, url, json=None, **kwargs):
        self.calls.append(("POST", url, copy.deepcopy(json)))
        if url.endswith("/tables"):
            self.tables[json["tableReference"]["tableId"]] = copy.deepcopy(json)
            return Resp(200, json)
        ref = json["jobReference"]
        job, config = ref["jobId"], json["configuration"]
        body = {"jobReference": ref, "configuration": copy.deepcopy(config), "status": {"state": "DONE"}}
        self.jobs[job] = body
        if "uploadType" in url:
            body["status"]["state"] = "RUNNING"
            return Resp(200, headers={"Location": "https://fake-upload.invalid/" + job})
        if job.startswith("tfb_capture_commit_"):
            capture = job.split("tfb_capture_commit_", 1)[1]
            stage_rows = self.uploads["tfb_capture_load_" + capture]
            receipt = json_load(next(r["record_json"] for r in stage_rows if r["tab"] == "_Capture_Receipt"))
            # Model the documented BQ transaction boundary, including rollback.
            newer = any((int(r["source_run_id"]), r["source_attempt"], r["captured_at"]) >
                        (int(receipt["source_run_id"]), receipt["source_attempt"], receipt["captured_at"])
                        for r in self.receipts.values())
            if capture not in self.receipts and (self.fail_commit or newer):
                body["status"]["errorResult"] = {"message": "newer capture already published" if newer else "transaction aborted"}
            elif capture not in self.receipts:
                self.production["today"] = copy.deepcopy(stage_rows)
                self.history[capture] = copy.deepcopy(stage_rows)
                self.receipts[capture] = receipt
            if self.lose_commit:
                self.lose_commit = False
                raise TimeoutError("committed before response")
        return Resp(200, body)

    def put(self, url, data=None, **kwargs):
        self.calls.append(("PUT", url, None))
        job = url.rsplit("/", 1)[1]
        self.uploads[job] = [json.loads(line) for line in data.decode().splitlines()]
        self.jobs[job]["status"]["state"] = "DONE"
        if self.collide_recovery:
            self.jobs[job]["configuration"]["load"]["writeDisposition"] = "WRITE_TRUNCATE"
        if self.lose_load:
            self.lose_load = False
            raise TimeoutError("loaded before response")
        return Resp(200)


def json_load(value):
    return json.loads(value)


def do_capture(session, **kwargs):
    async def audit(sheet, **args):
        # Contract exercised against the actual frozen reader; not a later live
        # read. Negative mutations in this data cannot be greened by another read.
        assert args["publish"] is False and args["run_id"] == SOURCE
        assert args["reader"](sheet, "My_Portfolio!A1:B2")[1][1] == 10
        return accepted()
    return co.capture(session, SID, SOURCE, provenance=lambda _: provenance(),
                      inflight=lambda: [], audit=audit, admission=transport_admission, now=NOW, **kwargs)


def test_valid_capture_is_the_audited_payload_and_retains_all_inputs():
    rows, receipt = do_capture(Session())
    assert receipt["audit_exit_codes"] == [0, 0] and receipt["feed_promoted"] is False
    assert {row["tab"] for row in rows} == set(co.CAPTURE_TABS) | {"_Capture_Receipt"}
    assert all(row["source_run_id"] is None for row in rows if row["tab"] == "_News_Items")
    assert all(row["source_run_id"] is None for row in rows if row["tab"] == "_Portfolio_CostBasis")
    assert all(row["source_run_id"] == SOURCE for row in rows if row["tab"] == "My_Portfolio")
    assert receipt["source_sha"] == "3" * 40 and receipt["audit_policy"]["expected_source_run"] == SOURCE


def test_regression_late_read_failure_prevents_legacy_partial_publication():
    legacy = LegacySession(bad_tabs={"Calendar_Events"})
    assert arc.run(legacy, sid=SID, project=PROJECT, dataset=DATASET, location="US",
                   tabs=[("_Selection_Log", 1), ("Calendar_Events", 1)],
                   snapshot_date="2026-11-03", captured_at=NOW.isoformat(), run_id="1", write=True) == 1
    assert legacy.uploads, "baseline publishes the first tab before the second read fails"
    current = Session()
    current.fail_tab = "Calendar_Events"
    with pytest.raises(co.Refused, match="Calendar_Events"):
        do_capture(current)
    assert not any(method in {"PUT", "POST"} for method, *_ in current.calls)


@pytest.mark.parametrize("tab", ["Global_Markets", "My_Portfolio", "_Portfolio_CostBasis", "_Status"])
def test_source_cell_changes_during_capture_withhold_every_write(tab):
    session = Session()
    session.change_tab = tab
    with pytest.raises(co.Refused, match="changed during capture"):
        do_capture(session)
    assert not session.jobs


def test_captured_bad_value_cannot_borrow_a_later_valid_audit():
    session = Session()
    session.grids["My_Portfolio"][1][1] = 0
    async def audit(sheet, **args):
        result = accepted()
        assert args["reader"](sheet, "My_Portfolio!A1:B2")[1][1] == 0
        result.update(exit_code=2, audit_exit_codes=[2, 0], blockers=["coverage_failed"])
        return result
    with pytest.raises(co.Refused, match="captured cohort withheld"):
        co.capture(session, SID, SOURCE, provenance=lambda _: provenance(), inflight=lambda: [], audit=audit)
    assert not session.jobs


@pytest.mark.parametrize("bad", [None, [["x"]] * (arc.MAX_ROWS_PER_TAB + 1), [["x"] * (arc.MAX_COLS + 1)]])
def test_malformed_or_truncated_grids_are_blocking(bad):
    session = Session()
    session.grids["Global_Markets"] = bad
    with pytest.raises(co.Refused):
        do_capture(session)
    assert not session.jobs


def test_empty_auxiliary_is_an_explicit_empty_capture_not_yesterdays_rows():
    session = Session()
    session.grids["_News_Items"] = []
    rows, receipt = do_capture(session)
    co.publish(session, PROJECT, DATASET, "US", rows, receipt)
    assert not [r for r in session.production["today"] if r["tab"] == "_News_Items"]
    sql = co.promotion_sql(PROJECT + "." + DATASET, "dashboard_stage_" + receipt["capture_id"])
    assert "DELETE FROM `proj-x.tfb_archive.news_items` WHERE snapshot_date = d" in sql


@pytest.mark.parametrize("conclusion", ["cancelled", "skipped", "timed_out", None])
def test_incomplete_source_provenance_never_reads_sheets(conclusion):
    session = Session()
    with pytest.raises(co.Refused):
        co.capture(session, SID, SOURCE, provenance=lambda _: provenance(conclusion=conclusion))
    assert session.calls == []


def test_writer_appearing_after_capture_withholds():
    session, calls = Session(), []
    async def audit(*args, **kwargs):
        return accepted()
    def inflight():
        calls.append(1)
        return [] if len(calls) == 1 else [{"id": 99999}]
    with pytest.raises(co.Refused, match="writer started"):
        co.capture(session, SID, SOURCE, provenance=lambda _: provenance(), audit=audit,
                   admission=transport_admission, inflight=inflight)


def test_schema_mismatch_never_relabels_existing_table():
    session = Session()
    session.tables["my_portfolio"] = {"schema": {"fields": arc.SCHEMA},
        "tableReference": {"projectId": PROJECT, "datasetId": DATASET, "tableId": "my_portfolio"},
        "timePartitioning": {"type": "HOUR", "field": "snapshot_date"}}
    with pytest.raises(co.Refused, match="incompatible"):
        co.ensure_table(session, PROJECT, DATASET, "my_portfolio", arc.SCHEMA)
    assert not [call for call in session.calls if call[0] == "POST"]


@pytest.mark.parametrize("status", [403, 503])
def test_unreadable_table_is_never_treated_as_missing(status):
    session = Session()
    original = session.get
    def unreadable(url, **kwargs):
        if "/tables/" in url:
            session.calls.append(("GET", url, kwargs))
            return Resp(status)
        return original(url, **kwargs)
    session.get = unreadable
    with pytest.raises(co.Refused, match="unavailable/incompatible"):
        co.ensure_table(session, PROJECT, DATASET, "my_portfolio", arc.SCHEMA)
    assert not any(call[0] in {"POST", "PUT"} for call in session.calls)


def test_racing_table_creation_requires_compatible_authoritative_readback():
    session = Session()
    reads = []
    def read(url, **kwargs):
        reads.append(url)
        return Resp(404 if len(reads) == 1 else 200, {"schema": {"fields": []}})
    session.get = read
    def race(url, **kwargs):
        session.calls.append(("POST", url, kwargs))
        return Resp(409)
    session.post = race
    with pytest.raises(co.Refused, match="incompatible"):
        co.ensure_table(session, PROJECT, DATASET, "my_portfolio", arc.SCHEMA)
    assert len(reads) == 2 and len(session.calls) == 1


@pytest.mark.parametrize("status", [403, 503])
def test_unreadable_prior_job_prevents_submission(status):
    session = Session()
    session.get = lambda *args, **kwargs: Resp(status)
    with pytest.raises(co.Unknown, match="readback unavailable"):
        co.run_job(session, PROJECT, "US", "stable_id", {"query": {"query": "SELECT 1"}})
    assert session.calls == []


def test_receipt_wait_requires_real_consumed_outcome_not_a_successful_exit():
    from tests.test_await_cockpit_receipts import key_grid, pf, t10, verified, SOURCE as WAIT_SOURCE
    session = Session()
    session.grids["_Status"] = [[""] * 11 + row for row in key_grid()]
    session.grids["Portfolio_Decision"] = pf(verified(WAIT_SOURCE))
    session.grids["Top_10_Investments"] = t10(verified(WAIT_SOURCE))
    co.wait_for_receipts(session, SID, WAIT_SOURCE, max_seconds=0)
    session.grids["Top_10_Investments"][1][1] = session.grids["Top_10_Investments"][1][1].replace("status: ok", "status: RUNNING")
    with pytest.raises(co.Refused, match="receipts not accepted"):
        co.wait_for_receipts(session, SID, WAIT_SOURCE, max_seconds=0)
    session.grids["Top_10_Investments"] = t10("")
    with pytest.raises(co.Refused, match="receipts not accepted"):
        co.wait_for_receipts(session, SID, WAIT_SOURCE, max_seconds=0)
    assert not session.jobs


@pytest.mark.parametrize("loss", ["lose_load", "lose_commit"])
def test_committed_then_timeout_recovers_by_exact_job_and_retry_has_no_duplicates(loss):
    session = Session()
    rows, receipt = do_capture(session)
    setattr(session, loss, True)
    actual, current = co.publish(session, PROJECT, DATASET, "US", rows, receipt)
    assert actual == receipt and current and len(session.history) == 1
    before = copy.deepcopy(session.production)
    def mutations():
        return [call for call in session.calls if call[0] in {"POST", "PUT"}
                and not (call[0] == "POST" and call[2].get("jobReference", {}).get("jobId", "").startswith("tfb_capture_verify_"))]
    posts = len(mutations())
    # New runner read time, same source/content identity: the persisted stage's
    # original receipt wins, not a fabricated fresh capture timestamp.
    later = copy.deepcopy(receipt)
    later["captured_at"] = "2026-11-04T12:00:00+00:00"
    assert co.publish(session, PROJECT, DATASET, "US", rows, later) == (receipt, True)
    assert session.production == before and len(session.history) == 1
    assert len(mutations()) == posts
    assert session.production["prior-day"] == "untouched"


def test_unknown_submission_is_not_blindly_replayed():
    session = Session()
    def lost(*args, **kwargs):
        session.calls.append(("POST", args[0], kwargs))
        raise TimeoutError("no authoritative job readback")
    session.post = lost
    with pytest.raises(co.Unknown):
        co.run_job(session, PROJECT, "US", "stable_job", {"query": {"query": "SELECT 1"}})
    assert len([call for call in session.calls if call[0] == "POST"]) == 1


def test_recovered_job_identity_is_verified_not_just_its_done_state():
    session = Session()
    rows, receipt = do_capture(session)
    session.lose_load = session.collide_recovery = True
    with pytest.raises(co.Refused, match="configuration"):
        co.publish(session, PROJECT, DATASET, "US", rows, receipt)
    assert session.receipts == {} and session.production["today"] == "last-good"


def test_transaction_failure_keeps_every_production_table_and_history_unchanged():
    session = Session()
    rows, receipt = do_capture(session)
    session.fail_commit = True
    before = copy.deepcopy(session.production)
    with pytest.raises(co.Refused, match="failed"):
        co.publish(session, PROJECT, DATASET, "US", rows, receipt)
    assert session.production == before and session.history == session.receipts == {}


def test_receipt_transport_failure_after_commit_is_unknown_not_a_failed_publication():
    session = Session()
    rows, receipt = do_capture(session)
    original = session.get
    def fault(url, **kwargs):
        if "/queries/" in url:
            raise TimeoutError("receipt read transport failure")
        return original(url, **kwargs)
    session.get = fault
    with pytest.raises(co.Unknown, match="commit confirmed"):
        co.publish(session, PROJECT, DATASET, "US", rows, receipt)
    assert receipt["capture_id"] in session.receipts


@pytest.mark.parametrize("newer", ["source", "attempt", "time"])
def test_late_old_capture_cannot_roll_back_a_newer_receipt(newer):
    session = Session()
    rows, receipt = do_capture(session)
    prior = copy.deepcopy(receipt)
    if newer == "source":
        prior["source_run_id"] = "90002"
    elif newer == "attempt":
        prior["source_attempt"] = 2
    else:
        prior["captured_at"] = "2026-11-03T01:00:00+00:00"
    session.receipts["newer"] = prior
    with pytest.raises(co.Refused):
        co.publish(session, PROJECT, DATASET, "US", rows, receipt)
    assert session.production["today"] == "last-good" and session.history == {}


def test_transaction_sql_is_all_tabs_one_commit_with_parameterized_dates_and_history():
    sql = co.promotion_sql(PROJECT + "." + DATASET, "dashboard_stage_" + "a" * 64)
    assert sql.count("BEGIN TRANSACTION;") == sql.count("COMMIT TRANSACTION;") == 1
    assert sql.count("DELETE FROM") == len(arc.TABS)
    assert sql.count("WHERE snapshot_date = d") == len(arc.TABS)
    for table in [arc.table_name(tab) for tab, _ in arc.TABS]:
        assert f"INSERT INTO `proj-x.tfb_archive.{table}`" in sql
    assert "source_attempt > a" in sql and "captured_at > t" in sql
    assert sql.index("IF NOT EXISTS") < sql.index("ASSERT NOT EXISTS")
    assert "dashboard_snapshot_rows" in sql and "dashboard_snapshot_receipts" in sql
    assert "CREATE OR REPLACE" not in sql and "DROP" not in sql


def test_old_confirmed_capture_is_not_claimed_current_after_a_newer_capture():
    session = Session()
    rows, receipt = do_capture(session)
    assert co.publish(session, PROJECT, DATASET, "US", rows, receipt)[1] is True
    prior_data = copy.deepcopy(session.production)
    newer = copy.deepcopy(receipt)
    newer["source_run_id"] = "90002"
    session.receipts["newer"] = newer
    assert co.publish(session, PROJECT, DATASET, "US", rows, receipt)[1] is False
    assert session.production == prior_data


def test_content_identity_is_stable_across_capture_times_but_changes_with_data():
    session = Session()
    _rows, first = do_capture(session)
    _rows, second = do_capture(session)
    assert first["capture_id"] == second["capture_id"]
    session.grids["_News_Items"][1][1] = 99
    _rows, changed = do_capture(session)
    assert first["capture_id"] != changed["capture_id"]


@pytest.mark.parametrize("after,master", [(None, "on"), ("off", "on"), ("on", "off")])
def test_master_or_feature_off_makes_no_request(monkeypatch, after, master, tmp_path):
    monkeypatch.setenv("TFB_BQ_ARCHIVE", master)
    if after is None:
        monkeypatch.delenv(co.GATE, raising=False)
    else:
        monkeypatch.setenv(co.GATE, after)
    monkeypatch.setattr(arc, "make_session", lambda: pytest.fail("gate-off must not authenticate"))
    assert co.main(["--write", "--json-out", str(tmp_path / "out.json")]) == 0
    assert not (tmp_path / "out.json").exists()


@pytest.mark.parametrize("failure,expected_calls,code", [
    ("source changed during capture: _Status", 2, 0),
    ("source provenance or writer state is not accepted", 1, 2),
])
def test_cli_retries_only_one_prewrite_capture_change(monkeypatch, tmp_path, failure, expected_calls, code):
    for key, value in {co.GATE: "on", "TFB_BQ_ARCHIVE": "on", "TFB_BQ_PROJECT": PROJECT,
                       "DEFAULT_SPREADSHEET_ID": SID}.items():
        monkeypatch.setenv(key, value)
    session, calls = Session(), []
    rows, receipt = do_capture(session)
    monkeypatch.setattr(arc, "make_session", lambda: session)
    def read(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise co.Refused(failure)
        return rows, receipt
    monkeypatch.setattr(co, "capture", read)
    monkeypatch.setattr(co, "publish", lambda *a, **k: pytest.fail("dry run must not publish"))
    path = tmp_path / "receipt.json"
    assert co.main(["--source-run", SOURCE, "--json-out", str(path)]) == code
    assert len(calls) == expected_calls
    assert json.loads(path.read_text())["published"] is False
    assert not session.jobs


def test_refused_count_mismatch_persists_latest_diagnostic_without_changing_data(monkeypatch, tmp_path):
    for key, value in {co.GATE: "on", "TFB_BQ_ARCHIVE": "on", "TFB_BQ_PROJECT": PROJECT,
                       "TFB_BQ_DATASET": DATASET, "DEFAULT_SPREADSHEET_ID": SID}.items():
        monkeypatch.setenv(key, value)
    session = Session()
    before = copy.deepcopy(session.production)
    monkeypatch.setattr(arc, "make_session", lambda: session)
    def refused(*args, **kwargs):
        raise co.Refused("captured cohort withheld: consumed holdings differ", details={
            "reason_codes": ["PORTFOLIO_CONSUMED_COUNT_MISMATCH"],
            "display": {"consumed_holdings": 6, "published_holdings": 2, "trading_permission_granted": False}})
    monkeypatch.setattr(co, "capture", refused)
    path = tmp_path / "diagnostic.json"
    assert co.main(["--write", "--source-run", SOURCE, "--json-out", str(path)]) == 2
    report = json.loads(path.read_text())
    assert report["latest_diagnostic"]["state"] == "WITHHELD"
    assert report["latest_diagnostic"]["readback"] == "VERIFIED" and not report["published"]
    assert session.production == before and session.history == session.receipts == {}
    assert not any(key.startswith("tfb_capture_") for key in session.jobs)
    row = next(rows[0] for job, rows in session.uploads.items() if job.startswith("tfb_refresh_attempt_"))
    evidence = json.loads(row["diagnostic_json"])
    assert evidence["details"]["display"]["consumed_holdings"] == 6
    assert evidence["details"]["display"]["published_holdings"] == 2
    assert evidence["feed_promoted"] is False and evidence["trading_permission_granted"] is False


def test_diagnostic_outage_is_explicit_unknown_and_does_not_promote_bad_source(monkeypatch, tmp_path):
    for key, value in {co.GATE: "on", "TFB_BQ_ARCHIVE": "on", "TFB_BQ_PROJECT": PROJECT,
                       "DEFAULT_SPREADSHEET_ID": SID}.items():
        monkeypatch.setenv(key, value)
    session = Session()
    monkeypatch.setattr(arc, "make_session", lambda: session)
    monkeypatch.setattr(co, "capture", lambda *a, **k: (_ for _ in ()).throw(co.Refused("source inconsistent")))
    monkeypatch.setattr(co, "publish_attempt", lambda *a, **k: (_ for _ in ()).throw(TimeoutError("BQ unavailable")))
    path = tmp_path / "unknown.json"
    assert co.main(["--write", "--source-run", SOURCE, "--json-out", str(path)]) == 3
    report = json.loads(path.read_text())
    assert report["latest_diagnostic"]["state"] == "UNKNOWN" and report["published"] is False
    assert session.production["today"] == "last-good" and not session.jobs


def test_diagnostic_uncertain_append_recovers_original_time_and_rejects_conflicting_payload(monkeypatch):
    monkeypatch.setenv("GITHUB_RUN_ID", "123456")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    session = Session()
    out = {"published": False, "outcome": "withheld", "reason": "source mismatch", "diagnostic": {"reason_codes": ["SOURCE_COHORT_UNPROVEN"]}}
    session.lose_load = True
    first = co.publish_attempt(session, PROJECT, DATASET, "US", out, SOURCE)
    second = co.publish_attempt(session, PROJECT, DATASET, "US", out, SOURCE)
    assert first == second and first["state"] == "WITHHELD"
    assert len(session.uploads) == 1 and not session.history and session.production["today"] == "last-good"
    changed = {**out, "published": True}
    with pytest.raises(co.Unknown, match="content differs"):
        co.publish_attempt(session, PROJECT, DATASET, "US", changed, SOURCE)
    assert len(session.uploads) == 1


def test_status_query_keeps_diagnostic_data_time_and_trading_permission_separate():
    sql = (Path(__file__).resolve().parents[1] / "docs/evidence/dashboard_refresh_status_query.sql").read_text()
    assert "latest_attempt_at" in sql and "coherent_data_captured_at" in sql and "coherent_publication_transaction_at" in sql
    assert "c.committed_at > a.attempted_at" in sql and "c.capture_id IS NULL" in sql
    assert "SAFE_CAST(c.source_run_id AS INT64) > SAFE_CAST(a.source_run_id AS INT64)" in sql
    assert "FALSE AS trading_permission_granted" in sql
    assert "a.attempted_at IS NULL AND c.capture_id IS NULL" in sql
    assert "LEFT JOIN attempt AS a ON TRUE" in sql
    assert "coherent_copy_age_hours" in sql and "diagnostic_age_minutes" in sql


def test_workflow_scope_and_private_serialization_preserve_legacy_off_path():
    text = (Path(__file__).resolve().parents[1] / ".github/workflows/bq_archive.yml").read_text()
    wf = yaml.safe_load(text)
    event = wf.get(True) or wf["on"]
    assert event["workflow_run"] == {"workflows": ["🏦 Tadawul Dashboard Advanced Sync"], "types": ["completed"]}
    assert "concurrency" not in wf
    job = wf["jobs"]["archive"]
    for predicate in ["head_branch == 'main'", "head_repository.full_name == github.repository",
                      "workflow_run.event", "workflow_run.conclusion", "vars.TFB_BQ_AFTER_SYNC"]:
        assert predicate in job["if"]
    assert "tadawul-production-write" not in str(job["concurrency"])
    assert job["concurrency"]["cancel-in-progress"] is False
    run = next(s for s in job["steps"] if s["name"].endswith("Run the archive"))
    assert "*) python scripts/archive_to_bigquery.py $MODE" in run["run"]
    assert "TFB_BQ_AFTER_SYNC" in run["env"] and "--source-run" in run["run"]
    assert "archive_coherent_snapshot.py" in run["run"]


def test_actual_workflow_condition_truth_table():
    wf = yaml.safe_load((Path(__file__).resolve().parents[1] / ".github/workflows/bq_archive.yml").read_text())
    expression = wf["jobs"]["archive"]["if"].strip()[3:-2].strip()
    expression = expression.replace("&&", " and ").replace("||", " or ")
    tree = ast.parse(expression, mode="eval")
    allowed = (ast.Expression, ast.BoolOp, ast.And, ast.Or, ast.Compare, ast.Eq, ast.NotEq,
               ast.Call, ast.Name, ast.Load, ast.Attribute, ast.Constant, ast.List)
    assert all(isinstance(node, allowed) for node in ast.walk(tree))
    for top_event, source_event, branch, repo, conclusion, gate in itertools.product(
            ["workflow_run", "schedule", "workflow_dispatch"],
            ["schedule", "workflow_dispatch", "push", "pull_request"],
            ["main", "feature"], ["owner/repo", "fork/repo"],
            ["success", "failure", "cancelled", "skipped"], ["on", "off"]):
        github = SimpleNamespace(event_name=top_event, repository="owner/repo",
            event=SimpleNamespace(workflow_run=SimpleNamespace(event=source_event, head_branch=branch,
                head_repository=SimpleNamespace(full_name=repo), conclusion=conclusion)))
        expected = top_event != "workflow_run" or (gate == "on" and branch == "main" and repo == "owner/repo"
            and source_event in {"schedule", "workflow_dispatch"} and conclusion in {"success", "failure"})
        actual = eval(compile(tree, "workflow-if", "eval"), {"__builtins__": {}},
                      {"github": github, "vars": SimpleNamespace(TFB_BQ_AFTER_SYNC=gate),
                       "contains": lambda seq, value: value in seq, "fromJSON": json.loads})
        assert actual == expected
