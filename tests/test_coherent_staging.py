"""Staging cannot authenticate or write against a production dataset."""
import copy
from pathlib import Path

import pytest
import yaml

from scripts import archive_coherent_snapshot as co
from scripts import validate_coherent_archive_staging as stage
from tests.test_archive_coherent_snapshot import Session, Resp


@pytest.mark.parametrize("dataset", ["tfb_archive", "", "other_project.dataset", "tfb_archive_staging_", "tfb_archive_staging_x;DROP"])
def test_nonstaging_target_is_refused_before_authentication(monkeypatch, dataset):
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-x")
    monkeypatch.setenv("TFB_BQ_DATASET", dataset)
    monkeypatch.setattr(co.arc, "make_session", lambda: pytest.fail("must not authenticate"))
    with pytest.raises(co.Refused, match="isolated"):
        stage.main(["--write"])


def test_staging_dry_run_neither_authenticates_nor_changes_global_flags(monkeypatch):
    monkeypatch.setenv("TFB_BQ_PROJECT", "proj-x")
    monkeypatch.setenv("TFB_BQ_DATASET", "tfb_archive_staging_test")
    monkeypatch.setenv(co.GATE, "on")
    monkeypatch.setenv("TFB_BQ_ARCHIVE", "on")
    monkeypatch.setattr(co.arc, "make_session", lambda: pytest.fail("dry run must not authenticate"))
    assert stage.main([]) == 0
    assert co.arc._env("TFB_BQ_DATASET") == "tfb_archive_staging_test"


def test_staging_fixture_is_explicitly_not_source_or_trading_certification():
    rows, receipt = stage.fixture()
    assert receipt["schema"] == "tfb.synthetic-publication-test/v1"
    assert receipt["capture_contract"]["accepted"] is False
    assert receipt["capture_contract"]["trading_permission_granted"] is False
    assert receipt["feed_promoted"] is False and all(row["source_run_id"] is None for row in rows[:-1])


def test_staging_dispatch_is_explicit_and_uses_existing_scoped_credentials():
    text = (Path(__file__).resolve().parents[1] / ".github/workflows/bq_archive.yml").read_text()
    wf = yaml.safe_load(text)
    event = wf.get(True) or wf["on"]
    inputs = event["workflow_dispatch"]["inputs"]
    assert inputs["staging_dataset"]["default"] == "" and inputs["staging_checks"]["default"] is False
    step = next(s for s in wf["jobs"]["archive"]["steps"] if s["name"].endswith("Run the archive"))
    assert "^tfb_archive_staging_[A-Za-z0-9_]{1,128}$" in step["run"]
    assert step["run"].index("Synthetic staging checks cannot target") < step["run"].index("validate_coherent_archive_staging.py")
    assert "secrets.GOOGLE_SHEETS_CREDENTIALS" in step["env"]["GOOGLE_SHEETS_CREDENTIALS"]
    assert "secrets." not in step["run"]


def test_staging_run_orchestrates_real_publisher_with_isolated_transport_boundaries(monkeypatch):
    """Exercise orchestration, not a claim that the local fake validates BQ SQL.

    Actual publish/job/append/readback helpers run. This transport models the
    documented transaction error boundary; only signature/status SELECT results
    are supplied locally, with explicit table-existence and isolation checks.
    """
    project, dataset, location = "proj-x", "tfb_archive_staging_orchestration", "US"

    class StagingSession(Session):
        rollback_seen = False

        def get(self, url, **kwargs):
            if url == f"{co.arc.BQ_API}/projects/{project}/datasets/{dataset}":
                self.calls.append(("GET", url, kwargs))
                return Resp(200, {"datasetReference": {"projectId": project, "datasetId": dataset}})
            assert not url.startswith(co.arc.SHEETS_API), "synthetic checks must not read real Sheets"
            return super().get(url, **kwargs)

        def post(self, url, json=None, **kwargs):
            before = copy.deepcopy(self.production)
            response = super().post(url, json=json, **kwargs)
            reference = json.get("jobReference", {})
            job = reference.get("jobId", "")
            sql = json.get("configuration", {}).get("query", {}).get("query", "")
            if job.startswith("tfb_staging_rollback_"):
                assert f"DELETE FROM `{project}.{dataset}.my_portfolio`" in sql
                assert sql.index("DELETE FROM") < sql.index("ASSERT FALSE") < sql.index("COMMIT TRANSACTION")
                self.rollback_seen = True
                self.jobs[job]["status"]["errorResult"] = {"message": "TFB_STAGING_ROLLBACK_EXPECTED"}
                # Rollback is modeled at the transport boundary; raw state is
                # never treated as changed merely because DELETE was submitted.
                assert self.production == before
            if job.startswith("tfb_capture_commit_") and self.jobs[job]["status"].get("errorResult"):
                assert "newer capture already published" in sql
                self.jobs[job]["status"]["errorResult"] = {"message": "newer capture already published"}
                assert self.production == before
            return response

    session = StagingSession()
    status_reads = []
    monkeypatch.setenv("GITHUB_RUN_ID", "734645")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")

    def read_query(actual_session, actual_project, actual_location, sql):
        assert (actual_session, actual_project, actual_location) == (session, project, location)
        assert f"`{project}.tfb_archive." not in sql
        assert "`project-2e3530e6-2311-4aa3-b36.tfb_archive." not in sql
        if " UNION ALL " in sql:
            result = []
            for tab, _ in co.arc.TABS:
                table = co.arc.table_name(tab)
                assert table in session.tables and f"`{project}.{dataset}.{table}`" in sql
                data = [row for row in session.production["today"] if row["tab"] == tab]
                result.append([table, str(len(data)), co.digest(data)])
            return result
        assert co.ATTEMPTS in session.tables, "initialize empty diagnostic table before the first status query"
        assert co.table_schema_ok(session.tables[co.ATTEMPTS], co.ATTEMPT_SCHEMA)
        diagnostics = [row for key, rows in session.uploads.items()
                       if key.startswith("tfb_refresh_attempt_") for row in rows]
        status_reads.append(copy.deepcopy(diagnostics))
        if len(status_reads) == 1:
            assert diagnostics == [], "first singleton read must exercise no diagnostic rows"
            assert session.receipts, "coherent receipt already exists"
        else:
            assert len(diagnostics) == 1
            assert diagnostics[0]["state"] == "WITHHELD"
            assert diagnostics[0]["reason"] == "STAGING_SOURCE_FAILURE"
        return [["one singleton status row"]]

    monkeypatch.setattr(stage, "query", read_query)
    result = stage.run(session, project, dataset, location)
    assert session.rollback_seen and len(status_reads) == 2
    assert len(session.history) == len(session.receipts) == 1
    assert result["real_source_policy_capture"] == "PENDING"
    assert result["synthetic_only"] is True and result["trading_permission_granted"] is False
    assert result["replay"] == result["rollback"] == result["diagnostic_readback"] == "PASS"
    assert session.production["prior-day"] == "untouched"
    for job in session.jobs.values():
        configuration = job["configuration"]
        if "load" in configuration:
            assert configuration["load"]["destinationTable"]["datasetId"] == dataset
        if "query" in configuration:
            assert f"`{project}.tfb_archive." not in configuration["query"]["query"]
