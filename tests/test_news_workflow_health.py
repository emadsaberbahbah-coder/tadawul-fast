"""News workflow repairs retain genuine incomplete-discovery failures."""
from __future__ import annotations

import argparse
import asyncio
import copy
from datetime import datetime, timezone
import importlib.util
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

if os.getenv("TFB_NEWS_HEALTH_SOURCE"):
    spec = importlib.util.spec_from_file_location("news_health_under_test", os.environ["TFB_NEWS_HEALTH_SOURCE"])
    archive = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = archive
    spec.loader.exec_module(archive)
else:
    from scripts import news_archive_collector as archive

NOW = datetime(2026, 10, 24, 6, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def offline_clock(monkeypatch):
    monkeypatch.setattr(archive, "_utc_now", lambda: NOW)
    monkeypatch.setattr(archive, "_riyadh_date", lambda: "2026-10-24")
    monkeypatch.delenv("GITHUB_STEP_SUMMARY", raising=False)
    monkeypatch.delenv("NEWS_ARCHIVE_UNIVERSE", raising=False)


def test_bmv_ampersand_identity_is_preserved_exactly_without_aliasing():
    grid = [["Symbol", "Name"], ["PE&OLES.MX", "Industrias Peñoles, S.A.B. de C.V."]]
    before = copy.deepcopy(grid)
    diagnostics = {}
    pairs, header, complete = archive._identity_pairs(grid, diagnostics=diagnostics)
    assert pairs == [("PE&OLES.MX", "Industrias Peñoles, S.A.B. de C.V.")]
    assert header and complete and diagnostics["invalid_identity_rows"] == 0
    assert archive._unique_symbols([pairs[0][0]]) == ["PE&OLES.MX"]
    assert grid == before


@pytest.mark.parametrize("symbol", ["PE&OLES.US", "PE&&OLES.MX", "&PEOLES.MX", "PEOLES&.MX",
                                   "PE&OLES.MX;X", "PE&OLES.MX&X", "PE&OLES.MX?x=y"])
def test_ampersand_extension_does_not_accept_other_venues_or_query_delimiters(symbol):
    diagnostics = {}
    pairs, header, complete = archive._identity_pairs(
        [["Symbol", "Name"], [symbol, "Industrias Peñoles"]], diagnostics=diagnostics)
    assert pairs == [] and header and complete
    assert diagnostics["invalid_identity_rows"] == 1


def test_valid_ampersand_syntax_does_not_invent_a_missing_company_name():
    diagnostics = {}
    pairs, _, _ = archive._identity_pairs([["Symbol", "Name"], ["PE&OLES.MX", ""]], diagnostics=diagnostics)
    assert pairs == [] and diagnostics["invalid_identity_rows"] == 1
    assert diagnostics["unknown_identity_symbols"] == ["PE&OLES.MX"]


def report(*, source=True, identity=False, acquisition=True):
    return {"selection": {"mode": "adaptive", "selected_count": 1, "queried_count": 1,
                          "analysis_cap": 60, "rotation_count": 0,
                          "source_read_complete": source,
                          "discovery_identity_coverage_complete": identity,
                          "discovery_complete": source and identity,
                          "unknown_identity_symbols": ["PRIVATE_MEMBERSHIP.US"]},
            "selected_acquisition_complete": acquisition,
            "complete": source and identity and acquisition,
            "status_counts": {"OK": 1} if acquisition else {"UNKNOWN": 1}}


def test_accepted_archive_and_incomplete_identity_discovery_are_reported_separately(
        monkeypatch, tmp_path, capsys):
    summary = tmp_path / "step.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    evidence = tmp_path / "evidence.json"
    result = report()
    archive._finish_collection_report(SimpleNamespace(evidence_json=str(evidence)), result,
                                      "APPEND_ACCEPTED", attempted_rows=1, accepted_rows=1)
    assert result["selected_acquisition_complete"] and not result["complete"]
    assert result["failure_codes"] == ["identity_coverage_incomplete"]
    assert result["archive_write"]["accepted_rows"] == 1
    assert json.loads(evidence.read_text())["failure_codes"] == ["identity_coverage_incomplete"]
    rendered = summary.read_text()
    assert "| Selected acquisition | COMPLETE |" in rendered
    assert "| Selection / discovery checks | INCOMPLETE |" in rendered
    assert "APPEND_ACCEPTED" in rendered and "identity_coverage_incomplete" in rendered
    assert "PRIVATE_MEMBERSHIP" not in rendered + capsys.readouterr().out


def test_source_read_and_provider_acquisition_failures_have_distinct_safe_codes():
    assert archive._collection_failure_codes(report(source=False)) == [
        "identity_source_read_incomplete", "identity_coverage_incomplete"]
    assert archive._collection_failure_codes(report(identity=True, acquisition=False)) == [
        "selected_acquisition_incomplete"]
    assert archive._collection_failure_codes(report(identity=True)) == []


def test_summary_does_not_publish_arbitrary_error_text(monkeypatch, tmp_path):
    path = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(path))
    result = report(identity=True)
    result.update(complete=False, error_code="PRIVATE credential value")
    archive._finish_collection_report(SimpleNamespace(evidence_json=None), result, "WITHHELD")
    assert result["failure_codes"] == ["collection_error"]
    assert "PRIVATE" not in path.read_text()


def test_discovery_gap_retains_nonzero_exit_and_successfully_acquired_archive(
        monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_ARCHIVE_UNIVERSE", "PE&OLES.MX")
    context = {"identities": {"PE&OLES.MX": "Industrias Peñoles"}, "portfolio_membership": [],
               "research": [], "pool": [], "eligible": [], "sources": [],
               "complete": False, "source_read_complete": True, "identity_coverage_complete": False}
    written = []
    calls = []

    class Store:
        sheet_name = "News_Archive"
        def __init__(self, *_args, **_kwargs):
            pass
        def load_news_source_context(self):
            return context
        def is_available(self):
            return True
        def existing_keys_for_date(self, _date):
            return {row[1] for row in written}
        def append(self, rows):
            written.extend(rows)
            return True

    async def batch(items, **_kwargs):
        calls.append(copy.deepcopy(items))
        return SimpleNamespace(items=[{"symbol": item["symbol"], "articles_analyzed": 0,
                                       "collection_evidence": {"status": "EMPTY"}} for item in items], meta={})

    monkeypatch.setattr(archive, "NewsArchiveStore", Store)
    monkeypatch.setattr(archive, "_import_news_module", lambda: SimpleNamespace(batch_news_intelligence=batch))
    path = tmp_path / "evidence.json"
    args = argparse.Namespace(universe=None, adaptive=True, max_symbols=60, sheet_id="test",
                              dry_run=False, evidence_json=str(path))
    assert asyncio.run(archive.collect(args)) == 1
    first = json.loads(path.read_text())
    assert first["archive_write"]["status"] == "APPEND_ACCEPTED" and len(written) == 1
    assert first["failure_codes"] == ["identity_coverage_incomplete"]
    assert calls == [[{"symbol": "PE&OLES.MX", "company_name": "Industrias Peñoles"}]]
    context.update(complete=True, identity_coverage_complete=True)
    assert asyncio.run(archive.collect(args)) == 0
    second = json.loads(path.read_text())
    assert second["archive_write"]["status"] == "SKIPPED_EXISTING" and len(written) == 1
    assert second["failure_codes"] == []


def test_news_workflow_keeps_single_writer_and_preserves_evidence_on_failure():
    import yaml
    workflow = yaml.load((Path(__file__).parents[1] / ".github/workflows/news_archive.yml").read_text(),
                         Loader=yaml.BaseLoader)
    assert workflow["concurrency"] == {"group": "news-archive", "cancel-in-progress": "false"}
    step = next(s for s in workflow["jobs"]["collect"]["steps"] if s.get("name") == "Preserve news acquisition evidence")
    assert step["if"] == "always()" and step["uses"] == "actions/upload-artifact@v7"
    assert "continue-on-error" not in workflow["jobs"]["collect"]


# --- v1.3.3: identity-gap policy (ENV-gated; default fail keeps v1.3.2 exits) ---

def _gap_store_and_batch(monkeypatch, context, written):
    class Store:
        sheet_name = "News_Archive"
        def __init__(self, *_args, **_kwargs):
            pass
        def load_news_source_context(self):
            return context
        def is_available(self):
            return True
        def existing_keys_for_date(self, _date):
            return {row[1] for row in written}
        def append(self, rows):
            written.extend(rows)
            return True

    async def batch(items, **_kwargs):
        return SimpleNamespace(items=[{"symbol": item["symbol"], "articles_analyzed": 0,
                                       "collection_evidence": {"status": "EMPTY"}} for item in items], meta={})

    monkeypatch.setattr(archive, "NewsArchiveStore", Store)
    monkeypatch.setattr(archive, "_import_news_module", lambda: SimpleNamespace(batch_news_intelligence=batch))


def _gap_context():
    return {"identities": {"PE&OLES.MX": "Industrias Penoles"}, "portfolio_membership": [],
            "research": [], "pool": [], "eligible": [], "sources": [],
            "complete": False, "source_read_complete": True, "identity_coverage_complete": False}


@pytest.mark.parametrize("value", ["", "fail", "FAIL", "warning", "1", "true"])
def test_identity_gap_policy_defaults_to_fail_for_every_other_value(monkeypatch, value):
    if value:
        monkeypatch.setenv("TFB_NEWS_IDENTITY_GAP_POLICY", value)
    else:
        monkeypatch.delenv("TFB_NEWS_IDENTITY_GAP_POLICY", raising=False)
    assert archive._identity_gap_policy() == "fail"
    assert archive._identity_gap_tolerated({"failure_codes": ["identity_coverage_incomplete"],
                                            "selected_acquisition_complete": True,
                                            "archive_write": {"status": "APPEND_ACCEPTED"},
                                            "selection": _ok_selection()}) is False


def test_identity_gap_policy_warn_turns_only_the_pure_gap_into_exit_zero(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("TFB_NEWS_IDENTITY_GAP_POLICY", "warn")
    monkeypatch.setenv("NEWS_ARCHIVE_UNIVERSE", "PE&OLES.MX")
    written = []
    _gap_store_and_batch(monkeypatch, _gap_context(), written)
    path = tmp_path / "evidence.json"
    args = argparse.Namespace(universe=None, adaptive=True, max_symbols=60, sheet_id="test",
                              dry_run=False, evidence_json=str(path))
    assert asyncio.run(archive.collect(args)) == 0
    first = json.loads(path.read_text())
    assert first["archive_write"]["status"] == "APPEND_ACCEPTED" and len(written) == 1
    # evidence is unchanged in substance: the gap is still a failure code and complete stays false
    assert first["failure_codes"] == ["identity_coverage_incomplete"]
    assert first["complete"] is False and first["selected_acquisition_complete"] is True
    assert first["identity_gap_policy"] == "warn" and first["identity_gap_tolerated"] is True
    out = capsys.readouterr().out
    assert "::warning::" in out and "identity_coverage_incomplete" in out
    assert "PE&OLES" not in out.split("::warning::", 1)[1]
    # second run: nothing new to write, still exit 0 under warn
    assert asyncio.run(archive.collect(args)) == 0
    assert json.loads(path.read_text())["archive_write"]["status"] == "SKIPPED_EXISTING"


def _ok_selection(**over):
    selection = {"selected_count": 1, "missing_identity_symbols": [], "omitted_priority_count": 0,
                 "lifecycle_registry": {"status": "VALID", "map_sha256": "x"}, "lifecycle_exclusions": []}
    selection.update(over)
    return selection


def test_identity_gap_policy_warn_never_tolerates_other_failures(monkeypatch):
    monkeypatch.setenv("TFB_NEWS_IDENTITY_GAP_POLICY", "warn")
    base = {"selected_acquisition_complete": True, "archive_write": {"status": "APPEND_ACCEPTED"},
            "selection": _ok_selection()}
    assert archive._identity_gap_tolerated(dict(base, failure_codes=["identity_coverage_incomplete"])) is True
    assert archive._identity_gap_tolerated(dict(base, failure_codes=["identity_discovery_incomplete"])) is True
    assert archive._identity_gap_tolerated(dict(base, failure_codes=[])) is False
    for extra in ("identity_source_read_incomplete", "selected_identity_missing", "priority_selection_capped",
                  "news_batch_failed", "collection_error", "news_response_unverified"):
        assert archive._identity_gap_tolerated(dict(base, failure_codes=["identity_coverage_incomplete", extra])) is False
    assert archive._identity_gap_tolerated(dict(base, failure_codes=["identity_coverage_incomplete"],
                                                selected_acquisition_complete=False)) is False
    for status in ("WITHHELD", "NOT_ATTEMPTED", "UNKNOWN", "UNVERIFIED"):
        assert archive._identity_gap_tolerated(dict(base, failure_codes=["identity_coverage_incomplete"],
                                                    archive_write={"status": status})) is False
    assert archive._identity_gap_tolerated(dict(base, failure_codes=["identity_coverage_incomplete"],
                                                archive_write={"status": "APPEND_ACCEPTED",
                                                               "error_code": "archive_append_outcome_unknown"})) is False


def test_identity_gap_policy_warn_still_fails_a_source_read_gap(monkeypatch, tmp_path):
    monkeypatch.setenv("TFB_NEWS_IDENTITY_GAP_POLICY", "warn")
    monkeypatch.setenv("NEWS_ARCHIVE_UNIVERSE", "PE&OLES.MX")
    context = _gap_context()
    context["source_read_complete"] = False
    written = []
    _gap_store_and_batch(monkeypatch, context, written)
    path = tmp_path / "evidence.json"
    args = argparse.Namespace(universe=None, adaptive=True, max_symbols=60, sheet_id="test",
                              dry_run=False, evidence_json=str(path))
    assert asyncio.run(archive.collect(args)) == 1
    first = json.loads(path.read_text())
    assert first["failure_codes"] == ["identity_source_read_incomplete", "identity_coverage_incomplete"]
    assert first["identity_gap_tolerated"] is False and len(written) == 1


def test_news_workflow_arms_warn_policy_with_variable_override():
    import yaml
    workflow = yaml.load((Path(__file__).parents[1] / ".github/workflows/news_archive.yml").read_text(),
                         Loader=yaml.BaseLoader)
    step = next(s for s in workflow["jobs"]["collect"]["steps"] if s.get("name") == "Collect news archive")
    assert step["env"]["TFB_NEWS_IDENTITY_GAP_POLICY"] == "${{ vars.TFB_NEWS_IDENTITY_GAP_POLICY || 'warn' }}"


def test_identity_gap_policy_warn_never_tolerates_lifecycle_or_selection_failures(monkeypatch):
    """Codex P2 (PR #839): an unverified lifecycle exclusion, an invalid registry, an empty
    selection or a budget cut only surface as identity_discovery/coverage_incomplete."""
    monkeypatch.setenv("TFB_NEWS_IDENTITY_GAP_POLICY", "warn")
    base = {"failure_codes": ["identity_discovery_incomplete"], "selected_acquisition_complete": True,
            "archive_write": {"status": "APPEND_ACCEPTED"}}
    assert archive._identity_gap_tolerated(dict(base, selection=_ok_selection())) is True
    legacy = {"symbol": "ROG.SW", "status": "VERIFIED_LEGACY", "historical_name": "Roche"}
    assert archive._identity_gap_tolerated(dict(base, selection=_ok_selection(lifecycle_exclusions=[legacy]))) is True
    for status in ("UNKNOWN", "EXPIRED", "", None):
        bad = dict(legacy, status=status)
        assert archive._identity_gap_tolerated(dict(base, selection=_ok_selection(lifecycle_exclusions=[legacy, bad]))) is False
    assert archive._identity_gap_tolerated(dict(base, selection=_ok_selection(lifecycle_exclusions=["ROG.SW"]))) is False
    for status in ("UNKNOWN", "INVALID", None):
        assert archive._identity_gap_tolerated(dict(base, selection=_ok_selection(
            lifecycle_registry={"status": status, "map_sha256": None}))) is False
    assert archive._identity_gap_tolerated(dict(base, selection=_ok_selection(lifecycle_registry=None))) is False
    assert archive._identity_gap_tolerated(dict(base, selection=_ok_selection(selected_count=0))) is False
    assert archive._identity_gap_tolerated(dict(base, selection=_ok_selection(missing_identity_symbols=["X.US"]))) is False
    assert archive._identity_gap_tolerated(dict(base, selection=_ok_selection(omitted_priority_count=1))) is False
    assert archive._identity_gap_tolerated(dict(base, selection=None)) is False
    assert archive._identity_gap_tolerated(dict(base)) is False


def test_identity_gap_policy_warn_still_fails_an_expired_lifecycle_exclusion_end_to_end(monkeypatch, tmp_path):
    """Codex's example: ROG.SW excluded at an expired review date + a successful ACTIVE.US fetch."""
    monkeypatch.setenv("TFB_NEWS_IDENTITY_GAP_POLICY", "warn")
    monkeypatch.setenv("NEWS_ARCHIVE_UNIVERSE", "ROG.SW ACTIVE.US")
    expired = {"ROG.SW": {"symbol": "ROG.SW", "status": "UNKNOWN", "historical_name": "Roche Holding AG",
                          "reason": "review_expired"}}
    monkeypatch.setattr(archive, "load_news_lifecycle",
                        lambda now=None: (expired, {"status": "VALID", "map_sha256": "x"}), raising=False)
    import core.news_identity_lifecycle as lifecycle_module
    monkeypatch.setattr(lifecycle_module, "load_news_lifecycle",
                        lambda now=None: (expired, {"status": "VALID", "map_sha256": "x"}))
    context = {"identities": {"ACTIVE.US": "Active Corp"}, "portfolio_membership": [], "research": [],
               "pool": [], "eligible": [], "sources": [], "complete": True, "source_read_complete": True,
               "identity_coverage_complete": True, "lifecycle_exclusions": [expired["ROG.SW"]]}
    written = []
    _gap_store_and_batch(monkeypatch, context, written)
    path = tmp_path / "evidence.json"
    args = argparse.Namespace(universe=None, adaptive=True, max_symbols=60, sheet_id="test",
                              dry_run=False, evidence_json=str(path))
    assert asyncio.run(archive.collect(args)) == 1
    first = json.loads(path.read_text())
    assert first["archive_write"]["status"] == "APPEND_ACCEPTED" and len(written) == 1
    assert first["identity_gap_tolerated"] is False
    assert first["selection"]["lifecycle_exclusions"][0]["status"] == "UNKNOWN"
