"""Bounded source-identity discovery and news selection; no network or credentials."""
from __future__ import annotations

import argparse
import asyncio
import copy
import json
import re
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from scripts import news_archive_collector as archive


NOW = datetime(2026, 10, 23, 7, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.delenv("NEWS_ARCHIVE_UNIVERSE", raising=False)
    monkeypatch.setattr(archive, "_utc_now", lambda: NOW)
    monkeypatch.setattr(archive, "_riyadh_date", lambda: "2026-10-23")


def args(**values):
    return argparse.Namespace(**{"universe": None, "adaptive": True, "max_symbols": 60,
                                 "sheet_id": "exact-id", "dry_run": True, "evidence_json": None,
                                 **values})


def context(pool_count=100):
    names = {f"S{i:03}.US": f"Company {i}" for i in range(pool_count)}
    names.update({"PRIOR.US": "Priority Corporation", "PORT.US": "Portfolio Corporation",
                  "BASE.US": "Baseline Corporation"})
    return {"identities": names, "portfolio_membership": ["PORT.US"], "research": ["PRIOR.US"],
            "pool": list(names), "sources": [], "complete": True,
            "source_read_complete": True, "identity_coverage_complete": True}


def test_adaptive_uses_env_as_baseline_and_prioritizes_current_membership(monkeypatch):
    monkeypatch.setenv("NEWS_ARCHIVE_UNIVERSE", "BASE.US")
    universe, items, evidence = archive._select_news_universe(args(max_symbols=6), context(), "2026-10-23")
    assert universe[:3] == ["PORT.US", "PRIOR.US", "BASE.US"]
    assert len(universe) == len(items) == 6
    assert items[1] == {"symbol": "PRIOR.US", "company_name": "Priority Corporation"}
    assert evidence["mode"] == "adaptive" and evidence["discovery_complete"]


def test_explicit_cli_remains_absolute_even_when_adaptive_is_enabled(monkeypatch):
    monkeypatch.setenv("NEWS_ARCHIVE_UNIVERSE", "BASE.US")
    universe, items, evidence = archive._select_news_universe(
        args(universe=[" s004.us ", "S004.US", "S005.US"]), context(), "2026-10-23")
    assert universe == ["S004.US", "S005.US"]
    assert [item["symbol"] for item in items] == universe
    assert evidence["mode"] == "cli" and evidence["rotation_count"] == 0


def test_adaptive_off_preserves_env_override_without_source_reads(monkeypatch):
    monkeypatch.setenv("NEWS_ARCHIVE_UNIVERSE", "BASE.US")
    universe, items, evidence = archive._select_news_universe(args(adaptive=False), None, "2026-10-23")
    assert universe == ["BASE.US"] and items == [{"symbol": "BASE.US"}]
    assert evidence["mode"] == "env" and evidence["discovery_complete"]


@pytest.mark.parametrize("budget,expected", [(0, 1), (-5, 1), (4, 4), (60, 60), (9000, 60)])
def test_actual_query_budget_is_always_bounded(monkeypatch, budget, expected):
    monkeypatch.setenv("NEWS_ARCHIVE_UNIVERSE", "BASE.US")
    universe, items, evidence = archive._select_news_universe(args(max_symbols=budget), context(), "2026-10-23")
    assert len(items) == len(universe) == expected
    assert evidence["analysis_cap"] == expected


def test_too_many_explicit_symbols_reports_omission_not_false_full_coverage():
    universe, items, evidence = archive._select_news_universe(
        args(universe=[f"S{i:03}.US" for i in range(100)]), context(), "2026-10-23")
    assert len(universe) == len(items) == 60
    assert evidence["omitted_priority_count"] == 40 and not evidence["discovery_complete"]


def test_rotation_is_stable_same_day_order_independent_and_fair_next_day(monkeypatch):
    monkeypatch.setenv("NEWS_ARCHIVE_UNIVERSE", "BASE.US")
    first = context()
    second = copy.deepcopy(first)
    second["pool"].reverse()
    day1, _, evidence1 = archive._select_news_universe(args(max_symbols=9), first, "2026-10-23")
    reordered, _, evidence2 = archive._select_news_universe(args(max_symbols=9), second, "2026-10-23")
    day2, _, _ = archive._select_news_universe(args(max_symbols=9), first, "2026-10-24")
    assert day1 == reordered and evidence1["cohort_sha256"] == evidence2["cohort_sha256"]
    assert set(day1[3:]).isdisjoint(day2[3:])
    assert day1[:3] == day2[:3]


def test_unknown_identity_is_not_guessed_or_queried(monkeypatch):
    monkeypatch.setenv("NEWS_ARCHIVE_UNIVERSE", "UNKNOWN.US, BASE.US")
    universe, items, evidence = archive._select_news_universe(args(max_symbols=5), context(), "2026-10-23")
    assert "UNKNOWN.US" in universe
    assert "UNKNOWN.US" not in {item["symbol"] for item in items}
    assert evidence["missing_identity_symbols"] == ["UNKNOWN.US"]
    assert not evidence["discovery_complete"]


def test_exact_venue_symbols_stay_distinct():
    assert archive._unique_symbols([" ABC.US ", "abc.l", "ABC.US"]) == ["ABC.US", "ABC.L"]


def test_research_parsing_uses_first_identity_block_and_keeps_grace_rows():
    grid = [["status", "private amount", "ignored"], ["Rank", "Symbol", "Name"],
            ["—", "GRACE.US", "Grace Corporation"], [1, "NEW.US", "New Corporation"], [],
            ["Rank", "Symbol", "Name"], [1, "OTHER.US", "Other Corporation"]]
    pairs, headers, complete = archive._identity_pairs(grid, research=True)
    assert pairs == [("GRACE.US", "Grace Corporation"), ("NEW.US", "New Corporation")]
    assert headers and complete


def test_research_block_cut_off_remains_incomplete():
    assert archive._identity_pairs([["Rank", "Symbol", "Name"], [1, "NEW.US", "New Corp"]], research=True)[2] is False


class Worksheet:
    def __init__(self, values, *, fail=False, row_count=None):
        self.values, self.fail = values, fail
        self.row_count = row_count or max(1, len(values))
        self.calls = []

    def get_values(self, source_range, **kwargs):
        self.calls.append((source_range, kwargs))
        if self.fail:
            raise RuntimeError("private failure details")
        bounds = re.fullmatch(r"([A-Z]+)(\d+):([A-Z]+)(\d+)", source_range)
        if bounds:
            first, last = int(bounds.group(2)) - 1, int(bounds.group(4))
            columns = ord(bounds.group(3)) - ord("A") + 1
            return copy.deepcopy([row[:columns] for row in self.values[first:last]])
        return copy.deepcopy(self.values)


def store_with_sources(*, publication="2026-10-23 08:00:00+03:00"):
    pages = {page: Worksheet([["Symbol", "Name"], [f"P{i}.US", f"Pool Company {i}"]])
             for i, page in enumerate(archive._SECTOR_JOIN_PAGES)}
    pages["My_Portfolio"] = Worksheet([["Symbol", "Name"], ["P0.US", "Pool Company 0"]])
    pages["Top_10_Investments"] = Worksheet([[] for _ in range(17)] +
                                           [["Rank", "Symbol", "Name"], [1, "P1.US", "Pool Company 1"], []])
    pages["Investable Symbol"] = Worksheet([["Symbol", "Name"], ["P0.US", "Pool Company 0"]])
    pages["_Status"] = Worksheet([["Page", "Last Updated", "Status", "Message"]] +
                                 [[page, publication, "SUCCESS", "source readiness VERIFIED (run 123)"
                                   if page == "Top_10_Investments" else "run=123"]
                                  for page in pages if page != "Investable Symbol"])
    store = object.__new__(archive.NewsArchiveStore)
    store.sheet = SimpleNamespace(worksheet=lambda page: pages[page])
    store.backoff = SimpleNamespace(run=lambda call: call())
    return store, pages


def test_source_context_reads_only_bounded_identity_columns_and_reports_clocks():
    store, pages = store_with_sources()
    result = store.load_news_source_context()
    assert result["complete"] and result["identities"]["P0.US"] == "Pool Company 0"
    assert pages["Global_Markets"].calls == [("A1:B2", {})]
    assert pages["Top_10_Investments"].calls == [("A18:A20", {}), ("A18:C20", {})]
    assert pages["_Status"].calls[0] == ("A1:D20", {"value_render_option": "UNFORMATTED_VALUE"})
    evidence = result["sources"][0]
    assert evidence["publication"]["published_utc"] == "2026-10-23T05:00:00+00:00"
    assert evidence["read_started_utc"] == NOW.isoformat()
    assert "private amount" not in json.dumps(result)


def test_conflicting_exact_symbol_names_remain_unknown():
    store, pages = store_with_sources()
    pages["Global_Markets"].values.append(["P0.US", "Different Corporation"])
    pages["Global_Markets"].row_count += 1
    result = store.load_news_source_context()
    assert "P0.US" not in result["identities"]
    assert result["ambiguous_symbols"] == ["P0.US"]
    assert result["source_read_complete"] and not result["identity_coverage_complete"]


@pytest.mark.parametrize("problem", ["read", "header", "status_missing", "status_duplicate", "stale", "missing_run"])
def test_failed_or_unproven_source_discovery_is_not_complete(problem):
    store, pages = store_with_sources()
    if problem == "read":
        pages["Global_Markets"].fail = True
    elif problem == "header":
        pages["Global_Markets"].values[0] = ["Ticker", "Label"]
    elif problem == "status_missing":
        pages["_Status"].values.pop()
    elif problem == "status_duplicate":
        pages["_Status"].values.append(pages["_Status"].values[1])
    elif problem == "stale":
        pages["_Status"].values[1][1] = "2026-10-01 08:00:00+03:00"
    else:
        pages["_Status"].values[1][3] = "no source receipt"
    result = store.load_news_source_context()
    assert not result["complete"]
    assert "private failure" not in json.dumps(result)


def test_publication_changing_during_identity_reads_is_not_complete():
    store, pages = store_with_sources()
    original = pages["_Status"].get_values
    calls = []
    def changed(source_range, **kwargs):
        calls.append(1)
        if len(calls) == 2:
            pages["_Status"].values[1][3] = "run=124"
        return original(source_range, **kwargs)
    pages["_Status"].get_values = changed
    result = store.load_news_source_context()
    assert not result["complete"]
    assert result["sources"][0]["publication_stable"] is False


@pytest.mark.parametrize("message", ["run=0", "run=123.4", "run=123 run=124", "run=123 run=123",
                                     "Last run 2026-10-23 00:21:53"])
def test_source_run_requires_one_positive_explicit_receipt(message):
    assert archive._source_run(message) is None


def test_top10_source_run_ignores_timestamp_year_and_requires_verified_receipt():
    message = "Last run 2026-10-23 00:21:53 | source readiness VERIFIED (run 39821354157)"
    assert archive._source_run(message, research=True) == "39821354157"
    assert archive._source_run("Last run 2026-10-23 00:21:53", research=True) is None
    assert archive._source_run("source readiness WITHHELD (run 123)", research=True) is None
    assert archive._source_run("source readiness WITHHELD | other VERIFIED (run 123)", research=True) is None
    assert archive._source_run("source readiness VERIFIED (run 123) | source readiness VERIFIED (run 123)", research=True) is None


def test_missing_research_name_does_not_end_the_block_or_disappear():
    diagnostics = {}
    pairs, header, ended = archive._identity_pairs(
        [["Rank", "Symbol", "Name"], [1, "MISSING.US", ""], [2, "KNOWN.US", "Known Corporation"], []],
        research=True, diagnostics=diagnostics)
    assert pairs == [("KNOWN.US", "Known Corporation")] and header and ended
    assert diagnostics == {"invalid_identity_rows": 1, "unknown_identity_symbols": ["MISSING.US"]}


def test_eligible_rotation_covers_small_priority_pool_across_two_daily_slots(monkeypatch):
    monkeypatch.setenv("NEWS_ARCHIVE_UNIVERSE", "BASE.US")
    source = context()
    source["eligible"] = [f"S{i:03}.US" for i in range(8)]
    slices = []
    for day, hour in [(23, 14), (23, 20), (24, 14), (24, 20)]:
        moment = datetime(2026, 10, day, hour, tzinfo=timezone.utc)
        monkeypatch.setattr(archive, "_utc_now", lambda stamp=moment: stamp)
        universe, items, evidence = archive._select_news_universe(args(max_symbols=5), source, f"2026-10-{day}")
        slices.append(set(universe[3:]))
        assert evidence["eligible_rotation_count"] == 2 and evidence["broader_rotation_count"] == 0
        assert evidence["eligible_pool_identity_count"] == 8 and len(items) == 5
    assert len(set.union(*slices)) == 8
    assert slices[0].isdisjoint(slices[1])


def test_broader_rotation_only_uses_spare_capacity(monkeypatch):
    monkeypatch.setenv("NEWS_ARCHIVE_UNIVERSE", "BASE.US")
    source = context()
    source["eligible"] = ["S000.US"]
    universe, _, evidence = archive._select_news_universe(args(max_symbols=6), source, "2026-10-23")
    assert universe[3] == "S000.US"
    assert evidence["eligible_rotation_count"] == 1 and evidence["broader_rotation_count"] == 2
    assert len(set(universe)) == 6


def test_whole_batch_timeout_is_bounded_and_preserves_unknown_evidence(monkeypatch, tmp_path):
    timeouts = []
    async def batch(_items, include_articles=False):
        return SimpleNamespace(items=[], meta={})
    async def timeout(coroutine, *, timeout):
        timeouts.append(timeout)
        coroutine.close()
        raise TimeoutError("private exception details")
    monkeypatch.setattr(archive, "_import_news_module", lambda: SimpleNamespace(batch_news_intelligence=batch))
    monkeypatch.setattr(archive.asyncio, "wait_for", timeout)
    path = tmp_path / "timeout.json"
    options = args(universe=["NEW.US"], timeout_seconds=100000, evidence_json=str(path))
    assert asyncio.run(archive.collect(options)) == 1
    report = json.loads(path.read_text())
    assert timeouts == [600] and report["complete"] is False
    assert report["error_code"] == "news_batch_timeout"
    assert report["status_counts"] == {"UNKNOWN": 1}
    assert "private exception" not in path.read_text()


def test_repeated_canonical_header_is_not_a_queryable_identity():
    diagnostics = {}
    pairs, _, _ = archive._identity_pairs([["Symbol", "Name"], ["AAPL.US", "Apple Inc."],
                                          ["Symbol", "Name"]], diagnostics=diagnostics)
    assert pairs == [("AAPL.US", "Apple Inc.")]
    assert diagnostics["invalid_identity_rows"] == 1


def test_unnamed_identity_is_explicit_and_does_not_claim_discovery_coverage():
    store, pages = store_with_sources()
    pages["Global_Markets"].values.append(["UNKNOWN.US", ""])
    pages["Global_Markets"].row_count += 1
    result = store.load_news_source_context()
    entry = next(entry for entry in result["sources"] if entry["page"] == "Global_Markets")
    assert result["source_read_complete"] and not result["identity_coverage_complete"]
    assert not result["complete"] and entry["invalid_identity_rows"] == 1
    assert entry["unknown_identity_symbols"] == ["UNKNOWN.US"]


def test_manual_cli_does_not_require_identity_discovery_or_guess_company_name(monkeypatch, tmp_path):
    calls = []
    class NoStore:
        def __init__(self, *_args, **_kwargs):
            pytest.fail("literal CLI dry-run should not read sources")
    async def batch(items, include_articles=False):
        calls.append(items)
        return SimpleNamespace(items=[{"symbol": item["symbol"], "collection_evidence": {"status": "EMPTY"}}
                                      for item in items], meta={})
    monkeypatch.setattr(archive, "NewsArchiveStore", NoStore)
    monkeypatch.setattr(archive, "_import_news_module", lambda: SimpleNamespace(batch_news_intelligence=batch))
    path = tmp_path / "literal.json"
    assert asyncio.run(archive.collect(args(universe=["NEW.US"], evidence_json=str(path)))) == 0
    assert calls == [[{"symbol": "NEW.US"}]]
    assert json.loads(path.read_text())["selected_acquisition_complete"]


@pytest.mark.parametrize("name", ["42", "private\nnotes", "A" * 301, "UNKNOWN", "P0.US"])
def test_unavailable_or_malformed_identity_names_are_not_guessed(name):
    pairs, headers, _ = archive._identity_pairs([["Symbol", "Name"], ["P0.US", name]])
    assert headers and pairs == []


def test_adoption_verification_is_main_only_path_scoped_and_uses_existing_writer():
    from pathlib import Path
    import yaml
    workflow = yaml.load((Path(__file__).parents[1] / ".github/workflows/news_archive.yml").read_text(),
                         Loader=yaml.BaseLoader)
    assert workflow["on"]["push"] == {
        "branches": ["main"], "paths": ["core/news_intelligence.py", "core/news_identity_lifecycle.py",
                                        "data/verified_name_labels.json", "scripts/news_archive_collector.py",
                                        ".github/workflows/news_archive.yml"]}
    assert list(workflow["jobs"]) == ["collect"]
    assert workflow["concurrency"]["cancel-in-progress"] == "false"
    assert workflow["concurrency"]["group"] == "news-archive"
    assert workflow["on"]["schedule"] == [{"cron": "0 14,20 * * *"}]
    step = next(step for step in workflow["jobs"]["collect"]["steps"] if step.get("name") == "Collect news archive")
    assert "--max-symbols 60" in step["run"]
    assert "github.event_name != 'workflow_dispatch'" in step["env"]["TFB_NEWS_ARCHIVE_ADAPTIVE"]


def test_read_only_source_store_does_not_create_or_format_archive(monkeypatch):
    calls = []
    class Client:
        def open_by_key(self, sheet_id):
            calls.append(sheet_id)
            return SimpleNamespace(worksheet=lambda _name: pytest.fail("read-only source init touched archive"))
    monkeypatch.setattr(archive, "GSPREAD_AVAILABLE", True)
    monkeypatch.setattr(archive, "gspread", SimpleNamespace(service_account=lambda: Client()))
    monkeypatch.setattr(archive.NewsArchiveStore, "_load_creds", lambda _self: None)
    store = archive.NewsArchiveStore("exact-id", read_only=True)
    assert store.sheet is not None and store.ws is None
    assert calls == ["exact-id"]


def test_discovery_failure_stays_visible_even_when_selected_news_succeeds(monkeypatch, tmp_path):
    monkeypatch.setenv("NEWS_ARCHIVE_UNIVERSE", "BASE.US")
    source = context()
    source["complete"] = False
    batches = []
    class Store:
        def __init__(self, _id, _name, *, read_only=False):
            assert read_only
        def load_news_source_context(self):
            return source
    async def batch(items, include_articles=False):
        batches.append(items)
        return SimpleNamespace(items=[{"symbol": item["symbol"], "collection_evidence": {"status": "EMPTY"}}
                                      for item in items], meta={})
    monkeypatch.setattr(archive, "NewsArchiveStore", Store)
    monkeypatch.setattr(archive, "_import_news_module", lambda: SimpleNamespace(batch_news_intelligence=batch))
    path = tmp_path / "evidence.json"
    assert asyncio.run(archive.collect(args(max_symbols=6, evidence_json=str(path)))) == 1
    report = json.loads(path.read_text())
    assert len(batches[0]) == 6 and not report["complete"]
    assert not report["selection"]["discovery_complete"]
    assert not any("holding" in key or "quantity" in key or "cash" in key for key in report["selection"])


def test_allocated_spare_grid_rows_do_not_imply_identity_read_truncation():
    store, pages = store_with_sources()
    pages["Global_Markets"].row_count = 50000
    result = store.load_news_source_context()
    entry = next(item for item in result["sources"] if item["page"] == "Global_Markets")
    assert result["complete"] and entry["status"] == "OK"
    assert not entry["read_cap_reached"]
    assert pages["Global_Markets"].calls == [("A1:B10001", {})]


def test_populated_capped_boundary_keeps_identity_discovery_incomplete():
    store, pages = store_with_sources()
    pages["Global_Markets"].values = [["Symbol", "Name"]] + [[] for _ in range(9999)] + [
        ["BOUNDARY.US", "Boundary Corporation"]]
    pages["Global_Markets"].row_count = 50000
    result = store.load_news_source_context()
    entry = next(item for item in result["sources"] if item["page"] == "Global_Markets")
    assert not result["complete"] and entry["status"] == "INCOMPLETE"
    assert entry["read_cap_reached"]


def test_unknown_portfolio_identity_survives_as_unattributed_public_diagnostic(monkeypatch):
    store, pages = store_with_sources()
    pages["My_Portfolio"].values.append(["MISSING.US", ""])
    pages["My_Portfolio"].row_count += 1
    result = store.load_news_source_context()
    entry = next(item for item in result["sources"] if item["page"] == "My_Portfolio")
    assert result["unknown_identity_symbols"] == ["MISSING.US"]
    assert not result["identity_coverage_complete"]
    assert "unknown_identity_symbols" not in entry
    monkeypatch.setenv("NEWS_ARCHIVE_UNIVERSE", "P0.US")
    _, _, evidence = archive._select_news_universe(args(), result, "2026-10-23")
    assert evidence["unknown_identity_symbols"] == ["MISSING.US"]
    assert not any(key in evidence for key in ("portfolio_membership", "quantity", "cash", "orders"))


class ArchiveWorksheet:
    def __init__(self):
        self.values = [[] for _ in range(4)] + [list(archive.NewsArchiveStore.HEADERS)]
        self.read_error = False
        self.append_error = None
        self.append_calls = 0
        self.read_calls = 0

    def get_all_values(self):
        self.read_calls += 1
        if self.read_error:
            raise RuntimeError("private transport details")
        return copy.deepcopy(self.values)

    def append_rows(self, rows, **_kwargs):
        self.append_calls += 1
        error, self.append_error = self.append_error, None
        if error == "before":
            raise TimeoutError("private pre-commit failure")
        self.values.extend(copy.deepcopy(rows))
        if error == "after":
            raise TimeoutError("private post-commit failure")


def install_archive_writer(monkeypatch, worksheet):
    real_store = archive.NewsArchiveStore
    store = object.__new__(real_store)
    store.ws, store.sheet_name = worksheet, "News_Archive"
    store.backoff = archive._Backoff(base=0, cap=0, tries=3)
    monkeypatch.setattr(archive, "NewsArchiveStore", lambda *_args, **_kwargs: store)
    async def batch(items, include_articles=False):
        return SimpleNamespace(items=[{"symbol": item["symbol"], "articles_analyzed": 0,
                                       "collection_evidence": {"status": "EMPTY"}}
                                      for item in items], meta={"status": "success", "errors": 0})
    monkeypatch.setattr(archive, "_import_news_module", lambda: SimpleNamespace(batch_news_intelligence=batch))
    return store


def test_archive_key_read_failure_withholds_append_and_preserves_acquisition(monkeypatch, tmp_path, capsys):
    worksheet = ArchiveWorksheet()
    worksheet.read_error = True
    install_archive_writer(monkeypatch, worksheet)
    path = tmp_path / "read-failed.json"
    assert asyncio.run(archive.collect(args(universe=["NEW.US"], dry_run=False,
                                           evidence_json=str(path)))) == 1
    assert worksheet.append_calls == 0 and worksheet.read_calls == 3
    report = json.loads(path.read_text())
    assert report["selected_acquisition_complete"] and not report["complete"]
    assert report["archive_write"]["status"] == "WITHHELD"
    assert report["archive_write"]["error_code"] == "archive_key_read_failed"
    assert report["status_counts"] == {"EMPTY": 1}
    assert "private transport" not in capsys.readouterr().out + path.read_text()


@pytest.mark.parametrize("values", [[], [[] for _ in range(5)],
                                    [[] for _ in range(4)] + [["Date", "Wrong symbol column"]]])
def test_unverified_archive_header_never_masquerades_as_empty(monkeypatch, tmp_path, values):
    worksheet = ArchiveWorksheet()
    worksheet.values = values
    install_archive_writer(monkeypatch, worksheet)
    path = tmp_path / "header-failed.json"
    assert asyncio.run(archive.collect(args(universe=["NEW.US"], dry_run=False,
                                           evidence_json=str(path)))) == 1
    assert worksheet.append_calls == 0
    assert json.loads(path.read_text())["archive_write"]["error_code"] == "archive_header_unverified"


def test_valid_empty_archive_accepts_once_then_skips_existing_key(monkeypatch, tmp_path):
    worksheet = ArchiveWorksheet()
    install_archive_writer(monkeypatch, worksheet)
    path = tmp_path / "accepted.json"
    options = args(universe=["NEW.US"], dry_run=False, evidence_json=str(path))
    assert asyncio.run(archive.collect(options)) == 0
    first = json.loads(path.read_text())["archive_write"]
    assert first == {"status": "APPEND_ACCEPTED", "attempted_rows": 1,
                     "accepted_rows": 1, "skipped_rows": 0}
    assert asyncio.run(archive.collect(options)) == 0
    second = json.loads(path.read_text())["archive_write"]
    assert second["status"] == "SKIPPED_EXISTING" and second["skipped_rows"] == 1
    assert worksheet.append_calls == 1 and len(worksheet.values) == 6


@pytest.mark.parametrize("tail,accepted", [(["", "  "], True), (["Unexpected column"], False)])
def test_archive_header_padding_is_distinct_from_schema_drift(monkeypatch, tmp_path, tail, accepted):
    worksheet = ArchiveWorksheet()
    worksheet.values[4] += tail
    install_archive_writer(monkeypatch, worksheet)
    path = tmp_path / "header-padding.json"
    outcome = asyncio.run(archive.collect(args(universe=["NEW.US"], dry_run=False,
                                               evidence_json=str(path))))
    assert outcome == (0 if accepted else 1)
    assert worksheet.append_calls == (1 if accepted else 0)
    report = json.loads(path.read_text())
    assert report["archive_write"]["status"] == ("APPEND_ACCEPTED" if accepted else "WITHHELD")


@pytest.mark.parametrize("error", ["before", "after"])
def test_ambiguous_append_is_not_retried_and_next_invocation_recovers(monkeypatch, tmp_path, error):
    worksheet = ArchiveWorksheet()
    worksheet.append_error = error
    install_archive_writer(monkeypatch, worksheet)
    path = tmp_path / "ambiguous.json"
    options = args(universe=["NEW.US"], dry_run=False, evidence_json=str(path))
    assert asyncio.run(archive.collect(options)) == 1
    assert worksheet.append_calls == 1  # no blind non-idempotent retry
    report = json.loads(path.read_text())
    assert report["archive_write"]["status"] == "UNKNOWN" and not report["complete"]
    assert report["archive_write"]["error_code"] == "archive_append_outcome_unknown"
    assert len(worksheet.values) == (6 if error == "after" else 5)
    assert asyncio.run(archive.collect(options)) == 0
    assert len(worksheet.values) == 6  # one daily row after either recovery path
    assert worksheet.append_calls == (1 if error == "after" else 2)


def test_safe_stdout_summary_keeps_source_evidence_and_omits_identities(tmp_path, capsys):
    path = tmp_path / "summary.json"
    source_evidence = [{"page": "SOURCE_IDENTITY", "read_started_utc": NOW.isoformat()}]
    report = {"selection": {"mode": "adaptive", "analysis_cap": 60, "selected_count": 4,
                            "queried_count": 3, "rotation_count": 1, "sources": source_evidence,
                            "missing_identity_symbols": ["DO_NOT_PRINT.US"], "discovery_complete": False},
              "selected_acquisition_complete": False, "complete": False,
              "status_counts": {"OK": 2, "EMPTY": 1, "UNKNOWN": 1}}
    archive._finish_collection_report(args(evidence_json=str(path)), report, "DRY_RUN")
    output = capsys.readouterr().out
    summary = json.loads(output.split("evidence ", 1)[1])
    assert summary["selection"]["analysis_cap"] == 60
    assert summary["status_counts"] == report["status_counts"]
    assert summary["archive_write"]["status"] == "DRY_RUN" and len(output) < 1500
    assert "DO_NOT_PRINT.US" not in output and "SOURCE_IDENTITY" not in output
    saved = json.loads(path.read_text())
    assert saved["selection"]["sources"] == source_evidence
    assert saved["selection"]["missing_identity_symbols"] == ["DO_NOT_PRINT.US"]


class HeaderWorksheet:
    def __init__(self, header=None, *, read_error=False):
        self.header = list(header or [])
        self.read_error = read_error
        self.updates = []

    def row_values(self, row):
        assert row == 5
        if self.read_error:
            raise RuntimeError("private header transport details")
        return list(self.header)

    def update(self, *, values, range_name):
        self.updates.append((copy.deepcopy(values), range_name))
        self.header = list(values[0])


def initialize_real_store(monkeypatch, worksheet, *, lookup_error=None):
    created = []
    class Book:
        def worksheet(self, name):
            assert name == "News_Archive"
            if lookup_error:
                raise lookup_error
            return worksheet
        def add_worksheet(self, **kwargs):
            created.append(kwargs)
            return worksheet
    class Client:
        def open_by_key(self, sheet_id):
            assert sheet_id == "exact-id"
            return Book()
    monkeypatch.setattr(archive, "GSPREAD_AVAILABLE", True)
    monkeypatch.setattr(archive, "gspread", SimpleNamespace(service_account=lambda: Client()))
    monkeypatch.setattr(archive.NewsArchiveStore, "_load_creds", lambda _self: None)
    monkeypatch.setattr(archive, "_Backoff", lambda: SimpleNamespace(run=lambda call: call()))
    return archive.NewsArchiveStore("exact-id"), created


def test_existing_archive_header_read_failure_never_relabels_it(monkeypatch, caplog):
    worksheet = HeaderWorksheet(read_error=True)
    store, created = initialize_real_store(monkeypatch, worksheet)
    assert not store.is_available() and store.init_error_code == "archive_header_read_failed"
    assert worksheet.updates == [] and created == []
    assert "private header" not in caplog.text


@pytest.mark.parametrize("header", [[], ["Date", "Wrong symbol column"]])
def test_existing_archive_header_drift_is_withheld_without_writes(monkeypatch, header):
    worksheet = HeaderWorksheet(header)
    store, created = initialize_real_store(monkeypatch, worksheet)
    assert not store.is_available() and store.init_error_code == "archive_header_unverified"
    assert worksheet.updates == [] and created == []


def test_worksheet_lookup_transport_failure_does_not_create_archive(monkeypatch, caplog):
    worksheet = HeaderWorksheet()
    store, created = initialize_real_store(
        monkeypatch, worksheet, lookup_error=RuntimeError("private lookup transport details"))
    assert not store.is_available() and store.init_error_code == "archive_lookup_failed"
    assert created == [] and worksheet.updates == []
    assert "private lookup" not in caplog.text


def test_only_confirmed_missing_worksheet_initializes_new_headers_once(monkeypatch):
    worksheet = HeaderWorksheet()
    store, created = initialize_real_store(
        monkeypatch, worksheet, lookup_error=archive._WorksheetNotFound("News_Archive"))
    assert store.is_available() and store.init_error_code is None
    assert created == [{"title": "News_Archive", "rows": 20000, "cols": 30}]
    assert worksheet.updates == [([store.HEADERS], "A5:L5")]
    assert worksheet.header == store.HEADERS


def test_existing_valid_header_with_case_and_padding_needs_no_write(monkeypatch):
    header = [value.lower() for value in archive.NewsArchiveStore.HEADERS] + ["", "  "]
    worksheet = HeaderWorksheet(header)
    store, created = initialize_real_store(monkeypatch, worksheet)
    assert store.is_available() and store.init_error_code is None
    assert created == [] and worksheet.updates == []
    assert worksheet.header == header


# --- v1.3.4: a failed identity page read records its reason and may be re-read once ---

class FlakyWorksheet(Worksheet):
    """Fails the first `failures` get_values calls, then behaves normally."""
    def __init__(self, values, *, failures=1, exc=None, **kwargs):
        super().__init__(values, **kwargs)
        self.failures, self.exc = failures, exc

    def get_values(self, source_range, **kwargs):
        if self.failures > 0:
            self.failures -= 1
            self.calls.append((source_range, kwargs))
            raise self.exc or RuntimeError("private failure details")
        return super().get_values(source_range, **kwargs)


def _no_sleep(monkeypatch):
    slept = []
    monkeypatch.setattr(archive.time, "sleep", lambda s: slept.append(s))
    return slept


def test_failed_read_records_type_and_known_reason_only(monkeypatch):
    monkeypatch.delenv("TFB_NEWS_SOURCE_READ_RETRIES", raising=False)
    store, pages = store_with_sources()
    pages["Top_10_Investments"].fail = True
    result = store.load_news_source_context()
    entry = next(s for s in result["sources"] if s["page"] == "Top_10_Investments")
    assert entry["status"] == "UNAVAILABLE" and entry["error_code"] == "identity_source_read_failed"
    assert entry["error_type"] == "RuntimeError" and entry["error_reason"] == "unspecified"
    assert "attempts" not in entry and not result["source_read_complete"]
    assert "private failure" not in json.dumps(result)
    # one of the module's own reason codes is published verbatim
    pages["Top_10_Investments"].fail = False
    pages["Top_10_Investments"].values[17] = ["Ticker", "Symbol", "Name"]
    entry = next(s for s in store.load_news_source_context()["sources"] if s["page"] == "Top_10_Investments")
    assert entry["error_reason"] == "research_identity_header_unknown"


def test_retry_off_by_default_is_byte_identical_to_a_single_attempt(monkeypatch):
    monkeypatch.delenv("TFB_NEWS_SOURCE_READ_RETRIES", raising=False)
    slept = _no_sleep(monkeypatch)
    store, pages = store_with_sources()
    pages["Top_10_Investments"] = FlakyWorksheet(pages["Top_10_Investments"].values, failures=1)
    result = store.load_news_source_context()
    entry = next(s for s in result["sources"] if s["page"] == "Top_10_Investments")
    assert entry["status"] == "UNAVAILABLE" and len(pages["Top_10_Investments"].calls) == 1
    assert slept == [] and not result["source_read_complete"]


def test_one_retry_recovers_a_transient_read_after_the_other_pages(monkeypatch):
    monkeypatch.setenv("TFB_NEWS_SOURCE_READ_RETRIES", "1")
    monkeypatch.setenv("TFB_NEWS_SOURCE_READ_RETRY_SLEEP_S", "45")
    slept = _no_sleep(monkeypatch)
    store, pages = store_with_sources()
    pages["Top_10_Investments"] = FlakyWorksheet(pages["Top_10_Investments"].values, failures=1)
    result = store.load_news_source_context()
    entry = next(s for s in result["sources"] if s["page"] == "Top_10_Investments")
    assert entry["status"] == "OK" and entry["attempts"] == 2
    assert entry["failed_attempts"][0]["error_type"] == "RuntimeError"
    assert entry["failed_attempts"][0]["error_reason"] == "unspecified"
    assert slept == [45.0] and result["source_read_complete"] and result["complete"]
    assert result["identities"]["P1.US"] == "Pool Company 1"
    assert "private failure" not in json.dumps(result)
    # the retried page is read last: the other pages' read order is untouched
    order = [s["page"] for s in result["sources"]]
    assert order.index("Top_10_Investments") == len(order) - 1


def test_retry_bound_keeps_a_persistent_failure_unavailable(monkeypatch):
    monkeypatch.setenv("TFB_NEWS_SOURCE_READ_RETRIES", "1")
    monkeypatch.setenv("TFB_NEWS_SOURCE_READ_RETRY_SLEEP_S", "0")
    slept = _no_sleep(monkeypatch)
    store, pages = store_with_sources()
    pages["Global_Markets"] = FlakyWorksheet(pages["Global_Markets"].values, failures=5)
    result = store.load_news_source_context()
    entry = next(s for s in result["sources"] if s["page"] == "Global_Markets")
    assert entry["status"] == "UNAVAILABLE" and entry["attempts"] == 2
    assert len(entry["failed_attempts"]) == 1 and len(pages["Global_Markets"].calls) == 2
    assert slept == [] and not result["source_read_complete"]


@pytest.mark.parametrize("raw,expected", [("", 0), ("0", 0), ("1", 1), ("3", 3), ("9", 3), ("-2", 0), ("x", 0), ("1.5", 0)])
def test_retry_env_is_bounded(monkeypatch, raw, expected):
    monkeypatch.setenv("TFB_NEWS_SOURCE_READ_RETRIES", raw)
    assert archive._source_read_retries() == expected


@pytest.mark.parametrize("raw,expected", [("", 45.0), ("0", 0.0), ("30", 30.0), ("999", 180.0), ("-1", 0.0), ("x", 45.0)])
def test_retry_sleep_env_is_bounded(monkeypatch, raw, expected):
    monkeypatch.setenv("TFB_NEWS_SOURCE_READ_RETRY_SLEEP_S", raw)
    assert archive._source_read_retry_sleep() == expected
