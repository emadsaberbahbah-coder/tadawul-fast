"""Historical captions never authorize current-symbol news or ticker aliases."""
import argparse
import copy
import json
from datetime import datetime, timedelta, timezone

import pytest

from scripts import news_archive_collector as archive

NOW = datetime(2026, 10, 23, 12, tzinfo=timezone.utc)


def options():
    return argparse.Namespace(universe=["ROG.SW", "ROP.SW", "GXTG.US", "GXDW.US"],
                              adaptive=False, max_symbols=60)


def test_cli_legacy_is_excluded_without_remapping(monkeypatch):
    monkeypatch.setattr(archive, "_utc_now", lambda: NOW)
    universe, items, evidence = archive._select_news_universe(options(), None, "2026-10-23")
    assert universe == ["ROP.SW", "GXDW.US"]
    assert items == [{"symbol": "ROP.SW"}, {"symbol": "GXDW.US"}]
    assert evidence["lifecycle_excluded_count"] == 2
    assert {r["status"] for r in evidence["lifecycle_exclusions"]} == {"VERIFIED_LEGACY"}
    assert evidence["requested_before_lifecycle"] == 4 and evidence["discovery_complete"]


def test_legacy_only_never_auto_adds_successor(monkeypatch):
    monkeypatch.setattr(archive, "_utc_now", lambda: NOW)
    args = options(); args.universe = ["ROG.SW"]
    universe, items, evidence = archive._select_news_universe(args, None, "2026-10-23")
    assert universe == items == [] and not evidence["discovery_complete"]
    assert evidence["lifecycle_excluded_count"] == 1


@pytest.mark.parametrize("name", ["", "Roche Holding Ltd", "  ROCHE  HOLDING LTD "])
def test_exact_legacy_row_is_preserved_and_explicitly_excluded(name):
    from core.news_identity_lifecycle import load_news_lifecycle
    lifecycle, _ = load_news_lifecycle(now=NOW)
    grid = [["Symbol", "Name"], ["ROG.SW", name], ["ROG.US", "Different US issuer"]]
    before = copy.deepcopy(grid); diag = {}
    pairs, header, complete = archive._identity_pairs(grid, diagnostics=diag, lifecycle=lifecycle)
    assert grid == before and header and complete
    assert pairs == [("ROG.US", "Different US issuer")]
    assert diag["invalid_identity_rows"] == 0 and diag["lifecycle_excluded_rows"] == 1


def test_conflicting_current_caption_cannot_certify_legacy_identity():
    from core.news_identity_lifecycle import load_news_lifecycle
    lifecycle, _ = load_news_lifecycle(now=NOW); diag = {}
    pairs, _, _ = archive._identity_pairs([["Symbol", "Name"], ["ROG.SW", "Conflicting Issuer"]],
                                         diagnostics=diag, lifecycle=lifecycle)
    assert pairs == [] and diag["invalid_identity_rows"] == 1
    assert diag["lifecycle_exclusions"][0]["status"] == "UNKNOWN"
    assert diag["lifecycle_exclusions"][0]["reason"] == "current_caption_conflict"


def test_expired_review_is_unknown_and_never_queried(monkeypatch):
    monkeypatch.setattr(archive, "_utc_now", lambda: NOW + timedelta(days=8))
    universe, items, evidence = archive._select_news_universe(options(), None, "2026-10-31")
    assert universe == ["ROP.SW", "GXDW.US"] and len(items) == 2
    assert not evidence["discovery_complete"]
    assert {r["status"] for r in evidence["lifecycle_exclusions"]} == {"UNKNOWN"}


def test_expired_source_identity_remains_incomplete():
    from core.news_identity_lifecycle import load_news_lifecycle
    lifecycle, _ = load_news_lifecycle(now=NOW + timedelta(days=8)); diag = {}
    pairs, _, _ = archive._identity_pairs([["Symbol", "Name"], ["ROG.SW", "Roche"]],
                                         lifecycle=lifecycle, diagnostics=diag)
    assert pairs == [] and diag["invalid_identity_rows"] == 1
    assert diag["unknown_identity_symbols"] == ["ROG.SW"]


def test_bare_or_different_venue_is_never_a_legacy_alias(monkeypatch):
    monkeypatch.setattr(archive, "_utc_now", lambda: NOW)
    args = options(); args.universe = ["ROG", "ROG.US", "GXTG.L"]
    universe, _, evidence = archive._select_news_universe(args, None, "2026-10-23")
    assert universe == args.universe and evidence["lifecycle_excluded_count"] == 0


@pytest.mark.parametrize("bad", ["digest", "clock", "lifecycle", "effective", "successor", "namespace", "missing"])
def test_invalid_registry_or_declaration_cannot_verify_empty_exclusions(tmp_path, bad):
    from core import verified_name_labels as labels
    from core.news_identity_lifecycle import load_news_lifecycle
    data = json.loads(labels.REGISTRY_PATH.read_text())
    item = next(x for x in data["bindings"] if x["symbol"] == "ROG.SW"); data["bindings"] = [item]
    if bad == "digest": item["review_receipt_sha256"] = "a" * 64
    elif bad == "lifecycle": item["identifiers"]["news_lifecycle"] = "ACTIVE"
    elif bad == "effective": item["identifiers"]["lifecycle_effective_utc"] = (NOW + timedelta(days=40)).isoformat()
    elif bad == "successor": item["identifiers"]["successor_symbol"] = "ROG.SW"
    elif bad == "namespace": item["symbol"] = "ROG"
    elif bad == "missing": item["identifiers"].pop("lifecycle_effective_utc")
    if bad != "digest": item["review_receipt_sha256"] = labels._binding_digest(item)
    path = tmp_path / "labels.json"; path.write_text(json.dumps(data))
    entries, status = load_news_lifecycle(now=NOW.replace(tzinfo=None) if bad == "clock" else NOW,
                                        registry_path=path)
    assert entries == {} and status["status"] == "UNKNOWN"


def test_invalid_registry_withholds_queries_and_discovery(monkeypatch):
    from core import news_identity_lifecycle
    monkeypatch.setattr(news_identity_lifecycle, "load_news_lifecycle",
                        lambda **kw: ({}, {"status": "UNKNOWN", "map_sha256": None}))
    _, items, evidence = archive._select_news_universe(options(), None, "2026-10-23")
    assert items == [] and not evidence["discovery_complete"]


def test_source_discovery_reports_legacy_without_hiding_unknown(monkeypatch):
    from tests.test_news_adaptive_universe import store_with_sources
    monkeypatch.setattr(archive, "_utc_now", lambda: NOW)
    store, pages = store_with_sources(); ws = pages["Global_Markets"]
    ws.values.extend([["ROG.SW", ""], ["UNRESOLVED.US", ""]]); ws.row_count += 2
    result = store.load_news_source_context()
    assert "ROG.SW" not in result["identities"] and not result["complete"]
    assert result["unknown_identity_symbols"] == ["UNRESOLVED.US"]
    assert [r["symbol"] for r in result["lifecycle_exclusions"]] == ["ROG.SW"]


@pytest.mark.parametrize("conflict_page", ["Global_Markets", "Mutual_Funds"])
def test_cross_page_aggregation_preserves_unknown_conflict(monkeypatch, conflict_page):
    from tests.test_news_adaptive_universe import store_with_sources
    monkeypatch.setattr(archive, "_utc_now", lambda: NOW)
    store, pages = store_with_sources()
    for page in ["Global_Markets", "Mutual_Funds"]:
        ws = pages[page]
        ws.values.extend([["ROG.SW", "Reused Ticker Issuer" if page == conflict_page else "Roche Holding Ltd"]])
        ws.row_count += 1
    result = store.load_news_source_context()
    assert not result["complete"] and "ROG.SW" in result["unknown_identity_symbols"]
    record = next(r for r in result["lifecycle_exclusions"] if r["symbol"] == "ROG.SW")
    assert record["status"] == "UNKNOWN" and record["reason"] == "current_caption_conflict"


@pytest.mark.parametrize("extra", [
    [{"symbol": "ROG.SW", "collection_evidence": {"status": "OK"}}],
    [{"symbol": "ACTIVE", "collection_evidence": {"status": "OK"}}],
    [None, {"symbol": 42}],
])
def test_unsolicited_legacy_alias_and_malformed_responses_never_reach_rows(extra):
    ok = {"symbol": "ACTIVE.US", "collection_evidence": {"status": "OK"}}
    accepted, evidence = archive._validated_news_results([ok, *extra], [{"symbol": "ACTIVE.US"}])
    assert accepted == [ok] and not evidence["complete"]
    assert evidence["accepted_returned"] == 1


@pytest.mark.parametrize("status", ["OK", "ERROR"])
def test_duplicate_requested_responses_are_unknown_instead_of_last_wins(status):
    raw = [{"symbol": "ACTIVE.US", "collection_evidence": {"status": "OK"}},
           {"symbol": "active.us", "collection_evidence": {"status": status}}]
    accepted, evidence = archive._validated_news_results(raw, [{"symbol": "ACTIVE.US"}])
    assert accepted == [] and evidence["duplicate_symbol_count"] == 1 and not evidence["complete"]
    assert archive._collection_report(["ACTIVE.US"], accepted, "2026-10-23")["status_counts"] == {"UNKNOWN": 1}


def test_valid_ok_empty_responses_remain_unchanged():
    raw = [{"symbol": s, "collection_evidence": {"status": status}}
           for s, status in [("ACTIVE.US", "OK"), ("EMPTY.US", "EMPTY")]]
    accepted, evidence = archive._validated_news_results(raw, [{"symbol": r["symbol"]} for r in raw])
    assert accepted == raw and evidence["complete"]


def test_collect_excludes_unsolicited_legacy_response_before_row_formation(monkeypatch, tmp_path):
    import asyncio
    from types import SimpleNamespace
    monkeypatch.setattr(archive, "_utc_now", lambda: NOW)
    monkeypatch.setattr(archive, "_riyadh_date", lambda: "2026-10-23")
    formed = []
    async def batch(items, **kw):
        return SimpleNamespace(items=[{"symbol": "ROP.SW", "collection_evidence": {"status": "EMPTY"}},
                                      {"symbol": "ROG.SW", "collection_evidence": {"status": "OK"}}], meta={})
    monkeypatch.setattr(archive, "_import_news_module", lambda: SimpleNamespace(batch_news_intelligence=batch))
    def row(result, date):
        formed.append(result["symbol"])
        return None
    monkeypatch.setattr(archive, "_row_from_result", row)
    path = tmp_path / "news.json"; args = options(); args.universe = ["ROG.SW", "ROP.SW"]
    args.sheet_id = ""; args.dry_run = True; args.evidence_json = str(path)
    assert asyncio.run(archive.collect(args)) == 1
    assert formed == ["ROP.SW"]
    report = json.loads(path.read_text())
    assert report["response_validation"]["unexpected_count"] == 1 and not report["complete"]


@pytest.mark.parametrize("eligible", [True, False])
def test_legacy_priority_is_removed_before_rotation_capacity_is_sized(monkeypatch, eligible):
    from tests.test_news_adaptive_universe import context, args
    monkeypatch.setattr(archive, "_utc_now", lambda: NOW)
    monkeypatch.setenv("NEWS_ARCHIVE_UNIVERSE", "ROG.SW,BASE.US")
    source = context()
    if eligible:
        source["eligible"] = list(source["pool"])
    universe, items, evidence = archive._select_news_universe(args(max_symbols=6), source, "2026-10-23")
    assert len(universe) == len(items) == 6 and "ROG.SW" not in universe
    assert evidence["rotation_count"] == 3 and evidence["lifecycle_excluded_count"] == 1
    assert evidence["discovery_complete"]
