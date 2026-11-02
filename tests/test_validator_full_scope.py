"""Full literal-sheet scope must cover tail rows and report bounded overflow."""
from __future__ import annotations

import asyncio
import re

from scripts import validate_dashboard as vd
from scripts import tfb_workbook_dq_audit as dq


class Registry:
    version = "test"

    def normalize(self, page):
        return page

    def headers(self, page):
        return ["Symbol", "Current Price", "Expected ROI 12M"]

    def keys(self, page):
        return ["symbol", "current_price", "expected_roi_12m"]


def validate_grid(monkeypatch, size, cap=None):
    reg = Registry()
    grid = [reg.headers("Global_Markets")]
    grid += [[f"S{i}", 10, 0.1] for i in range(size)]
    grid[-1][-1] = 0.9  # Only tail row violates the ROI ceiling.
    calls = []

    def read_range(sid, area):
        calls.append(area)
        end = int(re.search(r"DZ(\d+)$", area).group(1))
        return grid[:end]

    async def no_logical_reader():
        return None

    monkeypatch.setattr(vd, "_load_registry", lambda: reg)
    monkeypatch.setattr(vd, "_engine_version", lambda: "test")
    monkeypatch.setattr(vd, "_resolve_read_range", lambda: read_range)
    monkeypatch.setattr(vd, "_resolve_get_sheet_rows", no_logical_reader)
    if cap is None:
        monkeypatch.delenv("VALIDATE_MAX_ROWS", raising=False)
        cap = vd.create_parser().parse_args([]).max_rows
    results, _ = asyncio.run(vd.validate(pages=["Global_Markets"], sid="test", max_rows=cap))
    return {r.name: r for r in results}, calls


def test_default_audit_reaches_current_global_tail(monkeypatch):
    checks, calls = validate_grid(monkeypatch, 6611)
    assert checks["scope.coverage"].status == "PASS"
    assert checks["scope.coverage"].count == 6611
    assert checks["sanity.roi12_within_softcap"].examples == ["S6610=0.9"]
    assert len(calls) == 1  # Larger bounded Sheets read; no per-row/provider fan-out.


def test_overflow_is_failed_incomplete_audit(monkeypatch):
    checks, _ = validate_grid(monkeypatch, 20001, cap=20000)
    scope = checks["scope.coverage"]
    assert scope.count == 20000
    assert scope.status == "FAIL" and "UNKNOWN" in scope.detail
    assert vd._exit_code(list(checks.values())) == 2


def test_exact_bound_is_complete(monkeypatch):
    checks, _ = validate_grid(monkeypatch, 1500, cap=1500)
    assert checks["scope.coverage"].status == "PASS"
    assert checks["scope.coverage"].count == 1500


def test_logical_engine_fallback_cannot_certify_literal_extent(monkeypatch):
    monkeypatch.setattr(vd, "_load_registry", Registry)
    monkeypatch.setattr(vd, "_engine_version", lambda: "test")
    monkeypatch.setattr(vd, "_resolve_read_range", lambda: None)

    async def logical_reader():
        return (lambda **kw: [{"symbol": "S1", "current_price": 10,
                              "expected_roi_12m": .1}], False, "engine")

    monkeypatch.setattr(vd, "_resolve_get_sheet_rows", logical_reader)
    results, _ = asyncio.run(vd.validate(pages=["Global_Markets"], sid="test", max_rows=20000))
    scope = next(r for r in results if r.name == "scope.coverage")
    assert scope.status == "FAIL" and "UNKNOWN literal-sheet extent" in scope.detail


def context(size, audit_count, status="PASS", detail=""):
    rows = [["Symbol", "Expected ROI 12M"]] + [[f"S{i}", .1] for i in range(size)]
    page = dq.Page("Global_Markets", rows, len(rows))
    audit = [["Page", "Check", "Status", "Count", "Detail"],
             ["Global_Markets", "scope.coverage", status, audit_count, detail]]

    class Book:
        def rows(self, title):
            return audit if title == "Dashboard_Audit" else []

    return {"book": Book(), "pages": {"Global_Markets": page}}


def test_independent_audit_accepts_real_exact_1500_page():
    result = dq.dq_72_validator_scope(context(1500, 1500))
    assert result["status"] == "PASS" and result["count"] == 0


def test_independent_audit_detects_arbitrary_cap():
    result = dq.dq_72_validator_scope(context(6611, 2000))
    assert result["status"] == "FAIL" and result["count"] == 1


def test_independent_audit_rejects_sampling_marker_even_with_matching_count():
    result = dq.dq_72_validator_scope(context(1500, 1500, "WARN", "SAMPLE_ONLY: 1500"))
    assert result["status"] == "FAIL"


def test_independent_audit_requires_scope_record():
    ctx = context(10, 10)
    ctx["pages"]["Mutual_Funds"] = dq.Page("Mutual_Funds", [["Symbol"], ["ETF.US"]], 2)
    result = dq.dq_72_validator_scope(ctx)
    assert result["status"] == "FAIL" and result["count"] == 1


def test_independent_audit_rejects_duplicate_scope_receipts():
    ctx = context(10, 10)
    audit = ctx["book"].rows("Dashboard_Audit")
    audit.append(list(audit[-1]))
    result = dq.dq_72_validator_scope(ctx)
    assert result["status"] == "FAIL" and result["count"] == 1
