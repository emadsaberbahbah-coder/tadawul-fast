"""Repeated display counters cannot replace complete source evidence."""
import pytest

from scripts import audit_decision_surface_freshness as audit
from tests.test_decision_surface_freshness import (
    FLOORS,
    NOW,
    _attach_pool_receipt,
    _native_pool_case,
    approved_fixture,
    approved_grids,
    portfolio_grid,
    source_columns_for,
    status_grid,
    top10_grid,
)


@pytest.fixture
def scope_case(tmp_path):
    return approved_fixture.__wrapped__(tmp_path)


def audit_case(scope_case, *, native=False, page=None, replacement=None):
    if native:
        scope, grids, sources, receipt = _native_pool_case(scope_case)
        _attach_pool_receipt(grids, receipt)
    else:
        scope, symbols = scope_case
        grids, sources = approved_grids(symbols)
    if page:
        physical = 255 if page == "Market_Leaders" else 2474
        used = physical - (5 if native and page == "Mutual_Funds" else 0)
        original = f"{page} {used}/{physical}"
        grids[2][1][1] = grids[2][1][1].replace(
            original, replacement.format(page=page, used=used, physical=physical))
    return audit.audit_surfaces(
        *grids, now_utc=NOW, approved_scope=scope, source_grids=sources,
        source_columns=source_columns_for(sources), expected_source_run="90001")


@pytest.mark.parametrize("page", audit.POOL_PAGES)
@pytest.mark.parametrize("claims", ["1/255, 255/255", "255/255, 1/255",
                                    "255/255, 255/255", "1/255, 255/255, 255/255"])
def test_parser_keeps_any_repeated_page_unknown(page, claims):
    other = next(name for name in audit.POOL_PAGES if name != page)
    text = ", ".join(f"{page} {claim}" for claim in claims.split(", "))

    assert audit.parse_pool_counts(text + f", {other} 7/7") == {
        other: {"used": 7, "total": 7}}


@pytest.mark.parametrize("native", [False, True])
def test_single_counter_controls_keep_legacy_and_native_proofs(scope_case, native):
    report = audit_case(scope_case, native=native)

    assert report.exit_code == 0 and report.executable
    assert not report.findings
    assert report.top10_pool_reconciliation["valid"] is True
    assert report.top10_pool_reconciliation["receipt_required"] is native


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("page", ["Market_Leaders", "Mutual_Funds"])
@pytest.mark.parametrize("replacement", [
    "{page} 1/{physical}, {page} {used}/{physical}",
    "{page} {used}/{physical}, {page} 1/{physical}",
    "{page} {used}/{physical}, {page} {used}/{physical}",
])
def test_duplicates_block_both_compatibility_and_native_receipts(
        scope_case, native, page, replacement):
    report = audit_case(scope_case, native=native, page=page, replacement=replacement)

    assert report.exit_code == 2 and not report.executable
    assert page not in report.top10_pool_counts
    assert report.top10_pool_reconciliation["valid"] is False
    assert "T10_POOL_RECONCILIATION_UNPROVEN" in {f.code for f in report.findings}


def test_generic_legacy_cap_still_works_but_duplicate_counter_does_not():
    top10 = top10_grid(full=False)
    top10[1][1] = top10[1][1].replace("Market_Leaders 1356/1356", "Market_Leaders 1/1356")
    kwargs = {"now_utc": NOW, "min_rows": FLOORS}
    control = audit.audit_surfaces(status_grid(), portfolio_grid(), top10, **kwargs)
    assert control.exit_code == 0 and control.executable

    top10[1][1] += " | Market_Leaders 1356/1356"
    ambiguous = audit.audit_surfaces(status_grid(), portfolio_grid(), top10, **kwargs)

    assert ambiguous.exit_code == 2 and not ambiguous.executable
    assert "T10_POOL_COUNT_MISSING" in {f.code for f in ambiguous.findings}
