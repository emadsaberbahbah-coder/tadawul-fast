"""tests/test_recent_fixes.py — regression net for the 2026-06-26 fix wave.

Locks in the behavioral fixes shipped this cycle so CI fails on regression
instead of re-discovering them by hand:

  (a) opportunity_builder v1.0.13 — gated Yahoo->GICS sector normalization
  (b) run_dashboard_sync   v6.10.0 — gated reroute of the four cross-sectional
                                     market pages to the ANALYSIS gateway, which
                                     is the only path carrying the page-level
                                     global rank (v4.4.0) + dedup (v4.5.0) passes
  (c) run_dashboard_sync   v6.9.0  — empty-rows wipe-guard structural invariant
  (d) build_universes      v1.1.0  — safe_write_csv below-floor refusal

WHY THIS FILE EXISTS: the v6.10.0 rank fix was a *routing* miss — the correct
global-rank pass already existed in routes/analysis_sheet_rows.py but the daily
sync never sent the market pages through it. Nothing caught that the fix was off
the live path. These tests assert the wiring (effective gateway, guard defaults,
floor refusal) so that class of silent regression fails the build.

Style: pytest-native (plain def test_* / assert). The two /scripts modules
(run_dashboard_sync, build_universes) are NOT package-importable, so they are
loaded by path via importlib; opportunity_builder imports from the package
(conftest bootstraps sys.path). Import failures SKIP (never hard-fail) so adding
this file to the CI merge gate can only be tripped by a genuine assertion
regression, not by an environment/dependency gap. Every default-state assertion
explicitly clears its own env gate via monkeypatch so the outcome does not depend
on the ambient CI environment.
"""
import importlib.util
import sys
from pathlib import Path

import pytest

_REPO = Path(__file__).resolve().parents[1]
_LOADED: dict = {}


def _ver_at_least(actual, minimum) -> bool:
    """True if dotted version `actual` is >= `minimum`, compared NUMERICALLY
    component-by-component (so "1.0.18" >= "1.0.13" is True and "1.0.9" >=
    "1.0.13" is False — unlike a string compare, which would get the latter
    wrong). WHY (2026-06-29): the version pins below were `== "<introducing
    version>"`, which froze each assertion to one exact release and turned every
    legitimate forward bump into a red CI run (ob 1.0.13->1.0.18, rds
    6.10.0->6.14.0 both tripped it). The intent of this regression net is "the
    fix is PRESENT / not regressed below the version that shipped it", which is a
    floor (>=), not an exact match. The behavior tests beside each pin still
    assert the actual feature, so this only loosens the redundant version proxy.
    """
    def _t(v):
        return tuple(int(p) for p in str(v).strip().split(".")[:3])
    return _t(actual) >= _t(minimum)


def _load_script(rel_path: str, mod_name: str):
    """Load a /scripts module by path (these are scripts, not packages)."""
    if mod_name in _LOADED:
        return _LOADED[mod_name]
    p = _REPO / rel_path
    if not p.exists():
        pytest.skip(f"{rel_path} not present")
    spec = importlib.util.spec_from_file_location(mod_name, str(p))
    mod = importlib.util.module_from_spec(spec)
    # Register BEFORE exec: @dataclass(slots=True) recreates its class and writes
    # it back via sys.modules[__name__].__dict__, which is None if unregistered.
    sys.modules[mod_name] = mod
    try:
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
    except Exception as exc:  # import guard — never block the gate on env gaps
        sys.modules.pop(mod_name, None)
        pytest.skip(f"could not import {rel_path}: {exc!r}")
    _LOADED[mod_name] = mod
    return mod


# --------------------------------------------------------------------------- #
# (a) opportunity_builder v1.0.13 — sector normalization
# --------------------------------------------------------------------------- #
def _ob():
    try:
        from core.analysis import opportunity_builder as ob
    except Exception as exc:
        pytest.skip(f"opportunity_builder import failed: {exc!r}")
    return ob


def test_ob_version_at_least_1_0_13():
    assert _ver_at_least(_ob().OPPORTUNITY_BUILDER_VERSION, "1.0.13")


def test_ob_normalize_sector_translates_yahoo_to_gics():
    ob = _ob()
    # the six differing Yahoo-provider spellings the diversifier must fold to GICS
    assert ob._normalize_sector("Basic Materials") == "Materials"
    assert ob._normalize_sector("Healthcare") == "Health Care"
    assert ob._normalize_sector("Consumer Cyclical") == "Consumer Discretionary"
    assert ob._normalize_sector("Consumer Defensive") == "Consumer Staples"
    assert ob._normalize_sector("Technology") == "Information Technology"
    assert ob._normalize_sector("Financial Services") == "Financials"


def test_ob_normalize_sector_passthrough_for_gics_and_gaps():
    ob = _ob()
    # already-GICS strings and the "" / Unknown data-gap buckets pass through
    for s in ("Financials", "Health Care", "Materials", "Energy", "Unknown", "", "  "):
        assert ob._normalize_sector(s) == (s or "").strip()


def test_ob_sector_normalize_gate_default_on(monkeypatch):
    # 2026-07-09: was test_..._default_off, asserting False. opportunity_builder
    # v1.0.22 deliberately flipped the default OFF->ON (dormant-bug fix + live
    # 5023.SR damage -- see _env_sector_normalize's own docstring) and this test
    # was never updated, so it has failed on every push-triggered CI run since
    # v1.0.22 shipped (schedule-triggered runs skip ci-tests, so it went
    # unnoticed). Asserting the CURRENT, already-shipped, already-safe default.
    monkeypatch.delenv("TFB_OPP_SECTOR_NORMALIZE", raising=False)
    assert _ob()._env_sector_normalize() is True


def test_ob_sector_normalize_gate_on_recognized(monkeypatch):
    ob = _ob()
    for v in ("1", "true", "on", "yes"):
        monkeypatch.setenv("TFB_OPP_SECTOR_NORMALIZE", v)
        assert ob._env_sector_normalize() is True


# --------------------------------------------------------------------------- #
# (b) run_dashboard_sync v6.10.0 — ranked-market-page reroute
# --------------------------------------------------------------------------- #
_RANKED = {"Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds"}


def _rds():
    return _load_script("scripts/run_dashboard_sync.py", "rds_under_test")


def test_rds_version_at_least_6_10_0():
    assert _ver_at_least(_rds().SCRIPT_VERSION, "6.10.0")


def test_rds_ranked_market_pages_scope_exact():
    # must mirror routes/analysis_sheet_rows.py's ranked-market-page scope exactly
    assert set(_rds()._RANKED_MARKET_PAGES) == _RANKED


def test_rds_market_gateway_toggle_default_off(monkeypatch):
    monkeypatch.delenv("TFB_SYNC_MARKET_ANALYSIS_GATEWAY", raising=False)
    assert _rds()._market_analysis_gateway_enabled() is False


def test_rds_gateway_off_is_byte_identical_routing(monkeypatch):
    """OFF: every task's effective gateway == its configured gateway."""
    rds = _rds()
    monkeypatch.delenv("TFB_SYNC_MARKET_ANALYSIS_GATEWAY", raising=False)
    # 2026-07-09: v6.22.0 added TFB_SYNC_SAFE_GATEWAYS, an INDEPENDENT override
    # that is default-ON and forces the four ranked pages to "analysis"
    # regardless of the toggle above -- this test predates that and was never
    # updated, so it failed on every push since v6.22.0 shipped (schedule runs
    # skip ci-tests, so it went unnoticed). Disabling it here restores this
    # test's original, narrower claim: with BOTH override mechanisms off, every
    # task's gateway is its own configured value, byte-identical to v6.9.0.
    # The safe-gateways-ON production default is covered separately below.
    monkeypatch.setenv("TFB_SYNC_SAFE_GATEWAYS", "0")
    for t in rds._default_tasks():
        assert rds._effective_gateway(t) == t.gateway


def test_rds_safe_gateways_default_on_reroutes_ranked_pages(monkeypatch):
    # 2026-07-09 (new): the actual production default -- TFB_SYNC_SAFE_GATEWAYS
    # unset (defaults ON) -- was not covered by any existing test. This is the
    # v6.22.0 fix for the 2026-07-08 poisoning event: ranked pages must resolve
    # to "analysis" (the only market router with the transposition firewall)
    # even with the older v6.10.0 toggle left at its own default-off.
    rds = _rds()
    monkeypatch.delenv("TFB_SYNC_SAFE_GATEWAYS", raising=False)
    monkeypatch.delenv("TFB_SYNC_MARKET_ANALYSIS_GATEWAY", raising=False)
    monkeypatch.delenv("TFB_SYNC_MARKET_GATEWAY", raising=False)
    for t in rds._default_tasks():
        eff = rds._effective_gateway(t)
        if t.sheet_name in _RANKED:
            assert eff == "analysis", f"{t.sheet_name} should default to analysis (SAFE-GATEWAYS)"
        else:
            assert eff == t.gateway, f"{t.sheet_name} must keep its configured gateway"


def test_rds_gateway_on_reroutes_only_the_four_market_pages(monkeypatch):
    rds = _rds()
    monkeypatch.setenv("TFB_SYNC_MARKET_ANALYSIS_GATEWAY", "1")
    for t in rds._default_tasks():
        eff = rds._effective_gateway(t)
        if t.sheet_name in _RANKED:
            assert eff == "analysis", f"{t.sheet_name} should reroute to analysis"
        else:
            assert eff == t.gateway, f"{t.sheet_name} must keep its configured gateway"


def test_rds_my_portfolio_never_rerouted(monkeypatch):
    rds = _rds()
    monkeypatch.setenv("TFB_SYNC_MARKET_ANALYSIS_GATEWAY", "1")
    mp = [t for t in rds._default_tasks() if t.sheet_name == "My_Portfolio"]
    assert mp, "My_Portfolio task missing"
    assert rds._effective_gateway(mp[0]) == "enriched"


def test_rds_analysis_gateway_targets_router_with_enriched_fallback(monkeypatch):
    # 2026-07-09: v6.22.1 deliberately made the SAFE-mode (default) analysis
    # chain analysis-ONLY -- no enriched tail -- because /v1/advanced (the
    # chain's old next hop) is unfirewalled; an analysis outage now PRESERVES
    # last-good rows via the empty/shrink guards instead of falling back to an
    # unsafe live route. This test predates that decision and was never
    # updated, so it failed on every push since v6.22.1 shipped (schedule runs
    # skip ci-tests, so it went unnoticed). Disabling safe-gateways here
    # restores the legacy (v6.21.0) chain this test was written to check.
    monkeypatch.setenv("TFB_SYNC_SAFE_GATEWAYS", "0")
    rds = _rds()
    cands = rds._endpoint_candidates_for_gateway("analysis")
    assert cands and cands[0] == "/v1/analysis/sheet-rows"
    # the legacy (safe-gateways-off) analysis chain must end at enriched so an
    # analysis outage fails soft
    assert any("enriched" in c for c in cands)


def test_rds_safe_gateways_default_on_analysis_chain_has_no_unfirewalled_fallback(monkeypatch):
    # 2026-07-09 (new): the actual production default was not covered by any
    # existing test. In SAFE mode the analysis chain must stay analysis-only --
    # no /v1/advanced or /v1/enriched hop -- so a routing bug can never
    # silently accept rows from an unfirewalled endpoint (routes.investment_
    # advisor v2.17.0 carries no transposition firewall; confirmed serving
    # 200s live, 2026-07-09 audit).
    monkeypatch.delenv("TFB_SYNC_SAFE_GATEWAYS", raising=False)
    rds = _rds()
    cands = rds._endpoint_candidates_for_gateway("analysis")
    assert cands and cands[0] == "/v1/analysis/sheet-rows"
    assert not any("enriched" in c or "advanced" in c for c in cands)


# --------------------------------------------------------------------------- #
# (c) run_dashboard_sync v6.9.0 — empty-rows wipe-guard invariant
# --------------------------------------------------------------------------- #
def test_rds_empty_guard_default_on(monkeypatch):
    monkeypatch.delenv("TFB_SYNC_EMPTY_GUARD", raising=False)
    assert _rds()._empty_guard_enabled() is True


def test_rds_empty_guard_off_recognized(monkeypatch):
    rds = _rds()
    for v in ("0", "false", "off", "no"):
        monkeypatch.setenv("TFB_SYNC_EMPTY_GUARD", v)
        assert rds._empty_guard_enabled() is False


def test_rds_all_data_pages_expect_rows():
    """The five data pages must stay protected (expects_rows=True) so a 0-row
    fetch SKIPS clear+write instead of blanking the tab on a provider outage."""
    rds = _rds()
    by_name = {t.sheet_name: t for t in rds._default_tasks()}
    for name in (_RANKED | {"My_Portfolio"}):
        assert name in by_name, f"{name} task missing"
        assert by_name[name].expects_rows is True, f"{name} must have expects_rows=True"


# --------------------------------------------------------------------------- #
# (d) build_universes v1.1.0 — safe_write_csv below-floor refusal
# --------------------------------------------------------------------------- #
def _bu():
    return _load_script("scripts/build_universes.py", "bu_under_test")


def test_bu_version_at_least_1_1_0():
    assert _ver_at_least(_bu().BUILD_UNIVERSES_VERSION, "1.1.0")


def test_bu_safe_write_refuses_below_floor(tmp_path):
    """Market_Leaders floor is 50; 10 rows must be REFUSED and no file written."""
    bu = _bu()
    out = tmp_path / "market_leaders.csv"
    written, skipped = [], []
    rows = [(f"S{i}", f"N{i}") for i in range(10)]
    ok = bu.safe_write_csv(str(out), rows, "Market_Leaders", written, skipped)
    assert ok is False
    assert not out.exists(), "below-floor write must not create the CSV"
    assert ("Market_Leaders", 10) in skipped
    assert written == []


def test_bu_safe_write_refuses_empty_unknown_page(tmp_path):
    """Unknown page => floor defaults to 1; 0 rows must be refused."""
    bu = _bu()
    out = tmp_path / "x.csv"
    written, skipped = [], []
    ok = bu.safe_write_csv(str(out), [], "Some_Other_Page", written, skipped)
    assert ok is False
    assert not out.exists()


def test_bu_safe_write_allows_at_or_above_floor(tmp_path):
    """Clearing the floor writes the CSV and records it as written."""
    bu = _bu()
    out = tmp_path / "market_leaders.csv"
    written, skipped = [], []
    rows = [(f"S{i}", f"N{i}") for i in range(50)]  # == floor
    ok = bu.safe_write_csv(str(out), rows, "Market_Leaders", written, skipped)
    assert ok is True
    assert out.exists()
    assert ("Market_Leaders", 50) in written
    assert skipped == []
