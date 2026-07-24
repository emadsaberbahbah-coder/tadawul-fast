"""
scripts/verify_deployment.py — TFB Deployment Verifier
=======================================================
VERSION 1.0.0  (2026-07-19)  — NEW SCRIPT (operations, deliverable #24)

WHY: every session ends with a run of one-line version checks pasted one at a
time, and the state of the flag set has to be reconstructed from memory each
time. Both are mechanical and both are error-prone by hand — a flag believed
armed but actually absent has already cost this project real debugging time.
This collapses the whole check into ONE command.

WHAT IT REPORTS:
  1. VERSIONS  — every Gen-1/Gen-2 module's live __version__, compared against
     an expected manifest. Drift is flagged as BEHIND / AHEAD / MISSING, never
     silently tolerated.
  2. FLAGS     — every environment switch the platform reads, with its live
     value, its default, and whether it is ARMED. Kill-switches are shown with
     their safe direction so "off" is never mistaken for "broken".
  3. SELFTESTS — optional (--selftests): runs each module's own selftest and
     reports pass/fail counts.
  4. VERDICT   — one line: CLEAN, DRIFT, or FAIL.

HONESTY: a module that cannot be imported is reported as MISSING with the
exception type — never counted as passing. An unknown flag value is shown
verbatim rather than coerced.

USAGE:
  python3 scripts/verify_deployment.py              # versions + flags
  python3 scripts/verify_deployment.py --selftests  # also run selftests
  python3 scripts/verify_deployment.py --json       # machine-readable
  python3 scripts/verify_deployment.py --strict     # DRIFT exits 2 (v1.0.15+)

EXIT CODES
  Default (v1.0.14-compatible):  FAIL -> 1,  everything else -> 0
  With --strict:                 CLEAN -> 0,  DRIFT -> 2,  FAIL -> 1

SCOPE OF "CLEAN"
  CLEAN describes the Render process this shell can see. Four flags are
  workflow-scoped (set in .github/workflows/daily_sync.yml) and are reported
  but deliberately excluded from the verdict — they are not observable here.
  CLEAN therefore means "the Render side is clean", not "the deployment is".
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

SCRIPT_VERSION = "1.0.17"  # v1.0.17 (2026-07-24) — MANIFEST PIN ONLY, no logic touched. data_engine_v2 advances 5.118.0 -> 5.119.0 (Fix BB: canonical env parsing, bounded batch hydration, thread-safe fundamentals LKG with an identity-compatibility guard, health/stats observability). Verified before bumping: AST diff shows ZERO function or class removals and all 49 decision-related functions — recommendation, investability, scoring, forecast, projection, valuation, ranking, eligibility, Shariah, compliance, risk, target, ROI, confidence and tier logic — are BYTE-IDENTICAL to 5.118.0. Only 14 functions changed, every one of them env parsing, LKG store management, batch hydration, or observability. The author's "no recommendation or numerical-model change" claim is therefore proven, not accepted. This pin is bumped because the verifier's own v1.0.10 rule stands: a stale manifest makes every drift report fiction, and leaving 5.118.0 here would report a deliberate, evidence-checked deployment as DRIFT. That is the one exception to the window freeze on this file — a wrong VERDICT, not a wrong label. # v1.0.16 (2026-07-24)  # v1.0.16 (2026-07-24) — EXTERNAL AUDIT OF v1.0.15, four accepted findings, all REPORTING-ONLY (this script still changes zero production behaviour). (A) MY DEFECT, SHIPPED IN v1.0.15: an EXPLICITLY SET TFB_OPP_MAX_CANDIDATES=0 was scored OUT OF SAFE RANGE, because _range_violation only exempts the UNSET case while the band is 300..2000 and 0 is BOTH the documented code default AND the documented "full pool" value. v1.0.14 fixed unset; the explicitly-set-to-the-default case was one step over and I missed it. _RANGE_FLAGS entries now carry an explicit set of sentinel values that are always legal. (B) BOOLEAN VOCABULARY SPLIT-BRAIN — the audit reported this as a verifier gap; it is worse than that, and it is a PRODUCTION defect the verifier merely made visible. Live grep of main: verifier _ARMED={1,true,yes,on}; run_dashboard_sync _TRUTHY={1,true,yes,y,on}; track_performance _TRUTHY={1,true,yes,y,on,t,enabled,enable}; advanced_analysis carries BOTH {1,true,yes,y,on} AND {1,true,yes}; data_engine_v2 carries THREE different sets. So TFB_X=enabled is ON for track_performance and OFF for advanced_analysis SIMULTANEOUSLY. The naive fix — widen _ARMED to the union — would be actively harmful: it would print ARMED over a flag that half the codebase reads as off, converting a false DRIFT into a false CLEAN. Instead, values inside the union but outside the intersection are now reported AMBIGUOUS and degrade the verdict to DRIFT with the disagreement named. Unifying the production vocabularies is a POST-WINDOW defect (it touches five modules on the hot path). (C) SELFTESTS COULD PASS WITHOUT RUNNING — status was "PASS" if returncode==0, and the SELFTEST marker was only harvested for the summary line, never required. A target that exits 0 without testing anything reported PASS. Marker is now required; returncode 0 with no marker is NO_MARKER, which degrades to DRIFT (not FAIL — the target ran, we just cannot prove it tested). (D) DATA ENGINE WAS UNVERIFIABLE — expected version was "", so check_modules short-circuited to INFO whatever it read. Pinned to 5.118.0 against live main, and the attribute corrected to __version__ (ENGINE_VERSION does not exist in that module; the check was only ever succeeding through the alternate-attribute fallback). ALSO: duplicate "default" key removed from the check_flags record; --strict documented in the usage header; the CLEAN line now states its own scope. AUDIT ITEMS DEFERRED POST-WINDOW, with reasons: subprocess-isolated imports with timeouts (real, but rewriting the import path of the tool we depend on mid-window is the wrong trade); parsing daily_sync.yml for workflow-scoped flags; self-tests for the decision layer (largely answered instead by ci.yml, which now runs the real opportunity_builder and portfolio_actions suites on every push); config/deployment_manifest.json as a shared source; split armed-counts; packaging.version for semantic compare (adds a dependency to solve a problem no current version string has). # v1.0.15 (2026-07-24) — TWO GATE HOLES, both found by live inspection of main, both closed here. (A) DRIFT EXITED 0: main() returned "0 if verdict != FAIL else 1" on BOTH the --json and the text path, so a pipeline that shelled out to this verifier continued happily over BEHIND/AHEAD versions, a disarmed kill-switch, or an out-of-range budget — the verifier said DRIFT and the exit status said proceed. A --strict flag now maps CLEAN=0, DRIFT=2, FAIL=1. It is OPT-IN: without --strict the exit codes are byte-identical to v1.0.14, so no existing caller changes behaviour, and the flag itself is the kill-switch. (B) run_dashboard_sync WAS NOT IN THE MANIFEST AT ALL: the script that writes every market page and My_Portfolio into the production workbook was the one major script whose version this verifier could not see. Added at 6.27.0 (live-verified against main). Import-safety checked before adding: its top-level imports are stdlib+asyncio only (gspread/google/requests are all deferred inside functions), it carries a __main__ guard, and its sole module-level side effect is logging.basicConfig(), which cannot affect this verifier because the verifier prints rather than logs. NOTE for the record: track_performance was NOT missing — it is present at 6.27.0 and matches live. An earlier review claim that the verifier "expects 6.27 while the file shows 6.18" confused the stale module DOCSTRING with the runtime SCRIPT_VERSION constant; the constant is correct and so is this manifest. # v1.0.14 (2026-07-24) — MY BUG, caught by its own first live run: _range_violation was fed the RESOLVED value (env or code default), so an UNSET TFB_OPP_MAX_CANDIDATES resolved to its legitimate default 0 ("no clamp") and printed OUT OF RANGE, forcing a permanent false DRIFT. A verifier that cries wolf is worse than none. The range check now sees the RAW env only: unset => code default governs => no violation, exactly as the v1.0.13 docstring promised. # v1.0.13 (2026-07-24) — MY OWN BUG, caught in review: the v1.0.8/1.0.11 edits appended the new value flags with a str.replace anchored on a token that ends BOTH _VALUE_FLAGS and _WORKFLOW_SCOPED, so seven RENDER-scoped flags (the three rule lists + the four numeric budgets) were silently filed as workflow-only and printed "GH-ENV — NOT visible from this shell" instead of being read from the live process. That is the exact class of fiction this verifier exists to prevent — and it is why TFB_EXIT_BY_RULE_EXTRA looked workflow-scoped on 2026-07-22. _WORKFLOW_SCOPED is now the genuine four. ALSO: numeric budgets are range-checked (a value outside its safe band is DRIFT, not "SET"); TFB_OPP_BUILD_OFFLOOP + TFB_ADV_ENGINE_CALL_TIMEOUT are marked protection-expected-armed so CLEAN can never print over a disabled 502 guard; route pin 4.12.0->4.13.0. # v1.0.12 (2026-07-24): routes.advanced_analysis 4.11.0->4.12.0 (off-loop opportunity build); FLAGS +TFB_OPP_BUILD_OFFLOOP/_S. # v1.0.11 (2026-07-24, external-review catches): +routes.advanced_analysis 4.11.0 pinned (the layer that owns the edge-timeout guard was unverifiable); TFB_OPP_MAX_CANDIDATES + TFB_OPP_AUDIT_ROWS_MAX moved to _VALUE_FLAGS (a numeric 300/1000 was being read as a boolean and printed "off"); FLAGS += eight live-verified switches never checked (TFB_ADV_ENGINE_CALL_TIMEOUT[_S], TFB_PF_IDENTITY_GATE, TFB_PF_CONFIRM_PERSIST, TFB_PF_VF_CONFLICT_GUARD, TFB_RULE1B_CAPPED_EXIT_GATE, TFB_PA_SUKUK_ASSET_CLASS, TFB_OPP_AUDIT_ROWS_MAX); and a DISARMED KILL-SWITCH now degrades the verdict to DRIFT instead of printing CLEAN over an unprotected system. pa pin 1.7.0->1.7.1. # v1.0.10 (2026-07-24): MANIFEST RE-SYNC after two unattended build days — a stale manifest makes every drift report fiction. Live-verified pins: opportunity_builder 1.5.0->1.7.0, portfolio_actions 1.4.0->1.7.0 (PRECEDENCE), run_daily_brief 1.13.0->1.15.1, run_calendar_sync 1.1.0->1.1.1. FLAGS += the five builder/pa switches shipped since v1.0.9 and never checked: TFB_OPP_PREGATE_ORDER, TFB_OPP_SELL_CLASS_GATE, TFB_TRIM_BY_RULE_GATE, TFB_PA_PRECEDENCE_GATE (armed kills) + TFB_OPP_MAX_CANDIDATES, TFB_OPP_INVESTABILITY_GATE, TFB_OPP_RANK_BY_ENGINE_ROI (value flags). # v1.0.9 (2026-07-22): +run_daily_brief 1.13.0 + send_digest 1.2.1 (rulebook on all mail surfaces) + run_calendar_sync 1.1.0 (sticky dates), +run_shadow_scorer 1.2.1 (DEF-R _now_riyadh), +pit_snapshot 1.0.1 (harvest ticker-shape guard); # v1.0.8 (2026-07-21 PM): compliance-gate wave — opportunity_builder 1.5.0, +core.analysis.portfolio_actions 1.4.0; FLAGS +TFB_COMPLIANCE_SURFACE_GATE/TFB_ELIGIBILITY_GATE/TFB_EXIT_BY_RULE_GATE (armed kills) +TFB_SHARIAH_FAIL_LIST/TFB_EXIT_BY_RULE_EXTRA/TFB_KSA_FOREIGN_RESTRICTED (value flags); # v1.0.7 (2026-07-21): WINDOW MANIFEST SYNC — the verifier must mirror every live-verified bump or its drift reports are fiction: opportunity_builder 1.4.0 (W-2), run_shadow_scorer 1.2.0 (P0-C), track_performance 6.27.0 (W-1); +core.providers.yahoo_chart_provider 8.10.0 (W-5); SCRIPTS +refresh_shariah_authority 1.1.0, +backup_workbook 1.0.0 (W-4), +pit_snapshot 1.0.0 (W-6); check_scripts gains a __version__ fallback (new scripts use the calendar_sync convention); FLAGS +W-2 freshness family, +scorer honesty pair (workflow-scoped), +TFB_YC_SYMBOL_SKIP, +TFB_SHARIAH_SHEET_ID; v1.0.6 (2026-07-20): +TFB_SR_TRANSIENT_RETRY; v1.0.5: v1.0.5 (2026-07-20): manifest sync — opportunity_builder 1.3.0, portfolio_actions 1.3.0 (live-verified); v1.0.4: +TFB_OPP_REF_CONSERVATIVE (D-12) in FLAGS

# (import path, version attribute, expected version, label)
MODULES: List[Tuple[str, str, str, str]] = [
    ("core.compliance_gate", "__version__", "1.0.0", "compliance gate"),
    ("core.shariah_authority", "__version__", "1.0.0", "shariah authority"),
    ("core.corporate_actions", "__version__", "1.0.0", "corporate actions"),
    ("core.regime", "__version__", "1.0.0", "regime"),
    ("core.risk_limits", "__version__", "1.0.0", "risk limits"),
    ("core.validation", "__version__", "1.0.0", "validation harness"),
    ("core.regret", "__version__", "1.0.0", "regret ledger"),
    ("core.scoring", "__version__", "5.10.0", "scoring"),
    ("core.enriched_quote", "MODULE_VERSION", "4.10.0", "enriched quote"),
    ("core.analysis.opportunity_builder", "OPPORTUNITY_BUILDER_VERSION",
     "1.7.0", "opportunity builder"),
    ("core.analysis.portfolio_actions", "PORTFOLIO_ACTIONS_VERSION",
     "1.7.1", "portfolio actions"),
    ("routes.advanced_analysis", "ADVANCED_ANALYSIS_VERSION", "4.13.0",
     "advanced analysis route"),
    ("core.analysis.top10_selector", "TOP10_SELECTOR_VERSION", "4.23.0",
     "top10 selector"),
    ("core.providers.yahoo_chart_provider", "PROVIDER_VERSION", "8.10.0",
     "yahoo chart provider"),
    # v1.0.16: was ("ENGINE_VERSION", "") -> always INFO, never enforced. The
    # attribute does not exist in that module (only __version__), so the read
    # was surviving purely on the alternate-attribute fallback.
    ("core.data_engine_v2", "__version__", "5.119.0", "data engine"),
]

# scripts are checked by SCRIPT_VERSION via a light import
SCRIPTS: List[Tuple[str, str, str]] = [
    ("run_shadow_board", "1.1.3", "shadow board"),
    ("run_weekly_brief", "1.0.2", "weekly brief"),
    ("run_shadow_scorer", "1.2.1", "shadow scorer"),
    ("track_performance", "6.27.0", "track performance"),
    # v1.0.15: the production workbook writer — every market page and the
    # My_Portfolio rebuild pass through this script. Verified import-safe
    # (stdlib+asyncio at module level) before being added here.
    ("run_dashboard_sync", "6.27.0", "dashboard sync"),
    ("refresh_shariah_authority", "1.1.0", "shariah refresh"),
    ("backup_workbook", "1.0.0", "workbook backup"),
    ("pit_snapshot", "1.0.1", "pit snapshot"),
    ("run_daily_brief", "1.15.1", "daily brief"),
    ("send_digest", "1.2.1", "digest"),
    ("run_calendar_sync", "1.1.1", "calendar sync"),
]

# (env name, default, meaning when ARMED, is_kill_switch)
FLAGS: List[Tuple[str, str, str, bool]] = [
    ("TFB_COMPLIANCE_GATE_ENABLED", "0", "compliance gate evaluates", False),
    ("TFB_SHADOW_COMPLIANCE", "0", "selector logs [SHADOW-GATE]", False),
    ("TFB_ENRICH_COMPLIANCE", "0", "quotes carry compliance stamp", False),
    ("TFB_OPP_NETEDGE_ANNOTATE", "0", "tickets carry net-edge stamp", False),
    ("TFB_PA_HOLDEDGE_ANNOTATE", "0", "holdings carry hold-edge stamp", False),
    ("TFB_OPP_VENUE_FLOORS", "0", "sub-floor tickets defer (D-8)", False),
    ("TFB_SCORE_ROI_SOFTCAP", "0", "ROI soft-cap ends 35/17.5 saturation", False),
    ("TFB_TOP10_TRADABILITY_GATE", "0", "untradable names excluded (D-10)", False),
    ("TFB_OPP_STOP_VOL_UNITS_FIX", "0", "stop volatility units fixed (D-11)", False),
    ("TFB_OPP_REF_CONSERVATIVE", "0", "ticket ref = min(valuation, engine 12M) (D-12)", False),
    ("TFB_PA_PROTECT_SUKUK", "1", "sukuk never a SELL leg (D-9)", True),
    ("TFB_TRACK_CA_LEDGER", "1", "confirmed CA forces verification", True),
    ("TFB_SR_TRANSIENT_RETRY", "1", "sheets reads retry transient transport errors", True),
    ("TRACK_HORIZONS", "1M,3M", "7D/14D checkpoints recorded", False),
    ("TFB_BACKTEST_KSA_YF", "0", "deep history unblocks hypothesis backtest", False),
    ("TFB_BACKTEST_NONOVERLAP", "0", "non-overlapping windows (honest t-stat)", False),
    ("TFB_BACKTEST_DSR_GATE", "0", "deflated-Sharpe penalty on acceptance", False),
    ("TFB_OPP_STOP_VOL_MULT", "2.5", "stop = mult x monthlyized vol", False),
    ("TFB_SYNC_NAME_DEDUP_MODE", "", "duplicate-name quarantine (D-4)", False),
    ("TFB_TICKET_FRESHNESS_GATE", "1", "stale-priced candidates defer (W-2)", True),
    ("TFB_TICKET_MAX_QUOTE_AGE_MIN", "15", "in-session live-quote max age (W-2)", False),
    ("TFB_TICKET_FALLBACK_MAX_AGE_H", "78", "no-calendar freshness cap (W-2)", False),
    ("TFB_YC_SYMBOL_SKIP", "", "yahoo-chart hard-skips dead symbols (W-5)", False),
    ("TFB_SHARIAH_SHEET_ID", "", "authority reader workbook override (P0-B)", False),
    ("TFB_SHADOW_PRICE_HONESTY", "1", "scorer excludes stale bars, honest exclusions (P0-C)", True),
    ("TFB_SHADOW_MIN_FRESH_PCT", "60", "challenger fresh-coverage floor (P0-C)", False),
    ("TFB_COMPLIANCE_SURFACE_GATE", "1", "KSA authority FAIL blocks candidates", True),
    ("TFB_ELIGIBILITY_GATE", "1", "Nomu + foreign-restricted blocked (operator universe)", True),
    ("TFB_EXIT_BY_RULE_GATE", "1", "held FAIL names force EXIT-BY-RULE, uncappable", True),
    ("TFB_SHARIAH_FAIL_LIST", "", "authority FAIL override list (CSV)", False),
    ("TFB_EXIT_BY_RULE_EXTRA", "", "operator model-screen exits, e.g. BBD.US,NMM.US", False),
    ("TFB_KSA_FOREIGN_RESTRICTED", "4030.SR", "broker-rejected symbols for the operator", False),
    ("TFB_OPP_PREGATE_ORDER", "1", "candidate clamp cuts the QUALITY slice, not arrival order", True),
    ("TFB_OPP_SELL_CLASS_GATE", "1", "sell-class names excluded from candidates", True),
    ("TFB_TRIM_BY_RULE_GATE", "1", "risk-budget trims uncappable by low confidence", True),
    ("TFB_PA_PRECEDENCE_GATE", "1", "allocator may narrow the engine verdict, never upgrade", True),
    ("TFB_OPP_MAX_CANDIDATES", "0", "0 = full pool; >0 clamps the scan (pregate-ordered)", False),
    ("TFB_OPP_INVESTABILITY_GATE", "0", "engine WATCHLIST/BLOCKED excluded from candidates", False),
    ("TFB_OPP_RANK_BY_ENGINE_ROI", "0", "INVEST pool ordered by engine 12M, not opportunity score", False),
    ("TFB_OPP_AUDIT_ROWS_MAX", "0", "0 = unlimited written audit rows", False),
    ("TFB_ADV_ENGINE_CALL_TIMEOUT", "0", "engine call fails SOFT before Render's ~100s edge kill (502 guard)", True),
    ("TFB_ADV_ENGINE_CALL_TIMEOUT_S", "75", "edge-guard budget in seconds", False),
    ("TFB_PF_IDENTITY_GATE", "0", "holding identity mismatch blocks its action", False),
    ("TFB_PF_CONFIRM_PERSIST", "1", "ADD needs 2 consecutive days before funding", True),
    ("TFB_PF_VF_CONFLICT_GUARD", "0", "valuation EXIT/TRIM withheld on provider conflict", False),
    ("TFB_RULE1B_CAPPED_EXIT_GATE", "1", "capped EXIT still surfaces as a rule exit", True),
    ("TFB_PA_SUKUK_ASSET_CLASS", "0", "sukuk scored on its own asset-class rules", False),
    ("TFB_OPP_BUILD_OFFLOOP", "0", "opportunity build runs off the event loop under a budget (502 cure)", True),
    ("TFB_OPP_BUILD_TIMEOUT_S", "70", "off-loop build budget in seconds", False),
]

_ARMED = {"1", "true", "yes", "on"}
# v1.0.16: tokens that SOME production modules accept as true and others do not
# (see the version note). A flag carrying one of these is genuinely ambiguous:
# track_performance reads "enabled" as ON while advanced_analysis reads it as OFF.
# We do NOT silently widen _ARMED — that would print CLEAN over a half-armed
# protection. We name the disagreement instead.
_ARMED_AMBIGUOUS = {"y", "t", "enabled", "enable"}


def _bool_ambiguity(value: Any) -> str:
    """'' when the token is unambiguous, else a message naming the split."""
    tok = str(value or "").strip().lower()
    if tok in _ARMED_AMBIGUOUS:
        return ("%r is read as ON by track_performance/data_engine_v2 and as OFF "
                "by advanced_analysis — use 1 or 0" % tok)
    return ""

# v1.0.2: not every env var is a toggle. A multiplier or a mode string is a
# PARAMETER — reporting TFB_OPP_STOP_VOL_MULT=1.5 as "off" because 1.5 is not
# in {1,true,yes,on} is a false alarm on a correctly configured system, which
# is worse than no check at all. Parameters report SET vs DEFAULT instead.
_VALUE_FLAGS = {"TFB_OPP_STOP_VOL_MULT", "TFB_BACKTEST_MIN_DSR",
                "TRACK_HORIZONS", "TFB_SYNC_NAME_DEDUP_MODE",
                "TFB_TICKET_MAX_QUOTE_AGE_MIN", "TFB_TICKET_FALLBACK_MAX_AGE_H",
                "TFB_YC_SYMBOL_SKIP", "TFB_SHARIAH_SHEET_ID",
                "TFB_SHADOW_MIN_FRESH_PCT",
                "TFB_SHARIAH_FAIL_LIST", "TFB_EXIT_BY_RULE_EXTRA",
                "TFB_KSA_FOREIGN_RESTRICTED",
                "TFB_OPP_MAX_CANDIDATES", "TFB_OPP_AUDIT_ROWS_MAX",
                "TFB_ADV_ENGINE_CALL_TIMEOUT_S", "TFB_OPP_BUILD_TIMEOUT_S"}

# v1.0.3: SCOPE. These live in GitHub workflow env blocks, never in Render, so
# this script — which reads the LOCAL process environment — structurally
# cannot see them. Reporting them as "using default" implied they were
# unconfigured when they were correctly committed to daily_sync.yml. A checker
# that cannot observe something must say so, not report absence as a finding.
# v1.0.13: ONLY flags consumed by scripts running inside GitHub Actions. Every
# flag the FastAPI process reads must stay Render-scoped so this shell reports
# the live truth. (The rule lists are read by opportunity_builder INSIDE the
# backend, so they belong here no longer.)
_WORKFLOW_SCOPED = {"TRACK_HORIZONS", "TFB_SYNC_NAME_DEDUP_MODE",
                    "TFB_SHADOW_PRICE_HONESTY", "TFB_SHADOW_MIN_FRESH_PCT"}

# v1.0.13: numeric budgets whose SAFE band matters — "set" is not "correct".
_RANGE_FLAGS = {
    "TFB_OPP_MAX_CANDIDATES": (300.0, 2000.0),
    "TFB_OPP_AUDIT_ROWS_MAX": (0.0, 1000.0),
    "TFB_OPP_BUILD_TIMEOUT_S": (60.0, 80.0),
    "TFB_ADV_ENGINE_CALL_TIMEOUT_S": (60.0, 85.0),
}

# v1.0.16: values that are ALWAYS legal for a range flag regardless of the band,
# because the flag documents them as a sentinel rather than as a quantity.
# TFB_OPP_MAX_CANDIDATES=0 means "no clamp / full pool" (see FLAGS) and is also
# the code default, so it can be set explicitly and must never read as drift.
_RANGE_SENTINELS = {
    "TFB_OPP_MAX_CANDIDATES": {0.0},
}


def _range_violation(name: str, live: Any) -> str:
    """v1.0.13: '' when fine, else 'value not in lo..hi'. Unset values are
    NOT violations — the code default governs and is documented per flag."""
    band = _RANGE_FLAGS.get(name)
    raw = str(live if live is not None else "").strip()
    if not band or not raw:
        return ""
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return "%s not numeric (expected %g..%g)" % (raw, band[0], band[1])
    if v in _RANGE_SENTINELS.get(name, ()):   # v1.0.16: documented sentinel
        return ""
    if v < band[0] or v > band[1]:
        return "%g not in %g..%g" % (v, band[0], band[1])
    return ""


def check_modules() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for path, attr, expected, label in MODULES:
        rec: Dict[str, Any] = {"module": path, "label": label,
                               "expected": expected or None}
        try:
            mod = importlib.import_module(path)
            live = str(getattr(mod, attr, "") or "")
            if not live:                      # tolerate alternate attribute names
                for alt in ("__version__", "MODULE_VERSION", "SCRIPT_VERSION",
                            "ENGINE_VERSION", "VERSION"):
                    live = str(getattr(mod, alt, "") or "")
                    if live:
                        break
            rec["live"] = live or None
            if not expected:
                rec["status"] = "INFO"
            elif not live:
                rec["status"] = "NO_VERSION"
            elif live == expected:
                rec["status"] = "OK"
            else:
                rec["status"] = ("AHEAD" if _newer(live, expected) else "BEHIND")
        except Exception as exc:  # noqa: BLE001
            rec["live"] = None
            rec["status"] = "MISSING"
            rec["error"] = f"{type(exc).__name__}: {exc}"
        out.append(rec)
    return out


def _newer(a: str, b: str) -> bool:
    def parts(v: str) -> List[int]:
        out = []
        for chunk in str(v).split("."):
            digits = "".join(c for c in chunk if c.isdigit())
            out.append(int(digits) if digits else 0)
        return out
    pa, pb = parts(a), parts(b)
    n = max(len(pa), len(pb))
    pa += [0] * (n - len(pa))
    pb += [0] * (n - len(pb))
    return pa > pb


def check_scripts() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    scripts_dir = os.path.join(_ROOT, "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    for name, expected, label in SCRIPTS:
        rec: Dict[str, Any] = {"module": name, "label": label,
                               "expected": expected}
        try:
            mod = importlib.import_module(name)
            live = str(getattr(mod, "SCRIPT_VERSION", "") or "")
            if not live:  # v1.0.7: calendar_sync-convention scripts use __version__
                live = str(getattr(mod, "__version__", "") or "")
            rec["live"] = live or None
            rec["status"] = ("OK" if live == expected else
                             "NO_VERSION" if not live else
                             "AHEAD" if _newer(live, expected) else "BEHIND")
        except Exception as exc:  # noqa: BLE001
            rec["live"] = None
            rec["status"] = "MISSING"
            rec["error"] = f"{type(exc).__name__}: {exc}"
        out.append(rec)
    return out


def check_flags() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for name, default, meaning, kill in FLAGS:
        raw = os.getenv(name)
        live = raw if raw is not None else default
        kind = "value" if name in _VALUE_FLAGS else "bool"
        if name == "TRACK_HORIZONS":
            armed = bool(raw) and ("1W" in raw or "2W" in raw)
        elif name == "TFB_SYNC_NAME_DEDUP_MODE":
            armed = str(live).strip().lower() == "quarantine"
        elif kind == "value":
            # a parameter is "armed" when explicitly configured, whatever value
            armed = raw is not None and str(raw).strip() != ""
        else:
            armed = str(live).strip().lower() in _ARMED
        out.append({"flag": name, "value": live, "set": raw is not None,
                    "default": default, "armed": armed, "meaning": meaning,
                    "kill_switch": kill, "kind": kind,
                    "scope": ("workflow" if name in _WORKFLOW_SCOPED
                              else "render"),
                    # v1.0.16: "default" appeared twice here; the second literal
                    # silently replaced the first. Same value, so no behaviour
                    # change, but a duplicate key in an audit record is exactly
                    # the kind of thing that hides a real one later.
                    "ambiguous": (_bool_ambiguity(live)
                                  if kind not in ("value",) else ""),
                    "range_bad": _range_violation(name, raw)})
    return out


def run_selftests(timeout: int = 120) -> List[Dict[str, Any]]:
    targets = [("core/regime.py", "regime"), ("core/risk_limits.py", "risk limits"),
               ("core/validation.py", "validation"), ("core/regret.py", "regret"),
               ("core/compliance_gate.py", "compliance gate"),
               ("core/shariah_authority.py", "shariah authority")]
    out: List[Dict[str, Any]] = []
    for rel, label in targets:
        path = os.path.join(_ROOT, rel)
        if not os.path.exists(path):
            out.append({"target": rel, "label": label, "status": "MISSING"})
            continue
        try:
            p = subprocess.run([sys.executable, path], capture_output=True,
                               text=True, timeout=timeout, cwd=_ROOT)
            tail = [ln for ln in (p.stdout or "").strip().splitlines()
                    if "SELFTEST" in ln]
            # v1.0.16: returncode 0 alone proved only that the process exited,
            # not that it tested anything. The SELFTEST marker is now required.
            if p.returncode != 0:
                st = "FAIL"
            elif not tail:
                st = "NO_MARKER"
            else:
                st = "PASS"
            out.append({"target": rel, "label": label, "status": st,
                        "summary": (tail[-1] if tail else
                                    "exited 0 but printed no SELFTEST line")})
        except Exception as exc:  # noqa: BLE001
            out.append({"target": rel, "label": label, "status": "ERROR",
                        "summary": f"{type(exc).__name__}: {exc}"})
    return out


def _exit_code(verdict: str, strict: bool) -> int:
    """Map a verdict to a process exit status.

    v1.0.15. DEFAULT BEHAVIOUR IS UNCHANGED FROM v1.0.14: FAIL -> 1, everything
    else -> 0. That is deliberate; existing callers must not change meaning
    underneath them.

    With --strict, DRIFT gets its own status (2) so a deployment pipeline can
    refuse to continue over a configuration this verifier has already judged
    unsafe. Absence of --strict is the kill-switch: drop the flag and v1.0.14
    semantics return exactly.

        CLEAN -> 0
        DRIFT -> 2 (strict) / 0 (default)
        FAIL  -> 1
    """
    if verdict == "FAIL":
        return 1
    if strict and verdict == "DRIFT":
        return 2
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftests", action="store_true",
                    help="also run each module's selftest")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--strict", action="store_true",
                    help="exit 2 on DRIFT (default: DRIFT exits 0, as v1.0.14)")
    args = ap.parse_args(argv)

    mods = check_modules()
    scripts = check_scripts()
    flags = check_flags()
    tests = run_selftests() if args.selftests else []

    drift = [m for m in mods + scripts
             if m["status"] in ("BEHIND", "AHEAD", "NO_VERSION")]
    missing = [m for m in mods + scripts if m["status"] == "MISSING"]
    # v1.0.16: NO_MARKER is not a failure — the target ran and exited clean, we
    # simply cannot prove it tested anything. That is drift, not breakage.
    failed = [t for t in tests if t["status"] in ("FAIL", "ERROR", "MISSING")]
    nomarker = [t for t in tests if t["status"] == "NO_MARKER"]
    # v1.0.11: a kill-switch that ships ARMED but reads disarmed on the live
    # box is a silently unprotected system — it must never print CLEAN.
    disarmed = [f for f in flags
                if f.get("kill_switch") and not f.get("armed")
                and f.get("scope") != "workflow"]
    out_of_range = [f for f in flags if f.get("range_bad")]
    # v1.0.16: a token that half the codebase reads as ON and half as OFF is not
    # a clean configuration, whichever way this verifier happens to guess.
    ambiguous = [f for f in flags
                 if f.get("ambiguous") and f.get("scope") != "workflow"]
    verdict = ("FAIL" if (missing or failed)
               else "DRIFT" if (drift or disarmed or out_of_range
                                or ambiguous or nomarker)
               else "CLEAN")

    if args.json:
        print(json.dumps({"version": SCRIPT_VERSION, "verdict": verdict,
                          "modules": mods, "scripts": scripts,
                          "flags": flags, "selftests": tests}, indent=2))
        return _exit_code(verdict, args.strict)

    print(f"TFB DEPLOYMENT VERIFIER v{SCRIPT_VERSION}"
          + ("   [--strict: DRIFT will exit 2]" if args.strict else ""))
    print("=" * 64)
    print("\nMODULES")
    for m in mods + scripts:
        mark = {"OK": "  ok  ", "INFO": " info ", "BEHIND": "BEHIND",
                "AHEAD": "AHEAD ", "MISSING": "MISS! ",
                "NO_VERSION": "NOVER "}.get(m["status"], "  ?   ")
        exp = f"(expected {m['expected']})" if m.get("expected") and m["status"] != "OK" else ""
        print(f"  [{mark}] {m['label']:<28} {str(m.get('live') or '-'):<10} {exp}")
        if m.get("error"):
            print(f"           -> {m['error']}")

    print("\nFLAGS")
    for f in flags:
        if f.get("scope") == "workflow":
            state = "GH-ENV"
        elif f.get("kind") == "value":
            state = " SET  " if f["armed"] else "DEFAULT"
        else:
            state = "ARMED " if f["armed"] else "  off "
        if f.get("scope") == "workflow":
            src = "  (set in daily_sync.yml — NOT visible from this shell)"
        else:
            src = "" if f["set"] else "  (not set — using default)"
        if f.get("ambiguous"):
            note = "  [AMBIGUOUS TOKEN — see footer]"
        elif f.get("range_bad"):
            note = "  [OUT OF SAFE RANGE %s]" % (f["range_bad"],)
        elif f["kill_switch"] and not f["armed"]:
            note = ("  [protection expected ARMED in production]"
                    if str(f.get("default")) == "0"
                    else "  [kill-switch: off DISABLES protection]")
        else:
            note = ""
        print(f"  [{state}] {f['flag']:<32} {str(f['value'])[:14]:<15} {f['meaning']}{src}{note}")

    if tests:
        print("\nSELFTESTS")
        for t in tests:
            print(f"  [{t['status']:<5}] {t['label']:<24} {t.get('summary','')}")

    armed_n = sum(1 for f in flags
                  if f["armed"] and f.get("scope") != "workflow")
    wf_n = sum(1 for f in flags if f.get("scope") == "workflow")
    print("\n" + "=" * 64)
    if verdict == "CLEAN":
        # v1.0.16 (audit finding 4): CLEAN is a statement about the Render
        # process this shell can see. Workflow-scoped values live in
        # daily_sync.yml and are NOT part of this verdict.
        print("SCOPE: Render process configuration only — "
              "workflow-scoped flags must be read in daily_sync.yml")
    print(f"VERDICT: {verdict}   modules {len(mods)+len(scripts)} "
          f"({len(drift)} drift, {len(missing)} missing) | "
          f"flags {armed_n}/{len(flags) - wf_n} armed "
          f"(+{wf_n} workflow-scoped, verify in daily_sync.yml)"
          + (f" | selftests {sum(1 for t in tests if t['status']=='PASS')}"
             f"/{len(tests)}" if tests else ""))
    if drift:
        print("  drift: " + ", ".join(f"{m['label']}={m.get('live')}" for m in drift))
    if out_of_range:
        print("  OUT OF RANGE: " + ", ".join(
            "%s=%s" % (f["flag"], f["value"]) for f in out_of_range))
    if disarmed:
        print("  UNARMED PROTECTIONS: "
              + ", ".join(f["flag"] for f in disarmed))
    if ambiguous:
        print("  AMBIGUOUS BOOLEANS (production modules disagree on these tokens):")
        for f in ambiguous:
            print("    %s=%s -> %s" % (f["flag"], f["value"], f["ambiguous"]))
    if nomarker:
        print("  SELFTEST UNPROVEN: "
              + ", ".join(t["label"] for t in nomarker)
              + "  (exit 0 but no SELFTEST line printed)")
    if missing:
        print("  MISSING: " + ", ".join(m["label"] for m in missing))
    unarmed = [f["flag"] for f in flags
               if not f["armed"] and not f["kill_switch"]
               and f.get("kind") != "value" and f.get("scope") != "workflow"]
    undef = [f["flag"] for f in flags
             if not f["armed"] and f.get("kind") == "value"
             and f.get("scope") != "workflow"]
    if unarmed:
        print("  not armed: " + ", ".join(unarmed))
    if undef:
        print("  using default: " + ", ".join(undef))
    return _exit_code(verdict, args.strict)


if __name__ == "__main__":
    sys.exit(main())
