#!/usr/bin/env python3
# tests/test_pf_research_view_p06.py
"""
portfolio_actions v1.16.0 [P0-6 RESEARCH VIEW] harness - REAL module, ZERO
network, synthetic holdings and invented account captures only.

TFB_PF_RESEARCH_VIEW = off (default) | observe | enforce. Off must be the
v1.15.1 output except the version string; observe adds text only; enforce
shows rule verdicts, market value, cost, P&L and levels on rows that stay
BLOCK with zero money. The D4 daily custody tier is NOT part of this build.

  R0   off is byte-identical to the base on 6 fixtures x 7 env spellings
       (needs TFB_TEST_PA_BASE; skips cleanly when unset)
  R0b  off never enters the research view (no base needed)
  R0c  the env reader: only exact observe / enforce arm it
  R0d  a direct call with the gate off returns the off output
  R1   observe: numbers, KPIs, alerts identical to off; tag on every row
  R2   enforce, no packet: BLOCK + legacy prefix + tag; MV / P&L / levels
       equal the certified build for the same rows
  R3   rule verdicts: STRONG_SELL -> EXIT, valuation TRIM -> TRIM
  R3b  TFB_PF_DD_EXIT=enforce -> drawdown EXIT; EXIT-BY-RULE extra -> EXIT
  R4   ADD stays blocked exactly; the ADD verdict is never named or sized
  R5   confirmation / session / loser-veto / f1 seams never entered; the
       confirmation store is unchanged
  R6   contradicted rows are the legacy blank row; holdings KPIs None
  R7   a 3 h old packet -> invalid_evidence -> research view applies
  R8   matched rows under cash or quote withholding -> [custody-matched]
  R9   privacy: no account, source, reservation or packet text in output
  R10  portfolio value / cash None, sector_summary [], alerts unchanged
  R11  a sukuk row keeps a blank ladder (P-153) under enforce
  R15  a fresh valid packet: enforce output equals off output
  R16  a cost-basis BLOCK row: cost / P&L None, excluded from KPIs
  R17  a row without a price: holdings_value_sar None
  R18  real 5023.SR + real classifier: levels on 3 of 4 rows
  R19  an exception inside the view falls back to the off output
  R20  review fix: an unlinked custody position (unrepresented_positions)
       withholds the four holdings KPIs; rows keep their figures
  R21  review fix: a row whose FX the funded path rejects shows no SAR
       figure and withholds the KPIs (fresh packet and no packet)
  R21b the FX rule helper on SAR, USD, row FX columns and bad inputs
  H1   review fix: every case stamps its rows when it runs, so a late
       start in a long CI process cannot age the quotes past 15 minutes
  V1   observe output passes the deployed readback verifier unchanged
  V2   enforce output fails the default verifier, passes the lockstep one
  G1   GOLDEN NEGATIVE: every gate-on predicate is False on the base
       (TFB_TEST_PA_BASE=<git show dda1211:core/analysis/portfolio_actions.py>)
  G2   AST: 0 functions removed vs base, the 3 new ones added
  G3   every line added vs base is ASCII

Run:  python tests/test_pf_research_view_p06.py            (prints PASS k/k)
      python -m pytest -q tests/test_pf_research_view_p06.py
"""
from __future__ import annotations

import ast
import contextlib
import copy
import datetime as dt
import difflib
import importlib.util
import inspect
import json
import os
import sys
import traceback
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    import pytest
except ImportError:  # standalone run without pytest
    pytest = None

from core.analysis import portfolio_actions as pa  # noqa: E402
from scripts import verify_core_repair_readback as vr  # noqa: E402

DELIVERED = ROOT / "core" / "analysis" / "portfolio_actions.py"
BASE_PATH = (os.environ.get("TFB_TEST_PA_BASE") or "").strip()
NEW_FUNCTIONS = {"_env_research_view_mode", "_research_verdict", "_research_fx_rejected",
                 "_research_view_portfolio"}

# Policy the harness pins for every build; everything in CLEARED is unset
# unless a case names it. Restored after each build (no env leaks).
POLICY = {"TFB_PF_ENABLED": "1", "TFB_PF_CONFIRM_PERSIST": "0",
          "TFB_PF_ADD_CONFIRM_DAYS": "0", "TFB_PF_TRUST_GATE": "0",
          "TFB_EXIT_BY_RULE_GATE": "0"}
CLEARED = ("TFB_PF_RESEARCH_VIEW", "TFB_PF_CUSTODY_DAILY_MAX_AGE_S", "TFB_PF_DD_EXIT",
           "TFB_EXIT_BY_RULE_EXTRA", "TFB_SHARIAH_FAIL_LIST", "TFB_PF_CONFIRM_SESSION",
           "TFB_PF_ADD_LOSER_VETO", "TFB_FORECAST_BASIS", "TFB_PA_SUKUK_LADDER_LEGACY",
           "TFB_PA_PROTECT_SUKUK", "TFB_PF_MIN_TICKET_SAR", "TFB_PA_REDUCE_ACTION",
           "TFB_PF_MAX_HOLDINGS", "TFB_PF_ENGINE_ROI_DISPLAY", "TFB_PA_HOLDEDGE_ANNOTATE")
CTL = {"cash_available_sar": 10_000, "target_cash_pct": 10, "max_position_pct": 20,
       "max_sector_pct": 30, "trust_gate_enabled": False}
FX = {"SAR": 1}
ACCOUNT = "acct-private-sentinel"
SOURCE = "synthetic://private-source-sentinel"
INSTRUMENT = "instr-private-sentinel-%d"
TAG_U = "[custody-unverified]"
TAG_M = "[custody-matched]"
LEVEL_KEYS = ("market_value_sar", "cost_sar", "pnl_sar", "pnl_pct", "stop_sar", "tp1_sar", "tp2_sar")
KPI_KEYS = ("holdings_value_sar", "cost_basis_sar", "pnl_sar", "pnl_pct")
FX_NOTE = "SAR figures withheld: holding FX differs from the supplied rate"
OFF_SPELLINGS = ({}, {"TFB_PF_RESEARCH_VIEW": "OFF"}, {"TFB_PF_RESEARCH_VIEW": "bogus"},
                 {"TFB_PF_RESEARCH_VIEW": ""}, {"TFB_PF_RESEARCH_VIEW": "enforced"},
                 {"TFB_PF_RESEARCH_VIEW": "1"}, {"TFB_PF_CUSTODY_DAILY_MAX_AGE_S": "86400"})


# ---------------------------------------------------------------------------
# Fixtures (synthetic; nothing here is a real account or a real capture)
# ---------------------------------------------------------------------------

def _stamp(age_s=0):
    when = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=age_s)
    return when.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def holding(symbol, **updates):
    stamp = _stamp()
    row = {"Symbol": symbol, "Currency": "SAR", "Position Qty": 10, "Buy Price": 90,
           "Current Price": 100, "Intrinsic Value": 130, "Sector": "Energy", "Market": "Tadawul",
           "Forecast Reliability Score": 90, "Data Quality Score": 90, "Recommendation": "BUY",
           "Investability Status": "INVESTABLE", "Risk Bucket": "Moderate",
           "Data Provider": "EODHD", "Last Updated": stamp,
           "Warnings": ("acquisition_status:success; acquisition_provider:EODHD; "
                        "acquisition_acquired_at:" + stamp + "; acquisition_quote_asof:" + stamp)}
    row.update(updates)
    return row


def main_rows():
    """ADD-qualifying, STRONG_SELL, valuation TRIM, drawdown and sukuk rows."""
    return [holding("ADDME.SR"),
            holding("SELLME.SR", **{"Recommendation": "STRONG_SELL", "Intrinsic Value": 104}),
            holding("TRIMME.SR", **{"Recommendation": "HOLD", "Intrinsic Value": 92}),
            holding("DDOWN.SR", **{"Recommendation": "HOLD", "Buy Price": 120, "Intrinsic Value": 105}),
            holding("SYNTHSUKUK.SR", **{"Name": "Synthetic Sukuk", "Recommendation": "HOLD",
                                        "Current Price": 101, "Buy Price": 100,
                                        "Intrinsic Value": 104})]


# The shape of the main fixture (symbols, quantities, row count). Its quote
# stamps date from import, so a build never reads it: run() and every case
# that builds rows call main_rows() when they run (review fix H1).
ROWS = main_rows()


def packet(rows, age_s=0, cash=10_000, qty=None):
    stamp = _stamp(age_s)
    qty = qty or {}
    return {"schema_version": 1, "source_ref": SOURCE, "captured_at": stamp,
            "accounts": [{"account_id": ACCOUNT, "positions_complete": True, "positions_asof": stamp,
                          "positions": [{"instrument_id": INSTRUMENT % i, "symbol": r["Symbol"],
                                         "currency": "SAR", "quantity": qty.get(i, r["Position Qty"])}
                                        for i, r in enumerate(rows)],
                          "cash_complete": True, "cash_asof": stamp,
                          "cash": [{"currency": "SAR", "settled_cash": cash, "reserved_cash": 0,
                                    "reservations_complete": True}]}],
            "holding_links": [{"row_index": i, "account_id": ACCOUNT, "instrument_id": INSTRUMENT % i,
                               "position_symbol": r["Symbol"], "symbol": r["Symbol"], "currency": "SAR"}
                              for i, r in enumerate(rows)],
            "funding_accounts": [ACCOUNT]}


def fixtures(rows=None):
    rows = ROWS if rows is None else rows
    return {"none": None, "empty": {}, "stale3h": packet(rows, 3 * 3600),
            "qtymismatch": packet(rows, qty={1: 4}), "cashmismatch": packet(rows, cash=5),
            "fresh": packet(rows)}


@contextlib.contextmanager
def policy_env(env=None):
    env = env or {}
    keys = set(POLICY) | set(CLEARED) | set(env)
    saved = {key: os.environ.get(key) for key in keys}
    try:
        for key in CLEARED:
            os.environ.pop(key, None)
        os.environ.update(POLICY)
        for key, value in env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield
    finally:
        for key, value in saved.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def run(mod, evidence=None, rows=None, env=None, controls=None):
    rows = main_rows() if rows is None else rows
    meta = {"rows_supplied_by": "harness"}
    if evidence is not None:
        meta["reconciliation_evidence"] = copy.deepcopy(evidence)
    ctl = dict(CTL)
    ctl.update(controls or {})
    with policy_env(env):
        out = mod.build_portfolio_actions(copy.deepcopy(rows), ctl, dict(FX), meta)
    out["meta"].pop("generated_utc", None)
    return out


def mode_env(mode, **extra):
    env = {"TFB_PF_RESEARCH_VIEW": mode}
    env.update(extra)
    return env


def norm(payload):
    payload = copy.deepcopy(payload)
    payload["version"] = "X"
    payload["meta"]["versions"]["portfolio_actions"] = "X"
    return json.dumps(payload, sort_keys=True)


def by_symbol(payload):
    return {row["symbol"]: row for row in payload["actions"]}


def strip_text(row):
    """A row without the two text fields and the research witness."""
    row = copy.deepcopy(row)
    row.pop("action_reason", None)
    row.pop("advisor_note", None)
    row.get("detail", {}).pop("research_view", None)
    return row


def research_of(row):
    return (row.get("detail") or {}).get("research_view") or {}


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load module from " + str(path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


_BASE = {}


def base_module():
    if not BASE_PATH:
        raise unittest.SkipTest("golden negative skipped: set TFB_TEST_PA_BASE=<copy made with "
                                "git show dda1211:core/analysis/portfolio_actions.py>")
    if not os.path.exists(BASE_PATH):
        raise unittest.SkipTest("TFB_TEST_PA_BASE path does not exist: " + BASE_PATH)
    if "mod" not in _BASE:
        _BASE["mod"] = load_module(BASE_PATH, "pa_base_p06_golden")
    return _BASE["mod"]


# ---------------------------------------------------------------------------
# Gate-on predicates: True on the delivered module, False on the base (G1)
# ---------------------------------------------------------------------------

def _safe(fn):
    def wrapper(mod):
        try:
            return bool(fn(mod))
        except Exception:  # noqa: BLE001 - a raising predicate is a False one
            return False
    wrapper.__name__ = fn.__name__
    return wrapper


@_safe
def pred_r1_observe_text_only(mod):
    off_rows = run(mod, None)
    obs = run(mod, None, env=mode_env("observe"))
    if obs["kpis"] != off_rows["kpis"] or obs["alerts"] != off_rows["alerts"]:
        return False
    if obs["sector_summary"] != off_rows["sector_summary"]:
        return False
    meta_obs = dict(obs["meta"])
    meta_obs.pop("research_view", None)
    if meta_obs != off_rows["meta"] or obs["meta"].get("research_view", {}).get("mode") != "observe":
        return False
    for new, old in zip(obs["actions"], off_rows["actions"]):
        if strip_text(new) != strip_text(old):
            return False
        if not new["action_reason"].startswith(old["action_reason"] + " | [research-observe] " + TAG_U + " "):
            return False
        if "[research-observe]" not in new["advisor_note"] or research_of(new).get("mode") != "observe":
            return False
    return len(obs["actions"]) == len(ROWS)


@_safe
def pred_r2_enforce_levels(mod):
    enf = by_symbol(run(mod, None, env=mode_env("enforce")))
    cert = by_symbol(run(mod, packet(ROWS)))
    if set(enf) != set(cert) or len(enf) != len(ROWS):
        return False
    for symbol, row in enf.items():
        if row["action"] != "BLOCK":
            return False
        if not row["action_reason"].startswith("RECONCILIATION_REQUIRED: missing_evidence | " + TAG_U + " "):
            return False
        if not row["action_reason"].endswith("; not executable"):
            return False
        if any(row[key] != cert[symbol][key] for key in LEVEL_KEYS):
            return False
        if row["market_value_sar"] is None or row["pnl_sar"] is None:
            return False
    return sum(1 for row in enf.values() if row["stop_sar"] is not None) == len(ROWS) - 1


@_safe
def pred_r3_rule_verdicts(mod):
    enf = by_symbol(run(mod, None, env=mode_env("enforce")))
    verdicts = {symbol: research_of(row).get("verdict") for symbol, row in enf.items()}
    return (verdicts == {"ADDME.SR": "NEW_MONEY_WITHHELD", "SELLME.SR": "EXIT", "TRIMME.SR": "TRIM",
                         "DDOWN.SR": "HOLD", "SYNTHSUKUK.SR": "HOLD"}
            and "research verdict EXIT: Engine recommendation 'STRONG_SELL' is sell-tier"
            in enf["SELLME.SR"]["action_reason"]
            and "research verdict TRIM: Valuation ROI -8.0% <= trim threshold" in enf["TRIMME.SR"]["action_reason"])


@_safe
def pred_r3b_guard_and_rule_exit(mod):
    dd = by_symbol(run(mod, None, env=mode_env("enforce", TFB_PF_DD_EXIT="enforce")))
    rule = by_symbol(run(mod, None, env=mode_env("enforce", TFB_EXIT_BY_RULE_GATE="1",
                                                    TFB_EXIT_BY_RULE_EXTRA="ADDME.SR")))
    return (research_of(dd["DDOWN.SR"]).get("verdict") == "EXIT"
            and "research verdict EXIT: Drawdown/time guard" in dd["DDOWN.SR"]["action_reason"]
            and research_of(rule["ADDME.SR"]).get("verdict") == "EXIT"
            and "research verdict EXIT: EXIT-BY-RULE" in rule["ADDME.SR"]["action_reason"]
            and all(row["action"] == "BLOCK" and row["proceeds_sar"] == 0
                    for row in list(dd.values()) + list(rule.values())))


@_safe
def pred_r4_add_withheld(mod):
    row = by_symbol(run(mod, None, env=mode_env("enforce")))["ADDME.SR"]
    tail = row["action_reason"].split(" | ", 1)[1]
    return (research_of(row).get("verdict") == "NEW_MONEY_WITHHELD"
            and tail == TAG_U + " " + pa.RESEARCH_ADD_WITHHELD + "; not executable"
            and "ADD" not in tail)


@_safe
def pred_r7_stale_packet(mod):
    out = run(mod, packet(ROWS, 3 * 3600), env=mode_env("enforce"))
    return all(row["detail"]["reconciliation_status"] == "invalid_evidence"
               and row["action_reason"].startswith("RECONCILIATION_REQUIRED: invalid_evidence | " + TAG_U)
               and row["action"] == "BLOCK" and row["market_value_sar"] is not None
               for row in out["actions"]) and len(out["actions"]) == len(ROWS)


@_safe
def pred_r8_matched(mod):
    cash = run(mod, packet(ROWS, cash=5), env=mode_env("enforce"))
    stale_quote = main_rows()
    stale_quote[0]["Warnings"] += "; kept_last_good"
    quote = run(mod, packet(stale_quote), rows=stale_quote, env=mode_env("enforce"))
    return (all(TAG_M in row["action_reason"] and "cash_request_mismatch" in row["action_reason"]
                and research_of(row).get("tier") == "matched"
                and row["detail"]["position_evidence_matched"] is True for row in cash["actions"])
            and all(TAG_M in row["action_reason"] and "holding_quote_unverified" in row["action_reason"]
                    for row in quote["actions"])
            and len(cash["actions"]) == len(quote["actions"]) == len(ROWS))


@_safe
def pred_r16_cost_basis_scrub(mod):
    rows = [holding("ADDME.SR"), holding("BADBASIS.SR", **{"Buy Price": 10_000})]
    out = run(mod, None, rows=rows, env=mode_env("enforce"))
    bad = by_symbol(out)["BADBASIS.SR"]
    return (research_of(bad).get("verdict") == "BLOCK" and "review cost basis" in bad["action_reason"]
            and bad["market_value_sar"] == 1000 and bad["cost_sar"] is None
            and bad["pnl_sar"] is None and bad["pnl_pct"] is None
            and out["kpis"]["holdings_value_sar"] == 2000 and out["kpis"]["cost_basis_sar"] == 900
            and out["kpis"]["pnl_sar"] == 100 and out["kpis"]["pnl_pct"] == 11.1)


@_safe
def pred_r17_missing_price(mod):
    rows = [holding("ADDME.SR"), holding("NOPRICE.SR", **{"Current Price": ""})]
    out = run(mod, None, rows=rows, env=mode_env("enforce"))
    rv = out["meta"]["research_view"]
    return (out["kpis"]["holdings_value_sar"] is None and "market_value_incomplete" in rv["kpi_withheld"]
            and out["kpis"]["cost_basis_sar"] == 900 and out["kpis"]["pnl_sar"] == 100
            and by_symbol(out)["ADDME.SR"]["market_value_sar"] == 1000)


def rows_5023():
    return [holding("ADDME.SR"),
            holding("SELLME.SR", **{"Recommendation": "STRONG_SELL", "Intrinsic Value": 104}),
            holding("DDOWN.SR", **{"Recommendation": "HOLD", "Buy Price": 120, "Intrinsic Value": 105}),
            holding("5023.SR", **{"Recommendation": "HOLD", "Current Price": 101, "Buy Price": 100,
                                  "Intrinsic Value": 104})]


@_safe
def pred_r18_real_sukuk(mod):
    out = by_symbol(run(mod, None, rows=rows_5023(), env=mode_env("enforce")))
    full = [s for s, row in out.items() if all(row[k] is not None for k in ("stop_sar", "tp1_sar", "tp2_sar"))]
    bond = out["5023.SR"]
    return (sorted(full) == ["ADDME.SR", "DDOWN.SR", "SELLME.SR"]
            and bond["stop_sar"] is bond["tp1_sar"] is bond["tp2_sar"] is None
            and bond["detail"].get("ladder_display") == "sukuk_na"
            and bond["market_value_sar"] == 1010 and TAG_U in bond["action_reason"])


def unlinked_packet(rows):
    """A fresh packet whose account also holds a position no row links to."""
    evidence = packet(rows)
    evidence["accounts"][0]["positions"].append({"instrument_id": INSTRUMENT % 99, "symbol": "UNLINKED.SR",
                                                 "currency": "SAR", "quantity": 1000})
    return evidence


def fx_rows():
    return [holding("ADDME.SR"), holding("FXBAD.SR", **{"FX To SAR": 3.75})]


@_safe
def pred_r20_unrepresented_withholds_totals(mod):
    rows = main_rows()
    out = run(mod, unlinked_packet(rows), rows=rows, env=mode_env("enforce"))
    rv = out["meta"]["research_view"]
    return (out["meta"]["input_certification"]["reason_counts"] == {"unrepresented_positions": 1}
            and all(out["kpis"][key] is None for key in KPI_KEYS)
            and "unrepresented_positions" in rv["kpi_withheld"] and rv["kpi_basis"] == "withheld"
            and rv["tiers"] == {"matched": len(ROWS)}
            and all(TAG_M in row["action_reason"] and row["action"] == "BLOCK" for row in out["actions"])
            and by_symbol(out)["ADDME.SR"]["market_value_sar"] == 1000)


@_safe
def pred_r21_fx_rejected_row_blank(mod):
    rows = fx_rows()
    funded = run(mod, packet(rows), rows=rows, env=mode_env("enforce"))
    bare = run(mod, None, rows=rows, env=mode_env("enforce"))
    if funded["meta"]["input_certification"]["reason_counts"] != {"holding_fx_mismatch": 1}:
        return False
    for out, prefix in ((funded, "RECONCILIATION_REQUIRED: holding_fx_mismatch | " + TAG_M + " "),
                        (bare, "RECONCILIATION_REQUIRED: missing_evidence | " + TAG_U + " ")):
        rv = out["meta"]["research_view"]
        good, bad = by_symbol(out)["ADDME.SR"], by_symbol(out)["FXBAD.SR"]
        if not (bad["action"] == "BLOCK" and bad["action_reason"].startswith(prefix)
                and bad["action_reason"].endswith("; " + FX_NOTE + "; not executable")
                and all(bad[key] is None for key in LEVEL_KEYS)
                and research_of(bad).get("fx_rejected") is True
                and good["market_value_sar"] == 1000 and research_of(good).get("fx_rejected") is False
                and FX_NOTE not in good["action_reason"]
                and all(out["kpis"][key] is None for key in KPI_KEYS)
                and "holding_fx_mismatch" in rv["kpi_withheld"] and rv["fx_rejected_rows"] == 1):
            return False
    return True


class _FakeBackend:
    """Canned board responses; the portfolio probe runs the REAL builder."""

    def __init__(self, mod, mode):
        self.mod, self.mode = mod, mode
        self.version = mod.PORTFOLIO_ACTIONS_VERSION
        snapshot = {"rows": [], "snapshot_id": "synthetic-signature", "contract_version": 1,
                    "builder_version": "builder-test"}
        zero = {"selected_count": 0, "expected_gain_12m_sar": 0, "deployable_sar": 0,
                "capital_unallocated_sar": 0}
        self.research = {"version": "builder-test", "status": "ok", "selected": [], "kpis": dict(zero),
                         "alerts": [], "meta": {"board_funding": {
                             "stage": "research", "snapshot_id": "synthetic-signature",
                             "contract_version": 1, "snapshot_available": True, "snapshot": snapshot}}}
        self.allocated = {"version": "builder-test", "status": "no_candidates", "selected": [],
                          "kpis": dict(zero), "alerts": [],
                          "meta": {"execution_ready": False, "input_certification": {"funding_eligible": False},
                                   "board_funding": {"stage": "allocate", "contract_version": 1,
                                                     "snapshot_id": "synthetic-signature",
                                                     "eligible_symbols": [], "snapshot_available": True}}}
        self.rejected = {"status": "board_funding_mismatch", "selected": [], "kpis": dict(zero), "alerts": []}
        self.portfolio_body = None

    def __call__(self, path, body=None, authenticated=False):
        if path == "/health":
            return {"ready": True, "engine_version": "engine-test",
                    "deploy": {"render_git_commit": "commit-test"}, "engine_gates": {}}
        if path == vr.PORTFOLIO_ACTIONS_PATH:
            self.portfolio_body = copy.deepcopy(body)
            controls = {"cash_available_sar": body["controls"]["Cash Available (SAR)"]}
            controls.update({k: v for k, v in body["controls"].items() if k != "Cash Available (SAR)"})
            meta = {"rows_supplied_by": "gas_sheets",
                    "reconciliation_evidence": body.get("reconciliation_evidence")}
            with policy_env(mode_env(self.mode)):
                payload = self.mod.build_portfolio_actions(body["rows"], controls, body["fx_rates"], meta)
            payload = json.loads(json.dumps(payload))
            payload["meta"]["route"] = {"portfolio_actions_version": self.version}
            return payload
        if body["criteria"]["Board Funding Stage"] == "research":
            return copy.deepcopy(self.research)
        if body["portfolio"]["cash_available_sar"] or body["fx_rates"]["USD"] != 3.75:
            return copy.deepcopy(self.rejected)
        return copy.deepcopy(self.allocated)


def readback(mod, served, declared):
    """(ok, error text) of the real verify() against the real builder."""
    backend = _FakeBackend(mod, served)
    kwargs = {} if declared == "off" else {"expected_pf_research_view": declared}
    try:
        vr.verify(backend, "commit-test", "engine-test", "builder-test",
                  expected_portfolio_actions=mod.PORTFOLIO_ACTIONS_VERSION, **kwargs)
        return True, ""
    except vr.ReadbackError as exc:
        return False, str(exc)


@_safe
def pred_v2_lockstep_accepts_enforce(mod):
    return readback(mod, "enforce", "enforce")[0]


GATE_ON_PREDICATES = (pred_r1_observe_text_only, pred_r2_enforce_levels, pred_r3_rule_verdicts,
                      pred_r3b_guard_and_rule_exit, pred_r4_add_withheld, pred_r7_stale_packet,
                      pred_r8_matched, pred_r16_cost_basis_scrub, pred_r17_missing_price,
                      pred_r18_real_sukuk, pred_r20_unrepresented_withholds_totals,
                      pred_r21_fx_rejected_row_blank, pred_v2_lockstep_accepts_enforce)


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

def case_r0_off_byte_identical_to_base():
    base = base_module()
    assert base.PORTFOLIO_ACTIONS_VERSION == "1.15.1", base.PORTFOLIO_ACTIONS_VERSION
    compared = 0
    for env in OFF_SPELLINGS:
        for name, evidence in fixtures().items():
            rows = main_rows()
            new, old = run(pa, evidence, rows=rows, env=env), run(base, evidence, rows=rows, env=env)
            assert norm(new) == norm(old), "off differs from base: %s %s" % (env, name)
            assert tuple(map(int, new["version"].split("."))) >= (1, 16, 0) and old["version"] == "1.15.1"
            compared += 1
    print("R0 off byte-identical to base on %d fixture x spelling pairs" % compared)
    assert compared == len(OFF_SPELLINGS) * 6


def case_r0b_off_never_enters_research_view():
    original = pa._research_view_portfolio

    def boom(*_args, **_kwargs):
        raise AssertionError("off must not enter the research view")

    pa._research_view_portfolio = boom
    try:
        for name, evidence in fixtures().items():
            rows = main_rows()
            reference = norm(run(pa, evidence, rows=rows))
            for env in OFF_SPELLINGS:
                out = run(pa, evidence, rows=rows, env=env)
                assert out["status"] == "ok", (name, env, out["meta"].get("reason"))
                assert norm(out) == reference, (name, env)
                assert "research_view" not in out["meta"], (name, env)
                assert not any("[custody-" in row["action_reason"] for row in out["actions"])
    finally:
        pa._research_view_portfolio = original


def case_r0d_direct_call_with_gate_off_returns_off_output():
    from core.portfolio_reconciliation import certify_portfolio_inputs
    with policy_env():
        ctl = pa.make_controls(dict(CTL))
        rows = main_rows()
        cert = certify_portfolio_inputs(copy.deepcopy(rows), None, dict(FX))
        direct = pa._research_view_portfolio(copy.deepcopy(rows), ctl, dict(FX), {}, copy.deepcopy(cert))
        legacy = pa._unverified_portfolio(copy.deepcopy(rows), ctl, dict(FX), {}, copy.deepcopy(cert))
    direct["meta"].pop("generated_utc")
    legacy["meta"].pop("generated_utc")
    assert direct == legacy and "research_view" not in direct["meta"]


def case_r0c_env_reader():
    expected = {None: "off", "": "off", "off": "off", "OFF": "off", "bogus": "off", "1": "off",
                "true": "off", "enforced": "off", "observe,enforce": "off", "observe": "observe",
                " Observe ": "observe", "enforce": "enforce", "ENFORCE": "enforce"}
    for value, mode in expected.items():
        with policy_env({"TFB_PF_RESEARCH_VIEW": value}):
            assert pa._env_research_view_mode() == mode, (value, pa._env_research_view_mode())


def case_r1_observe_identical_numbers():
    assert pred_r1_observe_text_only(pa)
    for name in ("stale3h", "cashmismatch", "qtymismatch"):
        evidence = fixtures()[name]
        off_out, obs = run(pa, evidence), run(pa, evidence, env=mode_env("observe"))
        assert obs["kpis"] == off_out["kpis"] and obs["alerts"] == off_out["alerts"], name
        for new, old in zip(obs["actions"], off_out["actions"]):
            assert strip_text(new) == strip_text(old), (name, new["symbol"])
            if old["detail"]["reconciliation_status"] in ("quantity_mismatch", "position_closed"):
                assert new == old, name
            else:
                assert "[research-observe]" in new["action_reason"], (name, new["symbol"])


def case_r2_enforce_levels_equal_certified_values():
    assert pred_r2_enforce_levels(pa)


def case_r3_rule_verdicts_visible():
    assert pred_r3_rule_verdicts(pa)


def case_r3b_drawdown_and_rule_exit_visible():
    assert pred_r3b_guard_and_rule_exit(pa)
    plain = by_symbol(run(pa, None, env=mode_env("enforce")))
    assert research_of(plain["DDOWN.SR"]).get("verdict") == "HOLD"


def case_r4_add_blocked_exactly():
    assert pred_r4_add_withheld(pa)
    off_out = run(pa, None)
    for env in (mode_env("observe"), mode_env("enforce")):
        out = run(pa, None, env=env)
        for row in out["actions"]:
            assert row["action"] == "BLOCK"
            for key in ("suggested_delta_sar", "suggested_delta_shares", "proceeds_sar"):
                assert row[key] == 0 and not isinstance(row[key], bool), key
            for key in ("funds_from", "weight_pct", "post_trade_weight_pct"):
                assert row[key] is None, key
            assert row["detail"]["execution_ready"] is False
            assert row["detail"]["sector_weight_pct"] is None
            assert "research verdict ADD" not in row["action_reason"]
            assert research_of(row).get("verdict") != "ADD"
        for key in ("deployable_sar", "adds_funded_sar", "proceeds_pending_sar", "capital_unallocated_sar"):
            assert out["kpis"][key] == 0, key
        assert out["kpis"]["action_counts"] == {"ADD": 0, "HOLD": 0, "TRIM": 0, "EXIT": 0, "BLOCK": len(ROWS)}
        assert out["meta"]["execution_ready"] is False
        assert out["meta"]["input_certification"] == off_out["meta"]["input_certification"]


def case_r5_confirmation_never_entered():
    names = ("_apply_add_confirmation", "_apply_confirm_session_observe", "_apply_add_loser_veto",
             "_apply_f1_observe_tag", "_confirm_redis_get", "_confirm_redis_put", "_confirm_redis_del")
    originals = {name: getattr(pa, name) for name in names}
    saved_store = copy.deepcopy(pa._ADD_CONFIRM_STORE)
    pa._ADD_CONFIRM_STORE["SENTINEL.SR"] = {"date": "2026-10-12", "days": 1}
    seeded = copy.deepcopy(pa._ADD_CONFIRM_STORE)

    def boom(*_args, **_kwargs):
        raise AssertionError("research view entered a confirmation seam")

    try:
        for name in names:
            setattr(pa, name, boom)
        for mode in ("observe", "enforce"):
            for evidence in (None, packet(ROWS, 3 * 3600), packet(ROWS, cash=5)):
                env = mode_env(mode, TFB_PF_ADD_CONFIRM_DAYS="2", TFB_PF_CONFIRM_PERSIST="1",
                               TFB_PF_CONFIRM_SESSION="enforce", TFB_PF_ADD_LOSER_VETO="enforce",
                               TFB_FORECAST_BASIS="observe")
                out = run(pa, evidence, env=env)
                assert out["status"] == "ok", out["meta"].get("reason")
                assert out["meta"]["research_view"]["status"] == "ok", out["meta"]["research_view"]
                assert pa._ADD_CONFIRM_STORE == seeded
    finally:
        for name, fn in originals.items():
            setattr(pa, name, fn)
        pa._ADD_CONFIRM_STORE.clear()
        pa._ADD_CONFIRM_STORE.update(saved_store)


def case_r6_contradicted_rows_stay_legacy():
    for qty, status in ((4, "quantity_mismatch"), (0, "position_closed")):
        evidence = packet(ROWS, qty={1: qty})
        off_out = run(pa, evidence)
        for mode in ("observe", "enforce"):
            out = run(pa, evidence, env=mode_env(mode))
            assert out["actions"][1] == off_out["actions"][1], (status, mode)
            assert out["actions"][1]["detail"]["reconciliation_status"] == status
            assert out["actions"][1]["market_value_sar"] is None
            for key in ("holdings_value_sar", "cost_basis_sar", "pnl_sar", "pnl_pct"):
                assert out["kpis"][key] is None, (status, mode, key)
            rv = out["meta"]["research_view"]
            assert rv["tiers"] == {"contradicted": 1, "matched": len(ROWS) - 1}, rv
            if mode == "enforce":
                assert "contradicting_evidence" in rv["kpi_withheld"] and rv["kpi_basis"] == "withheld"
                assert TAG_M in out["actions"][0]["action_reason"]
                assert out["actions"][0]["market_value_sar"] == 1000


def case_r7_stale_capture_gets_research_view():
    assert pred_r7_stale_packet(pa)


def case_r8_matched_rows_under_withholding():
    assert pred_r8_matched(pa)


def case_r9_privacy():
    evidence = [packet(ROWS, 3 * 3600), packet(ROWS, cash=5), packet(ROWS, qty={1: 4})]
    before = copy.deepcopy(evidence)
    for mode in ("observe", "enforce"):
        for item in evidence:
            serialized = json.dumps(run(pa, item, env=mode_env(mode)))
            for private in (ACCOUNT, SOURCE, "instr-private-sentinel", "reserved_cash",
                            "reconciliation_evidence", "positions_complete"):
                assert private not in serialized, (mode, private)
    assert evidence == before


def case_r10_cash_kpis_sector_alerts_unchanged():
    off_out = run(pa, None)
    out = run(pa, None, env=mode_env("enforce"))
    for key in ("portfolio_value_sar", "cash_sar", "cash_pct"):
        assert out["kpis"][key] is None, key
    assert out["sector_summary"] == [] and out["alerts"] == off_out["alerts"]
    assert len(out["alerts"]) == 1 and out["alerts"][0]["type"] == "portfolio_inputs_unverified"
    assert out["kpis"]["holdings_value_sar"] == 5010 and out["kpis"]["cost_basis_sar"] == 4900
    assert out["kpis"]["pnl_sar"] == 110 and out["kpis"]["pnl_pct"] == 2.2
    assert out["meta"]["research_view"]["kpi_basis"] == "sheet_quantities_custody_unverified"


def case_r11_sukuk_ladder_stays_blank():
    bond = by_symbol(run(pa, None, env=mode_env("enforce")))["SYNTHSUKUK.SR"]
    assert bond["stop_sar"] is bond["tp1_sar"] is bond["tp2_sar"] is None
    assert bond["detail"]["ladder_display"] == "sukuk_na"
    assert bond["market_value_sar"] == 1010 and bond["action"] == "BLOCK"


def case_r15_funded_path_unaffected():
    rows = main_rows()
    evidence = packet(rows)
    off_out = run(pa, evidence, rows=rows)
    assert off_out["meta"]["execution_ready"] is True, off_out["meta"]["input_certification"]
    for mode in ("observe", "enforce"):
        out = run(pa, evidence, rows=rows, env=mode_env(mode))
        assert norm(out) == norm(off_out), mode
        assert "research_view" not in out["meta"]


def case_r16_cost_basis_block_scrubbed():
    assert pred_r16_cost_basis_scrub(pa)


def case_r17_missing_price_withholds_holdings_total():
    assert pred_r17_missing_price(pa)


def case_r18_real_5023_levels_three_of_four():
    assert pa._sukuk_display_active({"symbol": "5023.SR", "name": ""}) is True
    assert pred_r18_real_sukuk(pa)


def case_r19_failure_falls_back_to_off_output():
    original = pa._research_verdict

    def boom(*_args, **_kwargs):
        raise RuntimeError("synthetic research failure")

    pa._research_verdict = boom
    try:
        off_out = run(pa, None)
        for mode in ("observe", "enforce"):
            out = run(pa, None, env=mode_env(mode))
            rv = out["meta"].pop("research_view")
            assert rv == {"mode": mode, "status": "error", "error_class": "RuntimeError"}, rv
            assert norm(out) == norm(off_out), mode
    finally:
        pa._research_verdict = original


def case_r20_unrepresented_position_withholds_holdings_kpis():
    assert pred_r20_unrepresented_withholds_totals(pa)
    rows = main_rows()
    evidence = unlinked_packet(rows)
    off_out = run(pa, evidence, rows=rows)
    obs = run(pa, evidence, rows=rows, env=mode_env("observe"))
    assert all(off_out["kpis"][key] is None for key in KPI_KEYS)
    assert obs["kpis"] == off_out["kpis"] and obs["alerts"] == off_out["alerts"]
    for new, old in zip(obs["actions"], off_out["actions"]):
        assert strip_text(new) == strip_text(old), new["symbol"]


def case_r21_fx_rejected_row_shows_no_sar_figures():
    assert pred_r21_fx_rejected_row_blank(pa)
    rows = fx_rows()
    for evidence in (None, packet(rows)):
        off_out = run(pa, evidence, rows=rows)
        obs = run(pa, evidence, rows=rows, env=mode_env("observe"))
        assert obs["kpis"] == off_out["kpis"] and obs["alerts"] == off_out["alerts"]
        for new, old in zip(obs["actions"], off_out["actions"]):
            assert strip_text(new) == strip_text(old), new["symbol"]
        assert research_of(by_symbol(obs)["FXBAD.SR"]).get("fx_rejected") is True
    # A clean portfolio is unaffected: no row rejected, totals published.
    out = run(pa, None, env=mode_env("enforce"))
    assert out["meta"]["research_view"]["fx_rejected_rows"] == 0
    assert out["kpis"]["holdings_value_sar"] == 5010


def case_r21b_fx_rule_helper():
    rejected = pa._research_fx_rejected
    usd = {"currency": "USD", "fx_to_sar": 3.75}
    assert rejected({}, {"currency": "SAR", "fx_to_sar": 1.0}, {}) is False
    assert rejected({"FX To SAR": ""}, {"currency": "SAR", "fx_to_sar": 1.0}, {"SAR": 1}) is False
    assert rejected({"FX To SAR": 1}, {"currency": "SAR", "fx_to_sar": 1.0}, {"SAR": 1}) is False
    assert rejected({}, usd, {"USD": 3.75}) is False
    assert rejected({"FX Rate": "3.75"}, usd, {"USD": 3.75}) is False
    assert rejected({}, usd, {"USD": 3.7}) is True
    assert rejected({}, usd, {}) is True
    assert rejected({"FX Rate": 3.7}, usd, {"USD": 3.75}) is True
    assert rejected({}, {"currency": "SAR", "fx_to_sar": 3.75}, {"SAR": 1}) is True
    assert rejected({}, {"currency": "SAR", "fx_to_sar": None}, {"SAR": 1}) is True
    assert rejected({}, {"currency": None, "fx_to_sar": 1.0}, {"SAR": 1}) is True
    assert rejected({}, usd, ["not", "a", "map"]) is True


def case_h1_cases_restamp_rows_when_they_run():
    """Age the import-time ROWS past the 15-minute quote bound: the funded
    and certified-value cases must still pass, so none reads ROWS stamps."""
    saved = copy.deepcopy(ROWS)
    aged = _stamp(16 * 60)
    try:
        for row in ROWS:
            row["Warnings"] = row["Warnings"].replace(row["Last Updated"], aged)
            row["Last Updated"] = aged
        case_r2_enforce_levels_equal_certified_values()
        case_r15_funded_path_unaffected()
        assert pred_r8_matched(pa)
        assert pred_r20_unrepresented_withholds_totals(pa)
    finally:
        ROWS[:] = saved


def case_v1_observe_passes_unchanged_readback():
    ok, error = readback(pa, "observe", "off")
    assert ok, error
    ok, error = readback(pa, "observe", "observe")
    assert ok, error
    ok, error = readback(pa, "observe", "enforce")
    assert not ok and "research view" in error, error
    ok, error = readback(pa, "off", "observe")
    assert not ok and "research view" in error, error


def case_v2_enforce_needs_the_lockstep_verifier():
    ok, error = readback(pa, "enforce", "off")
    assert not ok and error == "uncertified portfolio amount was published: holdings_value_sar", error
    assert pred_v2_lockstep_accepts_enforce(pa)
    ok, error = readback(pa, "enforce", "observe")
    assert not ok, error
    ok, error = readback(pa, "off", "enforce")
    assert not ok and "research view" in error, error


def case_g1_golden_negative_base_has_no_research_view():
    base = base_module()
    failed_on_base = [fn.__name__ for fn in GATE_ON_PREDICATES if not fn(base)]
    passed_on_delivered = [fn.__name__ for fn in GATE_ON_PREDICATES if fn(pa)]
    print("G1 gate-on predicates: base fails %d/%d, delivered passes %d/%d" % (
        len(failed_on_base), len(GATE_ON_PREDICATES), len(passed_on_delivered), len(GATE_ON_PREDICATES)))
    assert len(failed_on_base) == len(GATE_ON_PREDICATES), sorted(
        set(fn.__name__ for fn in GATE_ON_PREDICATES) - set(failed_on_base))
    assert len(passed_on_delivered) == len(GATE_ON_PREDICATES)
    shown = run(base, None, env=mode_env("enforce"))
    assert not any(TAG_U in row["action_reason"] or row["stop_sar"] for row in shown["actions"])


def _defs(source):
    return {node.name for node in ast.walk(ast.parse(source))
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}


def case_g2_ast_zero_removed():
    delivered = DELIVERED.read_text(encoding="utf-8")
    names = _defs(delivered)
    assert NEW_FUNCTIONS <= names, sorted(NEW_FUNCTIONS - names)
    for name in pa.__all__:
        assert hasattr(pa, name), name
    if not BASE_PATH or not os.path.exists(BASE_PATH):
        print("G2 new functions present; base diff skipped (TFB_TEST_PA_BASE unset)")
        return
    base_names = _defs(Path(BASE_PATH).read_text(encoding="utf-8"))
    removed, added = base_names - names, names - base_names
    print("G2 defs base=%d delivered=%d added=%s removed=%s" % (
        len(base_names), len(names), sorted(added), sorted(removed)))
    assert not removed, sorted(removed)
    assert NEW_FUNCTIONS <= added, sorted(added)


def case_g3_added_lines_ascii():
    delivered = DELIVERED.read_text(encoding="utf-8")
    if BASE_PATH and os.path.exists(BASE_PATH):
        base = Path(BASE_PATH).read_text(encoding="utf-8")
        added = [line[1:] for line in difflib.unified_diff(base.splitlines(), delivered.splitlines(), lineterm="", n=0)
                 if line.startswith("+") and not line.startswith("+++")]
        print("G3 %d lines added vs base, all checked for ASCII" % len(added))
    else:
        start = delivered.index("# v1.16.0 (2026-10-13)")
        added = delivered[start:delivered.index("# v1.15.1:", start)].splitlines()
        added += inspect.getsource(pa._env_research_view_mode).splitlines()
        added += inspect.getsource(pa._research_verdict).splitlines()
        added += inspect.getsource(pa._research_fx_rejected).splitlines()
        added += inspect.getsource(pa._research_view_portfolio).splitlines()
        added += [pa.RESEARCH_TAG_UNVERIFIED, pa.RESEARCH_TAG_MATCHED, pa.RESEARCH_ADD_WITHHELD,
                  pa.RESEARCH_FX_WITHHELD]
        added += list(pa.RESEARCH_KPI_BLOCKING_REASONS)
        added += sorted(pa.RESEARCH_UNVERIFIED_STATES)
    bad = [line for line in added if any(ord(ch) > 127 for ch in line)]
    assert added and not bad, bad[:5]


CASES = [(name[len("case_"):], fn) for name, fn in sorted(globals().items())
         if name.startswith("case_") and callable(fn)]

if pytest is not None:
    @pytest.mark.parametrize("name", [name for name, _ in CASES])
    def test_pf_research_view_p06(name):
        dict(CASES)[name]()


def _main():
    passed = failed = skipped = 0
    for name, fn in CASES:
        try:
            fn()
        except unittest.SkipTest as exc:
            skipped += 1
            print("SKIP " + name + " :: " + str(exc))
            continue
        except Exception:  # noqa: BLE001
            failed += 1
            print("FAIL " + name)
            traceback.print_exc()
            continue
        passed += 1
        print("ok   " + name)
    suffix = ("  (skipped %d)" % skipped) if skipped else ""
    print("PASS %d/%d%s" % (passed, passed + failed, suffix))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_main())
