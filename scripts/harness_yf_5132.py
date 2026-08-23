#!/usr/bin/env python3
"""harness_yf_5132.py — proof battery for data_engine_v2.py v5.132.0
(W1B-3 / IR-076: gated, budgeted, equity-only TARGET-FALLBACK trigger).

The 16k-line engine module is not importable in a lean env, so this uses the
in-tree AST-lift pattern (accepted since harness_sync_6410): the REAL
FunctionDefs and module-level Assigns are lifted out of the source and
executed with the REAL core.symbols.normalize.normalize_symbol injected —
no stand-ins for anything under test. Also asserts the untouchability
contracts (24-field set, LKG twin, accept list) byte-level. Exit 0 iff all
checks pass.
"""
from __future__ import annotations

import ast
import io
import logging
import os
import re as _re
import sys
import math
import typing

SRC_PATH = os.environ.get("ENG_UNDER_TEST", "/home/claude/new_eng_5132.py")
REPO = os.environ.get("REPO_TREE", "/home/claude/ci2")
sys.path.insert(0, REPO)
from core.symbols.normalize import normalize_symbol  # REAL, tiny, pure

SRC = io.open(SRC_PATH, encoding="utf-8").read()
TREE = ast.parse(SRC)

LIFT_FUNCS = {
    "_is_missing_or_unknown_field", "_infer_asset_class_from_symbol",
    "_yf_target_fallback_enabled", "_yf_target_fallback_max",
    "_yf_target_budget_take", "_yf_asset_class_ok",
    "_row_needs_target_fallback", "_apply_target_fallback",
    "_append_yahoo_warning_tag", "_safe_str", "_as_float",
}

G = {
    "os": os, "re": _re, "logging": logging, "math": math,
    "logger": logging.getLogger("lift"),
    "normalize_symbol": normalize_symbol,
    "Any": typing.Any, "Dict": typing.Dict, "List": typing.List,
    "Tuple": typing.Tuple, "Set": typing.Set,
    "Optional": typing.Optional, "Sequence": typing.Sequence,
    "__version__": "5.132.0",
}
# module-level Assigns (constants incl. _YAHOO_UNKNOWN_STRINGS,
# _COMMODITY_SYMBOL_HINTS, _YF_TARGET_BUDGET) — try/skip like the in-tree lift
for node in TREE.body:
    if isinstance(node, (ast.Assign, ast.AnnAssign)):
        try:
            exec(compile(ast.Module([node], []), "<lift>", "exec"), G)
        except Exception:
            continue
lifted = set()
for node in ast.walk(TREE):
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) \
            and node.name in LIFT_FUNCS:
        exec(compile(ast.Module([node], []), "<lift>", "exec"), G)
        lifted.add(node.name)
missing = LIFT_FUNCS - lifted
assert not missing, f"lift incomplete: {missing}"

PASS = FAIL = 0
def check(name, cond, detail=""):
    global PASS, FAIL
    if cond: PASS += 1; print(f"  PASS  {name}")
    else:    FAIL += 1; print(f"  FAIL  {name}  {detail}")

def reset(**env):
    for k in ("TFB_YF_TARGET_FALLBACK", "TFB_YF_TARGET_FALLBACK_MAX"):
        os.environ.pop(k, None)
    os.environ.update(env)
    G["_YF_TARGET_BUDGET"]["used"] = 0
    G["_YF_TARGET_BUDGET"]["warned"] = False

def row(sym="AAPL", name="Apple", price=10.0, target=None, acls=None):
    r = {"symbol": sym, "name": name, "current_price": price,
         "warnings": ""}
    if target is not None: r["target_mean_price"] = target
    if acls is not None: r["asset_class"] = acls
    return r

print("== H1  gates and clamps ==")
reset()
check("master gate default OFF", G["_yf_target_fallback_enabled"]() is False)
reset(TFB_YF_TARGET_FALLBACK="1")
check("gate arms on 1", G["_yf_target_fallback_enabled"]() is True)
reset()
check("budget default 200", G["_yf_target_fallback_max"]() == 200)
reset(TFB_YF_TARGET_FALLBACK_MAX="0")
check("budget floor 1", G["_yf_target_fallback_max"]() == 1)
reset(TFB_YF_TARGET_FALLBACK_MAX="99999")
check("budget cap 5000", G["_yf_target_fallback_max"]() == 5000)
reset(TFB_YF_TARGET_FALLBACK_MAX="junk")
check("budget unparseable -> 200", G["_yf_target_fallback_max"]() == 200)

print("== H2  pure predicate — OFF is a hard no ==")
reset()
check("gate OFF: even perfect candidate -> False",
      G["_row_needs_target_fallback"](row()) is False)

print("== H3  pure predicate — armed matrix ==")
reset(TFB_YF_TARGET_FALLBACK="1")
P = G["_row_needs_target_fallback"]
check("equity, priced, named, target-blank -> True", P(row()) is True)
check("target present -> False", P(row(target=123.4)) is False)
check("blank name -> False", P(row(name="")) is False)
check("unknown-string name -> False", P(row(name="N/A")) is False)
check("no price -> False", P(row(price=None)) is False)
check("zero price -> False", P(row(price=0)) is False)
check("price fallback key honored",
      P({"symbol": "MSFT", "name": "Microsoft", "price": 5.0,
         "warnings": ""}) is True)
check("non-dict -> False", P(None) is False)

print("== H4  equity-only contract ==")
reset(TFB_YF_TARGET_FALLBACK="1")
check("sukuk asset_class excluded",
      P(row(sym="5023.SR", acls="Fixed Income / Sukuk")) is False)
check("Mutual Fund excluded", P(row(acls="Mutual Fund")) is False)
check("ETF excluded", P(row(acls="ETF")) is False)
check(".SR with Equity class -> True", P(row(sym="1050.SR")) is True)
check("crypto -USD excluded despite Equity default-infer",
      P(row(sym="DOT-USD")) is False)
check("futures =F excluded", P(row(sym="KE=F")) is False)
check("FX =X excluded", P(row(sym="SAR=X")) is False)
check("index ^ excluded", P(row(sym="^TASI.SR")) is False)
check("row asset_class wins over symbol infer",
      P(row(sym="AAPL", acls="Commodity")) is False)
check("missing asset_class falls to inferrer (US equity ok)",
      P(row(sym="OTIS")) is True)

print("== H5  _apply_target_fallback seam ==")
reset(TFB_YF_TARGET_FALLBACK="1", TFB_YF_TARGET_FALLBACK_MAX="2")
A = G["_apply_target_fallback"]
r1 = row()
check("flip: False->True, budget consumed, tag stamped",
      A(r1, False) is True and G["_YF_TARGET_BUDGET"]["used"] == 1
      and "yf_target_fallback_triggered" in r1["warnings"])
r2 = row(target=50.0)
check("non-candidate stays False, no budget, no tag",
      A(r2, False) is False and G["_YF_TARGET_BUDGET"]["used"] == 1
      and r2["warnings"] == "")
r3 = row()
check("needs_fund already True passes through FREE (no budget/tag)",
      A(r3, True) is True and G["_YF_TARGET_BUDGET"]["used"] == 1
      and r3["warnings"] == "")
r4 = row()
check("second flip consumes budget slot 2", A(r4, False) is True
      and G["_YF_TARGET_BUDGET"]["used"] == 2)
r5 = row()
check("budget exhausted -> False, no tag",
      A(r5, False) is False and r5["warnings"] == ""
      and G["_YF_TARGET_BUDGET"]["warned"] is True)
reset()
r6 = row()
check("gate OFF: seam is pure passthrough (False stays False, no tag)",
      A(r6, False) is False and r6["warnings"] == ""
      and G["_YF_TARGET_BUDGET"]["used"] == 0)

print("== H6  tag mechanics via REAL _append_yahoo_warning_tag ==")
reset(TFB_YF_TARGET_FALLBACK="1")
r = row(); r["warnings"] = "existing_warn"
A(r, False)
check("tag appended after existing with '; '",
      r["warnings"] == "existing_warn; yf_target_fallback_triggered")
rl = row(); rl["warnings"] = ["a"]
A(rl, False)
check("list-form warnings supported",
      rl["warnings"] == ["a", "yf_target_fallback_triggered"])

print("== H7  untouchability contracts (byte-level) ==")
needs = _re.search(r"_YAHOO_FUNDAMENTAL_NEEDS_CHECK_FIELDS: Tuple\[str, \.\.\.\] = \((.*?)\)\n",
                   SRC, _re.S).group(1)
check("24-field needs-check has NO target fields",
      "target" not in needs and "analyst" not in needs
      and needs.count('"') == 48)
check("_FUND_LKG_FIELDS still aliases the untouched tuple",
      "_FUND_LKG_FIELDS: Tuple[str, ...] = _YAHOO_FUNDAMENTAL_NEEDS_CHECK_FIELDS" in SRC)
check("accept list still carries targets",
      '"target_mean_price", "target_high_price", "target_low_price",' in SRC)
check("exactly ONE call-site seam",
      SRC.count("needs_fund = _apply_target_fallback(row, needs_fund)") == 1)
check("seam sits directly after the needs-check call",
      "needs_fund, needs_chart = _row_needs_yahoo_enrichment(row)\n        # v5.132.0"
      in SRC)

print(f"\nRESULT: {PASS} passed, {FAIL} failed")
sys.exit(1 if FAIL else 0)
