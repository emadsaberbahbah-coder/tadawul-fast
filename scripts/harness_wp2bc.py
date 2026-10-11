#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
scripts/harness_wp2bc.py - WP-2bc real-module harness (v1.0.0, 2026-10-11)
==============================================================================
Certifies, offline and without credentials:
  core/data_engine_v2.py 5.153.0      TFB_SETTLE_UNRESOLVED (WP-2b)
  scripts/run_dashboard_sync.py 6.66.0 TFB_RANK_OVERALL_PAGE (WP-2c)
  routes/analysis_sheet_rows.py 4.8.2  per-request rank withholds engine-withheld rows
  core/analysis/opportunity_builder.py 1.25.1  settle_unresolved:enforce fails Scoring Settlement

WHAT RUNS: the REAL functions of the REAL modules. The sync legs drive the REAL
SheetsWriter.write_table over a RECORDING TRANSPORT (the RecorderSheets
precedent of scripts/harness_sync_6410.py and the FakeSheetsAPI of
tests/test_sync_sheet_presentation.py): the only double is the Sheets client,
and it only records the update / append calls it receives (RAW null-skip
semantics applied to an in-memory grid). No predicate, stats dict or guard is
re-implemented here - this is not a fake-object harness. Engine fixtures are
the six real 2026-09-22 Global_Markets market rows and the synthetic F-7 rows
of tests/test_de_f7_scoring_settle.py; page matrices come from
tests/test_sync_rank_overall_page.py. No portfolio data, no network.

LEGS (all inside the digest):
  L1 ENGINE  : settle pass x (TFB_SCORING_SETTLE observe|enforce) x
               (TFB_SETTLE_UNRESOLVED off|observe|enforce) on the real
               fixtures and the drift / oscillation / error rows; then the
               gate, rank, Top-10, projection, portfolio-decision and stale-
               receipt (C5) readers; tag regex and forbidden-substring checks.
  L2 CORE    : _rank_overall_page_core off|observe|enforce and enforce(enforce).
  L3 SYNC    : write_table off|observe|enforce on four pages (recorded
               payloads + recorded _Run_Log rows, timestamps normalised).
  L4 ROUTE / BUILDER : per-request rank and the builder's settlement reader.
  L5 DUAL-TREE BYTE-IDENTITY (gate unset): the same off-mode corpus computed in
               two child processes, one importing this tree and one importing
               a BASE tree (the four files materialised from git at
               BASE_COMMIT beside copies of every other module); per-section
               digests must be equal. --base-tree DIR uses a prepared tree.
OPTIONAL EXPORT LEG (local only, NOT in the digest): TFB_WP2_EXPORT_PATH=<xlsx>
  prints the DQ-37 / DQ-41 export counts and the WP-0 DQ-41 record after the
  page-rank core (openpyxl read_only=True, data_only=True).

USAGE (repo root):  python scripts/harness_wp2bc.py [--base-tree DIR]
EXIT 0 iff every leg passes. Prints WP2BC-DIGEST sha256=<hex> ...
"""
from __future__ import annotations

import argparse
import contextlib
import copy
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Any, Dict, List

HARNESS_VERSION = "1.0.0"
BASE_COMMIT = "18a84168aac1d462dd58494239ac91f8cef7178c"
CHANGED_FILES = ("core/data_engine_v2.py", "scripts/run_dashboard_sync.py",
                 "routes/analysis_sheet_rows.py", "core/analysis/opportunity_builder.py")
COPY_DIRS = ("core", "routes", "scripts", "integrations", "tests", "config")
COPY_FILES = ("config.py", "env.py", "main.py", "symbols_reader.py")
REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PAGES = ("Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds")
VOLATILE = ("scoring_updated_utc", "scoring_updated_riyadh")
UNRES_RE = re.compile(r"^settle_unresolved:(observe|enforce):(pass_limit|oscillation):p\d+(:ov=[^:;]+)?(:rc=[^:;]+)?$")
WARNING_CORPUS = (
    "", "provider_ok", "f7_settle_failed:error", "f7_settle_failed:non_converged; provider_ok",
    "f7_settle:observe:st3:p2:ov=70>62:rc=hold>sell", "margin_publish:profit_margin:unresolved",
    "low_data_trust; rank_skipped_low_trust", "x_settle_unresolved:y",
)


# --------------------------------------------------------------------------- #
# environment discipline
# --------------------------------------------------------------------------- #
@contextlib.contextmanager
def pinned_env(**values):
    """Clear every TFB_* variable, set the given ones, restore everything."""
    saved = dict(os.environ)
    try:
        for key in list(os.environ):
            if key.startswith("TFB_"):
                del os.environ[key]
        os.environ["GITHUB_RUN_ID"] = "wp2bc-harness"
        for key, value in values.items():
            if value is not None:
                os.environ[key] = str(value)
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)


def _digest(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _clean(row: Any) -> Any:
    if isinstance(row, dict):
        return {k: v for k, v in row.items() if k not in VOLATILE}
    return row


# --------------------------------------------------------------------------- #
# recording transport around the REAL SheetsWriter
# --------------------------------------------------------------------------- #
class RecordingSheets:
    """Records values.update / values.append and applies RAW null-skip to a grid."""

    def __init__(self, grid=None):
        self.updates: List[dict] = []
        self.appends: List[dict] = []
        self.grid = copy.deepcopy(grid or [])
        self._last = None

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def update(self, **kwargs):
        self._last = ("update", copy.deepcopy(kwargs))
        self.updates.append(self._last[1])
        return self

    def append(self, **kwargs):
        self._last = ("append", copy.deepcopy(kwargs))
        self.appends.append(self._last[1])
        return self

    def execute(self):
        kind, kwargs = self._last
        if kind == "update":
            for ri, row in enumerate(kwargs["body"]["values"]):
                while len(self.grid) <= ri:
                    self.grid.append([])
                while len(self.grid[ri]) < len(row):
                    self.grid[ri].append("")
                for ci, value in enumerate(row):
                    if value is not None:
                        self.grid[ri][ci] = value
        return {}


def _norm_append(call: dict) -> dict:
    row = list(call["body"]["values"][0])
    row[0] = "<ts>"
    try:
        details = json.loads(row[9])
        details.pop("ts_utc", None)
        row[9] = details
    except Exception:
        pass
    return {"range": call["range"], "valueInputOption": call["valueInputOption"],
            "insertDataOption": call.get("insertDataOption"), "row": row}


def sync_write(sync, page, headers, rows, seeded=True):
    api = RecordingSheets([headers] + copy.deepcopy(rows) if seeded else None)
    writer = sync.SheetsWriter()
    writer._service = api
    n = writer.write_table("HARNESS_BOOK", page, "A1", headers, copy.deepcopy(rows))
    return {"written": n, "updates": api.updates, "appends": [_norm_append(a) for a in api.appends],
            "grid_rank": [r[headers.index("Rank (Overall)")] if len(r) > headers.index("Rank (Overall)") else None
                          for r in api.grid[1:]]}


SYNC_ENV = {"TFB_SYNC_OHLC_FILL_GUARD": "0", "TFB_WARNINGS_COMPACT": "off", "TFB_SYNC_OHLC_RUNLOG": "1"}


# --------------------------------------------------------------------------- #
# engine helpers (REAL module functions only)
# --------------------------------------------------------------------------- #
def _scorers(de):
    def drift(r):
        r["overall_score"] += 1.0

    def oscillate(r):
        r["overall_score"] = 20.0 if r["overall_score"] == 80.0 else 80.0

    def boom(r):
        r["current_price"] = 999
        raise RuntimeError("settle boom")
    return drift, oscillate, boom


def _settle(de, row, scorer, max_passes):
    from unittest.mock import patch
    os.environ["TFB_SCORING_SETTLE_MAX_PASSES"] = str(max_passes)
    with patch.object(de, "_compute_scores_canonical_first", scorer), \
            patch.object(de, "_apply_phase_dd_enhancements", lambda r: r):
        return de._f7_settle_pass(row, row.get("symbol", ""), "Global_Markets")


def engine_corpus(de, f7, unr_mod, unres_values):
    """Engine outputs for the given TFB_SETTLE_UNRESOLVED values (None = unset).
    Readers run on untagged rows only when called for the off corpus."""
    drift, oscillate, boom = _scorers(de)
    out: Dict[str, Any] = {}
    for settle in ("observe", "enforce"):
        for unres in unres_values:
            key = "%s|%s" % (settle, unres)
            with pinned_env(TFB_SCORING_SETTLE=settle, TFB_SETTLE_UNRESOLVED=unres):
                fx_out = []
                for fx in f7.FIXTURES:
                    os.environ["TFB_SCORING_SETTLE_MAX_PASSES"] = "4"
                    row = f7._prep(fx)
                    fx_out.append(_clean(de._f7_settle_pass(row, row["symbol"], "Global_Markets")))
                synth = {
                    "drift": _clean(_settle(de, f7._failure_row(), drift, 2)),
                    "oscillate": _clean(_settle(de, f7._failure_row(), oscillate, 4)),
                    "error": _clean(_settle(de, f7._failure_row(), boom, 4)),
                }
                cached = f7._failure_row()
                cached["warnings"] = "f7_settle_failed:non_converged"
                synth["cached_receipt"] = _clean(_settle(de, cached, drift, 2))
                out[key] = {"fixtures": fx_out, "synthetic": synth}
    return out


def engine_readers_untagged(de, f7, unr_mod):
    """Gate / rank / Top-10 / projection / portfolio decision on rows that carry
    no settle_unresolved receipt (every row of a never-armed deployment)."""
    res: Dict[str, Any] = {}
    with pinned_env(TFB_INVESTABILITY_GATE="1"):
        ready = f7._failure_row()
        ready.update(unr_mod.GATE_READY)
        g = copy.deepcopy(ready)
        de._apply_investability_gate(g)
        res["gate_ready"] = _clean(g)
        res["top10"] = de._top10_row_is_eligible(copy.deepcopy(g))
        res["projected"] = de._strict_project_row(de.INSTRUMENT_CANONICAL_KEYS, copy.deepcopy(ready))
        failed = f7._failure_row()
        failed["warnings"] = "f7_settle_failed:non_converged"
        low = {"symbol": "LOW.US", "overall_score": 90.0, "trust_level": "LOW"}
        plain = {"symbol": "PLAIN.US", "overall_score": 60.0}
        rows = [copy.deepcopy(g), failed, low, plain, {"symbol": "NOSCORE.US"}]
        de._apply_rank_overall(rows)
        res["rank"] = [_clean(r) for r in rows]
        res["portfolio_decision"] = [
            de._portfolio_decision(f7._failure_row(), 80.0, 5.0, 60.0),
            de._portfolio_decision(failed, 80.0, 5.0, 60.0),
            de._portfolio_decision(dict(f7._failure_row(), recommendation="SELL", recommendation_detailed="SELL"),
                                   80.0, 5.0, 60.0),
            de._portfolio_decision(f7._failure_row(), -80.0, 5.0, 60.0),
        ]
    return res


def engine_armed(de, f7, unr_mod):
    """Delivered-only: the enforce-published row through every reader, the
    observe receipt, the stale-receipt retirement (C5) and the composer."""
    drift, oscillate, _boom = _scorers(de)
    res: Dict[str, Any] = {"checks": {}}
    ok = True
    with pinned_env(TFB_SCORING_SETTLE="enforce", TFB_SETTLE_UNRESOLVED="enforce", TFB_INVESTABILITY_GATE="1"):
        ready = f7._failure_row()
        ready.update(unr_mod.GATE_READY)
        pub = _settle(de, ready, drift, 2)
        res["published"] = _clean(copy.deepcopy(pub))
        g = copy.deepcopy(pub)
        de._apply_investability_gate(g)
        res["gated"] = _clean(g)
        res["top10"] = de._top10_row_is_eligible(g)
        other = {"symbol": "OTHER.US", "overall_score": 50.0}
        rows = [copy.deepcopy(g), other]
        de._apply_rank_overall(rows)
        res["rank"] = [_clean(r) for r in rows]
        res["projected"] = de._strict_project_row(de.INSTRUMENT_CANONICAL_KEYS, copy.deepcopy(g))
        res["portfolio_decision"] = de._portfolio_decision(pub, 80.0, 5.0, 60.0)
        res["checks"]["gate_watchlist"] = g.get("investability_status") == "WATCHLIST"
        res["checks"]["top10_false"] = res["top10"] is False
        res["checks"]["rank_withheld"] = rows[0].get("rank_overall") is None and other["rank_overall"] == 1
        res["checks"]["projected_hold"] = res["projected"].get("recommendation") == "HOLD"
        res["checks"]["no_add"] = res["portfolio_decision"] == "HOLD"
        res["checks"]["json_kept"] = "scoring_settlement:" in str(pub.get("scoring_errors"))
    with pinned_env(TFB_SCORING_SETTLE="observe", TFB_SETTLE_UNRESOLVED="observe"):
        obs = _settle(de, f7._failure_row(), oscillate, 4)
        res["observe"] = _clean(obs)
    tags = [p.strip() for r in (res["published"], res["observe"])
            for p in str(r.get("warnings") or "").split(";") if p.strip().startswith("settle_unresolved:")]
    res["tags"] = tags
    res["checks"]["tag_regex"] = len(tags) == 2 and all(UNRES_RE.match(t) for t in tags)
    res["checks"]["tag_safe"] = not any(f in t.lower() for t in tags for f in de._REL_PATH_FORBIDDEN)
    with pinned_env(TFB_SCORING_SETTLE="observe"):
        os.environ["TFB_SCORING_SETTLE_MAX_PASSES"] = "4"
        row = f7._prep(f7.FIXTURES[1])
        row["warnings"] = (str(row.get("warnings") or "") +
                           "; settle_unresolved:enforce:pass_limit:p3:ov=70:rc=buy; rank_skipped_settle_unresolved")
        out = de._f7_settle_pass(row, row["symbol"], "Global_Markets")
        res["c5_strip"] = _clean(out)
        res["checks"]["c5_retired"] = not de._f7_settle_unresolved(out)
    states = {}
    for word in (None, "observe", "enforce", "1"):
        with pinned_env(TFB_SETTLE_UNRESOLVED=word):
            states[str(word)] = de.surface_gate_states().get("settle_unresolved")
    res["gate_states"] = states
    res["checks"]["gate_words"] = states == {"None": "off", "observe": "observe", "enforce": "enforce", "1": "off"}
    ok = all(res["checks"].values())
    return res, ok


# --------------------------------------------------------------------------- #
# sync / route / builder corpora
# --------------------------------------------------------------------------- #
def sync_off(sync, mats):
    out = {}
    with pinned_env(**SYNC_ENV):
        for page in PAGES:
            for name, builder in mats:
                headers, rows = builder(page)
                out["%s|%s" % (page, name)] = sync_write(sync, page, headers, rows)
    return out


def route_off(asr):
    rows = [{"symbol": "A.US", "overall_score": 80.0, "rank_overall": 1, "warnings": "provider_ok"},
            {"symbol": "B.US", "overall_score": 70.0, "rank_overall": 2, "warnings": "low_data_trust"},
            {"symbol": "C.US", "overall_score": None, "rank_overall": 3, "warnings": "f7_settle_failed:non_converged"},
            {"symbol": "D.US", "overall_score": "1,234.5", "rank_overall": 4, "warnings": "margin_publish:x:unresolved"},
            {"symbol": "E.US", "overall_score": 60.0, "rank_overall": 5, "warnings": ["a", "b"]}]
    res = {}
    with pinned_env():
        for page in PAGES + ("My_Portfolio",):
            rr = copy.deepcopy(rows)
            n = asr._apply_global_rank_overall(rr, page)
            res[page] = {"n": n, "rows": rr}
    return res


def builder_off(ob):
    from tests.decision_evidence_fixtures import build_with_observed_inputs, observed_portfolio, observed_price_fields
    res: Dict[str, Any] = {"reader": {w: ob._scoring_settlement_failure({"warnings": w}) for w in WARNING_CORPUS}}

    def row(sym, w):
        r = {"symbol": sym, "name": "Synthetic " + sym, "sector": "Energy", "market": "Tadawul",
             "currency": "SAR", "current_price": 100.0, "intrinsic_value": 130.0,
             "forecast_reliability_score": 82.0, "data_quality_score": 91.0, "risk_bucket": "Moderate",
             "provider_engine_conflict": "No", "volatility_30d": 4.0, "avg_volume_30d": 2500000,
             "expected_roi_12m": 24.0, "recommendation_detailed": "STRONG BUY",
             "investability_status": "WATCHLIST", "block_reason": "", "warnings": w}
        r.update(observed_price_fields())
        return r

    with pinned_env(TFB_OPP_ENABLED="1", TFB_OPP_FUNDING_PLAN="1", TFB_OPP_INVESTABILITY_GATE="0",
                    TFB_T10_REL_FLOOR_MODE="display"):
        os.environ["APP_TOKEN"] = "synthetic-board-test"
        rows = [row("CLEAN.SR", "provider_ok"), row("FAILED.SR", "f7_settle_failed:non_converged"),
                row("OBS.SR", "f7_settle:observe:st3:p2:ov=70>62")]
        payload = build_with_observed_inputs(
            ob, rows, criteria={"investability_gate_enabled": False, "sell_class_gate_enabled": False,
                                "max_selected": 2, "board_funding_stage": "research"},
            portfolio=observed_portfolio({}, {"SAR": 1.0}), fx_rates={"SAR": 1.0})
        res["status"] = payload.get("status")
        res["candidates"] = sorted(
            [{"symbol": c["symbol"], "verdict": c.get("verdict"), "opportunity_score": c.get("opportunity_score"),
              "first_fail": (c.get("first_fail") or {}).get("gate"),
              "first_fail_current": (c.get("first_fail") or {}).get("current")}
             for c in payload.get("candidates_rows", [])], key=lambda c: c["symbol"])
    return res


# --------------------------------------------------------------------------- #
# L5 child: the off-mode corpus inside ONE tree
# --------------------------------------------------------------------------- #
def child_off(tree: str) -> Dict[str, Any]:
    sys.path.insert(0, tree)
    import core.data_engine_v2 as de
    from tests import test_de_f7_scoring_settle as f7
    from tests import test_de_f7_settle_unresolved as unr_mod
    from tests import test_sync_rank_overall_page as srp
    from scripts import run_dashboard_sync as sync
    import routes.analysis_sheet_rows as asr
    from core.analysis import opportunity_builder as ob
    words = (None, "off", "1", "true")
    eng_all = engine_corpus(de, f7, unr_mod, words)
    # every off word must give the same engine output inside this tree
    per_word = {}
    for key, value in eng_all.items():
        settle, unres = key.split("|")
        per_word.setdefault(settle, []).append(_digest(value))
    words_equal = all(len(set(v)) == 1 for v in per_word.values())
    engine = {k: v for k, v in eng_all.items() if k.endswith("|None")}
    engine["readers"] = engine_readers_untagged(de, f7, unr_mod)
    sync_res = sync_off(sync, (("special", srp.special_matrix), ("batch", srp.batch_matrix)))
    rank_words = {}
    for word in ("off", "1", "on", "garbage"):
        with pinned_env(TFB_RANK_OVERALL_PAGE=word, **SYNC_ENV):
            headers, rows = srp.batch_matrix("Global_Markets")
            rank_words[word] = _digest(sync_write(sync, "Global_Markets", headers, rows))
    with pinned_env(**SYNC_ENV):
        headers, rows = srp.batch_matrix("Global_Markets")
        rank_words["unset"] = _digest(sync_write(sync, "Global_Markets", headers, rows))
    sections = {"engine": engine, "sync": sync_res, "route": route_off(asr), "builder": builder_off(ob)}
    return {
        "versions": {"engine": de.__version__, "sync": sync.SCRIPT_VERSION,
                     "route": asr.ANALYSIS_SHEET_ROWS_VERSION, "builder": ob.OPPORTUNITY_BUILDER_VERSION},
        "digests": {k: _digest(v) for k, v in sections.items()},
        "engine_off_words_equal": words_equal,
        "sync_off_words_equal": len(set(rank_words.values())) == 1,
    }


def build_base_tree(work: str) -> str:
    tree = os.path.join(work, "base_tree")
    os.makedirs(tree, exist_ok=True)
    for d in COPY_DIRS:
        shutil.copytree(os.path.join(REPO, d), os.path.join(tree, d),
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
    for f in COPY_FILES:
        if os.path.exists(os.path.join(REPO, f)):
            shutil.copy2(os.path.join(REPO, f), os.path.join(tree, f))
    for path in CHANGED_FILES:
        blob = subprocess.run(["git", "-C", REPO, "show", "%s:%s" % (BASE_COMMIT, path)],
                              check=True, capture_output=True).stdout
        with open(os.path.join(tree, path), "wb") as fh:
            fh.write(blob)
    return tree


def run_child(tree: str) -> Dict[str, Any]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("TFB_")}
    env["PYTHONHASHSEED"] = "0"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    proc = subprocess.run([sys.executable, "-I", os.path.abspath(__file__), "--child-off", tree],
                          cwd=tree, env=env, capture_output=True, text=True, timeout=900)
    if proc.returncode != 0:
        raise RuntimeError("child failed for %s: %s" % (tree, proc.stderr[-2000:]))
    line = [ln for ln in proc.stdout.splitlines() if ln.startswith("CHILD-JSON ")][-1]
    return json.loads(line[len("CHILD-JSON "):])


# --------------------------------------------------------------------------- #
# optional export leg (never in the digest)
# --------------------------------------------------------------------------- #
def export_leg(path: str, sync) -> None:
    import openpyxl
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    print("EXPORT-LEG (not in digest) %s" % os.path.basename(path))
    for page in PAGES:
        ws = wb[page]
        it = ws.iter_rows(values_only=True)
        hdr = [str(h).strip() if h is not None else "" for h in next(it)]
        ix = {h: i for i, h in enumerate(hdr)}
        rows = []
        for r in it:
            r = list(r) + [None] * (len(hdr) - len(r))
            if r[ix["Symbol"]] is None or str(r[ix["Symbol"]]).strip() == "":
                continue
            rows.append(r[:len(hdr)])

        def tags(r):
            return [p.strip() for p in str(r[ix["Warnings"]] or "").split(";") if p.strip()]

        nc = [r for r in rows if "f7_settle_failed:non_converged" in tags(r)]
        unres_prefix = sum(1 for r in rows if any(t.startswith("settle_unresolved:") for t in tags(r)))
        unres_sub = sum(1 for r in rows if "unresolved" in str(r[ix["Warnings"]] or ""))
        st = {}
        for r in nc:
            k = str(r[ix["Investability Status"]] or "")
            st[k] = st.get(k, 0) + 1
        ranks = [sync._rank_page_score(r[ix["Rank (Overall)"]]) for r in rows]
        blank_str = sum(1 for r in rows if r[ix["Rank (Overall)"]] == "")
        _o, s_obs = sync._rank_overall_page_core(hdr, rows, "observe")
        out, s_enf = sync._rank_overall_page_core(hdr, rows, "enforce")
        after = [r[ix["Rank (Overall)"]] for r in out if r[ix["Rank (Overall)"]] != ""]
        ranked_idx = {i for i, v in enumerate(ranks) if v is not None}
        new_idx = {i for i, r in enumerate(out) if r[ix["Rank (Overall)"]] != ""}
        print("  %s live=%d nonconv=%d status=%s investable_nonconv=%d settle_unresolved_prefix=%d "
              "substring_unresolved=%d ranked=%d distinct_before=%d blank_str_rank=%d eligible=%d "
              "elig_not_ranked=%d ranked_not_elig=%d distinct_after=%d changed=%d top=%s %s"
              % (page, len(rows), len(nc), json.dumps(st, sort_keys=True),
                 sum(1 for r in nc if str(r[ix["Investability Status"]]) == "INVESTABLE"),
                 unres_prefix, unres_sub, len(ranked_idx), s_obs["distinct_before"], blank_str,
                 s_enf["eligible"], len(new_idx - ranked_idx), len(ranked_idx - new_idx), len(set(after)),
                 s_enf["changed"], s_enf["top"], s_enf["top_score"]))
    wb.close()
    try:
        sys.path.insert(0, os.path.join(REPO, "scripts"))
        import tfb_workbook_dq_audit as dq
        book = dq.load_book(path)
        before = dq.build_context(book, None)
        b37, b41 = dq.dq_37_f7(before), dq.dq_41_rank(before)
        for page in PAGES:
            rows = book.sheets[page]
            hdr = [str(h).strip() if h is not None else "" for h in rows[0]]
            si = hdr.index("Symbol")
            live = [list(r) for r in rows[1:] if r[si] is not None and str(r[si]).strip()]
            out, _s = sync._rank_overall_page_core(hdr, live, "enforce")
            it = iter(out)
            book.sheets[page] = [rows[0]] + [tuple(next(it)) if (r[si] is not None and str(r[si]).strip())
                                             else r for r in rows[1:]]
        after = dq.build_context(book, None)
        a41 = dq.dq_41_rank(after)
        print("  WP-0 DQ-37 before: status=%s count=%s non_converged_total=%s" % (
            b37["status"], b37["count"], b37["values"].get("non_converged_total")))
        print("  WP-0 DQ-41 before: status=%s count=%s %s" % (b41["status"], b41["count"], json.dumps(b41["values"], sort_keys=True)))
        print("  WP-0 DQ-41 after page-rank enforce: status=%s count=%s %s" % (
            a41["status"], a41["count"], json.dumps(a41["values"], sort_keys=True)))
    except Exception as exc:  # noqa: BLE001
        print("  WP-0 leg skipped: %s: %s" % (type(exc).__name__, exc))


# --------------------------------------------------------------------------- #
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--child-off", default=None)
    ap.add_argument("--base-tree", default=os.getenv("TFB_HARNESS_BASE_TREE") or None)
    args = ap.parse_args()
    if args.child_off:
        print("CHILD-JSON " + json.dumps(child_off(args.child_off), sort_keys=True))
        return 0

    sys.path.insert(0, REPO)
    import core.data_engine_v2 as de
    from tests import test_de_f7_scoring_settle as f7
    from tests import test_de_f7_settle_unresolved as unr_mod
    from tests import test_sync_rank_overall_page as srp
    from scripts import run_dashboard_sync as sync
    import routes.analysis_sheet_rows as asr
    from core.analysis import opportunity_builder as ob

    legs: Dict[str, Any] = {}
    status: Dict[str, bool] = {}

    # L1 engine
    l1 = {"corpus": engine_corpus(de, f7, unr_mod, (None, "observe", "enforce"))}
    armed, ok_armed = engine_armed(de, f7, unr_mod)
    l1["armed"] = armed
    off = l1["corpus"]
    same = all(_digest(off["%s|None" % s]["fixtures"]) == _digest(off["%s|%s" % (s, u)]["fixtures"])
               for s in ("observe", "enforce") for u in ("observe", "enforce"))
    obs_tag = [p for p in str(off["enforce|observe"]["synthetic"]["drift"].get("warnings")).split("; ")
               if p.startswith("settle_unresolved:")]
    status["L1"] = ok_armed and same and obs_tag == ["settle_unresolved:observe:pass_limit:p1:ov=80:rc=buy"]
    legs["L1"] = l1

    # L2 page-rank core
    l2 = {}
    ok2 = True
    for page in PAGES:
        for name, builder in (("special", srp.special_matrix), ("batch", srp.batch_matrix)):
            headers, rows = builder(page)
            before = copy.deepcopy(rows)
            o_off, s_off = sync._rank_overall_page_core(headers, rows, "off")
            o_obs, s_obs = sync._rank_overall_page_core(headers, rows, "observe")
            o_enf, s_enf = sync._rank_overall_page_core(headers, rows, "enforce")
            o_twice, s_twice = sync._rank_overall_page_core(headers, o_enf, "enforce")
            ok2 = ok2 and o_off is rows and s_off is None and o_obs is rows and rows == before
            ok2 = ok2 and o_twice == o_enf and s_twice["changed"] == 0
            ri = headers.index("Rank (Overall)")
            l2["%s|%s" % (page, name)] = {"observe": s_obs, "enforce": s_enf, "twice": s_twice,
                                          "ranks": [r[ri] for r in o_enf]}
    status["L2"] = ok2
    legs["L2"] = l2

    # L3 sync wiring (recording transport around the REAL write_table)
    l3 = {}
    ok3 = True
    for page in PAGES:
        headers, rows = srp.special_matrix(page)
        by_mode = {}
        for mode in (None, "observe", "enforce"):
            with pinned_env(TFB_RANK_OVERALL_PAGE=mode, **SYNC_ENV):
                by_mode[str(mode)] = sync_write(sync, page, headers, rows)
        ok3 = ok3 and by_mode["None"]["updates"] == by_mode["observe"]["updates"]
        ok3 = ok3 and by_mode["None"]["appends"] == [] and len(by_mode["observe"]["appends"]) == 1
        ok3 = ok3 and len(by_mode["enforce"]["appends"]) == 1
        ok3 = ok3 and by_mode["observe"]["appends"][0]["row"][4] == "OBSERVE"
        ok3 = ok3 and by_mode["enforce"]["appends"][0]["row"][4] == "OK"
        ok3 = ok3 and by_mode["enforce"]["grid_rank"] == srp._special_expected()
        l3[page] = by_mode
    with pinned_env(TFB_RANK_OVERALL_PAGE="enforce", **SYNC_ENV):
        headers = list(srp.get_sheet_headers("My_Portfolio"))
        pf = sync_write(sync, "My_Portfolio", headers, [srp._blank_row(headers, **{"Symbol": "P.US"})])
    ok3 = ok3 and pf["appends"] == []
    l3["My_Portfolio_enforce"] = pf
    status["L3"] = ok3
    legs["L3"] = l3

    # L4 route + builder (armed corpus)
    tagged = [{"symbol": "A.US", "overall_score": 80.0, "rank_overall": 9, "warnings": "provider_ok"},
              {"symbol": "U.US", "overall_score": 70.0, "rank_overall": 9,
               "warnings": "settle_unresolved:enforce:pass_limit:p3:ov=70:rc=buy"},
              {"symbol": "F.US", "overall_score": 60.0, "rank_overall": 9, "warnings": "f7_settle_failed:non_converged"},
              {"symbol": "L.US", "overall_score": 50.0, "rank_overall": 9, "warnings": "low_data_trust"}]
    with pinned_env():
        asr._apply_global_rank_overall(tagged, "Global_Markets")
    reader = {w: ob._scoring_settlement_failure({"warnings": w}) for w in WARNING_CORPUS + (
        "settle_unresolved:enforce:pass_limit:p3:ov=55.02:rc=hold",
        "settle_unresolved:observe:pass_limit:p3:ov=55.02:rc=hold")}
    l4 = {"route": [r.get("rank_overall") for r in tagged], "builder_reader": reader,
          "route_off": route_off(asr), "builder_off": builder_off(ob)}
    status["L4"] = (l4["route"] == [1, "", "", ""]
                    and reader["settle_unresolved:enforce:pass_limit:p3:ov=55.02:rc=hold"] == "unresolved_pass_limit"
                    and reader["settle_unresolved:observe:pass_limit:p3:ov=55.02:rc=hold"] is None)
    legs["L4"] = l4

    # L5 dual-tree byte-identity with the gates unset
    work = tempfile.mkdtemp(prefix="wp2bc_")
    try:
        base_tree = args.base_tree or build_base_tree(work)
        delivered = run_child(REPO)
        base = run_child(base_tree)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    status["L5"] = (base["digests"] == delivered["digests"] and delivered["engine_off_words_equal"]
                    and delivered["sync_off_words_equal"] and base["engine_off_words_equal"])
    legs["L5"] = {"digests_base": base["digests"], "digests_delivered": delivered["digests"],
                  "versions_base": base["versions"], "versions_delivered": delivered["versions"]}

    for leg in ("L1", "L2", "L3", "L4", "L5"):
        print("%s %s %s" % (leg, "PASS" if status[leg] else "FAIL", _digest(legs[leg])[:16]))
    for sec, dg in sorted(delivered["digests"].items()):
        print("L5 off-digest %-8s base=%s delivered=%s %s" % (sec, base["digests"][sec][:16], dg[:16],
                                                            "EQUAL" if base["digests"][sec] == dg else "DIFF"))
    print("L5 versions base=%s delivered=%s" % (json.dumps(base["versions"], sort_keys=True),
                                               json.dumps(delivered["versions"], sort_keys=True)))
    print("WP2BC-DIGEST sha256=%s engine=%s sync=%s route=%s builder=%s harness=%s" % (
        _digest(legs), de.__version__, sync.SCRIPT_VERSION, asr.ANALYSIS_SHEET_ROWS_VERSION,
        ob.OPPORTUNITY_BUILDER_VERSION, HARNESS_VERSION))
    export = os.getenv("TFB_WP2_EXPORT_PATH")
    if export and os.path.exists(export):
        export_leg(export, sync)
    ok = all(status.values())
    print("RESULT %s" % ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
