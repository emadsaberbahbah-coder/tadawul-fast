"""
scripts/run_weekly_brief.py — TFB Gen-2 Weekly Decision Brief (advisor memo)
=============================================================================
VERSION 1.0.0  (2026-07-18)  — NEW SCRIPT (Wave S, deliverable #12)

WHY (Master Plan v2.1 §18.8, §18.1, §18.4): the weekly decision session gets
an ADVISOR MEMO, not a scorecard — verdict first (switch proposals cleared by
arithmetic, or the first-class NO_ACTION with its reason), then the stamped
Gen-2 board, holdings hold-edges, the regime stamp with its governance line,
and "what changed since last brief". Numbers serve the narrative.

DESIGN: computations are IMPORTED from scripts/run_shadow_board.py (same
mapper, same gate, same cost model, same regime block) so the memo and the
Shadow_Board tab can never disagree. Change-diff persists in a one-cell state
tab `_Brief_State`. Mail mirrors run_daily_brief's proven TFB_MAIL_* chain;
missing mail creds degrade to a logged no-send, never a red run.

HONESTY: SHADOW MODE banner on every memo — Gen-2 moves no capital; the S-1
gate (~Aug 31) decides Tranche 1. p_hit remains a labeled proxy.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

from core import shariah_authority as sa            # noqa: E402
from core.analysis import portfolio_actions as pa   # noqa: E402

_SB_PATH = os.path.join(_ROOT, "scripts", "run_shadow_board.py")
_spec = importlib.util.spec_from_file_location("tfb_shadow_board", _SB_PATH)
sb = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sb)  # type: ignore[union-attr]

# v1.0.1 (2026-07-18): eligibility-coherent scan — BUY proposals restricted
# to Gen2-eligible candidates via shadow_board.eligible_symbols() (§4: a
# blocked name can never be a BUY). Caught reading the first live memo.
# v1.0.2 (2026-07-19): RISK section in the memo — the concentration verdict
# from risk_limits v1.0.0 (via shadow_board v1.1.3's build_risk_block) now
# appears in both the text and HTML memo, so a structural concentration is
# read on Sunday morning rather than discovered after capital moves.
# -----------------------------------------------------------------------------
# v1.0.3 (2026-08-30) - HARD-GATE TERMINAL AT THE RENDER SEAM (audit D-1/D-2)
# -----------------------------------------------------------------------------
# EVIDENCE (2026-08-30 12:54 live brief): 1015.KL rendered Edge Verdict=TRADE
# with a full cost model (RT 0.55%, net edge 8.23%) while its own row said
# Gen2 Eligible=NO / BROKER_UNTRADABLE and the header counted it blocked -
# the F-06 defect class reproduced on the Gen-2 surface: hard blocks arriving
# AFTER scoring instead of terminating it. The verdict is computed upstream in
# shadow_board; this brief owns the last seam both text and HTML flow through
# (key_cols), so v1.0.3 terminalizes there:
#   (1) _hard_gate_rows(): any row whose Gen2 Eligible cell is not YES gets
#       Edge Verdict overwritten to "BLOCKED - <status>" and its edge economics
#       (ROI %, RT Cost %, Net Edge %) suppressed to em-dash. Debt/MCap stays -
#       it IS the block evidence (5110.SR 323.6%). Pure + fail-open: any parse
#       problem returns the row untouched. Upstream shadow_board fix (compute
#       no cost model for blocked names at all) is registered separately.
#   (2) Column label "Shariah Status" renders as "Status": the screening
#       policy was retired 2026-08-13 and the field factually carries
#       tradability/model codes (BROKER_UNTRADABLE, MODEL_SCREEN_*) - a
#       compliance-labeled column holding non-compliance codes is D-2.
#       Authority plumbing untouched this pass (removal rides the
#       shadow_board revision).
# SELFTEST extended with the 1015.KL-shape fixture: blocked row must render
# BLOCKED, never TRADE, with economics suppressed; eligible rows byte-same.
# Functions added: 1 (_hard_gate_rows). Removed: 0.
# -----------------------------------------------------------------------------
# v1.1.0 (2026-11-03) - BOARD PARITY: THE MEMO SHOWS THE BOARD THE
# Shadow_Board TAB SHOWS (TFB_WEEKLY_BOARD_PARITY, default off)
# -----------------------------------------------------------------------------
# EVIDENCE (live, Sunday 1 Nov 2026):
#   11:13:43Z weekly_brief  run 41300907168:
#     "[shariah_authority v1.1.0] load failed: RuntimeError:sheet_id_missing"
#     "[WEEKLY-BRIEF v1.0.3] cands=10 switch=NO_ACTION ... mail=SENT:1"
#   11:47:45Z shadow_board  run 41303426265 (same Top_10, 34 min later):
#     "[SHADOW-BOARD v1.6.0] cands=10 eligible=8 blocked={'FLOOR_LOCKED': 2}
#      switch=SWITCH_CANDIDATES auth_rows=408"
# The owner was mailed NO ACTION while the board proposed switches. The
# DESIGN line above ("so the memo and the Shadow_Board tab can never
# disagree") had stopped being true for two reasons:
#   (1) POSITION. run_shadow_board v1.4.0 (2026-09-13) inserted "ROI Src" at
#       index 16 and moved "Gen2 Eligible" to 17. This script still reads
#       r[16] as Gen2 Eligible in snapshot(), _hard_gate_rows() and the memo
#       key_cols, so EVERY row - TRADE rows included - renders "BLOCKED -
#       <status>" with ROI / RT / Net Edge suppressed, the memo's last column
#       is ROI Src under the header of that name, and _Brief_State saves ROI
#       Src as "gen2". (The switch verdict itself reads the last column through
#       shadow_board.eligible_symbols, which is correct.)
#   (2) LANE. weekly_brief.yml runs the imported board without the
#       shadow_board.yml lane: no TFB_COMPLIANCE_SCREEN_RETIRED (retired
#       2026-08-13; the board hardcodes it), no TFB_BOARD_ENGINE_ROI=enforce
#       (blank Top_10 ROI stays NO_ROI), no TFB_SHADOW_T10_READ_RETRIES, and no
#       DEFAULT_SPREADSHEET_ID (so the authority read fails, as logged). This
#       script also never called the board's engine-ROI fill or settled read.
# Draft PR #795 (2026-10-18, stale, conflicted, 125 files) carries an ungated
# fix for (1) as v1.0.4; this build credits it and takes 1.1.0.
# WHAT (TFB_WEEKLY_BOARD_PARITY = on / 1 / true / yes; anything else = off):
#   * off (default): every read, write, log line, _Run_Log row, state cell
#     and mail byte is v1.0.3's (the version string aside).
#   * on, before anything is read: _apply_board_lane_env() sets each
#     BOARD_LANE_ENV knob that is unset or blank to the shadow_board.yml
#     value, and DEFAULT_SPREADSHEET_ID to the sheet this run opens when it is
#     unset (the shadow lane maps both from the same Variable). A knob already
#     set in this lane is kept.
#   * on, inputs: the Top_10 read goes through shadow_board.read_top10_settled
#     when TFB_SHADOW_T10_READ_RETRIES > 0, and blank ROI is filled through
#     fetch_engine_roi_map / apply_engine_roi when TFB_BOARD_ENGINE_ROI is not
#     off - the calls and the order of shadow_board.main.
#   * on, columns: snapshot_by_header(), _hard_gate_rows_by_header() and the
#     memo columns bind by shadow_board.OUT_HEADER name; a missing name
#     raises, so no memo is mailed on a schema it cannot read.
#   * on, the render seam: a row that is not Gen2-eligible is terminalized
#     as v1.0.3 does ('BLOCKED - <status>', ROI / RT / Net Edge suppressed)
#     when it reads TRADE, when the board already BLOCKED it (verdict kept),
#     or when its own gate evidence says hard block (status outside
#     compliance_gate.INVEST_OK_STATUSES and not GATE_DISABLED, or Floor OK
#     not YES) - the TFB_SB_COST_LEGACY=1 shapes. A compliant row the board
#     calls EDGE_BELOW_COST / NO_ROI / NO_COST_MODEL, and TRADE + YES, read
#     as the tab reads. v1.0.3's rule also relabelled those compliant rows
#     "BLOCKED - <status>" ("BLOCKED - SCREEN_RETIRED" under the retired
#     screen). The saved state (snapshot_by_header) takes the verdict after
#     the seam, so a CHANGES line never shows TRADE for a blocked name.
#   * on, evidence: one "[WEEKLY-BRIEF-PARITY v1.1.0] ..." line, a BOARD LANE
#     line under PROVENANCE (text and HTML), and "board_parity" in the
#     _Run_Log details.
# KNOWN: the first memo after arming diffs against the last v1.0.3 state
# once: a status FLIP per row (model-screen code -> SCREEN_RETIRED) and, for
# most rows, a verdict FLIP too (NO_ROI / BLOCKED - MODEL_SCREEN_* -> TRADE /
# EDGE_BELOW_COST), so flips= reads about twice the row count that Sunday.
# The first memo after a rollback shows the reverse diff once (the gate-off
# code is v1.0.3's; the saved state is the armed one). shadow_board's
# non-trading freeze (TFB_BOARD_FREEZE_*) is not mirrored and cannot be by
# env: this script never calls freeze_verdict, and neither lane sets it (the
# harness fails if shadow_board.yml ever does).
# ROLLBACK: unset the TFB_WEEKLY_BOARD_PARITY Variable (= v1.0.3).
# Functions added: 6 (_board_parity_enabled, _apply_board_lane_env,
# _board_cols, snapshot_by_header, _hard_gate_rows_by_header,
# _board_lane_line). Removed: 0.
# -----------------------------------------------------------------------------
SCRIPT_VERSION = "1.1.0"
TAB_STATE = "_Brief_State"
ENV_BOARD_PARITY = "TFB_WEEKLY_BOARD_PARITY"     # v1.1.0 on|off, default off
# v1.1.0: the shadow_board.yml job-env knobs this memo must share with the
# board (tests/test_weekly_brief_board_parity.py reads that workflow and fails
# when a literal knob there is missing here or differs).
BOARD_LANE_ENV: Tuple[Tuple[str, str], ...] = (
    ("TFB_COMPLIANCE_SCREEN_RETIRED", "1"),
    ("TFB_BOARD_ENGINE_ROI", "enforce"),
    ("TFB_SHADOW_T10_READ_RETRIES", "6"),
)
# v1.1.0: the memo's board columns, by Shadow_Board header name (v1.0.3's
# key_cols [0, 3, 4, 5, 6, 11, 12, 14, 15, 16] as authored on the 17-column
# board).
MEMO_COLS: Tuple[str, ...] = (
    "Symbol", "Sector", "ROI %", "Confidence", "Shariah Status", "RT Cost %",
    "Net Edge %", "Edge Verdict", "Debt/MCap %", "Gen2 Eligible")


def _now_riyadh() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=3)).strftime(
        "%Y-%m-%d %H:%M")


# --------------------------------------------------------------------------- #
# v1.1.0 board parity (TFB_WEEKLY_BOARD_PARITY, default off)                   #
# --------------------------------------------------------------------------- #
def _board_parity_enabled() -> bool:
    """v1.1.0 gate - DEFAULT OFF. on / 1 / true / yes arms it."""
    return (os.getenv(ENV_BOARD_PARITY) or "").strip().lower() in (
        "on", "1", "true", "yes")


def _apply_board_lane_env(sheet_id: Optional[str]) -> Dict[str, str]:
    """v1.1.0: give this process the shadow_board.yml lane before anything is
    read. Each BOARD_LANE_ENV knob that is unset or blank takes the board's
    value (one already set here is kept); DEFAULT_SPREADSHEET_ID, which the
    Shariah authority reader needs, takes the sheet this run opens (the same
    resolution order as shadow_board._open_sheet) when it is unset. -> the
    effective values for the evidence line (the sheet id itself is never
    returned, only where it came from)."""
    out: Dict[str, str] = {}
    for key, value in BOARD_LANE_ENV:
        if not (os.getenv(key) or "").strip():
            os.environ[key] = value
        out[key] = str(os.getenv(key) or "").strip()
    if (os.getenv("DEFAULT_SPREADSHEET_ID") or "").strip():
        out["DEFAULT_SPREADSHEET_ID"] = "preset"
    else:
        sid = None
        for v in (sheet_id, os.getenv("TARGET_SHEET_ID"),
                  os.getenv("TRACK_SHEET_ID")):
            if v and str(v).strip():
                sid = str(v).strip()
                break
        if sid:
            os.environ["DEFAULT_SPREADSHEET_ID"] = sid
            out["DEFAULT_SPREADSHEET_ID"] = "this_sheet"
        else:
            out["DEFAULT_SPREADSHEET_ID"] = "missing"
    # the Shariah authority reader takes TFB_SHARIAH_SHEET_ID first
    out["authority_sheet"] = ("shariah_sheet_id"
                              if (os.getenv("TFB_SHARIAH_SHEET_ID") or "").strip()
                              else out["DEFAULT_SPREADSHEET_ID"])
    return out


def _board_cols(*names: str) -> List[int]:
    """v1.1.0: column indexes by Shadow_Board header name (the producer's own
    OUT_HEADER), never by position. A name the board does not carry raises
    ValueError, so a memo is never mailed on a schema it cannot read."""
    return [sb.OUT_HEADER.index(n) for n in names]


def _board_lane_line(lane: Dict[str, Any]) -> str:
    """v1.1.0: the BOARD LANE evidence text shared by the log line, the
    memo and the _Run_Log details. Pure."""
    roi = lane.get("engine_roi") or {}
    t10 = lane.get("t10_read") or {}
    env = lane.get("env") or {}
    return ("lane=shadow_board"
            f" screen_retired={env.get('TFB_COMPLIANCE_SCREEN_RETIRED', '')}"
            f" engine_roi={roi.get('mode', 'off')}"
            f" applied={roi.get('applied', 0)}/{roi.get('blank', 0)}"
            f" unresolved={roi.get('unresolved', 0)}"
            f" roi_errors={roi.get('errors', 0)}"
            f" t10_read={t10.get('first', 'single')}->{t10.get('final', 'single')}"
            f"/{t10.get('retries', 0)}"
            f" authority_sheet={env.get('authority_sheet', '')}")


# --------------------------------------------------------------------------- #
# state + diff (pure)                                                          #
# --------------------------------------------------------------------------- #
def snapshot(rows: List[List[Any]], scan_verdict: str) -> Dict[str, Any]:
    """Board rows -> compact state: {sym: {status, verdict, gen2}}."""
    return {"date": _now_riyadh(),
            "switch": scan_verdict,
            "board": {r[0]: {"status": r[6], "verdict": r[14], "gen2": r[16]}
                      for r in rows}}


def snapshot_by_header(rows: List[List[Any]],
                       scan_verdict: str) -> Dict[str, Any]:
    """v1.1.0 (parity on): snapshot() with the four fields bound by
    Shadow_Board header name, so "gen2" is Gen2 Eligible, not ROI Src. The
    verdict is the one the memo shows (after _hard_gate_rows_by_header), so
    a CHANGES line never says a blocked name turned TRADE."""
    sym, status, verdict, gen2 = _board_cols(
        "Symbol", "Shariah Status", "Edge Verdict", "Gen2 Eligible")
    rows = _hard_gate_rows_by_header(rows)
    return {"date": _now_riyadh(),
            "switch": scan_verdict,
            "board": {r[sym]: {"status": r[status], "verdict": r[verdict],
                               "gen2": r[gen2]}
                      for r in rows}}


def compute_diff(prev: Optional[Dict[str, Any]],
                 cur: Dict[str, Any]) -> Dict[str, Any]:
    if not prev or not prev.get("board"):
        return {"first": True, "entered": sorted(cur["board"]),
                "exited": [], "flips": []}
    p, c = prev["board"], cur["board"]
    entered = sorted(set(c) - set(p))
    exited = sorted(set(p) - set(c))
    flips = [{"symbol": s, "from": p[s]["verdict"], "to": c[s]["verdict"]}
             for s in sorted(set(p) & set(c))
             if p[s]["verdict"] != c[s]["verdict"]]
    flips += [{"symbol": s, "from": p[s]["status"], "to": c[s]["status"]}
              for s in sorted(set(p) & set(c))
              if p[s]["status"] != c[s]["status"]]
    return {"first": False, "entered": entered, "exited": exited,
            "flips": flips, "prev_date": prev.get("date", "")}


def load_state(sh) -> Optional[Dict[str, Any]]:
    try:
        ws = sh.worksheet(TAB_STATE)
        raw = ws.acell("A2").value
        return json.loads(raw) if raw else None
    except Exception:  # noqa: BLE001
        return None


def save_state(sh, state: Dict[str, Any]) -> None:
    try:
        try:
            ws = sh.worksheet(TAB_STATE)
        except Exception:  # noqa: BLE001
            ws = sh.add_worksheet(title=TAB_STATE, rows=4, cols=2)
        ws.update(values=[["Weekly Brief State (JSON, machine-managed)"],
                          [json.dumps(state, ensure_ascii=False)]],
                  range_name="A1")
    except Exception:  # noqa: BLE001
        pass


# --------------------------------------------------------------------------- #
# memo rendering (pure)                                                        #
# --------------------------------------------------------------------------- #
def _hard_gate_rows(rows: List[List[Any]]) -> List[List[Any]]:
    """v1.0.3: terminalize hard blocks at the render seam. A row whose
    Gen2 Eligible cell (index 16) is not YES gets Edge Verdict (14)
    overwritten to 'BLOCKED - <status(6)>' and ROI/RT/Net Edge (4/11/12)
    suppressed. Pure (copies), FAIL-OPEN (any error -> row untouched)."""
    out: List[List[Any]] = []
    for r in rows or []:
        try:
            if (isinstance(r, list) and len(r) >= 17
                    and str(r[16] or "").strip().upper() != "YES"):
                c = list(r)
                status = str(c[6] or "").strip() or "INELIGIBLE"
                c[14] = "BLOCKED — " + status
                c[4] = c[11] = c[12] = "—"
                out.append(c)
                continue
        except Exception:  # noqa: BLE001
            pass
        out.append(r)
    return out


def _hard_gate_rows_by_header(rows: List[List[Any]]) -> List[List[Any]]:
    """v1.1.0 (parity on): the render seam on the current board schema,
    columns bound by header name. A row that is not Gen2-eligible is
    terminalized as v1.0.3 does ('BLOCKED - <status>', ROI / RT / Net Edge
    suppressed) when it reads TRADE, when the board already BLOCKED it (its
    verdict is kept), or when its own gate evidence says hard block: a status
    outside compliance_gate.INVEST_OK_STATUSES (GATE_DISABLED aside) or Floor
    OK not YES - the rows TFB_SB_COST_LEGACY=1 scores. Every other row (a
    compliant EDGE_BELOW_COST / NO_ROI / NO_COST_MODEL, TRADE + YES) reads as
    the Shadow_Board tab reads it. Pure (copies), FAIL-OPEN per row like
    v1.0.3; a missing header name raises (see _board_cols)."""
    gen2_c, status_c, verdict_c, roi_c, rt_c, ne_c, floor_c = _board_cols(
        "Gen2 Eligible", "Shariah Status", "Edge Verdict", "ROI %",
        "RT Cost %", "Net Edge %", "Floor OK")
    need = max(gen2_c, status_c, verdict_c, roi_c, rt_c, ne_c, floor_c)
    ok_statuses = set(getattr(sb.cg, "INVEST_OK_STATUSES", ()))
    gate_off = getattr(sb.cg, "GATE_DISABLED", "GATE_DISABLED")
    out: List[List[Any]] = []
    for r in rows or []:
        try:
            if (isinstance(r, list) and len(r) > need
                    and str(r[gen2_c] or "").strip().upper() != "YES"):
                verdict = str(r[verdict_c] or "").strip()
                st = str(r[status_c] or "").strip()
                hard = not (st == gate_off or (
                    st in ok_statuses
                    and str(r[floor_c] or "").strip().upper() == "YES"))
                if (verdict == "TRADE" or hard
                        or verdict.upper().startswith("BLOCKED")):
                    c = list(r)
                    if not verdict.upper().startswith("BLOCKED"):
                        status = st or "INELIGIBLE"
                        c[verdict_c] = "BLOCKED \u2014 " + status
                    c[roi_c] = c[rt_c] = c[ne_c] = "\u2014"
                    out.append(c)
                    continue
        except Exception:  # noqa: BLE001
            pass
        out.append(r)
    return out


def _verdict_headline(scan: Dict[str, Any]) -> Tuple[str, str]:
    if scan["verdict"] == "NO_ACTION":
        return ("لا فعل هذا الأسبوع — الإبقاء على كل المراكز هو التوصية",
                "NO ACTION this week: no candidate beats any holding by "
                "2xRT + 1% after costs. Keeping everything is the "
                "recommendation (§18.4).")
    props = scan["proposals"]
    ar = f"{len(props)} اقتراح استبدال تجاوز عتبة التكاليف — راجع الجدول"
    lines = "; ".join(f"SELL {p['sell']} → BUY {p['buy']} "
                      f"(Δ{p['delta_pct']}% vs hurdle {p['hurdle_pct']}%)"
                      for p in props)
    return ar, "SWITCH candidates cleared the arithmetic: " + lines

def render_memo(rows, summary, scan, regime, ameta, fnd_n, cand_n,
                diff, equity, risk=None, lane=None) -> Tuple[str, str, str]:
    """-> (subject, text, html). v1.1.0: lane (parity on only) binds the seam
    and the columns by header name and adds the BOARD LANE line; lane=None
    is v1.0.3 byte for byte."""
    if lane is None:
        rows = _hard_gate_rows(rows)  # v1.0.3: blocks terminate before display
    else:
        rows = _hard_gate_rows_by_header(rows)  # v1.1.0: by header name
    ar, en = _verdict_headline(scan)
    subject = (f"TFB Weekly Decision Brief — {_now_riyadh()[:10]} — "
               f"{scan['verdict']} — SHADOW MODE")
    if lane is None:
        key_cols = [0, 3, 4, 5, 6, 11, 12, 14, 15, 16]
    else:
        key_cols = _board_cols(*MEMO_COLS)  # v1.1.0
    key_hdr = [sb.OUT_HEADER[i] for i in key_cols]
    # v1.0.3 D-2: retired-policy label; the field carries tradability codes.
    key_hdr = ["Status" if h == "Shariah Status" else h for h in key_hdr]

    L: List[str] = []
    L.append("TFB GEN-2 WEEKLY DECISION BRIEF (SHADOW MODE)")
    L.append(f"As of {_now_riyadh()} Riyadh | equity model {int(equity):,} SAR")
    L.append("Gen-2 moves NO capital. Gate S-1 (~Aug 31) decides Tranche 1.")
    L.append("")
    L.append("VERDICT | " + ar)
    L.append(en)
    L.append("")
    L.append(f"BOARD ({cand_n} candidates | compliance-eligible "
             f"{summary['compliance_eligible']} | blocked "
             f"{json.dumps(summary['blocked'])})")
    L.append(" | ".join(key_hdr))
    for r in rows:
        L.append(" | ".join(str(r[i]) for i in key_cols))
    L.append("")
    if diff.get("first"):
        L.append("CHANGES | first brief — baseline recorded.")
    else:
        L.append(f"CHANGES since {diff.get('prev_date', '')}: "
                 f"entered={diff['entered'] or '-'} "
                 f"exited={diff['exited'] or '-'}")
        for f in diff["flips"]:
            L.append(f"  FLIP {f['symbol']}: {f['from']} -> {f['to']}")
    L.append("")
    if risk:
        L.append(f"RISK | {risk.get('verdict')} — heat naive "
                 f"{risk.get('naive_heat_pct')}% -> effective "
                 f"{risk.get('effective_heat_pct')}% "
                 f"(diversification ratio {risk.get('diversification_ratio')})")
        L.append("  country: " + json.dumps(risk.get("country") or {}))
        for br in (risk.get("breaches") or []):
            L.append(f"  BREACH {br}")
        if risk.get("note"):
            L.append("  " + risk["note"])
        L.append("")
    L.append("REGIME | " + json.dumps(regime.get("sleeves", {}), default=str))
    L.append("Suggested weights: "
             + json.dumps(regime.get("suggested_weights") or {}))
    L.append(regime.get("governance", ""))
    L.append("")
    L.append(f"PROVENANCE | authority rows={ameta.get('rows', 0)} "
             f"as_of={ameta.get('as_of')} err={sa.last_error() or '-'} | "
             f"fundamentals {fnd_n}/{cand_n} | p_hit = confidence proxy "
             f"until the calibrator graduates")
    if lane is not None:                       # v1.1.0 board parity evidence
        L.append("BOARD LANE | " + _board_lane_line(lane))
    text = "\n".join(L)

    def esc(x: Any) -> str:
        return (str(x).replace("&", "&amp;").replace("<", "&lt;")
                .replace(">", "&gt;"))
    lane_html = ""
    if lane is not None:                       # v1.1.0 board parity evidence
        lane_html = "board " + esc(_board_lane_line(lane)) + " &middot;\n"
    tr = "".join
    board_html = tr(
        ["<tr>" + tr(f"<th style='border:1px solid #ccc;padding:4px'>{esc(h)}</th>"
                     for h in key_hdr) + "</tr>"] +
        ["<tr>" + tr(f"<td style='border:1px solid #ccc;padding:4px'>{esc(r[i])}</td>"
                     for i in key_cols) + "</tr>" for r in rows])
    flips_html = "".join(f"<li>FLIP {esc(f['symbol'])}: {esc(f['from'])} → "
                         f"{esc(f['to'])}</li>" for f in diff.get("flips", []))
    changes_html = ("<p>First brief — baseline recorded.</p>"
                    if diff.get("first") else
                    f"<p>Since {esc(diff.get('prev_date', ''))}: entered "
                    f"{esc(diff['entered'] or '-')} · exited "
                    f"{esc(diff['exited'] or '-')}</p><ul>{flips_html}</ul>")
    html = f"""<html><body style='font-family:Arial,sans-serif;color:#222'>
<h2>TFB Gen-2 Weekly Decision Brief <span style='color:#b30000'>(SHADOW MODE)</span></h2>
<p>As of {esc(_now_riyadh())} Riyadh · equity model {int(equity):,} SAR<br>
<b>Gen-2 moves no capital.</b> Gate S-1 (~Aug 31) decides Tranche&nbsp;1.</p>
<h3>الحكم | Verdict</h3><p><b>{esc(ar)}</b><br>{esc(en)}</p>
<h3>Board — {cand_n} candidates (eligible {summary['compliance_eligible']},
blocked {esc(json.dumps(summary['blocked']))})</h3>
<table style='border-collapse:collapse;font-size:12px'>{board_html}</table>
<h3>Changes</h3>{changes_html}
<h3>Risk &amp; Concentration</h3>
<p><b>{esc(risk.get('verdict')) if risk else 'n/a'}</b> — heat
{esc(risk.get('naive_heat_pct')) if risk else '-'}% naive →
<b>{esc(risk.get('effective_heat_pct')) if risk else '-'}%</b> effective
(diversification ratio {esc(risk.get('diversification_ratio')) if risk else '-'})<br>
country: {esc(json.dumps((risk or {}).get('country') or {}))}<br>
{"".join(f"<span style='color:#b30000'>BREACH {esc(x)}</span><br>" for x in ((risk or {}).get('breaches') or []))}
<i>{esc((risk or {}).get('note',''))}</i></p>
<h3>Regime</h3><p>{esc(json.dumps(regime.get('sleeves', {}), default=str))}<br>
Suggested weights: {esc(json.dumps(regime.get('suggested_weights') or {}))}<br>
<i>{esc(regime.get('governance', ''))}</i></p>
<p style='color:#666;font-size:11px'>authority rows={ameta.get('rows', 0)}
as_of={esc(ameta.get('as_of'))} err={esc(sa.last_error() or '-')} ·
fundamentals {fnd_n}/{cand_n} · p_hit is a labeled proxy ·
{lane_html}run_weekly_brief v{SCRIPT_VERSION}</p></body></html>"""
    return subject, text, html


# --------------------------------------------------------------------------- #
# mail (mirrors run_daily_brief env chain)                                     #
# --------------------------------------------------------------------------- #
def send_mail(subject: str, text: str, html: str) -> str:
    host = (os.getenv("TFB_MAIL_SMTP_HOST") or "smtp.gmail.com").strip()
    port = int((os.getenv("TFB_MAIL_SMTP_PORT") or "587").strip())
    user = (os.getenv("TFB_MAIL_FROM") or "").strip()
    password = (os.getenv("TFB_MAIL_PASSWORD") or "").strip()
    to = [t.strip() for t in (os.getenv("TFB_MAIL_TO") or "").split(",")
          if t.strip()]
    if not (user and password and to):
        return "NO_MAIL_CREDS"
    import smtplib
    from email.message import EmailMessage
    msg = EmailMessage()
    msg["Subject"], msg["From"], msg["To"] = subject, user, ", ".join(to)
    msg.set_content(text)
    msg.add_alternative(html, subtype="html")
    if port == 465:
        with smtplib.SMTP_SSL(host, port, timeout=30) as s:
            s.login(user, password)
            s.send_message(msg)
    else:
        with smtplib.SMTP(host, port, timeout=30) as s:
            s.starttls()
            s.login(user, password)
            s.send_message(msg)
    return f"SENT:{len(to)}"


# --------------------------------------------------------------------------- #
# main                                                                         #
# --------------------------------------------------------------------------- #
def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--dry-run", action="store_true",
                    help="compute + print text memo; no mail, no state save")
    ap.add_argument("--no-mail", action="store_true")
    ap.add_argument("--sheet-id")
    args = ap.parse_args(argv)
    if args.selftest:
        return _selftest()

    parity = _board_parity_enabled()                     # v1.1.0
    lane: Optional[Dict[str, Any]] = None
    if parity:
        lane = {"env": _apply_board_lane_env(args.sheet_id)}

    os.environ.setdefault("TFB_COMPLIANCE_GATE_ENABLED", "1")
    equity = float(os.getenv("TFB_SHADOW_EQUITY_SAR") or "130000")

    sh = sb._open_sheet(args.sheet_id)
    if parity and sb._t10_read_retries() > 0:            # v1.1.0 settled read
        _t10_vals, cands, t10_info = sb.read_top10_settled(
            lambda: sh.worksheet(sb.TAB_TOP10).get_all_values(),
            sb._t10_read_retries(), sb._t10_read_wait_s())
        lane["t10_read"] = dict(t10_info)
    else:
        cands = sb.rows_to_records(sh.worksheet(sb.TAB_TOP10).get_all_values())
    try:
        holds = sb.rows_to_records(sh.worksheet(sb.TAB_HOLDINGS).get_all_values())
    except Exception:  # noqa: BLE001
        holds = []

    auth = sa.get_authority_index(force=True)
    monitor = sa.get_monitor_map()
    ameta = sa.get_meta()
    if parity:                                           # v1.1.0 engine ROI
        roi_mode = sb._engine_roi_mode()
        roi_counters = {"blank": 0, "applied": 0, "fraction_fixed": 0,
                        "unresolved": 0}
        roi_errs: List[str] = []
        if roi_mode != "off":
            blanks = [c["symbol"] for c in cands if c.get("roi_pct") is None]
            roi_map, roi_errs = sb.fetch_engine_roi_map(sh, blanks) \
                if blanks else ({}, [])
            roi_counters = sb.apply_engine_roi(cands, roi_map, roi_mode)
        lane["engine_roi"] = dict(roi_counters, mode=roi_mode,
                                  errors=len(roi_errs))
    fnd, _fnd_errs = sb.fetch_board_fundamentals([c["symbol"] for c in cands])
    rows, summary = sb.evaluate_board(cands, auth, monitor, equity, fnd)
    ok = sb.eligible_symbols(rows)
    scan = pa.advisor_switch_scan(
        holds, [c for c in cands if c["symbol"] in ok])
    regime = sb.build_regime_block()

    prev = load_state(sh)
    if parity:
        cur = snapshot_by_header(rows, scan["verdict"])  # v1.1.0
    else:
        cur = snapshot(rows, scan["verdict"])
    diff = compute_diff(prev, cur)
    risk = sb.build_risk_block(cands, ok, fnd)
    subject, text, html = render_memo(rows, summary, scan, regime, ameta,
                                      len(fnd), len(cands), diff, equity, risk,
                                      lane=lane)

    verdict = (f"[WEEKLY-BRIEF v{SCRIPT_VERSION}] cands={len(cands)} "
               f"switch={scan['verdict']} entered={len(diff['entered'])} "
               f"exited={len(diff['exited'])} flips={len(diff.get('flips', []))}")
    if lane is not None:                                 # v1.1.0 evidence
        print(f"[WEEKLY-BRIEF-PARITY v{SCRIPT_VERSION}] "
              + _board_lane_line(lane)
              + f" eligible={summary['compliance_eligible']}"
              + f" switch={scan['verdict']}")
    if args.dry_run:
        print(verdict)
        print(text)
        return 0

    mail_status = "SKIPPED" if args.no_mail else send_mail(subject, text, html)
    save_state(sh, cur)
    print(verdict + f" mail={mail_status}")
    _rl_details: Dict[str, Any] = {"version": SCRIPT_VERSION}
    if lane is not None:                                 # v1.1.0 evidence
        _rl_details["board_parity"] = _board_lane_line(lane)
    try:
        sh.worksheet("_Run_Log").append_row(
            [datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "INFO",
             "weekly_brief", "Shadow_Board", "OK",
             verdict + f" mail={mail_status}", "", "", "",
             json.dumps(_rl_details)],
            value_input_option="RAW")
    except Exception:  # noqa: BLE001
        pass
    return 0


# --------------------------------------------------------------------------- #
# SELFTEST (offline)                                                           #
# --------------------------------------------------------------------------- #
def _selftest() -> int:
    checks: List[Tuple[str, bool]] = []
    r1 = ["7010.SR", "STC", "", "Telecom", 14.5, "High", "AUTHORITY_PASS",
          "AL_RAJHI_OFFICIAL", "BROKER_TRADABLE", "TASI", "YES", 0.36, 8.5,
          1.5, "TRADE", 12.1, "YES"]
    r2 = ["2269.T", "Meiji", "", "Staples", 17.5, "High",
          "MODEL_SCREEN_PASS_NOT_FATWA", "MODEL_SCREEN_RAJHI_STYLE",
          "BROKER_TRADABLE", "", "YES", 0.6, 10.7, 1.8, "TRADE", 18.2, "YES"]
    rows = [r1, r2]
    cur = snapshot(rows, "NO_ACTION")
    checks.append(("snapshot shape", cur["board"]["7010.SR"]["gen2"] == "YES"
                   and cur["board"]["2269.T"]["status"].startswith("MODEL")))
    d0 = compute_diff(None, cur)
    checks.append(("first-brief baseline", d0["first"] and
                   d0["entered"] == ["2269.T", "7010.SR"]))
    prev = {"date": "2026-07-11 08:00", "switch": "NO_ACTION",
            "board": {"7010.SR": {"status": "AUTHORITY_PASS",
                                  "verdict": "EDGE_BELOW_COST", "gen2": "NO"},
                      "9999.HK": {"status": "UNKNOWN", "verdict": "TRADE",
                                  "gen2": "NO"}}}
    d1 = compute_diff(prev, cur)
    checks.append(("diff: entered/exited/flip detected",
                   d1["entered"] == ["2269.T"] and d1["exited"] == ["9999.HK"]
                   and any(f["symbol"] == "7010.SR"
                           and f["to"] == "TRADE" for f in d1["flips"])))
    scan_na = {"verdict": "NO_ACTION", "proposals": [], "pairs_checked": 16,
               "note": "no candidate beats any holding"}
    regime = {"sleeves": {"Global": {"state": "RISK_ON"}},
              "suggested_weights": {"Global": 0.7, "Saudi": 0.3, "Cash": 0.0},
              "governance": "advisory stamp — drives nothing until gated"}
    risk_fx = {"verdict": "BREACH", "naive_heat_pct": 3.75,
               "effective_heat_pct": 2.51, "diversification_ratio": 0.6693,
               "country": {"Japan": 60.0, "USA": 40.0},
               "breaches": ["Japan 60.0% > cap 40.0%"],
               "note": "caps CANNOT be met by trimming this book"}
    subject, text, html = render_memo(
        rows, {"compliance_eligible": 2, "blocked": {}}, scan_na, regime,
        {"rows": 137, "as_of": "2026-07-15"}, 2, 2, d1, 130000, risk_fx)
    checks.append(("memo carries the RISK section with breach + note",
                   "RISK | BREACH" in text and "BREACH Japan 60.0%" in text
                   and "CANNOT be met" in text))
    checks.append(("risk section sits before regime in the memo",
                   text.index("RISK |") < text.index("REGIME |")))
    checks.append(("subject carries verdict + shadow banner",
                   "NO_ACTION" in subject and "SHADOW MODE" in subject))
    checks.append(("text memo: verdict-first + governance + provenance",
                   text.index("VERDICT") < text.index("BOARD")
                   and "drives nothing" in text
                   and "authority rows=137" in text
                   and "FLIP 7010.SR" in text))
    checks.append(("NO_ACTION phrased as first-class recommendation",
                   "Keeping everything is the recommendation" in text
                   and "لا فعل" in text))
    checks.append(("brief binds the shared eligibility filter",
                   sb.eligible_symbols([r1, r2]) == {"7010.SR", "2269.T"}))
    scan_sw = {"verdict": "SWITCH_CANDIDATES", "pairs_checked": 8,
               "proposals": [{"sell": "1120.SR", "buy": "STNG.US",
                              "delta_pct": 5.7, "hurdle_pct": 1.72}]}
    _, t2, h2 = render_memo(rows, {"compliance_eligible": 2, "blocked": {}},
                            scan_sw, regime, {"rows": 0, "as_of": None},
                            2, 2, d0, 130000)
    checks.append(("switch proposal rendered with delta vs hurdle",
                   "SELL 1120.SR" in t2 and "Δ5.7%" in t2
                   and "hurdle 1.72%" in t2))
    checks.append(("html escapes + contains board table",
                   "&lt;" not in h2.split("<table")[0]
                   and "MODEL_SCREEN_PASS_NOT_FATWA" in h2))
    # v1.0.3: the 1015.KL-shape fixture - blocked must terminalize.
    r3 = ["1015.KL", "AMMB", "", "Financial Services", 13.5, "High",
          "BROKER_UNTRADABLE", "SHADOW", "BROKER_UNTRADABLE", "KLSE", "NO",
          0.55, 8.23, 1.2, "TRADE", 109.2, "NO"]
    _, t3, h3 = render_memo(rows + [r3],
                            {"compliance_eligible": 2,
                             "blocked": {"BROKER_UNTRADABLE": 1}},
                            scan_na, regime, {"rows": 0, "as_of": None},
                            2, 3, d0, 130000)
    line1015 = next(l for l in t3.splitlines() if l.startswith("1015.KL"))
    checks.append(("blocked row terminalized: BLOCKED verdict, no TRADE",
                   "BLOCKED — BROKER_UNTRADABLE" in line1015
                   and "TRADE" not in line1015.replace("BROKER_UNTRADABLE", "")))
    checks.append(("blocked row economics suppressed, Debt/MCap kept",
                   line1015.count("—") >= 3 and "109.2" in line1015))
    checks.append(("eligible rows untouched by the gate",
                   any(l.startswith("7010.SR") and "TRADE" in l
                       for l in t3.splitlines())))
    checks.append(("retired label renders as Status (text+html)",
                   "Shariah Status" not in t3 and "Shariah Status" not in h3
                   and "Status" in t3 and "BLOCKED — BROKER_UNTRADABLE" in h3))
    # v1.1.0: board parity on the current 18-column board (ROI Src at 16).
    # b2 = a compliant EDGE_BELOW_COST row; b3 = TRADE + NO (legacy cost);
    # b4 = the production blocked shape (evaluate_board v1.3.0: no cost
    # model, verdict BLOCKED - <status>); b5 = a legacy-scored hard block
    # (BROKER_UNTRADABLE, EDGE_BELOW_COST, Gen2 NO).
    dash = "\u2014"
    b1 = r1[:16] + ["engine", "YES"]
    b2 = (r2[:11] + [0.6, 1.2, 1.8, "EDGE_BELOW_COST", 18.2]
          + ["", "NO"])
    b3 = r3[:16] + ["", "NO"]
    b4 = (["1016.KL", "Maybank", "", "Financial Services", 11.0, "High",
           "BROKER_UNTRADABLE", "SHADOW", "BROKER_UNTRADABLE", "KLSE", "NO",
           "", "", "", "BLOCKED " + dash + " BROKER_UNTRADABLE", 95.0]
          + ["", "NO"])
    b5 = (["1017.KL", "CIMB", "", "Financial Services", 2.5, "High",
           "BROKER_UNTRADABLE", "SHADOW", "BROKER_UNTRADABLE", "KLSE", "NO",
           0.55, 1.08, 1.65, "EDGE_BELOW_COST", 88.0] + ["", "NO"])
    lane_fx = {"env": {"TFB_COMPLIANCE_SCREEN_RETIRED": "1",
                       "DEFAULT_SPREADSHEET_ID": "this_sheet",
                       "authority_sheet": "this_sheet"},
               "engine_roi": {"mode": "enforce", "applied": 1, "blank": 1,
                              "unresolved": 0, "errors": 0}}
    _, t4, h4 = render_memo([b1, b2, b3, b4, b5],
                            {"compliance_eligible": 2,
                             "blocked": {"BROKER_UNTRADABLE": 3}},
                            scan_na, regime, {"rows": 0, "as_of": None},
                            2, 5, d0, 130000, lane=lane_fx)
    lines4 = {l.split(" | ")[0]: l for l in t4.splitlines()
              if l.startswith(("7010.SR", "2269.T", "1015.KL", "1016.KL",
                               "1017.KL"))}
    _econ_dashed = (lambda line: [line.split(" | ")[i] for i in (2, 5, 6)]
                    == [dash] * 3)
    checks.append(("v1.1.0 parity: columns bound by name (Gen2 Eligible, "
                   "not ROI Src); TRADE + YES row untouched",
                   "Gen2 Eligible" in t4 and "| ROI Src" not in t4
                   and lines4["7010.SR"].endswith("| TRADE | 12.1 | YES")))
    checks.append(("v1.1.0 parity: compliant EDGE_BELOW_COST row reads as "
                   "the board; TRADE + NO terminalized, economics dashed",
                   "| 17.5 | High | MODEL_SCREEN_PASS_NOT_FATWA | 0.6 | 1.2 | "
                   "EDGE_BELOW_COST |" in lines4["2269.T"]
                   and "BLOCKED " + dash + " BROKER_UNTRADABLE" in lines4["1015.KL"]
                   and _econ_dashed(lines4["1015.KL"])
                   and "BOARD LANE | lane=shadow_board" in t4
                   and "board lane=shadow_board" in h4))
    checks.append(("v1.1.0 parity: a board-BLOCKED row keeps its verdict, "
                   "economics dashed; a legacy-scored hard block is "
                   "terminalized",
                   "BLOCKED " + dash + " BROKER_UNTRADABLE" in lines4["1016.KL"]
                   and _econ_dashed(lines4["1016.KL"])
                   and "EDGE_BELOW_COST" not in lines4["1017.KL"]
                   and "BLOCKED " + dash + " BROKER_UNTRADABLE" in lines4["1017.KL"]
                   and _econ_dashed(lines4["1017.KL"])))
    snap4 = snapshot_by_header([b1, b2, b3], "NO_ACTION")
    checks.append(("v1.1.0 parity: state gen2 is Gen2 Eligible; state "
                   "verdict is the memo's (never TRADE for a block)",
                   snap4["board"]["7010.SR"]["gen2"] == "YES"
                   and snap4["board"]["2269.T"]["gen2"] == "NO"
                   and snap4["board"]["1015.KL"]["verdict"]
                   == "BLOCKED " + dash + " BROKER_UNTRADABLE"
                   and dict(BOARD_LANE_ENV).get(
                       "TFB_COMPLIANCE_SCREEN_RETIRED") == "1"))
    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[weekly_brief v{SCRIPT_VERSION}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    sys.exit(main())
