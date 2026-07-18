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

SCRIPT_VERSION = "1.0.0"
TAB_STATE = "_Brief_State"


def _now_riyadh() -> str:
    return (datetime.now(timezone.utc) + timedelta(hours=3)).strftime(
        "%Y-%m-%d %H:%M")


# --------------------------------------------------------------------------- #
# state + diff (pure)                                                          #
# --------------------------------------------------------------------------- #
def snapshot(rows: List[List[Any]], scan_verdict: str) -> Dict[str, Any]:
    """Board rows -> compact state: {sym: {status, verdict, gen2}}."""
    return {"date": _now_riyadh(),
            "switch": scan_verdict,
            "board": {r[0]: {"status": r[6], "verdict": r[14], "gen2": r[16]}
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
                diff, equity) -> Tuple[str, str, str]:
    """-> (subject, text, html)."""
    ar, en = _verdict_headline(scan)
    subject = (f"TFB Weekly Decision Brief — {_now_riyadh()[:10]} — "
               f"{scan['verdict']} — SHADOW MODE")
    key_cols = [0, 3, 4, 5, 6, 11, 12, 14, 15, 16]
    key_hdr = [sb.OUT_HEADER[i] for i in key_cols]

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
    L.append("REGIME | " + json.dumps(regime.get("sleeves", {}), default=str))
    L.append("Suggested weights: "
             + json.dumps(regime.get("suggested_weights") or {}))
    L.append(regime.get("governance", ""))
    L.append("")
    L.append(f"PROVENANCE | authority rows={ameta.get('rows', 0)} "
             f"as_of={ameta.get('as_of')} err={sa.last_error() or '-'} | "
             f"fundamentals {fnd_n}/{cand_n} | p_hit = confidence proxy "
             f"until the calibrator graduates")
    text = "\n".join(L)

    def esc(x: Any) -> str:
        return (str(x).replace("&", "&amp;").replace("<", "&lt;")
                .replace(">", "&gt;"))
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
<h3>Regime</h3><p>{esc(json.dumps(regime.get('sleeves', {}), default=str))}<br>
Suggested weights: {esc(json.dumps(regime.get('suggested_weights') or {}))}<br>
<i>{esc(regime.get('governance', ''))}</i></p>
<p style='color:#666;font-size:11px'>authority rows={ameta.get('rows', 0)}
as_of={esc(ameta.get('as_of'))} err={esc(sa.last_error() or '-')} ·
fundamentals {fnd_n}/{cand_n} · p_hit is a labeled proxy ·
run_weekly_brief v{SCRIPT_VERSION}</p></body></html>"""
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

    os.environ.setdefault("TFB_COMPLIANCE_GATE_ENABLED", "1")
    equity = float(os.getenv("TFB_SHADOW_EQUITY_SAR") or "130000")

    sh = sb._open_sheet(args.sheet_id)
    cands = sb.rows_to_records(sh.worksheet(sb.TAB_TOP10).get_all_values())
    try:
        holds = sb.rows_to_records(sh.worksheet(sb.TAB_HOLDINGS).get_all_values())
    except Exception:  # noqa: BLE001
        holds = []

    auth = sa.get_authority_index(force=True)
    monitor = sa.get_monitor_map()
    ameta = sa.get_meta()
    fnd, _fnd_errs = sb.fetch_board_fundamentals([c["symbol"] for c in cands])
    rows, summary = sb.evaluate_board(cands, auth, monitor, equity, fnd)
    scan = pa.advisor_switch_scan(holds, cands)
    regime = sb.build_regime_block()

    prev = load_state(sh)
    cur = snapshot(rows, scan["verdict"])
    diff = compute_diff(prev, cur)
    subject, text, html = render_memo(rows, summary, scan, regime, ameta,
                                      len(fnd), len(cands), diff, equity)

    verdict = (f"[WEEKLY-BRIEF v{SCRIPT_VERSION}] cands={len(cands)} "
               f"switch={scan['verdict']} entered={len(diff['entered'])} "
               f"exited={len(diff['exited'])} flips={len(diff.get('flips', []))}")
    if args.dry_run:
        print(verdict)
        print(text)
        return 0

    mail_status = "SKIPPED" if args.no_mail else send_mail(subject, text, html)
    save_state(sh, cur)
    print(verdict + f" mail={mail_status}")
    try:
        sh.worksheet("_Run_Log").append_row(
            [datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"), "INFO",
             "weekly_brief", "Shadow_Board", "OK",
             verdict + f" mail={mail_status}", "", "", "",
             json.dumps({"version": SCRIPT_VERSION})],
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
    subject, text, html = render_memo(
        rows, {"compliance_eligible": 2, "blocked": {}}, scan_na, regime,
        {"rows": 137, "as_of": "2026-07-15"}, 2, 2, d1, 130000)
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
    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[weekly_brief v{SCRIPT_VERSION}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    sys.exit(main())
