#!/usr/bin/env python3
"""run_shadow_board v1.5.0 [P-175 NON-TRADING FREEZE] dual-tree harness.

Loads the REAL delivered module (scripts/run_shadow_board.py) and, when the
base file is present beside it (run_shadow_board_base_v1.4.0.py or the path
in SB_BASE), the REAL base module too. Network and Sheets are replaced by
fakes at the seams main() already uses (_open_sheet, fetch_board_fundamentals,
build_regime_block, fetch_engine_roi_map, shariah_authority, switch scan);
every other function - rows_to_records, evaluate_board, build_risk_block,
write_board, the freeze helpers - runs for real.

F1 pure battery | F2 off == base call-for-call on a Sunday (byte-identical
writes) | F3 observe == off writes + note + freeze_observe in the Run_Log
JSON | F4 enforce on Sunday: ZERO board/history writes, ONE FROZEN Run_Log
row, no Top_10 read | F5 enforce on Monday == off | F6 holiday list |
F7 dry-run under enforce writes nothing at all | F8 selftest parity.
Run x3, identical digest.  Exit 0 = PASS.
"""
import copy
import hashlib
import importlib.util
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
DELIVERED = os.path.join(ROOT, "scripts", "run_shadow_board.py")
BASE = os.environ.get("SB_BASE") or os.path.join(ROOT, "scripts", "run_shadow_board_base_v1.4.0.py")


def load(name, path):
    sp = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(sp)
    sp.loader.exec_module(m)
    return m


TOP10 = [["TOP 10 INVESTMENTS"], ["Status:", "Last run"], [""], ["CONTROL PANEL"],
         ["T10: knob", "1"], [""], ["SELECTED"],
         ["Rank", "Symbol", "Name", "Sector", "Ticket SAR", "ROI %", "Conf"],
         ["1", "ITRN.US", "Ituran", "Technology", "10,000", "34.7", "High"],
         ["2", "PINE.US", "Alpine Income", "Real Estate", "9,000", "28.6", "High"],
         ["3", "NVDA.US", "NVIDIA", "Technology", "9,000", "34.8", "High"],
         [""], ["ALL QUALIFIED"], ["Rank", "Symbol", "Name", "Sector", "Ticket SAR", "ROI %", "Conf"],
         ["1", "GOOGL.US", "Alphabet", "Communication", "9,000", "24.9", "High"]]
HOLDS = [["MY PORTFOLIO"], ["Status:", "x"], [""], ["ACTIONS"],
         ["Action", "Symbol", "Name", "Qty", "Market Value (SAR)", "Confidence", "Expected ROI %"],
         ["HOLD", "YUM", "Yum", "24", "12,465", "Low", "25.5"]]


class FakeWS:
    def __init__(self, name, values, log):
        self.name, self.values, self.log = name, values, log

    def get_all_values(self):
        self.log.append(("read", self.name))
        return copy.deepcopy(self.values)

    def update(self, values=None, range_name=None):
        self.log.append(("update", self.name, range_name, len(values), values[0][:2] if values else None))

    def clear(self):
        self.log.append(("clear", self.name))

    def append_row(self, row, value_input_option=None):
        self.log.append(("append_row", self.name, row[4], row[9]))

    def append_rows(self, rows, value_input_option=None):
        self.log.append(("append_rows", self.name, len(rows)))

    def freeze(self, rows=None):
        pass


class FakeSH:
    def __init__(self, log):
        self.log = log
        self.tabs = {"Top_10_Investments": FakeWS("Top_10_Investments", TOP10, log),
                     "Portfolio_Decision": FakeWS("Portfolio_Decision", HOLDS, log),
                     "Shadow_Board": FakeWS("Shadow_Board", [], log),
                     "Regime_History": FakeWS("Regime_History", [], log),
                     "_Run_Log": FakeWS("_Run_Log", [], log)}

    def worksheet(self, name):
        self.log.append(("worksheet", name))
        return self.tabs[name]

    def add_worksheet(self, title, rows, cols):
        return self.tabs[title]


REGIME = {"sleeves": {"Global": {"state": "RISK_ON", "distance_pct": 9.3, "months_in_state": 6, "abs_mom_pct": 25.5},
                      "Saudi": {"state": "RISK_OFF", "distance_pct": -2.9, "months_in_state": 1, "abs_mom_pct": -14.0}},
          "suggested_weights": {"Global": 0.7, "Saudi": 0.0, "Cash": 0.3}, "errors": [], "version": "t", "governance": "advisory stamp"}


def wire(mod, log):
    """Replace the network/sheets seams of a loaded module with fakes."""
    mod._open_sheet = lambda cli: (log.append(("open_sheet",)) or FakeSH(log))
    mod.fetch_board_fundamentals = lambda syms, sleep_s=0.4: ({}, [])
    mod.build_regime_block = lambda: copy.deepcopy(REGIME)
    mod.fetch_engine_roi_map = lambda sh, symbols: ({}, [])
    sa = mod.sa

    class SA:
        @staticmethod
        def get_authority_index(force=True): return {}
        @staticmethod
        def get_monitor_map(): return {}
        @staticmethod
        def get_meta(): return {"rows": 408, "as_of": "2026-03-31"}
        @staticmethod
        def last_error(): return None
    mod.sa = SA
    pa = mod.pa

    class PA:
        @staticmethod
        def advisor_switch_scan(holds, cands): return {"verdict": "NONE", "proposals": [], "pairs_checked": 0}
    mod.pa = PA
    return sa, pa


def run_main(mod, env, dry_run=False):
    log = []
    wire(mod, log)
    saved = {k: os.environ.get(k) for k in ("TFB_BOARD_FREEZE_NONTRADING", "TFB_BOARD_FREEZE_HOLIDAYS", "TFB_BOARD_FREEZE_TODAY", "TFB_BOARD_ENGINE_ROI")}
    for k in saved:
        os.environ.pop(k, None)
    os.environ.update(env)
    import io
    import contextlib
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            rc = mod.main(["--dry-run"] if dry_run else [])
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    return rc, log, buf.getvalue()


def norm(log):
    """Strip run-time-varying bits (timestamps inside meta cell 2) for comparison."""
    out = []
    for e in log:
        if e[0] == "update":
            out.append(("update", e[1], e[2], e[3], e[4][0] if e[4] else None))
        elif e[0] == "append_row":
            out.append(("append_row", e[1], e[2], json.loads(e[3]) if e[3] else None))
        else:
            out.append(e)
    return out


def main():
    out, fails = [], 0

    def T(name, cond, detail=""):
        nonlocal fails
        out.append(("PASS " if cond else "FAIL ") + name + ((" | " + str(detail)) if detail else ""))
        if not cond:
            fails += 1

    fix = load("sb_fix", DELIVERED)
    base = load("sb_base", BASE) if os.path.exists(BASE) else None
    T("versions", fix.SCRIPT_VERSION == "1.5.0" and (base is None or base.SCRIPT_VERSION == "1.4.0"), f"fix={fix.SCRIPT_VERSION} base={(base.SCRIPT_VERSION if base else 'absent')}")
    from datetime import date
    # F1 pure battery
    T("F1 weekend Sat/Sun", fix._is_nontrading_day(date(2026, 9, 26)) == (True, "weekend:Sat") and fix._is_nontrading_day(date(2026, 9, 27)) == (True, "weekend:Sun"))
    T("F1 weekday", fix._is_nontrading_day(date(2026, 9, 28)) == (False, "") and fix._is_nontrading_day(date(2026, 10, 2)) == (False, ""))
    hol = fix._freeze_holidays("2026-11-26,2026-12-25; junk 2026-13-40")
    T("F1 holidays parse", hol == {date(2026, 11, 26), date(2026, 12, 25)}, sorted(map(str, hol)))
    T("F1 holiday detection", fix._is_nontrading_day(date(2026, 11, 26), hol) == (True, "holiday:2026-11-26"))
    v = fix.freeze_verdict("enforce", date(2026, 9, 27))
    T("F1 verdict enforce Sunday", v["skip"] is True and v["nontrading"] and "frozen: weekend:Sun" in v["note"] and v["date"] == "2026-09-27")
    T("F1 verdict observe Sunday", fix.freeze_verdict("observe", date(2026, 9, 27))["skip"] is False and "would freeze" in fix.freeze_verdict("observe", date(2026, 9, 27))["note"])
    T("F1 verdict off Sunday", fix.freeze_verdict("off", date(2026, 9, 27)) == {"mode": "off", "date": "2026-09-27", "nontrading": True, "reason": "weekend:Sun", "skip": False, "note": ""})
    T("F1 verdict enforce Monday", fix.freeze_verdict("enforce", date(2026, 9, 28))["skip"] is False and fix.freeze_verdict("enforce", date(2026, 9, 28))["note"] == "")
    os.environ["TFB_BOARD_FREEZE_NONTRADING"] = "ENFORCE "
    T("F1 mode parser case/space", fix._freeze_mode() == "enforce")
    os.environ["TFB_BOARD_FREEZE_NONTRADING"] = "yes"
    T("F1 mode parser junk -> off", fix._freeze_mode() == "off")
    os.environ.pop("TFB_BOARD_FREEZE_NONTRADING", None)
    os.environ["TFB_BOARD_FREEZE_TODAY"] = "2026-09-27"
    T("F1 clock hook", fix._freeze_today() == date(2026, 9, 27))
    os.environ["TFB_BOARD_FREEZE_TODAY"] = "garbage"
    T("F1 clock hook junk -> real clock", isinstance(fix._freeze_today(), date))
    os.environ.pop("TFB_BOARD_FREEZE_TODAY", None)

    # F2 off on Sunday == base call-for-call
    sun = {"TFB_BOARD_FREEZE_TODAY": "2026-09-27"}
    rc_off, log_off, out_off = run_main(fix, dict(sun, TFB_BOARD_FREEZE_NONTRADING="off"))
    T("F2 off rc", rc_off == 0)
    T("F2 off writes the board", any(e[0] == "update" and e[1] == "Shadow_Board" for e in log_off))
    T("F2 off prints no freeze note", "SHADOW-BOARD-FREEZE" not in out_off)
    if base is not None:
        rc_b, log_b, out_b = run_main(base, dict(sun))
        nb, nf = norm(log_b), norm(log_off)
        # the only permitted difference: the version string inside the Run_Log JSON and the meta header cell
        nb2 = [(e[0], e[1], e[2], {**e[3], "version": "X"}) if e[0] == "append_row" and e[1] == "_Run_Log" else e for e in nb]
        nf2 = [(e[0], e[1], e[2], {**e[3], "version": "X"}) if e[0] == "append_row" and e[1] == "_Run_Log" else e for e in nf]
        nb2 = [(e[0], e[1], e[2], e[3], str(e[4]).replace("1.4.0", "V")) if e[0] == "update" else e for e in nb2]
        nf2 = [(e[0], e[1], e[2], e[3], str(e[4]).replace("1.5.0", "V")) if e[0] == "update" else e for e in nf2]
        T("F2 off == base call sequence (version cells masked)", nb2 == nf2, f"base={len(nb2)} fix={len(nf2)}")
        T("F2 off == base stdout (version masked)", out_b.replace("1.4.0", "V") == out_off.replace("1.5.0", "V"))
    # F3 observe on Sunday: same writes + note + freeze_observe key
    rc_obs, log_obs, out_obs = run_main(fix, dict(sun, TFB_BOARD_FREEZE_NONTRADING="observe"))
    T("F3 observe rc", rc_obs == 0)
    T("F3 observe writes identical to off", norm([e for e in log_obs if e[0] != "append_row" or e[1] != "_Run_Log"]) == norm([e for e in log_off if e[0] != "append_row" or e[1] != "_Run_Log"]))
    T("F3 observe note printed once", out_obs.count("would freeze (observe): weekend:Sun") == 1)
    rl_obs = [e for e in log_obs if e[0] == "append_row" and e[1] == "_Run_Log"]
    T("F3 observe Run_Log JSON carries freeze_observe", len(rl_obs) == 1 and rl_obs[0][2] == "OK" and json.loads(rl_obs[0][3]) == {"version": "1.5.0", "freeze_observe": "weekend:Sun"}, rl_obs)
    rl_off = [e for e in log_off if e[0] == "append_row" and e[1] == "_Run_Log"]
    T("F3 off Run_Log JSON has no freeze key", json.loads(rl_off[0][3]) == {"version": "1.5.0"})
    # F4 enforce on Sunday: nothing but the FROZEN row
    rc_enf, log_enf, out_enf = run_main(fix, dict(sun, TFB_BOARD_FREEZE_NONTRADING="enforce"))
    T("F4 enforce rc", rc_enf == 0)
    T("F4 enforce no board/history writes", not any(e[0] in ("update", "clear", "append_rows") for e in log_enf), log_enf)
    T("F4 enforce no Top_10 read", not any(e[0] == "read" for e in log_enf))
    T("F4 enforce exactly one FROZEN Run_Log row", [e for e in log_enf if e[0] == "append_row"] == [("append_row", "_Run_Log", "FROZEN", json.dumps({"version": "1.5.0", "freeze": "weekend:Sun", "date": "2026-09-27"}))], log_enf)
    T("F4 enforce note printed", "frozen: weekend:Sun - board kept, nothing written" in out_enf and "[SHADOW-BOARD v1.5.0]" not in out_enf)
    # F5 enforce on Monday == off
    mon = {"TFB_BOARD_FREEZE_TODAY": "2026-09-28"}
    rc_m, log_m, out_m = run_main(fix, dict(mon, TFB_BOARD_FREEZE_NONTRADING="enforce"))
    rc_mo, log_mo, out_mo = run_main(fix, dict(mon, TFB_BOARD_FREEZE_NONTRADING="off"))
    T("F5 enforce Monday == off Monday", norm(log_m) == norm(log_mo) and out_m == out_mo and rc_m == 0)
    # F6 holiday
    thx = {"TFB_BOARD_FREEZE_TODAY": "2026-11-26", "TFB_BOARD_FREEZE_HOLIDAYS": "2026-11-26,2026-12-25"}
    rc_h, log_h, out_h = run_main(fix, dict(thx, TFB_BOARD_FREEZE_NONTRADING="enforce"))
    T("F6 holiday frozen", rc_h == 0 and [e for e in log_h if e[0] == "append_row"][0][2] == "FROZEN" and "holiday:2026-11-26" in out_h and not any(e[0] == "update" for e in log_h))
    rc_h2, log_h2, _ = run_main(fix, dict({"TFB_BOARD_FREEZE_TODAY": "2026-11-26"}, TFB_BOARD_FREEZE_NONTRADING="enforce"))
    T("F6 same date without the list runs normally", any(e[0] == "update" and e[1] == "Shadow_Board" for e in log_h2))
    # F7 dry-run under enforce on Sunday writes nothing, opens nothing
    rc_d, log_d, out_d = run_main(fix, dict(sun, TFB_BOARD_FREEZE_NONTRADING="enforce"), dry_run=True)
    T("F7 dry-run enforce: zero calls", rc_d == 0 and log_d == [] and "frozen: weekend:Sun" in out_d, log_d)
    # F8 selftest parity (environment golden: the two fundamentals checks depend on optional deps)
    r = subprocess.run([sys.executable, DELIVERED, "--selftest"], capture_output=True, text=True)
    tail = [l for l in r.stdout.splitlines() if "SELFTEST" in l][-1]
    fz_lines = [l for l in r.stdout.splitlines() if l.startswith("PASS freeze:")]
    T("F8 delivered selftest: 7 freeze checks PASS", len(fz_lines) == 7, tail)
    if base is not None:
        rb = subprocess.run([sys.executable, BASE, "--selftest"], capture_output=True, text=True)
        tb = [l for l in rb.stdout.splitlines() if "SELFTEST" in l][-1]
        fb = sorted(l for l in rb.stdout.splitlines() if l.startswith("FAIL "))
        ff = sorted(l for l in r.stdout.splitlines() if l.startswith("FAIL "))
        T("F8 same FAIL set as base (environment golden)", fb == ff, f"base='{tb.strip()}' fix='{tail.strip()}'")
    digest = hashlib.sha256("\n".join(out).encode()).hexdigest()[:12]
    print("\n".join(out))
    print(f"SUMMARY {len(out) - fails}/{len(out)} PASS | digest {digest}")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
