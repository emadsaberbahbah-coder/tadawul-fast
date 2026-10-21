"""run_shadow_scorer v1.9.4 [P-186 lastscored safety] harness - REAL module,
REAL main() on an in-memory sheet stub (the P-176/P-186 harness pattern); the
network fetch and the clock are the only injected parts.

Replays the production days of 2026-10-13..15 (shapes taken from the live
verdict lines): 10-14 scored a 1-name challenger (CVBF.US), 10-15 seated 4
new names and was excluded by the legacy denominator (`chal fresh=1/4`).
  L1 10-15 replay: legacy excludes (fresh=1/4, fresh-floor); lastscored scores
     (fresh=1/1) and its read-back carries `legs=1/1` with no readback_error;
     observe writes the legacy rows and names `ls_day=scored`
  L2 empty-challenger weekday under lastscored: excluded as no-challenger,
     no numeric challenger return (v1.9.3 scored it - set S1_BASE to see)
  L3 no lock: the next day with a recovered board is scored against the last
     real base under lastscored (v1.9.3 stayed excluded at fresh=0/N)
  L4 observe on the empty day: legacy rows, `ls_day=excluded:no-challenger`
  L5 pure ls_day_verdict precedence table
  L6 legacy rows/gate/_Run_Log byte-identical to the base module on the L1 and
     L2 stubs (version token aside) - runs only when S1_BASE=<v1.9.3 file>
  L7 a scored lastscored base with no stored price: aligned legacy fallback for
     every basket, `fallback=empty-base` on the read-back
Set S1_FILE=<path> to test a file outside the repo tree.
Run under pytest in an isolated subprocess.
"""
from pathlib import Path
import subprocess
import sys


def main():
    import contextlib
    import copy
    import importlib.util
    import io
    import json
    import os
    from datetime import datetime, timezone

    here = os.path.dirname(os.path.abspath(__file__))
    root = os.path.abspath(os.path.join(here, ".."))
    os.chdir(root)
    s1_file = os.environ.get("S1_FILE") or os.path.join(root, "scripts", "run_shadow_scorer.py")

    def _load(path, name):
        spec = importlib.util.spec_from_file_location(name, path)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        return m

    s1 = _load(s1_file, "s1_under_test")
    assert tuple(map(int, s1.SCRIPT_VERSION.split("."))) >= (1, 9, 4), s1.SCRIPT_VERSION
    V = s1.SCRIPT_VERSION

    class WS:
        def __init__(self, rows):
            self.rows = [list(r) for r in rows]

        def get_all_values(self):
            return [list(r) for r in self.rows]

        def append_row(self, row, value_input_option=None):
            self.rows.append(list(row))

        def append_rows(self, rows, value_input_option=None):
            self.rows.extend(list(r) for r in rows)

        def update(self, values=None, range_name=None):
            self.rows = [list(r) for r in (values or [])]

        def clear(self):
            self.rows = []

    class Sheet:
        def __init__(self, tabs):
            self.tabs = {k: WS(v) for k, v in tabs.items()}

        def worksheet(self, name):
            if name not in self.tabs:
                raise KeyError(name)
            return self.tabs[name]

        def add_worksheet(self, title, rows=0, cols=0):
            self.tabs[title] = WS([])
            return self.tabs[title]

    OUT = s1.sb.OUT_HEADER

    def board(asof_txt, yes, no=()):
        meta = [["SHADOW BOARD v1.5.0", f"as of {asof_txt} Riyadh", "equity=130,000 SAR"],
                ["authority rows=408 as_of=2026-03-31", "authority_error=-"],
                [f"evaluated={len(yes) + len(no)}", f"compliance_eligible={len(yes) + len(no)}", "blocked={}"],
                ["advisory stamp - drives nothing until gated"], []]
        body = [list(OUT)]
        for s, flag, verdict in [(s, "YES", "TRADE") for s in yes] + [(s, "NO", "EDGE_BELOW_COST") for s in no]:
            row = [s, s + " Inc", "", "Financials", "20.0", "", "SCREEN_RETIRED", "retired_2026-08-13",
                   "BROKER_TRADABLE", "", "YES", "0.1", "9.0", "1.5", verdict, "1.0", "engine", flag]
            assert len(row) == len(OUT)
            body.append(row)
        return meta + body

    CHAMP = ["YUM", "DDI.US"]
    BENCH = ["SPUS", "^TASI.SR"]
    B13 = ["2222.SR", "INDV.US", "JMSB.US"]
    NEW15 = ["ACTG.US", "GRT-UN.TO", "MGY.US", "VLY.US"]
    P13 = {"2222.SR": 25.0, "INDV.US": 30.0, "JMSB.US": 12.0, "CVBF.US": 20.0, "SPUS": 60.0,
           "^TASI.SR": 10500.0, "YUM": 137.0, "DDI.US": 13.0}
    P14 = {"2222.SR": 25.2, "INDV.US": 29.5, "JMSB.US": 12.1, "CVBF.US": 20.2, "SPUS": 60.3,
           "^TASI.SR": 10520.0, "YUM": 137.5, "DDI.US": 13.1}
    P15 = {"2222.SR": 25.1, "INDV.US": 29.9, "JMSB.US": 12.3, "CVBF.US": 20.5, "SPUS": 60.1,
           "^TASI.SR": 10490.0, "YUM": 136.9, "DDI.US": 13.2, "ACTG.US": 4.1, "GRT-UN.TO": 70.0,
           "MGY.US": 24.0, "VLY.US": 9.8}
    P16 = dict(P15, **{"ACTG.US": 4.2, "GRT-UN.TO": 70.5, "MGY.US": 24.4, "VLY.US": 9.9,
                       "2222.SR": 25.0, "INDV.US": 30.2, "JMSB.US": 12.4, "SPUS": 60.4, "^TASI.SR": 10510.0})

    def hrow(day, basket, syms, prices, ret, idx, note):
        return [day, basket, ",".join(syms), json.dumps({s: prices[s] for s in syms}),
                "" if ret is None else ret, idx, 0.0, 0.0, note]

    def history(upto_14=True):
        h = [list(s1.HISTORY_HEADER),
             hrow("2026-10-13", "CHAMPION", CHAMP, P13, 0.1, 100.6, "n=2"),
             hrow("2026-10-13", "CHALLENGER", B13, P13, 0.4, 108.2, "n=3"),
             hrow("2026-10-13", "BENCHMARK", BENCH, P13, 0.2, 102.5, "n=2")]
        if upto_14:
            h += [hrow("2026-10-14", "CHAMPION", CHAMP, P14, 0.2, 100.8, "n=2"),
                  hrow("2026-10-14", "CHALLENGER", ["CVBF.US"], P14, -0.3, 107.9, "n=3"),
                  hrow("2026-10-14", "BENCHMARK", BENCH, P14, 0.3, 102.8, "n=2")]
        return h

    def sheet(hist, board_rows):
        top10 = [["TOP 10 INVESTMENTS - DECISION"], ["Status:", "Last run"], [],
                 ["Rank", "Symbol", "Name", "Price", "ROI %"]] + \
                [[str(i + 1), s, s + " Co", "1.0", "10%"] for i, s in enumerate(CHAMP)]
        return Sheet({
            s1.sb.TAB_TOP10: top10,
            s1.sb.TAB_OUT: board_rows,
            s1.TAB_HISTORY: copy.deepcopy(hist),
            s1.TAB_REGRET: [list(s1.rg.LEDGER_HEADER)],
            "_Run_Log": [["Timestamp", "Level", "Action", "Page", "Status", "Message",
                          "Endpoint", "HTTP Code", "Duration ms", "Details JSON"],
                         ["2026-10-01 05:00:00", "INFO", "workbook_backup", "_Backup", "OK",
                          "[RESTORE-TEST v1.4.0] PASS rto=8m", "", "", "", "{}"]],
        })

    envs = ("TFB_S1_BASE_POLICY", "TFB_S1_DAY_KEY", "TFB_S1_BOARD_FRESH_GUARD", "TFB_SHADOW_EODHD",
            "TFB_SHADOW_EQW", "TFB_S1_CRITERIA_V2")

    def run_at(mod, when_utc, sh, spot, bar_day, policy=None):
        fixed = datetime.strptime(when_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

        class FakeDT(datetime):
            @classmethod
            def now(cls, tz=None):
                return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)
        bar = datetime.strptime(bar_day, "%Y-%m-%d").date()

        def fake_fetch(symbols):
            need = [s for s in symbols if s in spot]
            return ({s: spot[s] for s in need}, {s: bar for s in need}, [])
        saved = (mod.datetime, mod.fetch_spot, mod.sb._open_sheet, {k: os.environ.get(k) for k in envs})
        try:
            mod.datetime = FakeDT
            mod.fetch_spot = fake_fetch
            mod.sb._open_sheet = lambda cli: sh
            for k in envs:
                os.environ.pop(k, None)
            if policy is not None:
                os.environ["TFB_S1_BASE_POLICY"] = policy
            os.environ["TFB_S1_BOARD_FRESH_GUARD"] = "observe"
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                rc = mod.main([])
            return rc, buf.getvalue()
        finally:
            mod.datetime, mod.fetch_spot, mod.sb._open_sheet = saved[0], saved[1], saved[2]
            for k, v in saved[3].items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def rows_on(sh, day):
        return {r[1]: r for r in sh.tabs[s1.TAB_HISTORY].rows[1:] if r[0] == day}

    def verdict(sh):
        return sh.tabs["_Run_Log"].rows[-1][5]

    # ------------------------------------------------------------------ L1 -- #
    sh_leg = sheet(history(), board("2026-10-15 17:10", NEW15))
    assert run_at(s1, "2026-10-15T15:20:00Z", sh_leg, P15, "2026-10-15")[0] == 0
    v = verdict(sh_leg)
    r = rows_on(sh_leg, "2026-10-15")["CHALLENGER"]
    assert r[4] == "" and "chal fresh=1/4" in v and "excluded_reason=fresh-floor" in v and "[S1-BASE" not in v, v
    sh_ls = sheet(history(), board("2026-10-15 17:10", NEW15))
    assert run_at(s1, "2026-10-15T15:20:00Z", sh_ls, P15, "2026-10-15", "lastscored")[0] == 0
    v_ls = verdict(sh_ls)
    r_ls = rows_on(sh_ls, "2026-10-15")["CHALLENGER"]
    assert isinstance(r_ls[4], float) and "chal fresh=1/1" in v_ls and "day_scored" in v_ls, v_ls
    assert f"[S1-BASE v{V}] mode=lastscored" in v_ls and "legs=1/1" in v_ls and "readback_error" not in v_ls, v_ls
    sh_obs = sheet(history(), board("2026-10-15 17:10", NEW15))
    assert run_at(s1, "2026-10-15T15:20:00Z", sh_obs, P15, "2026-10-15", "observe")[0] == 0
    v_obs = verdict(sh_obs)
    assert rows_on(sh_obs, "2026-10-15") == rows_on(sh_leg, "2026-10-15"), "observe must write legacy rows"
    assert "ls_day=scored" in v_obs and "excluded_reason=fresh-floor" in v_obs, v_obs
    print("L1 PASS  10-15 replay: legacy fresh=1/4 excluded | lastscored fresh=1/1 scored, legs=1/1, "
          "no readback_error | observe legacy rows + ls_day=scored")

    # ------------------------------------------------------------------ L2 -- #
    empty14 = board("2026-10-14 17:10", [], no=["CVBF.US", "2286.SR"])
    sh_e = sheet(history(upto_14=False), empty14)
    assert run_at(s1, "2026-10-14T15:20:00Z", sh_e, P14, "2026-10-14", "lastscored")[0] == 0
    v_e = verdict(sh_e)
    r_e = rows_on(sh_e, "2026-10-14")["CHALLENGER"]
    assert r_e[4] == "" and "reason=no-challenger" in r_e[8], r_e
    assert "excluded_reason=no-challenger" in v_e and "readback_error" not in v_e, v_e
    print("L2 PASS  empty-challenger weekday under lastscored: excluded no-challenger, no scored empty base")

    # ------------------------------------------------------------------ L3 -- #
    sh_e.tabs[s1.sb.TAB_OUT] = WS(board("2026-10-15 17:10", NEW15))
    assert run_at(s1, "2026-10-15T15:20:00Z", sh_e, P15, "2026-10-15", "lastscored")[0] == 0
    v_n = verdict(sh_e)
    r_n = rows_on(sh_e, "2026-10-15")["CHALLENGER"]
    assert isinstance(r_n[4], float) and "chal fresh=3/3" in v_n and "day_scored" in v_n, v_n
    print("L3 PASS  no lock: next day scored against the 10-13 base (fresh=3/3)")

    # ------------------------------------------------------------------ L4 -- #
    sh_o = sheet(history(upto_14=False), empty14)
    sh_l = sheet(history(upto_14=False), empty14)
    run_at(s1, "2026-10-14T15:20:00Z", sh_o, P14, "2026-10-14", "observe")
    run_at(s1, "2026-10-14T15:20:00Z", sh_l, P14, "2026-10-14")
    assert rows_on(sh_o, "2026-10-14") == rows_on(sh_l, "2026-10-14")
    assert "ls_day=excluded:no-challenger" in verdict(sh_o), verdict(sh_o)
    print("L4 PASS  observe on the empty day: legacy rows + ls_day=excluded:no-challenger")

    # ------------------------------------------------------------------ L5 -- #
    f = s1.ls_day_verdict
    table = [
        ((4, True, 1, 1, 0.6, False, False, True, False), "scored"),
        ((4, True, 1, 0, 0.6, False, False, True, False), "excluded:fresh-floor"),
        ((0, True, 1, 1, 0.6, False, False, True, False), "excluded:no-challenger"),
        ((4, True, 3, 5, 0.6, False, False, True, False), "scored"),
        ((4, True, 2, 5, 0.6, False, False, True, False), "excluded:fresh-floor"),
        ((4, True, 5, 5, 0.6, True, False, True, False), "non-trading"),
        ((4, True, 5, 5, 0.6, False, True, True, False), "excluded:stale-board"),
        ((4, True, 5, 5, 0.6, False, False, True, True), "excluded:benchmark-leg"),
        ((4, True, 5, 5, 0.6, False, False, False, True), "scored"),
        ((4, False, 0, 0, 0.6, False, False, False, False), "seed"),
    ]
    for args, want in table:
        assert f(*args) == want, (args, f(*args), want)
    print(f"L5 PASS  ls_day_verdict precedence table {len(table)}/{len(table)}")

    # ------------------------------------------------------------------ L7 -- #
    # A scored base row that stored no price (all seats new and unpriced): lastscored
    # must not pair against it forever - aligned legacy fallback for this run.
    def history_priceless_base():
        h = history(upto_14=False)
        h += [hrow("2026-10-14", "CHAMPION", CHAMP, P14, 0.2, 100.8, "n=2"),
              ["2026-10-14", "CHALLENGER", "2286.SR,4280.SR", "{}", -0.3, 107.9, 0.0, 0.0, "n=3"],
              hrow("2026-10-14", "BENCHMARK", BENCH, P14, 0.3, 102.8, "n=2")]
        return h
    sh_fb = sheet(history_priceless_base(), board("2026-10-15 17:10", NEW15))
    sh_fl = sheet(history_priceless_base(), board("2026-10-15 17:10", NEW15))
    assert run_at(s1, "2026-10-15T15:20:00Z", sh_fb, P15, "2026-10-15", "lastscored")[0] == 0
    assert run_at(s1, "2026-10-15T15:20:00Z", sh_fl, P15, "2026-10-15")[0] == 0
    v_fb = verdict(sh_fb)
    assert rows_on(sh_fb, "2026-10-15") == rows_on(sh_fl, "2026-10-15"), "fallback must write the legacy rows"
    assert f"[S1-BASE v{V}] mode=lastscored fallback=empty-base" in v_fb and "readback_error" not in v_fb, v_fb
    print("L7 PASS  priceless lastscored base: aligned legacy fallback (rows == legacy), fallback=empty-base")

    # ------------------------------------------------------------------ L6 -- #
    base_path = os.environ.get("S1_BASE")
    if base_path:
        b = _load(base_path, "s1_base")
        norm = lambda rows: [[str(c).replace("v" + b.SCRIPT_VERSION, "vX").replace("v" + V, "vX")  # noqa: E731
                              for c in r] for r in rows]
        for label, hist, brd, when, spot, day in (
                ("L1", history(), board("2026-10-15 17:10", NEW15), "2026-10-15T15:20:00Z", P15, "2026-10-15"),
                ("L2", history(upto_14=False), empty14, "2026-10-14T15:20:00Z", P14, "2026-10-14")):
            a, c = sheet(hist, brd), sheet(hist, brd)
            run_at(s1, when, a, spot, day)
            run_at(b, when, c, spot, day)
            for tab in (s1.TAB_HISTORY, s1.TAB_GATE, "_Run_Log"):
                ra = [r[:9] if tab == "_Run_Log" else r for r in a.tabs[tab].rows]
                rc = [r[:9] if tab == "_Run_Log" else r for r in c.tabs[tab].rows]
                assert norm(ra) == norm(rc), (label, tab)
        # the base reproduces both defects on the same stubs
        c2 = sheet(history(upto_14=False), empty14)
        run_at(b, "2026-10-14T15:20:00Z", c2, P14, "2026-10-14", "lastscored")
        base_row = rows_on(c2, "2026-10-14")["CHALLENGER"]
        c2.tabs[s1.sb.TAB_OUT] = WS(board("2026-10-15 17:10", NEW15))
        run_at(b, "2026-10-15T15:20:00Z", c2, P15, "2026-10-15", "lastscored")
        base_next = verdict(c2)
        print(f"L6 PASS  legacy byte-identical to v{b.SCRIPT_VERSION} on both stubs | base under lastscored: "
              f"empty day return={base_row[4]!r} note={base_row[8]!r} | next day: "
              f"{'readback_error' if 'readback_error' in base_next else 'no readback_error'}, "
              f"{'fresh-floor' if 'fresh-floor' in base_next else 'scored'}")
    else:
        print("L6 SKIP  dual-tree (set S1_BASE=<v1.9.3 file>)")
    print("RESULT PASS")


def test_all():
    result = subprocess.run([sys.executable, str(Path(__file__).resolve())],
                            cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr


if __name__ == "__main__":
    main()
