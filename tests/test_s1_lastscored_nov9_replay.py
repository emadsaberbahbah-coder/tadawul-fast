"""run_shadow_scorer [P-186 lastscored] on the real 6-9 Nov 2026 shape - the
REAL module's main() on an in-memory sheet (the P-186 harness pattern); the
price fetch and the clock are the only injected parts.

WHY (10 Nov 2026, owner: "check the learning curve ... still in day 26 from 28
days ... solve the stopper"): Shadow_History, challenger rows as recorded -
  2026-11-06 ZTO,ANIP,1030.SR,SNDA  prices ZTO/ANIP/SNDA  return -2.3555 (scored)
  2026-11-07 (empty board)                                DAY_NON_TRADING fresh=0/0
  2026-11-08 ZTO,EFC,PFS,SPFI       prices none           DAY_NON_TRADING fresh=0/4
  2026-11-09 ZTO,EFC,MBIN,PFS,SPFI  prices all five       DAY_EXCLUDED_INFRA fresh=0/5 reason=fresh-floor
Under legacy the 9 Nov challenger paired against the 8 Nov row, which held no
price, so Monday was lost. The basket that was actually held into Monday is
the last scored one (6 Nov); under lastscored every basket pairs against its
last scored row, so the same interval (Fri close -> Mon close) is measured for
challenger and benchmark alike and the day counts.

  L1 legacy (env unset) reproduces the live loss: Monday excluded, fresh=0/5
  L2 lastscored: Monday scored; challenger and benchmark both pair against the
     6 Nov rows; the return equals the module's own pure math; fresh=3/3
  L3 lastscored, the next session (Tue): pairs against Monday's stored prices
     (Monday is now the last scored row) - ordinary one-session day
  L4 the workflow maps TFB_S1_BASE_POLICY with default lastscored
"""
from __future__ import annotations

import contextlib
import copy
import io
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import run_shadow_scorer as s1  # noqa: E402


class WS:
    def __init__(self, rows):
        self.rows = [list(r) for r in rows]

    def get_all_values(self):
        return [list(r) for r in self.rows]

    def append_row(self, row, value_input_option=None):
        self.rows.append(list(row))

    def append_rows(self, rows, value_input_option=None):
        for r in rows:
            self.rows.append(list(r))

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


CHAMP = ["DDI.US", "GLPI.US", "SUPN.US"]
CHAL_FRI = ["ZTO.US", "ANIP.US", "1030.SR", "SNDA.US"]
CHAL_SUN = ["ZTO.US", "EFC.US", "PFS.US", "SPFI.US"]
CHAL_MON = ["ZTO.US", "EFC.US", "MBIN.US", "PFS.US", "SPFI.US"]
BENCH = ["SPUS", "^TASI.SR"]
P_FRI = {"ZTO.US": 20.10, "ANIP.US": 88.40, "SNDA.US": 30.20, "SPUS": 60.10, "^TASI.SR": 10500.0,
         "DDI.US": 13.58, "GLPI.US": 38.89, "SUPN.US": 26.16}
P_MON = {"ZTO.US": 20.66, "ANIP.US": 87.10, "SNDA.US": 30.95, "EFC.US": 11.40, "MBIN.US": 53.41,
         "PFS.US": 23.29, "SPFI.US": 30.10, "SPUS": 60.55, "^TASI.SR": 10460.0,
         "DDI.US": 15.70, "GLPI.US": 39.44, "SUPN.US": 27.62}
P_TUE = dict(P_MON, **{"ZTO.US": 20.90, "EFC.US": 11.52, "MBIN.US": 53.00, "PFS.US": 23.50,
                       "SPFI.US": 30.40, "SPUS": 60.70, "^TASI.SR": 10480.0})
FRI, MON, TUE = date(2026, 11, 6), date(2026, 11, 9), date(2026, 11, 10)


def hrow(day, basket, syms, prices, ret, idx, note):
    return [day, basket, ",".join(syms), json.dumps({s: prices[s] for s in syms if s in prices}),
            "" if ret is None else ret, idx, 0.0, 0.0, note]


def history_as_recorded():
    """The 6-8 Nov rows as Shadow_History holds them (prices abbreviated to the legs that matter)."""
    H = [list(s1.HISTORY_HEADER)]
    H += [hrow("2026-11-06", "CHAMPION", CHAMP, P_FRI, 0.1, 99.0, "n=3"),
          hrow("2026-11-06", "CHALLENGER", CHAL_FRI, P_FRI, -2.3555, 101.0, "n=3"),
          hrow("2026-11-06", "BENCHMARK", BENCH, P_FRI, -0.2, 100.0, "n=2")]
    H += [hrow("2026-11-07", "CHAMPION", CHAMP, P_FRI, None, 99.0, "DAY_NON_TRADING no-venue-session fresh=0/3"),
          hrow("2026-11-07", "CHALLENGER", [], {}, None, 101.0, "DAY_NON_TRADING no-venue-session fresh=0/0"),
          hrow("2026-11-07", "BENCHMARK", BENCH, P_FRI, None, 100.0, "DAY_NON_TRADING no-venue-session fresh=0/2")]
    H += [hrow("2026-11-08", "CHAMPION", CHAMP, P_FRI, None, 99.0, "DAY_NON_TRADING no-venue-session fresh=0/3"),
          hrow("2026-11-08", "CHALLENGER", CHAL_SUN, {}, None, 101.0, "DAY_NON_TRADING no-venue-session fresh=0/4"),
          hrow("2026-11-08", "BENCHMARK", BENCH, P_FRI, None, 100.0, "DAY_NON_TRADING no-venue-session fresh=0/2")]
    return H


def board_rows(syms):
    meta = [["SHADOW BOARD v1.6.0", "as of 2026-11-09 13:48 Riyadh", "equity=130,000 SAR"],
            ["authority rows=408 as_of=2026-03-31", "authority_error=-"],
            [f"evaluated={len(syms)}", f"compliance_eligible={len(syms)}", "blocked={}"],
            ["advisory stamp - drives nothing until gated"], []]
    body = [list(s1.sb.OUT_HEADER)]
    for s in syms:
        row = [s, s + " Inc", "", "Financials", "20.0", "", "SCREEN_RETIRED", "retired_2026-08-13",
               "BROKER_TRADABLE", "", "YES", "0.1", "9.0", "1.5", "TRADE", "1.0", "engine", "YES"]
        assert len(row) == len(s1.sb.OUT_HEADER)
        body.append(row)
    return meta + body


def sheet(hist, chal):
    top10 = [["TOP 10 INVESTMENTS - DECISION"], ["Status:", "Last run"], [],
             ["Rank", "Symbol", "Name", "Price", "ROI %"]] + \
        [[str(i + 1), s, s + " Co", "1.0", "10%"] for i, s in enumerate(CHAMP)]
    return Sheet({
        s1.sb.TAB_TOP10: top10,
        s1.sb.TAB_OUT: board_rows(chal),
        s1.TAB_HISTORY: copy.deepcopy(hist),
        s1.TAB_REGRET: [list(s1.rg.LEDGER_HEADER)],
        "_Run_Log": [["Timestamp", "Level", "Action", "Page", "Status", "Message", "Endpoint", "HTTP Code",
                      "Duration ms", "Details JSON"],
                     ["2026-11-09 21:57:00", "INFO", "workbook_backup", "_Backup", "OK",
                      "[RESTORE-TEST v1.5.1] PASS DAILY_TFB_Workbook_2026-11-09.xlsx tabs=75", "", "", "", "{}"]],
    })


ENVS = ("TFB_S1_BASE_POLICY", "TFB_S1_DAY_KEY", "TFB_S1_BOARD_FRESH_GUARD", "TFB_SHADOW_EODHD", "TFB_SHADOW_EQW",
        "TFB_S1_SEED_NEW_SEATS", "TFB_S1_CRITERIA_V2")


def run_at(when, sh, spot, bar, policy):
    fixed = datetime.strptime(when, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

    class FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)

    def fake_fetch(symbols):
        need = [s for s in symbols if s in spot]
        return ({s: spot[s] for s in need}, {s: bar for s in need}, [])
    saved = (s1.datetime, s1.fetch_spot, s1.sb._open_sheet, {k: os.environ.get(k) for k in ENVS})
    try:
        s1.datetime, s1.fetch_spot = FakeDT, fake_fetch
        s1.sb._open_sheet = lambda cli: sh
        for k in ENVS:
            os.environ.pop(k, None)
        if policy:
            os.environ["TFB_S1_BASE_POLICY"] = policy
        os.environ["TFB_S1_BOARD_FRESH_GUARD"] = "observe"
        os.environ["TFB_S1_SEED_NEW_SEATS"] = "on"
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = s1.main([])
        return rc, buf.getvalue()
    finally:
        s1.datetime, s1.fetch_spot, s1.sb._open_sheet = saved[0], saved[1], saved[2]
        for k, v in saved[3].items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def rows_for(sh, day):
    return {r[1]: r for r in sh.tabs[s1.TAB_HISTORY].rows[1:] if r[0] == day}


def verdict(sh):
    return sh.tabs["_Run_Log"].rows[-1][5]


def test_l1_legacy_loses_monday():
    sh = sheet(history_as_recorded(), CHAL_MON)
    rc, _ = run_at("2026-11-09T15:30:00Z", sh, P_MON, MON, None)
    assert rc == 0
    r = rows_for(sh, "2026-11-09")
    assert r["CHALLENGER"][4] == "" and "fresh=0/5" in r["CHALLENGER"][8] and "reason=fresh-floor" in r["CHALLENGER"][8]
    assert "excluded_reason=fresh-floor" in verdict(sh)


def test_l2_lastscored_scores_monday_on_one_interval():
    sh = sheet(history_as_recorded(), CHAL_MON)
    rc, _ = run_at("2026-11-09T15:30:00Z", sh, P_MON, MON, "lastscored")
    assert rc == 0
    r = rows_for(sh, "2026-11-09")
    asof = {s: MON for s in P_MON}
    chal = s1.basket_return_fresh({s: P_FRI[s] for s in ("ZTO.US", "ANIP.US", "SNDA.US")}, P_MON, asof, FRI)[0]
    bench = s1.blended_benchmark_return_fresh({s: P_FRI[s] for s in BENCH}, P_MON, asof, FRI)[0]
    assert abs(float(r["CHALLENGER"][4]) - round(chal, 4)) < 1e-6, r["CHALLENGER"]
    assert abs(float(r["BENCHMARK"][4]) - round(bench, 4)) < 1e-6, r["BENCHMARK"]
    v = verdict(sh)
    assert "chal fresh=3/3" in v and "excluded_reason" not in v and "[S1-BASE v" in v and "mode=lastscored" in v, v
    assert "@2026-11-06" in v and "legs=3/3" in v and "readback_error" not in v, v
    stored = json.loads(r["CHALLENGER"][3])
    assert set(stored) == set(CHAL_MON) and stored["EFC.US"] == P_MON["EFC.US"]   # tomorrow's base


def test_l3_next_session_pairs_against_monday():
    sh = sheet(history_as_recorded(), CHAL_MON)
    assert run_at("2026-11-09T15:30:00Z", sh, P_MON, MON, "lastscored")[0] == 0
    assert run_at("2026-11-10T15:40:00Z", sh, P_TUE, TUE, "lastscored")[0] == 0
    r = rows_for(sh, "2026-11-10")
    asof = {s: TUE for s in P_TUE}
    chal = s1.basket_return_fresh({s: P_MON[s] for s in CHAL_MON}, P_TUE, asof, MON)[0]
    assert abs(float(r["CHALLENGER"][4]) - round(chal, 4)) < 1e-6, r["CHALLENGER"]
    assert "chal fresh=5/5" in verdict(sh) and "@2026-11-09" in verdict(sh)


def test_l4_workflow_maps_lastscored():
    wf = yaml.safe_load((ROOT / ".github/workflows/shadow_scorer.yml").read_text(encoding="utf-8"))
    envs = {}
    for job in wf["jobs"].values():
        envs.update(job.get("env") or {})
        for step in job.get("steps") or []:
            envs.update(step.get("env") or {})
    assert envs.get("TFB_S1_BASE_POLICY") == "${{ vars.TFB_S1_BASE_POLICY || 'lastscored' }}", envs.get("TFB_S1_BASE_POLICY")
