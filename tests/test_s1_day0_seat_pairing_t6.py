"""run_shadow_scorer v1.10.0 [T6 DAY-0 SEAT PAIRING] harness - REAL module, REAL
main() on an in-memory sheet stub (the P-176 / P-186 harness pattern); the
network fetch and the clock are the only injected parts.

Replays the structure of the four October `fresh-floor` days (10-05, 10-08,
10-12, 10-15 - all exclusions, zero `no-challenger`): the challenger's base
row carries ten priced seats, today's board keeps five and replaces five
(16-Oct: new=[AX,CRC,EPRT,GLPI,VICI]). Every pairable leg is fresh, yet
fresh=5/10 = 50% < 60% excludes the day under legacy.

  T1 legacy (env unset): DAY_EXCLUDED_INFRA fresh=5/10 reason=fresh-floor;
     [S1-FRESH] names the five new seats; no `day0` token anywhere
  T2 spot: the day SCORES - fresh=10/10, return = 5-leg mean diluted over 10,
     note `n=10 day0=5`, [S1-FRESH] `day0=[...]` on verdict, S1_Gate and
     _Run_Log; champion (no new seats) and benchmark rows unchanged
  T3 junk / unset modes: rows byte-identical to legacy
  T4 already-scored shape (no new seats): spot rows byte-identical to legacy
  T5 full turnover (10/10 new): legacy excluded (fresh=0/10), spot scores flat
  T6 a new seat whose only bar is not newer than the base row is NOT day-0
"""
from __future__ import annotations

import contextlib
import copy
from datetime import date, datetime, timezone
import io
import json
import os

import pytest

from scripts import run_shadow_scorer as s1

OUT = s1.sb.OUT_HEADER
ENVS = ("TFB_S1_BASE_POLICY", "TFB_S1_DAY_KEY", "TFB_S1_BOARD_FRESH_GUARD", "TFB_SHADOW_EODHD",
        "TFB_SHADOW_EQW", "TFB_S1_NEW_SEAT_DAY0", "TFB_S1_CRITERIA_V2")

CHAMP = ["YUM", "DDI.US", "CWBC.US"]
CARRIED = ["VLY.US", "ATAT.US", "RDN.US", "PGC.US", "BBOX.L"]
PREV_ONLY = ["PHP.L", "ACTG.US", "KINS.US", "AUB.US", "BLX.US"]
NEW = ["AX.US", "CRC.US", "EPRT.US", "GLPI.US", "VICI.US"]
CHAL_PREV = CARRIED + PREV_ONLY
CHAL_NOW = CARRIED + NEW
BENCH = ["SPUS", "^TASI.SR"]

P15 = {"VLY.US": 10.0, "ATAT.US": 20.0, "RDN.US": 30.0, "PGC.US": 40.0, "BBOX.L": 50.0,
       "PHP.L": 60.0, "ACTG.US": 70.0, "KINS.US": 80.0, "AUB.US": 90.0, "BLX.US": 100.0,
       "SPUS": 60.0, "^TASI.SR": 10500.0, "YUM": 138.0, "DDI.US": 13.0, "CWBC.US": 26.0}
# Today's spot: the five departed seats have NO price (the scorer prices the
# base row's seats too, and a departed seat that still prices counts as a fresh
# leg - the P-186 asymmetry - so the starvation the brief measured needs them
# unpriced: nopair). P16_FULL prices them for the parity case.
P16 = {"VLY.US": 10.5, "ATAT.US": 19.6, "RDN.US": 30.9, "PGC.US": 40.4, "BBOX.L": 49.0,
       "AX.US": 76.5, "CRC.US": 72.2, "EPRT.US": 33.1, "GLPI.US": 47.9, "VICI.US": 29.4,
       "SPUS": 60.6, "^TASI.SR": 10400.0, "YUM": 137.0, "DDI.US": 13.3, "CWBC.US": 25.9}
P16_FULL = dict(P16, **{"PHP.L": 61.0, "ACTG.US": 69.0, "KINS.US": 81.0, "AUB.US": 91.0, "BLX.US": 99.0})
NOPAIR = "[ACTG.US,AUB.US,BLX.US,KINS.US,PHP.L]"


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


def board_rows(asof_txt, syms):
    meta = [["SHADOW BOARD v1.5.0", f"as of {asof_txt} Riyadh", "equity=130,000 SAR"],
            ["authority rows=408 as_of=2026-03-31", "authority_error=-"],
            [f"evaluated={len(syms)}", f"compliance_eligible={len(syms)}", "blocked={}"],
            ["advisory stamp - drives nothing until gated"], []]
    body = [list(OUT)]
    for s in syms:
        row = [s, s + " Inc", "", "Financials", "20.0", "", "SCREEN_RETIRED", "retired_2026-08-13",
               "BROKER_TRADABLE", "", "YES", "0.1", "9.0", "1.5", "TRADE", "1.0", "engine", "YES"]
        assert len(row) == len(OUT)
        body.append(row)
    return meta + body


def hrow(day, basket, syms, prices, ret, idx, note):
    return [day, basket, ",".join(syms), json.dumps({s: prices[s] for s in syms}),
            "" if ret is None else ret, idx, 0.0, 0.0, note]


def history_scored_15():
    return [list(s1.HISTORY_HEADER),
            hrow("2026-10-15", "CHAMPION", CHAMP, P15, 0.2, 100.5, "n=3"),
            hrow("2026-10-15", "CHALLENGER", CHAL_PREV, P15, 0.3, 106.0, "n=10"),
            hrow("2026-10-15", "BENCHMARK", BENCH, P15, 0.1, 99.4, "n=2")]


def fresh_sheet(hist, board_syms):
    top10 = [["TOP 10 INVESTMENTS - DECISION"], ["Status:", "Last run"], [],
             ["Rank", "Symbol", "Name", "Price", "ROI %"]] + \
            [[str(i + 1), s, s + " Co", "1.0", "10%"] for i, s in enumerate(CHAMP)]
    return Sheet({
        s1.sb.TAB_TOP10: top10,
        s1.sb.TAB_OUT: board_rows("2026-10-16 22:16", board_syms),
        s1.TAB_HISTORY: copy.deepcopy(hist),
        s1.TAB_REGRET: [list(s1.rg.LEDGER_HEADER)],
        "_Run_Log": [["Timestamp", "Level", "Action", "Page", "Status", "Message",
                      "Endpoint", "HTTP Code", "Duration ms", "Details JSON"],
                     ["2026-10-01 05:00:00", "INFO", "workbook_backup", "_Backup", "OK",
                      "[RESTORE-TEST v1.4.0] PASS rto=8m", "", "", "", "{}"]],
    })


def run_at(sheet, spot, bar_day="2026-10-16", when_utc="2026-10-16T15:20:00Z", day0=None, asof_override=None):
    fixed = datetime.strptime(when_utc, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

    class FakeDT(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed.astimezone(tz) if tz else fixed.replace(tzinfo=None)
    bar = datetime.strptime(bar_day, "%Y-%m-%d").date()

    def fake_fetch(symbols):
        need = [s for s in symbols if s in spot]
        asof = {s: (asof_override or {}).get(s, bar) for s in need}
        return ({s: spot[s] for s in need}, asof, [])
    saved = (s1.datetime, s1.fetch_spot, s1.sb._open_sheet, {k: os.environ.get(k) for k in ENVS})
    try:
        s1.datetime = FakeDT
        s1.fetch_spot = fake_fetch
        s1.sb._open_sheet = lambda cli: sheet
        for k in ENVS:
            os.environ.pop(k, None)
        if day0 is not None:
            os.environ["TFB_S1_NEW_SEAT_DAY0"] = day0
        os.environ["TFB_S1_BOARD_FRESH_GUARD"] = "observe"
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


def hist_rows(sheet, day="2026-10-16"):
    return {r[1]: r for r in sheet.tabs[s1.TAB_HISTORY].rows[1:] if r[0] == day}


def runlog_last(sheet):
    return sheet.tabs["_Run_Log"].rows[-1]


def gate_text(sheet):
    return json.dumps(sheet.tabs[s1.TAB_GATE].rows)


def _mean_paired():
    rets = [(P16[s] / P15[s] - 1.0) * 100.0 for s in CARRIED]
    return sum(rets) / len(rets)


# ---------------------------------------------------------------- T1 ------ #
def test_legacy_excludes_the_turnover_day_as_fresh_floor():
    sh = fresh_sheet(history_scored_15(), CHAL_NOW)
    rc, out = run_at(sh, P16)
    assert rc == 0, out
    rows = hist_rows(sh)
    assert rows["CHALLENGER"][4] == "" and rows["CHALLENGER"][8] == "DAY_EXCLUDED_INFRA fresh=5/10 stale=0 reason=fresh-floor"
    assert rows["CHAMPION"][4] == "" and "reason=fresh-floor" in rows["CHAMPION"][8]
    v = runlog_last(sh)[5]
    assert "DAY_EXCLUDED_INFRA" in v and "excluded_reason=fresh-floor" in v
    assert "chal fresh=5/10 floor=60%% stale=- nopair=%s new=[AX.US,CRC.US,EPRT.US,GLPI.US,VICI.US]" % NOPAIR in v
    assert "day0" not in v and "day0" not in gate_text(sh) and "day0" not in json.dumps(rows)


# ---------------------------------------------------------------- T2 ------ #
def test_spot_scores_the_turnover_day_with_day0_read_back():
    sh = fresh_sheet(history_scored_15(), CHAL_NOW)
    rc, out = run_at(sh, P16, day0="spot")
    assert rc == 0, out
    rows = hist_rows(sh)
    expected = round(_mean_paired() * 5 / 10.0, 4)
    assert abs(float(rows["CHALLENGER"][4]) - expected) < 1e-9, (rows["CHALLENGER"][4], expected)
    assert rows["CHALLENGER"][8] == "n=10 day0=5"
    stored = json.loads(rows["CHALLENGER"][3])
    assert sorted(stored) == sorted(CHAL_NOW) and stored["VICI.US"] == P16["VICI.US"]   # pairable tomorrow
    # champion (no new seats) and benchmark score exactly as legacy would on a scored day
    assert rows["CHAMPION"][8] == "n=3" and rows["BENCHMARK"][8] == "n=2"
    v = runlog_last(sh)[5]
    assert "day_scored" in v and "excluded_reason" not in v
    assert "chal fresh=10/10 floor=60%% stale=- nopair=%s new=[AX.US,CRC.US,EPRT.US,GLPI.US,VICI.US] day0=[AX.US,CRC.US,EPRT.US,GLPI.US,VICI.US]" % NOPAIR in v
    assert "day0=[AX.US,CRC.US,EPRT.US,GLPI.US,VICI.US]" in gate_text(sh)
    details = json.loads(runlog_last(sh)[9])
    assert details["freshness"]["day0"] == NEW and details["freshness"]["fresh"] == 10


# ---------------------------------------------------------------- T3 ------ #
@pytest.mark.parametrize("mode", ("legacy", "SPOTTY", "lastscored", ""))
def test_other_modes_are_byte_identical_to_legacy(mode):
    base = fresh_sheet(history_scored_15(), CHAL_NOW)
    run_at(base, P16)
    sh = fresh_sheet(history_scored_15(), CHAL_NOW)
    rc, _ = run_at(sh, P16, day0=mode)
    assert rc == 0
    assert sh.tabs[s1.TAB_HISTORY].rows == base.tabs[s1.TAB_HISTORY].rows
    assert gate_text(sh) == gate_text(base)
    assert runlog_last(sh)[5] == runlog_last(base)[5]


# ---------------------------------------------------------------- T4 ------ #
def test_already_scored_shape_without_new_seats_is_unchanged_under_spot():
    base = fresh_sheet(history_scored_15(), CHAL_PREV)      # today's board == yesterday's
    run_at(base, P16_FULL)
    sh = fresh_sheet(history_scored_15(), CHAL_PREV)
    rc, _ = run_at(sh, P16_FULL, day0="spot")
    assert rc == 0
    assert hist_rows(base)["CHALLENGER"][8] == "n=10"
    assert sh.tabs[s1.TAB_HISTORY].rows == base.tabs[s1.TAB_HISTORY].rows
    assert runlog_last(sh)[5] == runlog_last(base)[5]
    assert gate_text(sh) == gate_text(base)


# ---------------------------------------------------------------- T5 ------ #
def test_full_turnover_is_excluded_under_legacy_and_flat_under_spot():
    fresh = ["A1.US", "A2.US", "A3.US", "A4.US", "A5.US", "A6.US", "A7.US", "A8.US", "A9.US", "A10.US"]
    # the whole previous basket left AND is unpriced today: nothing to pair
    spot = {k: v for k, v in P16.items() if k not in CHAL_PREV}
    spot.update({s: 11.0 for s in fresh})
    sh_leg = fresh_sheet(history_scored_15(), fresh)
    rc, _ = run_at(sh_leg, spot)
    assert rc == 0
    assert hist_rows(sh_leg)["CHALLENGER"][8] == "DAY_EXCLUDED_INFRA fresh=0/10 stale=0 reason=fresh-floor"
    sh = fresh_sheet(history_scored_15(), fresh)
    rc, _ = run_at(sh, spot, day0="spot")
    assert rc == 0
    row = hist_rows(sh)["CHALLENGER"]
    assert float(row[4]) == 0.0 and row[8] == "n=10 day0=10"
    assert "chal fresh=10/10" in runlog_last(sh)[5]


# ---------------------------------------------------------------- T6 ------ #
def test_new_seat_without_a_newer_bar_is_not_day0():
    # the five new seats carry a bar dated the base row's day: not fresh, not day-0
    stale_asof = {s: date(2026, 10, 15) for s in NEW}
    sh = fresh_sheet(history_scored_15(), CHAL_NOW)
    rc, _ = run_at(sh, P16, day0="spot", asof_override=stale_asof)
    assert rc == 0
    row = hist_rows(sh)["CHALLENGER"]
    assert row[8] == "DAY_EXCLUDED_INFRA fresh=5/10 stale=0 reason=fresh-floor"
    v = runlog_last(sh)[5]
    assert "chal fresh=5/10" in v and "day0" not in v and "DAY_EXCLUDED_INFRA" in v
