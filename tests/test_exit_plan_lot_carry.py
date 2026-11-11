#!/usr/bin/env python3
"""tests/test_exit_plan_lot_carry.py - scripts/exit_plan.py 1.2.0 [LOT-CARRY].

read_lots merges a symbol's active lots (weighted buy price, latest buy date) and the ledger keys the holding by
both. A sale from one of several lots, or a closed lot, changes that key, prior_row finds nothing, and the holding
loses the highest stop ever recommended and its once-only TAKE PROFIT flag: 1.1.0 then issues 'sell half' again
and can publish a lower stop. With TFB_EXIT_LOT_CARRY on, the holding's earlier rows carry over.

Runs as a script ("PASS k/k") and under pytest. Zero network: the real run() reads an in-memory workbook and a fake
Yahoo chart opener. Controls: TFB_TEST_CARRY_TARGET (the exit_plan.py under test; the untouched main copy there is
the golden negative), TFB_TEST_EXIT_BASE (a path to the main 53f2abdc copy instead of `git show`),
TFB_TEST_REQUIRE_BASE=1 (a missing base FAILS the base legs instead of skipping them; set in CI).

  L0  version floor; the gate words (unset and anything else read off)
  L1  two lots (230 @ 100.00, 10 @ 104.00): TAKE PROFIT on the 110 close, 120 shares of the first lot sold; the next
      close: on -> no second TAKE PROFIT, TP1 kept, the stop not lower, the note names the earlier key, the e-mail
      says 'hold all'; off (= 1.1.0) -> TAKE PROFIT again and 'take profit' in the e-mail subject
  L2  two fills a few cents apart on one day, a sale after the stop was raised: off publishes a LOWER stop with
      'RAISE STOP' and loses the TP1 flag; on keeps the stop and the flag and reads HOLD
  L3  the newest lot closed entirely (the key's date moves earlier): carried
  L4  a top-up (a lot bought since): nothing carried - on equals off
  L5  sold out, bought again later: nothing carried - on equals off
  L6  a single lot sold down (the key does not change), and a second holding beside a carried one: on equals off
  L7  a second run on the same close after the change: the same action, the same stop, no RAISE STOP of its own
  L8  OFF IDENTITY: every scenario, gate unset / off / junk, against the untouched 1.1.0 module: ledger, page,
      e-mail and every log line (the version tokens folded)
  L9  carry_rows, directly: symbol, the earliest-buy-date window, the top-up guard, malformed keys, relabel
  L10 the workflow maps the gate (default 'off'), the manifest pins the version, ci.yml runs this file with the
      base fetched by SHA

Run: python tests/test_exit_plan_lot_carry.py  |  pytest -q tests/test_exit_plan_lot_carry.py
"""
from __future__ import annotations

import contextlib
import copy
import datetime as dt
import importlib.util
import io
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

BASE_SHA = "53f2abdc5be5c13294f4009b3b1bc3d1964907d5"     # main: exit_plan 1.1.0 (#956, #957, #959)
GATE = "TFB_EXIT_LOT_CARRY"
DELIVERED = os.getenv("TFB_TEST_CARRY_TARGET", "").strip() or str(ROOT / "scripts" / "exit_plan.py")
UTC = dt.timezone.utc


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


ep = _load("ep_lot_carry", DELIVERED)
_BASE = {}


def base():
    """The untouched main copy (53f2abdc, exit_plan 1.1.0), or skip (or fail when TFB_TEST_REQUIRE_BASE=1)."""
    if "mod" not in _BASE:
        path = os.getenv("TFB_TEST_EXIT_BASE", "").strip()
        if not path:
            try:
                text = subprocess.run(["git", "show", BASE_SHA + ":scripts/exit_plan.py"], cwd=ROOT,
                                      capture_output=True, text=True, check=True).stdout
            except Exception:
                text = ""
            if text:
                d = Path(tempfile.mkdtemp()) / "base"
                d.mkdir()
                (d / "exit_plan_v110_main.py").write_text(text, encoding="utf-8")
                path = str(d / "exit_plan_v110_main.py")
        _BASE["mod"] = _load("ep_lot_carry_base", path) if path else None
    if _BASE["mod"] is None:
        if os.getenv("TFB_TEST_REQUIRE_BASE", "").strip() == "1":
            raise AssertionError("base copy required (TFB_TEST_REQUIRE_BASE=1) but unavailable")
        pytest.skip("base copy unavailable (set TFB_TEST_EXIT_BASE)")
    return _BASE["mod"]


# ----------------------------------------------------------------------------- fakes
class WorksheetNotFound(Exception):
    pass


def _colnum(letters):
    n = 0
    for ch in letters:
        n = n * 26 + ord(ch) - 64
    return n


class Sheet:
    def __init__(self, rows):
        self.values = copy.deepcopy(rows)
        self.row_count, self.col_count = 500, 30

    def get(self, rng):
        return copy.deepcopy(self.values)

    def batch_get(self, ranges):
        out = []
        for r in ranges:
            m = re.match(r"([A-Z]+)(\d+):([A-Z]+)$", r)
            c, start = _colnum(m.group(1)) - 1, int(m.group(2)) - 1
            out.append([[row[c] if c < len(row) else ""] for row in self.values[start:]])
        return out

    def resize(self, rows=None, cols=None):
        pass

    def update(self, values=None, range_name=None, value_input_option=None):
        # what a later read returns: the non-empty rows, every cell as the text Sheets shows
        self.values = [["" if c is None else str(c) for c in r] for r in values if any(str(c) != "" for c in r)]


class Book:
    def __init__(self, tabs):
        self.tabs = dict(tabs)

    def worksheet(self, name):
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return self.tabs[name]

    def add_worksheet(self, title, rows, cols):
        self.tabs[title] = Sheet([])
        return self.tabs[title]


class Resp:
    def __init__(self, b):
        self.b = b

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def read(self):
        return self.b


D0 = dt.date(2026, 9, 1)


def days(n, start=D0):
    out, d = [], start
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


DAYS = days(60)


def opener_for(series):
    """A Yahoo chart opener serving `series` {YAHOO_SYMBOL: [closes]}: a 1.5% high-low spread around each close."""
    def opener(req, timeout):
        sym = req.full_url.split("/chart/")[1].split("?")[0]
        if sym not in series:
            e = Exception("404")
            e.code = 404
            raise e
        closes = series[sym]
        ts = [int(dt.datetime(d.year, d.month, d.day, 14, 30, tzinfo=UTC).timestamp()) for d in DAYS[:len(closes)]]
        ind = {"quote": [{"open": list(closes), "high": [c * 1.015 for c in closes],
                          "low": [c * 0.985 for c in closes], "close": list(closes)}]}
        return Resp(json.dumps({"chart": {"result": [{"meta": {"gmtoffset": 0}, "timestamp": ts,
                                                       "indicators": ind}]}}).encode())
    return opener


LOT_HDR = ["Symbol", "Name", "Ccy", "Status", "Buy Date", "Buy Price", "Shares"]


def lot(sym, i, price, shares, status="Active"):
    return [sym, sym, "USD", status, DAYS[i].isoformat(), str(price), str(shares)]


def morning_after(i, hours=0):
    """07:30 Riyadh on the day after bar i (its close is complete), plus `hours`."""
    d = DAYS[i] + dt.timedelta(days=1)
    return dt.datetime(d.year, d.month, d.day, 4, 30, tzinfo=UTC) + dt.timedelta(hours=hours)


FLAT = [100.0] * 30
SCENARIOS = {
    # L1: two lots at different prices; TAKE PROFIT on the 110 close; 120 shares of the first lot sold
    "l1_sell_half_from_one_lot": [
        (31, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10), lot("BBB.US", 28, "50", 40)],
         {"AAA": FLAT + [104.0, 110.0], "BBB": [50.0] * 32}),
        (32, 0, [lot("AAA.US", 28, "100.00", 110), lot("AAA.US", 30, "104.00", 10), lot("BBB.US", 28, "50", 40)],
         {"AAA": FLAT + [104.0, 110.0, 110.5], "BBB": [50.0] * 33}),
    ],
    # L2: two fills a few cents apart on one day; TAKE PROFIT, a raised stop, then a sale on a wide down day
    "l2_close_fills_stop_drop": [
        (31, 0, [lot("AAA.US", 30, "100.00", 230), lot("AAA.US", 30, "100.30", 10)],
         {"AAA": FLAT + [101.0, 110.0]}),
        (32, 0, [lot("AAA.US", 30, "100.00", 230), lot("AAA.US", 30, "100.30", 10)],
         {"AAA": FLAT + [101.0, 110.0, 118.0]}),
        (33, 0, [lot("AAA.US", 30, "100.00", 110), lot("AAA.US", 30, "100.30", 10)],
         {"AAA": FLAT + [101.0, 110.0, 118.0, 108.0]}),
    ],
    # L3: the newest lot closed entirely: the key's date moves back to the first lot
    "l3_newest_lot_closed": [
        (31, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10)],
         {"AAA": FLAT + [104.0, 110.0]}),
        (32, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10, "Closed")],
         {"AAA": FLAT + [104.0, 110.0, 110.5]}),
    ],
    # L4: a top-up after TAKE PROFIT: a new lot bought since - a new position, nothing carried
    "l4_top_up": [
        (31, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10)],
         {"AAA": FLAT + [104.0, 110.0]}),
        (32, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10), lot("AAA.US", 32, "110.50", 20)],
         {"AAA": FLAT + [104.0, 110.0, 110.5]}),
        (33, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10), lot("AAA.US", 32, "110.50", 20)],
         {"AAA": FLAT + [104.0, 110.0, 110.5, 111.0]}),
    ],
    # L5: sold out, bought again later: the old rows predate the new holding's first buy - nothing carried
    "l5_sold_out_rebought": [
        (31, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10)],
         {"AAA": FLAT + [104.0, 110.0]}),
        (34, 0, [lot("AAA.US", 28, "100.00", 230, "Closed"), lot("AAA.US", 30, "104.00", 10, "Closed"),
                 lot("AAA.US", 33, "105.00", 50)],
         {"AAA": FLAT + [104.0, 110.0, 108.0, 105.0, 106.0]}),
    ],
    # L6: a single lot sold down: the key does not change
    "l6_single_lot": [
        (31, 0, [lot("AAA.US", 30, "100.00", 240)], {"AAA": FLAT + [104.0, 110.0]}),
        (32, 0, [lot("AAA.US", 30, "100.00", 120)], {"AAA": FLAT + [104.0, 110.0, 110.5]}),
    ],
    # L7: L1, then a second run on the same close two hours later
    "l7_same_close_rerun": [
        (31, 0, [lot("AAA.US", 28, "100.00", 230), lot("AAA.US", 30, "104.00", 10)],
         {"AAA": FLAT + [104.0, 110.0]}),
        (32, 0, [lot("AAA.US", 28, "100.00", 110), lot("AAA.US", 30, "104.00", 10)],
         {"AAA": FLAT + [104.0, 110.0, 110.5]}),
        (32, 2, [lot("AAA.US", 28, "100.00", 110), lot("AAA.US", 30, "104.00", 10)],
         {"AAA": FLAT + [104.0, 110.0, 110.5]}),
    ],
}


@contextlib.contextmanager
def env(gate):
    saved = {k: v for k, v in os.environ.items() if k.startswith("TFB_EXIT_")}
    try:
        for k in saved:
            os.environ.pop(k, None)
        os.environ["TFB_EXIT_PLAN"] = "on"
        os.environ["TFB_EXIT_MAIL"] = "1"
        if gate is not None:
            os.environ[GATE] = gate
        yield
    finally:
        for k in [k for k in os.environ if k.startswith("TFB_EXIT_")]:
            os.environ.pop(k, None)
        os.environ.update(saved)


def play(mod, name, gate):
    """Run a scenario; return [(rc, stdout, ledger, page, mails)] after each step."""
    book = Book({})
    out = []
    for i, hours, lots, series, in SCENARIOS[name]:
        book.tabs["_Portfolio_CostBasis"] = Sheet([LOT_HDR] + lots)
        mails = []
        buf = io.StringIO()
        with env(gate), contextlib.redirect_stdout(buf):
            rc = mod.run(False, True, book=book, now=morning_after(i, hours), opener=opener_for(series),
                         mailer=lambda *a: mails.append(a))
        led = copy.deepcopy(book.tabs["_Exit_Plan"].values) if "_Exit_Plan" in book.tabs else None
        page = copy.deepcopy(book.tabs["Exit_Plan"].values) if "Exit_Plan" in book.tabs else None
        out.append((rc, buf.getvalue(), led, page, mails))
    return out


def rows(led, sym=None, day=None):
    h = ep.LEDGER_HEADERS
    return [dict(zip(h, r)) for r in led[1:] if (sym is None or r[2] == sym) and (day is None or r[0] == day)]


def last_row(led, sym):
    return rows(led, sym)[-1]


def num(x):
    return float(x) if str(x).strip() not in ("", "None") else None


def page_line(page, sym):
    return [r for r in page if r and r[0] == sym][0]


def fold(x):
    """The only bytes 1.2.0 may differ in while the gate is off: its version token."""
    s = json.dumps(x, sort_keys=True, default=str)
    for v in ("1.2.0", "1.1.0"):
        s = s.replace("exit_plan %s" % v, "exit_plan V").replace("[exit_plan %s]" % v, "[exit_plan V]")
    return s


# ----------------------------------------------------------------------------- L0
def test_l0_version_floor_and_gate_words():
    assert tuple(int(x) for x in ep.__version__.split(".")) >= (1, 2, 0)
    for word, want in ((None, False), ("off", False), ("", False), ("no", False), ("0", False), ("enforce", False),
                       ("on", True), ("ON", True), (" on ", True), ("1", True), ("true", True), ("Yes", True)):
        with env(word):
            assert ep.lot_carry_on() is want, word


# ----------------------------------------------------------------------------- L1
def test_l1_no_second_take_profit_after_selling_half():
    on = play(ep, "l1_sell_half_from_one_lot", "on")
    off = play(ep, "l1_sell_half_from_one_lot", None)
    for steps in (on, off):
        r0 = last_row(steps[0][2], "AAA.US")
        assert r0["Action"] == "TAKE PROFIT" and r0["TP1 Reached"] == "yes"
        assert steps[0][4][0][0].endswith("1 action: AAA.US take profit")
    stop0 = num(last_row(on[0][2], "AAA.US")["Stop"])
    rc, log, led, page, mails = on[1]
    r1 = last_row(led, "AAA.US")
    assert rc == 0 and r1["Action"] != "TAKE PROFIT" and r1["TP1 Reached"] == "yes"
    assert r1["Lot"] != last_row(on[0][2], "AAA.US")["Lot"]                    # the key did change
    assert num(r1["Stop"]) >= stop0
    note = [ln for ln in log.splitlines() if "AAA.US" in ln and "the lots changed" in ln]
    assert len(note) == 1 and note[0].endswith(
        "the lots changed (a sale or a closed lot): the stop and the first-target state carry over from %s"
        % last_row(on[0][2], "AAA.US")["Lot"])
    assert mails[0][0].endswith("hold all") and "take profit" not in mails[0][0]
    assert "TAKE PROFIT" not in " ".join(str(c) for c in page_line(page, "AAA.US"))
    # 1.1.0 (off): the holding lost its history - 'sell half' again
    r1off = last_row(off[1][2], "AAA.US")
    assert r1off["Action"] == "TAKE PROFIT" and "take profit" in off[1][4][0][0]
    assert "the lots changed" not in off[1][1]
    # the other holding is untouched by the carry
    assert last_row(led, "BBB.US") == last_row(off[1][2], "BBB.US")


# ----------------------------------------------------------------------------- L2
def test_l2_close_fills_off_lowers_the_stop_on_keeps_it():
    on = play(ep, "l2_close_fills_stop_drop", "on")
    off = play(ep, "l2_close_fills_stop_drop", None)
    assert [last_row(s[2], "AAA.US")["Action"] for s in on[:2]] == ["TAKE PROFIT", "RAISE STOP"]
    raised = num(last_row(on[1][2], "AAA.US")["Stop"])
    assert raised == num(last_row(off[1][2], "AAA.US")["Stop"])
    r_off = last_row(off[2][2], "AAA.US")
    assert num(r_off["Stop"]) < raised and r_off["Action"] == "RAISE STOP"      # 1.1.0: a lower stop, 'raised'
    assert r_off["TP1 Reached"] == "no"                                          # 1.1.0: the flag is lost too
    r_on = last_row(on[2][2], "AAA.US")
    assert num(r_on["Stop"]) == raised and r_on["Action"] == "HOLD" and r_on["TP1 Reached"] == "yes"


# ----------------------------------------------------------------------------- L3
def test_l3_newest_lot_closed_is_carried():
    on = play(ep, "l3_newest_lot_closed", "on")
    off = play(ep, "l3_newest_lot_closed", None)
    k0, k1 = last_row(on[0][2], "AAA.US")["Lot"], last_row(on[1][2], "AAA.US")["Lot"]
    assert k0.split("|")[1] == DAYS[30].isoformat() and k1.split("|")[1] == DAYS[28].isoformat()
    assert last_row(on[1][2], "AAA.US")["Action"] != "TAKE PROFIT"
    assert last_row(off[1][2], "AAA.US")["Action"] == "TAKE PROFIT"
    assert num(last_row(on[1][2], "AAA.US")["Stop"]) >= num(last_row(on[0][2], "AAA.US")["Stop"])


# ----------------------------------------------------------------------------- L4 / L5 / L6
@pytest.mark.parametrize("name", ["l4_top_up", "l5_sold_out_rebought", "l6_single_lot"])
def test_l4_l5_l6_nothing_to_carry_on_equals_off(name):
    on = play(ep, name, "on")
    off = play(ep, name, None)
    assert fold(on) == fold(off)
    assert all("the lots changed" not in s[1] for s in on)


def test_l4_a_top_up_stays_a_new_position():
    on = play(ep, "l4_top_up", "on")
    keys = [last_row(s[2], "AAA.US")["Lot"] for s in on]
    assert keys[0] != keys[1] == keys[2] and keys[1].split("|")[1] == DAYS[32].isoformat()


# ----------------------------------------------------------------------------- L7
def test_l7_same_close_rerun_after_the_change():
    on = play(ep, "l7_same_close_rerun", "on")
    first, second = last_row(on[1][2], "AAA.US"), last_row(on[2][2], "AAA.US")
    assert first["Action"] == second["Action"] != "TAKE PROFIT" and second["Action"] != "RAISE STOP"
    assert first["Stop"] == second["Stop"] and second["TP1 Reached"] == "yes"
    assert all("the lots changed" in s[1] for s in on[1:])          # no row on an earlier close under the new key yet
    assert len(rows(on[2][2], "AAA.US", morning_after(32).astimezone(ep.RIYADH).date().isoformat())) == 1


# ----------------------------------------------------------------------------- L8
@pytest.mark.parametrize("name", sorted(SCENARIOS))
def test_l8_off_identity_against_main(name):
    b = base()
    want = fold(play(b, name, None))
    for gate in (None, "off", "OFF ", "no", "0", "enforce", ""):
        assert fold(play(ep, name, gate)) == want, (name, gate)


# ----------------------------------------------------------------------------- L9
def test_l9_carry_rows():
    d28, d30, d33 = DAYS[28], DAYS[30], DAYS[33]

    def r(sym, key_day, price, extra=None):
        d = {"Symbol": sym, "Lot": "%s|%s|%.4f" % (sym, key_day.isoformat(), price), "Stop": 100.0}
        d.update(extra or {})
        return d
    old = r("AAA.US", d30, 100.1667)
    cur = "AAA.US|%s|100.3333" % d30.isoformat()
    led = [old, r("BBB.US", d30, 50.0), {"Symbol": "AAA.US", "Lot": "garbage", "Stop": 1.0},
           {"Symbol": "AAA.US", "Lot": "BBB.US|%s|1.0000" % d30.isoformat(), "Stop": 1.0}, dict(old, Lot=cur)]
    got = ep.carry_rows(led, "AAA.US", cur, d30, d28)
    assert got == [dict(old, Lot=cur, **{"Carried From": old["Lot"]})]
    assert led[0]["Lot"] == old["Lot"]                                             # the ledger is not mutated
    assert ep.carry_rows(led, "aaa.us", cur, d30, d28)[0]["Lot"] == cur           # symbol case
    assert ep.carry_rows(led, "AAA.US", cur, d33, d28) == []                       # a lot bought since: top-up
    assert ep.carry_rows(led, "AAA.US", cur, d30, d33) == []                       # an older holding
    assert ep.carry_rows(led, "AAA.US", cur, d28, d28)[0]["Carried From"] == old["Lot"]   # newest lot closed
    assert ep.carry_rows(led, "CCC.US", cur, d30, d28) == [] and ep.carry_rows([], "AAA.US", cur, d30, d28) == []
    lots = ep.read_lots([LOT_HDR, lot("AAA.US", 30, "104", 10), lot("AAA.US", 28, "100", 230),
                         lot("AAA.US", 25, "90", 5, "Closed")])
    assert lots["AAA.US"]["first"] == d28 and lots["AAA.US"]["date"] == d30


# ----------------------------------------------------------------------------- L10
def test_l10_workflow_manifest_ci():
    raw = (ROOT / ".github/workflows/exit_plan.yml").read_text(encoding="utf-8")
    wf = yaml.safe_load(raw)
    step = [s for s in wf["jobs"]["exit_plan"]["steps"] if "exit_plan.py" in str(s.get("run", ""))][0]
    assert step["env"][GATE] == "${{ vars.TFB_EXIT_LOT_CARRY || 'off' }}"
    src = Path(DELIVERED).read_text(encoding="utf-8")
    assert '"%s"' % GATE in src
    manifest = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("exit_plan", "%s", "exit plan")' % ep.__version__ in manifest
    ci = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    hits = [(j, s) for j, job in ci["jobs"].items() for s in job.get("steps", [])
            if "tests/test_exit_plan_lot_carry.py" in str(s.get("run", ""))]
    assert len(hits) == 1 and hits[0][0] == "lean-unit"
    s = hits[0][1]
    assert str((s.get("env") or {}).get("TFB_TEST_REQUIRE_BASE")) == "1" and BASE_SHA in s["run"]
    assert not re.search(r"create_order|place_order|requests\.post|cancel_order", src)


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    passed = skipped = total = 0
    for name, fn in tests:
        params = [m for m in getattr(fn, "pytestmark", []) if m.name == "parametrize"]
        calls = ([((p,), "%s[%s]" % (name, p)) for p in params[0].args[1]] if params else [((), name)])
        for args, label in calls:
            total += 1
            try:
                fn(*args)
                passed += 1
                print("ok  ", label)
            except pytest.skip.Exception as exc:
                skipped += 1
                print("SKIP", label, "::", exc)
            except Exception as exc:  # noqa: BLE001
                print("FAIL", label, "::", type(exc).__name__, str(exc)[:300])
    print("PASS %d/%d" % (passed, total) + (" (skipped %d)" % skipped if skipped else ""))
    sys.exit(0 if passed + skipped == total else 1)
