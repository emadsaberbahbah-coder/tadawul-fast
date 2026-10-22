#!/usr/bin/env python3
"""tests/test_pit_day_key.py - pit_snapshot v1.1.3 TFB_PIT_DAY_KEY: one
Snapshot Date per scheduled slot, so a late run and the next on-time run can
no longer share a label and drop the second run's captures as duplicates.

WHY (2026-10-22)
----------------
The label is the Riyadh wall-clock date at run time and the duplicate guard
is keyed (label, symbol). Run 31136275420 (6 Aug slot, fired 2026-08-07
00:54 UTC) wrote 53 rows labelled 2026-08-07; the on-time run 31217800255
(7 Aug slot, 20:53 UTC) got the same label and logged skipped_dup=36, so 36
symbols' 7 Aug (Fri) captures were never archived while _Run_Log said OK.
v1.1.3 adds a
default-off gate: observe counts the captures the guard would drop; slot
labels each run by the date its slot ends (20:10 UTC on D -> D+1).

Driven through the REAL main() with a fake workbook, a frozen clock (the
module alias pit._dt is replaced by a shim) and the gate environment. No
network, no workbook, no provider.

Cases
  P1  slot: the real Aug fire times write every capture (labels 08-06..08-09)
  P2  off (unset, wallclock and 6 non-words) == the v1.1.1 base copy on rows,
      _Run_Log, stderr and exit code, over a fire-time grid and a legacy tab
  P3  observe: rows and labels equal off; one [PIT-DAYKEY] line and a day_key
      entry count the 3 captures the 7 Aug pair drops
  P4  slot_day_key table; it differs from the wall-clock label exactly on
      fires in [20:10, 21:00) UTC (every minute over 72 hours)
  P5  arming after wall-clock runs: no lost and no doubled (label, symbol)
  P6  slot keeps the guard: a re-run inside one slot writes nothing
  P7  cross_slot_dups pure table (24-wide, legacy 16-wide, unparseable)
  P8  _Run_Log details: off carries exactly v1.1.1's keys; slot adds day_key
  P9  wallclock and observe still read the module-global _today_riyadh
      (open PRs #785/#795 pin the label through it)
  P10 slot dry-run prints the slot label and writes nothing
  P11 the slot is read on the capture clock (a run starting at 20:09:50 and
      capturing at 20:10:20 is labelled by its capture)
  N1  workflow mapping, slot anchor == the cron, version floor, verifier pin
  N2  added lines are ASCII; no function removed; exactly four added

Golden negative: TFB_TEST_PIT_DELIVERED=<base copy> runs every case against
v1.1.1 as the delivered module (all but P2 fail; N2 skips).
Base copy: TFB_TEST_PIT_BASE=<path>, else `git show 57391476:scripts/
pit_snapshot.py` (main after #817; the file is unchanged since v1.1.1);
P2/N2 skip without it.

Run:  python3 -m pytest -q tests/test_pit_day_key.py
  or  python3 tests/test_pit_day_key.py   (PASS k/k + digest)
"""
from __future__ import annotations

import contextlib
import datetime as real_dt
import hashlib
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

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/pit_snapshot.py"
DELIVERED = Path(os.getenv("TFB_TEST_PIT_DELIVERED", "").strip() or SCRIPT)
BASE_SHA = "57391476"   # main after #817; pit_snapshot.py is v1.1.1 there
GATE = "TFB_PIT_DAY_KEY"
SYMS = ["A.US", "B.US", "C.SR"]
AUG = ["2026-08-05T21:22:08", "2026-08-07T00:54:51",
       "2026-08-07T20:53:35", "2026-08-08T20:44:13"]


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


pit = _load("pit_day_key_delivered", DELIVERED)
_BASE = {}


def base():
    """The v1.1.1 copy (main 57391476), or skip."""
    if "mod" not in _BASE:
        path = os.getenv("TFB_TEST_PIT_BASE", "").strip()
        if not path:
            try:
                text = subprocess.run(
                    ["git", "show", BASE_SHA + ":scripts/pit_snapshot.py"],
                    cwd=ROOT, capture_output=True, text=True, check=True).stdout
            except Exception:
                text = ""
            if text:
                tmp = Path(tempfile.mkdtemp()) / "pit_v111_base.py"
                tmp.write_text(text, encoding="utf-8")
                path = str(tmp)
        _BASE["mod"] = _load("pit_day_key_base", path) if path else None
    if _BASE["mod"] is None:
        pytest.skip("base copy unavailable (set TFB_TEST_PIT_BASE)")
    return _BASE["mod"]


# ------------------------------------------------------------ fixtures -----
class FrozenDT(real_dt.datetime):
    now_utc = None

    @classmethod
    def now(cls, tz=None):
        n = cls.now_utc
        return n.astimezone(tz) if tz else n.replace(tzinfo=None)


class DTShim:
    """Stands in for the module alias pit._dt."""
    datetime = FrozenDT
    timezone = real_dt.timezone
    date = real_dt.date
    timedelta = real_dt.timedelta


class WS:
    def __init__(self, vals):
        self.vals = vals
        self.on_get = None   # P11: advances the clock when the ledger is read

    def get(self, _rng):
        if self.on_get is not None:
            self.on_get()
        return [list(r) for r in self.vals]

    def append_rows(self, rows, value_input_option=None):
        self.vals.extend([list(r) for r in rows])

    def append_row(self, row, value_input_option=None):
        self.vals.append(list(row))

    def update(self, values=None, range_name=None):
        self.vals[0] = list(values[0])

    def freeze(self, rows=None):
        return None


class Book:
    def __init__(self, ledger, run_log, syms):
        self.w = {"Src": WS([["Symbol", "Current Price", "P/E Ratio"]]
                            + [[s, "1.5", "9"] for s in syms]),
                  "Dec": WS([["Rank", "Symbol"]]
                            + [[str(i + 1), s] for i, s in enumerate(syms)]),
                  "_Run_Log": run_log}
        if ledger is not None:
            self.w["_PIT_Fundamentals"] = ledger

    def worksheet(self, name):
        if name not in self.w:
            raise KeyError(name)
        return self.w[name]

    def add_worksheet(self, title, rows, cols):
        self.w[title] = WS([[]])
        return self.w[title]


class Rig:
    """One ledger driven through main() by successive frozen fire times."""

    def __init__(self, mod, ledger_rows=None, syms=SYMS):
        self.mod = mod
        self.ledger = WS([list(r) for r in (ledger_rows if ledger_rows
                                            is not None
                                            else [mod.HEADERS])])
        self.run_log = WS([["ts"]])
        self.syms = list(syms)
        self.runs = []

    def fire(self, ts, mode=None, argv=("--write",), today=None, later=None):
        mod = self.mod
        saved_env = {k: os.environ.get(k) for k in (
            GATE, "TFB_PIT_SOURCE_PAGES", "TFB_PIT_SYMBOL_PAGES",
            "TFB_PIT_SHEET")}
        saved = {"_dt": mod._dt, "_open_book": mod._open_book,
                 "_today_riyadh": mod._today_riyadh}
        os.environ.update({"TFB_PIT_SOURCE_PAGES": "Src",
                           "TFB_PIT_SYMBOL_PAGES": "Dec",
                           "TFB_PIT_SHEET": "_PIT_Fundamentals"})
        if mode is None:
            os.environ.pop(GATE, None)
        else:
            os.environ[GATE] = mode
        FrozenDT.now_utc = real_dt.datetime.fromisoformat(ts).replace(
            tzinfo=real_dt.timezone.utc)
        if later is not None:
            after = real_dt.datetime.fromisoformat(later).replace(
                tzinfo=real_dt.timezone.utc)
            self.ledger.on_get = lambda: setattr(FrozenDT, "now_utc", after)
        book = Book(self.ledger, self.run_log, self.syms)
        mod._dt = DTShim
        mod._open_book = lambda: book
        if today is not None:
            mod._today_riyadh = lambda: today
        err = io.StringIO()
        n_ledger, n_log = len(self.ledger.vals), len(self.run_log.vals)
        try:
            with contextlib.redirect_stderr(err):
                rc = mod.main(list(argv))
        finally:
            self.ledger.on_get = None
            for k, v in saved.items():
                setattr(mod, k, v)
            for k, v in saved_env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
        run = {"rc": rc, "stderr": err.getvalue(),
               "rows": self.ledger.vals[n_ledger:],
               "log": self.run_log.vals[n_log:]}
        self.runs.append(run)
        return run


def labels(run):
    return sorted({r[0] for r in run["rows"]})


def daykey_lines(run):
    return [ln for ln in run["stderr"].splitlines() if "[PIT-DAYKEY" in ln]


def details(run):
    return json.loads(run["log"][-1][9])


def _norm(text, mod):
    return text.replace("v" + mod.__version__, "v<V>")


# ---------------------------------------------------------------- P1 -----
def test_p1_slot_writes_every_capture_of_the_august_pair():
    rig = Rig(pit)
    runs = [rig.fire(ts, mode="slot") for ts in AUG]
    assert [r["rc"] for r in runs] == [0, 0, 0, 0]
    assert [len(r["rows"]) for r in runs] == [3, 3, 3, 3]
    assert [labels(r) for r in runs] == [["2026-08-06"], ["2026-08-07"],
                                         ["2026-08-08"], ["2026-08-09"]]
    keys = [(r[0], r[1]) for r in rig.ledger.vals[1:]]
    assert len(keys) == len(set(keys)) == 12
    assert "skipped_dup=0" in runs[2]["stderr"]


# ---------------------------------------------------------------- P2 -----
OFF_WORDS = (None, "wallclock", "", "WallClock", "on", "1", "true",
             "enforce", "next")
GRID = ["2026-10-20T20:10:00", "2026-10-20T20:59:59", "2026-10-20T21:01:00",
        "2026-10-20T23:13:00", "2026-10-21T04:20:00", "2026-10-21T20:30:00"]


def _legacy_ledger(mod):
    rows = [list(mod.LEGACY_HEADERS_16)]
    rows.append(["2026-07-25", "A.US", "Src"] + [""] * 11
                + ["", "2026-07-25 20:15:00"])
    return rows


def test_p2_off_is_byte_identical_to_the_v111_base():
    b = base()
    for word in OFF_WORDS:
        for start in ("current", "legacy", "absent"):
            got = {}
            for name, mod in (("new", pit), ("base", b)):
                if start == "current":
                    rig = Rig(mod)
                elif start == "legacy":
                    rig = Rig(mod, _legacy_ledger(mod))
                else:
                    rig = Rig(mod, [])
                runs = [rig.fire(ts, mode=word) for ts in GRID + AUG]
                got[name] = {
                    "rc": [r["rc"] for r in runs],
                    "stderr": [_norm(r["stderr"], mod) for r in runs],
                    "log": [[_norm(str(c), mod) for c in row]
                            for r in runs for row in r["log"]],
                    "ledger": rig.ledger.vals}
            assert got["new"] == got["base"], (word, start)
    # the base really does drop the 7 Aug captures (the defect P1 fixes)
    rig = Rig(b)
    runs = [rig.fire(ts, mode="slot") for ts in AUG]
    assert [len(r["rows"]) for r in runs] == [3, 3, 0, 3]


# ---------------------------------------------------------------- P3 -----
def test_p3_observe_counts_the_drop_and_changes_no_row():
    off, obs = Rig(pit), Rig(pit)
    for ts in AUG:
        a, o = off.fire(ts), obs.fire(ts, mode="observe")
        assert o["rows"] == a["rows"] and o["rc"] == a["rc"] == 0
        extra = [ln for ln in o["stderr"].splitlines()
                 if ln not in a["stderr"].splitlines()]
        assert extra == daykey_lines(o) and len(extra) == 1, extra
        assert not daykey_lines(a)
    third = obs.runs[2]
    assert labels(third) == [] and "skipped_dup=3" in third["stderr"]
    assert daykey_lines(third)[0].endswith(
        "[PIT-DAYKEY v%s] mode=observe key=2026-08-07 wallclock=2026-08-07 "
        "slot=2026-08-08 cross_slot_dups=3" % pit.__version__)
    d = details(third)
    assert d["day_key"] == {"mode": "observe", "key": "2026-08-07",
                            "wallclock": "2026-08-07", "slot": "2026-08-08",
                            "cross_slot_dups": 3}
    assert [details(r)["day_key"]["cross_slot_dups"] for r in obs.runs] \
        == [0, 0, 3, 0]


# ---------------------------------------------------------------- P4 -----
def test_p4_slot_day_key_table_and_wallclock_agreement():
    utc = real_dt.timezone.utc

    def at(s):
        return real_dt.datetime.fromisoformat(s).replace(tzinfo=utc)

    table = {"2026-10-20T20:09:59": "2026-10-20",
             "2026-10-20T20:10:00": "2026-10-21",
             "2026-10-20T20:30:00": "2026-10-21",
             "2026-10-20T20:59:59": "2026-10-21",
             "2026-10-20T21:00:00": "2026-10-21",
             "2026-10-21T00:00:00": "2026-10-21",
             "2026-10-21T04:20:00": "2026-10-21",
             "2026-10-21T20:09:59": "2026-10-21",
             "2026-10-21T20:10:00": "2026-10-22",
             "2026-12-31T20:30:00": "2027-01-01",
             "2028-02-28T21:00:00": "2028-02-29"}
    for ts, want in table.items():
        assert pit.slot_day_key(at(ts)) == want, ts
    for ts in ("2026-10-20T20:30:00", "2026-10-20T18:00:00"):
        riyadh = at(ts).astimezone(pit._RIYADH)   # 21:00 Riyadh is still 10-20's slot
        assert pit.slot_day_key(riyadh) == table.get(ts, "2026-10-20"), ts
    t = at("2026-10-19T00:00:00")
    differs = 0
    for _ in range(72 * 60):
        wall = t.astimezone(pit._RIYADH).date().isoformat()
        on_time = (20, 10) <= (t.hour, t.minute) < (21, 0)
        assert (pit.slot_day_key(t) != wall) == on_time, t
        assert pit.slot_day_key(t) >= wall, t   # never earlier: no look-ahead
        differs += on_time
        t += real_dt.timedelta(minutes=1)
    assert differs == 3 * 50


# ---------------------------------------------------------------- P5 -----
def test_p5_arming_after_wallclock_runs_loses_and_doubles_nothing():
    rig = Rig(pit)
    plan = [("2026-10-20T23:08:00", None), ("2026-10-21T23:13:00", None),
            ("2026-10-22T20:30:00", "slot"), ("2026-10-23T23:00:00", "slot")]
    runs = [rig.fire(ts, mode=m) for ts, m in plan]
    assert [labels(r) for r in runs] == [["2026-10-21"], ["2026-10-22"],
                                         ["2026-10-23"], ["2026-10-24"]]
    assert [len(r["rows"]) for r in runs] == [3, 3, 3, 3]
    keys = [(r[0], r[1]) for r in rig.ledger.vals[1:]]
    assert len(keys) == len(set(keys))
    assert all(details(r)["day_key"]["cross_slot_dups"] == 0
               for r in runs[2:])


# ---------------------------------------------------------------- P6 -----
def test_p6_slot_keeps_the_guard_for_a_rerun_in_the_same_slot():
    rig = Rig(pit)
    first = rig.fire("2026-10-22T21:30:00", mode="slot")
    again = rig.fire("2026-10-23T02:00:00", mode="slot")
    late = rig.fire("2026-10-23T20:09:00", mode="slot")
    assert len(first["rows"]) == 3
    assert again["rows"] == [] and late["rows"] == []
    assert "skipped_dup=3" in again["stderr"]
    assert details(again)["skipped"] == 3
    assert details(again)["day_key"]["cross_slot_dups"] == 0
    nxt = rig.fire("2026-10-23T20:10:00", mode="slot")
    assert labels(nxt) == ["2026-10-24"] and len(nxt["rows"]) == 3


# ---------------------------------------------------------------- P7 -----
def test_p7_cross_slot_dups_table():
    h = pit.HEADERS
    def row24(day, sym, cap):
        r = [day, sym] + [""] * (len(h) - 3) + [cap]
        assert len(r) == len(h)
        return r
    legacy = ["2026-08-07", "B.US"] + [""] * 13 + ["2026-08-07 00:54:51"]
    assert len(legacy) == len(pit.LEGACY_HEADERS_16)
    vals = [row24("2026-08-07", "a.us", "2026-08-07 00:54:51"),   # other slot
            legacy,                                               # other slot
            row24("2026-08-07", "C.SR", "2026-08-07 20:40:00"),   # same slot
            row24("2026-08-07", "D.US", "2026-08-07 00:54:51"),   # not in run
            row24("2026-08-06", "C.SR", "2026-08-06 01:00:00"),   # other label
            row24("2026-08-07", "C.SR", "garbage"),               # unparseable
            ["2026-08-07"], []]
    syms = ["A.US", "B.US", "C.SR"]
    assert pit.cross_slot_dups(vals, "2026-08-07", "2026-08-08", syms) == 2
    # against slot 08-07 only the 20:40 C.SR capture is from another slot
    assert pit.cross_slot_dups(vals, "2026-08-07", "2026-08-07", syms) == 1
    iso = [row24("2026-08-07", "A.US", "2026-08-07T00:54:51Z")]
    assert pit.cross_slot_dups(iso, "2026-08-07", "2026-08-08", syms) == 1
    assert pit.cross_slot_dups([], "2026-08-07", "2026-08-08", syms) == 0
    assert pit.cross_slot_dups(iso, "2026-08-07", "2026-08-08", ["a.us"]) == 1
    assert pit._captured_at(["x"] * 5) is None
    # sentinel captures at the calendar ends are skipped, never raised
    ends = [row24("2026-08-07", "A.US", "9999-12-31 23:59:59"),
            row24("2026-08-07", "B.US", "0001-01-01 00:00:00")]
    assert pit.cross_slot_dups(ends, "2026-08-07", "2026-08-08", syms) == 0
    for mode in ("observe", "slot"):
        label = "2026-08-07" if mode == "observe" else "2026-08-08"
        rig = Rig(pit, [pit.HEADERS]
                  + [row24(label, "A.US", "9999-12-31 23:59:59")])
        run = rig.fire("2026-08-07T20:53:35", mode=mode)
        assert run["rc"] == 0 and len(run["rows"]) == 2, mode
        assert details(run)["day_key"]["cross_slot_dups"] == 0


# ---------------------------------------------------------------- P8 -----
def test_p8_run_log_details_shape():
    off = Rig(pit).fire("2026-10-22T21:30:00")
    assert set(details(off)) == {"written", "skipped", "missing"}
    assert off["log"][-1][4] == "OK"
    on = Rig(pit).fire("2026-10-22T20:30:00", mode="SLOT")   # case-insensitive
    d = details(on)
    assert set(d) == {"written", "skipped", "missing", "day_key"}
    assert d["day_key"] == {"mode": "slot", "key": "2026-10-23",
                            "wallclock": "2026-10-22", "slot": "2026-10-23",
                            "cross_slot_dups": 0}
    assert "date=2026-10-23 written=3" in on["log"][-1][5]


# ---------------------------------------------------------------- P9 -----
def test_p9_wallclock_and_observe_read_the_module_global_today():
    for mode in (None, "wallclock", "observe"):
        run = Rig(pit).fire("2026-10-22T20:30:00", mode=mode,
                            today="2026-10-17")
        assert labels(run) == ["2026-10-17"], mode
    run = Rig(pit).fire("2026-10-22T20:30:00", mode="slot", today="2026-10-17")
    assert labels(run) == ["2026-10-23"]


# --------------------------------------------------------------- P10 -----
def test_p10_slot_dry_run_prints_the_label_and_writes_nothing():
    rig = Rig(pit)
    run = rig.fire("2026-10-22T20:30:00", mode="slot", argv=("--dry-run",))
    assert run["rc"] == 0 and run["rows"] == [] and run["log"] == []
    assert "date=2026-10-23 symbols=3 to_write=3" in run["stderr"]
    assert len(daykey_lines(run)) == 1 and rig.ledger.vals == [pit.HEADERS]


# --------------------------------------------------------------- P11 -----
def test_p11_slot_is_read_on_the_capture_clock():
    """A run that starts at 20:09:50 UTC and captures at 20:10:20 (the clock
    moves when the ledger is read) belongs to the slot it captured in: its
    slot label equals slot_day_key(Captured At), and observe's count agrees
    with what slot does on the next run."""
    for mode in ("slot", "observe"):
        rig = Rig(pit)
        first = rig.fire("2026-10-21T20:09:50", mode=mode,
                         later="2026-10-21T20:10:20")
        second = rig.fire("2026-10-21T20:40:00", mode=mode)
        assert len(first["rows"]) == 3
        assert {r[-1] for r in first["rows"]} == {"2026-10-21 20:10:20"}
        assert details(first)["day_key"]["slot"] == "2026-10-22"
        assert second["rows"] == [] and "skipped_dup=3" in second["stderr"]
        assert details(second)["day_key"]["cross_slot_dups"] == 0
        if mode == "slot":
            assert labels(first) == ["2026-10-22"]
            for row in rig.ledger.vals[1:]:
                cap = pit._captured_at(row)
                assert row[0] == pit.slot_day_key(cap), row[:2]
        else:
            assert labels(first) == ["2026-10-21"]   # wall-clock at start


# ---------------------------------------------------------------- N1 -----
def test_n1_workflow_anchor_version_and_pin():
    wf = (ROOT / ".github/workflows/pit_snapshot.yml").read_text(
        encoding="utf-8")
    assert re.search(r"TFB_PIT_DAY_KEY: \$\{\{ vars\.TFB_PIT_DAY_KEY \|\| "
                     r"'wallclock' \}\}", wf)
    crons = re.findall(r'cron:\s*"(\d+) (\d+) \* \* \*"', wf)
    assert len(crons) == 1
    assert (int(crons[0][1]), int(crons[0][0])) == pit._SLOT_UTC_HM
    v = tuple(int(x) for x in pit.__version__.split("."))
    assert v >= (1, 1, 3)
    src = DELIVERED.read_text(encoding="utf-8")
    assert "v%s (W-6" % pit.__version__ in src.splitlines()[4]
    vd = (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert '("pit_snapshot", "%s", "pit snapshot")' % pit.__version__ in vd
    assert src.index("# v1.1.1 WHY (2026-10-11)") \
        < src.index("# v1.1.3 WHY (2026-10-22)") \
        < src.index('__version__ = "%s"' % pit.__version__)
    assert "TFB_PIT_DAY_KEY" in src.split('"""')[1]


# ---------------------------------------------------------------- N2 -----
def test_n2_added_lines_are_ascii_and_nothing_removed():
    b = base()
    if pit.__version__ != "1.1.3":
        pytest.skip("exact 57391476 -> 1.1.3 diff checks apply to 1.1.3 only")
    import ast
    import difflib
    old = Path(b.__file__).read_text(encoding="utf-8").splitlines()
    new = DELIVERED.read_text(encoding="utf-8").splitlines()
    diff = list(difflib.unified_diff(old, new, lineterm="", n=0))
    added = [ln[1:] for ln in diff
             if ln.startswith("+") and not ln.startswith("+++")]
    removed = [ln[1:] for ln in diff
               if ln.startswith("-") and not ln.startswith("---")]
    title = ("PIT Snapshot \u2014 v%s (W-6, Execution Plan v2.1; "
             "the evidence-preservation stopgap)")
    assert removed == [title % "1.1.1", '__version__ = "1.1.1"',
                       '        append_run_log(book, "OK", msg,',
                       '                       {"written": len(rows), '
                       '"skipped": skipped,',
                       '                        "missing": missing})'], removed
    # The docstring title keeps its pre-existing em dash; only its version moves.
    assert added.count(title % "1.1.3") == 1
    fresh = [ln for ln in added if ln != title % "1.1.3"]
    assert fresh and all(ln.isascii() for ln in fresh), \
        [ln for ln in fresh if not ln.isascii()][:3]

    def fns(text):
        return {n.name for n in ast.walk(ast.parse(text))
                if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    assert not fns("\n".join(old)) - fns("\n".join(new))
    assert fns("\n".join(new)) - fns("\n".join(old)) == {
        "_pit_day_key_mode", "slot_day_key", "_captured_at",
        "cross_slot_dups"}


def _digest():
    parts = [name for name, fn in sorted(globals().items())
             if name.startswith("test_") and callable(fn)]
    parts.append(pit.__version__)
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()[:16]


if __name__ == "__main__":
    tests = [(k, v) for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    passed = skipped = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
            print("ok  ", name)
        except pytest.skip.Exception as exc:
            skipped += 1
            print("SKIP", name, "::", exc)
    print(f"PASS {passed}/{len(tests)}"
          + (f" (skipped {skipped})" if skipped else "")
          + f" digest {_digest()}")
