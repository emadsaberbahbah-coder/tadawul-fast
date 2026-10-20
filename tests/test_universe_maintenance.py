"""tests/test_universe_maintenance.py -- scripts/universe_maintenance.py v1.0.0 + its workflow.

Offline and read-only: in-memory fakes stand in for the workbook (gspread) and for EODHD; no
credentials, no network, no Google Sheet writes.

Cases
  U1  modes: unset/junk = off; off needs no key and touches nothing; --selftest passes
  U2  red predicate = the v1.2.24 rule (regex, blank / unreadable / > 2-day-old Last Updated)
  U3  watch streak: +1 per new weekday, same-day rerun and weekends do not count, recovery drops
  U4  retirement rules: held (also bare), non-US, still listed, list unavailable, floor, per-run cap
  U5  Israeli exclusion (ISIN, fundamentals country / address, curated list) and the curated file
  U6  addition filters, cap, fundamentals budget and per-run duplicate guard
  U7  weekly cadence and the observe -> enforce switch
  U8  observe run end-to-end: Global_Markets and _Retired_Symbols untouched, own tabs + one log row
  U9  enforce run end-to-end: retirement rows aligned to the header, additions after the last row,
      grid extended, idempotent within the week, retired symbols never re-added
  U10 fail-safes: partial page read, US list failure, screener failure, title rows above a header
  U11 EODHD client: the API key never reaches an error message; screener offset bound
  U12 workflow contract: default off, enforce only via the Variable, overlap guard, pins, no
      expression inside run blocks, the sync's retirement mappings, no audit findings
"""
from __future__ import annotations

import collections
import datetime as dt
import importlib.util
import json
import re
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("universe_maintenance", ROOT / "scripts" / "universe_maintenance.py")
um = importlib.util.module_from_spec(SPEC)
sys.modules["universe_maintenance"] = um
SPEC.loader.exec_module(um)

WORKFLOW = ROOT / ".github" / "workflows" / "universe_maintenance.yml"
TUE = dt.datetime(2026, 10, 20, 6, 41)          # Tuesday 09:41 Riyadh
GM_HEADER = ["Symbol", "Name", "Exchange", "Last Updated (UTC)", "Warnings"]


# --------------------------------------------------------------------------- #
# fakes                                                                       #
# --------------------------------------------------------------------------- #
def _col_index(letters: str) -> int:
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch) - 64)
    return n - 1


class FakeWS:
    def __init__(self, title, values=None, rows=None, cols=26):
        self.title = title
        self.values = [list(r) for r in (values or [])]
        self.row_count = rows if rows is not None else len(self.values) + 10
        self.col_count = cols
        self.calls = []

    def row_values(self, n):
        return list(self.values[n - 1]) if n - 1 < len(self.values) else []

    def batch_get(self, ranges):
        out = []
        for rg in ranges:
            m = re.match(r"^([A-Z]+)(\d+):([A-Z]+)$", rg)
            col, start = _col_index(m.group(1)), int(m.group(2))
            vals = [([r[col]] if col < len(r) and r[col] != "" else []) for r in self.values[start - 1:]]
            while vals and not vals[-1]:
                vals.pop()
            out.append(vals)
        return out

    def get_all_values(self):
        return [[("" if v is None else str(v)) for v in r] for r in self.values]

    def update(self, values, range_name, value_input_option=None):
        m = re.match(r"^([A-Z]+)(\d+)", range_name)
        c0, r0 = _col_index(m.group(1)), int(m.group(2)) - 1
        width = max((len(r) for r in values), default=0)
        if r0 + len(values) > self.row_count or c0 + width > self.col_count:
            raise AssertionError(f"{self.title}: {range_name} exceeds grid {self.row_count}x{self.col_count}")
        self.calls.append(("update", range_name, len(values)))
        for i, row in enumerate(values):
            while len(self.values) <= r0 + i:
                self.values.append([])
            line = self.values[r0 + i]
            for j, v in enumerate(row):
                while len(line) <= c0 + j:
                    line.append("")
                line[c0 + j] = v

    def append_rows(self, rows, value_input_option=None):
        self.calls.append(("append_rows", len(rows)))
        self.values.extend(list(r) for r in rows)
        self.row_count = max(self.row_count, len(self.values))

    def append_row(self, row, value_input_option=None):
        self.append_rows([row], value_input_option)

    def add_rows(self, n):
        self.calls.append(("add_rows", n))
        self.row_count += n

    def add_cols(self, n):
        self.col_count += n

    def clear(self):                               # must never be used (clear-then-write)
        raise AssertionError("clear() called")


class WorksheetNotFound(Exception):                # same class name as gspread.exceptions.WorksheetNotFound
    pass


class FakeSheet:
    def __init__(self, *tabs, broken=()):
        self.tabs = {t.title: t for t in tabs}
        self.created = []
        self.broken = set(broken)

    def worksheet(self, name):
        if name in self.broken:
            raise RuntimeError("APIError: [503] The service is currently unavailable.")
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return self.tabs[name]

    def add_worksheet(self, title, rows, cols):
        ws = FakeWS(title, [], rows=rows, cols=cols)
        self.tabs[title] = ws
        self.created.append(title)
        return ws


class FakeEod:
    def __init__(self, us_list, screener=(), fundamentals=None, fail_list=False, fail_screener=False):
        self._us, self._scr, self._fund = us_list, list(screener), dict(fundamentals or {})
        self.fail_list, self.fail_screener = fail_list, fail_screener
        self.calls = collections.Counter()

    def us_list(self):
        self.calls["us_list"] += 1
        if self.fail_list:
            raise RuntimeError("EODHD exchange-symbol-list failed: URLError")
        return dict(self._us)

    def screener_us(self, want):
        self.calls["screener"] += 1
        if self.fail_screener:
            raise RuntimeError("EODHD screener failed: HTTPError")
        return self._scr[:want]

    def fundamentals(self, code):
        self.calls["fundamentals"] += 1
        f = self._fund.get(code, {"General": {"CountryISO": "US", "Name": code}})
        if isinstance(f, Exception):
            raise f
        return f


def _cfg(**over):
    c = {"mode": "observe", "add_max": 25, "retire_max": 25, "max_rows": 6750, "min_rows": 6512, "red_days": 10,
         "stale_days": 2, "screen_n": 1000, "fund_max": 120, "add_weekday": "SUN", "add_foreign": False,
         "retire_tab": "_Retired_Symbols", "retire_mode": "enforce"}
    c.update(over)
    return c


FRESH = "2026-10-20T01:00:00+00:00"
SPECIAL = [  # symbol, name, warnings
    ("DEAD.US", "Dead Corp", "identity_quarantined:kept_last_good:v6.25.1; acquisition_status:preserved"),
    ("LIVEQ.US", "Live Quarantined Inc", "identity_quarantined:v6.24.0"),
    ("HELD", "Held Company", "acquisition_status:unavailable"),
    ("GONE.L", "Gone plc", "quote_current_price_missing"),
    ("DUPX.US", "Duplicate Holdings Inc", ""),
]


def _gm(n_plain=6600):
    rows = [GM_HEADER] + [[f"P{i:04d}.US", f"Plain Company {i}", "NYSE", FRESH, ""] for i in range(n_plain)]
    rows += [[s, nm, "NYSE", FRESH, w] for s, nm, w in SPECIAL]
    return FakeWS("Global_Markets", rows, rows=len(rows) + 5)


def _us_list(extra=None):
    us = {f"P{i:04d}": {"Type": "Common Stock", "Exchange": "NYSE", "Isin": f"US{i:010d}"} for i in range(6600)}
    us["LIVEQ"] = {"Type": "Common Stock", "Exchange": "NASDAQ", "Isin": "US0000000001"}
    us["HELD"] = {"Type": "Common Stock", "Exchange": "NYSE", "Isin": "US0000000002"}
    us.update(extra or {})
    return us


CANDIDATES = [
    {"Code": "P0001", "Name": "Plain Company 1", "MarketCap": 9e11},          # already on the page
    {"Code": "NEWA", "Name": "Newa Corp", "MarketCap": 8e11},                 # added
    {"Code": "DUPY", "Name": "Duplicate Holdings Inc. Class B", "MarketCap": 7e11},
    {"Code": "OTCX", "Name": "Otc Example AG", "MarketCap": 6e11},
    {"Code": "ETFX", "Name": "Some Fund Trust", "MarketCap": 5e11},
    {"Code": "ILCO", "Name": "Il Domiciled Ltd", "MarketCap": 4e11},
    {"Code": "MBLY", "Name": "Mobileye Global Inc", "MarketCap": 3.9e11},
    {"Code": "ADRX", "Name": "Foreign Adr Co", "MarketCap": 3.8e11},
    {"Code": "HQIL", "Name": "Herzliya Hq Inc", "MarketCap": 3.7e11},
    {"Code": "FERR", "Name": "Fundamentals Error Inc", "MarketCap": 3.6e11},
    {"Code": "RETD", "Name": "Retired Before Inc", "MarketCap": 3.5e11},
    {"Code": "NEWB", "Name": "Newb Inc", "MarketCap": 3e11},
    {"Code": "NEWC", "Name": "Newc Inc", "MarketCap": 2e11},
]
CAND_LIST = {
    "NEWA": {"Type": "Common Stock", "Exchange": "NYSE", "Isin": "US1111111111"},
    "DUPY": {"Type": "Common Stock", "Exchange": "NYSE", "Isin": "US1111111112"},
    "OTCX": {"Type": "Common Stock", "Exchange": "PINK", "Isin": "DE0000000001"},
    "ETFX": {"Type": "ETF", "Exchange": "NYSE ARCA", "Isin": "US1111111113"},
    "ILCO": {"Type": "Common Stock", "Exchange": "NASDAQ", "Isin": "IL0011111111"},
    "MBLY": {"Type": "Common Stock", "Exchange": "NASDAQ", "Isin": "US60741F1049"},
    "ADRX": {"Type": "Common Stock", "Exchange": "NYSE", "Isin": "US1111111114"},
    "HQIL": {"Type": "Common Stock", "Exchange": "NASDAQ", "Isin": "US1111111115"},
    "FERR": {"Type": "Common Stock", "Exchange": "NYSE", "Isin": "US1111111116"},
    "RETD": {"Type": "Common Stock", "Exchange": "NYSE", "Isin": "US1111111117"},
    "NEWB": {"Type": "Common Stock", "Exchange": "NASDAQ", "Isin": "US1111111118"},
    "NEWC": {"Type": "Common Stock", "Exchange": "NYSE", "Isin": "US1111111119"},
}
CAND_FUND = {
    "ADRX": {"General": {"CountryISO": "TW", "Name": "Foreign Adr Co"}},
    "HQIL": {"General": {"CountryISO": "US", "Address": "1 Hamada St, Herzliya, Israel"}},
    "FERR": RuntimeError("EODHD fundamentals failed: HTTPError"),
}


RETIRED_DEFAULT = [list(um.RETIRE_HEADER), ["RETD.US", "2026-10-18", "acquired", "", "Global_Markets"]]


def _book(gm=None, retired_rows="default", portfolio=("Symbol", "HELD", "5023.SR")):
    if retired_rows == "default":
        retired_rows = RETIRED_DEFAULT
    tabs = [gm or _gm(), FakeWS("My_Portfolio", [[p] for p in portfolio]),
            FakeWS("_Run_Log", [["Timestamp", "Level", "Action", "Page", "Status", "Message", "Endpoint",
                                 "HTTP Code", "Duration ms", "Details JSON"]], rows=1000)]
    if retired_rows is not None:
        tabs.append(FakeWS("_Retired_Symbols", retired_rows, rows=500))
    return FakeSheet(*tabs)


def _eod(**kw):
    return FakeEod(_us_list(CAND_LIST), CANDIDATES, CAND_FUND, **kw)


def _run(sh, eod, cfg, now, **kw):
    kw.setdefault("sync_check", lambda: False)
    return um.run(sh, eod, cfg, now, **kw)


def _weekdays_from(start: dt.datetime, n: int):
    d, out = start, []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += dt.timedelta(days=1)
    return out


# --------------------------------------------------------------------------- #
# U1 modes                                                                    #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("raw,expected", [(None, "off"), ("", "off"), ("OBSERVE", "observe"), (" enforce ", "enforce"),
                                          ("on", "off"), ("1", "off")])
def test_u1_mode_parsing_defaults_off(monkeypatch, raw, expected):
    if raw is None:
        monkeypatch.delenv("TFB_UNIVERSE_MAINT", raising=False)
    else:
        monkeypatch.setenv("TFB_UNIVERSE_MAINT", raw)
    assert um.mode() == expected


def test_u1_off_needs_no_key_and_opens_nothing(monkeypatch, capsys):
    monkeypatch.delenv("TFB_UNIVERSE_MAINT", raising=False)
    monkeypatch.delenv("EODHD_API_KEY", raising=False)
    monkeypatch.setattr(um, "open_sheet", lambda: pytest.fail("off mode must not open the workbook"))
    assert um.main([]) == 0
    assert "nothing read or written" in capsys.readouterr().out


def test_u1_selftest_passes_and_missing_key_fails_closed(monkeypatch):
    assert um.selftest() == []
    assert um.main(["--selftest"]) == 0
    monkeypatch.setenv("TFB_UNIVERSE_MAINT", "observe")
    monkeypatch.delenv("EODHD_API_KEY", raising=False)
    monkeypatch.delenv("EODHD_API_TOKEN", raising=False)
    monkeypatch.setattr(um, "open_sheet", lambda: pytest.fail("no key: the workbook must not be opened"))
    assert um.main([]) == 1


def test_u1_enforce_degrades_to_observe_when_selftest_fails(monkeypatch):
    monkeypatch.setenv("TFB_UNIVERSE_MAINT", "enforce")
    monkeypatch.setenv("EODHD_API_KEY", "k")
    monkeypatch.setattr(um, "selftest", lambda: ["seeded"])
    seen = {}
    monkeypatch.setattr(um, "open_sheet", lambda: object())
    monkeypatch.setattr(um, "run", lambda sh, eod, cfg, now, force_add=False: seen.setdefault("mode", cfg["mode"]) and {
        "rows": 0, "red_today": 0, "added": [], "retired": [], "kept_red": [], "notes": []})
    um.main([])
    assert seen["mode"] == "observe"


def test_u1_settings_clamp_and_defaults(monkeypatch):
    for k in ("TFB_UNIVERSE_ADD_MAX", "TFB_UNIVERSE_MIN_ROWS", "TFB_UNIVERSE_ADD_WEEKDAY", "TFB_UNIVERSE_ADD_FOREIGN",
              "TFB_SYNC_RETIRE_TAB", "TFB_SYNC_RETIRE_TAB_NAME", "TFB_UNIVERSE_SCREEN_N"):
        monkeypatch.delenv(k, raising=False)
    s = um.settings()
    assert (s["add_max"], s["min_rows"], s["max_rows"], s["red_days"], s["add_weekday"]) == (25, 6512, 6750, 10, "SUN")
    assert s["add_foreign"] is False and s["retire_mode"] == "off" and s["retire_tab"] == "_Retired_Symbols"
    monkeypatch.setenv("TFB_UNIVERSE_ADD_MAX", "999")
    monkeypatch.setenv("TFB_UNIVERSE_SCREEN_N", "5000")
    monkeypatch.setenv("TFB_UNIVERSE_ADD_WEEKDAY", "xyz")
    s = um.settings()
    assert s["add_max"] == 200 and s["screen_n"] == 1000 and s["add_weekday"] == "SUN"


# --------------------------------------------------------------------------- #
# U2 red predicate                                                            #
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("warnings,last,red", [
    ("identity_quarantined:kept_last_good:v6.25.1; acquisition_status:preserved", FRESH, True),
    ("acquisition_status:unavailable", FRESH, True),
    ("quote_exchange_from_suffix; quote_current_price_missing; quote_attempt:eodhd:unpriced", FRESH, True),
    ("quote_exchange_from_suffix; name_from_chart_meta", FRESH, False),
    ("", "", True),
    ("", "n/a", True),
    ("", "2026-02-31T00:00:00", True),
    ("", "2026-10-18T23:00:00+00:00", False),      # 2 days old: not red (same as TODAY()-2)
    ("", "2026-10-17T23:59:59+00:00", True),       # 3 days old
])
def test_u2_red_predicate_matches_the_sheet_rule(warnings, last, red):
    assert um.is_red(warnings, last, TUE.date(), 2)[0] is red


def test_u2_regex_is_the_v1_2_24_rule():
    gs = (ROOT / "apps_script" / "27_Decision_Pages_v2.gs")
    if gs.exists() and "DP2_FLAG_RE_" in gs.read_text(encoding="utf-8"):
        m = re.search(r"DP2_FLAG_RE_\s*=\s*'([^']+)'", gs.read_text(encoding="utf-8"))
        assert m and m.group(1) == um.RED_RE.pattern
    else:
        assert um.RED_RE.pattern == "acquisition_status:(unavailable|preserved)|identity_quarantined|quote_current_price_missing"


# --------------------------------------------------------------------------- #
# U3 watch streak                                                             #
# --------------------------------------------------------------------------- #
def test_u3_streak_counts_weekdays_once_and_recovery_clears():
    red = [{"symbol": "X.US", "name": "X", "warnings": "acquisition_status:unavailable", "last_utc": ""}]
    w, n = um.update_watch({}, red, dt.date(2026, 10, 20), 2)
    assert n == 1 and w["X.US"]["days"] == 1 and w["X.US"]["first"] == "2026-10-20"
    w2, _ = um.update_watch(w, red, dt.date(2026, 10, 20), 2)                  # same-day rerun
    assert w2["X.US"]["days"] == 1
    w3, _ = um.update_watch(w2, red, dt.date(2026, 10, 24), 2)                 # Saturday
    w4, _ = um.update_watch(w3, red, dt.date(2026, 10, 25), 2)                 # Sunday
    assert w4["X.US"]["days"] == 1 and w4["X.US"]["first"] == "2026-10-20"
    w5, _ = um.update_watch(w4, red, dt.date(2026, 10, 26), 2)                 # Monday
    assert w5["X.US"]["days"] == 2
    ok = [{"symbol": "X.US", "name": "X", "warnings": "", "last_utc": "2026-10-27T01:00:00"}]
    w6, n6 = um.update_watch(w5, ok, dt.date(2026, 10, 27), 2)
    assert w6 == {} and n6 == 0                                                # streak restarts from zero
    w7, _ = um.update_watch(w6, red, dt.date(2026, 10, 28), 2)
    assert w7["X.US"]["days"] == 1


def test_u3_first_seen_on_weekend_starts_at_zero():
    red = [{"symbol": "X.US", "warnings": "identity_quarantined", "last_utc": FRESH}]
    w, _ = um.update_watch({}, red, dt.date(2026, 10, 24), 2)
    assert w["X.US"]["days"] == 0
    w2, _ = um.update_watch(w, red, dt.date(2026, 10, 26), 2)
    assert w2["X.US"]["days"] == 1


# --------------------------------------------------------------------------- #
# U4 retirement rules                                                         #
# --------------------------------------------------------------------------- #
def _w(days):
    return {"days": days, "name": "n", "reason": "identity_quarantined"}


def test_u4_retire_only_confirmed_us_delistings():
    watch = {"DEAD.US": _w(10), "YOUNG.US": _w(9), "HELD.US": _w(15), "GONE.L": _w(30), "LIVEQ.US": _w(12),
             "OLD.US": _w(11)}
    retire, kept = um.plan_retire(watch, 6600, ["HELD"], ["OLD.US"], {"LIVEQ", "AAPL"}, 10, 6512, 25)
    assert [r["symbol"] for r in retire] == ["DEAD.US"]
    why = {k["symbol"]: k["why"] for k in kept}
    assert set(why) == {"HELD.US", "GONE.L", "LIVEQ.US"}
    assert "held on My_Portfolio" in why["HELD.US"] and "non-US" in why["GONE.L"]
    assert "data problem" in why["LIVEQ.US"]


def test_u4_no_confirmation_without_the_list_and_floor_and_cap():
    watch = {f"D{i}.US": _w(10 + i) for i in range(40)}
    r, k = um.plan_retire(watch, 6600, [], [], None, 10, 6512, 25)
    assert r == [] and all("unavailable" in x["why"] for x in k)
    r, k = um.plan_retire(watch, 6520, [], [], set(), 10, 6512, 25)
    assert len(r) == 8 and sum("cap reached" in x["why"] for x in k) == 32          # floor 6,512 holds
    r, _ = um.plan_retire(watch, 6600, [], [], set(), 10, 6512, 25)
    assert len(r) == 25 and r[0]["symbol"] == "D39.US"                               # longest streak first


# --------------------------------------------------------------------------- #
# U5 Israeli exclusion                                                        #
# --------------------------------------------------------------------------- #
def test_u5_israeli_policy_sources():
    assert um.is_israeli("X", "IL0011762130", None, []) == "ISIN IL (Israeli-domiciled)"
    assert "country Israel" in um.is_israeli("X", "US1", {"General": {"CountryISO": "IL"}}, [])
    assert "country Israel" in um.is_israeli("X", "US1", {"General": {"CountryName": "Israel"}}, [])
    assert "headquarters" in um.is_israeli("X", "US1", {"General": {"AddressData": {"Country": "Israel"}}}, [])
    assert "headquarters" in um.is_israeli("X", "US1", {"General": {"Address": "Har Hotzvim, Jerusalem, Israel"}}, [])
    assert "exclusion list" in um.is_israeli("mbly", "US60741F1049", None, ["MBLY"])
    assert um.is_israeli("AAPL", "US0378331005", {"General": {"CountryISO": "US", "Address": "Cupertino, CA"}}, []) is None


def test_u5_curated_file_is_well_formed():
    data = json.loads(um.EXCLUSIONS_PATH.read_text(encoding="utf-8"))
    assert data["schema"] == "tfb.universe-exclusions/v1"
    codes = [e["code"] for e in data["israeli_founded_us_listed"]]
    assert len(codes) == len(set(codes)) and all(c == c.upper() and c.strip() == c for c in codes)
    assert all(e.get("basis") for e in data["israeli_founded_us_listed"])
    assert {"MBLY", "SEDG", "TEVA", "NICE", "S", "DOX", "ORA"} <= set(codes)
    assert um.load_curated() == codes
    assert um.load_curated(Path("/nonexistent/x.json")) == []


# --------------------------------------------------------------------------- #
# U6 addition filters                                                         #
# --------------------------------------------------------------------------- #
def _plan(cap=25, **kw):
    eod = _eod()
    page = [f"P{i:04d}.US" for i in range(6600)] + [s for s, _, _ in SPECIAL]
    names = [f"Plain Company {i}" for i in range(6600)] + [n for _, n, _ in SPECIAL]
    add, skipped, notes = um.plan_add(CANDIDATES, page, names, ["RETD.US"], _us_list(CAND_LIST), ["MBLY"],
                                      eod.fundamentals, cap, **kw)
    return add, {s["symbol"]: s["why"] for s in skipped}, notes, eod


def test_u6_every_filter_reports_its_reason():
    add, why, notes, eod = _plan()
    assert [a["symbol"] for a in add] == ["NEWA.US", "NEWB.US", "NEWC.US"]
    assert add[0]["rank"] == 2 and "missing from Global_Markets" in add[0]["why"]
    assert "P0001.US" not in why                                     # already on the page: silent
    assert "same company" in why["DUPY.US"]
    assert "OTC" in why["OTCX.US"] and "Common Stock" in why["ETFX.US"]
    assert why["ILCO.US"].startswith("policy: ISIN IL") and why["MBLY.US"].startswith("policy: owner exclusion")
    assert why["HQIL.US"].startswith("policy: fundamentals: headquarters")
    assert "foreign issuer (TW)" in why["ADRX.US"]
    assert "fundamentals unavailable" in why["FERR.US"] and "_Retired_Symbols" in why["RETD.US"]
    # the cheap filters run before any fundamentals call (no call for ILCO / MBLY / OTCX / ETFX / DUPY)
    assert eod.calls["fundamentals"] == 6 and notes == []


def test_u6_cap_budget_foreign_switch_and_bare_tickers():
    add, _, _, _ = _plan(cap=1)
    assert [a["symbol"] for a in add] == ["NEWA.US"]
    add, why, notes, _ = _plan(fund_max=2)
    assert [a["symbol"] for a in add] == ["NEWA.US"] and "budget" in notes[0]
    add, _, _, _ = _plan(add_foreign=True)
    assert "ADRX.US" in [a["symbol"] for a in add]
    add, _, _ = um.plan_add([{"Code": "ZZZ", "Name": "Zzz Inc"}], ["ZZZ"], [], [],
                            {"ZZZ": {"Type": "Common Stock", "Exchange": "NYSE"}}, [], lambda c: {}, 25)
    assert add == []                                                  # bare ZZZ on the page = ZZZ.US
    two = [{"Code": "AAA", "Name": "Same Name Inc"}, {"Code": "AAB", "Name": "Same Name Inc Class B"}]
    lst = {c: {"Type": "Common Stock", "Exchange": "NYSE"} for c in ("AAA", "AAB")}
    add, skipped, _ = um.plan_add(two, [], [], [], lst, [], lambda c: {"General": {"CountryISO": "US"}}, 25)
    assert [a["symbol"] for a in add] == ["AAA.US"] and "same company" in skipped[0]["why"]


def test_u6_norm_name_share_classes_and_adr_wording():
    assert um.norm_name("Alphabet Inc. Class A") == um.norm_name("ALPHABET INC CLASS C") == "alphabet"
    assert um.norm_name("Toyota Motor Corporation ADR") == um.norm_name("Toyota Motor Corp")
    assert um.norm_name("Johnson & Johnson") == um.norm_name("JOHNSON AND JOHNSON")
    assert um.norm_name("Corp Inc Ltd") == ""


# --------------------------------------------------------------------------- #
# U7 cadence                                                                  #
# --------------------------------------------------------------------------- #
def test_u7_weekly_cadence_and_mode_switch():
    sun, tue, sat = dt.date(2026, 10, 18), dt.date(2026, 10, 20), dt.date(2026, 10, 24)
    assert um.week_start(tue, "SUN") == sun and um.week_start(sun, "SUN") == sun
    assert um.week_start(sat, "SUN") == sun and um.week_start(dt.date(2026, 10, 25), "SUN") == dt.date(2026, 10, 25)
    assert um.additions_due(tue, "SUN", None, "", "observe")
    assert not um.additions_due(sat, "SUN", sun, "observe", "observe")
    assert um.additions_due(dt.date(2026, 10, 25), "SUN", sat, "observe", "observe")
    assert um.additions_due(tue, "SUN", sun, "observe", "enforce")          # observe -> enforce switch
    assert not um.additions_due(tue, "SUN", sun, "enforce", "enforce")
    assert um.add_room(6740, 6750, 25) == 10 and um.add_room(6700, 6750, 25) == 25
    assert um.add_room(6750, 6750, 25) == 0 and um.add_room(6800, 6750, 25) == 0


# --------------------------------------------------------------------------- #
# U8 observe end-to-end                                                       #
# --------------------------------------------------------------------------- #
def test_u8_observe_writes_only_its_own_tabs():
    sh, eod = _book(), _eod()
    gm_before = json.dumps(sh.tabs["Global_Markets"].values)
    out = _run(sh, eod, _cfg(mode="observe"), TUE)
    assert json.dumps(sh.tabs["Global_Markets"].values) == gm_before
    assert sh.tabs["Global_Markets"].calls == []
    assert sh.tabs["_Retired_Symbols"].calls == [] and sh.tabs["_Retired_Symbols"].values == RETIRED_DEFAULT
    assert set(sh.created) == {"_Universe_Watch", "_Universe_Report"}
    assert out["red_today"] == 4 and [a["symbol"] for a in out["added"]] == ["NEWA.US", "NEWB.US", "NEWC.US"]
    watch = sh.tabs["_Universe_Watch"].get_all_values()
    assert watch[0] == um.WATCH_HEADER and {r[0] for r in watch[1:] if r[0]} == {"DEAD.US", "LIVEQ.US", "HELD", "GONE.L"}
    report = sh.tabs["_Universe_Report"].get_all_values()
    assert report[1][:4] == [um.STATE_LABEL, "2026-10-20", "3", "observe"]
    assert any(r[0] == "WOULD ADD (observe)" for r in report)
    log = sh.tabs["_Run_Log"].values[-1]
    assert log[2] == "universe_maintenance" and log[4] == "OBSERVE" and "would_add=3" in log[5]
    # a second run the same week: no new screener call, the streak does not double count
    _run(sh, eod, _cfg(mode="observe"), TUE + dt.timedelta(hours=11))
    assert eod.calls["screener"] == 1
    assert {r[0]: r[3] for r in sh.tabs["_Universe_Watch"].get_all_values()[1:] if r[0]}["DEAD.US"] == "1"


def test_u8_ten_red_weekdays_reach_the_retirement_list_in_observe():
    sh, eod = _book(), _eod()
    for day in _weekdays_from(TUE, 10):
        out = _run(sh, eod, _cfg(mode="observe"), day)
    assert [r["symbol"] for r in out["retired"]] == ["DEAD.US"]
    kept = {k["symbol"]: k["why"] for k in out["kept_red"]}
    assert set(kept) == {"LIVEQ.US", "HELD", "GONE.L"}
    assert sh.tabs["_Retired_Symbols"].calls == [] and sh.tabs["Global_Markets"].calls == []
    assert eod.calls["screener"] == 3                                # weeks from Sun 18 Oct, 25 Oct and 1 Nov


# --------------------------------------------------------------------------- #
# U9 enforce end-to-end                                                       #
# --------------------------------------------------------------------------- #
def _seed_watch(sh, days=9, last="2026-10-19"):
    sh.tabs["_Universe_Watch"] = FakeWS("_Universe_Watch", [um.WATCH_HEADER,
                                        ["DEAD.US", "2026-10-06", last, str(days), "identity_quarantined", "Dead Corp"]])


def test_u9_enforce_appends_retirements_and_additions():
    retired_rows = [["Retired symbols (operator list)"], [], ["Page", "Symbol", "Reason", "Retired On"],
                    ["Global_Markets", "RETD.US", "acquired", "2026-10-18"]]
    gm = _gm()
    last_row = len(gm.values)
    gm.row_count = last_row                                           # no spare grid rows
    sh = _book(gm=gm, retired_rows=retired_rows)
    _seed_watch(sh)
    eod = _eod()
    out = _run(sh, eod, _cfg(mode="enforce"), TUE)
    assert [r["symbol"] for r in out["retired"]] == ["DEAD.US"]
    tail = sh.tabs["_Retired_Symbols"].values[-1]
    assert tail[0] == "Global_Markets" and tail[1] == "DEAD.US" and tail[3] == "2026-10-20"
    assert tail[2].startswith("universe_maintenance v1.0.0: delisted")
    assert "DEAD.US" not in {r[0] for r in sh.tabs["_Universe_Watch"].get_all_values()[1:]}
    assert [r[0] for r in gm.values[last_row:]] == ["NEWA.US", "NEWB.US", "NEWC.US"]
    assert all(len([c for c in r if c != ""]) == 1 for r in gm.values[last_row:])     # column A only
    assert ("add_rows", 3) in gm.calls and gm.values[1][0] == "P0000.US"
    assert sh.tabs["_Run_Log"].values[-1][4] == "OK"
    # same week again: nothing new added, DEAD.US never appended twice
    n_ret = len(sh.tabs["_Retired_Symbols"].values)
    out2 = _run(sh, eod, _cfg(mode="enforce"), TUE + dt.timedelta(days=1))
    assert out2["added"] == [] and eod.calls["screener"] == 1
    assert len(sh.tabs["_Retired_Symbols"].values) == n_ret and len(gm.values) == last_row + 3


def test_u9_enforce_creates_the_retirement_tab_with_the_sync_header():
    sh = _book(retired_rows=None)
    _seed_watch(sh)
    _run(sh, _eod(), _cfg(mode="enforce", retire_mode="off"), TUE)
    tab = sh.tabs["_Retired_Symbols"].values
    assert tab[0] == um.RETIRE_HEADER and tab[1][0] == "DEAD.US" and tab[1][4] == "Global_Markets"


def test_u9_observe_then_enforce_runs_the_additions_the_same_week():
    sh, eod = _book(), _eod()
    _run(sh, eod, _cfg(mode="observe"), TUE)
    out = _run(sh, eod, _cfg(mode="enforce"), TUE + dt.timedelta(days=1))
    assert [a["symbol"] for a in out["added"]] == ["NEWA.US", "NEWB.US", "NEWC.US"] and eod.calls["screener"] == 2
    out3 = _run(sh, eod, _cfg(mode="enforce"), TUE + dt.timedelta(days=2))
    assert out3["added"] == [] and eod.calls["screener"] == 2


@pytest.mark.parametrize("retire_mode", ["off", "enforce"])
def test_u9_room_counts_the_rows_physically_on_the_page(retire_mode):
    gm = _gm(n_plain=6744)                                            # 6,749 rows: one seat left
    sh = _book(gm=gm)
    _seed_watch(sh)
    out = _run(sh, _eod(), _cfg(mode="enforce", retire_mode=retire_mode), TUE)
    assert [r["symbol"] for r in out["retired"]] == ["DEAD.US"]       # listed, but still on the page
    assert len(out["added"]) == 1 and len(gm.values) - 1 == 6750      # never above the cap, even briefly


# --------------------------------------------------------------------------- #
# U10 fail-safes                                                              #
# --------------------------------------------------------------------------- #
def test_u10_partial_page_read_writes_nothing():
    sh = _book(gm=_gm(n_plain=6000))
    out = _run(sh, _eod(), _cfg(mode="enforce"), TUE)
    assert out["wrote"] is False and sh.created == [] and len(sh.tabs["_Run_Log"].values) == 1
    assert "partial read" in out["notes"][0]


def test_u10_us_list_failure_blocks_retirement_and_additions_but_keeps_the_watch():
    sh = _book()
    _seed_watch(sh)
    eod = _eod(fail_list=True)
    out = _run(sh, eod, _cfg(mode="enforce"), TUE)
    assert out["retired"] == [] and out["added"] == [] and eod.calls["screener"] == 0
    assert any("unavailable" in k["why"] for k in out["kept_red"])
    assert sh.tabs["_Retired_Symbols"].calls == []
    assert {r[0]: r[3] for r in sh.tabs["_Universe_Watch"].get_all_values()[1:] if r[0]}["DEAD.US"] == "10"
    assert sh.tabs["_Universe_Report"].get_all_values()[1][1] == ""   # additions not marked as done


def test_u10_short_us_list_is_treated_as_unavailable():
    sh = _book()
    eod = FakeEod({"AAPL": {"Type": "Common Stock", "Exchange": "NASDAQ"}}, CANDIDATES)
    out = _run(sh, eod, _cfg(), TUE)
    assert any("only 1 codes" in n for n in out["notes"]) and eod.calls["screener"] == 0


def test_u10_screener_failure_retries_next_run():
    sh = _book()
    out = _run(sh, _eod(fail_screener=True), _cfg(), TUE)
    assert out["added"] == [] and any("screener unavailable" in n for n in out["notes"])
    out2 = _run(sh, _eod(), _cfg(), TUE + dt.timedelta(hours=11))
    assert len(out2["added"]) == 3


def test_u10_header_found_below_title_rows_and_held_bare_symbol():
    ws = FakeWS("X", [["Title"], ["note"], ["#", "symbol", "Page"], ["1", "AAA.US", ""], ["2", "", ""], ["3", "BBB", "x"]])
    assert um.read_symbols(FakeSheet(ws), "X") == ["AAA.US", "BBB"]
    assert um.read_symbols(FakeSheet(FakeWS("Y", [["no header here"]])), "Y") == []
    assert um.read_symbols(FakeSheet(), "missing") is None                     # absent tab != empty tab
    assert um.read_retired(FakeSheet(ws), "X") == [("AAA.US", ""), ("BBB", "x")]


def test_u10_report_rewrite_never_clears_and_pads_the_old_extent():
    sh = _book()
    sh.tabs["_Universe_Report"] = FakeWS("_Universe_Report", [["x"] * 9 for _ in range(400)], rows=400, cols=9)
    _run(sh, _eod(), _cfg(), TUE)
    vals = sh.tabs["_Universe_Report"].get_all_values()
    assert len(vals) == 400 and all(c == "" for c in vals[-1])         # stale rows blanked, not cleared


# --------------------------------------------------------------------------- #
# U11 EODHD client                                                            #
# --------------------------------------------------------------------------- #
def test_u11_api_key_never_reaches_an_error_message(monkeypatch):
    import urllib.error

    def boom(req, timeout=0):
        raise urllib.error.HTTPError(req.full_url, 401, "Unauthorized", None, None)

    monkeypatch.setattr(um.urllib.request, "urlopen", boom)
    monkeypatch.setattr(um.time, "sleep", lambda s: None)
    client = um.Eodhd("SECRET-KEY-123")
    with pytest.raises(RuntimeError) as exc:
        client.us_list()
    assert "SECRET-KEY-123" not in str(exc.value) and "HTTPError" in str(exc.value)


def test_u11_screener_offset_bound_and_partial_tail(monkeypatch):
    calls = []

    def fake_json(self, path, params, timeout=90.0):
        calls.append(params["offset"])
        if params["offset"] == 300:
            raise RuntimeError("EODHD screener failed: HTTPError")
        return {"data": [{"code": f"C{params['offset'] + i}", "name": "n", "market_capitalization": 1} for i in range(100)]}

    monkeypatch.setattr(um.Eodhd, "_json", fake_json)
    monkeypatch.setattr(um.time, "sleep", lambda s: None)
    rows = um.Eodhd("k").screener_us(1000)
    assert calls == [0, 100, 200, 300] and len(rows) == 300

    calls.clear()

    def full(self, path, params, timeout=90.0):
        calls.append(params["offset"])
        return {"data": [{"code": f"C{params['offset'] + i}"} for i in range(100)]}

    monkeypatch.setattr(um.Eodhd, "_json", full)
    assert len(um.Eodhd("k").screener_us(5000)) == 1000 and max(calls) == 900


# --------------------------------------------------------------------------- #
# U12 workflow contract                                                       #
# --------------------------------------------------------------------------- #
def _wf():
    return yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))


def test_u12_triggers_permissions_and_runner():
    wf = _wf()
    on = wf.get("on") or wf.get(True)
    assert {"schedule", "workflow_dispatch"} == set(on)
    assert on["workflow_dispatch"]["inputs"]["mode"]["options"] == ["inherit", "observe", "off"]
    assert "enforce" not in on["workflow_dispatch"]["inputs"]["mode"]["options"]
    assert wf["permissions"] == {"contents": "read", "actions": "read"}
    assert wf["concurrency"] == {"group": "tfb-universe-maintenance", "cancel-in-progress": False}
    job = wf["jobs"]["universe-maintenance"]
    assert job["runs-on"] == "ubuntu-24.04" and job["timeout-minutes"] <= 30
    assert job["env"]["VAR_MODE"] == "${{ vars.TFB_UNIVERSE_MAINT || 'off' }}"
    assert "environment" not in job


def test_u12_steps_guard_order_and_mappings():
    job = _wf()["jobs"]["universe-maintenance"]
    names = [s.get("id") or s.get("name") for s in job["steps"]]
    run_i = next(i for i, s in enumerate(job["steps"]) if "universe_maintenance.py" in s.get("run", "") and "--json-out" in s.get("run", ""))
    guard_i = names.index("overlap")
    assert names[0] == "mode" and guard_i < run_i
    guard = job["steps"][guard_i]["run"]
    assert "daily_sync.yml/runs" in guard and '.event == "schedule"' in guard and "fail-closed" in guard
    step = job["steps"][run_i]
    assert step["if"] == "steps.mode.outputs.mode != 'off' && steps.overlap.outputs.skip == 'false'"
    env = step["env"]
    assert env["TFB_UNIVERSE_MAINT"] == "${{ steps.mode.outputs.mode }}"
    assert env["TFB_SYNC_RETIRE_TAB"] == "${{ vars.TFB_SYNC_RETIRE_TAB || 'off' }}"
    assert env["TFB_SYNC_RETIRE_TAB_NAME"] == "${{ vars.TFB_SYNC_RETIRE_TAB_NAME || '_Retired_Symbols' }}"
    assert env["EODHD_API_KEY"] == "${{ secrets.EODHD_API_KEY }}"
    assert not any(k in env for k in ("TFB_UNIVERSE_MIN_ROWS", "TFB_UNIVERSE_MAX_ROWS"))   # floors stay in code
    for s in job["steps"]:
        assert "${{" not in s.get("run", ""), s.get("name")
        if s.get("uses", "").startswith("actions/checkout@"):
            assert s["with"]["persist-credentials"] is False
    every_later_step = job["steps"][1:]
    assert all("steps.mode.outputs.mode != 'off'" in s.get("if", "") for s in every_later_step)


def test_u12_mode_resolution_cannot_raise_to_enforce_from_dispatch():
    script = _wf()["jobs"]["universe-maintenance"]["steps"][0]["run"]
    dispatch = script[script.index("workflow_dispatch"):]
    assert 'mode="enforce"' not in dispatch and "observe) mode=\"observe\"" in dispatch


def test_u12_workflow_audit_has_no_findings():
    spec = importlib.util.spec_from_file_location("audit_wf", ROOT / "scripts" / "audit_repository_workflows.py")
    audit = importlib.util.module_from_spec(spec)
    sys.modules["audit_wf"] = audit
    spec.loader.exec_module(audit)
    assert audit.audit_workflow_text(str(WORKFLOW), WORKFLOW.read_text(encoding="utf-8")) == []


# --------------------------------------------------------------------------- #
# U13 review round 1 (#806): fail-closed holdings, existing retirements vs.   #
#     the floor, marker-first additions, write-time sync re-check             #
# --------------------------------------------------------------------------- #
def test_u13_portfolio_transport_error_stops_before_any_write():
    sh = _book()
    sh.broken.add("My_Portfolio")
    _seed_watch(sh)
    with pytest.raises(RuntimeError):
        _run(sh, _eod(), _cfg(mode="enforce"), TUE)
    assert sh.tabs["Global_Markets"].calls == [] and sh.tabs["_Retired_Symbols"].calls == []
    assert sh.created == [] and len(sh.tabs["_Run_Log"].values) == 1


def test_u13_missing_portfolio_retires_nothing():
    sh = _book()
    del sh.tabs["My_Portfolio"]
    _seed_watch(sh)
    out = _run(sh, _eod(), _cfg(mode="enforce"), TUE)
    assert out["retired"] == [] and "holdings unknown" in out["kept_red"][0]["why"]
    assert sh.tabs["_Retired_Symbols"].values == RETIRED_DEFAULT
    assert any("My_Portfolio not found" in n for n in out["notes"])


def test_u13_existing_retirements_on_the_page_count_against_the_floor():
    watch = {"DEAD.US": _w(12)}
    assert um.plan_retire(watch, 6600, [], [], set(), 10, 6512, 25, pending=88)[0] == []
    assert len(um.plan_retire(watch, 6600, [], [], set(), 10, 6512, 25, pending=87)[0]) == 1
    rows = [{"symbol": "A.US"}, {"symbol": "B.US"}, {"symbol": "C"}, {"symbol": "D.US"}, {"symbol": "D.US"}]
    listed = [("A.US", "globalmarkets"), ("C.US", ""), ("D.US", "marketleaders"), ("Z.US", "")]
    assert um.pending_on_page(rows, listed) == 2                     # A (scoped) + C (blank page, twin spelling)
    assert um.norm_page("Global_Markets") == "globalmarkets" == um.norm_page(" global markets ")


def test_u13_run_counts_listed_rows_still_on_the_page():
    gm = _gm(n_plain=6600)                                            # 6,605 rows
    listed = [list(um.RETIRE_HEADER)] + [[f"P{i:04d}.US", "2026-10-18", "", "", "Global_Markets"] for i in range(93)]
    listed += [[f"P{i:04d}.US", "2026-10-18", "", "", "Market_Leaders"] for i in range(93, 150)]
    sh = _book(gm=gm, retired_rows=listed)
    _seed_watch(sh)
    out = _run(sh, _eod(), _cfg(mode="enforce"), TUE)
    assert out["pending_retired"] == 93 and out["retired"] == []      # 6,605 - 93 = 6,512: no room
    assert "already-listed" in out["kept_red"][0]["why"]
    listed.pop()                                                      # Market_Leaders scope never counted
    del listed[1]                                                     # 92 Global_Markets lines left
    sh2 = _book(gm=_gm(n_plain=6600), retired_rows=listed)
    _seed_watch(sh2)
    assert [r["symbol"] for r in _run(sh2, _eod(), _cfg(mode="enforce"), TUE)["retired"]] == ["DEAD.US"]


class _FailingReport(FakeWS):
    def update(self, values, range_name, value_input_option=None):
        if range_name == "A1":
            raise RuntimeError("APIError: [500] internal error")
        return super().update(values, range_name, value_input_option)


def test_u13_marker_is_written_before_the_page_append():
    gm = _gm()
    last_row = len(gm.values)
    sh = _book(gm=gm)
    sh.tabs["_Universe_Report"] = _FailingReport("_Universe_Report", [], rows=300, cols=8)
    eod = _eod()
    with pytest.raises(RuntimeError):
        _run(sh, eod, _cfg(mode="enforce"), TUE)                      # page appended, report rewrite fails
    assert [r[0] for r in gm.values[last_row:]] == ["NEWA.US", "NEWB.US", "NEWC.US"]
    assert sh.tabs["_Universe_Report"].values[1][:4] == [um.STATE_LABEL, "2026-10-20", 3, "enforce"]
    sh.tabs["_Universe_Report"].__class__ = FakeWS                    # the sheet recovers
    out = _run(sh, eod, _cfg(mode="enforce"), TUE + dt.timedelta(hours=11))
    assert out["added"] == [] and eod.calls["screener"] == 1 and len(gm.values) == last_row + 3


@pytest.mark.parametrize("busy", [True, None])
def test_u13_sync_in_flight_at_write_time_defers_every_enforce_write(busy):
    sh = _book()
    _seed_watch(sh)
    gm_before = json.dumps(sh.tabs["Global_Markets"].values)
    eod = _eod()
    out = _run(sh, eod, _cfg(mode="enforce"), TUE, sync_check=lambda: busy)
    assert out["deferred"] is True and json.dumps(sh.tabs["Global_Markets"].values) == gm_before
    assert sh.tabs["_Retired_Symbols"].values == RETIRED_DEFAULT
    report = sh.tabs["_Universe_Report"].get_all_values()
    assert report[1][1] == "" and any(r[0] == "DEFERRED ADD" for r in report)
    assert sh.tabs["_Run_Log"].values[-1][4] == "DEFERRED"
    assert {r[0]: r[3] for r in sh.tabs["_Universe_Watch"].get_all_values()[1:] if r[0]}["DEAD.US"] == "10"
    out2 = _run(sh, eod, _cfg(mode="enforce"), TUE + dt.timedelta(hours=11))
    assert [a["symbol"] for a in out2["added"]] == ["NEWA.US", "NEWB.US", "NEWC.US"]
    assert [r["symbol"] for r in out2["retired"]] == ["DEAD.US"] and eod.calls["screener"] == 2


def test_u13_observe_never_consults_the_write_time_check():
    sh = _book()
    _run(sh, _eod(), _cfg(mode="observe"), TUE, sync_check=lambda: pytest.fail("observe has no writes to guard"))


class _Resp:
    def __init__(self, payload):
        self._b = json.dumps(payload).encode("utf-8")

    def read(self):
        return self._b

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.mark.parametrize("runs,expected", [
    ([{"event": "schedule", "status": "in_progress"}], True),
    ([{"event": "workflow_dispatch", "status": "queued"}], True),
    ([{"event": "pull_request", "status": "in_progress"}, {"event": "push", "status": "queued"},
      {"event": "schedule", "status": "completed"}], False),
    ([], False),
])
def test_u13_sync_in_flight_reads_only_workbook_writer_runs(monkeypatch, runs, expected):
    monkeypatch.setenv("GH_TOKEN", "tok")
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    seen = {}

    def fake_urlopen(req, timeout=0):
        seen["url"] = req.full_url
        return _Resp({"workflow_runs": runs})

    monkeypatch.setattr(um.urllib.request, "urlopen", fake_urlopen)
    assert um.sync_in_flight() is expected
    assert seen["url"].endswith("/repos/owner/repo/actions/workflows/daily_sync.yml/runs?per_page=30")


def test_u13_sync_in_flight_unknown_without_token_or_on_error(monkeypatch):
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    assert um.sync_in_flight() is None
    monkeypatch.setenv("GH_TOKEN", "tok")
    monkeypatch.setattr(um.urllib.request, "urlopen", lambda req, timeout=0: (_ for _ in ()).throw(OSError("down")))
    assert um.sync_in_flight() is None
    monkeypatch.setattr(um.urllib.request, "urlopen", lambda req, timeout=0: _Resp({"message": "Not Found"}))
    assert um.sync_in_flight() is None


def test_u13_workflow_passes_the_token_and_keeps_its_private_group():
    wf = _wf()
    step = next(s for s in wf["jobs"]["universe-maintenance"]["steps"] if "--json-out" in s.get("run", ""))
    assert step["env"]["GH_TOKEN"] == "${{ github.token }}"
    assert "tadawul-production-write" not in json.dumps(wf["concurrency"])
