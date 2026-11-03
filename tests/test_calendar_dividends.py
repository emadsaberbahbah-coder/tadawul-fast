"""The corporate-action guard's data lane: calendar_provider v1.4.0
fetch_next_exdiv_detail + run_calendar_sync v1.3.0 Dividend_Events behind
TFB_CAL_DIVIDENDS (default off).

WHY (3 Nov 2026): Calendar_Events read 'INDV.US | Next Ex-Div Date
2026-11-03 | Days To ExDiv 1' on 2 Nov at 16:28 Riyadh. That day's cockpit
issued an INDV.US ticket and the owner bought 95 shares. The dividend was
Indivior's 8.13 USD special (about 22 % of the 36.28 close; paid 6 Nov). The
date was known, the size was not.

REAL modules; the only stubs are in-memory worksheets, the harvest page and
the provider's HTTP seam (_get_json). No network, no workbook.

D1  provider pure: the earliest FUTURE ex-date wins; past items are ignored;
    same-day declarations in one currency are summed; two currencies ->
    MIXED with no amount; 'value' preferred, else 'unadjustedValue'; a
    missing or negative amount -> None; junk payloads -> None
D2  provider fetch: one /div/<code> request per symbol over [today,
    today + 90d]; KSA never sent; one failed symbol drops only itself;
    disabled -> {}; the live 3 Nov shape (INDV.US 8.13 USD, paid 6 Nov)
D3  provider: fetch_next_exdiv_map and fetch_event_context make the same
    requests and return the same values as the untouched v1.3.2 module
D4  sync OFF (unset / off / observe / junk): Dividend_Events is never read,
    created or written; the provider detail function is never called
D5  sync ON, first run: Calendar_Events is published first and unchanged,
    then one atomic A1:J write creates Dividend_Events with the header and
    one row per symbol with a declared dividend; one summary line
D6  sync ON, existing tab: fresh wins; a harvested symbol whose fetch is
    empty keeps its prior FUTURE row (+carried, its own as-of); an expired
    row and an unharvested symbol are dropped; the old extent is blanked
D7  sync ON, failures: provider disabled or not importable -> the tab is
    left exactly as it is (rc 0); prior tab unreadable -> preserved (rc 2);
    a failed fetch publishes the prior rows only; a failed write -> rc 3
    after the calendar publication succeeded
D8  sync ON, dry run: the dividend rows are printed, nothing is written
D9  pure helpers: parse_prior_div (future only, ticker guard), merge
    (never adds an unharvested symbol), build rows (amount blank when
    unknown, carried source and as-of)
D10 workflow + pins: calendar_sync.yml maps vars.TFB_CAL_DIVIDENDS with
    default 'off' in the run step; verify_deployment pins both versions
"""
from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import date
import importlib.util
from pathlib import Path
import re
import subprocess
import sys
from types import ModuleType

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
BASE_REV = "8478029d"   # main with calendar_provider 1.3.2 / run_calendar_sync 1.2.0
TODAY = date(2026, 11, 2)
ASOF = "2026-11-01 08:20"


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


sync = _load(ROOT / "scripts" / "run_calendar_sync.py", "calendar_dividends_target")

INDV_DIV = {"date": "2026-11-03", "declarationDate": "2026-09-17", "recordDate": "2026-11-03",
            "paymentDate": "2026-11-06", "period": "Other", "value": 8.13,
            "unadjustedValue": 8.13, "currency": "USD"}
FRST_DIV = {"date": "2026-11-06", "declarationDate": "2026-10-22", "recordDate": "2026-11-06",
            "paymentDate": "2026-11-20", "period": "Quarterly", "value": 0.1,
            "unadjustedValue": 0.1, "currency": "USD"}


# ---------------------------------------------------------------- provider
@pytest.fixture
def cp(monkeypatch):
    import core.providers.calendar_provider as mod
    monkeypatch.setattr(mod, "is_enabled", lambda: True)
    monkeypatch.setattr(mod, "_today", lambda: TODAY)
    monkeypatch.delenv("TFB_CALENDAR_DIV_DAYS_AHEAD", raising=False)
    return mod


def test_d1_detail_rules(cp):
    f = cp._div_detail_of
    assert f([INDV_DIV], TODAY) == {
        "ex_date": "2026-11-03", "amount": 8.13, "currency": "USD", "pay_date": "2026-11-06",
        "record_date": "2026-11-03", "declaration_date": "2026-09-17", "period": "Other", "items": 1}
    past = dict(INDV_DIV, date="2026-10-30", value=1.0)
    later = dict(FRST_DIV, date="2026-12-06")
    got = f([later, past, INDV_DIV], TODAY)
    assert got["ex_date"] == "2026-11-03" and got["amount"] == 8.13, "earliest FUTURE, past ignored"
    regular = dict(INDV_DIV, value=0.2, unadjustedValue=0.2, period="Quarterly")
    got = f([INDV_DIV, regular], TODAY)
    assert got["amount"] == 8.33 and got["items"] == 2 and got["period"] == "Other/Quarterly"
    mixed = f([INDV_DIV, dict(INDV_DIV, currency="GBP")], TODAY)
    assert mixed["amount"] is None and mixed["currency"] == "MIXED"
    # review fix (Codex P2 on PR #905): a declaration without a currency next to
    # one with a currency is not summed into a currency-qualified total
    partial = f([dict(INDV_DIV, value=1.0), dict(INDV_DIV, value=2.0, currency=None)], TODAY)
    assert partial["amount"] is None and partial["currency"] == "MIXED"
    blank = f([dict(INDV_DIV, value=1.0, currency=""), dict(INDV_DIV, value=2.0, currency=None)], TODAY)
    assert blank["amount"] == 3.0 and blank["currency"] is None, "no currency claimed at all"
    assert f([dict(INDV_DIV, value=None, unadjustedValue="8.13")], TODAY)["amount"] == 8.13
    assert f([dict(INDV_DIV, value="n/a", unadjustedValue=None)], TODAY)["amount"] is None
    assert f([dict(INDV_DIV, value=-1)], TODAY)["amount"] is None
    assert f([INDV_DIV, dict(INDV_DIV, value=None, unadjustedValue=None)], TODAY)["amount"] is None
    nocc = f([dict(INDV_DIV, currency=None)], TODAY)
    assert nocc["currency"] is None and nocc["amount"] == 8.13
    today_ex = f([dict(INDV_DIV, date="2026-11-02")], TODAY)
    assert today_ex["ex_date"] == "2026-11-02", "an ex-date of today is still ahead of the open"
    for junk in (None, {}, "x", [], ["x", 1], [{"date": "bad"}], [{"value": 1}]):
        assert f(junk, TODAY) is None, junk
    assert f([dict(INDV_DIV, paymentDate="", recordDate=None)], TODAY)["pay_date"] is None


def test_d2_fetch_requests_failures_and_live_shape(cp, monkeypatch):
    calls = []

    async def fake_get_json(_client, path, params):
        calls.append((path, dict(params)))
        if path == "/div/BAD.US":
            raise RuntimeError("429 Too Many Requests")
        return {"/div/INDV.US": [INDV_DIV], "/div/FRST.US": [FRST_DIV]}.get(path, [])

    monkeypatch.setattr(cp, "_get_json", fake_get_json)
    out = cp.fetch_next_exdiv_detail_sync(["INDV.US", "FRST.US", "BAD.US", "NODIV.US", "2222.SR"])
    assert sorted(out) == ["FRST.US", "INDV.US"]
    assert out["INDV.US"]["amount"] == 8.13 and out["INDV.US"]["pay_date"] == "2026-11-06"
    assert out["FRST.US"]["period"] == "Quarterly"
    assert sorted(p for p, _ in calls) == ["/div/BAD.US", "/div/FRST.US", "/div/INDV.US", "/div/NODIV.US"]
    assert all(q["from"] == "2026-11-02" and q["to"] == "2027-01-31" for _, q in calls)
    assert not any("2222" in p for p, _ in calls), "KSA never sent"
    monkeypatch.setenv("TFB_CALENDAR_DIV_DAYS_AHEAD", "30")
    calls.clear()
    cp.fetch_next_exdiv_detail_sync(["INDV.US"])
    assert calls[0][1]["to"] == "2026-12-02"
    monkeypatch.setattr(cp, "is_enabled", lambda: False)
    assert cp.fetch_next_exdiv_detail_sync(["INDV.US"]) == {}
    # 2026-11-03: v1.5.0 (#889, Saudi past reports from Yahoo, gated off) stacked; pin moved
    assert cp.__version__ == "1.5.0"
    assert {"fetch_next_exdiv_detail", "fetch_next_exdiv_detail_sync"} <= set(cp.__all__)


def _base_provider():
    try:
        blob = subprocess.run(["git", "-C", str(ROOT), "show", BASE_REV + ":core/providers/calendar_provider.py"],
                              capture_output=True, timeout=60)
    except Exception:
        return None
    if blob.returncode != 0 or not blob.stdout:
        return None
    tmp = ROOT / ".pytest_cache_div_base_calendar_provider.py"
    tmp.write_bytes(blob.stdout)
    try:
        return _load(tmp, "calendar_provider_base_132")
    finally:
        tmp.unlink()


def test_d3_existing_paths_unchanged(cp, monkeypatch):
    base = _base_provider()
    if base is None:
        pytest.skip("git show %s unavailable (shallow clone)" % BASE_REV)
    results = []
    for mod in (base, cp):
        calls = []

        async def fake_get_json(_client, path, params, calls=calls):
            calls.append((path, tuple(sorted(params.items()))))
            if path.startswith("/div/"):
                return {"/div/INDV.US": [INDV_DIV], "/div/FRST.US": [FRST_DIV]}.get(path, [])
            return {"earnings": [{"code": "FRST.US", "report_date": "2027-01-28"}]}
        monkeypatch.setattr(mod, "_get_json", fake_get_json)
        monkeypatch.setattr(mod, "is_enabled", lambda: True)
        monkeypatch.setattr(mod, "_flag_enabled", lambda: True)
        monkeypatch.setattr(mod, "_today", lambda: TODAY)
        monkeypatch.setattr(mod, "_yahoo_fallback_enabled", lambda: False)
        exdiv = asyncio.run(mod.fetch_next_exdiv_map(["INDV.US", "FRST.US", "2222.SR"]))
        ctx = mod.fetch_event_context_sync(["INDV.US", "FRST.US", "2222.SR"])
        results.append((exdiv, ctx, sorted(calls)))
    assert results[0] == results[1]
    assert results[1][0] == {"INDV.US": "2026-11-03", "FRST.US": "2026-11-06"}


# -------------------------------------------------------------------- sync
class WorksheetNotFound(Exception):
    pass


class Sheet:
    def __init__(self, rows, cols, row_count=1000, fail_read=None, fail_write=None):
        self.values = deepcopy(rows)
        self.col_count, self.row_count = cols, row_count
        self.fail_read, self.fail_write = fail_read, fail_write
        self.reads, self.requests, self.resizes = [], [], []

    def get(self, range_name):
        self.reads.append(range_name)
        if self.fail_read:
            raise self.fail_read
        m = re.fullmatch(r"A1:([A-Z])(\d+)", range_name)
        assert m, range_name
        width = ord(m.group(1)) - ord("A") + 1
        assert width <= self.col_count, "read beyond the grid"
        return [row[:width] for row in deepcopy(self.values)]

    def resize(self, rows=None, cols=None):
        self.resizes.append((rows, cols))
        if rows is not None:
            self.row_count = rows
        if cols is not None:
            self.col_count = cols

    def update(self, *, values, range_name, value_input_option):
        if self.fail_write:
            raise self.fail_write
        m = re.fullmatch(r"A1:([A-Z])(\d+)", range_name)
        width = ord(m.group(1)) - ord("A") + 1
        assert width <= self.col_count and int(m.group(2)) == len(values) <= self.row_count
        assert all(len(r) == width for r in values) and value_input_option == "RAW"
        self.requests.append({"values": deepcopy(values), "range": range_name})
        self.values = deepcopy(values)

    def freeze(self, *, rows):
        assert rows == 1


class Book:
    def __init__(self, calendar, symbols, dividends=None):
        self.calendar, self.symbols, self.dividends = calendar, symbols, dividends
        self.opened, self.added = [], []

    def worksheet(self, name):
        self.opened.append(name)
        if name == "Calendar_Events":
            return self.calendar
        if name == "Dividend_Events":
            if self.dividends is None:
                raise WorksheetNotFound(name)
            return self.dividends
        return type("Page", (), {"get": lambda _s, _r: [["Symbol"]] + [[s] for s in self.symbols]})()

    def add_worksheet(self, *, title, rows, cols):
        self.added.append((title, rows, cols))
        assert title == "Dividend_Events" and cols == 10 and rows >= 1000
        self.dividends = Sheet([], cols=cols, row_count=rows)
        return self.dividends


CTX = {"INDV.US": {"next_earnings_date": "2027-02-25", "next_ex_div_date": "2026-11-03"},
       "FRST.US": {"next_earnings_date": "2027-01-28", "next_ex_div_date": "2026-11-06"},
       "ZTO.US": {"next_earnings_date": "2026-11-17", "next_ex_div_date": None}}
DETAIL = {"INDV.US": {"ex_date": "2026-11-03", "amount": 8.13, "currency": "USD",
                      "pay_date": "2026-11-06", "record_date": "2026-11-03",
                      "declaration_date": "2026-09-17", "period": "Other", "items": 1},
          "FRST.US": {"ex_date": "2026-11-06", "amount": 0.1, "currency": "USD",
                      "pay_date": "2026-11-20", "record_date": "2026-11-06",
                      "declaration_date": "2026-10-22", "period": "Quarterly", "items": 1}}


def _calendar_prior():
    return [list(sync.HEADERS), ["ZTO.US", "2026-11-17", 15, "", "", ASOF, "eodhd via calendar_provider v1.3.2"]]


def _run(monkeypatch, book, *, gate, detail=None, detail_raises=None, enabled=True,
         import_fails=False, mode="--write"):
    prov = ModuleType("core.providers.calendar_provider")
    prov.__version__ = "1.4.0"
    prov.is_enabled = lambda: enabled
    prov.fetch_event_context_sync = lambda _s: deepcopy(CTX)
    prov.detail_calls = []

    def _detail(symbols, _days=None):
        prov.detail_calls.append(list(symbols))
        if detail_raises:
            raise detail_raises
        return deepcopy(DETAIL if detail is None else detail)
    if not import_fails:
        prov.fetch_next_exdiv_detail_sync = _detail
    monkeypatch.setitem(sys.modules, "core.providers.calendar_provider", prov)
    monkeypatch.setattr(sync, "_open_book", lambda: book)
    monkeypatch.setattr(sync, "_today_riyadh", lambda: TODAY)
    monkeypatch.setenv("TFB_CALENDAR_PAGES", "Top_10_Investments")
    for name in ("TFB_CALENDAR_TICKER_GUARD", "TFB_CAL_LAST_REPORT", "TFB_CAL_DIVIDENDS_SHEET"):
        monkeypatch.delenv(name, raising=False)
    if gate is None:
        monkeypatch.delenv("TFB_CAL_DIVIDENDS", raising=False)
    else:
        monkeypatch.setenv("TFB_CAL_DIVIDENDS", gate)
    return sync.main([mode]), prov


@pytest.mark.parametrize("gate", [None, "", "off", "observe", "junk", "0"])
def test_d4_off_never_touches_dividend_events(monkeypatch, capsys, gate):
    cal = Sheet(_calendar_prior(), cols=7)
    book = Book(cal, ["INDV.US", "FRST.US", "ZTO.US"], dividends=Sheet([["x"]], cols=10))
    rc, prov = _run(monkeypatch, book, gate=gate)
    err = capsys.readouterr().err
    assert rc == 0 and "Dividend_Events" not in book.opened and book.added == []
    assert prov.detail_calls == [] and "dividends" not in err
    assert book.dividends.reads == [] and book.dividends.requests == []
    assert cal.requests and cal.requests[0]["range"] == "A1:G1000"


def test_d5_on_first_run_creates_the_tab_after_the_calendar(monkeypatch, capsys):
    cal = Sheet(_calendar_prior(), cols=7)
    book = Book(cal, ["INDV.US", "FRST.US", "ZTO.US"], dividends=None)
    order = []
    cal_update = cal.update

    def tracked(**kw):
        order.append("calendar")
        return cal_update(**kw)
    cal.update = tracked
    rc, prov = _run(monkeypatch, book, gate="on")
    err = capsys.readouterr().err
    assert rc == 0 and order == ["calendar"]
    assert cal.requests[0]["range"] == "A1:G1000" and cal.requests[0]["values"][0] == sync.HEADERS
    assert book.added == [("Dividend_Events", 1000, 10)]
    req = book.dividends.requests
    assert len(req) == 1 and req[0]["range"] == "A1:J1000"
    values = req[0]["values"]
    assert values[0] == sync.DIV_HEADERS
    body = [r for r in values[1:] if r[0]]
    assert [r[0] for r in body] == ["INDV.US", "FRST.US"], "harvest order, no row without a dividend"
    indv = body[0]
    assert indv[:8] == ["INDV.US", "2026-11-03", 1, 8.13, "USD", "2026-11-06", "2026-11-03", "Other"]
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", indv[8])
    assert indv[9] == "eodhd /div via calendar_provider v1.4.0"
    assert all(r == [""] * 10 for r in values[1 + len(body):])
    assert prov.detail_calls == [["INDV.US", "FRST.US", "ZTO.US"]]
    assert "dividends: with=2 fresh=2 carried=0 -> Dividend_Events" in err
    assert "dividends: wrote 2 rows -> Dividend_Events" in err


def test_d5b_calendar_resurrection_never_reaches_the_dividend_lane(monkeypatch, capsys):
    """Review fix (Codex P2 on PR #905): a symbol the calendar resurrects from
    its prior tab (no longer harvested) is neither fetched nor carried."""
    cal_prior = [list(sync.HEADERS),
                 ["GONE.US", "2026-11-20", 18, "2026-11-10", 8, ASOF, "eodhd via calendar_provider v1.3.2"]]
    div_prior = [list(sync.DIV_HEADERS),
                 ["GONE.US", "2026-11-10", 8, 5.0, "USD", "", "", "Other", ASOF, "eodhd"]]
    cal, divs = Sheet(cal_prior, cols=7), Sheet(div_prior, cols=10)
    book = Book(cal, ["INDV.US"], dividends=divs)
    rc, prov = _run(monkeypatch, book, gate="on")
    capsys.readouterr()
    assert rc == 0
    cal_body = [r[0] for r in cal.requests[0]["values"][1:] if r[0]]
    assert "GONE.US" in cal_body, "the calendar's own resurrection is unchanged"
    assert prov.detail_calls == [["INDV.US"]], "only the harvest is fetched"
    div_body = [r[0] for r in divs.requests[0]["values"][1:] if r[0]]
    assert div_body == ["INDV.US"], "a resurrected symbol is not carried into Dividend_Events"


def test_d6_on_existing_tab_carry_expire_and_extent(monkeypatch, capsys):
    prior = [list(sync.DIV_HEADERS),
             ["ZTO.US", "2026-11-20", 18, 0.5, "USD", "2026-12-01", "2026-11-20", "Quarterly",
              ASOF, "eodhd /div via calendar_provider v1.4.0"],
             ["OLD.US", "2026-10-30", -3, 1.0, "USD", "", "", "", ASOF, "eodhd"],
             ["GONE.US", "2026-12-15", 43, 2.0, "USD", "", "", "", ASOF, "eodhd"]] + [[""] * 10] * 1500
    divs = Sheet(prior, cols=10, row_count=1504)
    book = Book(Sheet(_calendar_prior(), cols=7), ["INDV.US", "ZTO.US"], dividends=divs)
    rc, _ = _run(monkeypatch, book, gate="on", detail={"INDV.US": DETAIL["INDV.US"]})
    err = capsys.readouterr().err
    assert rc == 0 and divs.reads == ["A1:J1504"] and book.added == []
    req = divs.requests[0]
    assert req["range"] == "A1:J1504", "the full old extent is rewritten (blanks clear old rows)"
    body = {r[0]: r for r in req["values"][1:] if r[0]}
    assert sorted(body) == ["INDV.US", "ZTO.US"], "expired and unharvested rows dropped"
    assert body["ZTO.US"][1:4] == ["2026-11-20", 18, 0.5]
    assert body["ZTO.US"][8] == ASOF and body["ZTO.US"][9].endswith(" +carried")
    assert "dividends: with=2 fresh=1 carried=1 -> Dividend_Events" in err


def test_d6b_narrow_existing_tab_is_read_within_its_grid(monkeypatch, capsys):
    divs = Sheet([["Symbol", "Ex-Div Date"]], cols=2)
    book = Book(Sheet(_calendar_prior(), cols=7), ["INDV.US"], dividends=divs)
    rc, _ = _run(monkeypatch, book, gate="on")
    assert rc == 0 and divs.reads == ["A1:B1000"] and (None, 10) in divs.resizes
    assert divs.requests[0]["range"] == "A1:J1000"


def test_d7_failures(monkeypatch, capsys):
    prior_rows = [list(sync.DIV_HEADERS),
                  ["INDV.US", "2026-11-03", 1, 8.13, "USD", "2026-11-06", "2026-11-03", "Other", ASOF, "eodhd"]]
    # provider disabled / not importable: the tab is left exactly as it is
    for kw in ({"enabled": False}, {"import_fails": True}):
        divs = Sheet(prior_rows, cols=10)
        book = Book(Sheet(_calendar_prior(), cols=7), ["INDV.US"], dividends=divs)
        rc, _ = _run(monkeypatch, book, gate="on", **kw)
        err = capsys.readouterr().err
        assert rc == 0 and divs.reads == [] and divs.requests == [] and book.added == [], kw
        assert "left as is" in err
    # prior unreadable (not 'not found'): preserved, rc 2
    divs = Sheet(prior_rows, cols=10, fail_read=RuntimeError("503"))
    book = Book(Sheet(_calendar_prior(), cols=7), ["INDV.US"], dividends=divs)
    rc, _ = _run(monkeypatch, book, gate="on")
    err = capsys.readouterr().err
    assert rc == 2 and divs.requests == [] and "preserving tab" in err
    # the fetch raises: the prior rows only, still published
    divs = Sheet(prior_rows, cols=10)
    book = Book(Sheet(_calendar_prior(), cols=7), ["INDV.US"], dividends=divs)
    rc, _ = _run(monkeypatch, book, gate="on", detail_raises=RuntimeError("429"))
    err = capsys.readouterr().err
    assert rc == 0 and "WARN fetch failed" in err
    body = [r for r in divs.requests[0]["values"][1:] if r[0]]
    assert body[0][:4] == ["INDV.US", "2026-11-03", 1, 8.13] and body[0][9].endswith("+carried")
    # the write fails after the calendar publication: rc 3
    cal = Sheet(_calendar_prior(), cols=7)
    divs = Sheet(prior_rows, cols=10, fail_write=RuntimeError("quota"))
    book = Book(cal, ["INDV.US"], dividends=divs)
    rc, _ = _run(monkeypatch, book, gate="on")
    err = capsys.readouterr().err
    assert rc == 3 and cal.requests and "publication unconfirmed" in err


def test_d8_dry_run_prints_and_writes_nothing(monkeypatch, capsys):
    cal = Sheet(_calendar_prior(), cols=7)
    divs = Sheet([list(sync.DIV_HEADERS)], cols=10)
    book = Book(cal, ["INDV.US", "FRST.US"], dividends=divs)
    rc, _ = _run(monkeypatch, book, gate="on", mode="--dry-run")
    err = capsys.readouterr().err
    assert rc == 0 and cal.requests == [] and divs.requests == [] and book.added == []
    assert "  div INDV.US | 2026-11-03 | 1 | 8.13 | USD | 2026-11-06" in err
    assert "dry-run" in err


def test_d9_pure_helpers(monkeypatch):
    monkeypatch.setattr(sync, "_today_riyadh", lambda: TODAY)
    monkeypatch.delenv("TFB_CALENDAR_TICKER_GUARD", raising=False)
    values = [list(sync.DIV_HEADERS),
              ["INDV.US", "2026-11-03", 1, "8.13", "USD", "2026-11-06", "", "Other", ASOF, "src"],
              ["FORECAST", "2026-11-03", 1, 1, "USD", "", "", "", ASOF, "src"],
              ["OLD.US", "2026-11-01", -1, 1, "USD", "", "", "", ASOF, "src"],
              ["INDV.US", "2026-12-01", 29, 9, "USD", "", "", "", ASOF, "dup"]]
    prior = sync.parse_prior_div(values)
    assert list(prior) == ["INDV.US"], "future only, ticker-shaped, first row wins"
    assert prior["INDV.US"]["amount"] == 8.13 and prior["INDV.US"]["record_date"] is None
    assert sync.parse_prior_div([["Symbol", "Next Earnings Date"]]) == {}
    assert sync.parse_prior_div([]) == {}
    merged, nf, nc = sync.merge_dividends(
        ["INDV.US", "NEW.US"], {"NEW.US": dict(DETAIL["FRST.US"]), "OUT.US": dict(DETAIL["FRST.US"])},
        prior)
    assert sorted(merged) == ["INDV.US", "NEW.US"] and nf == 1 and nc == 1
    assert merged["INDV.US"]["carried"] is True and merged["NEW.US"]["carried"] is False
    rows = sync.build_div_rows(["NEW.US", "INDV.US", "NONE.US"],
                               dict(merged, **{"NEW.US": dict(merged["NEW.US"], amount=None)}), "fresh-src")
    assert [r[0] for r in rows] == ["NEW.US", "INDV.US"]
    assert rows[0][3] == "" and rows[0][9] == "fresh-src"
    assert rows[1][8] == ASOF and rows[1][9] == "src +carried"
    for word, armed in ((None, False), ("", False), ("off", False), ("observe", False),
                        ("on", True), ("1", True), ("TRUE", True), ("yes", True)):
        if word is None:
            monkeypatch.delenv("TFB_CAL_DIVIDENDS", raising=False)
        else:
            monkeypatch.setenv("TFB_CAL_DIVIDENDS", word)
        assert sync._dividends_enabled() is armed, word
    monkeypatch.setenv("TFB_CAL_DIVIDENDS_SHEET", "Divs_Test")
    assert sync._dividends_sheet() == "Divs_Test"
    assert sync.__version__ == "1.3.0" and len(sync.DIV_HEADERS) == 10


def test_d10_workflow_mapping_and_pins():
    wf = yaml.safe_load((ROOT / ".github" / "workflows" / "calendar_sync.yml").read_text(encoding="utf-8"))
    steps = {s.get("name"): s for s in wf["jobs"][next(iter(wf["jobs"]))]["steps"]}
    env = steps["📅 Run calendar sync"]["env"]
    assert env["TFB_CAL_DIVIDENDS"] == "${{ vars.TFB_CAL_DIVIDENDS || 'off' }}"
    assert env["TFB_CAL_LAST_REPORT"] == "${{ vars.TFB_CAL_LAST_REPORT || 'off' }}"
    verifier = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    # 2026-11-03: calendar_provider v1.5.0 (#889) stacked; pin moved
    assert '("core.providers.calendar_provider", "__version__", "1.5.0",' in verifier
    assert '("run_calendar_sync", "1.3.0", "calendar sync"),' in verifier
    for path in (ROOT / "scripts" / "run_calendar_sync.py", ROOT / "core" / "providers" / "calendar_provider.py"):
        added = path.read_text(encoding="utf-8")
        assert "fetch_next_exdiv_detail" in added
