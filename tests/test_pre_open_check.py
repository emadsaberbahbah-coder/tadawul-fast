#!/usr/bin/env python3
"""tests/test_pre_open_check.py - scripts/pre_open_check.py v1.0.0: the
17:00 Riyadh re-check of the executable tickets (post-mortem item 2).

  P1  symbols, numbers and the Top_10 grid reader (header below a banner;
      executable = Ticket SAR > 0 and Shares > 0; planned price = Price)
  P2  the Yahoo chart parser: pre / regular / post session, previous close,
      the last print at or before now, malformed payloads
  P3  verdicts: ES=F WAIT / GO / CHECK; ticket WAIT on the market, WAIT on a
      gap either way, GO inside the band, CHECK on a missing or unread
      quote, SKIP for another session or a research seat; never a silent GO
  P4  the pre-open window on the NYSE calendar (winter and summer opens,
      a holiday, early and late firings)
  P5  main: outside the window nothing is read or written; inside, the tab
      is written whole (header + market row + tickets); dry run writes
      nothing; a broken page refuses; --ignore-window rehearses; the
      quote breaker is reported, never hidden
  P6  the workflow: two UTC firings, dispatch writes only with write=true,
      ignore_window, read-only permissions, knobs

Run: pytest -q tests/test_pre_open_check.py
"""
from __future__ import annotations

import datetime as dt
import re
import sys
from copy import deepcopy
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import pre_open_check as pc  # noqa: E402

UTC = dt.timezone.utc


class WorksheetNotFound(Exception):
    pass


class Sheet:
    def __init__(self, rows, cols, row_count=60):
        self.values = deepcopy(rows)
        self.col_count, self.row_count = cols, row_count
        self.requests, self.resizes = [], []

    def get(self, range_name):
        return deepcopy(self.values)

    def resize(self, rows=None, cols=None):
        self.resizes.append((rows, cols))
        if rows is not None:
            self.row_count = rows
        if cols is not None:
            self.col_count = cols

    def update(self, *, values, range_name, value_input_option):
        m = re.fullmatch(r"A1:([A-Z])(\d+)", range_name)
        width = ord(m.group(1)) - ord("A") + 1
        assert width <= self.col_count and int(m.group(2)) == len(values) <= self.row_count
        assert all(len(r) == width for r in values) and value_input_option == "RAW"
        self.requests.append({"values": deepcopy(values), "range": range_name})
        self.values = deepcopy(values)

    def freeze(self, *, rows):
        assert rows == 1


class Book:
    def __init__(self, pages, tab=None):
        self.pages, self.tab, self.added = pages, tab, []

    def worksheet(self, name):
        if name in self.pages:
            return self.pages[name]
        if name == "Pre_Open_Check" and self.tab is not None:
            return self.tab
        raise WorksheetNotFound(name)

    def add_worksheet(self, *, title, rows, cols):
        self.tab = Sheet([], cols, row_count=rows)
        self.added.append((title, rows, cols))
        return self.tab


class Broken:
    def get(self, *_a, **_k):
        raise RuntimeError("quota")


HDR = ["Rank", "Symbol", "Name", "Market", "Sector", "Ccy", "FX→SAR", "Price", "Price SAR",
       "Entry Zone", "Ticket SAR", "Shares", "Stop SAR"]


def page(rows):
    banner = [["TFB Top 10 - FEED ACTIONABLE -- EXECUTABLE"] + [""] * 12,
              ["Last run: 2026-11-08 02:10"] + [""] * 12, [""] * 13]
    return Sheet(banner + [HDR] + rows, 13)


def trow(rank, sym, market, ccy, price, ticket, shares):
    return [rank, sym, "Name " + sym, market, "Sector", ccy, "3.75", price, "", "", ticket, shares, ""]


TOP10 = page([trow(1, "SNDA.US", "US", "USD", "25.40", "6,000", 60),
              trow(2, "IBOC.US", "US", "USD", "61.10", "5,250", 22),
              trow(3, "BRK.B.US", "US", "USD", "480.00", "4,500", 2),
              trow(4, "2222.SR", "Tadawul", "SAR", "27.10", "7,000", 258),
              trow(5, "FRST.US", "US", "USD", "12.00", "0", 0),      # research seat
              trow(6, "GHOST.US", "US", "USD", "9.00", "1,200", 0)])  # a reserved ticket with no shares

# winter session: 9 Nov 2026 open 14:30 UTC (17:30 Riyadh); check at 14:00 UTC
NOW = dt.datetime(2026, 11, 9, 14, 0, tzinfo=UTC)
OPEN_TS = int(dt.datetime(2026, 11, 9, 14, 30, tzinfo=UTC).timestamp())


def chart(sym, prev, prints, *, currency="USD", reg_start=OPEN_TS, reg_end=OPEN_TS + 6 * 3600 + 1800):
    """prints: [(ts, close)] - the Yahoo v8 shape (range=1d, includePrePost)."""
    return {"chart": {"result": [{
        "meta": {"currency": currency, "symbol": sym, "chartPreviousClose": prev,
                 "currentTradingPeriod": {"pre": {"start": reg_start - 5 * 3600 + 1800, "end": reg_start},
                                          "regular": {"start": reg_start, "end": reg_end},
                                          "post": {"start": reg_end, "end": reg_end + 4 * 3600}}},
        "timestamp": [t for t, _ in prints],
        "indicators": {"quote": [{"close": [c for _, c in prints]}]}}], "error": None}}


PRE = OPEN_TS - 1800          # 14:00 UTC, a pre-market print


class FakeOpener:
    def __init__(self, payloads, fail=()):
        self.payloads, self.fail, self.calls = payloads, set(fail), []

    def __call__(self, req, timeout):
        url = req.full_url
        self.calls.append(url)
        sym = re.search(r"/chart/([^?]+)\?", url).group(1)
        if sym in self.fail:
            raise ConnectionError(sym)
        payload = self.payloads.get(sym)
        if payload is None:
            err = Exception("404")
            err.code = 404
            raise err
        import io
        import json

        class R:
            def __init__(self, b):
                self.b = b

            def read(self):
                return self.b

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False
        return R(json.dumps(payload).encode("utf-8"))


def quotes(payloads, fail=()):
    return pc.Quotes(20, opener=FakeOpener(payloads, fail), sleep=lambda s: None)


PAYLOADS = {
    "ES=F": chart("ES=F", 6000.0, [(PRE - 600, 5950.0), (PRE, 5928.0)]),            # -1.20 %
    "SNDA": chart("SNDA", 25.60, [(PRE - 300, 25.10), (PRE, 24.90)]),                # -1.97 % vs planned 25.40
    "IBOC": chart("IBOC", 61.00, [(PRE, 58.90)]),                                    # -3.60 %
    "BRK-B": chart("BRK-B", 479.0, []),                                              # no pre-market print
}


def test_p1_symbols_numbers_and_grid():
    assert pc.us_yahoo_symbol("SNDA.US") == "SNDA" and pc.us_yahoo_symbol("BRK.B.US") == "BRK-B"
    assert pc.us_yahoo_symbol("2222.SR") == "" and pc.us_yahoo_symbol("") == "" and pc.us_yahoo_symbol("ES=F") == ""
    assert pc._num("6,000") == 6000.0 and pc._num("SAR 1,250.5") == 1250.5 and pc._num("-3.2%") == -3.2
    assert pc._num("") is None and pc._num(None) is None and pc._num(True) is None and pc._num(float("nan")) is None
    t = pc.read_tickets(TOP10.values)
    assert [x["symbol"] for x in t] == ["SNDA.US", "IBOC.US", "BRK.B.US", "2222.SR", "FRST.US", "GHOST.US"]
    assert [x["executable"] for x in t] == [True, True, True, True, False, False]
    assert t[0]["price"] == 25.40 and t[0]["market"] == "US" and t[0]["ccy"] == "USD"
    assert pc.read_tickets([]) == [] and pc.read_tickets([["Symbol", "Price"]]) == []     # no Ticket SAR: no header
    # a repeated header row and a note row inside the body are skipped
    assert [x["symbol"] for x in pc.read_tickets(TOP10.values + [HDR, ["", "Research seats below"]])] == \
        ["SNDA.US", "IBOC.US", "BRK.B.US", "2222.SR", "FRST.US", "GHOST.US"]


def test_p2_chart_parser():
    q = pc.parse_prepost(PAYLOADS["SNDA"], int(NOW.timestamp()))
    assert q == {"last": 24.90, "last_ts": PRE, "prev_close": 25.60, "session": "pre", "currency": "USD"}
    # a print after the regular open is a regular-session print; one after the close is post
    reg = chart("X", 10.0, [(PRE, 9.9), (OPEN_TS + 60, 10.2)])
    assert pc.parse_prepost(reg, OPEN_TS + 120)["session"] == "regular"
    assert pc.parse_prepost(reg, OPEN_TS + 120)["last"] == 10.2
    assert pc.parse_prepost(reg, PRE + 1)["last"] == 9.9                              # prints after now are ignored
    post = chart("X", 10.0, [(OPEN_TS + 8 * 3600, 10.5)])
    assert pc.parse_prepost(post)["session"] == "post"
    # null closes are skipped; no print at all -> session none, previous close still read
    q = pc.parse_prepost(chart("X", 10.0, [(PRE - 60, None), (PRE, None)]))
    assert q["last"] is None and q["session"] == "none" and q["prev_close"] == 10.0
    assert pc.parse_prepost(PAYLOADS["BRK-B"])["last"] is None
    for bad in ({}, {"chart": {"result": []}}, {"chart": {"result": [None]}}, None, "x"):
        assert pc.parse_prepost(bad)["last"] is None, bad
    assert pc.pct(97.0, 100.0) == -3.0 and pc.pct(None, 1.0) is None and pc.pct(1.0, 0.0) is None


def test_p3_verdicts():
    es_wait = pc.parse_prepost(PAYLOADS["ES=F"])
    assert pc.market_verdict(es_wait, 1.0) == ("WAIT", "S&P futures -1.20% vs settlement (wait at <= -1.00%)", -1.2)
    assert pc.market_verdict(es_wait, 1.5)[0] == "GO"                                 # a wider threshold lets it through
    assert pc.market_verdict({}, 1.0) == ("CHECK", "no ES=F quote - read the futures before the open", None)
    assert pc.market_verdict(pc.parse_prepost(chart("ES=F", 6000.0, [(PRE, 5940.0)])), 1.0)[0] == "WAIT"   # exactly -1.00 %
    t = pc.read_tickets(TOP10.values)
    snda, iboc, brk, saudi, frst, ghost = t
    q = {"SNDA.US": pc.parse_prepost(PAYLOADS["SNDA"]), "IBOC.US": pc.parse_prepost(PAYLOADS["IBOC"]),
         "BRK.B.US": pc.parse_prepost(PAYLOADS["BRK-B"])}
    # a GO market: the band decides
    r = pc.ticket_verdict(snda, q["SNDA.US"], "GO", 3.0)
    assert r["verdict"] == "GO" and r["gap"] == -1.97 and r["session"] == "pre" and "within 3.0%" in r["reason"]
    r = pc.ticket_verdict(iboc, q["IBOC.US"], "GO", 3.0)
    assert r["verdict"] == "WAIT" and r["gap"] == -3.6 and r["reason"].startswith("gap down -3.60%")
    up = pc.parse_prepost(chart("SNDA", 25.6, [(PRE, 26.30)]))                       # +3.54 %: buying above the plan
    assert pc.ticket_verdict(snda, up, "GO", 3.0)["verdict"] == "WAIT"
    assert pc.ticket_verdict(snda, up, "GO", 3.0)["reason"].startswith("gap up +3.54%")
    assert pc.ticket_verdict(iboc, q["IBOC.US"], "GO", 4.0)["verdict"] == "GO"          # the band is a knob
    # missing / unread quotes never read GO
    r = pc.ticket_verdict(brk, q["BRK.B.US"], "GO", 3.0)
    assert r["verdict"] == "CHECK" and r["reason"].startswith("no pre-market print") and r["gap"] is None
    r = pc.ticket_verdict(brk, {"unread": True}, "GO", 3.0)
    assert r["verdict"] == "CHECK" and r["reason"].startswith("quote unread (Yahoo)")
    assert pc.ticket_verdict(brk, None, "GO", 3.0)["verdict"] == "CHECK"
    # a WAIT market holds every executable US ticket, whatever its gap
    assert pc.ticket_verdict(snda, q["SNDA.US"], "WAIT", 3.0)["verdict"] == "WAIT"
    assert pc.ticket_verdict(brk, None, "WAIT", 3.0) == {
        "verdict": "WAIT", "reason": "market WAIT; no pre-market print - read the quote before the open",
        "last": None, "gap": None, "session": "none"}
    # an unread market downgrades a GO to CHECK, never upgrades a WAIT
    assert pc.ticket_verdict(snda, q["SNDA.US"], "CHECK", 3.0)["verdict"] == "CHECK"
    assert pc.ticket_verdict(iboc, q["IBOC.US"], "CHECK", 3.0)["verdict"] == "WAIT"
    # another session / a research seat: SKIP
    assert pc.ticket_verdict(saudi, None, "WAIT", 3.0)["verdict"] == "SKIP"
    assert pc.ticket_verdict(frst, None, "GO", 3.0) == {"verdict": "SKIP", "reason": "not an executable ticket",
                                                        "last": None, "gap": None, "session": ""}
    assert pc.ticket_verdict(ghost, None, "GO", 3.0)["verdict"] == "SKIP"           # SAR reserved, zero shares
    rows = pc.build_rows(NOW, NOW + dt.timedelta(minutes=30), t, q, es_wait, 1.0, 3.0)
    assert [r[2:4] for r in rows] == [["ES=F", "WAIT"], ["SNDA.US", "WAIT"], ["IBOC.US", "WAIT"],
                                      ["BRK.B.US", "WAIT"], ["2222.SR", "SKIP"], ["FRST.US", "SKIP"], ["GHOST.US", "SKIP"]]
    assert rows[0][:2] == ["2026-11-09 17:00", "2026-11-09 17:30"] and rows[0][5:10] == ["6000.00", "5928.00", "-1.20", "-1.20", "pre"]
    assert rows[1][5:9] == ["25.40", "24.90", "-1.97", "-1.20"] and all(len(r) == len(pc.HEADERS) for r in rows)


def test_p4_window():
    import exchange_calendars as xcals
    cal = xcals.get_calendar("XNYS")
    op, inside, why = pc.preopen_window(NOW, cal)
    assert op == dt.datetime(2026, 11, 9, 14, 30, tzinfo=UTC) and inside and why == "inside the pre-open window"
    assert pc.preopen_window(dt.datetime(2026, 11, 9, 13, 0, tzinfo=UTC), cal)[1] is False     # the 13:00 firing in winter
    assert pc.preopen_window(dt.datetime(2026, 11, 9, 14, 44, tzinfo=UTC), cal)[1] is True      # a late runner, 14 min after the open
    assert pc.preopen_window(dt.datetime(2026, 11, 9, 14, 46, tzinfo=UTC), cal)[1] is False
    # summer: 6 Jul 2026 opens 13:30 UTC -> the 13:00 firing lands, the 14:00 one does not
    assert pc.preopen_window(dt.datetime(2026, 7, 6, 13, 0, tzinfo=UTC), cal)[1] is True
    assert pc.preopen_window(dt.datetime(2026, 7, 6, 14, 0, tzinfo=UTC), cal)[1] is False
    # Thanksgiving: no session, nothing to do
    op, inside, why = pc.preopen_window(dt.datetime(2026, 11, 26, 14, 0, tzinfo=UTC), cal)
    assert op is None and not inside and why == "no US session today"
    assert "outside the pre-open window (2026-11-09 16:15 .. 2026-11-09 17:45 Riyadh)" == \
        pc.preopen_window(dt.datetime(2026, 11, 9, 10, 0, tzinfo=UTC), cal)[2]


def _env(monkeypatch):
    for k in ("TFB_PREOPEN_ES_WAIT_PCT", "TFB_PREOPEN_GAP_PCT", "TFB_PREOPEN_SHEET", "TFB_PREOPEN_PAGE",
              "TFB_PREOPEN_TIMEOUT_S"):
        monkeypatch.delenv(k, raising=False)


def test_p5_main(monkeypatch, capsys):
    import exchange_calendars as xcals
    cal = xcals.get_calendar("XNYS")
    _env(monkeypatch)
    book = Book({"Top_10_Investments": TOP10})
    # outside the window: nothing read from Yahoo, nothing written
    q = quotes(PAYLOADS)
    assert pc.main(["--write"], book=book, quotes=q, now=dt.datetime(2026, 11, 9, 10, 0, tzinfo=UTC), cal=cal) == 0
    assert q.opener.calls == [] and book.tab is None and "nothing to do" in capsys.readouterr().out
    # inside: dry run reads everything and writes nothing
    q = quotes(PAYLOADS)
    assert pc.main(["--dry-run"], book=book, quotes=q, now=NOW, cal=cal) == 0 and book.tab is None
    out = capsys.readouterr().out
    assert "6 ticket row(s), 4 executable, 3 US" in out and "dry run" in out
    assert [re.search(r"/chart/([^?]+)\?", u).group(1) for u in q.opener.calls] == ["ES=F", "SNDA", "IBOC", "BRK-B"]
    assert all("includePrePost=true" in u for u in q.opener.calls)
    assert "interval=5m" in q.opener.calls[0] and all("interval=1m" in u for u in q.opener.calls[1:])
    # write: the tab is created whole (header + market row + 5 tickets + blank fill), header frozen
    q = quotes(PAYLOADS)
    assert pc.main(["--write"], book=book, quotes=q, now=NOW, cal=cal) == 0
    assert book.added == [("Pre_Open_Check", 60, 11)]
    vals = book.tab.requests[-1]["values"]
    assert vals[0] == pc.HEADERS and len(vals) == 60 and book.tab.requests[-1]["range"] == "A1:K60"
    assert [r[2:4] for r in vals[1:8]] == [["ES=F", "WAIT"], ["SNDA.US", "WAIT"], ["IBOC.US", "WAIT"],
                                           ["BRK.B.US", "WAIT"], ["2222.SR", "SKIP"], ["FRST.US", "SKIP"], ["GHOST.US", "SKIP"]]
    assert vals[8] == [""] * 11
    # a calm market: GO / WAIT (gap) / CHECK (no print)
    calm = dict(PAYLOADS, **{"ES=F": chart("ES=F", 6000.0, [(PRE, 6012.0)])})
    q = quotes(calm)
    assert pc.main(["--write"], book=book, quotes=q, now=NOW, cal=cal) == 0
    vals = book.tab.requests[-1]["values"]
    assert [r[2:4] for r in vals[1:5]] == [["ES=F", "GO"], ["SNDA.US", "GO"], ["IBOC.US", "WAIT"], ["BRK.B.US", "CHECK"]]
    assert vals[1][8] == "+0.20"
    # the knobs move the verdicts
    monkeypatch.setenv("TFB_PREOPEN_GAP_PCT", "4")
    monkeypatch.setenv("TFB_PREOPEN_ES_WAIT_PCT", "0.1")
    assert pc.main(["--dry-run"], book=book, quotes=quotes(calm), now=NOW, cal=cal) == 0
    out = capsys.readouterr().out
    assert "IBOC.US | GO" in out and "ES=F | GO" in out
    _env(monkeypatch)
    # the quote breaker: two consecutive failures skip the rest; reported in the tab as CHECK, never GO
    q = quotes(calm, fail={"SNDA", "IBOC"})
    assert pc.main(["--write"], book=book, quotes=q, now=NOW, cal=cal) == 0
    assert q.failures == ["SNDA", "IBOC"] and q.skipped == ["BRK.B.US"]
    vals = book.tab.requests[-1]["values"]
    assert [r[3] for r in vals[2:5]] == ["CHECK", "CHECK", "CHECK"] and all(r[4].startswith("quote unread") for r in vals[2:5])
    assert "WARN: quotes unread: SNDA, IBOC | skipped after the breaker: BRK.B.US" in capsys.readouterr().out
    # ES=F unread: the market reads CHECK and every GO becomes CHECK; a gap WAIT stays WAIT
    q = quotes(calm, fail={"ES=F"})
    assert pc.main(["--write"], book=book, quotes=q, now=NOW, cal=cal) == 0
    vals = book.tab.requests[-1]["values"]
    assert [r[3] for r in vals[1:5]] == ["CHECK", "CHECK", "WAIT", "CHECK"]
    # a broken page refuses; nothing written
    before = len(book.tab.requests)
    broken = Book({"Top_10_Investments": Broken()}, tab=book.tab)
    assert pc.main(["--write"], book=broken, quotes=quotes(calm), now=NOW, cal=cal) == 2
    assert len(book.tab.requests) == before and "cannot read page Top_10_Investments" in capsys.readouterr().out
    # --ignore-window rehearses outside the window (dry run by default in the workflow)
    q = quotes(calm)
    assert pc.main(["--dry-run", "--ignore-window"], book=book, quotes=q, now=dt.datetime(2026, 11, 9, 10, 0, tzinfo=UTC), cal=cal) == 0
    assert len(q.opener.calls) == 4 and len(book.tab.requests) == before
    # a prior tab beyond the bound is preserved
    big = Book({"Top_10_Investments": TOP10}, tab=Sheet([pc.HEADERS], 11, row_count=pc.MAX_ROWS + 5))
    assert pc.main(["--write"], book=big, quotes=quotes(calm), now=NOW, cal=cal) == 2 and big.tab.requests == []
    # knob parsing
    monkeypatch.setenv("TFB_PREOPEN_ES_WAIT_PCT", "9")
    assert pc.es_wait_pct() == 5.0
    monkeypatch.setenv("TFB_PREOPEN_ES_WAIT_PCT", "junk")
    assert pc.es_wait_pct() == 1.0
    monkeypatch.setenv("TFB_PREOPEN_GAP_PCT", "0.1")
    assert pc.gap_pct() == 0.5
    monkeypatch.setenv("TFB_PREOPEN_TIMEOUT_S", "2")
    assert pc.timeout_s() == 5


def test_p6_workflow():
    wf = yaml.safe_load((ROOT / ".github" / "workflows" / "pre_open_check.yml").read_text(encoding="utf-8"))
    on = wf.get("on") or wf.get(True)
    assert on["schedule"] == [{"cron": "0 13,14 * * 1-5"}]
    assert on["workflow_dispatch"]["inputs"]["write"]["default"] in (False, "false")
    assert on["workflow_dispatch"]["inputs"]["ignore_window"]["default"] in (False, "false")
    job = wf["jobs"]["pre_open_check"]
    run_step = [s for s in job["steps"] if "pre_open_check.py" in (s.get("run") or "")][0]
    env = run_step["env"]
    assert "GOOGLE_SHEETS_CREDENTIALS" in env and env["DEFAULT_SPREADSHEET_ID"] == "${{ steps.sheet.outputs.sid }}"
    assert env["TFB_PREOPEN_ES_WAIT_PCT"] == "${{ vars.TFB_PREOPEN_ES_WAIT_PCT || '1.0' }}"
    assert env["TFB_PREOPEN_GAP_PCT"] == "${{ vars.TFB_PREOPEN_GAP_PCT || '3.0' }}"
    assert 'MODE="--dry-run"' in run_step["run"] and 'EXTRA="--ignore-window"' in run_step["run"]
    assert "python scripts/pre_open_check.py $MODE $EXTRA" in run_step["run"]
    assert wf["permissions"] == {"contents": "read"} and wf["concurrency"]["cancel-in-progress"] is True
    src = (ROOT / "scripts" / "pre_open_check.py").read_text(encoding="utf-8")
    assert '__version__ = "1.0.0"' in src and "never silently GO" in src


if __name__ == "__main__":   # pragma: no cover
    sys.exit(pytest.main([__file__, "-q"]))
