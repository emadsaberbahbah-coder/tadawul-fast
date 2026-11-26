"""32_Risk_Watch v1.0.0 (an e-mail within minutes when a holding nears its stop or its target, falls sharply, or sits
exposed at the open): run the native harness (the REAL Apps Script source in a vm) so the whole-suite CI job collects it,
and pin what the watch reads to what the Exit Plan writes - the page layout, the ledger columns, the broker note, the
Yahoo symbol and the holiday list. Mirrors tests/test_prd_v100_page_dispatch.py."""
from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from scripts import exit_plan as xp

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "apps_script" / "32_Risk_Watch.gs"
HARNESS = ROOT / "tests" / "test_rw_v100_risk_watch.js"


def node_binary():
    for candidate in (os.environ.get("TFB_TEST_NODE", ""), shutil.which("node") or "",
                      "/opt/node22/bin/node"):
        if candidate and os.path.exists(candidate):
            return candidate
    return None


def _node(script: str, payload: dict) -> dict:
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run([node, "-e", script, str(SOURCE)], cwd=ROOT, input=json.dumps(payload), capture_output=True,
                            text=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


LOAD = ("const fs=require('fs'),vm=require('vm');const ctx={Logger:{log(){}}};vm.createContext(ctx);"
        "vm.runInContext(fs.readFileSync(process.argv[1],'utf8'),ctx);"
        "const inp=JSON.parse(fs.readFileSync(0,'utf8'));")


def test_rw_v100_risk_watch_native_harness():
    node = node_binary()
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run([node, str(HARNESS)], cwd=ROOT, capture_output=True, text=True, timeout=600)
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.rstrip().endswith("PASS 19/19"), result.stdout[-2000:]


def _result(symbol, shares, entry, last, stop, target, atr_pct, broker, action="HOLD"):
    return {"ok": True, "symbol": symbol, "name": symbol + " Inc", "shares": shares, "entry": entry, "last": last, "live": None,
            "pl_pct": (last / entry - 1) * 100, "hc": last, "atr_pct": atr_pct, "r_high": 0.0, "stop": stop, "prev_stop": None,
            "target": target, "action": action, "why": ["why"], "broker_stop": broker}


def test_the_watch_reads_the_page_the_exit_plan_writes():
    """The page is built by the Exit Plan's own page_rows; the watch's reader must find every holding and level in it."""
    results = [_result("IBOC.US", 10, 66.0, 63.77, 62.67, 74.81, 1.9, 62.67),
               _result("BIG.US", 3, 1180.5, 1234.56, 1100.25, 1300.0, 2.2, None),          # 1,234.56 on the page
               _result("DDI.US", 10, 14.0, 15.5, 15.22, 16.25, 0.6, None, action="REVIEW")]
    for note, known in (("broker orders from the IBKR capture of 2026-11-25 08:05 Riyadh", True),
                        ("broker orders read 2026-11-25 08:05", True),
                        ("broker orders unknown (last read 2026-10-17 09:00, 940 h old)", False),
                        ("broker orders unknown (no _IBKR_Account read)", False)):
        info = {"headline": "As of %s Riyadh · daily bars to %s · %s" % ("2026-11-25 08:35", "2026-11-24", note),
                "summary": "Today: HOLD 3"}
        page = xp.page_rows(results, [], info, xp.knobs())
        assert "1,234.56" in page[8]                                               # the page formats with separators
        ledger = [list(xp.LEDGER_HEADERS),
                  ["2026-11-24", "x", "IBOC.US", "l", 66.0, 10, 64.0, 64.0, 1.2, 6.07, 62.67, 74.81, "yes", "HOLD", "", "", 1, "", ""],
                  ["2026-11-25", "x", "IBOC.US", "l", 66.0, 10, 63.77, 63.77, 1.2, 6.07, 62.67, 74.81, "", "HOLD", "", "", 1, "", ""],
                  ["2026-11-25", "x", "BIG.US", "l", 1180.5, 3, 1234.56, 1234.56, 27.0, 80.25, 1100.25, 1300.0, "yes", "HOLD", "", "", 1, "", "on_its_own:stop_stands:-9.1:-0.4"],
                  ["2026-11-25", "x", "DDI.US", "l", 14.0, 10, 15.5, 15.5, 0.09, 1.2, 15.22, 16.25, "", "REVIEW", "", "", 1, "", "offer:manual"]]
        got = _node(LOAD + "console.log(JSON.stringify(ctx.rwHoldingsCore_(inp.page, inp.ledger, null)));",
                    {"page": page, "ledger": ledger})
        assert got["ok"] is True and got["asOf"] == "2026-11-25 08:35" and got["count"] == 3 and got["brokerKnown"] is known, (note, got)
        by = {h["symbol"]: h for h in got["list"]}
        assert sorted(by) == ["BIG.US", "DDI.US", "IBOC.US"]
        i = by["IBOC.US"]
        assert (i["shares"], i["entry"], i["lastClose"], i["atrPct"], i["stop"], i["target"], i["action"]) == (10, 66.0, 63.77, 1.9, 62.67, 74.81, "HOLD")
        assert i["brokerStop"] == (62.67 if known else None) and i["tp1"] is False and i["offer"] is False   # the newest ledger day decides
        b = by["BIG.US"]
        assert (b["lastClose"], b["stop"], b["entry"]) == (1234.56, 1100.25, 1180.5) and b["brokerStop"] is None and b["tp1"] is True
        assert by["DDI.US"]["offer"] is True and by["DDI.US"]["target"] == 16.25 and by["DDI.US"]["action"] == "REVIEW"
    # an all-cash book: the table is there, empty
    empty = xp.page_rows([], [], {"headline": "As of 2026-11-25 08:35 Riyadh · daily bars to n/a · broker orders read 2026-11-25 08:05",
                                  "summary": "Today: no holdings"}, xp.knobs())
    got = _node(LOAD + "console.log(JSON.stringify(ctx.rwHoldingsCore_(inp.page, inp.ledger, null)));", {"page": empty, "ledger": []})
    assert got["ok"] is True and got["count"] == 0 and got["list"] == []


def test_a_cash_offer_is_read_from_the_exit_plans_own_words_without_the_ledger():
    """Codex P2 on #1088: with the ledger missing, a holding kept under a cash offer must not become a stop-watched one.
    The Why is written by the Exit Plan's own offer_action; the watch reads the offer from it, with no ledger at all."""
    import datetime as dt

    from core import corporate_offers as co

    offer = co.parse({"schema": co.SCHEMA, "offers": [{
        "symbol": "DDI.US", "kind": "cash_tender", "price": 16.25, "announced": "2026-11-10", "expires": "2026-12-10",
        "bidder": "Bidder Co", "conditions": "a minimum", "sale": "manual"}]})[0]
    held = _result("DDI.US", 10, 14.0, 15.5, 15.22, 16.25, 0.6, None)
    held["why"] = []
    xp.offer_action(held, offer, [], xp.knobs(), dt.date(2026, 11, 25))
    assert held["offer_held"] is True and "the sale is the owner's manual decision" in held["why"][-1]
    plain = _result("IBOC.US", 10, 66.0, 63.77, 62.67, 74.81, 1.9, 62.67)
    info = {"headline": "As of 2026-11-25 08:35 Riyadh · daily bars to 2026-11-24 · broker orders read 2026-11-25 08:05", "summary": "s"}
    page = xp.page_rows([held, plain], [], info, xp.knobs())
    for ledger in ([], [list(xp.LEDGER_HEADERS)], None):
        got = _node(LOAD + "console.log(JSON.stringify(ctx.rwHoldingsCore_(inp.page, inp.ledger, null)));", {"page": page, "ledger": ledger})
        by = {h["symbol"]: h for h in got["list"]}
        assert got["ok"] is True and got["partial"] is False and got["ledgerRead"] is False
        assert by["DDI.US"]["offer"] is True and by["DDI.US"]["target"] == 16.25 and by["DDI.US"]["tp1Known"] is False
        assert by["IBOC.US"]["offer"] is False and by["IBOC.US"]["stop"] == 62.67
    # the words the watch looks for are the Exit Plan's, in its source
    rx = re.search(r"var RW_OFFER_RE_ = /(.+?)/i;", SOURCE.read_text(encoding="utf-8")).group(1)
    assert rx == "the sale is the owner's manual decision" and rx in Path(xp.__file__).read_text(encoding="utf-8")


def test_a_long_page_is_read_whole_and_checked_against_its_own_count():
    """Codex P2 on #1088: page_rows has no limit on holdings, so the watch reads every row and holds the rows read
    against the page's own HOLDINGS count."""
    results = [_result("H%02d.US" % i, 10, 100.0, 100.0, 90.0, 120.0, 2.0, 90.0) for i in range(70)]
    # one holding without daily bars: listed on the page with blank levels, and counted
    results.append({"ok": False, "symbol": "NOBARS.US", "name": "n", "shares": 10, "entry": 5.0, "action": "REVIEW", "why": ["no daily price history"]})
    info = {"headline": "As of 2026-11-25 08:35 Riyadh · daily bars to 2026-11-24 · broker orders read 2026-11-25 08:05", "summary": "s"}
    page = xp.page_rows(results, [], info, xp.knobs())
    got = _node(LOAD + "console.log(JSON.stringify(ctx.rwHoldingsCore_(inp.page, [], null)));", {"page": page})
    assert got["ok"] is True and got["count"] == 71 and len(got["list"]) == 71 and got["partial"] is False
    blank = [h for h in got["list"] if h["symbol"] == "NOBARS.US"][0]
    assert blank["stop"] is None and blank["target"] is None and blank["lastClose"] is None
    cut = _node(LOAD + "console.log(JSON.stringify(ctx.rwHoldingsCore_(inp.page, [], null)));", {"page": page[:60]})
    assert cut["ok"] is True and cut["partial"] is True and cut["count"] == 71 and len(cut["list"]) == 53
    assert cut["why"] == "Exit_Plan says 71 holding(s); 53 row(s) were read"
    src = SOURCE.read_text(encoding="utf-8")
    assert "Math.min(sp.getLastRow(), RW_.PAGE_MAX_ROWS)" in src and "PAGE_MAX_ROWS: 2000" in src


def test_the_watch_names_what_the_exit_plan_names():
    src = SOURCE.read_text(encoding="utf-8")
    xsrc = Path(xp.__file__).read_text(encoding="utf-8")
    assert src.isascii()
    assert "TAB_PAGE: '%s'" % xp.PAGE_TAB in src and "TAB_LEDGER: '%s'" % xp.LEDGER_TAB in src
    for col in ("Date", "Symbol", "TP1 Reached", "Judgment"):
        assert col in xp.LEDGER_HEADERS and ("col.%s" % col in src or "col['%s']" % col in src), col
    # the headline and the three broker notes the reader tells apart
    assert '"As of %s Riyadh · daily bars to %s · %s"' in xsrc
    for note in ('"broker orders unknown (', '"broker orders read %s"', '"broker orders from the IBKR capture of %s Riyadh"'):
        assert note in xsrc, note
    assert 'res["judgment"] = "offer:manual"' in xsrc and "/^offer:manual/i" in src
    # a held offer's target is the offer price: the watch reads the offer price from the Target column
    assert 'target = offer["price"]' in xsrc
    # the same Yahoo symbol
    syms = ["IBOC.US", "BRK.B.US", "brk.b.us", "2222.SR", "MBIN.US", "BF.B.US"]
    got = _node(LOAD + "console.log(JSON.stringify(inp.syms.map(function (s) { return ctx.rwYahooSymbol_(s); })));", {"syms": syms})
    assert got == [xp.yahoo_symbol(s) for s in syms]
    # the same two hosts
    for host in xp.YAHOO_HOSTS:
        assert "'%s'" % host in src
    # the watch's limits name the plan's own offer limits
    k = xp.knobs()
    limits = _node(LOAD + "console.log(JSON.stringify(ctx.rwLimitsCore_('')));", {})
    assert limits["offer_gap_pct"] == k["offer_gap_pct"] and limits["offer_at_pct"] == k["offer_near_pct"]


def test_the_holiday_list_holds_the_repositorys_own():
    src = SOURCE.read_text(encoding="utf-8")
    block = src.split("var RW_HOLIDAYS_ = [")[1].split("];")[0]
    mine = set(re.findall(r"'(\d{4}-\d{2}-\d{2})'", block))
    core = (ROOT / "core" / "analysis" / "portfolio_actions.py").read_text(encoding="utf-8")
    us = core.split('_CONFIRM_SESSION_HOLIDAYS = {')[1].split('"KSA"')[0]
    theirs = set(re.findall(r'"(\d{4}-\d{2}-\d{2})"', us))
    assert len(theirs) >= 11 and theirs <= mine, sorted(theirs - mine)
    assert {"2026-11-26", "2026-12-25", "2027-11-25", "2027-12-24"} <= mine and "2026-11-27" not in mine
    # every listed day is a weekday (a closure on a weekend needs no entry)
    import datetime as dt
    assert all(dt.date.fromisoformat(d).weekday() < 5 for d in mine)


def test_source_says_what_it_does():
    src = SOURCE.read_text(encoding="utf-8")
    assert " * Version: 1.0.0 (2026-11-26)" in src and "var RW_VERSION = '1.0.0';" in src
    assert "everyMinutes(every)" in src and "EVERY_MIN: 5," in src
    # it sends e-mail and reads Yahoo; it calls nothing else
    assert src.count("UrlFetchApp.") == 1 and "UrlFetchApp.fetchAll(reqs)" in src
    assert src.count("MailApp.sendEmail(") == 2                                     # the alert and the owner's test e-mail
    low = src.lower()
    for word in ("ibkr.com", "interactivebrokers", "placeorder", "cancelorder", "modifyorder"):
        assert word not in low, word


def test_no_new_google_service_so_no_new_authorization():
    """A service the bound project does not use yet would add an authorization scope, and every trigger of the project
    stops until the owner grants it. The watch uses only services other files already use."""
    src = SOURCE.read_text(encoding="utf-8")
    code = "\n".join(ln for ln in src.splitlines() if not ln.lstrip().startswith(("*", "/*", "//")))
    used = set(re.findall(r"\b([A-Z][A-Za-z]+(?:App|Service)|Session|Utilities|Logger)\.", code))
    others = ""
    for path in sorted((ROOT / "apps_script").glob("*.gs")):
        if path.name != SOURCE.name:
            others += path.read_text(encoding="utf-8", errors="replace")
    assert used == {"PropertiesService", "SpreadsheetApp", "UrlFetchApp", "MailApp", "LockService", "ScriptApp", "Session",
                    "Utilities", "Logger"}, sorted(used)
    for service in used:
        assert service + "." in others, service
    # the calls that carry a scope of their own are already made elsewhere
    for call in ("MailApp.sendEmail(", "UrlFetchApp.fetch", "Session.getEffectiveUser().getEmail()", "ScriptApp.newTrigger("):
        assert call in others, call
