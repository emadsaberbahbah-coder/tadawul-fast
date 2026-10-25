"""run_daily_brief v1.19.0: ledger holdings the decision page omits are named,
partial book totals are withheld, and the send label follows the clock."""
import datetime as dt

import pytest

from scripts import run_daily_brief as brief


HEADER = ["Action", "Symbol", "Name", "P&L %", "Weight %", "Engine ROI %",
          "Rel", "Conf", "Advisor Note", "MV SAR", "Cost SAR"]
MP_HEADER = ["Symbol", "Name", "Position Qty", "Avg Cost"]
SUN_2115 = dt.datetime(2026, 10, 25, 21, 15)   # the evidence brief's send time


def row(action, symbol, mv=1_100.0, cost=1_000.0):
    return [action, symbol, "Synthetic holding", None, None, None, None, "", "", mv, cost]


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setenv("TFB_BRIEF_NEWS_CONTEXT", "0")
    monkeypatch.setenv("TFB_MIN_TICKET_SAR", "0")
    monkeypatch.delenv("TFB_BRIEF_HOLDINGS_RECONCILE", raising=False)
    monkeypatch.delenv("TFB_BRIEF_SEND_LABEL", raising=False)


def build(decision_rows, ledger_rows):
    return brief.build_model({brief.PAGE_DECISION: decision_rows,
                              brief.PAGE_PORTFOLIO: ledger_rows})


def test_empty_decision_with_ledger_holding_is_named_not_zero():
    data = build([HEADER], [MP_HEADER, ["5023.SR", "Arabian Centres Sukuk", 100, 101.0]])
    d = data["decision"]
    assert d["ledger_missing"] == ["5023.SR"]
    html = brief.render_html(data, "Owner", SUN_2115)
    text = brief.render_text(data, "Owner", SUN_2115)
    assert "0 of 1 holdings" in html
    assert "0 holdings" not in html.replace("0 of 1 holdings", "")
    for out in (html, text):
        assert "Portfolio_Decision does not list (5023.SR)" in out


def test_partial_book_total_is_withheld():
    data = build([HEADER, row("HOLD", "AAA.SR")],
                 [MP_HEADER, ["AAA.SR", "A", 10, 100.0], ["BBB.SR", "B", 5, 50.0]])
    d = data["decision"]
    assert d["ledger_missing"] == ["BBB.SR"]
    assert d["mv"] is d["cost"] is d["pl_pct"] is None
    assert "1 of 2 holdings" in brief.render_html(data, "Owner", SUN_2115)


def test_complete_book_is_unchanged():
    data = build([HEADER, row("HOLD", "AAA.SR")], [MP_HEADER, ["AAA.SR", "A", 10, 100.0]])
    d = data["decision"]
    assert "ledger_missing" not in d
    assert d["mv"] == 1_100.0 and d["cost"] == 1_000.0
    assert "1 holdings" in brief.render_html(data, "Owner", SUN_2115)


def test_zero_quantity_rows_are_not_holdings_but_unknown_quantity_is():
    assert brief._ledger_holdings([MP_HEADER, ["ZERO.SR", "Z", 0, 1.0],
                                   ["BLANK.SR", "B", "", 1.0], ["X.SR", "X", 3, 1.0]]) == [
        "BLANK.SR", "X.SR"]
    assert brief._ledger_holdings([]) == []


def test_kill_switch_restores_previous_count(monkeypatch):
    monkeypatch.setenv("TFB_BRIEF_HOLDINGS_RECONCILE", "0")
    data = build([HEADER], [MP_HEADER, ["5023.SR", "S", 100, 101.0]])
    assert "ledger_missing" not in data["decision"]
    assert "0 holdings" in brief.render_html(data, "Owner", SUN_2115)


@pytest.mark.parametrize("when,label", [
    (dt.datetime(2026, 10, 25, 7, 30), "before Tadawul open"),
    (dt.datetime(2026, 10, 25, 10, 0), "Tadawul session open"),
    (dt.datetime(2026, 10, 25, 14, 59), "Tadawul session open"),
    (SUN_2115, "after Tadawul close"),
    (dt.datetime(2026, 10, 23, 12, 0), "Tadawul closed (weekend)"),   # Friday
    (dt.datetime(2026, 10, 24, 12, 0), "Tadawul closed (weekend)"),   # Saturday
])
def test_send_window_label(when, label):
    assert brief._send_window_label(when) == label


def test_evening_brief_no_longer_claims_before_open(monkeypatch):
    data = build([HEADER, row("HOLD", "AAA.SR")], [MP_HEADER, ["AAA.SR", "A", 10, 100.0]])
    html = brief.render_html(data, "Owner", SUN_2115)
    assert "before market open" not in html
    assert "Sent after Tadawul close" in html
    monkeypatch.setenv("TFB_BRIEF_SEND_LABEL", "0")
    assert "Sent before market open" in brief.render_html(data, "Owner", SUN_2115)
