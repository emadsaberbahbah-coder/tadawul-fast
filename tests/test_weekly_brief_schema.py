"""Weekly memo/state consume actual shadow-board rows without network I/O."""
import copy
from html.parser import HTMLParser

import pytest

from scripts import run_weekly_brief as brief


class TableReader(HTMLParser):
    def __init__(self):
        super().__init__()
        self.rows, self.row, self.cell = [], None, None

    def handle_starttag(self, tag, _attrs):
        if tag == "tr":
            self.row = []
        elif tag in ("th", "td"):
            self.cell = ""

    def handle_data(self, data):
        if self.cell is not None:
            self.cell += data

    def handle_endtag(self, tag):
        if tag in ("th", "td"):
            self.row.append(self.cell)
            self.cell = None
        elif tag == "tr":
            self.rows.append(self.row)
            self.row = None


@pytest.fixture
def board(monkeypatch):
    # These local settings make the synthetic fixture eligible. The workflow's
    # settings and production policy are not part of this schema-only repair.
    monkeypatch.setenv("TFB_COMPLIANCE_GATE_ENABLED", "1")
    monkeypatch.setenv("TFB_COMPLIANCE_SCREEN_RETIRED", "1")
    monkeypatch.setenv("TFB_SB_COST_LEGACY", "0")
    monkeypatch.setattr(brief.sa, "last_error", lambda: None)
    monkeypatch.setattr(brief, "_now_riyadh", lambda: "2026-10-18 07:10")
    cands = [{"symbol": symbol, "name": "Synthetic fixture", "action": "BUY",
              "roi_pct": 20.0, "confidence_band": "High",
              "market_value_sar": 20_000.0, "sector": "Technology",
              "roi_src_cell": "engine:Global_Markets"}
             for symbol in ("7010.SR", "1015.KL")]
    fundamentals = {c["symbol"]: {"market_cap": 100_000_000.0,
                                   "interest_debt": 10_000_000.0}
                    for c in cands}
    rows, summary = brief.sb.evaluate_board(cands, {}, {}, 130_000.0, fundamentals)
    assert all(len(row) == len(brief.sb.OUT_HEADER) for row in rows)
    assert brief.sb.eligible_symbols(rows) == {"7010.SR"}
    return rows, summary


def render(rows, summary):
    return brief.render_memo(
        rows, summary, {"verdict": "NO_ACTION", "proposals": [], "pairs_checked": 0},
        {}, {}, len(rows), len(rows), {"first": True}, 130_000.0)


@pytest.mark.parametrize("roi_source", ["", "engine:Global_Markets", "obs:20.0"])
def test_current_board_eligible_row_keeps_its_verdict_economics_and_state(board, roi_source):
    rows, summary = board
    eligible = rows[0]
    eligible[brief.sb.OUT_HEADER.index("ROI Src")] = roi_source
    before = copy.deepcopy(rows)

    assert brief._hard_gate_rows(rows)[0] == eligible
    state = brief.snapshot(rows, "NO_ACTION")
    assert state["board"]["7010.SR"]["gen2"] == "YES"
    assert state["board"]["1015.KL"]["gen2"] == "NO"

    _, text, html = render(rows, summary)
    text_header = next(line for line in text.splitlines() if line.startswith("Symbol | "))
    text_row = next(line for line in text.splitlines() if line.startswith("7010.SR | "))
    cells = dict(zip(text_header.split(" | "), text_row.split(" | ")))
    assert cells["Gen2 Eligible"] == "YES"
    assert cells["Edge Verdict"] == "TRADE"
    for name in ("ROI %", "RT Cost %", "Net Edge %", "Debt/MCap %"):
        assert cells[name] == str(eligible[brief.sb.OUT_HEADER.index(name)])

    table = TableReader()
    table.feed(html)
    html_cells = dict(zip(table.rows[0], next(row for row in table.rows[1:] if row[0] == "7010.SR")))
    assert html_cells == cells
    assert rows == before, "memo rendering must not change source board rows"


def test_roi_source_yes_cannot_authorize_a_blocked_board_row(board):
    rows, summary = board
    blocked = rows[1]
    # Simulate a legacy producer's cost display on an actually ineligible row.
    # A provenance string of YES must not turn that row into an eligible trade.
    for name, value in {"ROI Src": "YES", "Edge Verdict": "TRADE", "ROI %": 20.0,
                        "RT Cost %": 0.5, "Net Edge %": 12.5}.items():
        blocked[brief.sb.OUT_HEADER.index(name)] = value
    before = copy.deepcopy(blocked)
    gated = brief._hard_gate_rows([blocked])[0]
    assert gated[brief.sb.OUT_HEADER.index("Edge Verdict")] == "BLOCKED — BROKER_UNTRADABLE"
    assert all(gated[brief.sb.OUT_HEADER.index(name)] == "—"
               for name in ("ROI %", "RT Cost %", "Net Edge %"))
    assert gated[brief.sb.OUT_HEADER.index("Debt/MCap %")] == 10.0
    assert brief.snapshot([blocked], "NO_ACTION")["board"]["1015.KL"]["gen2"] == "NO"
    _, text, html = render([blocked], summary)
    assert "BLOCKED — BROKER_UNTRADABLE" in text and "BLOCKED — BROKER_UNTRADABLE" in html
    assert blocked == before


def test_adding_a_producer_column_does_not_move_memo_semantics(board, monkeypatch):
    rows, summary = board
    before = render(rows, summary), brief.snapshot(rows, "NO_ACTION")
    header = list(brief.sb.OUT_HEADER)
    header.insert(0, "Additional Provenance")
    monkeypatch.setattr(brief.sb, "OUT_HEADER", header)
    shifted = [["synthetic provenance", *row] for row in rows]
    assert (render(shifted, summary), brief.snapshot(shifted, "NO_ACTION")) == before
