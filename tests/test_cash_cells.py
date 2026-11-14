"""scripts/cash_cells.py 1.0.0 - the typed cash cells follow the broker (owner, 13 Nov 2026 12:34: C8 "need to solve it").

  M1  the cell right of the label, on the live page shapes; A1 names
  M2  decide: write / same / no label / not a number; the tolerance
  M3  broker cash: whole SAR, rounded down, less working buys; no figure from an old, unread or unpriced capture
  M4  a live run writes exactly one cell per page and nothing else; observe, dry run and off write nothing
  M5  nothing printed carries an amount; a page that cannot be read is an error, the other page is still done
  M6  the workflow step, the gate default and the manifest pin

Zero network: fake workbook, parsed captures built by scripts/ibkr_capture.parse. Every figure and ticker here
is invented (the repository is public).
"""
import datetime as dt
import pathlib
import re
import sys
from copy import deepcopy

import pytest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import cash_cells as cz  # noqa: E402
from scripts import ibkr_capture as ic  # noqa: E402

UTC = dt.timezone.utc
NOW = dt.datetime(2026, 11, 13, 9, 30, tzinfo=UTC)               # 12:30 Riyadh
CREATED = dt.datetime(2026, 11, 13, 5, 41, 17, tzinfo=UTC)

T10 = [["TOP 10 INVESTMENTS"], ["Status:", "Last run 2026-11-13 10:52:11 | status: ok"],
       ["T10: Include Portfolio Holdings", "No", "", "T10: Base Currency", "SAR", "", "Cash Available (SAR)", "50,000 SAR"],
       ["Pending Proceeds (SAR)", "0 SAR", "", "Pool Source", "Sheets"], [], ["Rank", "Symbol"], [1, "CCC.US"]]
PD = [["Portfolio Decision"], [], ["Review Date", "2026-11-13"], [],
      ["PF: Cash Available SAR", "50,000 SAR", "", "PF: Target Cash %", "10.0", "", "PF: Max Position %", "15.0"],
      ["Portfolio (SAR)", "Holdings (SAR)", "Cash (SAR)"], ["130,000 SAR", "100,000 SAR", "30,000 SAR"]]


def cap(orders=(), cash=8000.5, settled=8000.5, created=CREATED, **over):
    raw = {"schema": "tfb.ibkr-capture/v1", "captured_at": created.astimezone(dt.timezone(dt.timedelta(hours=3))).isoformat(),
           "account_alias": "ibkr-primary", "positions": {"positions": [{"contract_description": "AAA", "position": 100.0}]},
           "balances": {"balances": [{"currency": "USD", "cash_balance": cash, "settled_cash": settled,
                                      "net_liquidation_value": 30000.0, "stock_market_value": 22000.0}]},
           "orders": {"orders": list(orders)}}
    raw.update(over)
    return ic.parse(raw, created, NOW, "TFB_IBKR_Capture_20261113-084101.json")


def buy(sym, qty, limit=None):
    o = {"order_id": 1, "order_status": "NEW", "order_type": "LIMIT" if limit else "MARKET", "side": "BUY",
         "total_shares_qty": str(qty), "cum_shares_qty": "0", "remaining_shares_qty": str(qty),
         "primary_description": "Buy %s %s" % (qty, sym), "secondary_description": "Day", "order_time": "2026-11-13T05:00:00Z"}
    if limit:
        o["limit_price"] = str(limit)
    return o


class WorksheetNotFound(Exception):
    pass


class Sheet:
    def __init__(self, rows):
        self.values, self.writes = deepcopy(rows), []

    def get(self, rng):
        assert rng == cz.SCAN
        return deepcopy(self.values)

    def update(self, values=None, range_name=None, value_input_option=None):
        self.writes.append((range_name, values, value_input_option))


class Book:
    def __init__(self, **tabs):
        self.tabs = {k: Sheet(v) for k, v in tabs.items()}

    def worksheet(self, name):
        if name not in self.tabs:
            raise WorksheetNotFound(name)
        return self.tabs[name]


def book():
    return Book(Top_10_Investments=T10, Portfolio_Decision=PD)


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    for name in ("TFB_CASH_CELLS", "TFB_CASH_CELLS_TOL_SAR", "TFB_PLAN_IBKR_MAX_AGE_H"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("TFB_CASH_CELLS", "on")


def run(b, c, dry=False):
    said = []
    return cz.run_once(dry, book=b, now=NOW, capture=c, out=said.append), said


def test_m1_the_cell_right_of_the_label():
    assert cz.find_cell(T10, "Cash Available (SAR)") == (2, 7, "50,000 SAR") and cz.a1(2, 7) == "H3"
    assert cz.find_cell(PD, "PF: Cash Available SAR") == (4, 1, "50,000 SAR") and cz.a1(4, 1) == "B5"
    assert cz.find_cell(PD, "Cash Available (SAR)") is None                       # the label whole, not a part of it
    assert cz.find_cell([["x", "Cash Available (SAR)"]], "Cash Available (SAR)") == (0, 2, "")
    assert cz.a1(0, 26) == "AA1"
    note = [["Type Cash Available (SAR) by hand", "see below"]] + T10               # a sentence that carries the label
    assert cz.find_cell(note, "Cash Available (SAR)") == (3, 7, "50,000 SAR")


def test_m2_decide():
    assert cz.decide(T10, "Cash Available (SAR)", 30001.0, 1) == ("write", "H3")
    assert cz.decide(T10, "Cash Available (SAR)", 50000.0, 1) == ("same", "H3")
    assert cz.decide(T10, "Cash Available (SAR)", 50001.0, 1) == ("same", "H3")
    assert cz.decide(T10, "Cash Available (SAR)", 50002.0, 1) == ("write", "H3")
    assert cz.decide(T10, "Cash Available (SAR)", 49960.0, 50) == ("same", "H3")
    assert cz.decide(T10, "No such label", 1.0, 1) == ("no label", "")
    moved = deepcopy(T10)
    moved[2][7] = "see Portfolio_Decision"
    assert cz.decide(moved, "Cash Available (SAR)", 30001.0, 1) == ("not a number", "H3")
    moved[2] = moved[2][:7]
    assert cz.decide(moved, "Cash Available (SAR)", 30001.0, 1) == ("not a number", "H3")


def test_m3_broker_cash():
    assert cz.broker_cash(cap(), 30) == (30001, "")                               # 8,000.50 x 3.75 = 30,001.875 -> down
    assert cz.broker_cash(cap(settled=6000.0), 30) == (22500, "")                 # the smaller of cash and settled cash
    assert cz.broker_cash(cap(orders=[buy("BBB", 180, 12.5)]), 30) == (21564, "")  # less 2,250 USD held by the order
    assert cz.broker_cash(cap(orders=[buy("BBB", 1000, 12.5)]), 30) == (0, "")    # never below zero
    none, why = cz.broker_cash(cap(orders=[buy("CCC", 5)]), 30)
    assert none is None and "no limit price" in why
    none, why = cz.broker_cash(cap(created=NOW - dt.timedelta(hours=31)), 30)
    assert none is None and "31 h old" in why
    assert cz.broker_cash(None, 30) == (None, "no capture")
    assert cz.broker_cash({"ok": False, "reason": "Drive list answered HTTP 403"}, 30)[1] == "Drive list answered HTTP 403"


def test_m4_one_cell_per_page_and_the_gate(monkeypatch):
    b = book()
    code, said = run(b, cap())
    assert code == 0
    assert b.tabs["Top_10_Investments"].writes == [("H3", [[30001]], "RAW")]
    assert b.tabs["Portfolio_Decision"].writes == [("B5", [[30001]], "RAW")]
    assert b.tabs["Top_10_Investments"].values == T10 and b.tabs["Portfolio_Decision"].values == PD
    assert said == ["Top_10_Investments 'Cash Available (SAR)': cell H3 set to the broker's figure",
                    "Portfolio_Decision 'PF: Cash Available SAR': cell B5 set to the broker's figure"]
    b = book()
    b.tabs["Top_10_Investments"].values[2][7] = "30,001 SAR"                       # already right: no write
    code, said = run(b, cap())
    assert b.tabs["Top_10_Investments"].writes == [] and len(b.tabs["Portfolio_Decision"].writes) == 1
    assert said[0].endswith("already the broker's figure")
    for setting, dry, word in (("observe", False, "observe"), ("on", True, "dry run")):
        monkeypatch.setenv("TFB_CASH_CELLS", setting)
        b = book()
        code, said = run(b, cap(), dry)
        assert code == 0 and not b.tabs["Top_10_Investments"].writes and not b.tabs["Portfolio_Decision"].writes
        assert said[0] == "Top_10_Investments 'Cash Available (SAR)': cell H3 differs from the broker's figure (not written: %s)" % word
    for setting in ("off", "", "yes"):
        monkeypatch.setenv("TFB_CASH_CELLS", setting)
        b = book()
        code, said = run(b, cap())
        assert code == 0 and said == ["TFB_CASH_CELLS=off - nothing to do"] and not b.tabs["Top_10_Investments"].writes
    monkeypatch.delenv("TFB_CASH_CELLS")
    assert cz.mode() == "off"                                                      # off unless the workflow says on


def test_m5_no_usable_capture_no_amounts_and_errors():
    b = book()
    code, said = run(b, cap(orders=[buy("CCC", 5)]))                                # a market buy: the cash it holds is unknown
    assert code == 0 and not b.tabs["Top_10_Investments"].writes and not b.tabs["Portfolio_Decision"].writes
    assert said == ["no broker cash to follow (a working buy order has no limit price, so the cash it holds is not known) "
                    "- the typed cells are left as they are"]
    code, said = run(book(), cap(created=NOW - dt.timedelta(hours=40)))
    assert code == 0 and "40 h old" in said[0]
    b = book()
    code, said = run(b, cap(orders=[buy("BBB", 180, 12.5)]))
    assert not re.search(r"\d{3}", " ".join(said))                                 # public log: no amount, no balance
    b = Book(Portfolio_Decision=PD)                                                # the main page is missing: _read gives no rows
    code, said = run(b, cap())
    assert code == 0 and said[0].endswith("label not found - nothing written") and len(b.tabs["Portfolio_Decision"].writes) == 1

    class Broken(Sheet):
        def get(self, rng):
            raise RuntimeError("quota")
    b = book()
    b.tabs["Top_10_Investments"] = Broken(T10)
    code, said = run(b, cap())
    assert code == 3 and said[0] == "ERROR: Top_10_Investments: RuntimeError" and len(b.tabs["Portfolio_Decision"].writes) == 1


def test_m6_workflow_and_manifest():
    raw = (ROOT / ".github/workflows/consistency_check.yml").read_text(encoding="utf-8")
    wf = yaml.safe_load(raw)
    steps = wf["jobs"]["consistency"]["steps"]
    names = [str(s.get("run", "")) for s in steps]
    cash = [i for i, r in enumerate(names) if "scripts/cash_cells.py" in r]
    check = [i for i, r in enumerate(names) if "scripts/consistency_check.py" in r]
    assert len(cash) == 1 and len(check) == 1 and cash[0] < check[0]              # the cells first, then the comparison
    env = steps[cash[0]]["env"]
    assert env["TFB_CASH_CELLS"] == "${{ vars.TFB_CASH_CELLS || 'on' }}"
    assert env["TFB_IBKR_CAPTURE"] == "${{ vars.TFB_IBKR_CAPTURE || 'on' }}" and "TFB_IBKR_CAPTURE_FOLDER_ID" in env
    assert steps[cash[0]].get("continue-on-error") is True                         # a failed write never stops the check
    assert wf["permissions"] == {"contents": "read"}
    src = (ROOT / "scripts/cash_cells.py").read_text(encoding="utf-8")
    for name in re.findall(r"(TFB_CASH[A-Z_]+): \$\{\{", raw):
        assert '"%s"' % name in src, name
    assert '("cash_cells", "%s", "cash cells")' % cz.__version__ in (ROOT / "scripts/verify_deployment.py").read_text(encoding="utf-8")
    assert "batch_update" not in src and src.count(".update(") == 1               # one write call, one cell
