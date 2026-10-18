"""Run the real Shadow Board CLI with synthetic Sheets and provider inputs.

An unreadable holdings source must not become a completed NO_ACTION scan.
Successful empty books remain valid; eligibility, risk and switch math run
unchanged. All publication is captured in memory and sockets are forbidden.
"""
import copy
import smtplib
import socket
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from scripts import run_shadow_board as board

HEADER = ["Symbol", "Name", "Action", "Expected ROI %", "Confidence",
          "Market Value (SAR)", "Sector"]
CANDIDATES = [HEADER, ["SYNCA.US", "Synthetic candidate", "INVEST", "35",
                       "High", "10000", "Technology"]]
HOLDINGS = [HEADER, ["SYNHO.US", "Synthetic holding", "HOLD", "1",
                     "Low", "10000", "Technology"]]
PRIVATE_FAILURE = "synthetic private holdings response token=must-not-appear"


class WorksheetNotFound(Exception):
    """Model a failed source lookup without importing the Sheets client."""


class FakeWorksheet:
    def __init__(self, state, name):
        self.state, self.name = state, name

    def get_all_values(self):
        self.state.reads.append(self.name)
        if self.name == board.TAB_TOP10:
            return copy.deepcopy(CANDIDATES)
        if self.name == board.TAB_HOLDINGS:
            if self.state.read_error is not None:
                raise self.state.read_error
            return copy.deepcopy(self.state.holdings)
        pytest.fail("unexpected source read: " + self.name)

    def update(self, values=None, range_name=None):
        assert range_name == "A1"
        self.state.writes.append((self.name, copy.deepcopy(values)))
        return {"updatedRows": len(values), "updatedCells": sum(map(len, values))}

    def append_row(self, row, value_input_option=None):
        assert value_input_option == "RAW"
        self.state.receipts.append((self.name, copy.deepcopy(row)))
        return {"updates": {"updatedRows": 1}}

    def append_rows(self, rows, value_input_option=None):
        assert value_input_option == "RAW"
        self.state.history.append((self.name, copy.deepcopy(rows)))
        return {"updates": {"updatedRows": len(rows)}}


class FakeSheet:
    def __init__(self, state):
        self.state = state

    def worksheet(self, name):
        self.state.lookups.append(name)
        if name == board.TAB_HOLDINGS and self.state.lookup_error is not None:
            raise self.state.lookup_error
        return FakeWorksheet(self.state, name)

    def add_worksheet(self, **_kwargs):
        pytest.fail("synthetic worksheets already exist")


@pytest.fixture
def live_board(monkeypatch):
    state = SimpleNamespace(
        holdings=copy.deepcopy(HOLDINGS), read_error=None, lookup_error=None,
        reads=[], lookups=[], writes=[], receipts=[], history=[],
        provider_calls=[], authority_calls=[], network_calls=[],
    )
    monkeypatch.setattr(board, "_open_sheet", lambda _sheet_id: FakeSheet(state))
    monkeypatch.setattr(board, "_now_riyadh", lambda: "2026-10-17 12:00:00")

    def fundamentals(symbols):
        state.provider_calls.append(("fundamentals", tuple(symbols)))
        return {"SYNCA.US": {"market_cap": 1_000_000, "interest_debt": 1,
                             "sector": "Technology", "industry": "Software"}}, []

    def monthly(symbol):
        state.provider_calls.append(("monthly", symbol))
        return [(date(2024 + i // 12, 1 + i % 12, 1), 100 + i * 2)
                for i in range(34)]

    def authority(force=True):
        assert force is True
        state.authority_calls.append("authority")
        return {"SYNCA.US": {"status": "PASS", "as_of": datetime.now(timezone.utc).date(),
                              "source": "SYNTHETIC"}}

    def unexpected_network(*_args, **_kwargs):
        state.network_calls.append("forbidden")
        pytest.fail("Shadow Board regression attempted network or SMTP I/O")

    monkeypatch.setattr(board, "fetch_board_fundamentals", fundamentals)
    monkeypatch.setattr(board, "fetch_monthly_closes", monthly)
    monkeypatch.setattr(board.sa, "get_authority_index", authority)
    monkeypatch.setattr(board.sa, "get_monitor_map", dict)
    monkeypatch.setattr(board.sa, "get_meta", lambda: {"rows": 1, "as_of": "2026-10-17"})
    monkeypatch.setattr(board.sa, "last_error", lambda: None)
    monkeypatch.setattr(socket, "create_connection", unexpected_network)
    monkeypatch.setattr(socket.socket, "connect", unexpected_network)
    monkeypatch.setattr(socket.socket, "connect_ex", unexpected_network)
    monkeypatch.setattr(smtplib, "SMTP", unexpected_network)
    monkeypatch.setattr(smtplib, "SMTP_SSL", unexpected_network)
    for key, value in {
        "TFB_BOARD_FREEZE_NONTRADING": "off", "TFB_BOARD_ENGINE_ROI": "off",
        "TFB_SB_REGIME_HISTORY": "1", "TFB_COMPLIANCE_GATE_ENABLED": "1",
        "TFB_COMPLIANCE_SCREEN_RETIRED": "0", "TFB_SHADOW_EQUITY_SAR": "130000",
        "TFB_SB_ATOMIC_WRITE": "1", "TFB_PA_PROTECT_SUKUK": "1",
    }.items():
        monkeypatch.setenv(key, value)
    yield state
    assert state.network_calls == []


def published_body(state):
    assert len(state.writes) == 1
    tab, body = state.writes[0]
    assert tab == board.TAB_OUT
    return body


def test_readable_holdings_publish_the_real_switch_scan_and_ok_receipt(live_board, capsys):
    assert board.main([]) == 0
    body = published_body(live_board)
    assert body[3][0] == "switch scan: SWITCH_CANDIDATES"
    assert body[3][2] == "pairs=1"
    header = body.index(board.OUT_HEADER)
    assert body[header + 1][14] == "TRADE"
    assert body[header + 1][-1] == "YES"
    assert live_board.receipts[-1][1][4] == "OK"
    assert len(live_board.history) == 1
    assert live_board.reads == [board.TAB_TOP10, board.TAB_HOLDINGS]
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("empty", ([], [HEADER]))
def test_verified_empty_holdings_keep_no_action_without_changing_candidate_book(live_board, empty):
    assert board.main([]) == 0
    readable = published_body(live_board)
    live_board.holdings = copy.deepcopy(empty)
    live_board.writes.clear()
    assert board.main([]) == 0
    confirmed_empty = published_body(live_board)
    assert confirmed_empty[3][:3] == ["switch scan: NO_ACTION", "[]", "pairs=0"]
    # Holdings affect only the switch scan: board candidate math and risk are
    # byte-identical for the same independent candidate/provider inputs.
    assert [i for i, (a, b) in enumerate(zip(readable, confirmed_empty)) if a != b] == [3]
    assert live_board.receipts[-1][1][4] == "OK"
    assert len(live_board.history) == 2


@pytest.mark.parametrize("error_class", (OSError, PermissionError, WorksheetNotFound))
@pytest.mark.parametrize("dry_run", (False, True))
def test_holdings_read_error_stops_before_providers_or_any_publication(
    live_board, capsys, error_class, dry_run,
):
    live_board.read_error = error_class(PRIVATE_FAILURE)
    assert board.main(["--dry-run"] if dry_run else []) == 2
    assert live_board.reads == [board.TAB_TOP10, board.TAB_HOLDINGS]
    assert live_board.lookups.count(board.TAB_HOLDINGS) == 1
    assert live_board.writes == live_board.history == live_board.receipts == []
    assert live_board.provider_calls == live_board.authority_calls == []
    output = capsys.readouterr()
    assert "Portfolio_Decision read failed (" + error_class.__name__ + ")" in output.err
    assert "no board or history published" in output.err
    assert PRIVATE_FAILURE not in output.out + output.err
    assert "switch=" not in output.out + output.err
    assert "NO_ACTION" not in output.out + output.err
    assert "SYNHO.US" not in output.out + output.err


def test_holdings_lookup_error_is_not_treated_as_an_empty_book(live_board, capsys):
    live_board.lookup_error = WorksheetNotFound(PRIVATE_FAILURE)
    assert board.main([]) == 2
    assert live_board.reads == [board.TAB_TOP10]
    assert live_board.lookups.count(board.TAB_HOLDINGS) == 1
    assert live_board.writes == live_board.history == live_board.receipts == []
    assert live_board.provider_calls == live_board.authority_calls == []
    output = capsys.readouterr()
    assert "WorksheetNotFound" in output.err
    assert PRIVATE_FAILURE not in output.out + output.err


def test_readable_dry_run_keeps_the_real_scan_without_publication(live_board, capsys):
    assert board.main(["--dry-run"]) == 0
    assert live_board.reads == [board.TAB_TOP10, board.TAB_HOLDINGS]
    assert live_board.provider_calls
    assert live_board.writes == live_board.history == live_board.receipts == []
    output = capsys.readouterr()
    assert "switch=SWITCH_CANDIDATES" in output.out
    assert output.err == ""


@pytest.mark.parametrize("dry_run", (False, True))
def test_enforced_freeze_still_precedes_holdings_read(live_board, monkeypatch, dry_run):
    live_board.read_error = OSError(PRIVATE_FAILURE)
    monkeypatch.setenv("TFB_BOARD_FREEZE_NONTRADING", "enforce")
    monkeypatch.setenv("TFB_BOARD_FREEZE_TODAY", "2026-10-18")
    assert board.main(["--dry-run"] if dry_run else []) == 0
    assert live_board.reads == live_board.writes == live_board.history == []
    assert live_board.provider_calls == live_board.authority_calls == []
    if dry_run:
        assert live_board.receipts == []
    else:
        assert len(live_board.receipts) == 1
        assert live_board.receipts[0][1][4] == "FROZEN"
