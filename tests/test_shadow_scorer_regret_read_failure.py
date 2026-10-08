"""History read integrity through the actual shadow scorer runner, offline."""
import copy
import importlib.util
import json
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest


TODAY = date(2026, 10, 8)
ROOT = Path(__file__).resolve().parents[1]
PRIVATE_ERROR = "synthetic-private-account https://example.invalid/?api_token=secret"


@pytest.fixture(scope="module")
def scorer():
    spec = importlib.util.spec_from_file_location(
        "shadow_scorer_read_contract", ROOT / "scripts/run_shadow_scorer.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeWorksheet:
    def __init__(self, sheet, title, rows):
        self.sheet, self.title = sheet, title
        self.rows = copy.deepcopy(rows)
        self.error = None
        self.payload = None

    def get_all_values(self):
        self.sheet.reads.append(self.title)
        if self.error:
            raise self.error
        return copy.deepcopy(self.rows if self.payload is None else self.payload)

    def append_rows(self, rows, **_kwargs):
        self.sheet.writes.append((self.title, "append", copy.deepcopy(rows)))
        self.rows.extend(copy.deepcopy(rows))

    def append_row(self, row, **kwargs):
        self.append_rows([row], **kwargs)

    def clear(self):
        self.sheet.writes.append((self.title, "clear"))
        self.rows = []

    def update(self, values, **_kwargs):
        self.sheet.writes.append((self.title, "update", copy.deepcopy(values)))
        self.rows = copy.deepcopy(values)


class FakeSheet:
    def __init__(self, tabs):
        self.writes, self.reads = [], []
        self.lookup_errors = {}
        self.tabs = {title: FakeWorksheet(self, title, rows)
                     for title, rows in tabs.items()}

    def worksheet(self, title):
        if title in self.lookup_errors:
            raise self.lookup_errors[title]
        return self.tabs[title]

    def add_worksheet(self, title, **_kwargs):
        self.writes.append((title, "create"))
        self.tabs[title] = FakeWorksheet(self, title, [])
        return self.tabs[title]

    def snapshot(self):
        return {title: copy.deepcopy(ws.rows) for title, ws in self.tabs.items()}


def _history(scorer):
    rows = [list(scorer.HISTORY_HEADER)]
    for age in range(32, 0, -1):
        day = TODAY - timedelta(days=age)
        for basket in (scorer.CHAMPION, scorer.CHALLENGER, scorer.BENCHMARK):
            syms = list(scorer.BENCH_WEIGHTS) if basket == scorer.BENCHMARK else ["SYNTHETIC.US"]
            rows.append([str(day), basket, ",".join(syms),
                         json.dumps({symbol: 100.0 for symbol in syms}),
                         0.1, 110.0 if basket == scorer.CHALLENGER else 100.0,
                         0.0, 0.0, "n=1"])
    return rows


def _fork(scorer, **changes):
    row = {"date": "2026-10-06", "fork": scorer.rg.FLOOR_LOCK,
           "symbol": "REFUSED.US", "counterparty": "", "reason": "FLOOR_LOCKED",
           "ref_price": 100.0, "alt_price": None}
    row.update(changes)
    return scorer.rg.to_rows([row])[0]


def _sheet(scorer, *, ledger=None, history=None):
    board = [list(scorer.sb.OUT_HEADER)]
    for symbol, eligible in (("SYNTHETIC.US", True), ("REFUSED.US", False)):
        row = [""] * len(scorer.sb.OUT_HEADER)
        row[0], row[1], row[6], row[14], row[-1] = (
            symbol, "Synthetic", "SCREEN_RETIRED", "TRADE", "YES" if eligible else "NO")
        board.append(row)
    return FakeSheet({
        scorer.TAB_HISTORY: _history(scorer) if history is None else history,
        scorer.TAB_REGRET: [list(scorer.rg.LEDGER_HEADER), _fork(scorer)] if ledger is None else ledger,
        scorer.TAB_REGRET_SUMMARY: [["previous accepted regret summary"]],
        scorer.TAB_GATE: [["previous accepted gate"]],
        scorer.sb.TAB_TOP10: [["Symbol", "Name"], ["SYNTHETIC.US", "Synthetic"]],
        scorer.sb.TAB_OUT: board,
        scorer.TAB_S1_CAL: [["State", "As of (Riyadh)", "Detail"],
                            ["PASS", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "synthetic calibration"]],
        "_Corporate_Actions": [], "Performance_Log": [],
        "_Run_Log": [[str(TODAY), "INFO", "synthetic", "", "OK", "[ROLLBACK-DRILL] passed"]],
    })


@pytest.fixture
def runner(monkeypatch, scorer):
    for key, value in {
        "TFB_S1_BOARD_FRESH_GUARD": "off", "TFB_SHADOW_BENCHMARK_EQW": "0",
        "TFB_S1_BASE_POLICY": "legacy", "TFB_S1_CRITERIA_V2": "off",
        "TFB_S1_CAL_CONSUME": "1", "TFB_SHADOW_PRICE_HONESTY": "1",
        "TFB_COMPLIANCE_GATE_ENABLED": "1",
    }.items():
        monkeypatch.setenv(key, value)
    day_info = {"mode": "wallclock", "key": str(TODAY), "wallclock": str(TODAY),
                "slot": "synthetic", "slot_utc": "15:20", "drift": False}
    monkeypatch.setattr(scorer, "resolve_evidence_day", lambda: (TODAY, day_info))
    observations = {"prices": [], "gates": []}
    real_evaluate = scorer.evaluate_s1

    def fetch(symbols):
        observations["prices"].append(list(symbols))
        return {symbol: 101.0 for symbol in symbols}, {symbol: TODAY for symbol in symbols}, []

    def evaluate(*args, **kwargs):
        gate = real_evaluate(*args, **kwargs)
        observations["gates"].append(copy.deepcopy(gate))
        return gate

    monkeypatch.setattr(scorer, "fetch_spot", fetch)
    monkeypatch.setattr(scorer, "evaluate_s1", evaluate)

    def run(sheet, args=()):
        monkeypatch.setattr(scorer.sb, "_open_sheet", lambda _sheet_id: sheet)
        return scorer.main(list(args))

    return run, observations


@pytest.mark.parametrize("tab", ["Regret_Ledger", "Shadow_History"])
@pytest.mark.parametrize("kind", ["lookup_timeout", "lookup_auth", "read_timeout", "read_auth", "missing"])
@pytest.mark.parametrize("dry_run", [False, True])
def test_failed_read_stops_actual_runner_before_any_evidence_or_promotion(
    scorer, runner, capsys, tab, kind, dry_run
):
    sheet = _sheet(scorer)
    if kind == "missing":
        del sheet.tabs[tab]
    elif kind.startswith("lookup"):
        sheet.lookup_errors[tab] = (TimeoutError if kind.endswith("timeout") else PermissionError)(PRIVATE_ERROR)
    else:
        sheet.tabs[tab].error = (TimeoutError if kind.endswith("timeout") else PermissionError)(PRIVATE_ERROR)
    before = sheet.snapshot()
    run, observations = runner
    assert run(sheet, ["--dry-run"] if dry_run else []) == 2
    assert sheet.snapshot() == before
    assert sheet.writes == []
    assert observations == {"prices": [], "gates": []}
    output = capsys.readouterr().out
    assert "NOT_DECIDABLE" in output and "evidence_unavailable" in output
    assert tab in output and "no writes" in output
    assert PRIVATE_ERROR not in output and "api_token" not in output


@pytest.mark.parametrize("tab", ["Regret_Ledger", "Shadow_History"])
@pytest.mark.parametrize("malformation", ["payload", "header", "zero_table", "false_table", "row_shape", "blank_date", "bad_date", "identity", "numeric"])
def test_malformed_read_never_accepts_a_valid_prefix_or_replaces_summary(
    scorer, runner, capsys, tab, malformation
):
    sheet = _sheet(scorer)
    ws = sheet.tabs[tab]
    if malformation == "payload":
        ws.payload = {"error": PRIVATE_ERROR}
    elif malformation in {"zero_table", "false_table"}:
        ws.rows = [[0 if malformation == "zero_table" else False] * 7]
    elif malformation == "header":
        ws.rows[0][0] = "unexpected private schema"
    else:
        row = copy.deepcopy(ws.rows[-1])
        if malformation == "row_shape":
            row = ["2026-10-07"]
        elif malformation == "blank_date":
            row[0] = ""
        elif malformation == "bad_date":
            row[0] = "2026-02-30"
        elif malformation == "identity":
            row[1] = "UNKNOWN_TYPE"
        elif tab == scorer.TAB_REGRET:
            row[5] = "not-a-number"
        else:
            row[3] = '{"SYNTHETIC.US": NaN}'
        ws.rows.append(row)
    before = sheet.snapshot()
    run, observations = runner
    assert run(sheet) == 2
    assert sheet.snapshot() == before and sheet.writes == []
    assert observations == {"prices": [], "gates": []}
    output = capsys.readouterr().out
    assert "NOT_DECIDABLE" in output and "evidence_unavailable" in output
    assert PRIVATE_ERROR not in output


@pytest.mark.parametrize("empty_kind", ["blank", "header_only", "blank_rows"])
def test_successful_empty_ledger_is_valid_and_opens_one_real_board_fork(scorer, runner, empty_kind):
    ledger = [] if empty_kind == "blank" else [list(scorer.rg.LEDGER_HEADER)]
    if empty_kind == "blank_rows":
        ledger.extend([[], [""] * len(scorer.rg.LEDGER_HEADER)])
    sheet = _sheet(scorer, ledger=ledger)
    run, observations = runner
    assert run(sheet) == 0
    assert len(observations["prices"]) == len(observations["gates"]) == 1
    assert observations["gates"][0]["verdict"] == "PASS"  # known complete evidence still evaluates normally
    appends = [write for write in sheet.writes if write[:2] == (scorer.TAB_REGRET, "append")]
    assert len(appends) == 1
    fork_rows = appends[0][2]
    if empty_kind == "blank":
        assert fork_rows[0] == scorer.rg.LEDGER_HEADER
        fork_rows = fork_rows[1:]
    assert len(fork_rows) == 1
    assert fork_rows[0][1:3] == [scorer.rg.FLOOR_LOCK, "REFUSED.US"]
    assert sheet.tabs[scorer.TAB_REGRET_SUMMARY].rows[0][2] == "open forks 1"


@pytest.mark.parametrize("empty_kind", ["blank", "header_only"])
def test_successful_empty_shadow_history_seeds_pending_evidence(scorer, runner, empty_kind):
    sheet = _sheet(scorer, history=[] if empty_kind == "blank" else [list(scorer.HISTORY_HEADER)])
    run, observations = runner
    assert run(sheet) == 0
    assert observations["gates"][0]["verdict"] == "NOT_DECIDABLE"
    assert len([write for write in sheet.writes if write[:2] == (scorer.TAB_HISTORY, "append")]) == 1
    assert not [write for write in sheet.writes if write[:2] == (scorer.TAB_REGRET, "append")]


def test_retry_after_failed_read_preserves_forks_and_refuses_same_day_duplicate(scorer, runner, capsys):
    sheet = _sheet(scorer)
    sheet.tabs[scorer.TAB_REGRET].error = TimeoutError(PRIVATE_ERROR)
    before = sheet.snapshot()
    run, observations = runner
    assert run(sheet) == 2
    assert sheet.snapshot() == before and sheet.writes == []
    sheet.tabs[scorer.TAB_REGRET].error = None
    assert run(sheet) == 0
    assert sheet.tabs[scorer.TAB_REGRET].rows == before[scorer.TAB_REGRET]
    assert sheet.tabs[scorer.TAB_REGRET_SUMMARY].rows[0][2] == "open forks 1"
    accepted = sheet.snapshot()
    sheet.writes.clear()
    assert run(sheet) == 0
    after = sheet.snapshot()
    assert {k: v for k, v in after.items() if k != "_Run_Log"} == {k: v for k, v in accepted.items() if k != "_Run_Log"}
    assert all(write[0] == "_Run_Log" for write in sheet.writes)
    assert len(observations["prices"]) == len(observations["gates"]) == 1
    assert "refusing duplicate" in capsys.readouterr().out


@pytest.mark.parametrize("tab", ["Regret_Ledger", "Shadow_History"])
@pytest.mark.parametrize("blank_rows", [[], [["", " ", None]]])
def test_successful_blank_table_remains_valid_on_idempotent_retry(scorer, runner, tab, blank_rows):
    sheet = _sheet(scorer)
    sheet.tabs[tab].rows = copy.deepcopy(blank_rows)
    run, observations = runner
    assert run(sheet) == 0
    accepted = sheet.snapshot()
    assert accepted[tab][:len(blank_rows)] == blank_rows
    assert accepted[tab][len(blank_rows)] == (scorer.rg.LEDGER_HEADER if tab == scorer.TAB_REGRET else scorer.HISTORY_HEADER)
    sheet.writes.clear()
    assert run(sheet) == 0
    after = sheet.snapshot()
    assert {k: v for k, v in after.items() if k != "_Run_Log"} == {k: v for k, v in accepted.items() if k != "_Run_Log"}
    assert all(write[0] == "_Run_Log" for write in sheet.writes)
    assert len(observations["prices"]) == len(observations["gates"]) == 1


@pytest.mark.parametrize("daily_return", [0.1, ""])
@pytest.mark.parametrize("broken_index", ["truncated", "blank", "nan", "boolean"])
def test_missing_cumulative_base_cannot_turn_known_failed_gate_into_pass(
    scorer, runner, capsys, daily_return, broken_index
):
    history = _history(scorer)
    last_challenger = next(row for row in reversed(history) if len(row) > 1 and row[1] == scorer.CHALLENGER)
    last_challenger[4], last_challenger[5] = daily_return, 90.0
    run, observations = runner
    control = _sheet(scorer, history=history)
    assert run(control) == 0
    assert observations["gates"][-1]["verdict"] == "FAIL"
    if broken_index == "truncated":
        del last_challenger[5:]
    else:
        last_challenger[5] = {"blank": "", "nan": "NaN", "boolean": False}[broken_index]
    broken = _sheet(scorer, history=history)
    before = broken.snapshot()
    observations["prices"].clear()
    observations["gates"].clear()
    capsys.readouterr()
    assert run(broken) == 2
    assert broken.snapshot() == before and broken.writes == []
    assert observations == {"prices": [], "gates": []}
    output = capsys.readouterr().out
    assert "NOT_DECIDABLE" in output and "evidence_unavailable:Shadow_History" in output
    assert "PASS" not in output


@pytest.mark.parametrize("stored_base,expected_index,expected_gate", [
    (None, 99.9, "NOT_DECIDABLE"), (0.0, 0.0, "FAIL"), (110.0, 111.1, "PASS"),
])
def test_actual_runner_preserves_zero_and_only_seeds_a_genuinely_missing_base(
    scorer, runner, stored_base, expected_index, expected_gate
):
    history = [list(scorer.HISTORY_HEADER)] if stored_base is None else _history(scorer)
    if stored_base is not None:
        last_challenger = next(row for row in reversed(history) if row[1] == scorer.CHALLENGER)
        last_challenger[5] = stored_base
    sheet = _sheet(scorer, history=history)
    run, observations = runner
    assert run(sheet) == 0
    today_challenger = next(row for row in sheet.tabs[scorer.TAB_HISTORY].rows
                            if row[0] == str(TODAY) and row[1] == scorer.CHALLENGER)
    assert today_challenger[5] == pytest.approx(expected_index)
    assert observations["gates"][0]["verdict"] == expected_gate


@pytest.mark.parametrize("scalar", [0, False, "0", "false", "null", "[]"])
def test_non_mapping_prices_json_is_not_empty_quote_evidence(scorer, runner, scalar):
    sheet = _sheet(scorer)
    sheet.tabs[scorer.TAB_HISTORY].rows[-1][3] = scalar
    before = sheet.snapshot()
    run, observations = runner
    assert run(sheet) == 2
    assert sheet.snapshot() == before and sheet.writes == []
    assert observations == {"prices": [], "gates": []}


def test_unpriced_pending_forks_and_unscored_history_keep_their_existing_meaning(scorer, runner):
    ledger = [list(scorer.rg.LEDGER_HEADER), _fork(scorer, ref_price=None)[:5]]
    history = [list(scorer.HISTORY_HEADER),
               ["2026-10-07", scorer.CHALLENGER, "", "{}", "", 100.0, "", "", "DAY_EXCLUDED_INFRA no-challenger"]]
    sheet = _sheet(scorer, ledger=ledger, history=history)
    run, observations = runner
    assert run(sheet) == 0
    assert observations["gates"][0]["verdict"] == "NOT_DECIDABLE"
    assert sheet.tabs[scorer.TAB_REGRET].rows == ledger
    assert sheet.tabs[scorer.TAB_REGRET_SUMMARY].rows[0][3] == "pending 1"
    assert sheet.tabs[scorer.TAB_HISTORY].rows[:2] == history
