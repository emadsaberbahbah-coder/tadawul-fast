"""WP-2c (run_dashboard_sync v6.66.0) [RANK-PAGE] - whole-page Rank (Overall)
at the sync's single Sheets write choke point.

Gate TFB_RANK_OVERALL_PAGE = off | observe | enforce (explicit words only):
  off      -> write_table publishes exactly the v6.65.0 payload (the same
              matrix object reaches the presenter), no log, no _Run_Log row
  observe  -> payload unchanged + one [RANK-PAGE] INFO line + one _Run_Log
              OBSERVE row per market page per write
  enforce  -> Rank (Overall) = int 1..N over eligible rows by Overall Score
              desc / Symbol asc; every other row's Rank cell is "" so the RAW
              in-place update clears a stale within-batch rank
Eligible = numeric score AND no exact low_data_trust token AND no token
starting f7_settle_failed: / settle_unresolved:. Fail-open in both modes.

Evidence: 2026-10-10 export (run 38071484709) distinct Rank 25/25/22/14 on
253/6,168/92/106 ranked rows (TFB_SYNC_SYMBOL_BATCH_SIZE "25").

The PURE CORE _rank_overall_page_core is exercised directly; the wiring runs
the REAL SheetsWriter.write_table over a recording transport (the
FakeSheetsAPI of tests/test_sync_sheet_presentation.py), never a live Sheet.
Synthetic market rows only. Every env var the tests read is pinned by the
offline_modes fixture (monkeypatch restores it).

Run: python -m pytest -q tests/test_sync_rank_overall_page.py   (or python tests/test_sync_rank_overall_page.py)
"""
from __future__ import annotations

import copy
import logging
import os
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.sheet_presentation import present_market_sheet_rows  # noqa: E402
from core.sheets.schema_registry import get_sheet_headers  # noqa: E402
from scripts import run_dashboard_sync as sync  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DAILY = ROOT / ".github" / "workflows" / "daily_sync.yml"
MARKET_PAGES = ("Market_Leaders", "Global_Markets", "Mutual_Funds", "Commodities_FX")
RANK, SCORE, WARN, SYM = "Rank (Overall)", "Overall Score", "Warnings", "Symbol"


# --------------------------------------------------------------------------- #
# Synthetic page matrices (deterministic; shared with scripts/harness_wp2bc.py)
# --------------------------------------------------------------------------- #
def _blank_row(headers, **cells):
    row = {h: "" for h in headers}
    row.update(cells)
    return [row[h] for h in headers]


def batch_matrix(page, batches=3, per_batch=20):
    """batches x per_batch rows ranked WITHIN their batch (1..per_batch);
    scores interleave across batches so only the page top keeps its rank."""
    headers = list(get_sheet_headers(page))
    rows = []
    for b in range(1, batches + 1):
        for i in range(1, per_batch + 1):
            pos = batches * (i - 1) + b
            rows.append(_blank_row(headers, **{SYM: "B%d%02d.US" % (b, i), SCORE: round(100.0 - 0.5 * pos, 2),
                                               RANK: i, WARN: "provider_ok"}))
    return headers, rows


SPECIAL = (
    # symbol,      score,     incoming rank, warnings,                                               expected
    ("TOP.US", 99.5, 3, "provider_ok", 1),
    ("LT1.US", 99.0, 7, "provider_ok; low_data_trust; rank_skipped_low_trust", ""),
    ("LT2.US", 50.0, "", "low_data_trust", ""),
    ("SF1.US", "", "", "f7_settle_failed:non_converged; rank_skipped_settlement_failure", ""),
    ("SU1.US", 77.7, 2, "settle_unresolved:enforce:pass_limit:p3:ov=77.7:rc=buy", ""),
    ("STR.US", "1,234.5", 1, "", 0),          # thousands separator parsed -> the page top
    ("TIE.B", 45.49, 4, "", 3),
    ("TIE.A", 45.49, 4, "", 2),
    ("MRG.US", 40.0, 5, "margin_publish:profit_margin:unresolved", 4),   # not a withheld prefix
    ("BOOL.US", True, 6, "", ""),
    ("NEG.US", -3.0, "", "x_settle_unresolved:y", 5),                     # substring is not a prefix
)


def special_matrix(page):
    headers = list(get_sheet_headers(page))
    rows = [_blank_row(headers, **{SYM: s, SCORE: sc, RANK: rk, WARN: w}) for s, sc, rk, w, _e in SPECIAL]
    return headers, rows


def _special_expected():
    # STR.US (1,234.5) outranks TOP.US (99.5): shift every expected rank by one
    out = []
    for _s, _sc, _rk, _w, e in SPECIAL:
        out.append("" if e == "" else (1 if e == 0 else e + 1))
    return out


def _col(headers, rows, name):
    i = headers.index(name)
    return [r[i] for r in rows]


# --------------------------------------------------------------------------- #
# Recording transport (verbatim pattern: tests/test_sync_sheet_presentation.py)
# --------------------------------------------------------------------------- #
class FakeSheetsAPI:
    """Record the real direct SDK values.update arguments, without credentials."""

    def __init__(self, seeded_grid=None):
        self.requests = []
        self.grid = copy.deepcopy(seeded_grid or [])

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def update(self, **kwargs):
        self.requests.append(copy.deepcopy(kwargs))
        return self

    def _apply_values(self, values):
        for row_i, row in enumerate(values):
            while len(self.grid) <= row_i:
                self.grid.append([])
            while len(self.grid[row_i]) < len(row):
                self.grid[row_i].append("")
            for column_i, value in enumerate(row):
                # Sheets RAW values.update skips nulls; only empty strings
                # remove a prior on-sheet value.
                if value is not None:
                    self.grid[row_i][column_i] = value
        return {"updatedRows": len(values)}

    def execute(self):
        return self._apply_values(self.requests[-1]["body"]["values"])


class _Recorder(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.lines = []

    def emit(self, record):
        self.lines.append((record.levelname, record.getMessage()))


@pytest.fixture(autouse=True)
def offline_modes(monkeypatch):
    modes = {
        "TFB_MARKET_SYMBOL_READBACK": "0", "TFB_SYNC_SYMBOL_BATCH_SIZE": "0",
        "TFB_SYNC_SYMBOL_PERSISTENCE": "1", "TFB_SYNC_KEEP_LAST_GOOD": "1",
        "TFB_SYNC_PERSISTENCE_HARD": "0", "TFB_SYNC_ROW_ID_FIREWALL": "0",
        "TFB_SYNC_NAME_DEDUP_MODE": "off", "TFB_SYNC_OHLC_LAKE": "0",
        "TFB_SYNC_FALSE_GREEN_SCREEN": "0", "TFB_SYNC_STATUS_STAMP": "0",
        "TFB_SYNC_FETCHFAIL_TRUTH": "off", "TFB_SYNC_IDENTITY_TRIPWIRE": "0",
        "TFB_SYNC_COHERENCE_TRIPWIRE": "0", "TFB_SYNC_OHLC_PREWRITE": "0",
        "TFB_SYNC_OHLC_FILLGUARD": "off", "TFB_SYNC_OHLC_READBACK": "0",
        "TFB_SYNC_IDFW_RUNLOG": "0",
        "TFB_SVC_CAPACITY_GUARD": "0",
        # pinned here too (the lean CI job runs many files in one process)
        "TFB_SYNC_OHLC_FILL_GUARD": "0", "TFB_WARNINGS_COMPACT": "off",
        "TFB_SYNC_OHLC_RUNLOG": "1", "GITHUB_RUN_ID": "wp2bc-test",
    }
    for name, value in modes.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("TFB_RANK_OVERALL_PAGE", raising=False)


@pytest.fixture
def log_lines():
    handler = _Recorder()
    sync.logger.addHandler(handler)
    old = sync.logger.level
    sync.logger.setLevel(logging.DEBUG)
    try:
        yield handler.lines
    finally:
        sync.logger.removeHandler(handler)
        sync.logger.setLevel(old)


@pytest.fixture
def runlog_calls(monkeypatch):
    calls = []
    monkeypatch.setattr(sync, "_append_runlog_rank_page",
                        lambda sheets, sid, page, stats: calls.append((sid, page, copy.deepcopy(stats))))
    return calls


def _write(page, headers, rows, seeded=None):
    api = FakeSheetsAPI(seeded if seeded is not None else [headers] + copy.deepcopy(rows))
    writer = sync.SheetsWriter()
    writer._service = api
    assert writer.write_table("SYNTHETIC_BOOK", page, "A1", headers, copy.deepcopy(rows)) == len(rows)
    return api, api.requests[-1]["body"]["values"]


# --------------------------------------------------------------------------- #
# PURE CORE
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("mode", [None, "", "off", "1", "true", "ON", "garbage"])
def test_r1_core_inactive_modes_return_the_same_object(mode):
    headers, rows = special_matrix("Global_Markets")
    out, stats = sync._rank_overall_page_core(headers, rows, mode)
    assert out is rows and stats is None


@pytest.mark.parametrize("page", MARKET_PAGES)
def test_r2_observe_is_values_untouched_with_exact_stats(page):
    headers, rows = special_matrix(page)
    before = copy.deepcopy(rows)
    out, stats = sync._rank_overall_page_core(headers, rows, "observe")
    assert out is rows and rows == before
    # eligible: STR, TOP, TIE.A, TIE.B, MRG, NEG; changed: TOP 3->2, LT1 7->"",
    # SU1 2->"", TIE.A 4->3, BOOL 6->"", NEG ""->6 (STR, TIE.B and MRG keep theirs)
    assert stats == {"mode": "observe", "rows": 11, "eligible": 6, "ranked": 6,
                     "withheld_low_trust": 2, "withheld_settle": 1, "withheld_no_score": 2,
                     "distinct_before": 7, "max_before": 7, "changed": 6,
                     "top": "STR.US", "top_score": 1234.5}


@pytest.mark.parametrize("page", MARKET_PAGES)
def test_r3_enforce_ranks_eligible_rows_and_blanks_the_rest(page):
    headers, rows = special_matrix(page)
    out, stats = sync._rank_overall_page_core(headers, rows, "enforce")
    assert out is not rows
    ranks = _col(headers, out, RANK)
    assert ranks == _special_expected()
    assert all(isinstance(r, int) and not isinstance(r, bool) for r in ranks if r != "")
    assert sorted(r for r in ranks if r != "") == list(range(1, stats["eligible"] + 1))
    # TIE.A before TIE.B (symbol tie-break), the stale LOW-trust rank 7 cleared
    sym = _col(headers, out, SYM)
    assert ranks[sym.index("TIE.A")] < ranks[sym.index("TIE.B")]
    assert ranks[sym.index("LT1.US")] == "" and ranks[sym.index("SU1.US")] == ""
    # every other cell is copied verbatim
    ri = headers.index(RANK)
    for a, b in zip(rows, out):
        assert a[:ri] + a[ri + 1:] == b[:ri] + b[ri + 1:]
    assert stats["mode"] == "enforce" and stats["ranked"] == 6 and stats["changed"] == 6


def test_r4_enforce_is_idempotent():
    headers, rows = special_matrix("Global_Markets")
    once, _s1 = sync._rank_overall_page_core(headers, rows, "enforce")
    twice, s2 = sync._rank_overall_page_core(headers, once, "enforce")
    assert twice == once and s2["changed"] == 0
    assert s2["distinct_before"] == s2["ranked"] == 6


def test_r5_scope_guards_never_touch_non_market_pages_or_odd_shapes(monkeypatch):
    monkeypatch.setenv("TFB_RANK_OVERALL_PAGE", "enforce")
    headers, rows = special_matrix("Global_Markets")
    for page in ("My_Portfolio", "Top_10_Investments", "Insights_Analysis", "Data_Dictionary", "_Run_Log"):
        out, stats = sync._rank_overall_page_apply(page, headers, rows)
        assert out is rows and stats is None
    out, stats = sync._rank_overall_page_apply("Global_Markets", headers, [])
    assert out == [] and stats is None                      # header-repair write
    out, stats = sync._rank_overall_page_core(["Symbol", "Warnings"], [["A", ""]], "enforce")
    assert out == [["A", ""]] and stats["skipped"] == "no_rank_or_score_column"
    ragged = [["A.US"], "not-a-row", None]
    out, stats = sync._rank_overall_page_core(headers, ragged, "enforce")
    assert stats["eligible"] == 0 and stats["withheld_no_score"] == 3 and out[0] == ["A.US"]


@pytest.mark.parametrize("page", MARKET_PAGES)
def test_r6_three_batch_simulation_becomes_one_page_rank(page):
    headers, rows = batch_matrix(page)
    out, stats = sync._rank_overall_page_core(headers, rows, "enforce")
    ranks = _col(headers, out, RANK)
    assert sorted(ranks) == list(range(1, 61))
    assert stats["distinct_before"] == 20 and stats["max_before"] == 20
    assert stats["eligible"] == 60 and stats["changed"] == 59        # only the page top keeps 1
    scores = _col(headers, out, SCORE)
    assert [s for _r, s in sorted(zip(ranks, scores))] == sorted(scores, reverse=True)


@pytest.mark.parametrize("mode", ["observe", "enforce"])
def test_r7_input_matrix_never_mutated(mode):
    for builder in (special_matrix, batch_matrix):
        headers, rows = builder("Commodities_FX")
        before = copy.deepcopy(rows)
        sync._rank_overall_page_core(headers, rows, mode)
        assert rows == before
    headers, rows = special_matrix("Commodities_FX")
    rows[0][headers.index(SCORE)] = float("nan")             # NaN -> ineligible, never raises
    _out, stats = sync._rank_overall_page_core(headers, rows, mode)
    assert stats["withheld_no_score"] == 3


# --------------------------------------------------------------------------- #
# WIRING through the REAL SheetsWriter.write_table
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("page", MARKET_PAGES)
def test_w1_unset_is_byte_identical_and_silent(page, log_lines, runlog_calls):
    headers, rows = batch_matrix(page)
    _api, payload = _write(page, headers, rows)
    assert payload[0] == headers
    assert payload[1:] == present_market_sheet_rows(page, headers, copy.deepcopy(rows))
    assert not [m for _l, m in log_lines if "[RANK-PAGE" in m]
    assert runlog_calls == []


@pytest.mark.parametrize("page", MARKET_PAGES)
def test_w2_observe_payload_identical_one_log_one_runlog(page, monkeypatch, log_lines, runlog_calls):
    headers, rows = batch_matrix(page)
    _api, off_payload = _write(page, headers, rows)
    monkeypatch.setenv("TFB_RANK_OVERALL_PAGE", "observe")
    _api, payload = _write(page, headers, rows)
    assert payload == off_payload
    lines = [m for lvl, m in log_lines if "[RANK-PAGE v%s]" % sync.SCRIPT_VERSION in m]
    assert len(lines) == 1 and "mode=observe" in lines[0] and "distinct_before=20" in lines[0]
    assert "changed=59" in lines[0] and ("page=%s" % page) in lines[0]
    assert len(runlog_calls) == 1
    sid, rl_page, stats = runlog_calls[0]
    assert sid == "SYNTHETIC_BOOK" and rl_page == page
    assert stats["page"] == page and stats["mode"] == "observe" and stats["changed"] == 59


@pytest.mark.parametrize("page", MARKET_PAGES)
def test_w3_enforce_publishes_the_page_rank_and_clears_stale_cells(page, monkeypatch, runlog_calls):
    headers, rows = special_matrix(page)
    monkeypatch.setenv("TFB_RANK_OVERALL_PAGE", "enforce")
    api, payload = _write(page, headers, rows)
    expected, _stats = sync._rank_overall_page_core(headers, copy.deepcopy(rows), "enforce")
    assert _col(headers, payload[1:], RANK) == _col(headers, expected, RANK) == _special_expected()
    grid_ranks = _col(headers, api.grid[1:], RANK)
    assert grid_ranks[1] == "" and grid_ranks[4] == ""        # stale 7 (LOW trust) and 2 (unresolved) cleared
    ranked = [r for r in grid_ranks if r != ""]
    assert len(set(ranked)) == len(ranked) == 6
    # every non-rank cell equals the off-mode publication
    monkeypatch.delenv("TFB_RANK_OVERALL_PAGE")
    _api, off_payload = _write(page, headers, rows)
    ri = headers.index(RANK)
    for a, b in zip(off_payload[1:], payload[1:]):
        assert a[:ri] + a[ri + 1:] == b[:ri] + b[ri + 1:]
    assert len(runlog_calls) == 1 and runlog_calls[0][2]["mode"] == "enforce"


@pytest.mark.parametrize("word", ["1", "true", "ON", "yes", "Enforce!"])
def test_w4_non_words_read_as_off(word, monkeypatch, runlog_calls):
    headers, rows = batch_matrix("Global_Markets")
    _api, off_payload = _write("Global_Markets", headers, rows)
    monkeypatch.setenv("TFB_RANK_OVERALL_PAGE", word)
    assert sync._rank_overall_page_mode() == "off"
    _api, payload = _write("Global_Markets", headers, rows)
    assert payload == off_payload and runlog_calls == []


def test_w5_portfolio_page_untouched(monkeypatch, runlog_calls):
    headers = list(get_sheet_headers("My_Portfolio"))
    assert len(headers) == 122
    rows = [_blank_row(headers, **{SYM: "P%d.US" % i, RANK: 9, SCORE: 50.0 + i}) for i in range(3)]
    monkeypatch.setenv("TFB_RANK_OVERALL_PAGE", "enforce")
    _api, payload = _write("My_Portfolio", headers, rows)
    monkeypatch.delenv("TFB_RANK_OVERALL_PAGE")
    _api, off_payload = _write("My_Portfolio", headers, rows)
    assert payload == off_payload and runlog_calls == []


@pytest.mark.parametrize("mode", ["observe", "enforce"])
def test_w6_fail_open_publishes_within_batch_ranks(mode, monkeypatch, log_lines, runlog_calls):
    headers, rows = batch_matrix("Global_Markets")
    _api, off_payload = _write("Global_Markets", headers, rows)

    def boom(*_a, **_k):
        raise RuntimeError("rank boom")

    monkeypatch.setattr(sync, "_rank_overall_page_core", boom)
    monkeypatch.setenv("TFB_RANK_OVERALL_PAGE", mode)
    _api, payload = _write("Global_Markets", headers, rows)
    assert payload == off_payload
    assert runlog_calls and runlog_calls[-1][2]["error"] == "RuntimeError"
    assert any(lvl == "WARNING" and "failed open" in m for lvl, m in log_lines)


def test_w6b_failed_postcondition_is_fail_open(monkeypatch, runlog_calls):
    headers, rows = batch_matrix("Global_Markets")
    real = sync._rank_overall_page_core

    def bad(hdr, matrix, mode):
        out, stats = real(hdr, matrix, mode)
        out[0][hdr.index(RANK)] = 999
        return out, stats

    monkeypatch.setattr(sync, "_rank_overall_page_core", bad)
    monkeypatch.setenv("TFB_RANK_OVERALL_PAGE", "enforce")
    out, stats = sync._rank_overall_page_apply("Global_Markets", headers, rows)
    assert out is rows and stats["error"] == "ValueError"


def test_w7_header_repair_write_is_silent(monkeypatch, log_lines, runlog_calls):
    headers = list(get_sheet_headers("Global_Markets"))
    monkeypatch.setenv("TFB_RANK_OVERALL_PAGE", "enforce")
    api = FakeSheetsAPI()
    writer = sync.SheetsWriter()
    writer._service = api
    assert writer.write_table("SYNTHETIC_BOOK", "Global_Markets", "A1", headers, []) == 0
    assert runlog_calls == [] and not [m for _l, m in log_lines if "[RANK-PAGE" in m]


# --------------------------------------------------------------------------- #
# _Run_Log appender (FW-3 shape, kill switch, telemetry-only failure)
# --------------------------------------------------------------------------- #
class _AppendRecorder:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def append(self, **kwargs):
        self.calls.append(copy.deepcopy(kwargs))
        return self

    def execute(self):
        if self.fail:
            raise RuntimeError("append boom")
        return {}


class _Sheets:
    def __init__(self, svc):
        self._svc = svc

    def _get_service(self):
        return self._svc


@pytest.mark.parametrize("stats,status,level", [
    ({"mode": "observe", "page": "Global_Markets", "rows": 60, "eligible": 60, "ranked": 60,
      "distinct_before": 20, "max_before": 20, "changed": 59}, "OBSERVE", "INFO"),
    ({"mode": "enforce", "page": "Global_Markets", "rows": 60, "eligible": 60, "ranked": 60,
      "distinct_before": 20, "max_before": 20, "changed": 59, "top": "B101.US"}, "OK", "INFO"),
    ({"mode": "enforce", "page": "Global_Markets", "rows": 60, "error": "RuntimeError"}, "ERROR", "WARNING"),
])
def test_a1_runlog_row_shape_and_status_words(stats, status, level, monkeypatch):
    monkeypatch.setattr(sync.time, "sleep", lambda s: None)
    svc = _AppendRecorder()
    sync._append_runlog_rank_page(_Sheets(svc), "SYNTHETIC_BOOK", "Global_Markets", stats)
    assert len(svc.calls) == 1
    call = svc.calls[0]
    assert call["range"] == "'_Run_Log'!A1" and call["valueInputOption"] == "USER_ENTERED"
    assert call["insertDataOption"] == "INSERT_ROWS"
    row = call["body"]["values"][0]
    assert len(row) == 10 and row[1] == level and row[2] == "run_dashboard_sync"
    assert row[3] == "Global_Markets" and row[4] == status
    assert row[5].startswith("[RANK-PAGE] Global_Markets | rows=")
    assert re.search(r"\| withheld lt/settle/noscore=\d+/\d+/\d+ \| mode=(observe|enforce)", row[5])
    assert '"version": "%s"' % sync.SCRIPT_VERSION in row[9] and '"run_id": "wp2bc-test"' in row[9]


def test_a2_runlog_kill_switch_and_failure_never_counted(monkeypatch):
    monkeypatch.setattr(sync.time, "sleep", lambda s: None)
    stats = {"mode": "observe", "page": "Global_Markets", "rows": 1}
    monkeypatch.setenv("TFB_SYNC_OHLC_RUNLOG", "0")
    svc = _AppendRecorder()
    sync._append_runlog_rank_page(_Sheets(svc), "SYNTHETIC_BOOK", "Global_Markets", stats)
    assert svc.calls == []
    monkeypatch.setenv("TFB_SYNC_OHLC_RUNLOG", "1")
    before = list(sync._RUNLOG_APPEND_FAILS)
    failing = _AppendRecorder(fail=True)
    sync._append_runlog_rank_page(_Sheets(failing), "SYNTHETIC_BOOK", "Global_Markets", stats)  # never raises
    assert len(failing.calls) == 2 and list(sync._RUNLOG_APPEND_FAILS) == before
    sync._append_runlog_rank_page(_Sheets(svc), "SYNTHETIC_BOOK", "Global_Markets", None)
    sync._append_runlog_rank_page(None, "SYNTHETIC_BOOK", "Global_Markets", stats)
    assert svc.calls == []


# --------------------------------------------------------------------------- #
# Workflow mapping + WP-6 interplay + version
# --------------------------------------------------------------------------- #
def test_y1_both_market_jobs_map_the_gate_through_the_repo_variable():
    text = DAILY.read_text(encoding="utf-8")
    declarations = re.findall(r"^\s+TFB_RANK_OVERALL_PAGE:\s*(.+)$", text, re.M)
    assert declarations == ["${{ vars.TFB_RANK_OVERALL_PAGE || 'enforce' }}"] * 2
    lines = text.splitlines()
    jobs = [(i, ln.strip()[:-1]) for i, ln in enumerate(lines) if re.match(r"^  [a-z][a-z0-9-]*:$", ln)]

    def job_of(line_no):
        owner = None
        for i, name in jobs:
            if i <= line_no:
                owner = name
        return owner

    owners = [job_of(i) for i, ln in enumerate(lines) if re.match(r"^\s+TFB_RANK_OVERALL_PAGE:", ln)]
    assert owners == ["sync-dashboard", "recover-missing-market-pages"]


def test_y4_recovery_job_maps_the_gate_too():
    """Integration 2026-10-11: .github/workflows/page_refresh_recovery.yml job recover-page
    also runs run_dashboard_sync.py with TFB_SYNC_SYMBOL_BATCH_SIZE '25'; it must read the
    same Variable, else a recovery write after arming republishes within-batch ranks."""
    path = ROOT / ".github" / "workflows" / "page_refresh_recovery.yml"
    lines = path.read_text(encoding="utf-8").splitlines()
    decl = [(i, ln.split(":", 1)[1].strip()) for i, ln in enumerate(lines)
            if re.match(r"^\s+TFB_RANK_OVERALL_PAGE:", ln)]
    assert [v for _, v in decl] == ["${{ vars.TFB_RANK_OVERALL_PAGE || 'enforce' }}"], decl
    jobs = [(i, ln.strip()[:-1]) for i, ln in enumerate(lines) if re.match(r"^  [a-z][a-z0-9-]*:$", ln)]
    owner = None
    for i, name in jobs:
        if i <= decl[0][0]:
            owner = name
    assert owner == "recover-page", owner


def test_y2_rank_tokens_stay_in_the_warnings_cell_under_wp6():
    for token in ("settle_unresolved:enforce:pass_limit:p3:ov=55.02:rc=hold",
                  "settle_unresolved:observe:oscillation:p2:ov=48.25:rc=sell",
                  "rank_skipped_settle_unresolved", "f7_settle_failed:non_converged",
                  "rank_skipped_settlement_failure", "low_data_trust"):
        assert not sync._wc_token_movable(token), token


def test_y3_version_and_seam_wiring():
    assert tuple(int(x) for x in sync.SCRIPT_VERSION.split(".")) >= (6, 66, 0)
    src = Path(sync.__file__).read_text(encoding="utf-8")
    assert src.count("matrix, _rp_stats = _rank_overall_page_apply(sheet_name, hdr, matrix)") == 1
    assert src.count("_append_runlog_rank_page(self, spreadsheet_id, sheet_name, _rp_stats)") == 1
    assert "DASHBOARD SYNC RUNNER (v%s)" % sync.SCRIPT_VERSION in src
    assert sync._RANK_PAGE_WITHHELD_PREFIXES == ("f7_settle_failed:", "settle_unresolved:")


if __name__ == "__main__":
    sys.exit(pytest.main(["-q", __file__]))
