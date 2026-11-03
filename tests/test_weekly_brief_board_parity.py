#!/usr/bin/env python3
"""tests/test_weekly_brief_board_parity.py - run_weekly_brief v1.1.0
[BOARD-PARITY] (TFB_WEEKLY_BOARD_PARITY, default off = v1.0.3 byte for byte).

WHY (2026-11-03): on Sunday 1 Nov the weekly memo (run 41300907168, 11:13Z)
said NO_ACTION and logged "shariah_authority ... sheet_id_missing", while the
Shadow_Board run 34 minutes later (41303426265) said SWITCH_CANDIDATES with
eligible=8 on the same 10 candidates. Two causes: the memo reads the board by
v1.0.3 positions (r[16] is 'ROI Src' since run_shadow_board v1.4.0, so every
row renders BLOCKED), and weekly_brief.yml runs the imported board without
the shadow_board.yml lane (retired screen, engine ROI enforce, settled Top_10
read, authority sheet). With the gate on the memo shows the board the tab
shows.

REAL modules (run_weekly_brief, the run_shadow_board it imports,
core.compliance_gate, core.shariah_authority, portfolio_actions,
risk_limits). Stubbed seams only: the gspread workbook (a fake book), the
authority fetch (core.shariah_authority._fetch_values, which still needs
DEFAULT_SPREADSHEET_ID like the real one), board fundamentals, the regime
block, the clock, time.sleep and send_mail. No network.

P1  off (unset / blank / off / 0 / false / no / enforce / observe / onn):
    stdout, stderr, mail (subject, text, HTML), every sheet read and write,
    the _Run_Log row and os.environ equal the untouched v1.0.3, for the
    default run, --no-mail, --dry-run and --sheet-id, on six book / env
    variants (default, no _Brief_State, empty Top_10, lane knobs preset,
    authority readable, no TARGET_SHEET_ID); only the brief's own version
    tokens are normalized
P1b the on-spellings; P1c junk stays off
P2  on, end to end on one fake book: the REAL shadow_board.main (dry run,
    shadow_board.yml job env) and the memo agree on eligible count, blocked
    counts, switch verdict and every board row (after the documented seam);
    authority read from this sheet; the memo env is weekly_brief.yml's own
P2b the same with four hard blocks (two BROKER_UNTRADABLE, two floor-locked),
    with and without TFB_SB_COST_LEGACY=1: every hard block reads BLOCKED
    with ROI / RT / Net Edge suppressed, also when the board scored it
P3  off on the same book is the 1 Nov defect: NO_ACTION against the board's
    SWITCH_CANDIDATES, every row BLOCKED, authority sheet_id_missing
P4  on, columns: the memo header ends with Gen2 Eligible (not ROI Src),
    TRADE rows keep ROI / RT / Net Edge, _Brief_State saves gen2 YES / NO
P5  the seam table: TRADE + NO -> BLOCKED - <status>, economics suppressed;
    BLOCKED + NO keeps its verdict, economics suppressed; a hard block by
    its own evidence (status not invest-ok, Floor OK not YES) is
    terminalized whatever its verdict; compliant EDGE_BELOW_COST / NO_ROI /
    NO_COST_MODEL, TRADE + YES and GATE_DISABLED untouched; short rows and
    non-lists untouched; v1.0.3's _hard_gate_rows unchanged on 17 columns
P6  _apply_board_lane_env: unset / blank knobs take the board value, preset
    knobs are kept; DEFAULT_SPREADSHEET_ID from --sheet-id, TARGET_SHEET_ID,
    TRACK_SHEET_ID, or kept; authority_sheet names TFB_SHARIAH_SHEET_ID when
    that takes precedence; the evidence never carries a sheet id
P6b main passes --sheet-id through (no TARGET_SHEET_ID: authority still read)
P7  on, settled read: a rendering Top_10 is re-read (two 30 s sleeps) and
    the memo carries the settled candidates; off reads once
P8  on, engine ROI: blank ROI filled from Global_Markets / Market_Leaders;
    off never reads those tabs; a preset observe knob is kept (ROI blank)
P9  on, a board header without 'Gen2 Eligible' raises before any mail, state
    or _Run_Log write; off on the same header still mails
P10 on, evidence: one [WEEKLY-BRIEF-PARITY v...] line, the BOARD LANE memo
    line (text and HTML) and _Run_Log details.board_parity; off has none
P11 drift: every literal knob in shadow_board.yml's job env is in
    BOARD_LANE_ENV (same value) or set identically by weekly_brief.yml, and
    every vars knob there is mapped here too or is DEFAULT_SPREADSHEET_ID;
    weekly_brief.yml presets no lane knob; no TFB_ knob sits outside either
    job env; a TFB_BOARD_FREEZE_* knob in shadow_board.yml fails (code, not
    env, must follow it)
P12 legacy cost on: _Brief_State keeps the memo's verdict (BLOCKED), never
    TRADE for a name that is not Gen2-eligible
G1  golden negative: the untouched v1.0.3 with the gate set still mails
    NO_ACTION against the board's SWITCH_CANDIDATES (P2 fails on it, on
    exactly that assertion)
V1  version floor, verify_deployment pin, weekly_brief.yml maps the gate
    once with default 'off', ci.yml's blocking heavy job runs this file after
    fetching the base, WHY order, ASCII file
V1b against the base: ASCII added lines, the exact removed lines, exactly
    six functions added, none removed

Base copy: TFB_TEST_WEEKLY_BASE=<path>, else `git show 3e326e26:scripts/
run_weekly_brief.py`; without either P1, G1 and V1b SKIP visibly. Golden
negative of the whole file: TFB_TEST_WEEKLY_DELIVERED=<base copy>.

Run: python3 tests/test_weekly_brief_board_parity.py   (prints PASS k/k)
  or python3 -m pytest -q tests/test_weekly_brief_board_parity.py
"""
from __future__ import annotations

import ast
import contextlib
import copy
import io
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import types
from datetime import date, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core import shariah_authority as sa  # noqa: E402

BASE_SHA = "3e326e26"   # main 2026-11-03; run_weekly_brief v1.0.3 there
GATE = "TFB_WEEKLY_BOARD_PARITY"
SHEET = "SHEET-TEST-0001"
NOW = "2026-11-01 07:10"
DASH = "\u2014"
_MODS = {}

# every env name either lane or this harness touches; cleared around each run
_ENV_KEYS = (GATE, "TFB_COMPLIANCE_SCREEN_RETIRED", "TFB_BOARD_ENGINE_ROI",
             "TFB_SHADOW_T10_READ_RETRIES", "TFB_SHADOW_T10_READ_WAIT_S",
             "DEFAULT_SPREADSHEET_ID", "TARGET_SHEET_ID", "TRACK_SHEET_ID",
             "TFB_SHARIAH_SHEET_ID", "TFB_COMPLIANCE_GATE_ENABLED",
             "TFB_SHADOW_EQUITY_SAR", "TFB_SB_COST_LEGACY",
             "TFB_BOARD_FREEZE_NONTRADING", "TFB_BOARD_FREEZE_HOLIDAYS",
             "TFB_BOARD_FREEZE_TODAY", "TFB_MAIL_FROM", "TFB_MAIL_PASSWORD",
             "TFB_MAIL_TO", "TFB_SB_ATOMIC_WRITE", "TFB_REGIME_HISTORY")


# ------------------------------------------------------------- modules -----
def _load(name, path):
    """Exec a run_weekly_brief copy as if it sat in scripts/ (its _ROOT and
    its imported run_shadow_board come from this checkout)."""
    src = Path(path).read_text(encoding="utf-8")
    mod = types.ModuleType(name)
    mod.__file__ = str(ROOT / "scripts" / "run_weekly_brief.py")
    mod.__tfb_src__ = str(path)
    sys.modules[name] = mod
    exec(compile(src, str(path), "exec"), mod.__dict__)
    return mod


def delivered():
    if "delivered" not in _MODS:
        path = os.getenv("TFB_TEST_WEEKLY_DELIVERED", "").strip() or str(
            ROOT / "scripts" / "run_weekly_brief.py")
        _MODS["delivered"] = _load("run_weekly_brief_delivered", path)
    return _MODS["delivered"]


def _base_or_none():
    if "base" not in _MODS:
        path = os.getenv("TFB_TEST_WEEKLY_BASE", "").strip()
        if not path:
            try:
                blob = subprocess.run(["git", "-C", str(ROOT), "show",
                                       BASE_SHA + ":scripts/run_weekly_brief.py"],
                                      capture_output=True, timeout=60)
            except Exception:
                blob = None
            if blob is not None and blob.returncode == 0 and b'SCRIPT_VERSION = "1.0.3"' in blob.stdout:
                tmp = Path(tempfile.mkdtemp()) / "run_weekly_brief_v103_base.py"
                tmp.write_bytes(blob.stdout)
                path = str(tmp)
        _MODS["base"] = _load("run_weekly_brief_v103_base", path) if path else None
    return _MODS["base"]


def base():
    if _base_or_none() is None:
        pytest.skip("base copy unavailable (set TFB_TEST_WEEKLY_BASE)")
    return _MODS["base"]


# ------------------------------------------------------------ fixtures -----
TOP10_HEADER = ["Rank", "Symbol", "Company Name", "Action", "Sector",
                "Expected ROI 12M", "Confidence", "Ticket SAR"]
TOP10 = [["TOP 10 INVESTMENTS"],
         ["Last run 2026-11-01 06:40 | status: done"],
         TOP10_HEADER,
         ["1", "AAPL.US", "Apple", "BUY", "Technology", "", "High", "20000"],
         ["2", "MSFT.US", "Microsoft", "BUY", "Technology", "30%", "High", "20000"],
         ["3", "2222.SR", "Saudi Aramco", "BUY", "Energy", "", "High", "20000"],
         ["4", "1120.SR", "Al Rajhi Bank", "BUY", "Financials", "2%", "Medium", "20000"]]
TOP10_RENDERING = [["TOP 10 INVESTMENTS"],
                   ["Last run 2026-11-01 06:40 | status: running..."]]
HOLDINGS = [["Symbol", "Company Name", "Action", "Expected ROI 12M", "Confidence"],
            ["7010.SR", "STC", "HOLD", "1%", "Medium"]]
ENGINE = {"Global_Markets": [["Symbol", "Name", "Expected ROI 12M"],
                             ["AAPL.US", "Apple", "35%"],
                             ["MSFT.US", "Microsoft", "31%"]],
          "Market_Leaders": [["Ticker", "Name", "Expected ROI 12M"],
                             ["2222.SR", "Saudi Aramco", "18%"]]}
FND = {"AAPL.US": {"market_cap": 3.4e12, "interest_debt": 9.5e10,
                   "sector": "Technology", "industry": "Consumer Electronics"},
       "MSFT.US": {"market_cap": 3.0e12, "interest_debt": 1.5e12,
                   "sector": "Technology", "industry": "Software"},
       "2222.SR": {"market_cap": 7.0e12, "interest_debt": 3.0e11,
                   "sector": "Energy", "industry": "Oil & Gas"},
       "1120.SR": {"market_cap": 3.0e11, "interest_debt": 1.0e10,
                   "sector": "Financials", "industry": "Banks"}}
# P2b: hard blocks next to the 4 candidates above - 1015.KL / 1016.KL are
# BROKER_UNTRADABLE, 0700.HK / 0005.HK are floor-locked at 130K SAR equity.
# With TFB_SB_COST_LEGACY=1 the board scores them (TRADE / NO_ROI /
# EDGE_BELOW_COST with Gen2 NO); without it they arrive BLOCKED.
TOP10_BLOCKED = TOP10 + [
    ["5", "1015.KL", "AMMB Holdings", "BUY", "Financials", "40%", "High", "20000"],
    ["6", "0700.HK", "Tencent", "BUY", "Communication", "25%", "High", "20000"],
    ["7", "1016.KL", "Malayan Banking", "BUY", "Financials", "", "High", "20000"],
    ["8", "0005.HK", "HSBC Holdings", "BUY", "Financials", "2%", "Medium", "20000"]]
FND.update({"1015.KL": {"market_cap": 1.0e10, "interest_debt": 1.0e9,
                        "sector": "Financials", "industry": "Banks"},
            "0700.HK": {"market_cap": 4.0e12, "interest_debt": 3.0e11,
                        "sector": "Communication", "industry": "Internet"},
            "1016.KL": {"market_cap": 9.0e9, "interest_debt": 1.0e9,
                        "sector": "Financials", "industry": "Banks"},
            "0005.HK": {"market_cap": 1.5e12, "interest_debt": 2.0e11,
                        "sector": "Financials", "industry": "Banks"}})
HARD = ("1015.KL", "0700.HK", "1016.KL", "0005.HK")
PREV_STATE = {"date": "2026-10-25 07:10", "switch": "NO_ACTION",
              "board": {"AAPL.US": {"status": "MODEL_SCREEN_PASS_NOT_FATWA",
                                    "verdict": "NO_ROI", "gen2": ""},
                        "9999.HK": {"status": "UNKNOWN", "verdict": "TRADE",
                                    "gen2": ""}}}
REGIME = {"sleeves": {"Global": {"state": "RISK_ON"}},
          "suggested_weights": {"Global": 0.7, "Saudi": 0.3, "Cash": 0.0},
          "governance": "advisory stamp - drives nothing until gated"}


def _auth_values():
    as_of = (date.today() - timedelta(days=10)).isoformat()
    return [["Symbol", "Status", "As Of", "Source", "Monitor", "Rule Version", "Doc Hash"],
            ["7010.SR", "PASS", as_of, "AL_RAJHI_OFFICIAL", "PASS", "r7", "h7"],
            ["2222.SR", "PASS", as_of, "AL_RAJHI_OFFICIAL", "PASS", "r7", "h7"],
            ["1120.SR", "PASS", as_of, "AL_RAJHI_OFFICIAL", "PASS", "r7", "h7"]]


class _Cell:
    def __init__(self, value):
        self.value = value


class FakeWS:
    def __init__(self, book, title, values):
        self.book, self.title = book, title
        self.values = [list(r) for r in values]

    def get_all_values(self):
        self.book.calls.append(("get_all_values", self.title))
        if self.title == "Top_10_Investments" and self.book.t10_seq:
            return [list(r) for r in self.book.t10_seq.pop(0)]
        return [list(r) for r in self.values]

    def acell(self, a1):
        self.book.calls.append(("acell", self.title, a1))
        m = re.fullmatch(r"([A-Z])(\d+)", a1)
        r, c = int(m.group(2)) - 1, ord(m.group(1)) - 65
        row = self.values[r] if r < len(self.values) else []
        return _Cell(row[c] if c < len(row) else None)

    def row_values(self, i):
        self.book.calls.append(("row_values", self.title, i))
        return list(self.values[i - 1]) if len(self.values) >= i else []

    def col_values(self, j):
        self.book.calls.append(("col_values", self.title, j))
        return [r[j - 1] if len(r) >= j else "" for r in self.values]

    def update(self, values=None, range_name=None):
        self.book.writes.append(("update", self.title, range_name,
                                 json.dumps(values, ensure_ascii=False)))

    def append_row(self, row, value_input_option=None):
        self.book.writes.append(("append_row", self.title, list(row), value_input_option))


class FakeBook:
    def __init__(self, t10_seq=None, top10=None, no_state=False):
        self.calls, self.writes = [], []
        self.t10_seq = [copy.deepcopy(v) for v in (t10_seq or [])]
        tabs = {"Top_10_Investments": top10 if top10 is not None else TOP10,
                "Portfolio_Decision": HOLDINGS,
                "_Brief_State": [["Weekly Brief State (JSON, machine-managed)"],
                                 [json.dumps(PREV_STATE)]],
                "_Run_Log": [["ts"]]}
        if no_state:
            del tabs["_Brief_State"]
        tabs.update(ENGINE)
        self.tabs = {k: FakeWS(self, k, v) for k, v in tabs.items()}

    def worksheet(self, title):
        self.calls.append(("worksheet", title))
        if title not in self.tabs:
            raise KeyError("WorksheetNotFound: " + title)
        return self.tabs[title]

    def add_worksheet(self, title, rows, cols):
        self.writes.append(("add_worksheet", title, rows, cols))
        ws = FakeWS(self, title, [])
        self.tabs[title] = ws
        return ws


@contextlib.contextmanager
def _env(values):
    saved = dict(os.environ)
    try:
        for k in _ENV_KEYS:
            os.environ.pop(k, None)
        os.environ.update(values)
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)


def _reset_authority():
    sa._CACHE.update({"ts": 0.0, "index": {}, "monitor": {}, "meta": {},
                      "error": "never_loaded", "stale": False, "good_ts": 0.0})


def _patch(mod, monkeypatch, book, out):
    sb = mod.sb
    monkeypatch.setattr(sb, "_open_sheet", lambda cli: book)
    monkeypatch.setattr(sb, "fetch_board_fundamentals",
                        lambda syms, sleep_s=0.4: ({s: dict(FND[s]) for s in syms if s in FND}, []))
    monkeypatch.setattr(sb, "build_regime_block", lambda: copy.deepcopy(REGIME))
    monkeypatch.setattr(sb, "_now_riyadh", lambda: NOW)
    monkeypatch.setattr(mod, "_now_riyadh", lambda: NOW)

    def mail(subject, text, html):
        out["mail"].append((subject, text, html))
        return "SENT:1"
    monkeypatch.setattr(mod, "send_mail", mail)

    def fetch():
        sid = sa._sheet_id()
        out["auth_sheet"].append(sid)
        if not sid:
            raise RuntimeError("sheet_id_missing (TFB_SHARIAH_SHEET_ID / DEFAULT_SPREADSHEET_ID)")
        if sid != SHEET:
            raise RuntimeError("wrong_sheet")
        return _auth_values()
    monkeypatch.setattr(sa, "_fetch_values", fetch)
    monkeypatch.setattr(time, "sleep", lambda s: out["sleeps"].append(s))
    _reset_authority()


def _weekly_env(target=True):
    """The literal knobs of weekly_brief.yml's job env (the lane the memo
    really runs in) + the sheet id its vars mapping supplies."""
    lit, _exprs = _job_env(".github/workflows/weekly_brief.yml", "brief")
    env = dict(lit)
    if target:
        env["TARGET_SHEET_ID"] = SHEET
    return env


def run_brief(mod, monkeypatch, gate=None, argv=(), book=None, extra=None, target=True):
    """-> dict: rc, stdout, stderr, mail, writes, calls, env (lane keys after)."""
    book = book or FakeBook()
    out = {"mail": [], "auth_sheet": [], "sleeps": []}
    env = _weekly_env(target)
    if gate is not None:
        env[GATE] = gate
    env.update(extra or {})
    with _env(env):
        _patch(mod, monkeypatch, book, out)
        buf, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(err):
            out["rc"] = mod.main(list(argv))
        out["env"] = {k: os.environ.get(k) for k in _ENV_KEYS}
    out["stdout"], out["stderr"] = buf.getvalue(), err.getvalue()
    out["writes"], out["calls"] = book.writes, book.calls
    return out


def _shadow_board_env():
    """The literal knobs of shadow_board.yml's job env (+ the two sheet ids)."""
    lit, _vars = _job_env(".github/workflows/shadow_board.yml", "shadow")
    env = dict(lit)
    env.update({"TARGET_SHEET_ID": SHEET, "DEFAULT_SPREADSHEET_ID": SHEET})
    return env


def run_board(mod, monkeypatch, book=None, extra=None):
    """The REAL shadow_board.main --dry-run on the same fake book."""
    book = book or FakeBook()
    out = {"mail": [], "auth_sheet": [], "sleeps": []}
    env = _shadow_board_env()
    env.update(extra or {})
    with _env(env):
        _patch(mod, monkeypatch, book, out)
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = mod.sb.main(["--dry-run"])
    assert rc == 0
    text = buf.getvalue()
    line = next(ln for ln in text.splitlines() if ln.startswith("[SHADOW-BOARD v"))
    rows = [ast.literal_eval(ln.strip()) for ln in text.splitlines() if ln.startswith("   [")]
    m = re.search(r"eligible=(\d+) blocked=(\{.*?\}) switch=(\S+)", line)
    return {"eligible": int(m.group(1)), "blocked": ast.literal_eval(m.group(2)),
            "switch": m.group(3), "rows": rows, "line": line}


def _job_env(rel, job):
    """Parse '<job>: ... env:' KEY: value lines (6-space indent) of a workflow
    without PyYAML. -> ({literal key: value}, {vars key: expression})."""
    text = (ROOT / rel).read_text(encoding="utf-8")
    body = text.split("\n  %s:\n" % job, 1)[1]
    env_block = body.split("\n    env:\n", 1)[1].split("\n    steps:", 1)[0]
    lit, exprs = {}, {}
    for ln in env_block.splitlines():
        m = re.fullmatch(r"      ([A-Z0-9_]+): (.+)", ln)
        if not m:
            continue
        key, val = m.group(1), m.group(2).strip()
        if "${{" in val:
            exprs[key] = val
        else:
            lit[key] = val.strip('"').strip("'")
    return lit, exprs


def _norm(mod, obj):
    """Normalize only the brief's own version tokens (shariah_authority's
    '[shariah_authority v1.1.0]' banner must stay as it is)."""
    s = json.dumps(obj, ensure_ascii=False, default=str)
    v = mod.SCRIPT_VERSION
    for tok in ("[WEEKLY-BRIEF v", "run_weekly_brief v", "[weekly_brief v"):
        s = s.replace(tok + v, tok + "X")
    return s.replace('\\"version\\": \\"%s\\"' % v, '\\"version\\": \\"X\\"')


def _strip_ts(writes):
    return [(w[0], w[1], w[2][1:], w[3]) if w[0] == "append_row" else w for w in writes]


def _memo_board_lines(text):
    lines = text.splitlines()
    i = next(k for k, ln in enumerate(lines) if ln.startswith("BOARD ("))
    out = []
    for ln in lines[i + 2:]:
        if not ln.strip():
            break
        out.append(ln)
    return lines[i], lines[i + 1], out


# ----------------------------------------------------------------- P1 ------
OFF_WORDS = [None, "", "off", "0", "false", "no", "enforce", "observe", "onn"]


P1_VARIANTS = {
    "default": (lambda: FakeBook(), {}, True),
    "no_state": (lambda: FakeBook(no_state=True), {}, True),
    "empty_top10": (lambda: FakeBook(top10=[]), {}, True),
    "lane_preset": (lambda: FakeBook(), {"TFB_COMPLIANCE_SCREEN_RETIRED": "1",
                                         "TFB_BOARD_ENGINE_ROI": "enforce",
                                         "TFB_SHADOW_T10_READ_RETRIES": "6"}, True),
    "authority_ok": (lambda: FakeBook(), {"DEFAULT_SPREADSHEET_ID": SHEET}, True),
    "no_target": (lambda: FakeBook(), {}, False),
}


@pytest.mark.parametrize("variant", sorted(P1_VARIANTS))
@pytest.mark.parametrize("argv", [(), ("--no-mail",), ("--dry-run",),
                                  ("--sheet-id", SHEET, "--no-mail")])
def test_p1_off_equals_the_untouched_v103(monkeypatch, argv, variant):
    b = base()
    d = delivered()
    make_book, extra, target = P1_VARIANTS[variant]
    for gate in OFF_WORDS:
        mine = run_brief(d, monkeypatch, gate, argv, make_book(), extra, target)
        theirs = run_brief(b, monkeypatch, gate, argv, make_book(), extra, target)
        for key in ("rc", "stdout", "stderr", "mail", "calls", "env", "auth_sheet", "sleeps"):
            assert _norm(d, mine[key]) == _norm(b, theirs[key]), (gate, argv, variant, key)
        assert _norm(d, _strip_ts(mine["writes"])) == _norm(b, _strip_ts(theirs["writes"])), \
            (gate, argv, variant)
        assert "[WEEKLY-BRIEF-PARITY" not in mine["stdout"]
    assert d._board_parity_enabled() is False


@pytest.mark.parametrize("gate", ["on", "ON", " on ", "1", "true", "Yes"])
def test_p1b_gate_words(monkeypatch, gate):
    monkeypatch.setenv(GATE, gate)
    assert delivered()._board_parity_enabled() is True


@pytest.mark.parametrize("gate", ["off", "0", "", "enforce", "observe", "onn", "y", "2"])
def test_p1c_junk_is_off(monkeypatch, gate):
    monkeypatch.setenv(GATE, gate)
    assert delivered()._board_parity_enabled() is False


# ----------------------------------------------------------------- P2 ------
def check_parity(mod, monkeypatch, top10=None, extra=None):
    board = run_board(mod, monkeypatch, FakeBook(top10=top10), extra)
    memo = run_brief(mod, monkeypatch, "on", ("--dry-run",), FakeBook(top10=top10), extra)
    assert memo["rc"] == 0
    text = memo["stdout"]
    wl = next(ln for ln in text.splitlines() if ln.startswith("[WEEKLY-BRIEF v"))
    assert ("switch=%s" % board["switch"]) in wl, (wl, board["line"])
    hdr, cols, lines = _memo_board_lines(text)
    m = re.search(r"compliance-eligible (\d+) \| blocked (\{.*\})\)", hdr)
    assert m and int(m.group(1)) == board["eligible"], (hdr, board["line"])
    assert json.loads(m.group(2)) == board["blocked"], (hdr, board["line"])
    seam = mod._hard_gate_rows_by_header(board["rows"])
    idx = mod._board_cols(*mod.MEMO_COLS)
    assert lines == [" | ".join(str(r[i]) for i in idx) for r in seam], (lines, board["rows"])
    pl = [ln for ln in text.splitlines() if ln.startswith("[WEEKLY-BRIEF-PARITY v")]
    assert len(pl) == 1 and ("eligible=%d switch=%s" % (board["eligible"], board["switch"])) in pl[0], pl
    assert memo["auth_sheet"] == [SHEET]
    return board, memo


def test_p2_on_memo_equals_the_real_shadow_board(monkeypatch):
    board, memo = check_parity(delivered(), monkeypatch)
    assert board["switch"] == "SWITCH_CANDIDATES" and board["eligible"] >= 3
    assert "SELL 7010.SR" in memo["stdout"]
    # the board's own Gen2 column decides eligibility; the memo shows it
    assert {r[0] for r in board["rows"] if r[-1] == "YES"} >= {"AAPL.US", "MSFT.US", "2222.SR"}


@pytest.mark.parametrize("legacy", [None, "1"])
def test_p2b_hard_blocks_end_to_end(monkeypatch, legacy):
    """Hard-blocked names next to the candidates, with and without the board's
    TFB_SB_COST_LEGACY kill switch: the memo still equals the board row for
    row (after the seam), and every hard block reads BLOCKED with ROI / RT /
    Net Edge suppressed - also when the board scored it."""
    extra = {"TFB_SB_COST_LEGACY": legacy} if legacy else None
    board, memo = check_parity(delivered(), monkeypatch, TOP10_BLOCKED, extra)
    raw = {r[0]: r for r in board["rows"]}
    assert set(HARD) <= set(raw)
    if legacy:   # the legacy shapes really occur on the board
        assert {raw[s][14] for s in HARD} >= {"TRADE", "NO_ROI", "EDGE_BELOW_COST"}, raw
    else:
        assert all(str(raw[s][14]).startswith("BLOCKED") for s in HARD)
    _hdr, _cols, lines = _memo_board_lines(memo["stdout"])
    by = {ln.split(" | ")[0]: ln.split(" | ") for ln in lines}
    for sym in HARD:
        cells = by[sym]
        assert cells[7].startswith("BLOCKED " + DASH), cells
        assert [cells[2], cells[5], cells[6]] == [DASH] * 3, cells
    # compliant rows are untouched in both modes
    assert by["1120.SR"][7] == "EDGE_BELOW_COST" and by["AAPL.US"][7] == "TRADE"


def test_p12_state_verdict_is_the_memo_verdict(monkeypatch):
    """With the legacy switch the board says TRADE for a hard block; the
    saved _Brief_State must say what the memo says, so next week's CHANGES
    never reports a blocked name turning TRADE."""
    d = delivered()
    memo = run_brief(d, monkeypatch, "on", (), FakeBook(top10=TOP10_BLOCKED),
                     {"TFB_SB_COST_LEGACY": "1"})
    upd = [w for w in memo["writes"] if w[0] == "update" and w[1] == "_Brief_State"]
    state = json.loads(json.loads(upd[-1][3])[1][0])["board"]
    for sym in HARD:
        assert state[sym]["verdict"].startswith("BLOCKED " + DASH) and state[sym]["gen2"] == "NO", state[sym]
    assert all(v["verdict"] != "TRADE" for v in state.values() if v["gen2"] != "YES")
    assert state["AAPL.US"]["verdict"] == "TRADE" and state["1120.SR"]["verdict"] == "EDGE_BELOW_COST"


# ----------------------------------------------------------------- P3 ------
def test_p3_off_is_the_1_nov_defect(monkeypatch):
    d = delivered()
    board = run_board(d, monkeypatch)
    memo = run_brief(d, monkeypatch, None, ("--dry-run",))
    assert board["switch"] == "SWITCH_CANDIDATES"
    assert "switch=NO_ACTION" in memo["stdout"]
    _hdr, _cols, lines = _memo_board_lines(memo["stdout"])
    assert lines and all(("BLOCKED " + DASH) in ln for ln in lines), lines
    assert memo["auth_sheet"] == [None]


# ----------------------------------------------------------------- P4 ------
def test_p4_on_columns_by_name_and_state(monkeypatch):
    d = delivered()
    memo = run_brief(d, monkeypatch, "on")
    _subject, text, html = memo["mail"][0]
    _hdr, cols, lines = _memo_board_lines(text)
    assert cols.endswith("| Debt/MCap % | Gen2 Eligible") and "ROI Src" not in cols, cols
    trade = [ln for ln in lines if "| TRADE |" in ln]
    assert trade and all(ln.endswith("| YES") and DASH not in ln for ln in trade), lines
    aapl = next(ln for ln in lines if ln.startswith("AAPL.US"))
    assert "| 35.0 |" in aapl, aapl
    upd = [w for w in memo["writes"] if w[0] == "update" and w[1] == "_Brief_State"]
    state = json.loads(json.loads(upd[-1][3])[1][0])
    assert {v["gen2"] for v in state["board"].values()} <= {"YES", "NO"}
    assert state["board"]["AAPL.US"]["gen2"] == "YES"
    off = run_brief(d, monkeypatch, None)
    upd = [w for w in off["writes"] if w[0] == "update" and w[1] == "_Brief_State"]
    state_off = json.loads(json.loads(upd[-1][3])[1][0])
    assert {v["gen2"] for v in state_off["board"].values()} == {""}   # ROI Src, the defect


# ----------------------------------------------------------------- P5 ------
def _row(sym, status, verdict, gen2, roi=14.5, rt=0.36, ne=8.5):
    return [sym, "N", "BUY", "Sector", roi, "High", status, "SRC", "BROKER_TRADABLE",
            "TASI", "YES", rt, ne, 1.5, verdict, 12.1, "", gen2]


def test_p5_seam_table():
    d = delivered()
    g = d._hard_gate_rows_by_header
    trade_no = _row("A.SR", "BROKER_UNTRADABLE", "TRADE", "NO")
    blocked = _row("B.SR", "SCREEN_RETIRED", "BLOCKED " + DASH + " SCREEN_RETIRED", "NO", rt="", ne="")
    keep = [_row("C.SR", "SCREEN_RETIRED", "EDGE_BELOW_COST", "NO"),
            _row("D.SR", "SCREEN_RETIRED", "NO_ROI", "NO", roi="", ne=""),
            _row("E.SR", "SCREEN_RETIRED", "NO_COST_MODEL", "NO", rt="", ne=""),
            _row("F.SR", "SCREEN_RETIRED", "TRADE", "YES"),
            _row("G.SR", "SCREEN_RETIRED", "TRADE", "yes"),
            ["SHORT.SR", "x", "TRADE"], "not-a-row", None]
    src = [trade_no, blocked] + keep
    snap = copy.deepcopy(src)
    out = g(src)
    assert src == snap                                    # pure
    assert out[0][14] == "BLOCKED " + DASH + " BROKER_UNTRADABLE"
    assert [out[0][i] for i in (4, 11, 12)] == [DASH] * 3
    assert out[1][14] == blocked[14] and [out[1][i] for i in (4, 11, 12)] == [DASH] * 3
    assert out[2:] == keep
    empty_status = _row("H.SR", "", "TRADE", "NO")
    assert g([empty_status])[0][14] == "BLOCKED " + DASH + " INELIGIBLE"
    # legacy-scored hard blocks: the row's own gate evidence decides
    untr_ebc = _row("I.KL", "BROKER_UNTRADABLE", "EDGE_BELOW_COST", "NO")
    untr_noroi = _row("J.KL", "BROKER_UNTRADABLE", "NO_ROI", "NO", roi="", ne="")
    floor_ebc = _row("K.HK", "SCREEN_RETIRED", "EDGE_BELOW_COST", "NO")
    floor_ebc[10] = "NO"
    model_fail = _row("L.US", "MODEL_SCREEN_FAIL", "NO_COST_MODEL", "NO", rt="", ne="")
    hard = g([untr_ebc, untr_noroi, floor_ebc, model_fail])
    assert [h[14] for h in hard] == ["BLOCKED " + DASH + " BROKER_UNTRADABLE",
                                     "BLOCKED " + DASH + " BROKER_UNTRADABLE",
                                     "BLOCKED " + DASH + " SCREEN_RETIRED",
                                     "BLOCKED " + DASH + " MODEL_SCREEN_FAIL"]
    assert all([h[i] for i in (4, 11, 12)] == [DASH] * 3 for h in hard)
    # a compliant row needs Floor OK = YES; the gate-disabled pass-through has no floor
    floor_blank = _row("M.SR", "SCREEN_RETIRED", "EDGE_BELOW_COST", "NO")
    floor_blank[10] = ""
    assert g([floor_blank])[0][14] == "BLOCKED " + DASH + " SCREEN_RETIRED"
    gate_off = _row("N.SR", "GATE_DISABLED", "EDGE_BELOW_COST", "NO")
    gate_off[10] = ""
    assert g([gate_off]) == [gate_off]
    # TRADE + NO is terminalized even when its evidence cells look compliant
    # (the v1.0.3 1015.KL rule, kept as a backstop for an inconsistent row)
    odd_trade = _row("O.SR", "SCREEN_RETIRED", "TRADE", "NO")
    assert g([odd_trade])[0][14] == "BLOCKED " + DASH + " SCREEN_RETIRED"
    # a verdict the board already wrote as BLOCKED is kept word for word
    worded = _row("P.HK", "SCREEN_RETIRED", "BLOCKED " + DASH + " FLOOR_LOCKED", "NO", rt="", ne="")
    worded[10] = "NO"
    assert g([worded])[0][14] == "BLOCKED " + DASH + " FLOOR_LOCKED"
    # v1.0.3's seam is untouched on the 17-column board it was written for
    r17 = ["1015.KL", "AMMB", "", "Fin", 13.5, "High", "BROKER_UNTRADABLE", "SHADOW",
           "BROKER_UNTRADABLE", "KLSE", "NO", 0.55, 8.23, 1.2, "TRADE", 109.2, "NO"]
    ok17 = ["7010.SR", "STC", "", "Tel", 14.5, "High", "AUTHORITY_PASS", "AL", "BT",
            "TASI", "YES", 0.36, 8.5, 1.5, "TRADE", 12.1, "YES"]
    assert d._hard_gate_rows([r17, ok17])[1] == ok17
    assert d._hard_gate_rows([r17])[0][14] == "BLOCKED " + DASH + " BROKER_UNTRADABLE"
    b = _base_or_none()
    if b is not None:
        for rows in ([r17, ok17], [trade_no, blocked] + keep[:4]):
            assert d._hard_gate_rows(copy.deepcopy(rows)) == b._hard_gate_rows(copy.deepcopy(rows))


# ----------------------------------------------------------------- P6 ------
def test_p6_lane_env():
    d = delivered()
    with _env({}):
        got = d._apply_board_lane_env(None)
        assert got == {"TFB_COMPLIANCE_SCREEN_RETIRED": "1", "TFB_BOARD_ENGINE_ROI": "enforce",
                       "TFB_SHADOW_T10_READ_RETRIES": "6", "DEFAULT_SPREADSHEET_ID": "missing",
                       "authority_sheet": "missing"}
        assert "DEFAULT_SPREADSHEET_ID" not in os.environ
        assert all(os.environ[k] == v for k, v in d.BOARD_LANE_ENV)
    with _env({"TFB_BOARD_ENGINE_ROI": "observe", "TFB_COMPLIANCE_SCREEN_RETIRED": "  ",
               "TARGET_SHEET_ID": "T-ID", "TRACK_SHEET_ID": "K-ID"}):
        got = d._apply_board_lane_env(None)
        assert got["TFB_BOARD_ENGINE_ROI"] == "observe"            # preset kept
        assert os.environ["TFB_COMPLIANCE_SCREEN_RETIRED"] == "1"   # blank filled
        assert os.environ["DEFAULT_SPREADSHEET_ID"] == "T-ID" and got["DEFAULT_SPREADSHEET_ID"] == "this_sheet"
        assert "T-ID" not in json.dumps(got)
    with _env({"TARGET_SHEET_ID": "T-ID"}):
        d._apply_board_lane_env("CLI-ID")
        assert os.environ["DEFAULT_SPREADSHEET_ID"] == "CLI-ID"
    with _env({"TRACK_SHEET_ID": "K-ID"}):
        d._apply_board_lane_env(None)
        assert os.environ["DEFAULT_SPREADSHEET_ID"] == "K-ID"
    with _env({"DEFAULT_SPREADSHEET_ID": "D-ID", "TARGET_SHEET_ID": "T-ID"}):
        got = d._apply_board_lane_env("CLI-ID")
        assert os.environ["DEFAULT_SPREADSHEET_ID"] == "D-ID" and got["DEFAULT_SPREADSHEET_ID"] == "preset"
        assert got["authority_sheet"] == "preset"
    # the authority reader takes TFB_SHARIAH_SHEET_ID first; the evidence says so
    with _env({"TFB_SHARIAH_SHEET_ID": "S-ID", "TARGET_SHEET_ID": "T-ID"}):
        got = d._apply_board_lane_env(None)
        assert got["DEFAULT_SPREADSHEET_ID"] == "this_sheet" and got["authority_sheet"] == "shariah_sheet_id"
        assert "S-ID" not in json.dumps(got) and "T-ID" not in json.dumps(got)


def test_p6b_sheet_id_argument_reaches_the_authority(monkeypatch):
    """main passes --sheet-id to the lane env: with no TARGET_SHEET_ID the
    authority still reads this sheet (gate on) and still fails (gate off)."""
    d = delivered()
    on = run_brief(d, monkeypatch, "on", ("--sheet-id", SHEET, "--dry-run"), target=False)
    assert on["auth_sheet"] == [SHEET] and "authority_sheet=this_sheet" in on["stdout"]
    off = run_brief(d, monkeypatch, None, ("--sheet-id", SHEET, "--dry-run"), target=False)
    assert off["auth_sheet"] == [None]


# ----------------------------------------------------------------- P7 ------
def test_p7_settled_read(monkeypatch):
    d = delivered()
    seq = [TOP10_RENDERING, TOP10_RENDERING, TOP10]
    on = run_brief(d, monkeypatch, "on", ("--dry-run",), book=FakeBook(t10_seq=seq))
    assert on["sleeps"] == [30.0, 30.0]
    assert [c for c in on["calls"] if c == ("get_all_values", "Top_10_Investments")] == \
        [("get_all_values", "Top_10_Investments")] * 3
    assert "cands=4" in on["stdout"] and "t10_read=rendering->ok/2" in on["stdout"]
    off = run_brief(d, monkeypatch, None, ("--dry-run",), book=FakeBook(t10_seq=seq))
    assert off["sleeps"] == [] and "cands=0" in off["stdout"]
    assert [c for c in off["calls"] if c == ("get_all_values", "Top_10_Investments")] == \
        [("get_all_values", "Top_10_Investments")]


# ----------------------------------------------------------------- P8 ------
def test_p8_engine_roi(monkeypatch):
    d = delivered()
    on = run_brief(d, monkeypatch, "on", ("--dry-run",))
    assert "engine_roi=enforce applied=2/2 unresolved=0 roi_errors=0" in on["stdout"]
    assert ("worksheet", "Global_Markets") in on["calls"] and ("worksheet", "Market_Leaders") in on["calls"]
    off = run_brief(d, monkeypatch, None, ("--dry-run",))
    assert not [c for c in off["calls"] if c[0] == "worksheet" and c[1] in ENGINE]
    obs = run_brief(d, monkeypatch, "on", ("--dry-run",), extra={"TFB_BOARD_ENGINE_ROI": "observe"})
    assert "engine_roi=observe applied=2/2" in obs["stdout"]
    aapl = next(ln for ln in obs["stdout"].splitlines() if ln.startswith("AAPL.US |"))
    assert "NO_ROI" in aapl, aapl


# ----------------------------------------------------------------- P9 ------
def test_p9_missing_header_raises_before_any_write(monkeypatch):
    d = delivered()
    hdr = list(d.sb.OUT_HEADER)
    monkeypatch.setattr(d.sb, "OUT_HEADER", [("Gen2 OK" if h == "Gen2 Eligible" else h) for h in hdr])
    book = FakeBook()
    with pytest.raises(ValueError):
        run_brief(d, monkeypatch, "on", book=book)
    assert book.writes == []
    off = run_brief(d, monkeypatch, None)
    assert off["rc"] == 0 and len(off["mail"]) == 1


# ---------------------------------------------------------------- P10 ------
def test_p10_evidence(monkeypatch):
    d = delivered()
    on = run_brief(d, monkeypatch, "on")
    pl = [ln for ln in on["stdout"].splitlines() if "[WEEKLY-BRIEF-PARITY" in ln]
    expect = ("lane=shadow_board screen_retired=1 engine_roi=enforce applied=2/2 "
              "unresolved=0 roi_errors=0 t10_read=ok->ok/0 authority_sheet=this_sheet")
    assert pl == ["[WEEKLY-BRIEF-PARITY v%s] %s eligible=4 switch=SWITCH_CANDIDATES"
                  % (d.SCRIPT_VERSION, expect)], pl
    _s, text, html = on["mail"][0]
    lines = text.splitlines()
    assert lines[-1] == "BOARD LANE | " + expect and lines[-2].startswith("PROVENANCE |")
    assert ("board " + expect.replace(">", "&gt;") + " &middot;\nrun_weekly_brief v") in html
    rl = [w for w in on["writes"] if w[0] == "append_row" and w[1] == "_Run_Log"]
    assert len(rl) == 1
    assert json.loads(rl[0][2][-1]) == {"version": d.SCRIPT_VERSION, "board_parity": expect}
    off = run_brief(d, monkeypatch, None)
    _s, t_off, h_off = off["mail"][0]
    assert "BOARD LANE" not in t_off and "board lane=" not in h_off
    rl = [w for w in off["writes"] if w[0] == "append_row" and w[1] == "_Run_Log"]
    assert json.loads(rl[0][2][-1]) == {"version": d.SCRIPT_VERSION}


# ---------------------------------------------------------------- P11 ------
def test_p11_lane_drift_against_shadow_board_yml():
    d = delivered()
    lit, exprs = _job_env(".github/workflows/shadow_board.yml", "shadow")
    wlit, wexprs = _job_env(".github/workflows/weekly_brief.yml", "brief")
    lane = dict(d.BOARD_LANE_ENV)
    assert lit, "shadow_board.yml job env not parsed"
    for key, val in lit.items():
        assert lane.get(key) == val or wlit.get(key) == val, (key, val)
    for key, val in lane.items():
        assert lit.get(key) == val, (key, val)           # nothing extra or stale
    for key, val in exprs.items():
        assert key == "DEFAULT_SPREADSHEET_ID" or wexprs.get(key) == val, (key, val)
    # the weekly lane must not preset a lane knob: _apply_board_lane_env keeps
    # a preset, so a different value there would split memo and board again
    for key, val in lane.items():
        assert key not in wexprs and wlit.get(key, val) == val, (key, wlit.get(key), wexprs.get(key))
    # every TFB_ / DEFAULT_ knob either workflow sets must sit in its job env
    # (a step-level env would escape both this check and P2's board run)
    for rel, job_lit, job_exprs in ((".github/workflows/shadow_board.yml", lit, exprs),
                                    (".github/workflows/weekly_brief.yml", wlit, wexprs)):
        text = (ROOT / rel).read_text(encoding="utf-8")
        anywhere = set(re.findall(r"^\s+((?:TFB|DEFAULT)_[A-Z0-9_]+):", text, re.M))
        assert anywhere <= set(job_lit) | set(job_exprs), (rel, anywhere - set(job_lit) - set(job_exprs))
    # the board's non-trading freeze is code (shadow_board.main calls
    # freeze_verdict), not env: mirroring it into BOARD_LANE_ENV would do
    # nothing, so a freeze armed in shadow_board.yml must change this script
    for key in ("TFB_BOARD_FREEZE_NONTRADING", "TFB_BOARD_FREEZE_HOLIDAYS", "TFB_BOARD_FREEZE_TODAY"):
        assert key not in lit and key not in exprs, \
            "%s armed in shadow_board.yml: run_weekly_brief must apply freeze_verdict" % key
        assert key not in lane
    try:
        import yaml  # optional: the contract lane has no PyYAML
    except Exception:
        return
    doc = yaml.safe_load((ROOT / ".github/workflows/shadow_board.yml").read_text(encoding="utf-8"))
    env = doc["jobs"]["shadow"]["env"]
    assert {k: str(v) for k, v in env.items() if "${{" not in str(v)} == lit


# ---------------------------------------------------------------- GOLD -----
def test_g1_golden_negative_base_disagrees_with_the_board(monkeypatch):
    b = base()
    board = run_board(b, monkeypatch)
    memo = run_brief(b, monkeypatch, "on", ("--dry-run",))
    assert board["switch"] == "SWITCH_CANDIDATES" and "switch=NO_ACTION" in memo["stdout"]
    assert memo["auth_sheet"] == [None]
    with pytest.raises(AssertionError, match=r"\[WEEKLY-BRIEF v1\.0\.3\] cands=4 switch=NO_ACTION"):
        check_parity(b, monkeypatch)


# ------------------------------------------------------------------- V -----
MAPPING = "${{ vars.TFB_WEEKLY_BOARD_PARITY || 'off' }}"


def _vt(s):
    return tuple(int(x) for x in s.split("."))


def test_v1_version_pin_workflow_ci_why():
    mod = delivered()
    assert _vt(mod.SCRIPT_VERSION) >= (1, 1, 0)
    vd = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    assert '("run_weekly_brief", "%s", "weekly brief")' % mod.SCRIPT_VERSION in vd
    wf = (ROOT / ".github" / "workflows" / "weekly_brief.yml").read_text(encoding="utf-8")
    lines = [ln for ln in wf.splitlines() if GATE + ":" in ln and not ln.lstrip().startswith("#")]
    assert lines == ["      %s: %s" % (GATE, MAPPING)], lines
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    heavy = ci.split("\n  heavy:\n", 1)[1].split("\n  summary:\n", 1)[0]
    assert re.search(r"\n          python -m pytest -q tests/test_weekly_brief_board_parity\.py\n", heavy)
    assert "git fetch --no-tags --depth=1 origin 3e326e26a66393bcf26d725184507f8c1772b290" in heavy
    src = Path(mod.__tfb_src__).read_text(encoding="utf-8")
    i11 = src.index("# v1.1.0 (2026-11-03) - BOARD PARITY")
    assert src.index("# v1.0.3 (2026-08-30) - HARD-GATE TERMINAL") < i11 < src.index("SCRIPT_VERSION = ")
    assert all(ord(c) < 128 for c in Path(__file__).read_text(encoding="utf-8"))


REMOVED_1_1_0 = [
    'SCRIPT_VERSION = "1.0.3"',
    "                diff, equity, risk=None) -> Tuple[str, str, str]:",
    '    """-> (subject, text, html)."""',
    "    rows = _hard_gate_rows(rows)  # v1.0.3: blocks terminate before display",
    "    key_cols = [0, 3, 4, 5, 6, 11, 12, 14, 15, 16]",
    "run_weekly_brief v{SCRIPT_VERSION}</p></body></html>\"\"\"",
    "    cands = sb.rows_to_records(sh.worksheet(sb.TAB_TOP10).get_all_values())",
    '    cur = snapshot(rows, scan["verdict"])',
    "                                      len(fnd), len(cands), diff, equity, risk)",
    '             json.dumps({"version": SCRIPT_VERSION})],',
]
ADDED_DEFS = {"_board_parity_enabled", "_apply_board_lane_env", "_board_cols",
              "snapshot_by_header", "_hard_gate_rows_by_header", "_board_lane_line"}


def test_v1b_against_the_base_ascii_and_functions():
    import difflib
    b = base()
    mod = delivered()
    if mod.SCRIPT_VERSION != "1.1.0":
        pytest.skip("the exact 3e326e26 -> 1.1.0 diff checks apply to 1.1.0 only")
    old = Path(b.__tfb_src__).read_text(encoding="utf-8").splitlines()
    new = Path(mod.__tfb_src__).read_text(encoding="utf-8").splitlines()
    diff = list(difflib.unified_diff(old, new, lineterm="", n=0))
    added = [ln[1:] for ln in diff if ln.startswith("+") and not ln.startswith("+++")]
    removed = [ln[1:] for ln in diff if ln.startswith("-") and not ln.startswith("---")]
    assert added and all(ln.isascii() for ln in added), [ln for ln in added if not ln.isascii()][:3]
    assert sorted(removed) == sorted(REMOVED_1_1_0), removed
    kinds = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)

    def defs(text):
        return {n.name for n in ast.walk(ast.parse(text)) if isinstance(n, kinds)}
    assert defs("\n".join(new)) - defs("\n".join(old)) == ADDED_DEFS
    assert not defs("\n".join(old)) - defs("\n".join(new))


if __name__ == "__main__":
    class _Tally:
        def __init__(self):
            self.passed = self.failed = self.skipped = 0

        def pytest_runtest_logreport(self, report):
            if report.when == "call" and report.passed:
                self.passed += 1
            elif report.skipped:
                self.skipped += 1
            elif report.failed:
                self.failed += 1

    tally = _Tally()
    rc = pytest.main([__file__, "-q", "-p", "no:cacheprovider"], plugins=[tally])
    run_n = tally.passed + tally.failed
    print("PASS %d/%d" % (tally.passed, run_n) + (" (%d skipped)" % tally.skipped if tally.skipped else ""))
    sys.exit(rc)
