#!/usr/bin/env python3
"""tests/test_board_scorecard_p182.py - scripts/score_selection_log.py v1.0.0
[P-182 BOARD SCORECARD].

REAL module. The only stub is an in-memory workbook (worksheet /
add_worksheet / get_all_values / update / append_row / add_rows / add_cols)
injected through _open_book; it records every call. No network.

Env (all optional):
  SSL_FILE=<path>             test a file outside the repo tree
  SSL_BASE=<v0.1.0 file>      dual-tree for the unchanged v0.1.0 ticket path
  TFB_TEST_SELLOG_TSV=<_Selection_Log.tsv>   real-data legs (10-01 export)
  TFB_TEST_EXPORT_DIR=<dir>   Global_Markets.tsv / Mutual_Funds.tsv /
                              Market_Leaders.tsv of the same export
P1 embedded self-test 16/16 (7 v0.1.0 cases + 9 new)
P2 v0.1.0 ticket path: run() CSV + stdout byte-identical to base
   (synthetic; + the real export when set)
P3 synthetic scorecard through main(): episodes, exit reasons, venue bar
   rule (US + Tadawul), +1/+5/+10/+20/exit/now, excess vs SPUS, summary,
   churn, artifacts, step summary
P4 real export (observe): 268 episodes / 4 open / 104 since 09-01; churn
   hard 68 / soft 22 / dropped 1 / board empty 9; 1 orphan exit; exact
   returns for every seat whose first close is inside the export's two
   closes (09-29, 09-30): RDN BHF DVN PINE NVDA GOOGL + MRP/NVDA pending.
   (Older seats are digest-only: the export carries two closes per symbol.)
P5 live path (serial-number 'Logged At', numeric prices) == TSV path; observe
   makes ZERO workbook writes
P6 write: NEW tab created and written as ONE rectangle in one update; a
   shorter second write is padded to the previous extent; one _Run_Log row
   per write (coverage floor set to 0 for these legs - the export fixture
   prices US symbols only); coverage below the default floor (0.5) -> no tab
   write, DEGRADED row, rc 1
P7 off: nothing computed, zero workbook calls, no artifacts
P8 idempotence (detail + rectangle digests)
"""
import contextlib, csv, hashlib, importlib.util, io, json, os, shutil, sys, tempfile
from datetime import date, datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
SSL_FILE = os.environ.get("SSL_FILE") or os.path.join(ROOT, "scripts", "score_selection_log.py")
SSL_BASE = os.environ.get("SSL_BASE", "")
TSV = os.environ.get("TFB_TEST_SELLOG_TSV", "")
EXPORT_DIR = os.environ.get("TFB_TEST_EXPORT_DIR", "")
ENVS = ("TFB_BOARD_SCORECARD", "TFB_BOARD_SCORECARD_MIN_COVERAGE", "GITHUB_STEP_SUMMARY")
ASOF = "2026-10-01T06:00:00Z"


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)
    return m


m = _load(SSL_FILE, "ssl_under_test")
assert m.SCRIPT_VERSION == "1.0.0", m.SCRIPT_VERSION
out = []
digests = []
TMP = tempfile.mkdtemp(prefix="tfb_p182_")


def T(name, cond, detail=""):
    out.append(("PASS " if cond else "FAIL ") + name + (" | " + str(detail)[:260] if detail else ""))
    if not cond:
        raise AssertionError(name + " " + str(detail))


def _h(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()[:16]


@contextlib.contextmanager
def _env(**kv):
    saved = {k: os.environ.get(k) for k in ENVS}
    try:
        for k in ENVS:
            os.environ.pop(k, None)
        for k, v in kv.items():
            if v is not None:
                os.environ[k] = v
        yield
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _call(fn, *a, **kw):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = fn(*a, **kw)
    return rc, buf.getvalue()


# ----------------------------------------------------------------- stub book
class WorksheetNotFound(Exception):
    pass


class StubWS:
    def __init__(self, book, title, values, rows=1000, cols=26):
        self.book, self.title = book, title
        self.values = [list(r) for r in values]
        self.row_count, self.col_count = max(rows, len(self.values)), cols

    def get_all_values(self, **kw):
        self.book.calls.append(("get_all_values", self.title, tuple(sorted(kw.items()))))
        return [list(r) for r in self.values]

    def update(self, values=None, range_name=None, value_input_option=None, **kw):
        self.book.calls.append(("update", self.title, range_name, value_input_option,
                                len(values), len(values[0]) if values else 0))
        assert range_name == "A1" and len({len(r) for r in values}) == 1
        assert len(values) <= self.row_count and len(values[0]) <= self.col_count
        self.values = [list(r) for r in values]

    def add_rows(self, n):
        self.book.calls.append(("add_rows", self.title, n))
        self.row_count += n

    def add_cols(self, n):
        self.book.calls.append(("add_cols", self.title, n))
        self.col_count += n

    def append_row(self, row, value_input_option=None, **kw):
        self.book.calls.append(("append_row", self.title, value_input_option))
        self.values.append(list(row))


class StubBook:
    def __init__(self, tabs):
        self.calls = []
        self.tabs = {t: StubWS(self, t, v) for t, v in tabs.items()}

    def worksheet(self, title):
        self.calls.append(("worksheet", title))
        if title not in self.tabs:
            raise WorksheetNotFound(title)
        return self.tabs[title]

    def add_worksheet(self, title, rows, cols):
        self.calls.append(("add_worksheet", title, rows, cols))
        ws = StubWS(self, title, [], rows, cols)
        self.tabs[title] = ws
        return ws


WRITES = ("update", "append_row", "add_worksheet", "add_rows", "add_cols")
RUNLOG_HDR = ["Timestamp", "Level", "Action", "Page", "Status", "Message", "Endpoint",
              "HTTP Code", "Duration ms", "Details JSON"]


def _writes(book):
    return [c for c in book.calls if c[0] in WRITES]


def _serialize_live(grid):
    """TSV strings -> what the live read returns (UNFORMATTED_VALUE +
    SERIAL_NUMBER): 'Logged At' as a serial number, numbers as numbers."""
    hdr = grid[0]
    i_at = hdr.index("Logged At")
    num_cols = {hdr.index(c) for c in ("Rank", "Price", "Price SAR", "FX→SAR") if c in hdr}
    outg = [list(hdr)]
    for r in grid[1:]:
        r = list(r)
        if i_at < len(r) and r[i_at]:
            dt = datetime.fromisoformat(r[i_at])
            r[i_at] = (dt - datetime(1899, 12, 30)).total_seconds() / 86400.0
        for j in num_cols:
            if j < len(r):
                try:
                    r[j] = float(r[j])
                except (TypeError, ValueError):
                    pass
        outg.append(r)
    return outg


# ------------------------------------------------------------- fixtures
SELLOG_HDR = ["Logged At", "Run Info", "Source Page", "Rank", "Symbol", "Name", "Market", "Sector",
              "Ccy", "FX→SAR", "Price", "Price SAR", "Entry Zone", "Ticket SAR", "Shares",
              "Stop SAR", "TP1 SAR", "TP2 SAR", "ROI %", "Engine ROI %", "Ann ROI %",
              "Gain 12M SAR", "Rel", "DQ", "Conf", "Funds From", "Review By", "Advisor Note",
              "Panel Snapshot", "Outcome", "Review Notes", "Stability", "Days"]


def _slrow(at, run, sym, rank="", px="", fx="", ticket="", stop="", tp1="", tp2="",
           outcome="", exit_=False, output="HELD", ccy="USD", sector="Tech"):
    row = [""] * len(SELLOG_HDR)
    H = {c: i for i, c in enumerate(SELLOG_HDR)}
    info = f"Last run {run} | status: ok | output: {output} | route v4.16.0"
    if exit_:
        info += " [membership exit]"
    vals = {"Logged At": at, "Run Info": info, "Source Page": "Top_10_Investments",
            "Rank": rank, "Symbol": sym, "Name": "" if exit_ else sym.split(".")[0] + " Co",
            "Sector": "" if exit_ else sector, "Ccy": "" if exit_ else ccy,
            "FX→SAR": fx, "Price": px, "Ticket SAR": ticket, "Stop SAR": stop,
            "TP1 SAR": tp1, "TP2 SAR": tp2, "Outcome": outcome,
            "Stability": "" if exit_ else "FAST-TRACK (day 1)"}
    for k, v in vals.items():
        row[H[k]] = v
    return row


SYN_LOG = [SELLOG_HDR,
           _slrow("2026-09-01T10:00:00", "2026-09-01 10:00:00", "AAA.US", "1.0", "100", "3.75",
                  "3750", "337.5", "412.5", "450"),
           _slrow("2026-09-01T10:00:00", "2026-09-01 10:00:00", "BBB.SR", "2.0", "20", "1.0",
                  "1000", "18", "22", "24", ccy="SAR"),
           _slrow("2026-09-01T16:00:00", "2026-09-01 16:00:00", "AAA.US", "1.0", "100.4", "3.75",
                  "3750", "337.5", "412.5", "450"),
           _slrow("2026-09-01T16:00:00", "2026-09-01 16:00:00", "BBB.SR", "2.0", "20.1", "1.0",
                  "1000", "18", "22", "24", ccy="SAR"),
           _slrow("2026-09-01T16:00:00", "2026-09-01 16:00:00", "CCC.US", "3.0", "50", "3.75"),
           _slrow("2026-09-02T09:00:00", "2026-09-02 09:00:00", "AAA.US", "1.0", "101", "3.75"),
           _slrow("2026-09-02T09:00:00", "2026-09-02 09:00:00", "CCC.US", "2.0", "49.5", "3.75"),
           _slrow("2026-09-02T09:00:04", "2026-09-02 09:00:00", "BBB.SR", outcome="EXIT: soft",
                  exit_=True),
           _slrow("2026-09-08T23:30:00", "2026-09-08 23:30:00", "CCC.US", "1.0", "47.5", "3.75"),
           _slrow("2026-09-08T23:30:03", "2026-09-08 23:30:00", "AAA.US", outcome="EXIT: hard",
                  exit_=True),
           _slrow("2026-09-09T09:00:00", "2026-09-09 09:00:00", "EMPTY_BOARD", output="EMPTY"),
           _slrow("2026-09-12T12:00:00", "2026-09-12 12:00:00", "ZZZ.US", outcome="EXIT: soft",
                  exit_=True),
           _slrow("2026-09-30T08:55:00", "2026-09-30 08:55:00", "DDD.US", "1.0", "32.75", "3.75"),
           _slrow("2026-10-01T08:27:00", "2026-10-01 08:27:00", "DDD.US", "1.0", "30.61", "3.75"),
           _slrow("2026-10-01T08:27:00", "2026-10-01 08:27:00", "EEE.US", "2.0", "10", "3.75")]


def _sessions(start, n, weekdays, skip=()):
    s, d = [], start
    while len(s) < n:
        if d.weekday() in weekdays and d not in skip:
            s.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return s


US = _sessions(date(2026, 9, 1), 22, {0, 1, 2, 3, 4}, {date(2026, 9, 7)})   # to 10-01
SR = _sessions(date(2026, 9, 1), 20, {6, 0, 1, 2, 3})                         # Sun-Thu
SYN_PRICES = ([("AAA.US", d, 100.0 + i) for i, d in enumerate(US, 1)]
              + [("CCC.US", d, 50.0 * (1 - 0.01 * i)) for i, d in enumerate(US, 1)]
              + [("BBB.SR", d, 20.0 * (1 + 0.005 * i)) for i, d in enumerate(SR, 1)]
              + [("DDD.US", "2026-09-29", 32.75), ("DDD.US", "2026-09-30", 30.61)]
              + [("SPUS.US", "2026-08-31", 50.0)]
              + [("SPUS.US", d, 50.0 + 0.1 * i) for i, d in enumerate(US, 1)])


def _write_tsv(path, grid):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        csv.writer(fh, delimiter="\t").writerows(grid)


def _write_prices(path, rows):
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["symbol", "date", "close"])
        w.writerows(rows)


def _read_csv(path):
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.reader(fh))


def _detail_by(path):
    rows = _read_csv(path)
    hdr = rows[0]
    return hdr, [dict(zip(hdr, r)) for r in rows[1:]]


syn_tsv = os.path.join(TMP, "syn_sellog.tsv")
syn_px = os.path.join(TMP, "syn_prices.csv")
_write_tsv(syn_tsv, SYN_LOG)
_write_prices(syn_px, SYN_PRICES)


def _export_closes():
    """US rows refreshed on 10-01 carry two closes each: Previous Close =
    09-29, Current Price = 09-30 (US session closed, feed epoch 09-30)."""
    rows = []
    for f in ("Global_Markets.tsv", "Mutual_Funds.tsv", "Market_Leaders.tsv"):
        p = os.path.join(EXPORT_DIR, f)
        if not os.path.exists(p):
            continue
        with open(p, newline="", encoding="utf-8") as fh:
            g = list(csv.reader(fh, delimiter="\t"))
        H = {c: i for i, c in enumerate(g[0])}
        for r in g[1:]:
            if len(r) <= H["Last Updated (Riyadh)"] or not r[0].endswith(".US"):
                continue
            if not r[H["Last Updated (Riyadh)"]].startswith("2026-10-01"):
                continue
            try:
                cur, prev = float(r[H["Current Price"]]), float(r[H["Previous Close"]])
            except ValueError:
                continue
            rows += [(r[0], "2026-09-29", prev), (r[0], "2026-09-30", cur)]
    return rows


REAL = bool(TSV and EXPORT_DIR)
if REAL:
    px_rows = _export_closes()
    real_px = os.path.join(TMP, "export_closes.csv")
    _write_prices(real_px, px_rows)

# ------------------------------------------------------------------- P1
rc, so = _call(m.selftest)
T("P1 embedded self-test 16/16", rc == 0 and "SELFTEST 16/16: ALL GREEN" in so, so.strip())

# ------------------------------------------------------------------- P2
if SSL_BASE:
    base = _load(SSL_BASE, "ssl_base")
    rc_b, so_b = _call(base.selftest)
    T("P2 base self-test is the v0.1.0 7/7", "SELFTEST 7/7: ALL GREEN" in so_b, so_b.strip())
    legs = [("synthetic", syn_tsv, syn_px)] + ([("real export", TSV, real_px)] if REAL else [])
    for leg, tsv_path, px_path in legs:
        o_new = os.path.join(TMP, "new_%s.csv" % leg.replace(" ", "_"))
        o_base = os.path.join(TMP, "base_%s.csv" % leg.replace(" ", "_"))
        r1, s1 = _call(m.run, tsv_path, o_new, px_path, False)
        r2, s2 = _call(base.run, tsv_path, o_base, px_path, False)
        T("P2 ticket path %s: CSV + stdout byte-identical to v0.1.0" % leg,
          (r1, s1.replace(o_new, "@"), open(o_new, "rb").read()) ==
          (r2, s2.replace(o_base, "@"), open(o_base, "rb").read()), s1.replace(o_new, "@").strip()[-110:])
        digests.append(_h(open(o_new, encoding="utf-8").read()))
else:
    out.append("SKIP P2 dual-tree (set SSL_BASE=<v0.1.0 score_selection_log.py>)")

# ------------------------------------------------------------------- P3
syn_out = os.path.join(TMP, "syn_out")
step = os.path.join(TMP, "step_summary.md")
with _env(TFB_BOARD_SCORECARD="observe", GITHUB_STEP_SUMMARY=step):
    rc, so = _call(m.main, ["--scorecard", "--tsv", syn_tsv, "--prices-csv", syn_px,
                            "--asof", ASOF, "--out-dir", syn_out])
T("P3 rc 0 + tag line", rc == 0 and "[BOARD-SCORECARD v1.0.0] mode=observe source=tsv episodes=5 "
  "open=2 since=2026-09-01(5) priced=4 prices=csv 4/5 bench=ok orphan_exits=1 wrote=0 artifacts=3" in so,
  so.strip().splitlines()[-1])
hdr, det = _detail_by(os.path.join(syn_out, "board_scorecard_detail.csv"))
T("P3 detail header", hdr == m.DETAIL_HEADER)
by = {(d["Symbol"], d["Seat Date"]): d for d in det}
T("P3 newest first, board order inside a snapshot",
  [(d["Seat Date"], d["Symbol"]) for d in det] ==
  [("2026-10-01", "EEE.US"), ("2026-09-30", "DDD.US"), ("2026-09-01", "CCC.US"),
   ("2026-09-01", "AAA.US"), ("2026-09-01", "BBB.SR")], [(d["Seat Date"], d["Symbol"]) for d in det])
a = by[("AAA.US", "2026-09-01")]
T("P3 AAA: +1 1 / +5 5 / +10 10 / +20 20 / exit 5 / now 21 / vs SPUS +5 4.0 / now 16.8, EXIT: hard",
  [a[k] for k in ("+1 %", "+5 %", "+10 %", "+20 %", "To Exit %", "To Now %", "vs SPUS +5 pp",
                  "vs SPUS Now pp", "Exit Kind", "Status", "Seat Price")] ==
  ["1.0", "5.0", "10.0", "20.0", "5.0", "21.0", "4.0", "16.8", "EXIT: hard", "CLOSED", "100.0"], a)
b = by[("BBB.SR", "2026-09-01")]
T("P3 BBB.SR: Tadawul bar 1 = seat-day close (10:00 < 15:00); exit next morning -> exit close = bar 1;"
  " soft reason attached", [b[k] for k in ("+1 %", "To Exit %", "Exit Kind", "Exit Date")] ==
  ["0.5", "0.5", "EXIT: soft", "2026-09-02"], b)
c = by[("CCC.US", "2026-09-01")]
T("P3 CCC: seated 16:00 Riyadh = 09:00 NY -> bar 1 same day; board emptied 09-09 -> exit close 09-08",
  [c[k] for k in ("+1 %", "+5 %", "To Exit %", "Exit Kind", "Days on Board")] ==
  ["-1.0", "-5.0", "-5.0", "BOARD EMPTY", "3"], c)
d_ = by[("DDD.US", "2026-09-30")]
T("P3 DDD: +1 -6.53, open, 2 days on board, 24.1 h",
  [d_[k] for k in ("+1 %", "Status", "Days on Board", "Hours on Board", "Exit Kind")] ==
  ["-6.53", "OPEN", "2", "24.1", ""], d_)
e = by[("EEE.US", "2026-10-01")]
T("P3 EEE: no closes -> NO_DATA, no returns", e["Status"] == "NO_DATA" and e["+1 %"] == "", e)
summ = _read_csv(os.path.join(syn_out, "board_scorecard_summary.csv"))
row = [r for r in summ if r[:2] == ["All", "+1"]][0]
T("P3 summary All +1: 5 episodes, 4 with data, hit 50, mean -1.51, median -0.25, vs SPUS -1.71,"
  " best 1, worst -6.53",
  row == ["All", "+1", "5", "4", "50.0", "-1.51", "-0.25", "-1.71", "1.0", "-6.53"], row)
churn = [r for r in summ if r and r[0] == "All" and len(r) == len(m.CHURN_HEADER) and r[1] == "5"]
T("P3 churn All: 5 / closed 3 / open 2 / hard 1 / soft 1 / dropped 0 / empty 1",
  churn and churn[0][:8] == ["All", "5", "3", "2", "1", "1", "0", "1"], churn)
T("P3 step summary + markdown artifact written",
  os.path.exists(step) and open(step, encoding="utf-8").read() ==
  open(os.path.join(syn_out, "board_scorecard.md"), encoding="utf-8").read())
digests.append(_h(det))

# ------------------------------------------------------------------- P4
real = None
if REAL:
    closes = {}
    for s, d, c in px_rows:
        closes.setdefault(s, {})[d] = c
    real_out = os.path.join(TMP, "real_out")
    with _env(TFB_BOARD_SCORECARD="observe"):
        rc, so = _call(m.main, ["--scorecard", "--tsv", TSV, "--prices-csv", real_px,
                                "--asof", ASOF, "--out-dir", real_out])
    tag = so.strip().splitlines()[-1]
    T("P4 real: rc 0, 1,919 rows -> 268 episodes, 4 open, 104 since 09-01, 1 orphan exit",
      rc == 0 and "episodes=268 open=4 since=2026-09-01(104)" in tag and "orphan_exits=1" in tag, tag)
    with open(TSV, newline="", encoding="utf-8") as fh:
        grid = list(csv.reader(fh, delimiter="\t"))
    ev, est = m.board_events(grid)
    eps, pst = m.build_episodes(ev, datetime(2026, 10, 1, 9, 0))
    since = [e for e in eps if e["seat_at"] >= datetime(2026, 9, 1)]
    kinds = {}
    for e_ in since:
        kinds[e_["exit_kind"] or "OPEN"] = kinds.get(e_["exit_kind"] or "OPEN", 0) + 1
    T("P4 real churn since 09-01: hard 68 / soft 22 / board empty 9 / dropped 1 / open 4",
      kinds == {"EXIT: hard": 68, "EXIT: soft": 22, "BOARD EMPTY": 9, "DROPPED": 1, "OPEN": 4}, kinds)
    allk = {}
    for e_ in eps:
        allk[e_["exit_kind"] or "OPEN"] = allk.get(e_["exit_kind"] or "OPEN", 0) + 1
    out.append("INFO real all-time exit kinds %s | snapshots %d (empty %d) | rows %d"
               % (json.dumps(allk, sort_keys=True), pst["snapshots"], pst["empty_snapshots"], est["rows"]))
    hdr, det = _detail_by(os.path.join(real_out, "board_scorecard_detail.csv"))
    by = {}
    for d in det:
        by.setdefault((d["Symbol"], d["Seat Date"], d["Seat Time"]), d)

    def pct(a, b):
        return str(round((a / b - 1) * 100, 2) + 0.0)
    sp = closes["SPUS.US"]
    exp = {
        ("RDN.US", "2026-09-30", "08:55"): {"+1 %": pct(closes["RDN.US"]["2026-09-30"], 32.75),
                                           "Status": "OPEN", "Exit Kind": ""},
        ("BHF.US", "2026-09-30", "16:02"): {"+1 %": pct(closes["BHF.US"]["2026-09-30"], 52.0),
                                           "To Exit %": "", "Status": "CLOSED", "Exit Kind": "EXIT: hard"},
        ("DVN.US", "2026-09-30", "16:02"): {"+1 %": pct(closes["DVN.US"]["2026-09-30"], 46.6),
                                           "To Exit %": "", "Status": "CLOSED", "Exit Kind": "EXIT: hard"},
        ("PINE.US", "2026-09-30", "03:56"): {"+1 %": pct(closes["PINE.US"]["2026-09-30"], 17.32),
                                            "Status": "OPEN"},
        ("NVDA.US", "2026-09-30", "03:56"): {"+1 %": pct(closes["NVDA.US"]["2026-09-30"], 227.21),
                                            "To Exit %": pct(closes["NVDA.US"]["2026-09-30"], 227.21),
                                            "Exit Kind": "EXIT: hard", "Exit Date": "2026-10-01"},
        ("GOOGL.US", "2026-09-29", "07:42"): {"+1 %": pct(closes["GOOGL.US"]["2026-09-29"], 342.75),
                                             "To Exit %": pct(closes["GOOGL.US"]["2026-09-29"], 342.75),
                                             "To Now %": pct(closes["GOOGL.US"]["2026-09-30"], 342.75)},
        ("MRP.US", "2026-10-01", "08:27"): {"Status": "PENDING", "+1 %": ""},
        ("NVDA.US", "2026-10-01", "08:27"): {"Status": "PENDING", "+1 %": ""},
    }
    bad = {k: {f: (by.get(k, {}).get(f), v) for f, v in fs.items() if by.get(k, {}).get(f) != v}
           for k, fs in exp.items()}
    bad = {k: v for k, v in bad.items() if v}
    T("P4 real exact set: RDN -6.53 / BHF -4.04 (exited before its first close) / DVN -1.20 / "
      "PINE +0.17 / NVDA exit +0.51 / GOOGL / MRP + NVDA pending", not bad, bad)
    rdn = by[("RDN.US", "2026-09-30", "08:55")]
    x = round((closes["RDN.US"]["2026-09-30"] / 32.75 - 1) * 100
              - (sp["2026-09-30"] / sp["2026-09-29"] - 1) * 100, 2)
    T("P4 real RDN vs SPUS now = -6.50 pp (SPUS 59.56 -> 59.54)",
      rdn["vs SPUS Now pp"] == str(x + 0.0) and str(x) == "-6.5", (rdn["vs SPUS Now pp"], x))
    out.append("INFO real exact set: " + "; ".join(
        "%s %s %s +1=%s exit=%s now=%s %s" % (k[0], k[1], k[2], by[k]["+1 %"], by[k]["To Exit %"],
                                             by[k]["To Now %"], by[k]["Status"]) for k in exp))
    real = (grid, real_px)
    digests.append(_h(det))
else:
    out.append("SKIP P4 real export (set TFB_TEST_SELLOG_TSV + TFB_TEST_EXPORT_DIR)")

# ------------------------------------------------------------------- P5
live_grid = _serialize_live(real[0] if real else SYN_LOG)
live_px = real[1] if real else syn_px
tsv_for_live = TSV if real else syn_tsv


def _live_run(book, mode, out_dir, px=None, extra=(), cov=None):
    m._open_book = lambda: book
    with _env(TFB_BOARD_SCORECARD=mode, TFB_BOARD_SCORECARD_MIN_COVERAGE=cov):
        return _call(m.main, ["--scorecard", "--live", "--prices-csv", px or live_px,
                              "--asof", ASOF, "--out-dir", out_dir] + list(extra))


book = StubBook({"_Selection_Log": live_grid, "_Run_Log": [RUNLOG_HDR]})
rc, so = _live_run(book, "observe", os.path.join(TMP, "live_obs"))
tsv_dir = os.path.join(TMP, "tsv_obs")
with _env(TFB_BOARD_SCORECARD="observe"):
    _call(m.main, ["--scorecard", "--tsv", tsv_for_live, "--prices-csv", live_px, "--asof", ASOF,
                   "--out-dir", tsv_dir])
live_det = open(os.path.join(TMP, "live_obs", "board_scorecard_detail.csv"), encoding="utf-8").read()
tsv_det = open(os.path.join(tsv_dir, "board_scorecard_detail.csv"), encoding="utf-8").read()
T("P5 live read (serial 'Logged At', numeric cells) == TSV path, detail byte-identical",
  rc == 0 and live_det == tsv_det, (len(live_det), len(tsv_det)))
T("P5 live read asks for UNFORMATTED_VALUE + SERIAL_NUMBER",
  ("get_all_values", "_Selection_Log", (("date_time_render_option", "SERIAL_NUMBER"),
                                        ("value_render_option", "UNFORMATTED_VALUE"))) in book.calls)
T("P5 observe: ZERO workbook writes", _writes(book) == [] and "wrote=0" in so, _writes(book))
digests.append(_h(live_det))

# ------------------------------------------------------------------- P6
book = StubBook({"_Selection_Log": live_grid, "_Run_Log": [RUNLOG_HDR]})
rc, so = _live_run(book, "write", os.path.join(TMP, "live_w1"), cov="0")
w = _writes(book)
tab = book.tabs.get("_Board_Scorecard")
T("P6 write: tab created once, ONE update at A1 (RAW), one _Run_Log row",
  rc == 0 and [c[0] for c in w] == ["add_worksheet", "update", "append_row"]
  and w[1][2:4] == ("A1", "RAW") and w[2][1] == "_Run_Log", w)
rect = tab.values
T("P6 rectangle: uniform width 22, title tag, DETAIL header present",
  len({len(r) for r in rect}) == 1 and len(rect[0]) == len(m.DETAIL_HEADER)
  and rect[0][0].startswith("[BOARD-SCORECARD v1.0.0]") and m.DETAIL_HEADER in rect, len(rect))
rl = book.tabs["_Run_Log"].values[-1]
T("P6 _Run_Log row: OK, tag, details JSON", rl[1:5] == ["INFO", "score_selection_log", "_Board_Scorecard", "OK"]
  and rl[5].startswith("[BOARD-SCORECARD v1.0.0] episodes=") and json.loads(rl[9])["mode"] == "write", rl[:6])
n1 = len(rect)
# second, shorter write: only the log rows up to 09-09 -> fewer episodes
cut = ((datetime(2026, 7, 1) if REAL else datetime(2026, 9, 9)) - datetime(1899, 12, 30)).days
short = [live_grid[0]] + [r for r in live_grid[1:] if isinstance(r[0], float) and r[0] < cut]
book.tabs["_Selection_Log"].values = short
rc, so = _live_run(book, "write", os.path.join(TMP, "live_w2"), cov="0")
rect2 = tab.values
n_real2 = max(i for i, r in enumerate(rect2) if any(str(c) for c in r)) + 1
T("P6 shorter rewrite padded to the previous extent (stale tail cleared)",
  rc == 0 and len(rect2) == n1 and n_real2 < n1 and all(all(c == "" for c in r) for r in rect2[n_real2:]),
  (n1, n_real2, len(rect2)))
# degraded: no closes at all -> coverage 0 < 0.5
empty_px = os.path.join(TMP, "empty_prices.csv")
_write_prices(empty_px, [])
before = len([c for c in book.calls if c[0] == "update"])
rc, so = _live_run(book, "write", os.path.join(TMP, "live_w3"), px=empty_px)
after = len([c for c in book.calls if c[0] == "update"])
rl = book.tabs["_Run_Log"].values[-1]
T("P6 degraded coverage: tab NOT overwritten, DEGRADED _Run_Log row, rc 1",
  rc == 1 and after == before and rl[4] == "DEGRADED" and "NOT overwritten" in rl[5], (rc, rl[4:6]))
digests.append(_h(rect))

# ------------------------------------------------------------------- P7
book = StubBook({"_Selection_Log": live_grid, "_Run_Log": [RUNLOG_HDR]})
off_dir = os.path.join(TMP, "live_off")
rc, so = _live_run(book, "off", off_dir)
T("P7 off: one line, zero workbook calls, no artifacts",
  rc == 0 and "mode=off" in so and book.calls == [] and not os.path.exists(off_dir), so.strip())

# ------------------------------------------------------------------- P8
book = StubBook({"_Selection_Log": live_grid, "_Run_Log": [RUNLOG_HDR]})
_live_run(book, "write", os.path.join(TMP, "live_i1"), cov="0")
r1 = [list(r) for r in book.tabs["_Board_Scorecard"].values]
book = StubBook({"_Selection_Log": live_grid, "_Run_Log": [RUNLOG_HDR]})
_live_run(book, "write", os.path.join(TMP, "live_i2"), cov="0")
r2 = [list(r) for r in book.tabs["_Board_Scorecard"].values]
T("P8 idempotence: same input -> identical rectangle", r1 == r2 and r1 == rect, _h(r1))

shutil.rmtree(TMP, ignore_errors=True)
digest = hashlib.sha256(("\n".join(out) + json.dumps(digests)).encode()).hexdigest()[:16]
print("\n".join(out))
print("RESULT %d PASS / %d FAIL, digest %s" % (sum(1 for l in out if l.startswith("PASS")),
                                              sum(1 for l in out if l.startswith("FAIL")), digest))


def test_all():
    assert not any(l.startswith("FAIL") for l in out)
