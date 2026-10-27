"""tests/test_board_scorecard_p182.py - scripts/score_selection_log.py v1.2.0
[P-182 BOARD SCORECARD] + [RANKING HEALTH] + [CHALLENGER].

REAL module. The only stub is an in-memory workbook (worksheet /
add_worksheet / get_all_values / update / append_row / add_rows / add_cols)
injected through _open_book; it records every call. No network.

Env (all optional):
  SSL_FILE=<path>             test a file outside the repo tree
  SSL_BASE=<v0.1.0 file>      dual-tree for the unchanged v0.1.0 ticket path
  SSL_BASE_V1=<v1.0.0 file>   golden negative for P9/P11 when `git show
                              d25468a5:scripts/score_selection_log.py` is not
                              available (shallow clone); otherwise git is used
  SSL_BASE_V11=<v1.1.0 file>  golden negative for P12/P14 when `git show
                              53c8544e:scripts/score_selection_log.py` is not
                              available (CI fetches both bases by SHA first)
  TFB_TEST_SELLOG_TSV=<_Selection_Log.tsv>   real-data legs (10-01 export)
  TFB_TEST_EXPORT_DIR=<dir>   Global_Markets.tsv / Mutual_Funds.tsv /
                              Market_Leaders.tsv of the same export
P1 embedded self-test 24/24 (7 v0.1.0 cases + 9 v1.0.0 + 5 v1.1.0:
   Spearman, reference mapping, ranking health / verdicts, wiring + 3 v1.2.0:
   shadow log parsing, shadow base price, the pre-registered verdict)
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
P9 v1.1.0 GOLDEN NEGATIVE: TFB_BOARD_SCORECARD_RANKING unset / off -> stdout,
   the three artifacts, the step summary, the tab rectangle and the _Run_Log
   row are byte-identical to the untouched v1.0.0 base (d25468a5, read with
   `git show`; only the version tag differs and is normalised); the base
   never emits RANKING HEALTH, even with the gate on
P10 gate on, synthetic _Selection_Log of 50 seats (5 boards x ranks 1-10,
   Engine ROI / Rel / Conf / Sector / Market filled; 46 .US names over 6
   sector words + PHP.L / WDP.BR / 2222.SR / AEFES.IS) with a prices CSV
   carrying SPUS.US, XLF.US, VNQ.US, XLE.US, ^STOXX50E, ^FTSE, ^TASI.SR (XLK /
   XLU deliberately missing): INVERTED / OK / INSUFFICIENT_DATA verdicts in
   three variants; reference mapping per row; the seven DETAIL columns at the
   END with hand-checked values; every RANKING HEALTH row re-aggregated
   independently from the detail CSV; Spearman cross-checked by an
   independent implementation + the hand-checked 6-point case; MD / step
   summary / stdout carry the section; write mode: rectangle block below
   CHURN, _Run_Log message carries the verdict, details JSON too
P11 the five v1.1.0 selftest cases still named (# 17-21); v1.0.0 base 16/16
P12 v1.2.0 GOLDEN NEGATIVE: TFB_BOARD_SCORECARD_CHALLENGER unset / off ->
   stdout, the three artifacts, the step summary, the tab rectangle and the
   _Run_Log row byte-identical to the untouched v1.1.0 (53c8544e), with
   TFB_BOARD_SCORECARD_RANKING off AND on; the base never emits CHALLENGER
P13 gate on, a synthetic _Shadow_Rank_Log in the cockpit's 24-column layout
   (contract-checked against 16_Decision_Top10.gs when v1.17.0 is on the
   tree): six verdicts end to end - PROMOTE_ELIGIBLE / CONTINUE (hit edge) /
   CONTINUE (churn) / KEEP_CHAMPION / INSUFFICIENT_DATA / NO_DATA - every
   order row re-derived independently (base = the close before the logged
   day's session, +5 vs XLF / SPUS, hit / mean / worst / churn); first event
   of a UTC day only (a later same-day event, ranks 6-10 and BOARD rows are
   never scored nor fetched); a Saturday log never counts as a trading day;
   live read (serials) == TSV; the read is UNFORMATTED + SERIAL_NUMBER and
   the tab is never written; fetch list = board + SPUS + refs + the shadow
   top-5 names and their references; a missing tab -> NO_DATA silently, a
   failed read -> WARN + NO_DATA; additivity: with the gate on, removing the
   CHALLENGER block (and the verdict suffixes) gives the gate-off outputs
P14 selftest() grew 21 -> 24 with the three v1.2.0 cases named; the v1.1.0
   base is 21/21

Run under pytest in an isolated subprocess.
"""
from pathlib import Path
import subprocess
import sys

def main():
    import contextlib, csv, hashlib, importlib.util, io, json, os, re, shutil, statistics, sys, tempfile
    from datetime import date, datetime, timedelta

    HERE = os.path.dirname(os.path.abspath(__file__))
    ROOT = os.path.abspath(os.path.join(HERE, ".."))
    SSL_FILE = os.environ.get("SSL_FILE") or os.path.join(ROOT, "scripts", "score_selection_log.py")
    SSL_BASE = os.environ.get("SSL_BASE", "")
    SSL_BASE_V1 = os.environ.get("SSL_BASE_V1", "")
    BASE_V1_REV = "d25468a5"          # main before v1.1.0: the untouched v1.0.0 script
    SSL_BASE_V11 = os.environ.get("SSL_BASE_V11", "")
    BASE_V11_REV = "53c8544e"         # main before v1.2.0: the untouched v1.1.0 script
    TSV = os.environ.get("TFB_TEST_SELLOG_TSV", "")
    EXPORT_DIR = os.environ.get("TFB_TEST_EXPORT_DIR", "")
    ENVS = ("TFB_BOARD_SCORECARD", "TFB_BOARD_SCORECARD_MIN_COVERAGE", "GITHUB_STEP_SUMMARY",
            "TFB_BOARD_SCORECARD_RANKING", "TFB_BOARD_SCORECARD_CHALLENGER")
    ASOF = "2026-10-01T06:00:00Z"


    def _load(path, name):
        spec = importlib.util.spec_from_file_location(name, path)
        m = importlib.util.module_from_spec(spec)
        sys.modules[name] = m
        spec.loader.exec_module(m)
        return m


    m = _load(SSL_FILE, "ssl_under_test")
    assert tuple(map(int, m.SCRIPT_VERSION.split("."))) >= (1, 1, 0), m.SCRIPT_VERSION
    VTAG = "v" + m.SCRIPT_VERSION
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
        num_cols = {hdr.index(c) for c in ("Rank", "Price", "Price SAR", "FX→SAR",
                                           "Engine ROI %", "Rel") if c in hdr}
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
               outcome="", exit_=False, output="HELD", ccy="USD", sector="Tech",
               eroi="", rel="", conf="", market=""):
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
                "Stability": "" if exit_ else "FAST-TRACK (day 1)",
                "Engine ROI %": eroi, "Rel": rel, "Conf": conf, "Market": market}
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

    # ------------------------------------------- v1.1.0 [RANKING HEALTH] fixture
    # 5 boards (09-01, 09-08, 09-15, 09-22, 10-01 at 10:00 Riyadh) x ranks 1-10
    # = 50 seats; every board replaces the previous one whole (DROPPED). Each
    # name's closes are a straight line close_i = 100 * (1 + g * i) over its
    # venue's sessions; the seat price is the previous session's close, so
    # the sign of every excess is sign(g_name - g_reference) by construction
    # (f(g) = (1 + N g) / (1 + k g) rises with g while k < N). References:
    # SPUS +0.001/session, XLF +0.002, VNQ +0.003, XLE +0.004, the venue
    # indexes +0.001. XLK.US and XLU.US are NOT in the prices CSV (mapped but
    # missing -> blank vs-Ref columns, SPUS fallback in the statistics).
    RK_ASOF = "2026-10-02T06:00:00Z"
    RK_SECTOR = {"FS": "Financial Services", "RE": "Real Estate", "EN": "Energy",
                 "TK": "Technology", "BK": "Banks", "UT": "Utilities"}
    RK_REF = {"FS": "XLF.US", "RE": "VNQ.US", "EN": "XLE.US", "TK": "XLK.US", "BK": "",
              "UT": "XLU.US"}
    RK_REF_G = {"SPUS.US": 0.001, "XLF.US": 0.002, "VNQ.US": 0.003, "XLE.US": 0.004,
                "^STOXX50E": 0.001, "^FTSE": 0.001, "^TASI.SR": 0.001}
    RK_RANK_G = {1: -0.004, 2: -0.003, 3: -0.002, 4: 0.0015, 5: 0.0025, 6: 0.0035,
                 7: 0.006, 8: 0.007, 9: 0.008, 10: 0.009}
    RK_DAYS = ["2026-09-01", "2026-09-08", "2026-09-15", "2026-09-22", "2026-10-01"]
    # (symbol, sector key, reference) in seat order: board-major, rank-minor
    RK_SEATS = [("FS01.US", "FS", "XLF.US"), ("RE01.US", "RE", "VNQ.US"), ("EN01.US", "EN", "XLE.US"),
                ("FS02.US", "FS", "XLF.US"), ("RE02.US", "RE", "VNQ.US"), ("EN02.US", "EN", "XLE.US"),
                ("PHP.L", "FS", "^FTSE"), ("WDP.BR", "RE", "^STOXX50E"), ("FS03.US", "FS", "XLF.US"),
                ("2222.SR", "EN", "^TASI.SR"),
                ("FS04.US", "FS", "XLF.US"), ("RE03.US", "RE", "VNQ.US"), ("EN03.US", "EN", "XLE.US"),
                ("FS05.US", "FS", "XLF.US"), ("RE04.US", "RE", "VNQ.US"), ("EN04.US", "EN", "XLE.US"),
                ("FS06.US", "FS", "XLF.US"), ("RE05.US", "RE", "VNQ.US"), ("EN05.US", "EN", "XLE.US"),
                ("TK01.US", "TK", "XLK.US"),
                ("FS07.US", "FS", "XLF.US"), ("RE06.US", "RE", "VNQ.US"), ("EN06.US", "EN", "XLE.US"),
                ("FS08.US", "FS", "XLF.US"), ("RE07.US", "RE", "VNQ.US"), ("EN07.US", "EN", "XLE.US"),
                ("FS09.US", "FS", "XLF.US"), ("RE08.US", "RE", "VNQ.US"), ("EN08.US", "EN", "XLE.US"),
                ("TK02.US", "TK", "XLK.US"),
                ("FS10.US", "FS", "XLF.US"), ("RE09.US", "RE", "VNQ.US"), ("BK01.US", "BK", ""),
                ("FS11.US", "FS", "XLF.US"), ("RE10.US", "RE", "VNQ.US"), ("AEFES.IS", "TK", ""),
                ("FS12.US", "FS", "XLF.US"), ("RE11.US", "RE", "VNQ.US"), ("TK03.US", "TK", "XLK.US"),
                ("BK02.US", "BK", ""),
                ("FS13.US", "FS", "XLF.US"), ("RE12.US", "RE", "VNQ.US"), ("UT01.US", "UT", "XLU.US"),
                ("FS14.US", "FS", "XLF.US"), ("FS15.US", "FS", "XLF.US"), ("FS16.US", "FS", "XLF.US"),
                ("FS17.US", "FS", "XLF.US"), ("FS18.US", "FS", "XLF.US"), ("FS19.US", "FS", "XLF.US"),
                ("FS20.US", "FS", "XLF.US")]
    assert len(RK_SEATS) == 50 and len({s[0] for s in RK_SEATS}) == 50

    def _rk_fixture(variant):
        """variant: 'inverted' (ranks 1-3 fall, 7-10 rise; Engine ROI falls
        with the drift), 'ok' (the mirror image: ranks 1-3 rise; Engine ROI
        rises with the drift), 'insufficient' (the first board only).
        Returns (grid, prices_rows, seats) with seats = per-seat facts."""
        boards = RK_DAYS[:1] if variant == "insufficient" else RK_DAYS
        grid, seats, rows = [SELLOG_HDR], [], []
        for b, day in enumerate(boards):
            for rank in range(1, 11):
                sym, key, ref = RK_SEATS[10 * b + rank - 1]
                g = RK_RANK_G[11 - rank] if variant == "ok" else RK_RANK_G[rank]
                roi = (40.0 + 5000.0 * g) if variant == "ok" else (60.0 - 5000.0 * g)
                conf = "Medium" if rank <= 3 else ("High" if rank <= 6 or rank % 2 == 0 else "Low")
                sess = SR if sym.endswith(".SR") else US
                k = sess.index(day)                       # sessions before the seat
                seat_px = 100.0 * (1.0 + g * k)
                ccy = "SAR" if sym.endswith(".SR") else ("GBP" if sym.endswith(".L") else
                                                         ("EUR" if sym.endswith(".BR") else
                                                          ("TRY" if sym.endswith(".IS") else "USD")))
                grid.append(_slrow(day + "T10:00:00", day + " 10:00:00", sym, str(float(rank)),
                                   repr(seat_px), "3.75", ccy=ccy, sector=RK_SECTOR[key],
                                   eroi=repr(roi), rel=str(70.0 + rank), conf=conf,
                                   market=sym.rsplit(".", 1)[1]))
                rows += [(sym, d, 100.0 * (1.0 + g * i)) for i, d in enumerate(sess, 1)]
                g_ref = RK_REF_G.get(ref, RK_REF_G["SPUS.US"])     # mapped-but-missing -> SPUS
                seats.append({"symbol": sym, "day": day, "rank": rank, "g": g, "roi": roi,
                              "conf": conf, "sector": RK_SECTOR[key], "ref": ref,
                              "hit_ref": g > g_ref, "hit_spus": g > RK_REF_G["SPUS.US"],
                              "ref_present": ref in RK_REF_G})
        for ref, gr in RK_REF_G.items():
            sess = SR if ref.endswith(".SR") else US
            rows += [(ref, "2026-08-31", 50.0)] + [(ref, d, 50.0 * (1.0 + gr * i))
                                                   for i, d in enumerate(sess, 1)]
        return grid, rows, seats

    def _spearman_ref(xs, ys):
        """Independent Spearman (average ranks for ties, Pearson on ranks)."""
        def ranks(v):
            idx = sorted(range(len(v)), key=lambda i: v[i])
            out, i = [0.0] * len(v), 0
            while i < len(idx):
                j = i
                while j + 1 < len(idx) and v[idx[j + 1]] == v[idx[i]]:
                    j += 1
                for t in range(i, j + 1):
                    out[idx[t]] = (i + j) / 2.0 + 1.0
                i = j + 1
            return out
        rx, ry = ranks(xs), ranks(ys)
        n = len(xs)
        mx, my = sum(rx) / n, sum(ry) / n
        num = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
        den = (sum((a - mx) ** 2 for a in rx) * sum((b - my) ** 2 for b in ry)) ** 0.5
        return num / den

    rk_fx = {}
    for variant in ("inverted", "ok", "insufficient"):
        g_, p_, s_ = _rk_fixture(variant)
        t_path, p_path = (os.path.join(TMP, "rk_%s_sellog.tsv" % variant),
                          os.path.join(TMP, "rk_%s_prices.csv" % variant))
        _write_tsv(t_path, g_)
        _write_prices(p_path, p_)
        rk_fx[variant] = (t_path, p_path, g_, s_, p_)


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
    T("P1 embedded self-test 24/24", rc == 0 and "SELFTEST 24/24: ALL GREEN" in so, so.strip())

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
    T("P3 rc 0 + tag line", rc == 0 and "[BOARD-SCORECARD %s] mode=observe source=tsv episodes=5 "
      "open=2 since=2026-09-01(5) priced=4 prices=csv 4/5 bench=ok orphan_exits=1 wrote=0 artifacts=3" % VTAG in so
      and so.strip().splitlines()[-1].endswith("artifacts=3"), so.strip().splitlines()[-1])
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
      and rect[0][0].startswith("[BOARD-SCORECARD %s]" % VTAG) and m.DETAIL_HEADER in rect, len(rect))
    rl = book.tabs["_Run_Log"].values[-1]
    T("P6 _Run_Log row: OK, tag, details JSON", rl[1:5] == ["INFO", "score_selection_log", "_Board_Scorecard", "OK"]
      and rl[5].startswith("[BOARD-SCORECARD %s] episodes=" % VTAG) and json.loads(rl[9])["mode"] == "write"
      and "ranking" not in rl[5] and "ranking" not in json.loads(rl[9]), rl[:6])
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

    # ----------------------------------------------------------- v1.1.0 helpers
    def _run_mod(mod, book, mode, out_dir, tsv_path, px_path, rk=None, asof=RK_ASOF, step=None,
                 ch=None, shadow=None, fetch=False):
        """One scorecard run of `mod`: live (stub book) when book is given,
        else the TSV path. rk = TFB_BOARD_SCORECARD_RANKING value or None;
        ch = TFB_BOARD_SCORECARD_CHALLENGER value or None (v1.2.0); shadow =
        --shadow-tsv path; fetch = --fetch-yahoo (mod.fetch_yahoo patched by
        the caller) instead of --prices-csv."""
        if book is not None:
            mod._open_book = lambda: book
        args = (["--scorecard", "--fetch-yahoo"] if fetch else ["--scorecard", "--prices-csv", px_path])
        args += ["--asof", asof, "--out-dir", out_dir]
        args += ["--live"] if book is not None else ["--tsv", tsv_path]
        if shadow:
            args += ["--shadow-tsv", shadow]
        with _env(TFB_BOARD_SCORECARD=mode, TFB_BOARD_SCORECARD_RANKING=rk,
                  TFB_BOARD_SCORECARD_MIN_COVERAGE="0", GITHUB_STEP_SUMMARY=step,
                  TFB_BOARD_SCORECARD_CHALLENGER=ch):
            return _call(mod.main, args)

    def _artifacts(out_dir):
        return {f: open(os.path.join(out_dir, f), "rb").read()
                for f in ("board_scorecard_detail.csv", "board_scorecard_summary.csv",
                          "board_scorecard.md")}

    def _norm(obj):
        """Version-tag normalisation: v1.1.0 text -> v1.0.0 (meta line, tag
        lines, details JSON) so the rest can be compared byte for byte."""
        return (obj if isinstance(obj, str) else json.dumps(obj, default=str)).replace(
            m.SCRIPT_VERSION, "1.0.0")

    base_v1_path = os.path.join(TMP, "base_v1_0_0", "score_selection_log.py")
    os.makedirs(os.path.dirname(base_v1_path), exist_ok=True)
    base_v1_src = ""
    if SSL_BASE_V1 and os.path.exists(SSL_BASE_V1):
        base_v1_src = SSL_BASE_V1
    else:
        try:
            blob = subprocess.run(["git", "-C", ROOT, "show", BASE_V1_REV + ":scripts/score_selection_log.py"],
                                  capture_output=True, text=True, timeout=60)
            if blob.returncode == 0 and "SCRIPT_VERSION = \"1.0.0\"" in blob.stdout:
                with open(base_v1_path, "w", encoding="utf-8") as fh:
                    fh.write(blob.stdout)
                base_v1_src = base_v1_path
        except (OSError, subprocess.SubprocessError):
            base_v1_src = ""
    base_v1 = _load(base_v1_src, "ssl_base_v1_0_0") if base_v1_src else None

    inv_tsv, inv_px, inv_grid, inv_seats, _inv_rows = rk_fx["inverted"]
    inv_live = _serialize_live(inv_grid)

    # ------------------------------------------------------------------- P9
    if base_v1 is not None:
        T("P9 base is the untouched v1.0.0 (SCRIPT_VERSION, no ranking gate helper)",
          base_v1.SCRIPT_VERSION == "1.0.0" and not hasattr(base_v1, "_ranking_enabled")
          and not hasattr(base_v1, "ranking_health"))
        for gate in (None, "off"):
            label = "unset" if gate is None else "off"
            res = {}
            for who, mod in (("new", m), ("base", base_v1)):
                od = os.path.join(TMP, "p9_%s_%s" % (who, label))
                st = os.path.join(TMP, "p9_step_%s_%s.md" % (who, label))
                rc, so = _run_mod(mod, None, "observe", od, inv_tsv, inv_px, rk=gate, step=st)
                res[who] = (rc, _norm(so), {k: _norm(v.decode("utf-8")) for k, v in _artifacts(od).items()},
                            _norm(open(st, encoding="utf-8").read()))
            T("P9 gate %s: stdout + 3 artifacts + step summary byte-identical to v1.0.0 (tag normalised)"
              % label, res["new"] == res["base"] and res["new"][0] == 0,
              [(k, len(res["new"][2][k]), len(res["base"][2][k])) for k in res["new"][2]])
            T("P9 gate %s: 22 DETAIL columns, no RANKING HEALTH anywhere" % label,
              res["new"][2]["board_scorecard_detail.csv"].splitlines()[0].count(",") == 21
              and "RANKING" not in res["new"][1] and "ranking" not in res["new"][1]
              and all("RANKING" not in v for v in res["new"][2].values()))
            # write path: rectangle + _Run_Log row
            rects, logs = {}, {}
            for who, mod in (("new", m), ("base", base_v1)):
                book = StubBook({"_Selection_Log": inv_live, "_Run_Log": [RUNLOG_HDR]})
                rc, so = _run_mod(mod, book, "write", os.path.join(TMP, "p9w_%s_%s" % (who, label)),
                                  inv_tsv, inv_px, rk=gate)
                rects[who] = (rc, _norm(book.tabs["_Board_Scorecard"].values),
                              [c[0] for c in _writes(book)])
                logs[who] = _norm(book.tabs["_Run_Log"].values[-1])
            T("P9 gate %s: write mode -> identical rectangle, identical write calls, identical _Run_Log row"
              % label, rects["new"] == rects["base"] and logs["new"] == logs["base"]
              and rects["new"][2] == ["add_worksheet", "update", "append_row"],
              (rects["new"][2], logs["new"][:120]))
        # the base ignores the gate: no RANKING HEALTH even when it is on
        od = os.path.join(TMP, "p9_base_on")
        rc, so = _run_mod(base_v1, None, "observe", od, inv_tsv, inv_px, rk="on")
        T("P9 base v1.0.0 never emits RANKING HEALTH (gate on is ignored by it)",
          rc == 0 and "RANKING HEALTH" not in so
          and all(b"RANKING" not in v for v in _artifacts(od).values())
          and _artifacts(od)["board_scorecard_detail.csv"].splitlines()[0].count(b",") == 21)
    else:
        out.append("SKIP P9 golden negative (git show %s unavailable; set SSL_BASE_V1=<v1.0.0 file>)"
                   % BASE_V1_REV)

    # ------------------------------------------------------------------ P10
    EXTRA = ["Rank", "Engine ROI %", "Rel", "Conf", "Ref", "vs Ref +5 pp", "vs Ref Now pp"]
    T("P10 constants: DETAIL_HEADER_RANKING = DETAIL_HEADER + 7 at the END; ROI_CAP_BUCKET 34; header",
      m.DETAIL_HEADER_RANKING == m.DETAIL_HEADER + EXTRA and m.ROI_CAP_BUCKET == 34.0
      and m.RANKING_HEADER == ["Window", "Cut", "Bucket", "Episodes", "With Data", "Hit % (vs ref)",
                               "Mean vs Ref pp", "Mean vs SPUS pp"])
    seen = []
    for v in (None, "off", "bogus", "on", "ON", "1", "true", "yes"):
        with _env(TFB_BOARD_SCORECARD_RANKING=v):
            seen.append(m._ranking_enabled())
    T("P10 gate parsing: unset/off/bogus -> off; on/1/true/yes -> on",
      seen == [False, False, False, True, True, True, True, True], seen)
    T("P10 Spearman hand check: 6 points with a tie -> 14.5/sqrt(17*17.5) = 0.84067; textbook -29/165",
      abs(m.spearman([1, 2, 2, 4, 5, 6], [2, 1, 3, 4, 6, 5]) - 14.5 / (17.0 * 17.5) ** 0.5) < 1e-12
      and abs(m.spearman([106, 86, 100, 101, 99, 103, 97, 113, 112, 110],
                         [7, 0, 27, 50, 28, 29, 20, 12, 6, 17]) + 29.0 / 165.0) < 1e-12
      and m.spearman([1, 2, 3], [1, 1, 1]) is None)
    T("P10 reference mapping: US sector words / non-US suffix / unmatched",
      [m.ref_symbol(s, sec) for s, sec in (("A.US", "Financial Services"), ("A.US", "Financials"),
                                           ("A.US", "Real Estate"), ("A.US", "Technology"),
                                           ("A.US", "Information Technology"), ("A.US", "Healthcare"),
                                           ("A.US", "Consumer Cyclical"), ("A.US", "Consumer Defensive"),
                                           ("A.US", "Basic Materials"), ("A.US", "Communication Services"),
                                           ("A.US", "Consumer"), ("A.US", "Banks"), ("A.US", ""),
                                           ("PHP.L", "Real Estate"), ("WDP.BR", "Real Estate"),
                                           ("URW.PA", ""), ("1972.HK", "Energy"), ("2222.SR", "Energy"),
                                           ("7203.T", "Consumer Cyclical"), ("RY.TO", "Financials"),
                                           ("BHP.AX", "Basic Materials"), ("D05.SI", "Financials"),
                                           ("AMXL.MX", ""), ("005930.KS", "Technology"), ("X.IS", ""))] ==
      ["XLF.US", "XLF.US", "VNQ.US", "XLK.US", "XLK.US", "XLV.US", "XLY.US", "XLP.US", "XLB.US", "XLC.US",
       "", "", "", "^FTSE", "^STOXX50E", "^STOXX50E", "^HSI", "^TASI.SR", "^N225", "^GSPTSE", "^AXJO",
       "^STI", "^MXX", "", ""])

    def _rk_rows(summary_csv):
        rows = _read_csv(summary_csv)
        i = rows.index(m.RANKING_HEADER)
        return [r for r in rows[i + 1:] if r]

    def _check_variant(variant, want_verdict):
        tsv_path, px_path, grid, seats, px_rows = rk_fx[variant]
        od = os.path.join(TMP, "p10_" + variant)
        st = os.path.join(TMP, "p10_step_%s.md" % variant)
        rc, so = _run_mod(m, None, "observe", od, tsv_path, px_path, rk="on", step=st)
        n = len(seats)
        last = so.strip().splitlines()[-1]
        T("P10 %s: rc 0, %d episodes, tag line ends with ranking=%s" % (variant, n, want_verdict),
          rc == 0 and ("episodes=%d" % n) in last and last.endswith(" ranking=" + want_verdict), last)
        hdr, det = _detail_by(os.path.join(od, "board_scorecard_detail.csv"))
        T("P10 %s: detail header = v1.0.0 columns + the 7 new ones at the END" % variant,
          hdr == m.DETAIL_HEADER_RANKING, hdr[-8:])
        by = {d["Symbol"]: d for d in det}
        T("P10 %s: every seat scored (OPEN / CLOSED), all %d symbols once" % (variant, n),
          len(det) == n and all(d["Status"] in ("OPEN", "CLOSED") for d in det)
          and all(s["symbol"] in by for s in seats), sorted({d["Status"] for d in det}))
        exp_ref = {s["symbol"]: s["ref"] for s in seats}
        T("P10 %s: Ref column per row = sector ETF (.US) / venue index / blank" % variant,
          all(by[s]["Ref"] == r for s, r in exp_ref.items()),
          [(s, by[s]["Ref"], r) for s, r in exp_ref.items() if by[s]["Ref"] != r][:5])
        missing = [s["symbol"] for s in seats if s["ref"] and not s["ref_present"]]
        T("P10 %s: mapped-but-missing series (XLK/XLU, %d names) -> blank vs-Ref columns, not an error"
          % (variant, len(missing)),
          (variant == "insufficient" or len(missing) == 4)
          and all(by[s]["vs Ref +5 pp"] == "" and by[s]["vs Ref Now pp"] == ""
                  and by[s]["vs SPUS Now pp"] != "" for s in missing), missing)
        T("P10 %s: Rank / Engine ROI %% / Rel / Conf carried from the first seat" % variant,
          all(by[s["symbol"]]["Rank"] == str(s["rank"]) and by[s["symbol"]]["Conf"] == s["conf"]
              and float(by[s["symbol"]]["Engine ROI %"]) == round(s["roi"], 2)
              and float(by[s["symbol"]]["Rel"]) == 70.0 + s["rank"] for s in seats))
        T("P10 %s: hit sign per row = sign(g_name - g_ref) by construction (vs ref and vs SPUS)" % variant,
          all(((float(by[s["symbol"]]["vs Ref Now pp"]) > 0) == s["hit_ref"] if s["ref_present"] else True)
              and (float(by[s["symbol"]]["vs SPUS Now pp"]) > 0) == s["hit_spus"] for s in seats))
        # EXACT excess per seat, independently from the fixture's own series
        # (bar 1 = the seat day for every venue here: 10:00 Riyadh is before
        # each venue's close); spec: reference % from its last close before
        # bar 1 to its last close on/before the name's last close.
        series = {}
        for sym_, d_, c_ in px_rows:
            series.setdefault(sym_, []).append((d_, c_))

        def _ret(ser, d1, dk):
            ref = val = None
            for d_, c_ in sorted(ser):
                if d_ < d1:
                    ref = c_
                if d_ <= dk:
                    val = c_
            return (val / ref - 1.0) * 100.0

        exact = {}
        for s_ in seats:
            ser = sorted(series[s_["symbol"]])
            k_ = [d_ for d_, _c in ser].index(s_["day"])
            seat_px = 100.0 if k_ == 0 else ser[k_ - 1][1]
            r_now = (ser[-1][1] / seat_px - 1.0) * 100.0
            x_spus = r_now - _ret(series["SPUS.US"], s_["day"], ser[-1][0])
            x_ref = (r_now - _ret(series[s_["ref"]], s_["day"], ser[-1][0])
                     if s_["ref_present"] else None)
            exact[s_["symbol"]] = {"now": r_now, "x_spus": x_spus, "x_ref": x_ref,
                                   "eff": x_spus if x_ref is None else x_ref,
                                   "rank": s_["rank"], "roi": s_["roi"], "conf": s_["conf"],
                                   "sector": s_["sector"]}
        r2s = lambda v: "" if v is None else str(round(v, 2) + 0.0)
        bad = [(sym_, by[sym_]["To Now %"], r2s(e["now"]), by[sym_]["vs SPUS Now pp"], r2s(e["x_spus"]),
                by[sym_]["vs Ref Now pp"], r2s(e["x_ref"])) for sym_, e in exact.items()
               if (by[sym_]["To Now %"], by[sym_]["vs SPUS Now pp"], by[sym_]["vs Ref Now pp"])
               != (r2s(e["now"]), r2s(e["x_spus"]), r2s(e["x_ref"]))]
        T("P10 %s: To Now %% / vs SPUS Now pp / vs Ref Now pp of all %d rows == exact values (2 dp)"
          % (variant, n), not bad, bad[:3])
        # independent re-aggregation of every RANKING HEALTH row of the Since window
        rows = _rk_rows(os.path.join(od, "board_scorecard_summary.csv"))
        W = "Since 2026-09-01"
        since_rows = [r for r in rows if r[0] == W]
        ex = list(exact.values())

        def agg(sub):
            xs = [e["eff"] for e in sub]
            sp = [e["x_spus"] for e in sub]
            return [str(len(sub)), str(len(xs)),
                    r2s(100.0 * sum(1 for x in xs if x > 0) / len(xs)) if xs else "",
                    r2s(sum(xs) / len(xs)) if xs else "", r2s(sum(sp) / len(sp)) if sp else ""]

        exp = []
        for lab, lo, hi in (("1-3", 1, 3), ("4-6", 4, 6), ("7-10", 7, 10)):
            exp.append([W, "Rank", lab] + agg([e for e in ex if lo <= e["rank"] <= hi]))
        exp.append([W, "Engine ROI", ">= 34"] + agg([e for e in ex if e["roi"] >= 34]))
        exp.append([W, "Engine ROI", "< 34"] + agg([e for e in ex if e["roi"] < 34]))
        for lab in ("High", "Medium"):
            exp.append([W, "Conf", lab] + agg([e for e in ex if e["conf"] == lab]))
        exp.append([W, "Conf", "Other"] + agg([e for e in ex if e["conf"] not in ("High", "Medium")]))
        cnt = {}
        for e in ex:
            cnt[e["sector"]] = cnt.get(e["sector"], 0) + 1
        big = sorted([k for k, c in cnt.items() if c >= 5], key=lambda k: (-cnt[k], k))
        for k in big:
            exp.append([W, "Sector", k] + agg([e for e in ex if e["sector"] == k]))
        exp.append([W, "Sector", "Other"] + agg([e for e in ex if e["sector"] not in big]))
        T("P10 %s: Rank / Engine ROI / Conf / Sector rows of the Since window == independent re-aggregation"
          " of the exact values (%d rows, byte-equal)" % (variant, len(exp)),
          since_rows[:len(exp)] == exp, [(a, b) for a, b in zip(since_rows, exp) if a != b][:2])
        rho = _spearman_ref([e["roi"] for e in ex], [e["eff"] for e in ex])
        rho_rk = _spearman_ref([e["rank"] for e in ex], [e["eff"] for e in ex])
        sp_rows = [r for r in since_rows if r[1] == "Spearman"]
        T("P10 %s: Spearman rows == independent implementation on the exact values (ROI %.3f, Rank %.3f, n=%d)"
          % (variant, rho, rho_rk, n),
          sp_rows[0][2] == "Engine ROI at seat vs excess: rho %.3f (n=%d)" % (round(rho, 3) + 0.0, n)
          and sp_rows[1][2] == "Rank at seat vs excess: rho %.3f (n=%d)" % (round(rho_rk, 3) + 0.0, n)
          and sp_rows[0][3:5] == [str(n), str(n)] and sp_rows[1][3:5] == [str(n), str(n)], sp_rows)
        text = "%s (spearman %.2f, n=%d)" % (want_verdict, round(rho, 2) + 0.0, n)
        vrow = [r for r in since_rows if r[1] == "Verdict"]
        T("P10 %s: ONE verdict row per window; Since window = '%s'" % (variant, text),
          len(vrow) == 1 and vrow[0][2] == text and vrow[0][3] == str(n) and since_rows[-1] == vrow[0]
          and [r[1] for r in rows].count("Verdict") == len({r[0] for r in rows}), vrow)
        md = open(os.path.join(od, "board_scorecard.md"), encoding="utf-8").read()
        T("P10 %s: RANKING HEALTH in stdout, MD artifact and step summary (Since rows + month verdicts)"
          % variant, "RANKING HEALTH" in so and md in so and open(st, encoding="utf-8").read() == md
          and "| " + " | ".join(vrow[0]) + " |" in md
          and all("| " + " | ".join(r) + " |" in md for r in rows if r[1] == "Verdict")
          and all(("| " + " | ".join(r) + " |" in md) == (r[0] == W or r[1] == "Verdict") for r in rows))
        return rows, det, by, rho

    rows_inv, det_inv, by_inv, rho_inv = _check_variant("inverted", "RANKING_INVERTED")
    W = "Since 2026-09-01"
    r13 = [r for r in rows_inv if r[:3] == [W, "Rank", "1-3"]][0]
    r710 = [r for r in rows_inv if r[:3] == [W, "Rank", "7-10"]][0]
    T("P10 inverted: ranks 1-3 15/15 hit 0 %%, ranks 7-10 20/20 hit 100 %%, Spearman(ROI) <= -0.20 (%.3f)" % rho_inv,
      r13[3:6] == ["15", "15", "0.0"] and r710[3:6] == ["20", "20", "100.0"] and rho_inv <= -0.20, (r13, r710))
    T("P10 inverted: Engine ROI >= 34 holds the 30 top-six seats, < 34 the 20 others; Conf High 25 / Medium 15 / Other 10",
      [r[3] for r in rows_inv if r[:2] == [W, "Engine ROI"]] == ["30", "20"]
      and [(r[2], r[3]) for r in rows_inv if r[:2] == [W, "Conf"]] == [("High", "25"), ("Medium", "15"), ("Other", "10")])
    T("P10 inverted: Sector rows = Financial Services 21, Real Estate 13, Energy 9 (>= 5 each), Other 7",
      [(r[2], r[3]) for r in rows_inv if r[:2] == [W, "Sector"]] ==
      [("Financial Services", "21"), ("Real Estate", "13"), ("Energy", "9"), ("Other", "7")])
    T("P10 inverted: month windows 2026-10 (10 seats) INSUFFICIENT_DATA, 2026-09 (40) RANKING_INVERTED",
      [r[2][:r[2].index(" (")] for r in rows_inv if r[1] == "Verdict"] ==
      ["RANKING_INVERTED", "INSUFFICIENT_DATA", "RANKING_INVERTED"]
      and [r[0] for r in rows_inv if r[1] == "Verdict"] == [W, "2026-10", "2026-09"]
      and [r for r in rows_inv if r[0] == "2026-10" and r[1] == "Verdict"][0][2].endswith("n=10)"))
    fs3 = by_inv["FS03.US"]
    T("P10 inverted hand check FS03.US (seat 09-01, g +0.8 %/session, XLF +0.2 %, SPUS +0.1 %): +5 4.0 /"
      " now 17.6 / vs XLF +5 3.0 / now 13.2 / vs SPUS +5 3.5 / now 15.4 / rank 9 / ROI 20 / Rel 79 / Low",
      [fs3[k] for k in ("+5 %", "To Now %", "vs Ref +5 pp", "vs Ref Now pp", "vs SPUS +5 pp", "vs SPUS Now pp",
                        "Rank", "Engine ROI %", "Rel", "Conf", "Ref", "Exit Kind")] ==
      ["4.0", "17.6", "3.0", "13.2", "3.5", "15.4", "9", "20.0", "79.0", "Low", "XLF.US", "DROPPED"], fs3)
    T("P10 inverted: PHP.L -> ^FTSE, WDP.BR -> ^STOXX50E, 2222.SR -> ^TASI.SR, AEFES.IS / Banks -> SPUS fallback",
      [by_inv[s]["Ref"] for s in ("PHP.L", "WDP.BR", "2222.SR", "AEFES.IS", "BK01.US")] ==
      ["^FTSE", "^STOXX50E", "^TASI.SR", "", ""]
      and all(by_inv[s]["vs Ref Now pp"] != "" for s in ("PHP.L", "WDP.BR", "2222.SR"))
      and all(by_inv[s]["vs Ref Now pp"] == "" and by_inv[s]["vs SPUS Now pp"] != "" for s in ("AEFES.IS", "BK01.US")))
    rows_ok, _d, _b, rho_ok = _check_variant("ok", "RANKING_OK")
    T("P10 ok: Spearman(ROI) >= +0.10 (%.3f) and ranks 1-3 hit 100 %% >= ranks 7-10" % rho_ok,
      rho_ok >= 0.10 and [r for r in rows_ok if r[:3] == [W, "Rank", "1-3"]][0][5] == "100.0")
    rows_ins, _d, _b, rho_ins = _check_variant("insufficient", "INSUFFICIENT_DATA")
    T("P10 insufficient: n=10 < 30 -> INSUFFICIENT_DATA although the sample is inverted (%.3f)" % rho_ins,
      rho_ins < 0 and [r for r in rows_ins if r[1] == "Verdict"][0][3] == "10")
    # write mode (live stub): rectangle block + _Run_Log verdict + details JSON
    book = StubBook({"_Selection_Log": inv_live, "_Run_Log": [RUNLOG_HDR]})
    rc, so = _run_mod(m, book, "write", os.path.join(TMP, "p10_write"), inv_tsv, inv_px, rk="on")
    rect = book.tabs["_Board_Scorecard"].values
    width = len(rect[0])
    i_ch = rect.index(["CHURN - how long seats lasted and how they ended"] + [""] * (width - 1))
    i_rk = rect.index([m.RANKING_TITLE] + [""] * (width - 1))
    i_dt = rect.index(["DETAIL - one row per board seat episode, newest first"] + [""] * (width - 1))
    rk_cells = [[str(c) for c in r[:8]] for r in rect[i_rk + 2:i_dt - 1]]
    T("P10 write: ONE rectangle %d wide (29 = 22 + 7), RANKING HEALTH block below CHURN and above DETAIL,"
      " header row, every row == summary CSV rows" % width,
      rc == 0 and len({len(r) for r in rect}) == 1 and width == len(m.DETAIL_HEADER_RANKING)
      and i_ch < i_rk < i_dt and rect[i_rk + 1][:8] == m.RANKING_HEADER
      and rk_cells == rows_inv and rect[i_dt + 1][:width] == m.DETAIL_HEADER_RANKING
      and [c[0] for c in _writes(book)] == ["add_worksheet", "update", "append_row"],
      (width, i_ch, i_rk, i_dt, len(rk_cells), len(rows_inv)))
    rl = book.tabs["_Run_Log"].values[-1]
    vtext = [r for r in rows_inv if r[:2] == [W, "Verdict"]][0][2]
    dj = json.loads(rl[9])
    T("P10 write: _Run_Log message ends with ' | ranking: %s'; details JSON carries the verdict" % vtext,
      rl[4] == "OK" and " wrote=" in rl[5] and rl[5].endswith(" | ranking: " + vtext)
      and dj["ranking"]["verdict"] == "RANKING_INVERTED" and dj["ranking"]["n"] == 50
      and dj["ranking"]["refs"] == 6 and dj["ranking"]["refs_total"] == 8
      and abs(dj["ranking"]["spearman_roi"] - round(rho_inv, 4)) < 1e-9, (rl[5], dj.get("ranking")))
    T("P10 write: meta line carries 'refs 6/8 | ranking RANKING_INVERTED' (XLK / XLU missing)",
      " | refs 6/8 | ranking RANKING_INVERTED" in rect[0][0], rect[0][0][-80:])
    live_det = _artifacts(os.path.join(TMP, "p10_write"))["board_scorecard_detail.csv"]
    tsv_det = _artifacts(os.path.join(TMP, "p10_inverted"))["board_scorecard_detail.csv"]
    T("P10 write: live read (serial 'Logged At', numeric Rank / Engine ROI % / Rel) == TSV path detail",
      live_det == tsv_det, (len(live_det), len(tsv_det)))
    digests.append(_h(live_det.decode("utf-8")))

    # ------------------------------------------------------------------ P11
    rc, so = _call(m.selftest)
    names = [l for l in open(SSL_FILE, encoding="utf-8").read().splitlines()
             if l.strip().startswith("# 17:") or l.strip().startswith("# 18:")
             or l.strip().startswith("# 19:") or l.strip().startswith("# 20:")
             or l.strip().startswith("# 21:")]
    T("P11 the five v1.1.0 selftest cases are still there (Spearman, mapping, ranking health, wiring, index clocks)",
      rc == 0 and "SELFTEST 24/24: ALL GREEN" in so and len(names) == 5
      and "Spearman" in names[0] and "reference" in names[1] and "ranking health" in names[2]
      and "gate" in names[3] and "venue" in names[4], names)
    if base_v1 is not None:
        rc_b, so_b = _call(base_v1.selftest)
        T("P11 base v1.0.0 selftest is 16/16 (the count rose by exactly the new cases)",
          rc_b == 0 and "SELFTEST 16/16: ALL GREEN" in so_b, so_b.strip())

    # ------------------------------------------------- v1.2.0 golden base (v1.1.0)
    base_v11_path = os.path.join(TMP, "base_v1_1_0", "score_selection_log.py")
    os.makedirs(os.path.dirname(base_v11_path), exist_ok=True)
    base_v11_src = ""
    if SSL_BASE_V11 and os.path.exists(SSL_BASE_V11):
        base_v11_src = SSL_BASE_V11
    else:
        try:
            blob = subprocess.run(["git", "-C", ROOT, "show", BASE_V11_REV + ":scripts/score_selection_log.py"],
                                  capture_output=True, text=True, timeout=60)
            if blob.returncode == 0 and "SCRIPT_VERSION = \"1.1.0\"" in blob.stdout:
                with open(base_v11_path, "w", encoding="utf-8") as fh:
                    fh.write(blob.stdout)
                base_v11_src = base_v11_path
        except (OSError, subprocess.SubprocessError):
            base_v11_src = ""
    base_v11 = _load(base_v11_src, "ssl_base_v1_1_0") if base_v11_src else None

    def _n11(who, obj):
        """The new module's version tag -> 1.1.0; the base is left as is."""
        txt = obj if isinstance(obj, str) else json.dumps(obj, default=str)
        return txt.replace(m.SCRIPT_VERSION, "1.1.0") if who == "new" else txt

    # ------------------------------------------------------------------ P12
    if base_v11 is not None:
        T("P12 base is the untouched v1.1.0 (SCRIPT_VERSION, ranking gate, no challenger helper)",
          base_v11.SCRIPT_VERSION == "1.1.0" and hasattr(base_v11, "_ranking_enabled")
          and not hasattr(base_v11, "_challenger_enabled") and not hasattr(base_v11, "shadow_events"))
        for rkv in (None, "on"):
            for gate in (None, "off"):
                label = "ranking %s / challenger %s" % (rkv or "unset", gate or "unset")
                res = {}
                for who, mod in (("new", m), ("base", base_v11)):
                    od = os.path.join(TMP, "p12_%s_%s_%s" % (who, rkv or "u", gate or "u"))
                    st = od + "_step.md"
                    rc, so = _run_mod(mod, None, "observe", od, inv_tsv, inv_px, rk=rkv, ch=gate, step=st)
                    res[who] = (rc, _n11(who, so),
                                {k: _n11(who, v.decode("utf-8")) for k, v in _artifacts(od).items()},
                                _n11(who, open(st, encoding="utf-8").read()))
                T("P12 %s: stdout + 3 artifacts + step summary byte-identical to v1.1.0 (tag normalised)"
                  % label, res["new"] == res["base"] and res["new"][0] == 0,
                  [(k, len(res["new"][2][k]), len(res["base"][2][k])) for k in res["new"][2]])
                T("P12 %s: no CHALLENGER anywhere" % label,
                  "CHALLENGER" not in res["new"][1] and "challenger" not in res["new"][1]
                  and all("CHALLENGER" not in v for v in res["new"][2].values()))
                rects, logs = {}, {}
                for who, mod in (("new", m), ("base", base_v11)):
                    book = StubBook({"_Selection_Log": inv_live, "_Run_Log": [RUNLOG_HDR]})
                    rc, so = _run_mod(mod, book, "write", os.path.join(TMP, "p12w_%s_%s_%s"
                                                                       % (who, rkv or "u", gate or "u")),
                                      inv_tsv, inv_px, rk=rkv, ch=gate)
                    rects[who] = (rc, _n11(who, book.tabs["_Board_Scorecard"].values),
                                  [c[0] for c in _writes(book)],
                                  [c for c in book.calls if "_Shadow_Rank_Log" in c])
                    logs[who] = _n11(who, book.tabs["_Run_Log"].values[-1])
                T("P12 %s: write mode -> identical rectangle, write calls, _Run_Log row; the shadow tab is "
                  "never touched" % label,
                  rects["new"] == rects["base"] and logs["new"] == logs["base"]
                  and rects["new"][2] == ["add_worksheet", "update", "append_row"] and rects["new"][3] == [],
                  (rects["new"][2], rects["new"][3], logs["new"][:120]))
        od = os.path.join(TMP, "p12_base_on")
        rc, so = _run_mod(base_v11, None, "observe", od, inv_tsv, inv_px, rk="on", ch="on")
        T("P12 base v1.1.0 never emits CHALLENGER (gate on is ignored by it)",
          rc == 0 and "CHALLENGER" not in so and "RANKING HEALTH" in so
          and all(b"CHALLENGER" not in v for v in _artifacts(od).values()))
    else:
        out.append("SKIP P12 golden negative (git show %s unavailable; set SSL_BASE_V11=<v1.1.0 file>)"
                   % BASE_V11_REV)

    # ------------------------------------------------------------------ P13
    # _Shadow_Rank_Log as the cockpit (16_Decision_Top10 v1.17.0) writes it:
    # one event = CHAMPION + CHALLENGER rows ranked 1-10 + BOARD rows, all
    # with one 'Logged At' (Riyadh wall time) and the UTC 'Day'. Each logged
    # day opens new top-5 names in both orders (rank 1 shared by the two
    # orders); ranks 6-10, BOARD rows and a later same-day event carry names
    # WITHOUT prices: they must never be scored nor fetched. A priced name's
    # closes are a straight line close_i = c0 * (1 + g * i) over US2 (i = 0
    # on 2026-08-31), so a name logged before the session of US2[j] has base
    # i = j and +5 at i = j + 5; its excess is sign(g - g_ref). Hits g =
    # +0.004, misses -0.002; references XLF.US +0.002 ('Financial Services')
    # and SPUS.US +0.001 ('Banks' maps to no reference -> SPUS).
    SH_HDR = ["Logged At", "Day", "Cohort", "Order", "Rank", "Symbol", "Market", "Sector", "Seat",
              "Live Rank", "Shadow Rank", "Engine ROI %", "Rel", "Conf", "Momentum", "Strength",
              "Strength Ref", "Cap", "Floor OK", "Mode", "Rule", "Builder", "Pool", "Overlap Top5"]
    gas_path = os.path.join(ROOT, "apps_script", "16_Decision_Top10.gs")
    gas_src = open(gas_path, encoding="utf-8").read() if os.path.exists(gas_path) else ""
    mh = re.search(r"var DT10_SHADOW_LOG_HEADERS = \[(.*?)\];", gas_src, re.S)
    if mh:
        gas_hdr = re.findall(r"'([^']*)'", mh.group(1))
        T("P13 contract: the fixture header == the cockpit's DT10_SHADOW_LOG_HEADERS (24 columns)",
          gas_hdr == SH_HDR, gas_hdr)
    else:
        out.append("SKIP P13 contract (16_Decision_Top10.gs v1.17.0 not on this tree)")
    T("P13 the scorer's required shadow columns are all in the cockpit's header",
      all(c in SH_HDR for c in ("Logged At", "Day", "Order", "Rank", "Symbol", "Sector",
                                "Engine ROI %", "Rel", "Conf", "Market")))

    US2 = _sessions(date(2026, 9, 1), 60, {0, 1, 2, 3, 4}, {date(2026, 9, 7)})
    CH_ASOF = (datetime.strptime(US2[-1], "%Y-%m-%d") + timedelta(days=3)).strftime("%Y-%m-%dT06:00:00Z")
    G_HIT, G_MISS, G_SPUS, G_XLF = 0.004, -0.002, 0.001, 0.002
    OC = {"CHAMPION": "M", "CHALLENGER": "N"}

    def _line(c0, g, i):
        return c0 * (1.0 + g * i)

    def _series(sym, g, c0=100.0):
        return ([(sym, "2026-08-31", _line(c0, g, 0))]
                + [(sym, d, _line(c0, g, i)) for i, d in enumerate(US2, 1)])

    CH_BOARD = [SELLOG_HDR] + [
        _slrow(d + "T10:00:00", d + " 10:00:00", s, str(float(r)), "100.0", "3.75",
               sector="Financial Services", eroi="20.0", rel="80.0", conf="High", market="US")
        for d in ("2026-09-01", "2026-09-02") for r, s in ((1, "BRD1.US"), (2, "BRD2.US"))]
    ch_board_tsv = os.path.join(TMP, "ch_board.tsv")
    _write_tsv(ch_board_tsv, CH_BOARD)
    CH_BASE_ROWS = (_series("BRD1.US", 0.001) + _series("BRD2.US", 0.0005)
                    + _series("SPUS.US", G_SPUS, 50.0) + _series("XLF.US", G_XLF, 50.0))

    def _ch_fixture(code, n_days, hits, sat_after=None, late_days=(), champ_fixed=()):
        """One _Shadow_Rank_Log variant -> (grid of Python values, price
        rows, expected scored episodes {order: [(symbol, j, g, g_ref)]},
        names that must never be fetched, trading days)."""
        grid, rows, unpriced = [SH_HDR], list(CH_BASE_ROWS), set()
        exp = {"CHAMPION": [], "CHALLENGER": []}
        g_of, seen = {}, {"CHAMPION": set(), "CHALLENGER": set()}

        def nm(order, j, r):
            o = "CHAMPION" if (order == "CHALLENGER" and r == 1) else order
            jj = 0 if (o == "CHAMPION" and r in champ_fixed) else j
            return "%s%s%02d%d.US" % (code, OC[o], jj, r)

        def ev_rows(at, day, j, cohort, late=False):
            rs = []
            for order in ("CHAMPION", "CHALLENGER"):
                for r in range(1, 11):
                    if late:
                        sym = "%sL%s%02d%d.US" % (code, OC[order], j, r)
                        unpriced.add(sym)
                    elif r <= 5:
                        sym = nm(order, j, r)
                    else:
                        sym = "%sX%s%02d%d.US" % (code, OC[order], j, r)
                        unpriced.add(sym)
                    rs.append([at, day, cohort, order, r, sym, "US",
                               "Banks" if r == 5 else "Financial Services", "",
                               r if order == "CHAMPION" else r + 1, r if order == "CHALLENGER" else r + 1,
                               20.0 + r, 80.0 - r, "High", 1.5, 70.0, 65.0, "N", "Y", "shadow", "1.0",
                               "1.28.0", 40, 3])
            for r in (1, 2):
                sym = "%sB%02d%d.US" % (code, j, r)
                unpriced.add(sym)
                rs.append([at, day, cohort, "BOARD", r, sym, "US", "Financial Services", "HELD", r,
                           r + 1, 20.0, 80.0, "High", "", "", "", "", "", "shadow", "1.0", "1.28.0", 40, 3])
            return rs

        for j in range(n_days):
            day = US2[j]
            grid += ev_rows(day + " 08:47:00", day, j, "cohort-%02d" % j)
            if j in late_days:
                grid += ev_rows(day + " 15:00:00", day, j, "cohort-%02dL" % j, late=True)
            if sat_after == j:
                sat = (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                assert datetime.strptime(sat, "%Y-%m-%d").weekday() == 5, sat
                grid += ev_rows(sat + " 08:47:00", sat, j, "cohort-%02dS" % j)
            for order in ("CHAMPION", "CHALLENGER"):
                for r in range(1, 6):
                    sym = nm(order, j, r)
                    g = G_HIT if r in hits[order] else G_MISS
                    assert g_of.setdefault(sym, g) == g, sym      # shared / fixed names agree
                    if sym not in seen[order]:
                        seen[order].add(sym)
                        exp[order].append((sym, j, g, G_SPUS if r == 5 else G_XLF))
        assert all(len(r) == 24 for r in grid)
        for sym, g in sorted(g_of.items()):
            rows += _series(sym, g)
        return grid, rows, exp, unpriced, n_days

    def _f(v):
        return str(round(v, 2) + 0.0)

    def _x5(g, gr, j):
        c0, c5 = _line(100.0, g, j), _line(100.0, g, j + 5)
        b0, b5 = _line(50.0, gr, j), _line(50.0, gr, j + 5)
        return (c5 / c0 - 1.0) * 100.0 - (b5 / b0 - 1.0) * 100.0

    def _ch_expect(exp, days, xlf_missing=False):
        """Independent per-order rows: +5 excess vs the reference (SPUS for
        'Banks', which maps to none), hit %, mean, worst, entries per trading
        day, mean vs SPUS. xlf_missing: the mapped XLF.US series is absent,
        so its episodes are unscored (never re-scored vs SPUS)."""
        rows_ = []
        for o in ("CHAMPION", "CHALLENGER"):
            lst = exp[o]
            xs = [_x5(g, gr, j) for (_s, j, g, gr) in lst if not (xlf_missing and gr == G_XLF)]
            sp = [_x5(g, G_SPUS, j) for (_s, j, g, _gr) in lst]
            rows_.append([o, str(days), str(len(lst)), str(len(xs)),
                          _f(100.0 * sum(1 for v in xs if v > 0) / len(xs)), _f(statistics.fmean(xs)),
                          _f(min(xs)), _f(len(lst) / days), _f(statistics.fmean(sp))])
        return rows_

    def _ch_block(out_dir):
        """The CHALLENGER block of the summary CSV: header, two orders, verdict."""
        rows_ = _read_csv(os.path.join(out_dir, "board_scorecard_summary.csv"))
        i = rows_.index(m.CHALLENGER_HEADER)
        return rows_[i:]

    VARIANTS = [
        # code, days, hits by order, saturday after, late days, champion fixed ranks, verdict, reason
        ("P", 25, {"CHAMPION": {1, 2}, "CHALLENGER": {1, 2, 3}}, 3, (1, 7), (),
         "PROMOTE_ELIGIBLE", "every condition met - owner decides"),
        ("C", 22, {"CHAMPION": {1, 2}, "CHALLENGER": {1, 2}}, None, (), (),
         "CONTINUE", "not yet: hit edge"),
        ("U", 25, {"CHAMPION": {1, 2, 3}, "CHALLENGER": {1, 2, 3}}, None, (), (1, 2, 3),
         "CONTINUE", "not yet: churn"),
        ("K", 41, {"CHAMPION": {1, 2}, "CHALLENGER": {1, 2}}, None, (), (),
         "KEEP_CHAMPION", "inconclusive at 41 days: hit edge"),
        ("I", 5, {"CHAMPION": {1, 2}, "CHALLENGER": {1, 2, 3}}, None, (), (),
         "INSUFFICIENT_DATA", "days 5/20, scored +5 25/25 (need 40 each)"),
    ]
    ch_fx = {}
    for code, nd, hits, sat, late, fixed, verdict, reason in VARIANTS:
        grid_, rows_, exp_, unp_, days_ = _ch_fixture(code, nd, hits, sat, late, fixed)
        sh_path, px_path = os.path.join(TMP, "ch_%s_shadow.tsv" % code), os.path.join(TMP, "ch_%s_px.csv" % code)
        _write_tsv(sh_path, grid_)
        _write_prices(px_path, rows_)
        ch_fx[code] = (grid_, sh_path, px_path, exp_, unp_, days_, verdict, reason)
        od = os.path.join(TMP, "p13_%s" % code)
        rc, so = _run_mod(m, None, "observe", od, ch_board_tsv, px_path, rk="on", asof=CH_ASOF, ch="on",
                          shadow=sh_path)
        blk = _ch_block(od)
        n_c, n_h = len(exp_["CHAMPION"]), len(exp_["CHALLENGER"])
        text = "%s (days %d, scored %d/%d; %s)" % (verdict, days_, n_c, n_h, reason)
        T("P13 %s: %d logged days -> %s (%s)" % (code, nd, verdict, reason),
          rc == 0 and blk[3] == ["Verdict", text, "", "", "", "", "", "", ""]
          and so.strip().splitlines()[-1].endswith(" ranking=INSUFFICIENT_DATA challenger=" + verdict),
          (blk[3:], so.strip()[-160:]))
        T("P13 %s: both order rows re-derived independently (days, episodes, scored, hit, mean, worst, "
          "churn, vs SPUS)" % code, blk[1:3] == _ch_expect(exp_, days_), (blk[1:3], _ch_expect(exp_, days_)))
        digests.append(_h(blk))

    # promote: first event of the UTC day only, Saturday not a trading day
    p_grid, p_sh, p_px, p_exp, p_unp, p_days, _v, _r = ch_fx["P"]

    # a MAPPED reference without prices (XLF.US absent) leaves its episodes
    # unscored - never vs SPUS (review on #851); 'Banks' (no mapping) still
    # scores vs SPUS
    px_noxlf = os.path.join(TMP, "ch_P_px_noxlf.csv")
    _write_prices(px_noxlf, [r for r in _read_csv(p_px)[1:] if r[0] != "XLF.US"])
    od = os.path.join(TMP, "p13_P_noxlf")
    rc, so = _run_mod(m, None, "observe", od, ch_board_tsv, px_noxlf, rk="on", asof=CH_ASOF, ch="on",
                      shadow=p_sh)
    blk = _ch_block(od)
    T("P13 P without XLF.US: the 100 Financial Services episodes per order stay unscored, the 25 'Banks' "
      "ones score vs SPUS -> INSUFFICIENT_DATA (the metric never switches benchmark)",
      rc == 0 and blk[1:3] == _ch_expect(p_exp, p_days, xlf_missing=True)
      and blk[1][:4] == ["CHAMPION", "25", "125", "25"] and blk[2][:4] == ["CHALLENGER", "25", "125", "25"]
      and blk[3][1] == "INSUFFICIENT_DATA (days 25, scored 25/25; days 25/20, scored +5 25/25 (need 40 each))",
      (blk[1:], _ch_expect(p_exp, p_days, xlf_missing=True)))
    od_p = os.path.join(TMP, "p13_P")
    blk_p = _ch_block(od_p)
    T("P13 P: 125 episodes per order although day 1 and day 7 carry a later event and 09-05 (Saturday) "
      "repeats Friday: 25 trading days, 5 entries per day",
      blk_p[1][:4] == ["CHAMPION", "25", "125", "125"] and blk_p[2][:4] == ["CHALLENGER", "25", "125", "125"]
      and blk_p[1][7] == "5.0" and blk_p[2][7] == "5.0", blk_p[1:3])
    T("P13 P: hit 40.0 (champion, ranks 1-2) vs 60.0 (challenger, ranks 1-3)",
      blk_p[1][4] == "40.0" and blk_p[2][4] == "60.0", (blk_p[1][4], blk_p[2][4]))
    md_p = open(os.path.join(od_p, "board_scorecard.md"), encoding="utf-8").read()
    T("P13 P: MD carries the CHALLENGER title, header and verdict; meta ' | challenger PROMOTE_ELIGIBLE'",
      m.CHALLENGER_TITLE in md_p and ("| " + " | ".join(m.CHALLENGER_HEADER) + " |") in md_p
      and "| Verdict | PROMOTE_ELIGIBLE (days 25, scored 125/125;" in md_p
      and md_p.splitlines()[0].endswith(" | challenger PROMOTE_ELIGIBLE"), md_p.splitlines()[0][-90:])
    _hdr_p, det_p = _detail_by(os.path.join(od_p, "board_scorecard_detail.csv"))
    T("P13 P: DETAIL never carries shadow names (board seats only)",
      sorted(d["Symbol"] for d in det_p) == ["BRD1.US", "BRD2.US"], [d["Symbol"] for d in det_p][:6])

    # NO_DATA: an empty shadow export (header only) and a missing one
    empty_sh = os.path.join(TMP, "ch_empty_shadow.tsv")
    _write_tsv(empty_sh, [SH_HDR])
    od = os.path.join(TMP, "p13_nodata")
    rc, so = _run_mod(m, None, "observe", od, ch_board_tsv, p_px, rk="on", asof=CH_ASOF, ch="on", shadow=empty_sh)
    blk = _ch_block(od)
    T("P13 NO_DATA: an empty _Shadow_Rank_Log -> zero rows per order, verdict NO_DATA, rc 0",
      rc == 0 and blk[1][:4] == ["CHAMPION", "0", "0", "0"] and blk[2][:4] == ["CHALLENGER", "0", "0", "0"]
      and blk[3][1] == "NO_DATA (days 0, scored 0/0; no trading day logged in _Shadow_Rank_Log)"
      and so.strip().splitlines()[-1].endswith(" challenger=NO_DATA"), (blk, so.strip()[-80:]))

    # live path: serials for 'Logged At' and 'Day', numbers as numbers
    def _ser_dt(txt):
        return (datetime.fromisoformat(txt) - datetime(1899, 12, 30)).total_seconds() / 86400.0

    def _serialize_shadow(grid):
        outg = [list(grid[0])]
        for r in grid[1:]:
            r = list(r)
            r[0] = _ser_dt(r[0])
            r[1] = float((datetime.strptime(r[1], "%Y-%m-%d") - datetime(1899, 12, 30)).days)
            outg.append(r)
        return outg

    ch_board_live = _serialize_live(CH_BOARD)
    book = StubBook({"_Selection_Log": ch_board_live, "_Run_Log": [RUNLOG_HDR],
                     "_Shadow_Rank_Log": _serialize_shadow(p_grid)})
    od_lw = os.path.join(TMP, "p13_live_write")
    rc, so = _run_mod(m, book, "write", od_lw, None, p_px, rk="on", asof=CH_ASOF, ch="on")
    T("P13 live: the shadow read asks for UNFORMATTED_VALUE + SERIAL_NUMBER",
      ("get_all_values", "_Shadow_Rank_Log", (("date_time_render_option", "SERIAL_NUMBER"),
                                              ("value_render_option", "UNFORMATTED_VALUE"))) in book.calls,
      [c for c in book.calls if "_Shadow_Rank_Log" in c])
    T("P13 live: _Shadow_Rank_Log is READ ONLY (worksheet + one read, no write call on it)",
      [c[0] for c in book.calls if "_Shadow_Rank_Log" in c] == ["worksheet", "get_all_values"]
      and [c[0] for c in _writes(book)] == ["add_worksheet", "update", "append_row"]
      and all(c[1] != "_Shadow_Rank_Log" for c in _writes(book)), _writes(book))
    T("P13 live (serial 'Logged At' / 'Day') == TSV path: identical CHALLENGER block",
      _ch_block(od_lw) == blk_p, (_ch_block(od_lw)[3:], blk_p[3:]))
    rect = book.tabs["_Board_Scorecard"].values
    i_t = [k for k, r in enumerate(rect) if r[0] == m.CHALLENGER_TITLE]
    i_rk = [k for k, r in enumerate(rect) if r[0] == m.RANKING_TITLE]
    i_det = [k for k, r in enumerate(rect) if str(r[0]).startswith("DETAIL - ")]
    T("P13 live write: the tab carries CHALLENGER below RANKING HEALTH and above DETAIL; meta suffix",
      len(i_t) == 1 and len(i_rk) == 1 and len(i_det) == 1 and i_rk[0] < i_t[0] < i_det[0]
      and rect[i_t[0] + 1][:len(m.CHALLENGER_HEADER)] == m.CHALLENGER_HEADER
      and [str(c) for c in rect[i_t[0] + 2][:9]] == blk_p[1]
      and [str(c) for c in rect[i_t[0] + 3][:9]] == blk_p[2]
      and [str(c) for c in rect[i_t[0] + 4][:2]] == blk_p[3][:2]
      and rect[0][0].endswith(" | refs 1/1 | ranking INSUFFICIENT_DATA | challenger PROMOTE_ELIGIBLE"),
      (i_rk, i_t, i_det, rect[0][0][-110:]))
    rl = book.tabs["_Run_Log"].values[-1]
    dj = json.loads(rl[9])
    T("P13 live write: _Run_Log message ends ' | challenger: PROMOTE_ELIGIBLE (days 25, scored 125/125)'; "
      "details JSON carries the challenger object",
      rl[4] == "OK" and rl[5].endswith(" | challenger: PROMOTE_ELIGIBLE (days 25, scored 125/125)")
      and dj["challenger"]["verdict"] == "PROMOTE_ELIGIBLE" and dj["challenger"]["days"] == 25
      and dj["challenger"]["champion"]["scored"] == 125 and dj["challenger"]["challenger"]["hit"] == 60.0
      and dj["challenger"]["champion"]["hit"] == 40.0 and dj["challenger"]["challenger"]["churn"] == 5.0,
      (rl[5][-90:], dj.get("challenger")))
    T("P13 live write: final stdout line ends ' challenger=PROMOTE_ELIGIBLE'",
      rc == 0 and so.strip().splitlines()[-1].endswith(" challenger=PROMOTE_ELIGIBLE"), so.strip()[-90:])
    digests.append(_h(rect))

    # live, tab missing -> NO_DATA silently; a failed read -> WARN + NO_DATA
    book = StubBook({"_Selection_Log": ch_board_live, "_Run_Log": [RUNLOG_HDR]})
    rc, so = _run_mod(m, book, "observe", os.path.join(TMP, "p13_live_missing"), None, p_px, rk="on",
                      asof=CH_ASOF, ch="on")
    T("P13 live: no _Shadow_Rank_Log tab -> NO_DATA, no WARN, rc 0, zero writes",
      rc == 0 and "WARN" not in so and so.strip().splitlines()[-1].endswith(" challenger=NO_DATA")
      and _writes(book) == [], so.strip()[-80:])

    class _BrokenShadowBook(StubBook):
        def worksheet(self, title):
            if title == "_Shadow_Rank_Log":
                self.calls.append(("worksheet", title))
                raise RuntimeError("quota exceeded")
            return StubBook.worksheet(self, title)

    book = _BrokenShadowBook({"_Selection_Log": ch_board_live, "_Run_Log": [RUNLOG_HDR]})
    rc, so = _run_mod(m, book, "observe", os.path.join(TMP, "p13_live_broken"), None, p_px, rk="on",
                      asof=CH_ASOF, ch="on")
    T("P13 live: a failed shadow read -> one WARN line, NO_DATA, the board still scored, rc 0",
      rc == 0 and "WARN: _Shadow_Rank_Log not read (quota exceeded) - challenger NO_DATA" in so
      and so.strip().splitlines()[-1].endswith(" challenger=NO_DATA") and "episodes=2" in so,
      so.strip()[-120:])

    # fetch path: the shadow top-5 names and their references join the fetch;
    # ranks 6-10, BOARD rows and the later same-day event never do
    p_prices = m.load_prices_csv(p_px)
    fetched = []
    real_fetch = m.fetch_yahoo

    def _fake_fetch(symbols, frm, asof_utc, pause=0.25):
        fetched.append((list(symbols), frm))
        return {s_: list(p_prices[s_]) for s_ in symbols if s_ in p_prices}

    m.fetch_yahoo = _fake_fetch
    try:
        od_f = os.path.join(TMP, "p13_fetch_on")
        rc, so = _run_mod(m, None, "observe", od_f, ch_board_tsv, None, rk="on", asof=CH_ASOF, ch="on",
                          shadow=p_sh, fetch=True)
        syms_on, frm_on = fetched[-1]
        rc_off, so_off = _run_mod(m, None, "observe", os.path.join(TMP, "p13_fetch_off"), ch_board_tsv,
                                  None, rk="on", asof=CH_ASOF, ch="off", shadow=p_sh, fetch=True)
        syms_off, frm_off = fetched[-1]
    finally:
        m.fetch_yahoo = real_fetch
    shadow_priced = sorted({s_ for o in p_exp.values() for (s_, _j, _g, _gr) in o})
    T("P13 fetch (gate on): board + SPUS + refs, then the shadow top-5 names - one fetch, no duplicates",
      rc == 0 and syms_on == ["BRD1.US", "BRD2.US", "SPUS.US", "XLF.US"] + shadow_priced
      and len(set(syms_on)) == len(syms_on) and frm_on == "2026-08-22", (syms_on[:6], len(syms_on), frm_on))
    T("P13 fetch (gate on): no rank 6-10, BOARD or later-event name is ever fetched",
      not (set(syms_on) & p_unp) and len(p_unp) > 0, sorted(set(syms_on) & p_unp)[:5])
    T("P13 fetch (gate off): the v1.1.0 list exactly (board + SPUS + refs)",
      rc_off == 0 and syms_off == ["BRD1.US", "BRD2.US", "SPUS.US", "XLF.US"] and frm_off == "2026-08-22",
      syms_off)
    T("P13 fetch path == CSV path: identical CHALLENGER block", _ch_block(od_f) == blk_p,
      _ch_block(od_f)[3:])

    # additivity: gate on minus the CHALLENGER block == gate off (same inputs)
    od_off = os.path.join(TMP, "p13_P_off")
    rc, so_off = _run_mod(m, None, "observe", od_off, ch_board_tsv, p_px, rk="on", asof=CH_ASOF, ch="off",
                          shadow=p_sh)
    a_on, a_off = _artifacts(od_p), _artifacts(od_off)
    sum_on, sum_off = (a_on["board_scorecard_summary.csv"].decode("utf-8"),
                       a_off["board_scorecard_summary.csv"].decode("utf-8"))
    T("P13 additive: DETAIL CSV byte-identical with the gate on or off",
      a_on["board_scorecard_detail.csv"] == a_off["board_scorecard_detail.csv"])
    T("P13 additive: summary CSV (on) == summary CSV (off) + one blank row + the CHALLENGER block",
      sum_on.startswith(sum_off)
      and list(csv.reader(io.StringIO(sum_on[len(sum_off):]))) == [[]] + blk_p, len(sum_on) - len(sum_off))
    md_on, md_off = a_on["board_scorecard.md"].decode("utf-8"), a_off["board_scorecard.md"].decode("utf-8")
    T("P13 additive: MD (on, meta suffix removed) starts with MD (off); the rest is the CHALLENGER table",
      md_on.replace(" | challenger PROMOTE_ELIGIBLE", "", 1).startswith(md_off)
      and md_on.replace(" | challenger PROMOTE_ELIGIBLE", "", 1)[len(md_off):].startswith(
          "\n" + m.CHALLENGER_TITLE + "\n"), md_on[len(md_off) - 20:len(md_off) + 60])
    rects, logs = {}, {}
    for gate in ("on", "off"):
        book = StubBook({"_Selection_Log": ch_board_live, "_Run_Log": [RUNLOG_HDR],
                         "_Shadow_Rank_Log": _serialize_shadow(p_grid)})
        _run_mod(m, book, "write", os.path.join(TMP, "p13_add_w_" + gate), None, p_px, rk="on",
                 asof=CH_ASOF, ch=gate)
        rects[gate] = [list(r) for r in book.tabs["_Board_Scorecard"].values]
        logs[gate] = list(book.tabs["_Run_Log"].values[-1])
        if gate == "off":
            T("P13 gate off: the shadow tab is not even opened",
              [c for c in book.calls if "_Shadow_Rank_Log" in c] == [], book.calls[:4])
    k_t = [k for k, r in enumerate(rects["on"]) if r[0] == m.CHALLENGER_TITLE][0]
    stripped = rects["on"][:k_t - 1] + rects["on"][k_t + 5:]       # blank, title, header, 2 orders, verdict
    stripped[0] = [stripped[0][0].replace(" | challenger PROMOTE_ELIGIBLE", "", 1)] + stripped[0][1:]
    T("P13 additive: rectangle (on) minus the 6-row CHALLENGER block == rectangle (off)",
      stripped == rects["off"], (len(rects["on"]), len(rects["off"])))
    msg_on = re.sub(r"wrote=\d+x\d+", "wrote=*", logs["on"][5])
    msg_off = re.sub(r"wrote=\d+x\d+", "wrote=*", logs["off"][5])
    T("P13 additive: _Run_Log message (on) == (off) + ' | challenger: ...'; details JSON only gains the key",
      msg_on == msg_off + " | challenger: PROMOTE_ELIGIBLE (days 25, scored 125/125)"
      and {k: v for k, v in json.loads(logs["on"][9]).items() if k != "challenger"}
      == json.loads(logs["off"][9]), (msg_on[-120:], msg_off[-60:]))

    # ------------------------------------------------------------------ P14
    rc, so = _call(m.selftest)
    names = [l for l in open(SSL_FILE, encoding="utf-8").read().splitlines()
             if l.strip().startswith("# 22:") or l.strip().startswith("# 23:")
             or l.strip().startswith("# 24:")]
    T("P14 selftest() grew 21 -> 24 with the three v1.2.0 cases (shadow parsing, shadow base price, verdict)",
      rc == 0 and "SELFTEST 24/24: ALL GREEN" in so and len(names) == 3
      and "shadow log parsing" in names[0] and "shadow episodes" in names[1]
      and "pre-registered verdict" in names[2], names)
    if base_v11 is not None:
        rc_b, so_b = _call(base_v11.selftest)
        T("P14 base v1.1.0 selftest is 21/21 (the count rose by exactly the new cases)",
          rc_b == 0 and "SELFTEST 21/21: ALL GREEN" in so_b, so_b.strip())

    shutil.rmtree(TMP, ignore_errors=True)
    digest = hashlib.sha256(("\n".join(out) + json.dumps(digests)).encode()).hexdigest()[:16]
    print("\n".join(out))
    print("RESULT %d PASS / %d FAIL, digest %s" % (sum(1 for l in out if l.startswith("PASS")),
                                                  sum(1 for l in out if l.startswith("FAIL")), digest))



def test_all():
    result = subprocess.run([sys.executable, str(Path(__file__).resolve())], cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr


if __name__ == "__main__":
    main()
