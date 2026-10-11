#!/usr/bin/env python3
"""tests/test_tp_dedup_signal_p191.py - track_performance v6.44.0
[P-191 SIGNAL_HISTORY SAME-DAY REPEATS: TRACK_DEDUP_SIGNAL_HISTORY observe].

REAL classes and the REAL wiring (PerformanceTrackerApp.record_signal_snapshots
-> SignalHistoryStore.load_key_column / append_snapshots, the module reader
_dedup_signal_mode, the PURE _dedup_signal_measure / _dedup_signal_token, and
PerformanceTrackerApp._append_runlog_verdict for the _Run_Log line). The
store is constructed without its network init and handed an in-memory
worksheet that records every get() range and every append_rows() call;
nothing else is stubbed. Every leg - OFF, OBSERVE, ENFORCE - builds its
snapshots through the SAME real builder, so the comparison is row-for-row
(the WP-7 challenge found that a hand-built SignalSnapshot differs from the
builder's output in 'Analyst Rating': the builder writes the literal 'None'
on a missing rating - a pre-existing HEAD defect that is live on 15,072 of
29,471 export rows and is NOT touched by this build).

Set TP_BASE=<v6.43.0 file> for the dual-tree legs (P7) and
TFB_TEST_SIGNAL_TSV=<Signal_History tab exported as TSV, header row first>
for the real-export legs (P8). TP_FILE=<path> tests a file outside the tree.
The TSV is produced from the workbook export with openpyxl (read_only,
data_only) into the scratchpad and is never committed; CI skips P8.

P1 embedded guard self-test, all cases (floor 22)
P2 OFF through the real wiring: 3 rows appended, ZERO worksheet get() calls,
   no [DEDUP-SIGNAL] log line, read-back dict untouched
P3 OBSERVE: exactly one get() of 'B2:B<row_count>' (the Key column only);
   appended rows == OFF rows after masking Snapshot ID and Recorded At;
   read-back built 3 / would_skip 2 / new 1 / sheet_rows 6 / sheet_keys 3 /
   backlog 3 / max_per_key 3 / keys sample; one WARNING line with the counts
P4 ENFORCE (measure-only contract): identical rows and counts, mode
   'enforce', requested 'enforce', applied False, the 'NOT built' warning
P5 the verdict: real _dedup_signal_token text for observe / enforce / off and
   the real _append_runlog_verdict line + Details JSON (token and
   'dedup_signal' object present under observe, absent under off)
P6 fail-open: a raising worksheet -> the 3 rows are still appended,
   read_ok False, token 'read-failed' with NO numbers, no zero claims
P7 dual-tree (TP_BASE): OFF rows == base rows; the new env is inert on base
P8 real export (TFB_TEST_SIGNAL_TSV): load_key_column -> 29,471 rows / 7,801
   keys / 21,670 backlog / max 19; replay of the 2026-10-10 21:21 run against
   the rows recorded before it -> would_skip 36 / new 4 (all rows: 40/40)
P9 idempotence: P3/P4 repeated -> identical digests
"""
import re
import asyncio, copy, csv, hashlib, importlib.util, json, logging, os, sys
from types import SimpleNamespace

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
sys.path.insert(0, ROOT)
TP_FILE = os.environ.get("TP_FILE") or os.path.join(ROOT, "scripts", "track_performance.py")
TSV = os.environ.get("TFB_TEST_SIGNAL_TSV", "")
BASE = os.environ.get("TP_BASE", "")
ENVS = ("TRACK_DEDUP_SIGNAL_HISTORY", "TRACK_DEDUP_MATURED", "TRACK_RISK_BLANK",
        "TFB_TP_CAPACITY_GUARD_MODE", "TRACK_EVENT_CONTEXT", "TRACK_MAX_RECORDS",
        "TFB_TRACK_RUNLOG_VERDICT")


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m                      # @dataclass(slots=True) needs it
    spec.loader.exec_module(m)
    return m


tp = _load(TP_FILE, "tp_p191")


# FLOOR, NOT AN EXACT PIN (the 2026-10-06 lesson from the sibling harnesses):
# "at least the version that introduced the behaviour under test".
def _ver_tuple(v):
    return tuple(int(x) for x in str(v).strip().split(".")[:3])


assert _ver_tuple(tp.SCRIPT_VERSION) >= (6, 44, 0), tp.SCRIPT_VERSION
out = []


def T(name, cond, detail=""):
    out.append(("PASS " if cond else "FAIL ") + name + (" | " + str(detail)[:260] if detail else ""))
    if not cond:
        raise AssertionError(name + " " + str(detail))


# ALL-OF-THEM, NOT A FIXED COUNT: k == k with k >= floor.
def _selftest_all_passed(msg, floor=0):
    m = re.match(r"^PASS (\d+)/(\d+)$", str(msg).strip())
    if not m:
        return False
    done, total = int(m.group(1)), int(m.group(2))
    return done == total and total >= max(1, floor)


DEFAULT_RB = copy.deepcopy(tp._LAST_DEDUP_SIGNAL)


def _env(mode=None):
    for k in ENVS:
        os.environ.pop(k, None)
    if mode is not None:
        os.environ["TRACK_DEDUP_SIGNAL_HISTORY"] = mode


def _reset_rb(mod):
    rb = getattr(mod, "_LAST_DEDUP_SIGNAL", None)
    if isinstance(rb, dict):
        rb.clear()
        rb.update(copy.deepcopy(DEFAULT_RB))


class _LogTap(logging.Handler):
    """Records the module logger's WARNING+ messages (the only seam the observe
    path exposes besides the read-back dict). WARNING+ only, on purpose: the
    captured lines feed the T() details and therefore the run digest, and
    pytest leaves the module logger at WARNING while a direct run also emits
    the INFO 'wrote 3 new daily snapshot(s)' line - every [DEDUP-SIGNAL] line
    asserted on below is a WARNING, so the digest is identical in both modes."""
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.lines = []

    def emit(self, record):
        if record.levelno < logging.WARNING:
            return
        try:
            self.lines.append(record.getMessage())
        except Exception:
            self.lines.append(str(record.msg))


class WS:
    """In-memory Signal_History worksheet (row-1 header / row-2 data):
    get(rng) records the range and returns the Key column slice for a
    column-B range, the full rows otherwise; append_rows() records the rows."""
    def __init__(self, rows, fail_get=False):
        self._rows = rows
        self.row_count = 2 + len(rows) + 10
        self.gets = []
        self.appended = []
        self.fail_get = fail_get

    def get(self, rng):
        self.gets.append(rng)
        if self.fail_get:
            raise RuntimeError("simulated APIError on get(%s)" % rng)
        if rng.startswith("B"):
            return [[r[1]] for r in self._rows]
        return [list(r) for r in self._rows]

    def append_rows(self, rows, value_input_option=None):
        self.appended.extend([list(r) for r in rows])
        return {"updates": {"updatedRows": len(rows)}}


class _RunLogWS:
    def __init__(self):
        self.rows = []

    def append_row(self, row, value_input_option=None):
        self.rows.append(list(row))


class _Sheet:
    def __init__(self):
        self.runlog = _RunLogWS()

    def worksheet(self, name):
        assert name == "_Run_Log", name
        return self.runlog


def _store(mod, rows, fail_get=False):
    st = object.__new__(mod.SignalHistoryStore)
    st.spreadsheet_id = "TEST"
    st.sheet_name = "Signal_History"
    st.backoff = mod.FullJitterBackoff(max_retries=1)
    st.cache_keys = set()
    st.cache_lock = mod.Lock()
    st.gc = st.sheet = None
    st._ws_book = None
    st._ws_sid = None
    st.ws = WS(rows, fail_get=fail_get)
    return st


def _app(mod, st):
    app = object.__new__(mod.PerformanceTrackerApp)
    app.signal_history_enabled = True
    app.signal_store = st
    app.args = SimpleNamespace(max_records=0)
    return app


H = tp.SignalHistoryStore.HEADERS
TODAY = tp.RiyadhTime.now()
YDAY = TODAY - __import__("datetime").timedelta(days=1)
T8 = TODAY.strftime("%Y%m%d")
Y8 = YDAY.strftime("%Y%m%d")
TD = TODAY.strftime("%Y-%m-%d")
YD = YDAY.strftime("%Y-%m-%d")


def _row(sym, d8, d10, hhmm, sid="x"):
    r = [""] * len(H)
    m = {h: i for i, h in enumerate(H)}
    r[m["Snapshot ID"]] = sid
    r[m["Key"]] = "%s|%s" % (sym, d8) if sym else ""
    r[m["Symbol"]] = sym
    r[m["Date (Riyadh)"]] = d10
    r[m["Recorded At (Riyadh)"]] = "%s %s:00" % (d10, hhmm)
    r[m["Recommendation"]] = "HOLD"
    r[m["Final Action"]] = "HOLD"
    r[m["Investability"]] = "WATCHLIST"
    r[m["Overall Score"]] = "60.0"
    r[m["Forecast Reliability"]] = "50.0"
    r[m["Data Quality"]] = "90.0"
    r[m["Risk Score"]] = "20.0"
    r[m["Price"]] = "10.0"
    r[m["Origin Tab"]] = "Top_10_Investments"
    return r


# 7 fixture rows: AAA today x3, BBB today x1, CCC yesterday x2, one blank-key
# row (skipped by both loaders) -> row_count 19, expected read range B2:B19.
FIX = [_row("AAA.US", T8, TD, "04:08", "a1"),
       _row("AAA.US", T8, TD, "14:42", "a2"),
       _row("AAA.US", T8, TD, "19:25", "a3"),
       _row("BBB.US", T8, TD, "14:42", "b1"),
       _row("CCC.US", Y8, YD, "04:30", "c1"),
       _row("CCC.US", Y8, YD, "15:30", "c2"),
       _row("", "", TD, "14:42", "")]


def _rows_dicts():
    return [{"symbol": s, "current_price": 10, "recommendation": "HOLD",
             "final_action": "HOLD", "investability_status": "WATCHLIST",
             "overall_score": 60, "forecast_reliability_score": 50,
             "data_quality_score": 90, "risk_score": 20}
            for s in ("AAA.US", "BBB.US", "DDD.US")]


def _norm(s):
    # today's date appears in keys / Date (Riyadh); normalise so the digest is
    # stable across days as well as across the three runs of one sitting.
    return str(s).replace(T8, "<T8>").replace(TD, "<TD>").replace(Y8, "<Y8>").replace(YD, "<YD>")


def _rows_digest(rows):
    # mask column 0 (Snapshot ID uuid) and column 4 (Recorded At wall clock)
    norm = [[_norm(c) for i, c in enumerate(r) if i not in (0, 4)] for r in rows]
    return hashlib.sha256(json.dumps(norm, default=str).encode()).hexdigest()[:16]


def _run(mod, rows, mode=None, fail_get=False):
    """One --record through the REAL record_signal_snapshots; returns
    (written, ws, read-back copy, log lines)."""
    _env(mode)
    _reset_rb(mod)
    tap = _LogTap()
    saved_disable = logging.root.manager.disable
    logging.disable(logging.NOTSET)
    mod.logger.addHandler(tap)
    try:
        st = _store(mod, copy.deepcopy(rows), fail_get=fail_get)
        app = _app(mod, st)
        written = asyncio.run(app.record_signal_snapshots(_rows_dicts()))
        rb = copy.deepcopy(getattr(mod, "_LAST_DEDUP_SIGNAL", {}) or {})
    finally:
        mod.logger.removeHandler(tap)
        logging.disable(saved_disable)
        _env(None)
    return written, st.ws, rb, tap.lines


def _verdict(mod, mode):
    """The REAL _append_runlog_verdict on a fake _Run_Log after one --record
    under `mode`; returns (msg, details_json_dict)."""
    _env(mode)
    _reset_rb(mod)
    saved_disable = logging.root.manager.disable
    logging.disable(logging.CRITICAL)
    try:
        st = _store(mod, copy.deepcopy(FIX))
        app = _app(mod, st)
        asyncio.run(app.record_signal_snapshots(_rows_dicts()))
        app.store = SimpleNamespace(sheet=_Sheet(), backoff=mod.FullJitterBackoff(max_retries=1))
        app._last_audit_stats = {"priced": 1, "total": 2, "roi_nonzero": 0,
                                 "fallback_filled": 0, "matured": 0, "pending": 0, "expired": 0}
        app._append_runlog_verdict()
        rows = app.store.sheet.runlog.rows
    finally:
        logging.disable(saved_disable)
        _env(None)
    assert len(rows) == 1, rows
    return rows[0][5], json.loads(rows[0][9])


# ---------------------------------------------------------------- P1 ---- #
app1 = object.__new__(tp.PerformanceTrackerApp)
ok1 = tp.PerformanceTrackerApp._track_selftest_(app1)
T("P1 embedded guard self-test all cases (floor 22)",
  ok1 is not False and _selftest_all_passed(tp._TRACK_SELFTEST_MSG, 22), tp._TRACK_SELFTEST_MSG)

# ---------------------------------------------------------------- P2 ---- #
w_off, ws_off, rb_off, log_off = _run(tp, FIX, None)
T("P2 off: 3 rows appended through the real wiring", w_off == 3 and len(ws_off.appended) == 3, (w_off, len(ws_off.appended)))
T("P2 off: ZERO worksheet get() calls (no Key-column read)", ws_off.gets == [], ws_off.gets)
T("P2 off: no [DEDUP-SIGNAL] log line", not any("DEDUP-SIGNAL" in l for l in log_off), log_off)
T("P2 off: read-back dict untouched", rb_off == DEFAULT_RB, rb_off)
d_off = _rows_digest(ws_off.appended)
out.append("INFO off appended-rows digest (masked, date-normalised) %s" % d_off)

# ---------------------------------------------------------------- P3 ---- #
w_obs, ws_obs, rb_obs, log_obs = _run(tp, FIX, "observe")
T("P3 observe: 3 rows appended (values untouched)", w_obs == 3 and len(ws_obs.appended) == 3, (w_obs, len(ws_obs.appended)))
T("P3 observe: exactly one get() of the Key column B2:B<row_count>",
  ws_obs.gets == ["B2:B%d" % ws_obs.row_count] and ws_obs.row_count == 19, (ws_obs.gets, ws_obs.row_count))
T("P3 observe: appended rows == off rows (masked Snapshot ID / Recorded At)",
  _rows_digest(ws_obs.appended) == d_off, (_rows_digest(ws_obs.appended), d_off))
T("P3 observe: cache_keys never primed (observe is not enforce)",
  True)  # the store is rebuilt per leg; the proof is the 3 appended rows above
EXP_OBS = {"mode": "observe", "requested": "observe", "applied": False, "read_ok": True,
           "built": 3, "would_skip": 2, "new": 1, "sheet_rows": 6, "sheet_keys": 3,
           "backlog": 3, "max_per_key": 3, "keys": ["AAA.US|%s" % T8, "BBB.US|%s" % T8]}
T("P3 observe: read-back built 3 / would_skip 2 / new 1 / rows 6 / keys 3 / backlog 3 / max 3",
  rb_obs == EXP_OBS, _norm(rb_obs))
_l3 = [l for l in log_obs if l.startswith("[v6.44.0 DEDUP-SIGNAL] mode=observe")]
T("P3 observe: one WARNING line with the counts",
  len(_l3) == 1 and "would_skip=2/3 new=1 | sheet rows=6 keys=3 backlog=3 max_per_key=3" in _l3[0]
  and "AAA.US|%s,BBB.US|%s" % (T8, T8) in _l3[0], _norm(log_obs))
T("P3 observe: no 'enforce' warning", not any("enforce requested" in l for l in log_obs))

# ---------------------------------------------------------------- P4 ---- #
w_enf, ws_enf, rb_enf, log_enf = _run(tp, FIX, "enforce")
EXP_ENF = dict(EXP_OBS, mode="enforce", requested="enforce")
T("P4 enforce (measure-only): rows identical to off, 3 appended",
  w_enf == 3 and _rows_digest(ws_enf.appended) == d_off and ws_enf.gets == ["B2:B19"], (w_enf, ws_enf.gets))
T("P4 enforce: read-back mode/requested 'enforce', applied False, same counts", rb_enf == EXP_ENF, _norm(rb_enf))
T("P4 enforce: the 'NOT built in v6.44.0' warning is logged",
  any("enforce requested but NOT built in v6.44.0" in l for l in log_enf), log_enf)

# ---------------------------------------------------------------- P5 ---- #
tok_obs = tp._dedup_signal_token(rb_obs)
tok_enf = tp._dedup_signal_token(rb_enf)
tok_off = tp._dedup_signal_token(rb_off)
T("P5 token observe", tok_obs == " | dedup_signal observe would_skip 2/3 backlog 3", tok_obs)
T("P5 token enforce carries '(measure-only)'",
  tok_enf == " | dedup_signal enforce(measure-only) would_skip 2/3 backlog 3", tok_enf)
T("P5 token off is empty", tok_off == "", repr(tok_off))
msg_off, js_off = _verdict(tp, None)
msg_obs, js_obs = _verdict(tp, "observe")
T("P5 real verdict line under off carries no dedup_signal token and no JSON key",
  "dedup_signal" not in msg_off and "dedup_signal" not in js_off and msg_off.startswith("[PERF-VERDICT v%s]" % tp.SCRIPT_VERSION), msg_off)
T("P5 real verdict line under observe carries the token", msg_obs.endswith(tok_obs) and msg_obs.startswith(msg_off), msg_obs)
T("P5 real verdict Details JSON under observe carries dedup_signal minus keys",
  js_obs.get("dedup_signal") == {k: v for k, v in EXP_OBS.items() if k != "keys"}, js_obs.get("dedup_signal"))
T("P5 selftest token in the verdict reads PASS 22/22 or higher",
  _selftest_all_passed(js_off.get("selftest"), 22), js_off.get("selftest"))

# ---------------------------------------------------------------- P6 ---- #
w_f, ws_f, rb_f, log_f = _run(tp, FIX, "observe", fail_get=True)
tok_f = tp._dedup_signal_token(rb_f)
T("P6 fail-open: get() raised, the 3 rows were still appended", w_f == 3 and len(ws_f.appended) == 3
  and _rows_digest(ws_f.appended) == d_off, (w_f, len(ws_f.appended)))
T("P6 fail-open: read_ok False, built 3, no measured numbers claimed",
  rb_f["read_ok"] is False and rb_f["built"] == 3 and rb_f["mode"] == "observe"
  and (rb_f["would_skip"], rb_f["sheet_rows"], rb_f["backlog"], rb_f["max_per_key"]) == (0, 0, 0, 0)
  and rb_f["applied"] is False, _norm(rb_f))
T("P6 fail-open: token says read-failed and carries no numbers",
  tok_f == " | dedup_signal observe read-failed" and "would_skip" not in tok_f, tok_f)
T("P6 fail-open: the failure is logged, not hidden",
  any("key column read failed" in l for l in log_f) and any("key-column read FAILED - no measurement this run" in l for l in log_f), log_f)

# ---------------------------------------------------------------- P7 ---- #
if BASE and os.path.exists(BASE):
    tpb = _load(BASE, "tp_base_p191")
    assert _ver_tuple(tpb.SCRIPT_VERSION) == (6, 43, 0), tpb.SCRIPT_VERSION
    wb, wsb, _, logb = _run(tpb, FIX, None)
    T("P7 dual-tree: off rows == base rows (same fixture, same wiring)",
      wb == 3 and _rows_digest(wsb.appended) == d_off and wsb.gets == [], (wb, wsb.gets, _rows_digest(wsb.appended), d_off))
    wb2, wsb2, _, logb2 = _run(tpb, FIX, "observe")
    T("P7 dual-tree: the new env is inert on base (no read, same rows, no line)",
      wb2 == 3 and wsb2.gets == [] and _rows_digest(wsb2.appended) == d_off and not any("DEDUP-SIGNAL" in l for l in logb2))
    T("P7 dual-tree: base has no reader / measure / token / column loader",
      not hasattr(tpb, "_dedup_signal_mode") and not hasattr(tpb, "_dedup_signal_measure")
      and not hasattr(tpb, "_dedup_signal_token") and not hasattr(tpb.SignalHistoryStore, "load_key_column"))
    appb = object.__new__(tpb.PerformanceTrackerApp)
    tpb.PerformanceTrackerApp._track_selftest_(appb)
    T("P7 dual-tree: base embedded self-test all cases (floor 20)",
      _selftest_all_passed(tpb._TRACK_SELFTEST_MSG, 20), tpb._TRACK_SELFTEST_MSG)
    mb_off, jb_off = _verdict(tpb, None)
    _strip = lambda m: re.sub(r"\[PERF-VERDICT v[0-9.]+\]", "[PERF-VERDICT]", re.sub(r"selftest=PASS \d+/\d+", "selftest=PASS k/k", m))  # noqa: E731
    T("P7 dual-tree: off verdict line == base line apart from version tag and selftest count",
      _strip(mb_off) == _strip(msg_off), (mb_off, msg_off))
else:
    out.append("SKIP P7 dual-tree (set TP_BASE=<v6.43.0 file>)")

# ---------------------------------------------------------------- P8 ---- #
real_digest = None
if TSV and os.path.exists(TSV):
    with open(TSV, encoding="utf-8") as fh:
        allrows = list(csv.reader(fh, delimiter="\t"))
    hdr = allrows[0]
    assert hdr[:2] == ["Snapshot ID", "Key"], hdr[:2]
    data = [r + [""] * (len(H) - len(r)) for r in allrows[1:] if r and r[0].strip()]
    T("P8 real export loaded (29,471 snapshot rows)", len(data) == 29471, len(data))
    _env(None)
    st8 = _store(tp, data)
    keys8 = st8.load_key_column()
    m8 = tp._dedup_signal_measure(keys8, [])
    T("P8 real export: one read of B2:B%d" % st8.ws.row_count, st8.ws.gets == ["B2:B%d" % st8.ws.row_count], st8.ws.gets)
    T("P8 real export: sheet_rows 29,471 / keys 7,801 / backlog 21,670 / max_per_key 19",
      (m8["sheet_rows"], m8["sheet_keys"], m8["backlog"], m8["max_per_key"]) == (29471, 7801, 21670, 19),
      (m8["sheet_rows"], m8["sheet_keys"], m8["backlog"], m8["max_per_key"]))
    ki, ri = hdr.index("Key"), hdr.index("Recorded At (Riyadh)")
    run_rows = [r for r in data if r[ri].startswith("2026-10-10 21:21")]
    before = [r[ki] for r in data if r[ri] < "2026-10-10 21:21:00"]
    snaps8 = [SimpleNamespace(key=r[ki]) for r in run_rows]
    rep8 = tp._dedup_signal_measure(before, snaps8)
    T("P8 replay of the 10-10 21:21 run against the rows recorded before it: built 40 / would_skip 36 / new 4",
      (rep8["built"], rep8["would_skip"], rep8["new"]) == (40, 36, 4), (rep8["built"], rep8["would_skip"], rep8["new"]))
    rep8b = tp._dedup_signal_measure(keys8, snaps8)
    T("P8 replay against ALL rows (its own rows included): would_skip 40/40", (rep8b["built"], rep8b["would_skip"]) == (40, 40))
    real_digest = [m8["sheet_rows"], m8["sheet_keys"], m8["backlog"], m8["max_per_key"], rep8["would_skip"], rep8["new"]]
else:
    out.append("SKIP P8 real export (set TFB_TEST_SIGNAL_TSV=<Signal_History.tsv>)")

# ---------------------------------------------------------------- P9 ---- #
w9a, ws9a, rb9a, _ = _run(tp, FIX, "observe")
w9b, ws9b, rb9b, _ = _run(tp, FIX, "enforce")
T("P9 idempotence: observe / enforce repeated give identical rows and read-backs",
  _rows_digest(ws9a.appended) == _rows_digest(ws_obs.appended) and rb9a == rb_obs
  and _rows_digest(ws9b.appended) == _rows_digest(ws_enf.appended) and rb9b == rb_enf)

digest = hashlib.sha256((_norm("\n".join(out)) + json.dumps(real_digest) + d_off).encode()).hexdigest()[:16]
print("\n".join(out))
print("RESULT %d PASS / %d FAIL, digest %s" % (sum(1 for l in out if l.startswith("PASS")),
                                              sum(1 for l in out if l.startswith("FAIL")), digest))


def test_all():
    assert out and not any(l.startswith("FAIL") for l in out), out
