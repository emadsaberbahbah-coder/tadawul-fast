"""track_performance v6.50.0 [LOG-READ] -- Performance_Log numbers read back at
full precision (TRACK_LOG_UNFORMATTED_READ = off (default) | observe | enforce).

Zero network. A fake worksheet holds the rows exactly as the tracker writes
them (RAW, full float precision) and renders numbers as their displayed text
('.10g', the Sheets Automatic format) unless UNFORMATTED_VALUE is requested,
which is what the Sheets API does. Records come from the real writer
(record_from_top10, canonical v2 targets) over 40 seeded source rows x 4
horizons; the clock and record ids are pinned so every run is identical.

  L1  version floor >= (6, 50, 0); the mode parser takes exact words only
  L2  OFF (unset, off, 1, yes, a typo): one get(rng) with no keyword, no
      LOG-READ line, records equal the base's (or the recorded base digest);
      the formatted read loses 1W/2W canonical targets and no 1M/3M one -
      the defect this build exists for, pinned so L3 is not vacuous
  L3  GOLDEN NEGATIVE / ENFORCE: one get with exactly UNFORMATTED_VALUE +
      FORMATTED_STRING, zero schema_price_mismatch, every reloaded canonical
      equals the in-memory one, strings / dates / blanks parse as off
  L4  persistence: enforce load -> save_records -> enforce load is exact;
      an off load -> save keeps the rounding (the documented residual)
  L5  observe: records equal off; one extra unformatted read; one WARNING
      with the counts; a percent number format is named as divergent; a
      failed extra read logs and changes nothing
  L6  the real gspread Worksheet.get / HTTPClient.values_get: off sends no
      render option, enforce sends valueRenderOption=UNFORMATTED_VALUE and
      dateTimeRenderOption=FORMATTED_STRING (skipped without gspread)
  L7  ASCII added lines; zero top-level functions removed (needs the base)

The module under test is scripts/track_performance.py, or the file named by
TFB_TRACK_UNDER_TEST (golden negative: point it at the untouched base copy,
git show 08a15bc0:scripts/track_performance.py). The base for L2 / L7 is
TFB_TRACK_BASE, else `git show 08a15bc0:scripts/track_performance.py` when
git has it; without it L2 checks the recorded digest and L7 skips.

Run: python tests/test_tracker_log_read_precision.py  (prints PASS k/k)
 or: python -m pytest -q tests/test_tracker_log_read_precision.py
"""
from __future__ import annotations

import asyncio
import contextlib
import copy
import hashlib
import importlib.util
import json
import logging
import os
import random
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
UNDER_TEST = Path(os.environ.get("TFB_TRACK_UNDER_TEST") or ROOT / "scripts" / "track_performance.py")
BASE_SHA = "08a15bc0"
ENV = "TRACK_LOG_UNFORMATTED_READ"
UNF = {"value_render_option": "UNFORMATTED_VALUE", "date_time_render_option": "FORMATTED_STRING"}
RIYADH = timezone(timedelta(hours=3))
NOW = datetime(2026, 10, 25, 8, 10, tzinfo=RIYADH)
# sha256[:16] of the base (v6.49.0) off load of the fixture: get-call log +
# _record_to_row of every loaded record. Proven against the base in L2 when
# the base is available, checked alone on a shallow CI checkout.
OFF_GOLDEN = "39dae323624dc93e"


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, str(path))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod          # dataclasses resolve their module by name
    old = list(sys.path)
    try:
        sys.path.insert(0, str(ROOT))
        spec.loader.exec_module(mod)
    finally:
        sys.path[:] = old
    return mod


TP = _load(UNDER_TEST, "tp_log_read_under_test")


def _base_source():
    env = os.environ.get("TFB_TRACK_BASE")
    if env and Path(env).is_file():
        return Path(env).read_text(encoding="utf-8")
    try:
        proc = subprocess.run(["git", "show", BASE_SHA + ":scripts/track_performance.py"],
                              cwd=str(ROOT), capture_output=True, text=True, timeout=60)
    except Exception:
        return None
    return proc.stdout if proc.returncode == 0 and proc.stdout else None


_BASE_MOD = None


def _base_module():
    global _BASE_MOD
    if _BASE_MOD is None:
        src = _base_source()
        if src is None:
            return None
        tmp = Path(tempfile.mkdtemp(prefix="tp_base_")) / "track_performance_base.py"
        tmp.write_text(src, encoding="utf-8")
        _BASE_MOD = _load(tmp, "tp_log_read_base")
    return _BASE_MOD


# ------------------------------------------------------------------ fakes
class ImmediateRetry:
    def execute_sync(self, operation):
        return operation()


class LogSheet:
    """Data rows of Performance_Log as RAW-written values; FORMATTED_VALUE
    renders numbers as '.10g' text (percent_cols as a percent number format),
    UNFORMATTED_VALUE returns the stored value."""

    def __init__(self, rows, headers, percent_cols=(), fail_unformatted=False, data_row0=6):
        self.rows = copy.deepcopy(rows)
        self.data_row0 = data_row0
        self.headers = list(headers)
        self.percent = {self.headers.index(c) for c in percent_cols}
        self.fail_unformatted = fail_unformatted
        self.row_count = 5000
        self.calls = []

    def _fmt(self, v, i):
        if isinstance(v, bool):
            return "TRUE" if v else "FALSE"
        if isinstance(v, (int, float)):
            if i in self.percent:
                return "%.2f%%" % (float(v) * 100.0)
            return format(v, ".10g")
        return v

    def get(self, range_name=None, **kw):
        self.calls.append((range_name, dict(kw)))
        if kw.get("value_render_option") == "UNFORMATTED_VALUE":
            if self.fail_unformatted:
                raise OSError("simulated unformatted read fault")
            return copy.deepcopy(self.rows)
        return [[self._fmt(v, i) for i, v in enumerate(r)] for r in self.rows]

    def batch_clear(self, ranges):
        self.rows = []

    def update(self, values=None, range_name=None, **kw):
        start = int("".join(ch for ch in range_name.split(":")[0] if ch.isdigit())) - self.data_row0
        while len(self.rows) < start + len(values):
            self.rows.append([])
        for k, row in enumerate(values):
            self.rows[start + k] = copy.deepcopy(list(row))


class _SignalSheet:
    rows = []
    row_count = 1000

    def get(self, range_name):
        return []

    def append_rows(self, rows, value_input_option="RAW"):
        return None


@contextlib.contextmanager
def _env(**values):
    old = {k: os.environ.get(k) for k in values}
    try:
        for k, v in values.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@contextlib.contextmanager
def _pinned(tp):
    """Pin the clock and record ids so the fixture is identical on every run."""
    counter = iter(range(1, 10 ** 6))
    saved = (tp.RiyadhTime.__dict__["now"], tp._utc_now, tp.uuid.uuid4)
    tp.RiyadhTime.now = classmethod(lambda cls: NOW)
    tp._utc_now = lambda: NOW.astimezone(timezone.utc)
    tp.uuid.uuid4 = lambda: "rid-%05d" % next(counter)
    try:
        yield
    finally:
        tp.RiyadhTime.now, tp._utc_now, tp.uuid.uuid4 = saved


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.lines = []

    def emit(self, record):
        self.lines.append((record.levelname, record.getMessage()))


@contextlib.contextmanager
def _captured(tp):
    h = _Capture()
    lg = tp.logger
    old_level = lg.level
    lg.addHandler(h)
    lg.setLevel(logging.DEBUG)
    try:
        yield h
    finally:
        lg.removeHandler(h)
        lg.setLevel(old_level)


def _source_rows(n=40, seed=20261025):
    rnd = random.Random(seed)
    out = []
    for i in range(n):
        price = round(rnd.uniform(5, 500), 2)
        r1 = rnd.uniform(-0.12, 0.25)
        r3 = r1 * rnd.uniform(1.4, 2.6)
        out.append({"symbol": "S%03d.US" % i, "current_price": price,
                    "forecast_price_1m": round(price * (1 + r1), 2), "expected_roi_1m": r1,
                    "forecast_price_3m": round(price * (1 + r3), 2), "expected_roi_3m": r3,
                    "recommendation": "BUY", "final_action": "INVEST"})
    return out


def _records(tp):
    store = object.__new__(tp.SignalHistoryStore)
    store.ws, store.sheet, store.spreadsheet_id = _SignalSheet(), None, "test"
    store.backoff = ImmediateRetry()
    store.cache_keys, store.cache_lock = set(), tp.Lock()
    app = object.__new__(tp.PerformanceTrackerApp)
    app.signal_store, app.signal_history_enabled = store, True
    app.args = SimpleNamespace(max_records=1000, horizons=["1W", "2W", "1M", "3M"])
    with _env(TRACK_CANONICAL_TARGETS="1"), _pinned(tp):
        recs = asyncio.run(app.record_from_top10([], _source_rows()))
    # two decided rows: a blank Realized ROI (MATURED, None) and a numeric one
    recs[0].status, recs[0].realized_roi, recs[0].outcome = tp.PerformanceStatus.MATURED, None, "UNPRICED"
    recs[1].status, recs[1].realized_roi, recs[1].outcome = tp.PerformanceStatus.MATURED, 3.0303030303030303, "WIN"
    return recs


def _store(tp, sheet):
    store = object.__new__(tp.PerformanceStore)
    store.ws, store.backoff = sheet, ImmediateRetry()
    store.cache, store.cache_lock = {}, tp.Lock()
    return store


_PRODUCER = re.compile(r'"producer_version": ?"[^"]*"')


def _fixture(tp):
    recs = _records(tp)
    for r in recs:   # the schema receipt names the writer's version; fix it so
        r.notes = _PRODUCER.sub('"producer_version": "fixture"', r.notes or "")   # the fixture is version-free
    writer = object.__new__(tp.PerformanceStore)
    rows = [writer._record_to_row(r) for r in recs]
    return recs, rows


def _load_with(tp, rows, mode, **sheet_kw):
    sheet = LogSheet(rows, tp.PerformanceStore.HEADERS, data_row0=tp.PerformanceStore.DATA_ROW0, **sheet_kw)
    store = _store(tp, sheet)
    env = {ENV: mode, "TRACK_CANONICAL_TARGETS": "1"}
    with _env(**env), _pinned(tp), _captured(tp) as cap:
        loaded = store.load_records(1000)
    return loaded, sheet, store, cap.lines


def _mismatch_by_horizon(tp, recs):
    rep = tp.canonical_performance_report(recs)
    out = {}
    for r, e in zip(recs, rep["records"]):
        if (e.get("reason") or "") == "schema_price_mismatch":
            out[r.horizon.value] = out.get(r.horizon.value, 0) + 1
    return out


def _canonicals(tp, recs):
    return [e.get("canonical_target_roi_pp", e.get("canonical")) for e in tp.canonical_performance_report(recs)["records"]]


def _rows_of(tp, recs):
    w = object.__new__(tp.PerformanceStore)
    return [w._record_to_row(r) for r in recs]


def _digest(calls, rows):
    blob = json.dumps({"calls": [[c[0], sorted(c[1].items())] for c in calls], "rows": rows},
                      sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _log_read_lines(lines):
    return [(lv, m) for lv, m in lines if "LOG-READ" in m]


def _version(v):
    return tuple(int(x) for x in str(v).split(".")[:3])


# ------------------------------------------------------------------ cases
def test_l1_version_floor_and_mode_parser():
    assert _version(TP.SCRIPT_VERSION) >= (6, 50, 0), TP.SCRIPT_VERSION
    for raw, want in ((None, "off"), ("", "off"), ("off", "off"), ("1", "off"), ("yes", "off"),
                      ("on", "off"), ("enforced", "off"), (" Enforce ", "enforce"),
                      ("ENFORCE", "enforce"), ("observe", "observe"), ("\tObserve\n", "observe")):
        with _env(**{ENV: raw}):
            assert TP._log_read_mode() == want, (raw, TP._log_read_mode())


def test_l2_off_is_the_base_read_and_pins_the_defect():
    recs, rows = _fixture(TP)
    assert len(recs) == 160, len(recs)
    assert _mismatch_by_horizon(TP, recs) == {}, "in memory every canonical target is exact"
    digests = set()
    for mode in (None, "off", "1", "yes", "Enforced"):
        loaded, sheet, _store_, lines = _load_with(TP, rows, mode)
        rng = sheet.calls[0][0]
        assert sheet.calls == [(rng, {})], (mode, sheet.calls)
        assert _log_read_lines(lines) == [], (mode, lines)
        mm = _mismatch_by_horizon(TP, loaded)
        assert mm.get("1W", 0) + mm.get("2W", 0) >= 10 and mm.get("1M", 0) == mm.get("3M", 0) == 0, mm
        digests.add(_digest(sheet.calls, _rows_of(TP, loaded)))
    assert len(digests) == 1, digests
    base = _base_module()
    if base is not None:
        b_recs, b_rows = _fixture(base)
        assert b_rows == rows, "the fixture writer is unchanged"
        b_loaded, b_sheet, _s, b_lines = _load_with(base, b_rows, None)
        assert b_sheet.calls == sheet.calls
        assert _rows_of(base, b_loaded) == _rows_of(TP, loaded)
        assert digests == {_digest(b_sheet.calls, _rows_of(base, b_loaded))}
    if OFF_GOLDEN != "0000000000000000":
        assert digests == {OFF_GOLDEN}, digests


def test_l3_enforce_reads_unformatted_and_keeps_every_canonical_target():
    recs, rows = _fixture(TP)
    loaded, sheet, _s, lines = _load_with(TP, rows, " enforce ")
    # the outcome first: on the base this is where the golden negative fails
    assert _mismatch_by_horizon(TP, loaded) == {}, _mismatch_by_horizon(TP, loaded)
    rng = sheet.calls[0][0]
    assert sheet.calls == [(rng, UNF)], sheet.calls
    assert _canonicals(TP, loaded) == _canonicals(TP, recs)
    assert _rows_of(TP, loaded) == rows, "an exact read-back of what the tracker wrote"
    assert loaded[0].realized_roi is None and loaded[0].status == TP.PerformanceStatus.MATURED
    assert loaded[1].realized_roi == 3.0303030303030303
    assert [l.notes for l in loaded] == [r.notes for r in recs]
    assert [TP.RiyadhTime.format(l.date_recorded) for l in loaded] == [TP.RiyadhTime.format(r.date_recorded) for r in recs]
    log = _log_read_lines(lines)
    assert log == [("INFO", "[v6.50.0 LOG-READ] mode=enforce rows=160")], log


def test_l4_enforce_persists_full_precision_and_off_keeps_the_residual():
    recs, rows = _fixture(TP)
    loaded, sheet, store, _l = _load_with(TP, rows, "enforce")
    with _env(**{ENV: "enforce"}), _pinned(TP):
        assert store.save_records(loaded) is True
        again = store.load_records(1000)
    assert sheet.rows == rows
    assert _mismatch_by_horizon(TP, again) == {}
    loaded_off, sheet_off, store_off, _l2 = _load_with(TP, rows, "off")
    with _env(**{ENV: "off"}), _pinned(TP):
        store_off.save_records(loaded_off)
    assert sheet_off.rows != rows, "an off audit pass rewrites the rounded values"
    with _env(**{ENV: "enforce"}), _pinned(TP):
        residual = store_off.load_records(1000)
    mm = _mismatch_by_horizon(TP, residual)
    assert mm.get("1W", 0) + mm.get("2W", 0) >= 10, mm


def test_l5_observe_compares_and_changes_nothing():
    recs, rows = _fixture(TP)
    off, _so, _st, _lo = _load_with(TP, rows, "off")
    obs, sheet, _s, lines = _load_with(TP, rows, "observe")
    rng = sheet.calls[0][0]
    assert sheet.calls == [(rng, {}), (rng, UNF)], sheet.calls
    assert _rows_of(TP, obs) == _rows_of(TP, off)
    log = _log_read_lines(lines)
    assert len(log) == 1 and log[0][0] == "WARNING", log
    info = TP._LAST_LOG_READ
    fm = sum(_mismatch_by_horizon(TP, off).values())
    assert info["rows"] == info["unformatted_rows"] == 160 and info["misaligned"] == 0, info
    assert info["divergent"] == {} and info["text_differs"] == {} and info["precision_only"] > 0, info
    assert info["canonical_mismatch_formatted"] == fm > 0 and info["canonical_mismatch_unformatted"] == 0, info
    assert ("divergent={}" in log[0][1] and "canonical_mismatch formatted=%d unformatted=0" % fm in log[0][1]), log
    # a percent number format on one column is named, records still equal off
    obs_pct, _sp, _s2, _lp = _load_with(TP, rows, "observe", percent_cols=("Target ROI %",))
    assert list(TP._LAST_LOG_READ["divergent"]) == ["Target ROI %"], TP._LAST_LOG_READ
    # a boolean cell (owner-typed TRUE) renders 'TRUE' formatted and True raw: text_differs names it
    col = TP.PerformanceStore.HEADERS.index("Entry Selected")
    rows_b = copy.deepcopy(rows)
    for r in rows_b[:7]:
        r[col] = True
    _ob, _sb, _s4, _lb = _load_with(TP, rows_b, "observe")
    assert TP._LAST_LOG_READ["text_differs"] == {"Entry Selected": 7} and TP._LAST_LOG_READ["divergent"] == {}, TP._LAST_LOG_READ
    # a failed extra read: one WARNING, records equal off, the load succeeds
    obs_f, sheet_f, _s3, lines_f = _load_with(TP, rows, "observe", fail_unformatted=True)
    assert len(obs_f) == 160 and _rows_of(TP, obs_f) == _rows_of(TP, off)
    log_f = _log_read_lines(lines_f)
    assert len(log_f) == 1 and "comparison skipped" in log_f[0][1] and "OSError" in log_f[0][1], log_f


def test_l6_real_gspread_client_sends_the_render_options():
    try:
        import requests
        from gspread.http_client import HTTPClient
        from gspread.worksheet import Worksheet
    except Exception:
        import pytest
        pytest.skip("gspread not installed")
    recs, rows = _fixture(TP)
    sent = []

    class _Resp:
        def __init__(self, values):
            self._v = values

        def json(self):
            return {"range": "'Performance_Log'!A6:AF1005", "majorDimension": "ROWS", "values": self._v}

    def request(method, url, params=None, **kw):
        sent.append(requests.Request(method.upper(), url, params=params).prepare().url)
        return _Resp(copy.deepcopy(rows))

    client = object.__new__(HTTPClient)
    client.request = request
    ws = Worksheet(SimpleNamespace(id="sheet-id"), {"title": "Performance_Log", "sheetId": 0,
                                                    "gridProperties": {"rowCount": 5000, "columnCount": 40}},
                   spreadsheet_id="sheet-id", client=client)
    for mode, want_unf in (("off", False), ("enforce", True)):
        sent.clear()
        store = _store(TP, ws)
        with _env(**{ENV: mode}), _pinned(TP):
            got = store.load_records(1000)
        assert len(got) == 160 and len(sent) == 1, (mode, sent)
        url = sent[0]
        if want_unf:
            assert "valueRenderOption=UNFORMATTED_VALUE" in url and "dateTimeRenderOption=FORMATTED_STRING" in url, url
            assert _rows_of(TP, got) == rows
        else:
            assert "RenderOption" not in url, url


def test_l7_ascii_added_lines_and_zero_removals():
    src = _base_source()
    if src is None:
        import pytest
        pytest.skip("base copy unavailable (shallow checkout)")
    import ast
    import difflib
    new = UNDER_TEST.read_text(encoding="utf-8")
    added = [l[1:] for l in difflib.unified_diff(src.split("\n"), new.split("\n"), n=0, lineterm="")
             if l.startswith("+") and not l.startswith("+++")]
    bad = [l for l in added if any(ord(c) > 127 for c in l) and "ADVANCED PERFORMANCE ANALYTICS ENGINE" not in l]
    assert bad == [], bad[:3]

    def names(text):
        return {n.name for n in ast.parse(text).body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}

    removed = names(src) - names(new)
    assert removed == set(), sorted(removed)
    assert names(new) - names(src) == {"_log_read_mode", "_log_read_observe"}, sorted(names(new) - names(src))


CASES = [test_l1_version_floor_and_mode_parser, test_l2_off_is_the_base_read_and_pins_the_defect,
         test_l3_enforce_reads_unformatted_and_keeps_every_canonical_target,
         test_l4_enforce_persists_full_precision_and_off_keeps_the_residual,
         test_l5_observe_compares_and_changes_nothing, test_l6_real_gspread_client_sends_the_render_options,
         test_l7_ascii_added_lines_and_zero_removals]


if __name__ == "__main__":
    passed = failed = skipped = 0
    for case in CASES:
        try:
            case()
            passed += 1
            print("PASS", case.__name__)
        except Exception as e:  # pytest.skip raises a BaseException subclass in pytest only
            if type(e).__name__ == "Skipped":
                skipped += 1
                print("SKIP", case.__name__, str(e)[:120])
                continue
            failed += 1
            print("FAIL", case.__name__, type(e).__name__, str(e)[:300])
        except BaseException as e:
            if type(e).__name__ == "Skipped":
                skipped += 1
                print("SKIP", case.__name__, str(e)[:120])
                continue
            raise
    total = passed + failed
    print("PASS %d/%d%s" % (passed, total, ("  (skipped %d)" % skipped) if skipped else "") if not failed
          else "FAIL %d/%d" % (passed, total))
    sys.exit(1 if failed else 0)
