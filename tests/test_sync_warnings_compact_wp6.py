#!/usr/bin/env python3
"""tests/test_sync_warnings_compact_wp6.py
WP-6 - run_dashboard_sync v6.65.0 WARNINGS-COMPACT (gate TFB_WARNINGS_COMPACT =
off | observe | enforce at the single market-page Sheets write seam).
Dual-tree, REAL-module harness (the K-battery loader of
tests/test_sync_klg_fetchfail_p162b.py): drives the REAL _wc_* helpers, the
REAL SheetsWriter.write_table, the REAL _keep_last_good_rows and the REAL
_mark_preserved_acquisitions with a Sheets boundary RECORDER as the only
non-real object (tests/test_sync_sheet_presentation.py FakeSheetsAPI shape),
on hand fixtures copied from real 2026-10-10 token shapes and, when
TFB_TEST_EXPORT_XLSX names the 2026-10-10 export (run 38071484709), on the
four real market pages (openpyxl read_only/data_only, frozen numbers).
Base parity: TFB_TEST_SYNC_BASE = the v6.64.14 file -> off payload ==
base payload byte-for-byte. Offline: no network, no credentials, no live Sheet.

Reader census (T12) classification of the non-writer hits (the heads appear
in these files but none reads a sheet Warnings cell):
  core/analysis/identity_guard.py   - docstring prose names quote_currency_from_suffix
  scripts/harness_ob_1_13_0.py      - a builder fixture STRING carries quote_exchange_from_suffix
  scripts/diag_eodhd_fundamentals.py - lists fundamentals_lkg in an ENGINE API row diagnostic
  scripts/audit_data_quality.py     - offline audit of ENGINE API rows (documented exception)
  scripts/repo_hygiene_check.py     - rel_path is a Python variable (relative file path)
Writer files (allowed): core/data_engine_v2.py, core/enriched_quote.py,
core/providers/*, core/sheet_presentation.py (sheet_horizon_label writer).

Run: pytest -q tests/test_sync_warnings_compact_wp6.py
  or python3 tests/test_sync_warnings_compact_wp6.py   (x3, digest)"""
from __future__ import annotations

import asyncio
import contextlib
import copy
import hashlib
import importlib.util
import inspect
import io
import json
import logging
import os
import re
import sys
import tempfile
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
SYNC = os.environ.get("TFB_TEST_SYNC_DELIVERED") or os.path.join(ROOT, "scripts", "run_dashboard_sync.py")
BASE = os.environ.get("TFB_TEST_SYNC_BASE", "")
EXPORT = os.environ.get("TFB_TEST_EXPORT_XLSX", "")
os.environ.setdefault("GITHUB_RUN_ID", "38071484709")
FIXED_NOW = datetime(2026, 10, 10, 18, 0, 0, tzinfo=timezone.utc)
PAGES = ("Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds")
WC_ENV = ("TFB_WARNINGS_COMPACT", "TFB_WARNINGS_COMPACT_KEEP", "TFB_WARNINGS_SINK_DIR")
# The sync's own offline modes (the test_sync_sheet_presentation.py fixture).
OFFLINE = {
    "TFB_MARKET_SYMBOL_READBACK": "0", "TFB_SYNC_SYMBOL_BATCH_SIZE": "0",
    "TFB_SYNC_SYMBOL_PERSISTENCE": "1", "TFB_SYNC_KEEP_LAST_GOOD": "1",
    "TFB_SYNC_PERSISTENCE_HARD": "0", "TFB_SYNC_ROW_ID_FIREWALL": "0",
    "TFB_SYNC_NAME_DEDUP_MODE": "off", "TFB_SYNC_OHLC_LAKE": "0",
    "TFB_SYNC_FALSE_GREEN_SCREEN": "0", "TFB_SYNC_STATUS_STAMP": "0",
    "TFB_SYNC_FETCHFAIL_TRUTH": "off", "TFB_SYNC_IDENTITY_TRIPWIRE": "0",
    "TFB_SYNC_COHERENCE_TRIPWIRE": "0", "TFB_SYNC_OHLC_PREWRITE": "0",
    "TFB_SYNC_OHLC_FILLGUARD": "off", "TFB_SYNC_OHLC_READBACK": "0",
    "TFB_SYNC_IDFW_RUNLOG": "0", "TFB_SVC_CAPACITY_GUARD": "0",
    "TFB_SYNC_KLG_FETCHFAIL": "off",
}


def _load(path, name):
    d = os.path.dirname(os.path.abspath(path))
    if d not in sys.path:
        sys.path.insert(0, d)
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    mod._utc_now = lambda: FIXED_NOW          # frozen clock (records, stamps)
    if hasattr(mod, "_RUN_ID"):
        mod._RUN_ID = "wp6fixedrun1"           # deterministic sink file names
    return mod


_M = {}


def m():
    if "d" not in _M:
        _M["d"] = _load(SYNC, "rds_wp6_delivered")
    return _M["d"]


def b():
    if "b" not in _M:
        _M["b"] = _load(BASE, "rds_wp6_base") if BASE and os.path.exists(BASE) else None
    return _M["b"]


class _Env(object):
    """Set/unset env vars for one block; restores the prior state exactly."""
    def __init__(self, **kw):
        self.kw, self.prev = kw, {}

    def __enter__(self):
        for k, v in dict(OFFLINE, **self.kw).items():
            self.prev[k] = os.environ.get(k)
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        for k in WC_ENV:
            if k not in self.kw:
                self.prev[k] = os.environ.get(k)
                os.environ.pop(k, None)
        return self

    def __exit__(self, *_a):
        for k, v in self.prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        return False


class FakeSheetsAPI(object):
    """Record the real direct SDK values.update / values.append arguments,
    without credentials (tests/test_sync_sheet_presentation.py shape)."""
    def __init__(self, fail_append=False):
        self.requests, self.fail_append = [], fail_append

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def update(self, **kwargs):
        self.requests.append(dict(copy.deepcopy(kwargs), _method="update"))
        return self

    def append(self, **kwargs):
        self.requests.append(dict(copy.deepcopy(kwargs), _method="append"))
        return self

    def execute(self):
        last = self.requests[-1]
        if last["_method"] == "append" and self.fail_append:
            raise RuntimeError("synthetic append failure")
        return {"updatedRows": len(last["body"]["values"])}

    def appends(self):
        return [r for r in self.requests if r["_method"] == "append"]

    def updates(self):
        return [r for r in self.requests if r["_method"] == "update"]


class _Grid(object):
    """Sheets boundary double for the KLG read (read_values only)."""
    def __init__(self, grid):
        self.grid, self.reads = grid, 0

    def read_values(self, *_a, **_k):
        self.reads += 1
        return [list(r) for r in self.grid]


class _LogCapture(logging.Handler):
    def __init__(self):
        logging.Handler.__init__(self, logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record.getMessage())


@contextlib.contextmanager
def _captured_log(mod):
    h = _LogCapture()
    lg = mod.logger
    old_level = lg.level
    lg.addHandler(h)
    lg.setLevel(logging.DEBUG)
    try:
        yield h
    finally:
        lg.removeHandler(h)
        lg.setLevel(old_level)


def _writer(mod, api):
    w = mod.SheetsWriter()
    w._service = api
    return w


def _tmpdir():
    return tempfile.mkdtemp(prefix="wp6_")


# ----------------------------------------------------------------- fixtures
GM_FULL = ("acquisition_status:success; acquisition_provider:eodhd; "
           "acquisition_acquired_at:2026-10-10T17:24:55.306609+00:00; "
           "acquisition_quote_asof:2026-10-10T16:59:58+00:00; quote_exchange_from_suffix; "
           "quote_currency_from_suffix; name_from_chart_meta; analyst_trend_block_applied; "
           "yahoo_enrichment_applied; fund_cache:hit:3h:2; fundamentals_lkg:4h; "
           "rel_path:b=B:fc=73.5:dq=100.0:pen=FS:os=ri_based:fs=ri_based:raw=12.0:cf=none:fin=12.0; "
           "f7_settle:observe:st4:p3:ov=38.06>39.3; eq_roi_backfill:expected_roi_1m:synth:observe; "
           "margin_publish:gross_margin:frac:observe; intrinsic_soft_ceiling_applied; "
           "sector_from_eodhd_verified; sheet_tuple_conflict:expected_roi_12m; "
           "confidence_default_suspected; analyst_lkg:12h; sheet_horizon_label:horizon_label:unknown; "
           "sheet_horizon_label:invest_period_label:1Y; "
           "sheet_margin_unit:gross_margin:fraction:0.49488997; "
           "sheet_margin_unit:profit_margin:fraction:0.226")
GM_COMPACT = ("sheet_tuple_conflict:expected_roi_12m; confidence_default_suspected; analyst_lkg:12h; "
              "acquisition_status:success; acquisition_provider:eodhd; "
              "acquisition_acquired_at:2026-10-10T17:24:55.306609+00:00; "
              "acquisition_quote_asof:2026-10-10T16:59:58+00:00; name_from_chart_meta; "
              "yahoo_enrichment_applied; sheet_margin_unit:gross_margin:fraction:0.49488997; "
              "sheet_margin_unit:profit_margin:fraction:0.226")
UNPRICED_FULL = ("acquisition_status:unavailable; quote_attempt:eodhd:unpriced; quote_current_price_missing; "
                 "low_data_trust; rank_skipped_low_trust; recommendation_forced_hold_missing_price; "
                 "empty_row_no_provider_data; fund_cache:skip_page")
UNPRICED_COMPACT = ("recommendation_forced_hold_missing_price; low_data_trust; rank_skipped_low_trust; "
                    "acquisition_status:unavailable; quote_attempt:eodhd:unpriced; "
                    "quote_current_price_missing; empty_row_no_provider_data")
SHELL_FULL = "identity_quarantined:v6.24.0; sheet_horizon_label:invest_period_label:unknown"
FF_FULL = ("fetch_failed:HTTP 402; dq_capped:coherence:fetch_failed; "
           "rel_path:b=F:fc=0.0:dq=0.0:pen=none:os=none:fs=none:raw=0.0:cf=none:fin=0.0; "
           "eq_roi_backfill:expected_roi_12m:synth:observe")
FF_COMPACT = "fetch_failed:HTTP 402; dq_capped:coherence:fetch_failed"
WITNESS_FULL = ("margin_publish:profit_margin:frac; eq_roi_backfill:expected_roi_1m:synth; "
                "f7_settle:enforce:st4:p3; fund_unit_contract:eodhd:debt_to_equity; "
                "fund_unit_contract:yahoo:debt_to_equity; fund_coherence_repaired:profit_margin; "
                "eodhd_fundamentals_fallback_applied; yahoo_enrichment_applied; f7_settle:observe:st2:p1; "
                "f7_settle_failed:non_converged; rank_skipped_settlement_failure")
WITNESS_COMPACT = ("f7_settle_failed:non_converged; rank_skipped_settlement_failure; "
                   "margin_publish:profit_margin:frac; eq_roi_backfill:expected_roi_1m:synth; "
                   "f7_settle:enforce:st4:p3; fund_unit_contract:eodhd:debt_to_equity; "
                   "fund_coherence_repaired:profit_margin; eodhd_fundamentals_fallback_applied; "
                   "yahoo_enrichment_applied")
UNKNOWN_FULL = ("wp9_new_tag:x; sanitized:week_52_high_provider_ceiling; "
                "provider_target_capped_for_short_horizon_derivation; xprovider_verified:eodhd,yahoo; "
                "price_bar_stale:3d; forecast_capped_at_ceiling; intrinsic_rejected_outlier(x0.2); "
                "market_cap_subunit_normalized_to_major")
UNKNOWN_COMPACT = ("price_bar_stale:3d; provider_target_capped_for_short_horizon_derivation; "
                   "forecast_capped_at_ceiling; wp9_new_tag:x; xprovider_verified:eodhd,yahoo")
BLANKSYM_FULL = "rel_path:b=B:fc=1; fund_cache:hit:1h:1; identity_quarantined:v6.24.0"
EXPECTED = {"GMFIX.US": GM_COMPACT, "UNPRX.US": UNPRICED_COMPACT, "RAY.US": SHELL_FULL,
            "FFX.US": FF_COMPACT, "WITN.US": WITNESS_COMPACT, "UNKN.US": UNKNOWN_COMPACT, "BLNK.US": ""}
FIXTURE_TAGS_REMOVED = 13 + 1 + 0 + 2 + 2 + 3     # per row, hand-counted
FIXTURE_ROWS_WITH_REMOVABLE = 5


def _headers(page):
    from core.sheets.schema_registry import get_sheet_headers
    return list(get_sheet_headers(page))


def _row(hdr, **cells):
    idx = {h: i for i, h in enumerate(hdr)}
    r = [None] * len(hdr)
    for k, v in cells.items():
        r[idx[k]] = v
    return r


def fixture(page="Global_Markets"):
    """Eight rows copied from real 10-10 token shapes (market rows only)."""
    h = _headers(page)
    common = {"Data Provider": "eodhd", "Last Updated (UTC)": "2026-10-10T17:24:55+00:00"}
    rows = [
        _row(h, Symbol="GMFIX.US", Name="GM Fixture", **{"Current Price": 10.0, "Gross Margin": 0.49488997,
             "Profit Margin": 0.226, "Horizon Days": 365, "Invest Period Label": "3M",
             "Forecast Price 12M": 12.0, "Expected ROI 12M": 0.3, "Warnings": GM_FULL}, **common),
        _row(h, Symbol="UNPRX.US", Name="Unpriced Fixture", Warnings=UNPRICED_FULL,
             **{"Data Provider": "unavailable"}),
        _row(h, Symbol="RAY.US", Name="", **{"Invest Period Label": "", "Warnings": SHELL_FULL}),
        _row(h, Symbol="FFX.US", Name="Fetch Failed Fixture", Warnings=FF_FULL,
             **{"Data Provider": "fallback_error"}),
        _row(h, Symbol="WITN.US", Name="Witness Fixture", **{"Current Price": 20.0, "Warnings": WITNESS_FULL}, **common),
        _row(h, Symbol="UNKN.US", Name="Unknown Tag Fixture", **{"Current Price": 5.0, "Warnings": UNKNOWN_FULL}, **common),
        _row(h, Symbol="BLNK.US", Name="Blank Warnings Fixture", **{"Current Price": 7.0, "Warnings": ""}, **common),
        _row(h, Symbol="", Name="", Warnings=BLANKSYM_FULL),
    ]
    return h, rows


def _warn_col(hdr):
    return hdr.index("Warnings")


def _res(mod, page="Global_Markets", dry_run=False):
    r = mod.TaskResult(key=page.upper(), sheet_name=page, status="success",
                       start_utc="2026-10-10T17:00:00+00:00", end_utc="2026-10-10T17:10:00+00:00",
                       rows_written=7)
    r.dry_run = dry_run
    return r


def _task(mod, page="Global_Markets"):
    return mod.TaskSpec(page.upper(), page, "analysis")


def _payload(mod, page, hdr, rows):
    api = FakeSheetsAPI()
    assert _writer(mod, api).write_table("BOOK", page, "A5", hdr, rows) == len(rows)
    req = api.updates()[-1]
    assert req["valueInputOption"] == "RAW" and req["range"] == page + "!A5"
    return req["body"]["values"], api


# T1 - vocabulary / selftest / version -----------------------------------------------------
def _t1():
    mod = m()
    out = {}
    for v, exp in ((None, "off"), ("", "off"), ("1", "off"), ("true", "off"), ("on", "off"),
                   ("observe", "observe"), (" ENFORCE ", "enforce"), ("Observe", "observe")):
        with _Env(TFB_WARNINGS_COMPACT=v):
            assert mod._wc_mode_raw() == exp, (v, exp)
            out[str(v)] = mod._wc_mode_raw()
    assert mod._wc_selftest() == "PASS"
    with _Env(TFB_WARNINGS_COMPACT="enforce"):
        assert mod._wc_mode() == "enforce"
    assert tuple(int(x) for x in mod.SCRIPT_VERSION.split(".")) >= (6, 65, 0)
    assert mod._WC_TAG == "[WARNINGS-COMPACT v%s]" % mod.SCRIPT_VERSION
    with _Env(TFB_WARNINGS_COMPACT_KEEP=" Rel_Path ; f7_settle,,"):
        assert mod._wc_keep_extra() == frozenset({"rel_path", "f7_settle"})
    with _Env(TFB_WARNINGS_COMPACT_KEEP=",,;;"):
        assert mod._wc_keep_extra() == frozenset()
    with _Env(TFB_WARNINGS_SINK_DIR=None):
        assert mod._wc_sink_dir() == "."
    out["selftest"] = mod._wc_selftest()
    out["version"] = mod.SCRIPT_VERSION
    return out


def test_t1_vocabulary_selftest_version():
    _t1()


# T2 - off is byte-identical (dual tree) ---------------------------------------------------
def _t2():
    mod = m()
    out = {"base": "n/a"}
    for page in PAGES:
        hdr, rows = fixture(page)
        original = copy.deepcopy(rows)
        with _Env(TFB_WARNINGS_COMPACT=None):
            p_unset, _ = _payload(mod, page, hdr, copy.deepcopy(rows))
        with _Env(TFB_WARNINGS_COMPACT="off"):
            p_off, _ = _payload(mod, page, hdr, copy.deepcopy(rows))
        assert p_unset == p_off
        assert rows == original
        if b() is not None:
            with _Env(TFB_WARNINGS_COMPACT=None):
                p_base, _ = _payload(b(), page, hdr, copy.deepcopy(rows))
            assert p_base == p_off, page
            assert mod._payload_sha8(hdr, p_off[1:]) == b()._payload_sha8(hdr, p_base[1:])
            out["base"] = "parity"
        out[page] = hashlib.sha256(json.dumps(p_off, default=str).encode()).hexdigest()[:12]
    # the seam under off: same object, zero recorder calls, nothing appended
    hdr, rows = fixture()
    api = FakeSheetsAPI()
    with _Env(TFB_WARNINGS_COMPACT=None):
        res = _res(mod)
        got = mod._wc_publish_seam(_task(mod), res, _writer(mod, api), "BOOK", hdr, rows)
    assert got is rows and api.requests == [] and "wc" not in res._stamp_meta
    # the report block is guarded on the requested mode
    src = inspect.getsource(mod.main_async)
    assert re.search(r'if _wc_mode_raw\(\) != "off":[^\n]*\n\s+report\["warnings_compact"\] = _wc_run_report\(\)', src)
    return out


def test_t2_off_is_byte_identical_dual_tree():
    _t2()


# T3 - observe counts only ----------------------------------------------------------------
def _t3():
    mod = m()
    hdr, rows = fixture()
    original = copy.deepcopy(rows)
    with _Env(TFB_WARNINGS_COMPACT="off"):
        p_off, _ = _payload(mod, "Global_Markets", hdr, copy.deepcopy(rows))
        res_off = _res(mod)
        stamp_off = mod._status_stamp_row("Global_Markets", res_off, len(hdr))
    api = FakeSheetsAPI()
    with _Env(TFB_WARNINGS_COMPACT="observe"):
        res = _res(mod)
        got = mod._wc_publish_seam(_task(mod), res, _writer(mod, api), "BOOK", hdr, rows)
        p_obs, _ = _payload(mod, "Global_Markets", hdr, copy.deepcopy(rows))
        stamp_obs = mod._status_stamp_row("Global_Markets", res, len(hdr))
        o, st, rec = mod._wc_compact_rows("Global_Markets", hdr, rows, "observe", mod._wc_keep_extra())
    assert got is rows and rows == original and o is rows and rec == []
    assert p_obs == p_off
    assert st["tags_removed"] == FIXTURE_TAGS_REMOVED and st["rows_with_removable"] == FIXTURE_ROWS_WITH_REMOVABLE
    assert st["rows"] == 7 and st["skipped_no_symbol"] == 1 and st["rows_empty"] == 1
    assert st["status"] == "WOULD_COMPACT"
    assert mod._wc_compact_cell(GM_FULL)[1].__len__() == 13
    # one _Run_Log row per page per leg, the FW-3 10-column shape
    apps = api.appends()
    assert len(apps) == 1 and apps[0]["range"] == "'_Run_Log'!A1"
    assert apps[0]["valueInputOption"] == "USER_ENTERED" and apps[0]["insertDataOption"] == "INSERT_ROWS"
    row = apps[0]["body"]["values"][0]
    assert len(row) == 10 and row[1] == "INFO" and row[2] == "run_dashboard_sync"
    assert row[3] == "Global_Markets" and row[4] == "WOULD_COMPACT"
    assert row[5].startswith(mod._WC_TAG + " Global_Markets | mode=observe status=WOULD_COMPACT rows=7 removable_rows=5 tags=-21")
    details = json.loads(row[9])
    for k in ("mode", "page", "rows", "rows_with_removable", "tags_removed", "chars_removed", "mean_chars_before",
              "mean_chars_after", "rows_gt5_before", "rows_gt5_after", "top_removed_families", "keep_extra",
              "sink", "sink_sha8", "selftest", "version", "run_id", "ts_utc"):
        assert k in details, k
    assert details["run_id"] == "38071484709" and details["version"] == mod.SCRIPT_VERSION
    wc = res._stamp_meta["wc"]
    assert wc["mode"] == "observe" and wc["status"] == "WOULD_COMPACT" and wc["tags_removed"] == FIXTURE_TAGS_REMOVED
    # the _Status stamp text is untouched by _stamp_meta['wc'] (B = wall clock)
    assert stamp_obs[0] == stamp_off[0] and stamp_obs[2:] == stamp_off[2:]
    assert mod._WC_RUN_STATS["Global_Markets"]["status"] == "WOULD_COMPACT"
    details.pop("ts_utc", None)
    return {"stats": {k: st[k] for k in ("rows", "rows_with_removable", "tags_removed", "chars_removed",
                                         "removed_families", "mean_chars_before", "mean_chars_after")},
            "details_keys": sorted(details), "message": row[5]}


def test_t3_observe_counts_only():
    _t3()


# T4 - enforce compacts exactly the movable set -------------------------------------------
def _t4():
    mod = m()
    hdr, rows = fixture()
    wi = _warn_col(hdr)
    si = hdr.index("Symbol")
    original = copy.deepcopy(rows)
    api = FakeSheetsAPI()
    sink_dir = _tmpdir()
    with _Env(TFB_WARNINGS_COMPACT="enforce", TFB_WARNINGS_SINK_DIR=sink_dir):
        res = _res(mod)
        got = mod._wc_publish_seam(_task(mod), res, _writer(mod, api), "BOOK", hdr, rows)
    assert got is not rows and rows == original
    assert [r[si] for r in got] == [r[si] for r in rows]
    for r_in, r_out in zip(rows, got):
        sym = r_in[si]
        if sym == "":
            assert r_out[wi] == r_in[wi]
        else:
            assert r_out[wi] == EXPECTED[sym], (sym, r_out[wi])
        for i, (a, c) in enumerate(zip(r_in, r_out)):
            if i != wi:
                assert a == c
    # receipts last, severity first (the GM row)
    gm = got[0][wi].split("; ")
    assert gm[-2:] == ["sheet_margin_unit:gross_margin:fraction:0.49488997", "sheet_margin_unit:profit_margin:fraction:0.226"]
    assert gm[0] == "sheet_tuple_conflict:expected_roi_12m"
    wc = res._stamp_meta["wc"]
    assert wc["status"] == "COMPACTED" and wc["mode"] == "enforce" and wc["tags_removed"] == FIXTURE_TAGS_REMOVED
    assert wc["sink"] == os.path.join(sink_dir, "sync_row_telemetry_Global_Markets_wp6fixedrun1.jsonl")
    assert os.path.exists(wc["sink"]) and len(wc["sink_sha8"]) == 8
    apps = api.appends()
    assert len(apps) == 1 and apps[0]["body"]["values"][0][4] == "COMPACTED"
    # the FW-KEEP / preserved rewrite still works on a compacted cell
    marked = mod._mark_preserved_acquisitions(hdr, got, {"GMFIX.US"})
    assert marked == 1
    assert got[0][wi].endswith("; acquisition_status:preserved")
    assert "acquisition_status:success" not in got[0][wi]
    assert got[0][wi].startswith("sheet_tuple_conflict:expected_roi_12m; confidence_default_suspected")
    return {"cells": {r[si]: r[wi] for r in got if r[si]}, "sink_sha8": wc["sink_sha8"]}


def test_t4_enforce_compacts_exactly_the_movable_set():
    _t4()


# T5 - witness rule -----------------------------------------------------------------------
def _t5():
    mod = m()
    text, removed = mod._wc_compact_cell(WITNESS_FULL)
    assert text == WITNESS_COMPACT
    assert removed == ["fund_unit_contract:yahoo:debt_to_equity", "f7_settle:observe:st2:p1"]
    for tok, exp in (("margin_publish:profit_margin:frac", False), ("margin_publish:profit_margin:frac:observe", True),
                     ("eq_roi_backfill:expected_roi_1m:synth", False), ("eq_roi_backfill:expected_roi_1m:synth:observe", True),
                     ("f7_settle:observe:st4", True), ("f7_settle:enforce:st4", False), ("f7_settle", False),
                     ("f7_settle_failed:non_converged", False), ("fund_unit_contract:eodhd:x", False),
                     ("fund_unit_contract:yahoo:x", True), ("fund_unit_contract", False),
                     ("fund_coherence_repaired:profit_margin", False), ("fund_coherence_quarantined:x", False),
                     ("fund_coherence_skipped:x", True), ("eodhd_fundamentals_fallback_applied", False),
                     ("yahoo_enrichment_applied", False), ("yahoo_chart_enrichment_applied", True),
                     ("intraday_quote_refreshed:x", False), ("sheet_horizon_label:invest_period_label:unknown", False),
                     ("sheet_horizon_label:invest_period_label:1Y", True), ("sheet_horizon_label:horizon_label:unknown", True),
                     ("sheet_horizon_label:horizon_label:1Y", True), ("acquisition_acquired_at:2026-10-10T17:24:55+00:00", False),
                     ("fetch_failed:HTTP 402", False), ("rel_path:b=B", True), ("fund_cache:skip_page", True)):
        assert mod._wc_token_movable(tok) is exp, tok
    return {"text": text, "removed": removed}


def test_t5_witness_rule():
    _t5()


# T6 - unknown head kept; KEEP valve; _Run_Log append contract ------------------------------
def _t6():
    mod = m()
    assert mod._wc_compact_cell(UNKNOWN_FULL)[0] == UNKNOWN_COMPACT
    assert mod._wc_compact_cell("wp9_new_tag:x")[0] == "wp9_new_tag:x"
    with _Env(TFB_WARNINGS_COMPACT_KEEP="rel_path,f7_settle"):
        keep = mod._wc_keep_extra()
        text, removed = mod._wc_compact_cell(GM_FULL, keep)
    assert "rel_path:b=B:fc=73.5:dq=100.0:pen=FS:os=ri_based:fs=ri_based:raw=12.0:cf=none:fin=12.0" in text
    assert "f7_settle:observe:st4:p3:ov=38.06>39.3" in text and len(removed) == 11
    with _Env(TFB_WARNINGS_COMPACT_KEEP="not a head"):
        assert mod._wc_compact_cell(GM_FULL, mod._wc_keep_extra())[0] == GM_COMPACT
    # _append_runlog_warnings_compact: FW-3 shape, two attempts, failure -> ::warning:: and NO raise,
    # and _RUNLOG_APPEND_FAILS untouched (telemetry can never flip the exit code - B1)
    hdr, rows = fixture()
    _, st, _ = mod._wc_compact_rows("Global_Markets", hdr, rows, "observe")
    st.update({"requested": "observe", "sink": "-", "sink_sha8": "", "selftest": "PASS", "keep_extra": []})
    ok = FakeSheetsAPI()
    mod._append_runlog_warnings_compact(_writer(mod, ok), "BOOK", "Global_Markets", st, "WOULD_COMPACT")
    assert len(ok.appends()) == 1 and len(ok.appends()[0]["body"]["values"][0]) == 10
    bad = FakeSheetsAPI(fail_append=True)
    fails_before = list(mod._RUNLOG_APPEND_FAILS)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        mod._append_runlog_warnings_compact(_writer(mod, bad), "BOOK", "Global_Markets", st, "WOULD_COMPACT")
    assert len(bad.appends()) == 2
    assert "::warning::" in buf.getvalue() and "_Run_Log append FAILED for Global_Markets" in buf.getvalue()
    assert list(mod._RUNLOG_APPEND_FAILS) == fails_before
    assert mod._append_fail_is_error() is True   # default ON - the rc-3 path the B1 fix guards against
    mod._append_runlog_warnings_compact(None, "BOOK", "Global_Markets", st, "WOULD_COMPACT")   # no sheets: no-op
    mod._append_runlog_warnings_compact(_writer(mod, ok), "BOOK", "Global_Markets", st, "bogus")  # unknown status: no-op
    assert len(ok.appends()) == 1
    return {"attempts_on_failure": len(bad.appends()), "warning": "::warning::" in buf.getvalue()}


def test_t6_unknown_head_keep_valve_and_runlog_append_contract():
    _t6()


# T7 - idempotency and the silent write_table safety net -----------------------------------
def _t7():
    mod = m()
    hdr, rows = fixture()
    wi = _warn_col(hdr)
    with _Env(TFB_WARNINGS_COMPACT="enforce", TFB_WARNINGS_SINK_DIR=_tmpdir()):
        once, st1, rec1 = mod._wc_compact_rows("Global_Markets", hdr, rows, "enforce")
        twice, st2, rec2 = mod._wc_compact_rows("Global_Markets", hdr, once, "enforce")
        assert st2["tags_removed"] == 0 and rec2 == [] and [r[wi] for r in twice] == [r[wi] for r in once]
        for r in rows:
            t1, _ = mod._wc_compact_cell(r[wi])
            t2, r2 = mod._wc_compact_cell(t1)
            assert t1 == t2 and r2 == []
        # the REAL path: final presentation copy -> seam -> write_table (second projection + safety net)
        projected = mod._present_market_sheet_rows("Global_Markets", hdr, copy.deepcopy(rows))
        api = FakeSheetsAPI()
        res = _res(mod)
        compacted = mod._wc_publish_seam(_task(mod), res, _writer(mod, api), "BOOK", hdr, projected)
        assert res._stamp_meta["wc"]["status"] == "COMPACTED"
        with _captured_log(mod) as log:
            payload, _ = _payload(mod, "Global_Markets", hdr, compacted)
        assert not any("late compaction" in line for line in log.records), log.records
        # Through the second projection every cell is byte-identical EXCEPT that
        # a blank-Horizon-Days row gains the KEEP literal (the pre-existing
        # v6.64.14 re-emission the B2 rule exists for) - nothing MOVABLE returns.
        hd = hdr.index("Horizon Days")
        literal = "sheet_horizon_label:invest_period_label:unknown"
        re_emitted = 0
        for before, after in zip(compacted, payload[1:]):
            assert before[:wi] == after[:wi] and before[wi + 1:] == after[wi + 1:]
            if before[wi] == after[wi]:
                continue
            assert before[hd] in (None, "") and literal not in mod._wc_cell_parts(before[wi])
            assert mod._wc_cell_parts(after[wi]) == mod._wc_cell_parts(before[wi]) + [literal], after[wi]
            re_emitted += 1
        assert re_emitted == 6                     # the six blank-Horizon-Days rows without the literal
        assert payload[1][wi] == GM_COMPACT        # Horizon Days 365: byte-identical
        # the identity shell keeps the re-emitted literal and nothing else moved
        shell = payload[1:][2]
        assert shell[wi] == SHELL_FULL and shell[hdr.index("Invest Period Label")] == ""
        # a second write of the published payload is byte-identical (idempotent publication)
        with _captured_log(mod) as log3:
            payload2, _ = _payload(mod, "Global_Markets", hdr, [list(r) for r in payload[1:]])
        assert payload2 == payload and not any("late compaction" in line for line in log3.records)
        # a page written WITHOUT the seam (a fixture that never went through the
        # seam) is the only way the safety net has work: it compacts and warns
        with _captured_log(mod) as log2:
            raw_payload, _ = _payload(mod, "Global_Markets", hdr, copy.deepcopy(rows))
        assert any("late compaction on Global_Markets removed" in line for line in log2.records)
        assert raw_payload[1][wi] == GM_COMPACT
    return {"cells": [r[wi] for r in compacted], "late_on_seamed": 0}


def test_t7_idempotent_and_safety_net_silent_after_the_seam():
    _t7()


# T8 - KLG restore with a compacted prior --------------------------------------------------
def _t8():
    mod = m()
    from core.data_validity import acquisition_tokens, row_acquisition
    from core.sheet_presentation import present_instrument_row
    hdr, rows = fixture()
    wi = _warn_col(hdr)
    full_prior = _row(hdr, Symbol="GMFIX.US", Name="GM Fixture", **{
        "Current Price": 10.0, "EPS (TTM)": 1.0, "P/E (TTM)": 10.0, "Gross Margin": 0.49488997,
        "Profit Margin": 0.226, "Horizon Days": 365, "Invest Period Label": "1Y", "Data Provider": "eodhd",
        "Last Updated (UTC)": "2026-10-10T17:24:55+00:00", "Warnings": GM_FULL})
    compact_prior = list(full_prior)
    compact_prior[wi] = GM_COMPACT
    stub = _row(hdr, Symbol="GMFIX.US", Name="GM Fixture", **{"Data Provider": "fallback_error", "Warnings": "fetch_failed:timeout"})
    with _Env():
        grid = _Grid([list(hdr), compact_prior])
        out, swapped = mod._keep_last_good_rows(grid, "BOOK", "Global_Markets", list(hdr), [list(stub)])
    assert swapped == ["GMFIX.US"] and grid.reads == 1
    restored = out[0]
    assert restored[wi] == GM_COMPACT and restored[hdr.index("Current Price")] == 10.0
    p_restored = present_instrument_row(dict(zip(hdr, restored)))
    p_full = present_instrument_row(dict(zip(hdr, full_prior)))
    for k in hdr:
        if k != "Warnings":
            assert p_restored[k] == p_full[k], k
    assert p_restored["Profit Margin"] == 0.226 and p_restored["Gross Margin"] == 0.49488997
    assert "sheet_margin_unit:profit_margin:fraction:0.226" in p_restored["Warnings"]
    assert "sheet_margin_unknown" not in p_restored["Warnings"]
    a_r = row_acquisition(dict(zip(hdr, restored)), FIXED_NOW, 86400)
    a_f = row_acquisition(dict(zip(hdr, full_prior)), FIXED_NOW, 86400)
    assert a_r.status == a_f.status and a_r.reason == a_f.reason
    assert acquisition_tokens(restored[wi]) == acquisition_tokens(full_prior[wi])
    assert mod._mark_preserved_acquisitions(hdr, out, ["GMFIX.US"]) == 1
    assert out[0][wi].endswith("acquisition_status:preserved") and "acquisition_status:success" not in out[0][wi]
    assert row_acquisition(dict(zip(hdr, out[0])), FIXED_NOW, 86400).status == "INVALID"
    return {"status": a_r.status, "tokens": acquisition_tokens(restored[wi]), "margin": p_restored["Profit Margin"]}


def test_t8_klg_restore_with_compacted_prior():
    _t8()


# T9 - sink JSONL and SINK_FAILED fallback -------------------------------------------------
def _t9():
    mod = m()
    hdr, rows = fixture()
    wi, si = _warn_col(hdr), hdr.index("Symbol")
    sink_dir = _tmpdir()
    with _Env(TFB_WARNINGS_COMPACT="enforce", TFB_WARNINGS_SINK_DIR=sink_dir):
        _, st, records = mod._wc_compact_rows("Global_Markets", hdr, rows, "enforce")
        path, sha8 = mod._wc_write_sink("Global_Markets", records)
        path2, sha8b = mod._wc_write_sink("Global_Markets", records)
    assert path == path2 == os.path.join(sink_dir, "sync_row_telemetry_Global_Markets_wp6fixedrun1.jsonl")
    assert sha8 == sha8b and len(sha8) == 8
    lines = open(path, encoding="utf-8").read().splitlines()
    assert len(lines) == FIXTURE_ROWS_WITH_REMOVABLE == len(records)
    by_symbol = {r[si]: r for r in rows}
    keys = set()
    for line in lines:
        rec = json.loads(line)
        keys.add((rec["run_id"], rec["symbol"]))
        assert rec["run_id"] == "38071484709" and rec["page"] == "Global_Markets"
        assert rec["warnings_full"] == by_symbol[rec["symbol"]][wi]
        assert rec["warnings_compact"] == EXPECTED[rec["symbol"]]
        assert rec["removed_n"] == len(rec["removed"]) > 0 and rec["wc_version"] == mod.SCRIPT_VERSION
        assert rec["written_at_utc"] == FIXED_NOW.isoformat()
        assert rec["chars_saved"] == len(rec["warnings_full"]) - len(rec["warnings_compact"])
    assert len(keys) == FIXTURE_ROWS_WITH_REMOVABLE
    assert not os.path.exists(path + ".tmp")
    # SINK_FAILED: the sink dir is a FILE -> mkdir raises -> cells untouched, loud, _Run_Log SINK_FAILED
    blocker = os.path.join(_tmpdir(), "not_a_dir")
    open(blocker, "w").close()
    api = FakeSheetsAPI()
    buf = io.StringIO()
    with _Env(TFB_WARNINGS_COMPACT="enforce", TFB_WARNINGS_SINK_DIR=blocker):
        res = _res(mod)
        with contextlib.redirect_stdout(buf), _captured_log(mod) as log:
            got = mod._wc_publish_seam(_task(mod), res, _writer(mod, api), "BOOK", hdr, rows)
    assert got is rows and rows[0][wi] == GM_FULL
    assert "::warning::" in buf.getvalue() and "sink write FAILED for Global_Markets" in buf.getvalue()
    assert res._stamp_meta["wc"]["status"] == "SINK_FAILED" and res._stamp_meta["wc"]["sink"] == "-"
    apps = api.appends()
    assert len(apps) == 1 and apps[0]["body"]["values"][0][4] == "SINK_FAILED" and apps[0]["body"]["values"][0][1] == "WARNING"
    assert any("status=SINK_FAILED" in line for line in log.records)
    return {"sha8": sha8, "lines": len(lines), "keys": sorted(keys)}


def test_t9_sink_jsonl_and_sink_failed_fallback():
    _t9()


# T10 - dry run -------------------------------------------------------------------------
def _t10():
    mod = m()
    hdr, rows = fixture()
    sink_dir = _tmpdir()
    api = FakeSheetsAPI()
    with _Env(TFB_WARNINGS_COMPACT="enforce", TFB_WARNINGS_SINK_DIR=sink_dir):
        res = _res(mod, dry_run=True)
        got = mod._wc_publish_seam(_task(mod), res, _writer(mod, api), "BOOK", hdr, rows)
    assert got is rows and api.requests == [] and os.listdir(sink_dir) == []
    assert res._stamp_meta["wc"]["status"] == "WOULD_COMPACT" and res._stamp_meta["wc"]["mode"] == "enforce"
    # _run_one_task(dry_run=True) returns 'skipped' BEFORE the seam / write_table (C1)
    class Backend(object):
        async def post_json(self, *_a, **_k):
            raise AssertionError("dry run must not call the backend")

    class Writer(mod.SheetsWriter):
        def __init__(self):
            mod.SheetsWriter.__init__(self)
            self._service = api

        def write_table(self, *_a, **_k):
            raise AssertionError("dry run must not write")
    saved = mod._read_symbols
    mod._read_symbols = lambda *_a, **_k: ["GMFIX.US"]
    try:
        with _Env(TFB_WARNINGS_COMPACT="enforce", TFB_WARNINGS_SINK_DIR=sink_dir):
            result = asyncio.run(mod._run_one_task(_task(mod), "BOOK", "A5", -1, False, True, Backend(), Writer()))
    finally:
        mod._read_symbols = saved
    assert result.status == "skipped" and result.dry_run is True and api.requests == [] and os.listdir(sink_dir) == []
    return {"seam_status": res._stamp_meta["wc"]["status"], "task_status": result.status}


def test_t10_dry_run_never_sinks_or_appends():
    _t10()


# T11 - gate-word preservation + no penalty re-fire -----------------------------------------
GATE_RE = re.compile(r"(cap|forecast|target|roi|drop|reject|provider_target|price_bar_stale|xprovider_price_conflict|"
                     r"identity_quarantined|sheet_tuple_conflict|acquisition_status|sheet_margin_unit|"
                     r"margin_publish(?!.*:observe)|fetch_failed|low_data_trust|analyst_lkg|"
                     r"confidence_default_suspected|f7_settle_failed|rel_path)")


def _t11():
    mod = m()
    from core.sheet_presentation import present_instrument_row
    hdr, rows = fixture()
    wi = _warn_col(hdr)
    checked = kept = 0
    for r in rows:
        text, removed = mod._wc_compact_cell(r[wi])
        out_parts = set(mod._wc_cell_parts(text))
        for tok in mod._wc_cell_parts(r[wi]):
            if GATE_RE.search(tok):
                checked += 1
                if mod._wc_token_movable(tok):
                    assert tok in removed and tok.endswith(":observe") or mod._wc_token_head(tok) in (
                        "rel_path", "intrinsic_rejected_outlier(x0.2)", "market_cap_subunit_normalized_to_major",
                        "percent_change_suspect_dropped", "previous_close_incoherent_dropped"), tok
                else:
                    assert tok in out_parts, tok
                    kept += 1
        # no penalty / receipt re-fire: the projection of the compacted row equals the original on every other key
        p_full = present_instrument_row(dict(zip(hdr, r)))
        rc = list(r)
        rc[wi] = text
        p_comp = present_instrument_row(dict(zip(hdr, rc)))
        for k in hdr:
            if k != "Warnings":
                assert p_full[k] == p_comp[k], (r[hdr.index("Symbol")], k)
        for tok in mod._wc_cell_parts(p_comp["Warnings"]):
            assert tok in set(mod._wc_cell_parts(p_full["Warnings"])) or not mod._wc_token_movable(tok), tok
    assert checked >= 20 and kept >= 14
    # every head of the reader census stays non-movable
    for head in ("acquisition_status", "acquisition_provider", "acquisition_acquired_at", "acquisition_quote_asof",
                 "sheet_margin_unit", "fetch_failed", "empty_row_no_provider_data", "identity_quarantined",
                 "kept_last_good", "no_data_stub", "placeholder_stub", "price_unverified_live", "price_bar_stale",
                 "operator_quarantine", "pl1_quarantined", "persist_sanity_quarantined", "xprovider_price_conflict",
                 "xprovider_verified", "low_data_trust", "rank_skipped_low_trust", "confidence_default_suspected",
                 "analyst_lkg", "f7_settle_failed", "rank_skipped_settlement_failure",
                 "provider_target_implausible_dropped_12m", "provider_target_12m_capped_to_phase_ii_ceiling",
                 "provider_target_capped_for_short_horizon_derivation", "quote_current_price_missing",
                 "quote_exchange_missing", "quote_currency_missing", "row_sanity",
                 "recommendation_forced_hold_missing_price", "name_from_chart_meta", "sheet_tuple_conflict",
                 "sheet_tuple_unknown", "sheet_margin_unknown", "sheet_quote_conflict", "dq_capped",
                 "pe_incoherent_cleared", "ohlc_incoherent_dropped", "operator_quarantine_stub", "crypto_pair_shape",
                 "fund_coherence_quarantined", "fund_coherence_repaired", "identity_guard_v1.3.0", "quote_attempt",
                 "eodhd_fundamentals_fallback_applied", "yahoo_enrichment_applied", "intraday_quote_refreshed",
                 "forecast_capped_at_ceiling", "klg", "horizon_label", "settle_unresolved", "momentum_only_fallback"):
        assert not mod._wc_token_movable(head) and not mod._wc_token_movable(head + ":x:y"), head
    from core.data_validity import _INVALID_WARNINGS, _TOKEN_KEYS
    for head in list(_TOKEN_KEYS) + re.findall(r"[a-z_0-9]+", _INVALID_WARNINGS.pattern.split("(?:", 1)[1]):
        assert head in ("", ) or not mod._wc_token_movable(head + ":x"), head
    return {"checked": checked, "kept": kept}


def test_t11_gate_word_preservation_and_no_refire():
    _t11()


# T12 - reader census -----------------------------------------------------------------------
WRITER_FILES = {"core/data_engine_v2.py", "core/enriched_quote.py", "core/sheet_presentation.py",
                "scripts/repo_hygiene_check.py", "scripts/audit_data_quality.py"}
CLASSIFIED_NON_READERS = {"core/analysis/identity_guard.py", "scripts/harness_ob_1_13_0.py",
                          "scripts/diag_eodhd_fundamentals.py"}


def _t12():
    mod = m()
    heads = sorted(mod._WC_MOVABLE_HEADS)
    pats = {}
    for h in heads:
        if h == "sanitized":   # a plain English word: only its tag form counts
            pats[h] = re.compile(r"(?<![\w.])sanitized(?=[:\"'])")
        else:
            pats[h] = re.compile(r"(?<![\w.])" + re.escape(h) + r"(?![\w])")
    hits = {}
    sync_rel = os.path.relpath(SYNC, ROOT)
    for sub in ("core", "routes", "scripts", "apps_script", "integrations"):
        for dp, _dn, fn in os.walk(os.path.join(ROOT, sub)):
            if "harness_archive" in dp or "__pycache__" in dp:
                continue
            for f in fn:
                if not f.endswith((".py", ".gs", ".js")):
                    continue
                rel = os.path.relpath(os.path.join(dp, f), ROOT).replace("\\", "/")
                if rel in ("scripts/run_dashboard_sync.py", sync_rel) or rel.startswith("core/providers/"):
                    continue
                with open(os.path.join(dp, f), encoding="utf-8", errors="replace") as fh:
                    for i, line in enumerate(fh, 1):
                        s = line.strip()
                        if s.startswith("#") or s.startswith("//") or s.startswith("*"):
                            continue
                        for h, pat in pats.items():
                            if pat.search(line):
                                hits.setdefault(rel, []).append((i, h))
    unexpected = {k: v for k, v in hits.items() if k not in WRITER_FILES | CLASSIFIED_NON_READERS}
    assert not unexpected, "new reader of a MOVABLE head: %s" % json.dumps(unexpected)
    assert "core/sheet_presentation.py" in hits          # the sheet_horizon_label writer is present
    return {k: len(v) for k, v in sorted(hits.items())}


def test_t12_reader_census_whitelisted_writers_only():
    _t12()


# T13 - the real 2026-10-10 export (frozen numbers; B2-corrected set) -------------------------
ACCEPT = {
    "Market_Leaders": {"rows": 255, "mean_chars_before": 981.6, "mean_tags_before": 21.32, "max_tags_before": 28,
                       "mean_chars_after": 408.5, "mean_tags_after": 9.76, "max_tags_after": 15,
                       "rows_gt5_before": 255, "rows_gt5_after": 255, "chars_before": 250304},
    "Global_Markets": {"rows": 6609, "mean_chars_before": 1052.5, "mean_tags_before": 24.01, "max_tags_before": 35,
                       "mean_chars_after": 403.3, "mean_tags_after": 10.07, "max_tags_after": 19,
                       "rows_gt5_before": 6526, "rows_gt5_after": 6518, "chars_before": 6956079},
    "Commodities_FX": {"rows": 453, "mean_chars_before": 714.3, "mean_tags_before": 16.65, "max_tags_before": 23,
                       "mean_chars_after": 252.1, "mean_tags_after": 7.63, "max_tags_after": 11,
                       "rows_gt5_before": 453, "rows_gt5_after": 416, "chars_before": 323576},
    "Mutual_Funds": {"rows": 2474, "mean_chars_before": 761.4, "mean_tags_before": 17.89, "max_tags_before": 23,
                     "mean_chars_after": 224.1, "mean_tags_after": 6.98, "max_tags_after": 10,
                     "rows_gt5_before": 2474, "rows_gt5_after": 2378, "chars_before": 1883676},
}
ACCEPT_TOTAL = {"rows": 9791, "chars_before": 9413635, "chars_after": 3437827, "chars_removed": 5975808,
                "rows_gt5_before": 9708, "rows_gt5_after": 9567}
ACCEPT_FAMILIES = {
    "eq_roi_backfill": 26442, "margin_publish": 19621, "sheet_horizon_label": 18776,
    "analyst_trend_block_applied": 9669, "rel_path": 9662, "f7_settle": 9515,
    "quote_currency_from_suffix": 8604, "quote_exchange_from_suffix": 8604, "fund_cache": 8311,
    "intrinsic_soft_ceiling_applied": 2323, "sanitized": 1245, "fund_unit_contract": 1007,
    "bearish_reco_high_modeled_upside": 909, "fundamentals_lkg": 419, "sector_from_eodhd_verified": 326,
    "yahoo_chart_enrichment_applied": 129, "fund_coherence_skipped": 120, "bar_age_failover_exhausted": 98,
    "intrinsic_rejected_outlier(x0.2)": 93, "provider_route": 81, "market_cap_subunit_normalized_to_major": 76,
    "revenue_currency_suspect": 38, "intrinsic_rejected_outlier(x0.1)": 34, "market_cap_currency_suspect": 25,
    "fctuple_vintage": 20, "market_cap_subunit_unverified_no_shares": 14, "percent_change_suspect_dropped": 5,
    "previous_close_incoherent_dropped": 1,
}


def _export_available():
    if not EXPORT or not os.path.exists(EXPORT):
        return False
    try:
        import openpyxl  # noqa: F401
    except Exception:
        return False
    return True


def _t13():
    if not _export_available():
        return "skipped"
    mod = m()
    import openpyxl
    wb = openpyxl.load_workbook(EXPORT, read_only=True, data_only=True)
    out, fam_all, total = {}, {}, {"rows": 0, "chars_before": 0, "chars_after": 0, "rows_gt5_before": 0, "rows_gt5_after": 0}
    for pg in PAGES:
        ws = wb[pg]
        it = ws.iter_rows(values_only=True)
        hdr = [str(h) if h is not None else "" for h in next(it)]
        si, wi = hdr.index("Symbol"), hdr.index("Warnings")
        rows = []
        for r in it:
            if si >= len(r) or r[si] is None or str(r[si]).strip() == "":
                continue
            rows.append(list(r) + [None] * (len(hdr) - len(r)))
        o_obs, st_obs, rec_obs = mod._wc_compact_rows(pg, hdr, rows, "observe")
        assert o_obs is rows and rec_obs == []
        o_enf, st_enf, rec_enf = mod._wc_compact_rows(pg, hdr, rows, "enforce")
        assert o_enf is not rows and len(rec_enf) == st_enf["rows_with_removable"]
        for k, v in ACCEPT[pg].items():
            assert st_enf[k] == v, (pg, k, st_enf[k], v)
            assert st_obs[k] == v, (pg, k, "observe")
        assert st_enf["rows_empty"] == 0            # Warnings non-empty on 100 % of live rows
        lost = 0
        for r_in, r_out in zip(rows, o_enf):
            outset = set(mod._wc_cell_parts(r_out[wi]))
            for t in mod._wc_cell_parts(r_in[wi]):
                if not mod._wc_token_movable(t) and t not in outset:
                    lost += 1
        assert lost == 0, (pg, lost)
        _, st2, rec2 = mod._wc_compact_rows(pg, hdr, o_enf, "enforce")
        assert st2["tags_removed"] == 0 and rec2 == []
        for k, v in st_enf["removed_families"].items():
            fam_all[k] = fam_all.get(k, 0) + v
        for k in total:
            total[k] += st_enf[k]
        out[pg] = {k: st_enf[k] for k in ("rows", "mean_chars_before", "mean_chars_after", "mean_tags_before",
                                           "mean_tags_after", "max_tags_before", "max_tags_after", "rows_gt5_before",
                                           "rows_gt5_after", "chars_before", "chars_after", "chars_removed",
                                           "tags_removed", "rows_with_removable")}
    total["chars_removed"] = total["chars_before"] - total["chars_after"]
    assert total == ACCEPT_TOTAL, total
    assert fam_all == ACCEPT_FAMILIES, fam_all
    assert round(100.0 * total["chars_removed"] / total["chars_before"], 1) == 63.5
    out["total"] = total
    out["families"] = fam_all
    return out


def test_t13_real_export_frozen_numbers():
    if not _export_available():
        import pytest
        pytest.skip("TFB_TEST_EXPORT_XLSX not set or openpyxl unavailable")
    _t13()


def _run_all():
    return {"t1": _t1(), "t2": _t2(), "t3": _t3(), "t4": _t4(), "t5": _t5(), "t6": _t6(), "t7": _t7(),
            "t8": _t8(), "t9": _t9(), "t10": _t10(), "t11": _t11(), "t12": _t12(), "t13": _t13()}


if __name__ == "__main__":
    digests, last = [], None
    for i in range(3):
        last = _run_all()
        digests.append(hashlib.sha256(json.dumps(last, sort_keys=True, default=str).encode()).hexdigest()[:12])
    export = last["t13"]
    print("T1..T13 PASS x3 | base:", last["t2"]["base"], "| export:",
          "skipped" if export == "skipped" else json.dumps(export["total"], sort_keys=True))
    if export != "skipped":
        for pg in PAGES:
            print("  %s %s" % (pg, json.dumps(export[pg], sort_keys=True)))
        print("  removed families:", json.dumps(export["families"], sort_keys=True))
    print("digests:", " ".join(digests), "identical=%s" % (len(set(digests)) == 1))
