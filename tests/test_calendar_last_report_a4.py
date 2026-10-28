"""Last report date (program register A4): calendar_provider v1.3.0 +
run_calendar_sync v1.2.0 behind TFB_CAL_LAST_REPORT (default off).

REAL modules; the only stubs are an in-memory Calendar_Events worksheet, the
harvest page, and the provider's HTTP seam (_get_json). No network, no
workbook.

L1  provider: latest report inside [today-back, today-1] per symbol; window
    bounds; 'percent' preferred, else (actual-estimate)/|estimate|; zero or
    missing estimate -> None; KSA never sent; a failed batch drops only
    itself; disabled -> {}; the request window and symbols are exact
L2  provider: fetch_event_context is untouched (no past-window request)
L3  sync OFF golden: with the gate unset / off / junk, main() reads and
    writes exactly what the untouched v1.1.4 base (git show 53c8544e) does -
    same read ranges, same update request, same log lines (version tag
    normalised)
L4  sync ON, first run on a 7-column tab: prior read stays A1:G (a wider
    read would leave the grid), the grid grows to 10 columns, one atomic
    A1:J write with the 10-column header, last-report cells per row, the
    summary line
L5  sync ON, 10-column tab: prior read A1:J; a 429-style empty fetch keeps
    the prior report inside the window (carried), drops an expired one, and
    never resurrects an unharvested symbol for its last report alone
L6  sync ON, the provider import or fetch raises: WARN, prior reports only,
    the publication still happens
L7  review fix (Codex P2 on PR #850): ON -> OFF with the rows reordered ->
    ON again with an empty fetch. The off run blanks H:J (header included) in
    its one atomic A1:J update, so the re-arm carries nothing (without the
    fix ZTO.US inherited FRST.US's -22.2%). An off run on a wide tab whose
    H1:J1 is not the last-report header keeps the A1:G write.
"""
from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import date, timedelta
import importlib.util
from pathlib import Path
import re
import subprocess
import sys
from types import ModuleType

import pytest

ROOT = Path(__file__).resolve().parents[1]
BASE_REV = "53c8544e"   # main before v1.2.0 / v1.3.0


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


sync = _load(ROOT / "scripts" / "run_calendar_sync.py", "calendar_last_report_target")
TODAY = date(2026, 10, 27)
ASOF = "2026-10-26 11:18"


def _base_sync():
    try:
        blob = subprocess.run(["git", "-C", str(ROOT), "show", BASE_REV + ":scripts/run_calendar_sync.py"],
                              capture_output=True, timeout=60)
    except Exception:
        return None
    if blob.returncode != 0 or not blob.stdout:
        return None
    tmp = ROOT / ".pytest_cache_a4_base_calendar_sync.py"
    tmp.write_bytes(blob.stdout)
    try:
        return _load(tmp, "calendar_last_report_base")
    finally:
        tmp.unlink()


# ---------------------------------------------------------------- provider
@pytest.fixture
def provider(monkeypatch):
    import core.providers.calendar_provider as cp
    monkeypatch.setattr(cp, "is_enabled", lambda: True)
    monkeypatch.setattr(cp, "_today", lambda: TODAY)
    monkeypatch.delenv("TFB_CALENDAR_DAYS_BACK", raising=False)
    monkeypatch.delenv("TFB_CALENDAR_EARNINGS_CHUNK", raising=False)
    return cp


def _payload(*items):
    return {"earnings": list(items)}


def test_l1_provider_latest_report_surprise_window_and_ksa(provider, monkeypatch):
    calls = []

    async def fake_get_json(_client, path, params):
        calls.append((path, dict(params)))
        return _payload(
            {"code": "FRST.US", "report_date": "2026-10-22", "before_after_market": "AfterMarket",
             "actual": 0.35, "estimate": 0.45, "percent": None},
            {"code": "FRST.US", "report_date": "2026-07-23", "actual": 0.5, "estimate": 0.4, "percent": 25},
            {"code": "FRST.US", "report_date": "2026-10-09", "actual": 0.4, "estimate": 0.4, "percent": 0},
            {"code": "ZTO.US", "report_date": "2026-10-20", "actual": 2.1, "estimate": 2.0, "percent": 5.0},
            {"code": "ZERO.US", "report_date": "2026-10-21", "actual": 0.1, "estimate": 0, "percent": None},
            {"code": "TODAY.US", "report_date": "2026-10-27", "actual": 1, "estimate": 1, "percent": 0},
            {"code": "OLD.US", "report_date": "2026-10-05", "actual": 1, "estimate": 1, "percent": 0},
            {"code": "GHOST.US", "report_date": "2026-10-21", "actual": 1, "estimate": 1, "percent": 0},
            "not a dict",
        )

    monkeypatch.setattr(provider, "_get_json", fake_get_json)
    out = provider.fetch_last_earnings_sync(["FRST.US", "ZTO.US", "ZERO.US", "TODAY.US", "OLD.US", "2222.SR"])
    assert set(out) == {"FRST.US", "ZTO.US", "ZERO.US"}
    assert out["FRST.US"] == {"last_earnings_date": "2026-10-22", "eps_surprise_pct": -22.2,
                              "eps_actual": 0.35, "eps_estimate": 0.45, "report_timing": "AfterMarket"}
    assert out["ZTO.US"]["eps_surprise_pct"] == 5.0 and out["ZTO.US"]["report_timing"] is None
    assert out["ZERO.US"]["eps_surprise_pct"] is None, "estimate 0 -> no surprise"
    assert len(calls) == 1 and calls[0][0] == "/calendar/earnings"
    params = calls[0][1]
    assert params["from"] == "2026-10-06" and params["to"] == "2026-10-26"
    assert "2222" not in params["symbols"] and "FRST.US" in params["symbols"]


def test_l1_provider_window_env_batches_and_failures(provider, monkeypatch):
    monkeypatch.setenv("TFB_CALENDAR_DAYS_BACK", "7")
    monkeypatch.setenv("TFB_CALENDAR_EARNINGS_CHUNK", "1")
    seen = []

    async def fake_get_json(_client, path, params):
        seen.append(params["symbols"])
        assert params["from"] == "2026-10-20" and params["to"] == "2026-10-26"
        if params["symbols"] == "BAD.US":
            raise RuntimeError("429 Too Many Requests")
        return _payload({"code": params["symbols"], "report_date": "2026-10-21",
                         "actual": 1.1, "estimate": 1.0})

    monkeypatch.setattr(provider, "_get_json", fake_get_json)
    out = provider.fetch_last_earnings_sync(["AAA.US", "BAD.US", "CCC.US"])
    assert seen == ["AAA.US", "BAD.US", "CCC.US"], "one batch per symbol at chunk 1"
    assert set(out) == {"AAA.US", "CCC.US"} and out["AAA.US"]["eps_surprise_pct"] == 10.0
    monkeypatch.setattr(provider, "is_enabled", lambda: False)
    assert provider.fetch_last_earnings_sync(["AAA.US"]) == {}
    # 2026-10-27: v1.3.1 (404 batch split, gated) stacked on this change; pin moved
    # 2026-10-27: v1.3.2 (venue batches + EODHD exchange codes, gated); pin moved
    assert provider.__version__ == "1.3.2"


def test_l2_forward_context_untouched(provider, monkeypatch):
    windows = []

    async def fake_get_json(_client, path, params):
        windows.append((path, params.get("from")))
        return _payload() if path == "/calendar/earnings" else []

    monkeypatch.setattr(provider, "_get_json", fake_get_json)
    monkeypatch.setattr(provider, "_yahoo_fallback_enabled", lambda: False)
    monkeypatch.setattr(provider, "_flag_enabled", lambda: True)
    ctx = provider.fetch_event_context_sync(["FRST.US"])
    assert ctx == {"FRST.US": {"next_earnings_date": None, "next_ex_div_date": None}}
    assert all(frm == TODAY.isoformat() for _p, frm in windows), "no past-window request on the forward path"


# -------------------------------------------------------------------- sync
class Sheet:
    def __init__(self, rows, cols=7, row_count=1000):
        self.values = deepcopy(rows)
        self.col_count = cols
        self.row_count = row_count
        self.reads, self.requests, self.resizes = [], [], []

    def get(self, range_name):
        self.reads.append(range_name)
        m = re.fullmatch(r"A1:([A-Z])(\d+)", range_name)
        assert m, range_name
        width = ord(m.group(1)) - ord("A") + 1
        assert width <= self.col_count, "read beyond the grid"
        return [row[:width] for row in deepcopy(self.values)]

    def resize(self, rows=None, cols=None):
        self.resizes.append((rows, cols))
        if rows is not None:
            assert rows >= self.row_count
            self.row_count = rows
        if cols is not None:
            assert cols >= self.col_count
            self.col_count = cols

    def update(self, *, values, range_name, value_input_option):
        m = re.fullmatch(r"A1:([A-Z])(\d+)", range_name)
        width = ord(m.group(1)) - ord("A") + 1
        assert width <= self.col_count and int(m.group(2)) == len(values) <= self.row_count
        assert all(len(r) == width for r in values) and value_input_option == "RAW"
        self.requests.append({"values": deepcopy(values), "range": range_name})
        self.values = deepcopy(values)

    def freeze(self, *, rows):
        assert rows == 1


class Book:
    def __init__(self, sheet, symbols):
        self.sheet, self.symbols = sheet, symbols

    def worksheet(self, name):
        if name == "Calendar_Events":
            return self.sheet
        return type("Page", (), {"get": lambda _s, _r: [["Symbol"]] + [[s] for s in self.symbols]})()

    def add_worksheet(self, *, title, rows, cols):  # pragma: no cover - not used
        raise AssertionError("tab exists in these legs")


def _run(mod, monkeypatch, sheet, symbols, ctx, last=None, last_raises=None, gate=None, capsys=None):
    book = Book(sheet, symbols)
    prov = ModuleType("core.providers.calendar_provider")
    prov.__version__ = "synthetic"
    prov.is_enabled = lambda: True
    prov.fetch_event_context_sync = lambda _s: deepcopy(ctx)

    def _last(_symbols, _window=None):
        if last_raises:
            raise last_raises
        return deepcopy(last or {})
    prov.fetch_last_earnings_sync = _last
    monkeypatch.setitem(sys.modules, "core.providers.calendar_provider", prov)
    monkeypatch.setattr(mod, "_open_book", lambda: book)
    monkeypatch.setattr(mod, "_today_riyadh", lambda: TODAY)
    monkeypatch.setenv("TFB_CALENDAR_PAGES", "Top_10_Investments")
    monkeypatch.delenv("TFB_CALENDAR_TICKER_GUARD", raising=False)
    monkeypatch.delenv("TFB_CALENDAR_DAYS_BACK", raising=False)
    if gate is None:
        monkeypatch.delenv("TFB_CAL_LAST_REPORT", raising=False)
    else:
        monkeypatch.setenv("TFB_CAL_LAST_REPORT", gate)
    rc = mod.main(["--write"])
    return rc, book


def _prior7():
    return [list(sync.HEADERS), ["ZTO.US", "2026-11-18", 22, "", "", ASOF, "eodhd via calendar_provider v1.2.2"]]


CTX = {"FRST.US": {"next_earnings_date": "2027-01-28", "next_ex_div_date": "2026-11-06"},
       "ZTO.US": {"next_earnings_date": "2026-11-18", "next_ex_div_date": None}}
LAST = {"FRST.US": {"last_earnings_date": "2026-10-22", "eps_surprise_pct": -22.2,
                    "eps_actual": 0.35, "eps_estimate": 0.45, "report_timing": "AfterMarket"}}


@pytest.mark.parametrize("gate", [None, "off", "junk"])
def test_l3_off_is_v114_byte_for_byte(monkeypatch, capsys, gate):
    base = _base_sync()
    if base is None:
        pytest.skip("git show %s unavailable (shallow clone)" % BASE_REV)
    s_new, s_old = Sheet(_prior7()), Sheet(_prior7())
    rc_new, _ = _run(sync, monkeypatch, s_new, ["FRST.US", "ZTO.US"], CTX, LAST, gate=gate)
    err_new = capsys.readouterr().err
    rc_old, _ = _run(base, monkeypatch, s_old, ["FRST.US", "ZTO.US"], CTX, LAST, gate=gate)
    err_old = capsys.readouterr().err
    assert rc_new == rc_old == 0
    assert s_new.reads == s_old.reads == ["A1:G1000"]
    assert s_new.requests == s_old.requests and s_new.resizes == s_old.resizes == []
    assert s_new.requests[0]["range"] == "A1:G1000" and s_new.requests[0]["values"][0] == sync.HEADERS
    assert err_new.replace("v1.2.0", "V") == err_old.replace("v1.1.4", "V")
    assert "last_report" not in err_new


def test_l4_on_first_run_grows_the_grid_and_writes_ten_columns(monkeypatch, capsys):
    sheet = Sheet(_prior7(), cols=7)
    rc, _ = _run(sync, monkeypatch, sheet, ["FRST.US", "ZTO.US"], CTX, LAST, gate="on")
    err = capsys.readouterr().err
    assert rc == 0
    assert sheet.reads == ["A1:G1000"], "a 7-column tab is read within its grid"
    assert sheet.resizes == [(None, 10)]
    req = sheet.requests[0]
    assert req["range"] == "A1:J1000"
    assert req["values"][0] == sync.HEADERS + ["Last Earnings Date", "Days Since Earnings", "EPS Surprise %"]
    body = {r[0]: r for r in req["values"][1:] if r[0]}
    assert body["FRST.US"][7:] == ["2026-10-22", 5, -22.2]
    assert body["ZTO.US"][7:] == ["", "", ""]
    assert body["FRST.US"][:3] == ["FRST.US", "2027-01-28", 93], "forward columns unchanged"
    assert "last_report: with=1 fresh=1 carried=0 window=21d" in err


def test_l5_on_carry_inside_window_expire_outside_never_resurrect(monkeypatch, capsys):
    hdr = sync.HEADERS + sync.LAST_REPORT_HEADERS
    prior = [hdr,
             ["FRST.US", "2027-01-28", 93, "", "", ASOF, "eodhd", "2026-10-22", 4, -22.2],
             ["ZTO.US", "2026-11-18", 22, "", "", ASOF, "eodhd", "2026-09-20", 37, 3.0],
             ["GONE.US", "", "", "", "", ASOF, "eodhd", "2026-10-21", 5, 1.0]]
    sheet = Sheet(prior, cols=10)
    rc, _ = _run(sync, monkeypatch, sheet, ["FRST.US", "ZTO.US"], CTX, {}, gate="on")
    err = capsys.readouterr().err
    assert rc == 0 and sheet.reads == ["A1:J1000"] and sheet.resizes == []
    body = {r[0]: r for r in sheet.requests[0]["values"][1:] if r[0]}
    assert body["FRST.US"][7:] == ["2026-10-22", 5, -22.2], "carried inside the window (the 429 case)"
    assert body["ZTO.US"][7:] == ["", "", ""], "expired report dropped"
    assert "GONE.US" not in body, "a last report never resurrects a symbol"
    assert "last_report: with=1 fresh=0 carried=1 window=21d" in err


@pytest.mark.parametrize("boom", [RuntimeError("429"), ImportError("no provider")])
def test_l6_on_fetch_failure_keeps_publication(monkeypatch, capsys, boom):
    hdr = sync.HEADERS + sync.LAST_REPORT_HEADERS
    prior = [hdr, ["FRST.US", "2027-01-28", 93, "", "", ASOF, "eodhd", "2026-10-22", 4, -22.2]]
    sheet = Sheet(prior, cols=10)
    rc, _ = _run(sync, monkeypatch, sheet, ["FRST.US"], CTX, None, last_raises=boom, gate="on")
    err = capsys.readouterr().err
    assert rc == 0 and sheet.requests and sheet.requests[0]["range"] == "A1:J1000"
    body = {r[0]: r for r in sheet.requests[0]["values"][1:] if r[0]}
    assert body["FRST.US"][7:] == ["2026-10-22", 5, -22.2]
    assert "WARN: last-report fetch failed" in err and "carried=1" in err


def test_l7_off_after_on_clears_h_to_j_so_a_rearm_cannot_misattribute(monkeypatch, capsys):
    hdr = sync.HEADERS + sync.LAST_REPORT_HEADERS
    prior = [hdr,
             ["FRST.US", "2027-01-28", 93, "", "", ASOF, "eodhd", "2026-10-22", 4, -22.2],
             ["ZTO.US", "2026-11-18", 22, "", "", ASOF, "eodhd", "", "", ""]]
    sheet = Sheet(prior, cols=10)
    # OFF, harvest order swapped: ZTO now sits on FRST's old row
    rc, _ = _run(sync, monkeypatch, sheet, ["ZTO.US", "FRST.US"], CTX, LAST, gate="off")
    err = capsys.readouterr().err
    assert rc == 0 and sheet.reads == ["A1:G1000", "A1:J1"] and sheet.resizes == []
    req = sheet.requests[0]
    assert req["range"] == "A1:J1000", "one atomic update covering H:J"
    assert req["values"][0] == list(sync.HEADERS) + ["", "", ""], "the last-report header is blanked too"
    assert all(r[7:] == ["", "", ""] for r in req["values"]), "no stale report survives the off run"
    assert [r[0] for r in req["values"][1:3]] == ["ZTO.US", "FRST.US"]
    assert "last_report: off - clearing H:J left by an earlier ON run" in err
    # ON again inside the window, the fetch returns nothing (a 429): no carry
    rc, _ = _run(sync, monkeypatch, sheet, ["ZTO.US", "FRST.US"], CTX, {}, gate="on")
    err = capsys.readouterr().err
    assert rc == 0 and sheet.reads[-1] == "A1:J1000"
    body = {r[0]: r for r in sheet.requests[-1]["values"][1:] if r[0]}
    assert body["ZTO.US"][7:] == ["", "", ""], "ZTO never inherits FRST's report"
    assert body["FRST.US"][7:] == ["", "", ""]
    assert "last_report: with=0 fresh=0 carried=0 window=21d" in err


def test_l7b_off_on_a_wide_tab_without_the_block_keeps_the_a_to_g_write(monkeypatch, capsys):
    sheet = Sheet([row + ["", "", ""] for row in _prior7()], cols=26)
    rc, _ = _run(sync, monkeypatch, sheet, ["FRST.US", "ZTO.US"], CTX, LAST, gate=None)
    err = capsys.readouterr().err
    assert rc == 0 and sheet.reads == ["A1:G1000", "A1:J1"]
    assert sheet.requests[0]["range"] == "A1:G1000" and sheet.requests[0]["values"][0] == sync.HEADERS
    assert "last_report" not in err


def test_selftest_and_version():
    assert sync.__version__ == "1.2.0"
    out = subprocess.run([sys.executable, str(ROOT / "scripts" / "run_calendar_sync.py"), "--selftest"],
                         capture_output=True, text=True, timeout=120, cwd=str(ROOT))
    assert out.returncode == 0 and "SELFTEST 13/13" in out.stdout, out.stdout + out.stderr
