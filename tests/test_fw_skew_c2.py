"""run_dashboard_sync v6.70.0 FW-SKEW (C2): classify FW-2 P/E-coherence quarantines.

Production evidence (GitHub Actions, 7 Global_Markets passes 10-14..10-16): every
backend call returned 200 with fetch_failed=0, yet fresh coverage sat at 91.6-93.2%
because FW-2 stripped 70-359 rows per pass (fresh_rows = 6412 - strips). A row is
stripped when |Price/EPS - P/E| / P/E >= 5%; a live price against a P/E computed at
an older fundamentals snapshot trips that rule without any identity problem.

The real runner (_run_one_task with a fake backend and writer) proves:
  off      -> v6.69.0 behaviour, no FW-SKEW line
  observe  -> published rows identical to off, one FW-SKEW line
  enforce  -> the skew-explained row keeps its fresh cells; the identity-broken
              row is stripped and restored from last-good exactly as before
"""
from datetime import datetime, timezone
from unittest import mock
import asyncio

import pytest

from core.sheets.schema_registry import get_sheet_headers
from scripts import run_dashboard_sync as sync

NOW = datetime(2026, 10, 16, 0, 30, tzinfo=timezone.utc)
HEADERS = get_sheet_headers("Global_Markets")


def quote(symbol, price, eps, pe, low, high, **changes):
    row = {"Symbol": symbol, "Name": symbol.split(".")[0] + " Corp", "Current Price": price,
           "EPS (TTM)": eps, "P/E (TTM)": pe, "52W Low": low, "52W High": high,
           "Data Provider": "eodhd", "Last Updated (UTC)": "2026-10-16T00:20:00Z",
           "Last Updated (Riyadh)": "2026-10-16T03:20:00+03:00", "Warnings": ""}
    row.update(changes)
    return row


def matrix(rows):
    return [[row.get(h, "") for h in HEADERS] for row in rows]


COHERENT = quote("AAA.US", 100.0, 5.0, 20.0, 80.0, 120.0)
SKEW = quote("HOOD.US", 120.0, 2.0, 50.0, 60.0, 140.0)        # snapshot price 100, +20% since
BROKEN = quote("IBO.US", 120.0, 2.0, 7.5, 60.0, 140.0)        # snapshot price 15: another instrument
PRIOR = [quote("HOOD.US", 101.0, 2.0, 50.0, 60.0, 140.0, **{"Last Updated (UTC)": "2026-10-15T00:20:00Z"}),
         quote("IBO.US", 118.0, 2.0, 59.0, 60.0, 140.0, **{"Last Updated (UTC)": "2026-10-15T00:20:00Z"})]


@pytest.mark.parametrize("args,want", [
    ((120.0, 2.0, 50.0, 140.0, 60.0, 0.35), (True, "explained")),
    ((120.0, 2.0, 7.5, 140.0, 60.0, 0.35), (False, "snap_outside_52w")),
    ((150.0, 2.0, 50.0, 140.0, 60.0, 0.35), (False, "px_outside_52w")),
    ((135.0, 2.0, 47.5, 140.0, 60.0, 0.35), (False, "rel_over_cap")),
    ((120.0, 2.0, 50.0, None, 60.0, 0.35), (False, "no_52w")),
    ((120.0, 2.0, 50.0, 50.0, 60.0, 0.35), (False, "range_invalid")),
    ((120.0, 2.0, 50.0, 0.0, 0.0, 0.35), (False, "range_invalid")),
    ((142.0, 2.0, 50.0, 140.0, 60.0, 0.45), (True, "explained")),     # within the +2% range margin
])
def test_classifier_table(args, want):
    ok, rel, why = sync._fw_skew_explained(*args)
    assert (ok, why) == want, (ok, rel, why)


@pytest.mark.parametrize("raw,mode", [(None, "off"), ("", "off"), ("observe", "observe"),
                                      ("ENFORCE", "enforce"), ("on", "off"), ("1", "off")])
def test_mode_parsing(monkeypatch, raw, mode):
    if raw is None:
        monkeypatch.delenv("TFB_SYNC_FW_SKEW_AWARE", raising=False)
    else:
        monkeypatch.setenv("TFB_SYNC_FW_SKEW_AWARE", raw)
    assert sync._fw_skew_mode() == mode


@pytest.mark.parametrize("raw,val", [(None, 0.35), ("0.2", 0.2), ("9", 1.0), ("0", 0.05), ("x", 0.35)])
def test_max_rel_clamped(monkeypatch, raw, val):
    if raw is None:
        monkeypatch.delenv("TFB_SYNC_FW_SKEW_MAX_REL", raising=False)
    else:
        monkeypatch.setenv("TFB_SYNC_FW_SKEW_MAX_REL", raw)
    assert sync._fw_skew_max_rel() == val


def test_firewall_without_skew_is_the_v6690_path():
    rows_a = matrix([COHERENT, SKEW, BROKEN])
    rows_b = matrix([COHERENT, SKEW, BROKEN])
    out_a, s_a = sync._row_identity_firewall(HEADERS, rows_a)
    out_b, s_b = sync._row_identity_firewall(HEADERS, rows_b, None)
    assert out_a == out_b and s_a == s_b == ["HOOD.US", "IBO.US"]


def test_firewall_observe_and_enforce_classification():
    obs = {"enforce": False, "max_rel": 0.35}
    out_o, s_o = sync._row_identity_firewall(HEADERS, matrix([COHERENT, SKEW, BROKEN]), obs)
    assert s_o == ["HOOD.US", "IBO.US"]
    assert [(r[0], r[1], r[3]) for r in obs["rows"]] == [("HOOD.US", True, "explained"),
                                                          ("IBO.US", False, "snap_outside_52w")]
    enf = {"enforce": True, "max_rel": 0.35}
    out_e, s_e = sync._row_identity_firewall(HEADERS, matrix([COHERENT, SKEW, BROKEN]), enf)
    assert s_e == ["IBO.US"] and enf["kept"] == ["HOOD.US"]
    assert out_e[1] == matrix([SKEW])[0]                       # untouched fresh cells
    line = sync._fw_skew_line("Global_Markets", "enforce", enf)
    assert "{" not in line and "}" not in line
    assert ("would_strip=2 explained=1 kept=1 reasons=explained:1,snap_outside_52w:1 "
            "rel_explained=10-20:1") in line, line
    assert "sample_explained=[HOOD.US:20.0%]" in line and "sample_unexplained=[IBO.US:snap_outside_52w:" in line


def _run_page(monkeypatch, mode):
    class Backend:
        async def post_json(self, _endpoint, _payload):
            return {"headers": HEADERS, "rows_matrix": matrix([COHERENT, SKEW, BROKEN])}, None, 200

    class Writer:
        published = []

        def _get_service(self):
            return object()

        def read_values(self, *_a, **_k):
            return [HEADERS] + matrix(PRIOR)

        def write_table(self, _sid, _page, _start, headers, rows):
            self.published = [list(r) for r in rows]
            return len(rows)

        def clear_from(self, *_a, **_k):
            pass

    env = {"TFB_MARKET_SYMBOL_READBACK": "0", "TFB_SYNC_SYMBOL_BATCH_SIZE": "0",
           "TFB_SYNC_PERSISTENCE_HARD": "0", "TFB_SYNC_ROW_ID_FIREWALL": "1",
           "TFB_SYNC_NAME_DEDUP_MODE": "off", "TFB_SYNC_OHLC_LAKE": "0",
           "TFB_SYNC_FALSE_GREEN_SCREEN": "0", "TFB_SYNC_STATUS_STAMP": "0",
           "TFB_SYNC_FETCHFAIL_TRUTH": "off", "TFB_SYNC_IDENTITY_TRIPWIRE": "0",
           "TFB_SYNC_COHERENCE_TRIPWIRE": "0", "TFB_SYNC_IDFW_RUNLOG": "0"}
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    if mode is None:
        monkeypatch.delenv("TFB_SYNC_FW_SKEW_AWARE", raising=False)
    else:
        monkeypatch.setenv("TFB_SYNC_FW_SKEW_AWARE", mode)
    writer = Writer()
    with mock.patch.object(sync, "_utc_now", return_value=NOW), \
            mock.patch.object(sync, "_read_symbols", return_value=["AAA.US", "HOOD.US", "IBO.US"]):
        result = asyncio.run(sync._run_one_task(sync.TaskSpec("GLOBAL_MARKETS", "Global_Markets", "analysis"),
                                                "offline", "A1", -1, False, False, Backend(), writer))
    published = {r[HEADERS.index("Symbol")]: dict(zip(HEADERS, r)) for r in writer.published}
    skew_lines = [w for w in result.warnings if w.startswith("[FW-SKEW")]
    return result, published, skew_lines


def test_real_runner_off_observe_enforce(monkeypatch):
    r_off, p_off, l_off = _run_page(monkeypatch, None)
    r_obs, p_obs, l_obs = _run_page(monkeypatch, "observe")
    r_enf, p_enf, l_enf = _run_page(monkeypatch, "enforce")
    # off: both mismatches quarantined and restored from last-good; no new line
    assert not l_off
    assert p_off["HOOD.US"]["Current Price"] == 101.0 and p_off["IBO.US"]["Current Price"] == 118.0
    assert "identity_quarantined" in p_off["HOOD.US"]["Warnings"]
    # observe: byte-identical rows, one line
    assert p_obs == p_off and len(l_obs) == 1
    assert "mode=observe would_strip=2 explained=1 kept=0" in l_obs[0], l_obs
    # enforce: skew row keeps its fresh cells, broken row restored as before
    assert len(l_enf) == 1 and "mode=enforce would_strip=2 explained=1 kept=1" in l_enf[0], l_enf
    assert p_enf["HOOD.US"]["Current Price"] == 120.0
    assert "identity_quarantined" not in p_enf["HOOD.US"]["Warnings"]
    assert "acquisition_status:preserved" not in p_enf["HOOD.US"]["Warnings"]
    assert p_enf["IBO.US"] == p_off["IBO.US"] and p_enf["AAA.US"] == p_off["AAA.US"]
    f_off = sync._page_fresh_fetch_metrics(r_off)
    f_enf = sync._page_fresh_fetch_metrics(r_enf)
    assert f_enf[0] == f_off[0] + 1, (f_off, f_enf)
