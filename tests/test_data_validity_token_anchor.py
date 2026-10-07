#!/usr/bin/env python3
"""Harness: core.data_validity v1.0.1 invalid-warning token anchoring.

WHY (2026-10-07): the v1.0.0 pattern matched 'identity_quarantined' inside
the engine's fundamentals-only tag 'fund_identity_quarantined', so a row with
a successfully acquired Yahoo price was classed INVALID and under-counted in
acquisition coverage. Real failure tags still invalidate wherever they start
a token.

Runs as a script ("PASS k/k") and under pytest. Pure, zero network.
Golden negative: TFB_DV_MODULE=<path to a base copy of core/data_validity.py>
loads that file; the fund_identity cases must then fail.
"""
from __future__ import annotations

import importlib.util
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

HARNESS_VERSION = (1, 0, 0)
NOW = datetime(2026, 10, 7, 9, tzinfo=timezone.utc)
FAIL_TOKENS = (
    "fetch_failed", "empty_row_no_provider_data", "identity_quarantined",
    "kept_last_good", "no_data_stub", "placeholder_stub", "price_unverified_live",
    "price_bar_stale", "operator_quarantine", "pl1_quarantined",
    "persist_sanity_quarantined", "xprovider_price_conflict",
)


def _dv():
    alt = os.getenv("TFB_DV_MODULE")
    if not alt:
        from core import data_validity
        return data_validity
    spec = importlib.util.spec_from_file_location("core._dv_base", alt)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _quote(**changes):
    row = {"Symbol": "AAPL.US", "Name": "", "Current Price": 190.0,
           "Data Provider": "yahoo_chart", "Last Updated (UTC)": "2026-10-07T08:59:00Z",
           "Last Updated (Riyadh)": "2026-10-07T11:59:00+03:00", "Warnings": ""}
    row.update(changes)
    return row


def _status(**changes):
    return _dv().row_acquisition(_quote(**changes), NOW, 30 * 3600).status


def test_baseline_clean_row_succeeds():
    assert _status() == "SUCCESS"


def test_fund_identity_quarantine_alone_keeps_price_success():
    assert _status(Warnings="fund_identity_quarantined") == "SUCCESS"


def test_fund_identity_quarantine_in_engine_warning_string():
    # Shape observed from the engine (EODHD realtime unpriced, Yahoo priced,
    # EODHD fundamentals fallback stripped display identity).
    w = ("quote_attempt:eodhd:unpriced; fund_identity_quarantined; "
         "eodhd_fundamentals_fallback_applied; name_unresolved")
    assert _status(Warnings=w) == "SUCCESS"


def test_fund_identity_quarantine_as_list_and_uppercase():
    assert _status(Warnings=["FUND_IDENTITY_QUARANTINED", "name_unresolved"]) == "SUCCESS"


def test_real_identity_quarantine_still_invalid_next_to_fund_tag():
    w = "fund_identity_quarantined; identity_quarantined:kept_last_good:v6.25.1"
    assert _status(Warnings=w) == "INVALID"


def test_every_failure_token_still_invalid_in_every_position():
    for token in FAIL_TOKENS:
        for text in (token, token + ":detail", "x; " + token, "a;" + token,
                     "tag:" + token, token.upper(), "note " + token):
            assert _status(Warnings=text) == "INVALID", text
        assert _status(error=token + ":timeout") == "INVALID", token


def test_word_prefixed_lookalikes_do_not_invalidate():
    for text in ("fund_identity_quarantined", "xfetch_failed_note", "prekept_last_good"):
        assert _status(Warnings=text) == "SUCCESS", text


def test_version_floor():
    v = tuple(int(x) for x in getattr(_dv(), "DATA_VALIDITY_VERSION", "0.0.0").split("."))
    assert v >= (1, 0, 1), v
    assert HARNESS_VERSION >= (1, 0, 0)


TESTS = [obj for name, obj in sorted(globals().items()) if name.startswith("test_") and callable(obj)]


def main() -> int:
    passed = 0
    for fn in TESTS:
        try:
            fn()
            passed += 1
            print("ok   " + fn.__name__)
        except Exception as exc:  # noqa: BLE001
            print("FAIL " + fn.__name__ + ": " + repr(exc)[:200])
    total = len(TESTS)
    print(("PASS" if passed == total else "FAIL") + " %d/%d" % (passed, total))
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
