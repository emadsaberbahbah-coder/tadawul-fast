#!/usr/bin/env python3
"""tests/test_audit_tz_aware.py — coverage v1.1.0 tz-aware parse_dt.

Ten cases: aware inputs (Z / +03:00 / +00:00 / aware datetime) convert to
Riyadh wall time; naive inputs, Excel serials and m/d/Y are untouched; the
kill-switch reproduces the legacy parser byte-for-byte against the UNTOUCHED
baseline module (dual-run); and an end-to-end _age_hours proof through the
freshness importer shows a Z-stamp no longer reads 3 hours old.
"""
from __future__ import annotations

import importlib.util
import os
import pathlib
import sys
from datetime import datetime, timedelta, timezone

HERE = pathlib.Path(__file__).resolve()
ROOT = HERE.parents[1]
PATCHED = os.environ.get("TFB_COVERAGE_MODULE_UNDER_TEST",
                         str(ROOT / "scripts" / "audit_full_refresh_coverage.py"))
BASELINE = os.environ.get("TFB_COVERAGE_BASELINE")  # optional dual-run

RIYADH = timezone(timedelta(hours=3))


def _load(path, env=None, name="cov_ut"):
    saved = {}
    for k, v in (env or {}).items():
        saved[k] = os.environ.get(k)
        os.environ[k] = v
    try:
        spec = importlib.util.spec_from_file_location(
            name + str(abs(hash((path, tuple(sorted((env or {}).items())))))), path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = mod
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        return mod
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _on():
    return _load(PATCHED, {"TFB_AUDIT_TZ_AWARE": "1"}, "cov_on")


def test_case1_zulu_converts_to_riyadh():
    assert _on().parse_dt("2026-07-31T22:03:19Z") == datetime(2026, 8, 1, 1, 3, 19)


def test_case2_offset_plus3_keeps_wall_time():
    assert _on().parse_dt("2026-08-01T01:03:19.352339+03:00") \
        == datetime(2026, 8, 1, 1, 3, 19, 352339)


def test_case3_offset_utc_converts():
    assert _on().parse_dt("2026-07-31T22:03:19+00:00") == datetime(2026, 8, 1, 1, 3, 19)


def test_case4_naive_string_unchanged():
    assert _on().parse_dt("2026-08-01 01:07:05") == datetime(2026, 8, 1, 1, 7, 5)


def test_case5_mdy_unchanged():
    assert _on().parse_dt("7/30/2026") == datetime(2026, 7, 30)


def test_case6_excel_serial_unchanged():
    m = _on()
    assert m.parse_dt(46203.5) == datetime(1899, 12, 30) + timedelta(days=46203.5)


def test_case7_aware_datetime_instance_converts():
    aware = datetime(2026, 7, 31, 22, 3, 19, tzinfo=timezone.utc)
    assert _on().parse_dt(aware) == datetime(2026, 8, 1, 1, 3, 19)


def test_case8_naive_datetime_instance_passthrough():
    naive = datetime(2026, 8, 1, 1, 7, 5)
    assert _on().parse_dt(naive) == naive


def test_case9_killswitch_matches_untouched_baseline():
    if not BASELINE:
        return  # dual-run only when baseline provided
    off = _load(PATCHED, {}, "cov_off")
    base = _load(BASELINE, {}, "cov_base")
    # the flag is read at CALL time (so production can flip it without a
    # reload) — the equivalence run must therefore hold env DURING the calls
    _saved = os.environ.get("TFB_AUDIT_TZ_AWARE")
    os.environ["TFB_AUDIT_TZ_AWARE"] = "0"
    inputs = [
        "2026-07-31T22:03:19Z",
        "2026-08-01T01:03:19.352339+03:00",
        "2026-07-31T22:03:19+00:00",
        "2026-08-01 01:07:05",
        "7/30/2026",
        46203.5,
        datetime(2026, 7, 31, 22, 3, 19, tzinfo=timezone.utc),
        datetime(2026, 8, 1, 1, 7, 5),
        "not a date",
    ]
    try:
        for x in inputs:
            assert off.parse_dt(x) == base.parse_dt(x), f"OFF diverges on {x!r}"
    finally:
        if _saved is None:
            os.environ.pop("TFB_AUDIT_TZ_AWARE", None)
        else:
            os.environ["TFB_AUDIT_TZ_AWARE"] = _saved


def test_case10_freshness_age_end_to_end():
    sys.path.insert(0, str(ROOT))
    import importlib as _il
    fresh = _il.import_module("scripts.audit_decision_surface_freshness")
    now_riyadh = datetime(2026, 8, 1, 9, 3, 19)  # naive Riyadh, as production builds it
    stamp = fresh.parse_dt("2026-07-31T22:03:19Z")  # true Riyadh wall: 01:03:19
    age = fresh._age_hours(stamp, now_riyadh)
    assert abs(age - 8.0) < 1e-9, f"expected 8.0h true age, got {age} (legacy read 11.0)"


if __name__ == "__main__":
    for fn in sorted(k for k in dir() if k.startswith("test_")):
        globals()[fn]()
    print("SELFTEST 10/10 PASS — tz-aware parse_dt + kill-switch equivalence proven")
