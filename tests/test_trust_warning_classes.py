#!/usr/bin/env python3
"""tests/test_trust_warning_classes.py — engine v5.122.0 CG-5 hook.

Nine cases on _apply_warning_class_trust: OFF-default byte-identity
(deep snapshot), string- and list-form warnings, demote-only semantics,
already-LOW and missing-key guards, no-marker pass, idempotence, DQ
untouched everywhere, and version/flag-default assertions. The flag is
read at call time, so env wraps the calls.
"""
from __future__ import annotations

import copy
import importlib
import os
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

eng = importlib.import_module("core.data_engine_v2")

LOW = eng.TRUST_LEVEL_LOW


def _row(warnings, trust="HIGH"):
    r = {"symbol": "SNX.US", "data_quality_score": 100.0, "warnings": warnings}
    if trust is not None:
        r["trust_level"] = trust
    return r


def _with_flag(value, fn):
    saved = os.environ.get("TFB_TRUST_WARNING_CLASSES")
    if value is None:
        os.environ.pop("TFB_TRUST_WARNING_CLASSES", None)
    else:
        os.environ["TFB_TRUST_WARNING_CLASSES"] = value
    try:
        return fn()
    finally:
        if saved is None:
            os.environ.pop("TFB_TRUST_WARNING_CLASSES", None)
        else:
            os.environ["TFB_TRUST_WARNING_CLASSES"] = saved


def test_1_off_default_byte_identical():
    r = _row("fetch_failed:HTTP 402; provider_unhealthy:eodhd")
    snap = copy.deepcopy(r)
    _with_flag(None, lambda: eng._apply_warning_class_trust(r))
    assert r == snap, "default OFF must be a strict no-op"


def test_2_on_string_warnings_demotes_and_tags():
    r = _row("fetch_failed:HTTP 402; provider_unhealthy:eodhd")
    _with_flag("1", lambda: eng._apply_warning_class_trust(r))
    assert r["trust_level"] == LOW
    assert "low_trust_warning_class" in r["warnings"]


def test_3_on_list_warnings_handled():
    r = _row(["provider_unhealthy:eodhd", "other_note"])
    _with_flag("1", lambda: eng._apply_warning_class_trust(r))
    assert r["trust_level"] == LOW
    assert "low_trust_warning_class" in r["warnings"]


def test_4_already_low_untouched():
    r = _row("fetch_failed:HTTP 402", trust=LOW)
    snap = copy.deepcopy(r)
    _with_flag("1", lambda: eng._apply_warning_class_trust(r))
    assert r == snap, "demote-only: LOW rows gain no tag and change nothing"


def test_5_missing_trust_key_untouched():
    r = _row("fetch_failed:HTTP 402", trust=None)
    snap = copy.deepcopy(r)
    _with_flag("1", lambda: eng._apply_warning_class_trust(r))
    assert r == snap, "hook must never invent trust the master did not band"


def test_6_no_marker_untouched():
    r = _row("some_benign_note; another")
    snap = copy.deepcopy(r)
    _with_flag("1", lambda: eng._apply_warning_class_trust(r))
    assert r == snap


def test_7_identity_quarantined_marker():
    r = _row("identity_quarantined:issuer:v6.31.0")
    _with_flag("1", lambda: eng._apply_warning_class_trust(r))
    assert r["trust_level"] == LOW


def test_8_idempotent_and_dq_untouched():
    r = _row("fetch_failed:HTTP 402")
    _with_flag("1", lambda: eng._apply_warning_class_trust(r))
    once = copy.deepcopy(r)
    _with_flag("1", lambda: eng._apply_warning_class_trust(r))
    assert r == once, "second call must change nothing"
    assert r["data_quality_score"] == 100.0
    assert r["warnings"].count("low_trust_warning_class") == 1


def test_9_version_and_flag_default():
    assert eng.__version__ == "5.122.0"
    assert _with_flag(None, eng._trust_warning_classes_enabled) is False
    assert _with_flag("1", eng._trust_warning_classes_enabled) is True


if __name__ == "__main__":
    names = sorted(k for k in dir() if k.startswith("test_"))
    for fn in names:
        globals()[fn]()
    print(f"SELFTEST {len(names)}/{len(names)} PASS — CG-5 demote-only hook proven")
