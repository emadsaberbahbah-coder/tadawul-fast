#!/usr/bin/env python3
# tests/test_security_pins_20261013.py
"""
Security pin floors for requirements.txt v1.1.5 -- pure stdlib, ZERO network.

VERSION 1.1.0 (2026-10-19)
WHY: close the remaining seven Starlette advisories with Starlette 1.3.1
and the first compatible FastAPI release, 0.133.0. NLTK has no patched
release for GHSA-8mgp-746c-j5xp and production does not use it, so enforce
its removal. S4 also rejects explicit regressions of all three decisions.

HISTORY: VERSION 1.0.0 (2026-10-13)
WHY: the advisory audit (dependency_typing_review.yml: pip-audit==2.9.0
--requirement requirements.txt --no-deps --disable-pip) reported 89 known
advisories in 13 pinned packages on main (dda1211). requirements.txt v1.1.4
moves twelve pins to the minimal version that closes their reported
advisories and still resolves with every other pin on Python 3.11 (89 -> 8).
The current harness preserves those floors and adds the framework and
NLTK policy above: a later edit that drops any of those pins
below its fixed version, loosens it from an exact pin, or lets the
production-pinned contract job drift from requirements.txt goes red in the
blocking lean lane. It parses text only; it imports none of the packages.

  S1 every bumped package is pinned exactly once with "==" and is >= the
     minimal version that closes its pip-audit advisories.
  S2 protobuf stays on 5.x (grpcio-status 1.64.3 needs >=5.26.1,<6 and
     opentelemetry-proto 1.28.0 needs >=5.0,<6).
  S3 pyOpenSSL and cryptography are a compatible pair (pyOpenSSL 26.4.x
     needs cryptography >=49,<51; 26.3.x needs >=49,<50).
  S4 framework versions below the fixed compatible floors and reintroduced
     NLTK are rejected by the same guard used for the real requirements.
  S5 every "pkg==ver" pin in ci.yml's contract job equals requirements.txt.
  S6 the httpcore direct-wheel URL pin still meets its audited 1.0.9 floor.
     The old requirements-only audit skipped URLs; the installed-package
     audit now includes the resolved httpcore distribution with all others.
  S7 the requirements.txt header carries the v1.1.5 dated WHY.
  G1 GOLDEN NEGATIVE: set TFB_SECPIN_BASE=<copy of the pre-change
     requirements.txt (v1.1.3)> and the S1 floors are evaluated against that
     file -- this test then FAILS (12 pins below their fixed versions). It
     SKIPS when the variable is unset, so CI stays green.

Run:  /home/user/tfb-venv/bin/python tests/test_security_pins_20261013.py
      /home/user/tfb-venv/bin/python -m pytest -q tests/test_security_pins_20261013.py
"""

from __future__ import annotations

import os
import re
import sys
import traceback
import unittest
from pathlib import Path
from typing import Dict, List, Tuple

HARNESS_VERSION = "1.1.0"

ROOT = Path(__file__).resolve().parents[1]
REQUIREMENTS = Path(os.environ.get("TFB_SECPIN_REQ", str(ROOT / "requirements.txt")))
CI_YML = ROOT / ".github" / "workflows" / "ci.yml"
BASE = (os.environ.get("TFB_SECPIN_BASE", "") or "").strip()

# package (PEP 503 normalised) -> minimal version that closes every advisory
# pip-audit 2.9.0 reported for it (on the old pin, and on the new pin itself:
# cryptography 49.0.0 carries PYSEC-2026-3552, fixed in 50.0.0).
SECURITY_FLOORS: Dict[str, str] = {
    "aiohttp": "3.14.3",
    "anyio": "4.14.2",
    "cryptography": "50.0.0",
    "fastapi": "0.133.0",
    "h2": "4.4.1",
    "lxml": "6.1.0",
    "protobuf": "5.29.6",
    "pydantic-settings": "2.14.2",
    "pyjwt": "2.15.0",
    "pyopenssl": "26.0.0",
    "python-dotenv": "1.2.2",
    "python-multipart": "0.0.31",
    "starlette": "1.3.1",
}

# Audited version floor; the installed-package audit includes this URL pin's
# resolved distribution. The historical requirements-only audit skipped it.
HTTPCORE_AUDITED = "1.0.9"


# ---------------------------------------------------------------------------
# Parsing helpers (import-time safe: nothing runs on import)
# ---------------------------------------------------------------------------

def _norm(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _vtuple(text: str) -> Tuple[int, ...]:
    """Numeric release tuple; every version this harness compares is X.Y.Z."""
    parts = re.match(r"^\s*(\d+(?:\.\d+)*)", text or "")
    if not parts:
        raise ValueError("not a release version: %r" % (text,))
    nums = [int(p) for p in parts.group(1).split(".")]
    while len(nums) < 3:
        nums.append(0)
    return tuple(nums)


def _parse_requirements(path: Path) -> Dict[str, List[Tuple[str, str]]]:
    """name -> list of (operator_or_url, version_or_url) for every entry."""
    out: Dict[str, List[Tuple[str, str]]] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip() if not raw.lstrip().startswith("#") else ""
        if not line:
            continue
        spec = line.split(";", 1)[0].strip()
        m_url = re.match(r"^([A-Za-z0-9][A-Za-z0-9._-]*)(\[[^\]]*\])?\s*@\s*(\S+)$", spec)
        if m_url:
            out.setdefault(_norm(m_url.group(1)), []).append(("@", m_url.group(3)))
            continue
        m_pin = re.match(r"^([A-Za-z0-9][A-Za-z0-9._-]*)(\[[^\]]*\])?\s*(==|>=|<=|~=|!=|>|<)\s*(\S+)$", spec)
        if m_pin:
            out.setdefault(_norm(m_pin.group(1)), []).append((m_pin.group(3), m_pin.group(4)))
            continue
        out.setdefault(_norm(re.split(r"[\s\[<>=!~;]", spec, 1)[0]), []).append(("", spec))
    return out


def _exact(reqs: Dict[str, List[Tuple[str, str]]], name: str) -> str:
    entries = reqs.get(_norm(name), [])
    assert len(entries) == 1, "%s must appear exactly once, found %r" % (name, entries)
    op, ver = entries[0]
    assert op == "==", "%s must be an exact pin (==), found %r" % (name, entries[0])
    return ver


def _floor_violations(reqs: Dict[str, List[Tuple[str, str]]]) -> List[str]:
    bad: List[str] = []
    for name, floor in sorted(SECURITY_FLOORS.items()):
        try:
            ver = _exact(reqs, name)
        except AssertionError as exc:
            bad.append(str(exc))
            continue
        if _vtuple(ver) < _vtuple(floor):
            bad.append("%s==%s is below its fixed version %s" % (name, ver, floor))
    if "nltk" in reqs:
        bad.append("nltk must remain absent: unused and no patched release for GHSA-8mgp-746c-j5xp")
    return bad


def _contract_job_pins(ci_text: str) -> Dict[str, str]:
    m = re.search(r"\n  contract:\n(.*?)(?=\n  [A-Za-z0-9_-]+:\n|\Z)", ci_text, re.S)
    assert m, "ci.yml has no contract job"
    pins: Dict[str, str] = {}
    for name, ver in re.findall(r'"([A-Za-z0-9._-]+)==([0-9][^"\s]*)"', m.group(1)):
        pins[_norm(name)] = ver
    return pins


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_s1_bumped_pins_meet_their_fixed_versions() -> None:
    reqs = _parse_requirements(REQUIREMENTS)
    bad = _floor_violations(reqs)
    print("S1 floors=%d violations=%d" % (len(SECURITY_FLOORS), len(bad)))
    assert not bad, "\n".join(bad)


def test_s2_protobuf_stays_on_5x() -> None:
    ver = _exact(_parse_requirements(REQUIREMENTS), "protobuf")
    assert _vtuple(ver)[0] == 5, "protobuf %s left 5.x (grpcio-status 1.64.3 needs <6)" % ver


def test_s3_pyopenssl_and_cryptography_are_a_compatible_pair() -> None:
    reqs = _parse_requirements(REQUIREMENTS)
    ossl = _vtuple(_exact(reqs, "pyOpenSSL"))
    crypto = _vtuple(_exact(reqs, "cryptography"))
    # Published Requires-Dist of each pyOpenSSL 26.x release (PyPI metadata).
    windows = {
        (26, 0): ((46, 0, 0), (47, 0, 0)),
        (26, 1): ((46, 0, 0), (48, 0, 0)),
        (26, 2): ((46, 0, 0), (49, 0, 0)),
        (26, 3): ((49, 0, 0), (50, 0, 0)),
        (26, 4): ((49, 0, 0), (51, 0, 0)),
    }
    key = ossl[:2]
    if key not in windows:
        # A later, deliberate pyOpenSSL move is not blocked here; pip in the
        # heavy/whole-suite lanes still refuses an incompatible pair.
        raise unittest.SkipTest("pyOpenSSL %r has no recorded cryptography window" % (ossl,))
    lo, hi = windows[key]
    assert lo <= crypto < hi, "cryptography %r outside pyOpenSSL %r window %r..%r" % (crypto, ossl, lo, hi)


def test_s4_fixed_framework_and_nltk_policy_rejects_regressions() -> None:
    reqs = _parse_requirements(REQUIREMENTS)
    for name, version in (("fastapi", "0.132.1"), ("starlette", "1.3.0"), ("nltk", "3.10.3")):
        mutated = {**reqs, name: [("==", version)]}
        bad = _floor_violations(mutated)
        assert any(name in error for error in bad), (name, version, bad)


def test_s5_contract_job_pins_follow_requirements() -> None:
    reqs = _parse_requirements(REQUIREMENTS)
    pins = _contract_job_pins(CI_YML.read_text(encoding="utf-8"))
    assert "anyio" in pins and "starlette" in pins, sorted(pins)
    drift = []
    for name, ver in sorted(pins.items()):
        if name in reqs:
            want = _exact(reqs, name)
            if want != ver:
                drift.append("contract %s==%s but requirements.txt %s==%s" % (name, ver, name, want))
    print("S5 contract pins=%d drift=%d" % (len(pins), len(drift)))
    assert not drift, "\n".join(drift)


def test_s6_httpcore_url_pin_meets_the_audited_floor() -> None:
    entries = _parse_requirements(REQUIREMENTS).get("httpcore", [])
    assert len(entries) == 1, entries
    op, value = entries[0]
    if op == "@":
        m = re.search(r"/httpcore-([0-9][0-9.]*)-py3-none-any\.whl$", value)
        assert m, "httpcore URL pin is not a recognisable wheel: %s" % value
        ver = m.group(1)
    else:
        assert op == "==", entries
        ver = value
    assert _vtuple(ver) >= _vtuple(HTTPCORE_AUDITED), ver


def test_s7_requirements_header_records_dated_why() -> None:
    head = "\n".join(REQUIREMENTS.read_text(encoding="utf-8").splitlines()[:12])
    m = re.search(r"requirements\.txt \(v(\d+\.\d+\.\d+)\)", head)
    assert m and _vtuple(m.group(1)) >= (1, 1, 5), head
    assert "WHY v1.1.5 (2026-10-19)" in REQUIREMENTS.read_text(encoding="utf-8")


def test_g1_golden_negative_base_requirements_fail_the_floors() -> None:
    if not BASE:
        raise unittest.SkipTest(
            "golden-negative skipped: set TFB_SECPIN_BASE=<copy of requirements.txt v1.1.3>"
        )
    base = Path(BASE)
    if not base.is_file():
        raise unittest.SkipTest("TFB_SECPIN_BASE path does not exist: " + BASE)
    bad = _floor_violations(_parse_requirements(base))
    print("G1 base=%s violations=%d" % (base, len(bad)))
    for line in bad:
        print("G1   " + line)
    assert not bad, "base requirements fail the security floors:\n" + "\n".join(bad)


# ---------------------------------------------------------------------------
# Standalone runner (pytest ignores this)
# ---------------------------------------------------------------------------

def _main() -> int:
    tests = [(n, f) for n, f in sorted(globals().items())
             if n.startswith("test_") and callable(f)]
    passed = failed = skipped = 0
    for name, fn in tests:
        try:
            fn()
        except unittest.SkipTest as exc:
            skipped += 1
            print("SKIP " + name + " :: " + str(exc))
            continue
        except Exception:  # noqa: BLE001
            failed += 1
            print("FAIL " + name)
            traceback.print_exc()
            continue
        passed += 1
        print("ok   " + name)
    total = passed + failed
    suffix = ("  (skipped %d)" % skipped) if skipped else ""
    print("PASS %d/%d%s" % (passed, total, suffix))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_main())
