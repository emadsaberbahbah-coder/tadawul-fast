#!/usr/bin/env python3
"""tests/test_eodhd_http402_gate.py — v4.16.0 HTTP-402 entitlement latch.

Six cases, each asserting on the (data, err) contract of
EODHDClient._request_json with a stubbed transport and a stubbed health
registry, so no network and no real breaker state are involved:

  1. flag ON : first 402 -> classified error, class "Entitlement", latch opens
  2. flag ON : second call short-circuits (no network), err carries both
               markers ("HTTP 402" for the unhealthy path, "circuit_open"
               for _classify_error)
  3. flag ON : latch expiry -> network resumes; a fresh 402 re-opens it
  4. flag OFF: v4.15.0-equivalence — 402 falls to the generic branch
               ("HTTP 402", class "FetchError"), NO latch, second call
               still hits the network
  5. flag ON : 200 success path untouched (data returned, success recorded)
  6. flag ON : non-402 4xx (418) untouched (generic "HTTP 418", FetchError,
               no latch)
"""
from __future__ import annotations

import asyncio
import importlib.util
import os
import pathlib
import sys
import time

MODULE_PATH = os.environ.get(
    "TFB_EODHD_MODULE_UNDER_TEST",
    str(pathlib.Path(__file__).resolve().parents[1]
        / "core" / "providers" / "eodhd_provider.py"),
)


def _load(env: dict):
    """Import a fresh copy of the module under a controlled env."""
    saved = {}
    for k, v in env.items():
        saved[k] = os.environ.get(k)
        os.environ[k] = v
    os.environ.setdefault("EODHD_API_KEY", "test-key")
    name = f"eodhd_ut_{abs(hash(tuple(sorted(env.items()))))}"
    spec = importlib.util.spec_from_file_location(name, MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    for k, v in saved.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    return mod


class _Resp:
    def __init__(self, status_code: int, body: bytes = b"{}"):
        self.status_code = status_code
        self.content = body
        self.text = body.decode()
        self.headers: dict = {}


class _StubClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    async def get(self, url, params=None):
        self.calls += 1
        return self.responses.pop(0) if self.responses else _Resp(200)


class _StubBucket:
    async def wait(self, n):  # noqa: ARG002
        return None


class _StubHealth:
    def __init__(self):
        self.failures: list = []
        self.successes = 0
        self.begins = 0

    async def begin_request(self):
        self.begins += 1
        return True, "closed"

    async def record_failure(self, error_class):
        self.failures.append(error_class)

    async def record_success(self):
        self.successes += 1


def _client(mod, responses, env=None):
    saved = {}
    for k, v in (env or {}).items():
        saved[k] = os.environ.get(k)
        os.environ[k] = v
    try:
        c = mod.EODHDClient()
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    c._client = _StubClient(responses)
    c._bucket = _StubBucket()
    c.daily_budget = 0.0
    h = _StubHealth()

    async def _stub_get_health():
        return h

    mod._get_health = _stub_get_health
    return c, h


def _run(coro):
    return asyncio.run(coro)


def test_case1_first_402_classifies_and_latches():
    mod = _load({})
    c, h = _client(mod, [_Resp(402)], env={"TFB_EODHD_HTTP402_CIRCUIT": "1"})
    data, err = _run(c._request_json("real-time/AAPL.US"))
    assert data is None and err == "HTTP 402 plan_or_entitlement", err
    assert h.failures == ["Entitlement"], h.failures
    assert c._http402_count == 1
    assert c._http402_open_until > time.monotonic()
    print("SELFTEST case1 PASS")


def test_case2_latch_short_circuits_without_network():
    mod = _load({})
    c, h = _client(mod, [_Resp(402), _Resp(200)], env={"TFB_EODHD_HTTP402_CIRCUIT": "1"})
    _run(c._request_json("real-time/AAPL.US"))
    data, err = _run(c._request_json("real-time/MSFT.US"))
    assert data is None
    assert err == "HTTP 402 plan_or_entitlement circuit_open", err
    assert "HTTP 402" in err and "circuit_open" in err  # both markers
    assert c._client.calls == 1, "second call must not reach the network"
    assert c._http402_short_circuits == 1
    assert h.begins == 1, "short-circuit must not touch breaker accounting"
    print("SELFTEST case2 PASS")


def test_case3_expiry_resumes_and_fresh_402_reopens():
    mod = _load({})
    c, h = _client(mod, [_Resp(402), _Resp(200), _Resp(402)], env={"TFB_EODHD_HTTP402_CIRCUIT": "1"})
    _run(c._request_json("e/a"))
    c._http402_open_until = time.monotonic() - 1.0  # force expiry
    data, err = _run(c._request_json("e/b"))
    assert err is None and data == {} and c._client.calls == 2
    assert h.successes == 1
    data, err = _run(c._request_json("e/c"))
    assert err == "HTTP 402 plan_or_entitlement" and c._http402_count == 2
    assert c._http402_open_until > time.monotonic(), "re-latched"
    print("SELFTEST case3 PASS")


def test_case4_killswitch_off_is_v4150_equivalent():
    mod = _load({})
    c, h = _client(mod, [_Resp(402), _Resp(402)], env={"TFB_EODHD_HTTP402_CIRCUIT": "0"})
    data, err = _run(c._request_json("e/a"))
    assert data is None and err == "HTTP 402", err  # generic branch verbatim
    assert h.failures == ["FetchError"], h.failures
    assert getattr(c, "_http402_open_until", 0.0) == 0.0, "no latch when disabled"
    _run(c._request_json("e/b"))
    assert c._client.calls == 2, "flag off: every call still attempts network"
    print("SELFTEST case4 PASS")


def test_case5_success_path_untouched():
    mod = _load({})
    c, h = _client(mod, [_Resp(200, b'{"ok":1}')], env={"TFB_EODHD_HTTP402_CIRCUIT": "1"})
    data, err = _run(c._request_json("e/a"))
    assert err is None and data == {"ok": 1}
    assert h.successes == 1 and h.failures == []
    print("SELFTEST case5 PASS")


def test_case6_other_4xx_untouched():
    mod = _load({})
    c, h = _client(mod, [_Resp(418)], env={"TFB_EODHD_HTTP402_CIRCUIT": "1"})
    data, err = _run(c._request_json("e/a"))
    assert data is None and err == "HTTP 418", err
    assert h.failures == ["FetchError"]
    assert getattr(c, "_http402_open_until", 0.0) == 0.0, "418 must not latch"
    print("SELFTEST case6 PASS")


if __name__ == "__main__":
    for fn in sorted(k for k in dir() if k.startswith("test_")):
        globals()[fn]()
    print("SELFTEST 6/6 PASS — v4.16.0 latch + kill-switch equivalence proven")
