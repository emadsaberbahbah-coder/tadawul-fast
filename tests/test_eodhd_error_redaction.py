"""Offline transport-to-row proofs for EODHD diagnostic redaction.

Use the real request, quote and enrichment methods. Only external transport,
health storage and retry sleep are replaced; successful quote fields are never
passed through the diagnostic redactor.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import os
from pathlib import Path
from types import SimpleNamespace
import uuid

import pytest

httpx = pytest.importorskip("httpx")
from core.providers import eodhd_provider as provider

if os.getenv("TFB_REDACTION_SOURCE_ROOT"):
    path = Path(os.environ["TFB_REDACTION_SOURCE_ROOT"]) / "core/providers/eodhd_provider.py"
    spec = importlib.util.spec_from_file_location("core.providers.redaction_baseline_eodhd", path)
    provider = importlib.util.module_from_spec(spec)
    # Dataclass resolution requires registration during offline module loading.
    __import__("sys").modules[spec.name] = provider
    spec.loader.exec_module(provider)


class _Health:
    def __init__(self):
        self.failures = []
        self.successes = 0

    async def begin_request(self):
        return True, "closed"

    async def is_open(self):
        return False

    async def record_failure(self, reason):
        self.failures.append(reason)

    async def record_success(self):
        self.successes += 1


def _client(monkeypatch, status, body, *, attempts=0):
    secret = "synthetic_" + uuid.uuid4().hex
    health = _Health()
    requests, sleeps, restricted = [], [], []

    async def get_health():
        return health

    async def sleep(delay):
        sleeps.append(delay)

    async def wait(units):
        pass

    async def respond(request):
        requests.append(request)
        value = body(secret) if callable(body) else body
        if isinstance(value, dict):
            return httpx.Response(status, json=value, request=request)
        return httpx.Response(status, text=value, request=request)

    monkeypatch.setattr(provider, "_get_health", get_health)
    monkeypatch.setattr(provider.asyncio, "sleep", sleep)
    monkeypatch.setattr(provider, "_plan_restricted_cache_active", lambda endpoint: False)
    monkeypatch.setattr(provider, "_plan_restricted_cache_set", restricted.append)
    monkeypatch.setattr(provider, "_plan_restricted_tokens", lambda: ("subscription",))
    monkeypatch.setattr(provider, "_env_bool", lambda name, default=False: (
        False if name in {"EODHD_ENABLE_FUNDAMENTALS", "EODHD_ENABLE_HISTORY"} else default))
    client = provider.EODHDClient.__new__(provider.EODHDClient)
    client.api_key = secret
    client.base_url = "https://eodhd.invalid/api"
    client.retry_attempts = attempts
    client.retry_base_delay = 0
    client.daily_budget = 0
    client._sem = asyncio.Semaphore(2)
    client._bucket = SimpleNamespace(wait=wait)
    client._sf = provider._SingleFlight()
    client.quote_cache = provider._TTLCache(maxsize=5, ttl_sec=12)
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    return client, secret, health, requests, sleeps, restricted


@pytest.mark.parametrize("status,prefix,error_class,failures", [
    (401, "not authorized", "auth_error", ["AuthError"]),
    (401, "subscription", "auth_error", ["AuthError"]),
    (403, "not authorized", "auth_error", ["AuthError"]),
    (403, "subscription", "plan_restricted", []),
    (403, "your ip blocked", "ip_blocked", ["IpBlocked"]),
])
def test_actual_request_failure_cannot_echo_key_into_quote_row(
    monkeypatch, status, prefix, error_class, failures,
):
    client, secret, health, requests, sleeps, restricted = _client(
        monkeypatch, status,
        lambda key: prefix + " api_token=" + key + " bare echo " + key,
    )

    async def exercise():
        try:
            return await client.fetch_quote("SYNTHETIC.US")
        finally:
            await client._client.aclose()

    row, error = asyncio.run(exercise())
    assert len(requests) == 1 and sleeps == []
    auth_preserved = requests[0].url.params["api_token"] == secret
    assert auth_preserved is True
    assert health.failures == failures and health.successes == 0
    assert error.startswith(f"HTTP {status} {error_class}")
    assert bool(restricted) is (error_class == "plan_restricted")
    assert row["symbol"] == "SYNTHETIC.US" and row["provider"] == "eodhd"
    assert row.get("current_price") is None
    rendered = json.dumps({"row": row, "error": error})
    leaked = secret in rendered
    assert leaked is False, "synthetic credential survived transport-to-row boundary"
    assert "[REDACTED]" in rendered
    assert error_class in rendered


def test_quota_precedence_retry_and_health_remain_unchanged(monkeypatch):
    client, secret, health, requests, sleeps, restricted = _client(
        monkeypatch, 403, lambda key: "quota exceeded subscription api_token=" + key,
        attempts=1,
    )

    async def exercise():
        try:
            return await client.fetch_quote("SYNTHETIC.US")
        finally:
            await client._client.aclose()

    row, error = asyncio.run(exercise())
    assert len(requests) == 2
    assert sleeps == [5.0, 7.0]
    assert error == "HTTP 403 quota_or_rate_limit"
    assert health.failures == ["RateLimited", "RateLimited", "RateLimited"]
    assert restricted == []
    leaked = secret in json.dumps({"row": row, "error": error})
    assert leaked is False


def test_actual_enriched_error_detail_is_safe_without_changing_raw_classifier(monkeypatch):
    # Removing a long key must not move subscription wording into the
    # classifier's original first 200 characters and change auth into plan.
    long_secret = "synthetic_" + uuid.uuid4().hex * 6
    client, _, health, requests, _, restricted = _client(
        monkeypatch, 403,
        lambda key: key + " subscription denied",
    )
    client.api_key = long_secret

    async def exercise():
        try:
            return await client.fetch_enriched_quote_patch("SYNTHETIC.US")
        finally:
            await client._client.aclose()

    # The mock body must echo the exact per-client key, not a global registry.
    original = client._client
    async def respond(request):
        requests.append(request)
        return httpx.Response(403, text=long_secret + " subscription denied", request=request)
    asyncio.run(original.aclose())
    client._client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    row = asyncio.run(exercise())
    assert len(requests) == 1 and restricted == []
    assert health.failures == ["AuthError"]
    assert row["error"] == "fetch_failed" and row["data_quality"] == "MISSING"
    assert "auth_error" in row["error_detail"]
    leaked = long_secret in json.dumps(row)
    assert leaked is False
    assert "[REDACTED]" in row["error_detail"]


def test_actual_success_quote_fields_are_unchanged(monkeypatch):
    client, secret, health, requests, sleeps, restricted = _client(
        monkeypatch, 200, {"code": "SYNTHETIC.US", "close": 19.5,
            "previousClose": 18.75, "currency": "USD", "volume": 123},
    )

    async def exercise():
        try:
            return await client.fetch_quote("SYNTHETIC.US")
        finally:
            await client._client.aclose()

    row, error = asyncio.run(exercise())
    assert error is None and len(requests) == 1
    auth_preserved = requests[0].url.params["api_token"] == secret
    assert auth_preserved is True
    assert health.failures == [] and health.successes == 1
    assert sleeps == [] and restricted == []
    assert row["symbol"] == "SYNTHETIC.US"
    assert row["current_price"] == 19.5 and row["previous_close"] == 18.75
    assert row["currency"] == "USD" and row["volume"] == 123
