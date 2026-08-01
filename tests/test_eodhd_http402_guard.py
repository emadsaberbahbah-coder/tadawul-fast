"""Native-provider regression tests for CG-1 EODHD HTTP 402 handling."""
from __future__ import annotations

import asyncio
import os
import unittest
from unittest.mock import patch

from core.providers import eodhd_http402_circuit as circuit
from core.providers import eodhd_provider as eodhd


class _FakeResponse:
    def __init__(
        self,
        status_code: int,
        *,
        text: str = "",
        content: bytes = b"{}",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self.text = text
        self.content = content
        self.headers = headers or {}


class _FakeHTTPClient:
    def __init__(self, responses: list[_FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls = 0

    async def get(self, _url: str, **_kwargs):
        self.calls += 1
        if not self.responses:
            raise AssertionError("unexpected network call")
        return self.responses.pop(0)

    async def aclose(self) -> None:
        return None


class EODHDHTTP402ProviderTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        circuit._reset_for_tests()
        eodhd._HEALTH = None
        eodhd._HEALTH_LOCK = asyncio.Lock()
        self.env = patch.dict(
            os.environ,
            {
                "EODHD_API_KEY": "test-key",
                "EODHD_RETRY_ATTEMPTS": "0",
                "EODHD_RATE_LIMIT_RPS": "0",
                "TFB_EODHD_HTTP402_CIRCUIT": "1",
            },
            clear=False,
        )
        self.env.start()
        self.client = eodhd.EODHDClient()
        self.real_http_client = self.client._client

    async def asyncTearDown(self) -> None:
        await self.real_http_client.aclose()
        self.env.stop()
        circuit._reset_for_tests()
        eodhd._HEALTH = None
        eodhd._HEALTH_LOCK = asyncio.Lock()

    def _use_responses(self, *responses: _FakeResponse) -> _FakeHTTPClient:
        fake = _FakeHTTPClient(list(responses))
        self.client._client = fake
        return fake

    async def test_first_402_opens_and_next_provider_call_is_not_sent(self) -> None:
        fake = self._use_responses(_FakeResponse(402, text="payment required"))

        first_data, first_error = await self.client._request_json("real-time/AAPL.US")
        second_data, second_error = await self.client._request_json("real-time/MSFT.US")

        self.assertIsNone(first_data)
        self.assertEqual(first_error, "HTTP 402 plan_or_entitlement")
        self.assertIsNone(second_data)
        self.assertIn("provider_circuit_open:eodhd", second_error or "")
        self.assertIn("plan_or_entitlement", second_error or "")
        self.assertEqual(fake.calls, 1)
        self.assertTrue(circuit.circuit_snapshot()["open"])

    async def test_402_patch_emits_unhealthy_without_synthetic_values(self) -> None:
        patch_row = eodhd._build_error_patch_with_geo(
            "AAPL.US",
            "AAPL.US",
            "HTTP 402 plan_or_entitlement",
        )
        warnings = patch_row.get("warnings") or []

        self.assertIn("fetch_failed:HTTP 402 plan_or_entitlement", warnings)
        self.assertIn("provider_unhealthy:eodhd", warnings)
        for forbidden in (
            "current_price",
            "price",
            "score",
            "overall_score",
            "rank",
            "recommendation",
        ):
            self.assertNotIn(forbidden, patch_row)

    async def test_short_circuit_expiry_permits_a_healthy_request(self) -> None:
        fake = self._use_responses(
            _FakeResponse(402, text="payment required"),
            _FakeResponse(200, content=b'{"close": 10.0}'),
        )
        await self.client._request_json("real-time/AAPL.US")
        circuit._OPEN_UNTIL = circuit.time.monotonic() - 0.01

        data, error = await self.client._request_json("real-time/MSFT.US")

        self.assertEqual(data, {"close": 10.0})
        self.assertIsNone(error)
        self.assertEqual(fake.calls, 2)
        self.assertFalse(circuit.circuit_snapshot()["open"])

    async def test_kill_switch_reproduces_v4_15_0_generic_402_behavior(self) -> None:
        fake = self._use_responses(
            _FakeResponse(402, text="payment required"),
            _FakeResponse(402, text="payment required"),
        )
        with patch.dict(
            os.environ,
            {"TFB_EODHD_HTTP402_CIRCUIT": "0"},
            clear=False,
        ):
            first = await self.client._request_json("real-time/AAPL.US")
            second = await self.client._request_json("real-time/MSFT.US")
            marker = eodhd._err_indicates_provider_unhealthy("HTTP 402")

        self.assertEqual(first, (None, "HTTP 402"))
        self.assertEqual(second, (None, "HTTP 402"))
        self.assertEqual(fake.calls, 2)
        self.assertFalse(marker)
        self.assertFalse(circuit.circuit_snapshot()["open"])

    async def test_existing_401_403_429_and_404_contracts_are_unchanged(self) -> None:
        cases = [
            (
                _FakeResponse(401, text="invalid token"),
                "HTTP 401 auth_error invalid token",
            ),
            (
                _FakeResponse(
                    403,
                    text="quota exceeded",
                    headers={"Retry-After": "0"},
                ),
                "HTTP 403 quota_or_rate_limit",
            ),
            (
                _FakeResponse(429, headers={"Retry-After": "0"}),
                "HTTP 429",
            ),
            (
                _FakeResponse(404),
                "HTTP 404 not_found",
            ),
        ]

        for response, expected in cases:
            with self.subTest(expected=expected):
                circuit._reset_for_tests()
                eodhd._HEALTH = None
                eodhd._HEALTH_LOCK = asyncio.Lock()
                fake = self._use_responses(response)
                data, error = await self.client._request_json("real-time/AAPL.US")
                self.assertIsNone(data)
                self.assertEqual(error, expected)
                self.assertEqual(fake.calls, 1)

    def test_non_systemic_errors_do_not_emit_provider_unhealthy(self) -> None:
        self.assertFalse(eodhd._err_indicates_provider_unhealthy("HTTP 404 not_found"))
        self.assertFalse(eodhd._err_indicates_provider_unhealthy("HTTP 429"))
        self.assertFalse(eodhd._err_indicates_provider_unhealthy("network_error:Timeout"))


if __name__ == "__main__":
    unittest.main()
