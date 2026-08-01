from __future__ import annotations

import os
import unittest
from unittest.mock import patch

import core.providers
from core.providers import eodhd_provider
from core.providers import eodhd_http402_circuit as circuit


class EODHDHTTP402CircuitTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        circuit._reset_for_tests()

    async def test_first_402_opens_and_second_call_is_short_circuited(self):
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            return None, "HTTP 402"

        with patch.dict(os.environ, {"TFB_EODHD_HTTP402_CIRCUIT": "1"}):
            first = await circuit.call_with_http402_circuit(operation)
            second = await circuit.call_with_http402_circuit(operation)

        self.assertEqual(first, (None, "HTTP 402"))
        self.assertEqual(calls, 1)
        self.assertIsNone(second[0])
        self.assertIn("provider_circuit_open:eodhd", second[1])
        self.assertNotIn("HTTP 402", second[1])
        snapshot = circuit.circuit_snapshot()
        self.assertEqual(snapshot["actual_http402_count"], 1)
        self.assertEqual(snapshot["short_circuit_count"], 1)
        self.assertTrue(snapshot["open"])

    async def test_non_402_error_does_not_open_circuit(self):
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            return None, "HTTP 404 not_found"

        await circuit.call_with_http402_circuit(operation)
        await circuit.call_with_http402_circuit(operation)
        self.assertEqual(calls, 2)
        self.assertFalse(circuit.circuit_snapshot()["open"])

    async def test_success_does_not_open_circuit(self):
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            return {"close": 10.0}, None

        first = await circuit.call_with_http402_circuit(operation)
        second = await circuit.call_with_http402_circuit(operation)
        self.assertEqual(first, ({"close": 10.0}, None))
        self.assertEqual(second, ({"close": 10.0}, None))
        self.assertEqual(calls, 2)
        self.assertFalse(circuit.circuit_snapshot()["open"])

    async def test_kill_switch_preserves_original_calls(self):
        calls = 0

        async def operation():
            nonlocal calls
            calls += 1
            return None, "HTTP 402"

        with patch.dict(
            os.environ,
            {"TFB_EODHD_HTTP402_CIRCUIT": "0"},
            clear=False,
        ):
            await circuit.call_with_http402_circuit(operation)
            await circuit.call_with_http402_circuit(operation)

        self.assertEqual(calls, 2)
        self.assertFalse(circuit.circuit_snapshot()["open"])

    def test_provider_client_is_patched_once(self):
        self.assertTrue(
            getattr(
                eodhd_provider.EODHDClient,
                "_TFB_HTTP402_CIRCUIT_INSTALLED",
                False,
            )
        )
        before = eodhd_provider.EODHDClient._request_json
        circuit.install_eodhd_http402_circuit()
        self.assertIs(eodhd_provider.EODHDClient._request_json, before)

    def test_provider_unhealthy_parser_accepts_short_circuit_marker(self):
        self.assertTrue(
            eodhd_provider._err_indicates_provider_unhealthy(
                "provider_circuit_open:eodhd:plan_restricted"
            )
        )


if __name__ == "__main__":
    unittest.main()
