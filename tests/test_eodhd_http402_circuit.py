from __future__ import annotations

import os
import time
import unittest
from unittest.mock import patch

from core.providers import eodhd_http402_circuit as circuit


class EODHDHTTP402CircuitTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        circuit._reset_for_tests()

    def tearDown(self) -> None:
        circuit._reset_for_tests()

    def test_boolean_vocabulary_is_canonical(self) -> None:
        for value in ("1", "true", "yes", "on"):
            with self.subTest(value=value), patch.dict(
                os.environ,
                {"TFB_EODHD_HTTP402_CIRCUIT": value},
                clear=False,
            ):
                self.assertTrue(circuit.circuit_enabled())
        for value in ("0", "false", "no", "off"):
            with self.subTest(value=value), patch.dict(
                os.environ,
                {"TFB_EODHD_HTTP402_CIRCUIT": value},
                clear=False,
            ):
                self.assertFalse(circuit.circuit_enabled())

        # Unknown values fail safe to ON; wider legacy vocabulary is not copied.
        for value in ("enabled", "y", "t", "disable"):
            with self.subTest(value=value), patch.dict(
                os.environ,
                {"TFB_EODHD_HTTP402_CIRCUIT": value},
                clear=False,
            ):
                self.assertTrue(circuit.circuit_enabled())

    async def test_first_402_opens_and_second_call_short_circuits(self) -> None:
        with patch.dict(
            os.environ,
            {"TFB_EODHD_HTTP402_CIRCUIT": "1"},
            clear=False,
        ):
            allowed, error = await circuit.before_request()
            self.assertTrue(allowed)
            self.assertIsNone(error)

            opened = await circuit.record_http402("HTTP 402 plan_or_entitlement")
            self.assertTrue(opened)

            allowed, error = await circuit.before_request()
            self.assertFalse(allowed)
            self.assertIsNotNone(error)
            self.assertIn("provider_circuit_open:eodhd", error or "")
            self.assertIn("plan_or_entitlement", error or "")

            snapshot = circuit.circuit_snapshot()
            self.assertTrue(snapshot["open"])
            self.assertEqual(snapshot["actual_http402_count"], 1)
            self.assertEqual(snapshot["short_circuit_count"], 1)

    async def test_expiry_closes_and_allows_the_next_request(self) -> None:
        with patch.dict(
            os.environ,
            {"TFB_EODHD_HTTP402_CIRCUIT": "1"},
            clear=False,
        ):
            await circuit.record_http402("HTTP 402 plan_or_entitlement")
            self.assertTrue(circuit.circuit_snapshot()["open"])

            circuit._OPEN_UNTIL = time.monotonic() - 0.01
            allowed, error = await circuit.before_request()

            self.assertTrue(allowed)
            self.assertIsNone(error)
            self.assertFalse(circuit.circuit_snapshot()["open"])
            self.assertEqual(circuit.circuit_snapshot()["last_reason"], "")

    async def test_non_402_failure_does_not_open(self) -> None:
        with patch.dict(
            os.environ,
            {"TFB_EODHD_HTTP402_CIRCUIT": "1"},
            clear=False,
        ):
            opened = await circuit.record_http402("HTTP 404 not_found")
            self.assertFalse(opened)
            self.assertFalse(circuit.circuit_snapshot()["open"])

    async def test_kill_switch_restores_per_call_behavior(self) -> None:
        with patch.dict(
            os.environ,
            {"TFB_EODHD_HTTP402_CIRCUIT": "0"},
            clear=False,
        ):
            opened = await circuit.record_http402("HTTP 402 plan_or_entitlement")
            first = await circuit.before_request()
            second = await circuit.before_request()

            self.assertFalse(opened)
            self.assertEqual(first, (True, None))
            self.assertEqual(second, (True, None))
            self.assertFalse(circuit.circuit_snapshot()["open"])
            self.assertEqual(circuit.circuit_snapshot()["actual_http402_count"], 0)
            self.assertEqual(circuit.circuit_snapshot()["short_circuit_count"], 0)

    def test_explicit_unavailable_error_has_no_synthetic_decision_data(self) -> None:
        error = "provider_circuit_open:eodhd:plan_or_entitlement:retry_after_sec=60"
        self.assertTrue(circuit.is_http402_error(error))
        self.assertNotIn("price", error)
        self.assertNotIn("score", error)
        self.assertNotIn("recommendation", error)


if __name__ == "__main__":
    unittest.main()
