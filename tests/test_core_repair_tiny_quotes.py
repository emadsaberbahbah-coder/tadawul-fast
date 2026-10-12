"""Bounded deployed tiny-price readback must require genuine, coherent rows."""
import contextlib
import copy
import hashlib
import io
import json
import os
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from scripts import verify_core_repair_readback as probe

COMMIT = "a" * 40
WORKFLOW_COMMIT = "f" * 40
ENGINE = "5.154.0"
ROUTE = "4.8.3"
SYMBOLS = ["SHIB-USD", "DOGE-USD", "XRP-USD"]
PATH = "/v1/analysis/sheet-rows"
HORIZONS = ("1m", "3m", "12m")
PRIVATE = "private-token-do-not-publish-4aa6"


def iso(stamp):
    return stamp.isoformat(timespec="microseconds")


def acquisition(stamp=None, provider="eodhd", status="success", quote_stamp=None):
    stamp = stamp or datetime.now(UTC) - timedelta(seconds=30)
    quote_stamp = quote_stamp or stamp
    return (f"acquisition_status:{status}; acquisition_provider:{provider}; "
            f"acquisition_acquired_at:{iso(stamp)}; "
            f"acquisition_quote_asof:{iso(quote_stamp)}")


def sample_row(symbol, price):
    row = {"symbol": symbol, "requested_symbol": symbol,
           "current_price": price, "data_provider": "eodhd",
           "forecast_source": "phase_ii_synthetic", "warnings": acquisition()}
    for horizon, roi in zip(HORIZONS, (0.01, 0.03, 0.12)):
        # Fixed independently constructed coherent tuples, not engine output.
        row["forecast_price_" + horizon] = price * (1 + roi)
        row["expected_roi_" + horizon] = roi
    return row


class FakeRequest:
    """A deterministic local transport with actual fake-response receipts."""
    def __init__(self):
        self.health = {"ready": True, "engine_ready": True, "engine_version": ENGINE,
                       "deploy": {"render_git_commit": COMMIT},
                       "engine_gates": {"price_precision": "on", "roi12_softcap": "observe",
                           "tuple_noise_release": "observe", "settle_unresolved": "observe",
                           "scoring_settle": "observe", "env_combo": "ok"},
                       "startup_warnings": []}
        self.after = None
        self.payload = {"page": "Commodities_FX", "version": ROUTE,
                        "status": "ok", "count": 3,
                        "row_objects": [sample_row(SYMBOLS[0], 0.00000549),
                                        sample_row(SYMBOLS[1], 0.08657),
                                        sample_row(SYMBOLS[2], 1.4083)]}
        self.calls = []
        self.receipts = []
        self.post_error = None
        self.post_status = 200

    def __call__(self, path, body=None, authenticated=False):
        self.calls.append((path, copy.deepcopy(body), authenticated))
        if path == "/health":
            payload = self.after if self.after is not None and len(self.calls) > 1 else self.health
            status = 200
        elif path == PATH:
            payload, status = self.payload, self.post_status
        else:
            raise AssertionError("Unexpected route: " + path)
        raw = json.dumps(payload, allow_nan=True, sort_keys=True).encode()
        self.receipts.append({"path": path, "http_status": status,
                              "response_sha256": hashlib.sha256(raw).hexdigest(),
                              "response_bytes": len(raw),
                              "captured_at_utc": iso(datetime.now(UTC))})
        if path == PATH and self.post_error is not None:
            raise self.post_error
        return copy.deepcopy(payload)


class TinyQuoteReadbackTests(unittest.TestCase):
    def setUp(self):
        self.request = FakeRequest()

    def run_probe(self):
        return probe.verify_tiny_quotes(self.request, COMMIT, ENGINE, ROUTE,
                                       workflow_commit=WORKFLOW_COMMIT)

    def assert_status(self, result, status):
        self.assertEqual(result["status"], status)
        self.assertIs(result["ok"], status == "verified")
        if status != "verified":
            self.assertTrue(result["reasons"] or any(
                row.get("reasons") or any(t.get("reasons") for t in row.get("tuples", {}).values())
                for row in result["rows"]), "A non-verification must explain its cause")
        # Artifacts must be strict JSON even when backend values are nonfinite.
        json.dumps(result, allow_nan=False)

    def post_receipt(self):
        return next(r for r in self.request.receipts if r["path"] == PATH)

    def test_success_uses_only_exact_authentic_three_symbol_protocol(self):
        result = self.run_probe()
        self.assert_status(result, "verified")
        self.assertEqual([c[0] for c in self.request.calls], ["/health", PATH, "/health"])
        post = self.request.calls[1]
        self.assertTrue(post[2])
        self.assertEqual(post[1]["page"], "Commodities_FX")
        self.assertEqual(post[1]["symbols"], SYMBOLS)
        self.assertEqual(post[1]["limit"], 3)
        self.assertIs(post[1]["include_matrix"], False)
        self.assertIs(post[1]["refresh"], True)
        self.assertNotIn("rows", post[1], "Real quote request must not manufacture probe rows")
        self.assertEqual(result["expected_backend_commit"], COMMIT)
        self.assertEqual(result["workflow_source_sha"], WORKFLOW_COMMIT)
        self.assertEqual(result["health_before"]["served_commit"], COMMIT)
        self.assertEqual(result["health_after"]["served_commit"], COMMIT)
        self.assertEqual({r["symbol"] for r in result["rows"]}, set(SYMBOLS))
        self.assertEqual(result["response_receipt"]["response_sha256"], self.post_receipt()["response_sha256"])
        self.assertEqual(result["response_receipt"]["response_bytes"], self.post_receipt()["response_bytes"])

    def test_start_health_requires_boolean_readiness_exact_engine_modes_and_no_warnings(self):
        patches = [("ready", 1), ("engine_ready", "true"), ("engine_version", "5.153.0"),
                   ("startup_warnings", ["configuration mismatch"])]
        for key, value in patches:
            with self.subTest(field=key):
                self.request = FakeRequest()
                self.request.health[key] = value
                self.assert_status(self.run_probe(), "failed")
                self.assertFalse(any(c[0] == PATH for c in self.request.calls))
        for key in ("price_precision", "roi12_softcap", "tuple_noise_release",
                    "settle_unresolved", "scoring_settle", "env_combo"):
            with self.subTest(gate=key):
                self.request = FakeRequest()
                self.request.health["engine_gates"][key] = "off"
                self.assert_status(self.run_probe(), "failed")
                self.assertFalse(any(c[0] == PATH for c in self.request.calls))

    def test_backend_commit_requires_full_matching_forty_hex_characters(self):
        for value in (COMMIT[:8], "g" * 40, "b" * 40, None):
            with self.subTest(commit=value):
                self.request = FakeRequest()
                self.request.health["deploy"]["render_git_commit"] = value
                self.assert_status(self.run_probe(), "failed")
                self.assertFalse(any(c[0] == PATH for c in self.request.calls))

    def test_end_health_change_invalidates_good_rows_but_retains_the_response_receipt(self):
        for section, key, value in (("deploy", "render_git_commit", "b" * 40),
                                    (None, "engine_version", "5.153.0"),
                                    ("engine_gates", "roi12_softcap", "enforce"),
                                    (None, "engine_ready", False)):
            with self.subTest(field=key):
                self.request = FakeRequest()
                self.request.after = copy.deepcopy(self.request.health)
                target = self.request.after[section] if section else self.request.after
                target[key] = value
                result = self.run_probe()
                self.assert_status(result, "failed")
                self.assertEqual(self.request.calls[-1][0], "/health")
                self.assertEqual(result["response_receipt"]["response_sha256"], self.post_receipt()["response_sha256"])

    def test_envelope_requires_exact_page_version_status_count_and_row_objects(self):
        for field, value in (("page", "Global_Markets"), ("version", "4.8.2"),
                             ("status", "disabled"), ("count", 2), ("count", True),
                             ("row_objects", {"symbol": SYMBOLS[0]})):
            with self.subTest(field=field, value=value):
                self.request = FakeRequest()
                self.request.payload[field] = value
                self.assert_status(self.run_probe(), "failed")

    def test_incomplete_duplicate_and_cross_symbol_rows_cannot_be_verified(self):
        for mode in ("missing", "duplicate", "unexpected", "malformed"):
            with self.subTest(mode=mode):
                self.request = FakeRequest()
                rows = self.request.payload["row_objects"]
                if mode == "missing":
                    rows.pop()
                elif mode == "duplicate":
                    rows[2] = copy.deepcopy(rows[0])
                elif mode == "unexpected":
                    rows[2]["symbol"] = rows[2]["requested_symbol"] = "BTC-USD"
                else:
                    rows[2] = ["XRP-USD", 1.4083]
                self.assert_status(self.run_probe(), "failed")

    def test_requested_symbol_oath_rejects_cross_symbol_ownership(self):
        self.request.payload["row_objects"][0]["requested_symbol"] = "DOGE-USD"
        self.assert_status(self.run_probe(), "failed")

    def test_unordered_exact_rows_and_absent_optional_requested_symbol_are_supported(self):
        rows = self.request.payload["row_objects"]
        rows.reverse()
        for row in rows:
            del row["requested_symbol"]
        self.request.payload["status"] = "success"
        self.assert_status(self.run_probe(), "verified")

    def test_arbitrary_backend_private_fields_are_excluded_from_success_artifact(self):
        self.request.payload["private_token"] = PRIVATE
        self.request.health["private_account"] = PRIVATE
        self.request.payload["row_objects"][0]["account_id"] = PRIVATE
        self.request.payload["row_objects"][0]["upstream_raw_response"] = {"token": PRIVATE}
        result = self.run_probe()
        self.assert_status(result, "verified")
        self.assertNotIn(PRIVATE, json.dumps(result))
        self.assertNotIn("upstream_raw_response", json.dumps(result))

    def test_http_failure_retains_actual_failure_receipt_without_leaking_exception_or_body(self):
        self.request.post_status = 401
        self.request.payload = {"error": PRIVATE, "account_id": PRIVATE}
        self.request.post_error = probe.ReadbackError("HTTP401 " + PRIVATE)
        result = self.run_probe()
        self.assert_status(result, "failed")
        self.assertEqual(result["response_receipt"]["http_status"], 401)
        self.assertEqual(result["response_receipt"]["response_sha256"], self.post_receipt()["response_sha256"])
        self.assertNotIn(PRIVATE, json.dumps(result))
        self.assertEqual(self.request.calls[-1][0], "/health")

    def test_missing_null_empty_and_zero_tiny_forecasts_are_withheld_not_passed(self):
        for horizon in HORIZONS:
            for mode in ("missing", None, "", 0):
                with self.subTest(horizon=horizon, value=mode):
                    self.request = FakeRequest()
                    row = self.request.payload["row_objects"][0]
                    field = "forecast_price_" + horizon
                    if mode == "missing":
                        del row[field]
                    else:
                        row[field] = mode
                    result = self.run_probe()
                    self.assert_status(result, "withheld")
                    self.assertEqual(result["response_receipt"]["response_sha256"], self.post_receipt()["response_sha256"])

    def test_invalid_nonfinite_or_wrong_type_numbers_fail_and_are_sanitized_to_null(self):
        for field in ("current_price", "forecast_price_1m", "expected_roi_12m"):
            for value in (True, "0.01", float("nan"), float("inf"), -float("inf"), {"token": PRIVATE}):
                with self.subTest(field=field, value=value):
                    self.request = FakeRequest()
                    self.request.payload["row_objects"][0][field] = value
                    result = self.run_probe()
                    self.assert_status(result, "failed")
                    if result["rows"]:
                        row = next(r for r in result["rows"] if r.get("symbol") == "SHIB-USD")
                        self.assertIsNone(row.get(field))
                    self.assertNotIn(PRIVATE, json.dumps(result, allow_nan=False))

    def test_percentage_points_cannot_pass_as_fractional_roi(self):
        row = self.request.payload["row_objects"][1]
        for horizon in HORIZONS:
            row["expected_roi_" + horizon] *= 100
        self.assert_status(self.run_probe(), "failed")

    def test_subcent_prices_and_three_horizon_tuple_numbers_survive_readback(self):
        result = self.run_probe()
        self.assert_status(result, "verified")
        shib = next(r for r in result["rows"] if r["symbol"] == "SHIB-USD")
        self.assertEqual(shib["current_price"], 0.00000549)
        for horizon in HORIZONS:
            self.assertGreater(shib["forecast_price_" + horizon], 0)
            self.assertAlmostEqual(shib["expected_roi_" + horizon],
                                   shib["forecast_price_" + horizon] / shib["current_price"] - 1,
                                   places=12)
            self.assertEqual(shib["tuples"][horizon]["status"], "verified")

    def test_precision_tolerance_includes_price_and_six_decimal_roi_rounding_but_is_bounded(self):
        # 5.5449e-6 has seven-significant-digit quantum1e-12. Half a
        # forecast-price quantum / CP plus half a six-decimal ROI quantum.
        tolerance = (0.5e-12 / 0.00000549) + 0.5e-6
        for factor, expected in ((0.9, "verified"), (1.1, "failed")):
            with self.subTest(error_factor=factor):
                self.request = FakeRequest()
                row = self.request.payload["row_objects"][0]
                row["expected_roi_1m"] += factor * tolerance
                self.assert_status(self.run_probe(), expected)

    def test_absent_acquisition_receipt_cannot_be_replaced_by_fresh_row_timestamp(self):
        row = self.request.payload["row_objects"][0]
        row["warnings"] = "low_data_trust"
        row["last_updated_utc"] = iso(datetime.now(UTC))
        before = copy.deepcopy(self.request.payload)
        result = self.run_probe()
        self.assert_status(result, "withheld")
        self.assertEqual(self.request.payload, before)
        observed = next(r for r in result["rows"] if r["symbol"] == "SHIB-USD")
        self.assertIn(observed.get("acquisition_acquired_at"), (None, ""))

    def test_acquisition_status_provider_time_zone_age_and_future_bounds_are_required(self):
        now = datetime.now(UTC)
        warnings = [acquisition(status="unavailable"), acquisition(provider="yahoo_chart"),
                    acquisition(stamp=now - timedelta(seconds=901)),
                    acquisition(quote_stamp=now - timedelta(seconds=901)),
                    acquisition(stamp=now + timedelta(seconds=65)),
                    acquisition(quote_stamp=now + timedelta(seconds=65)),
                    acquisition().replace("+00:00", ""),
                    acquisition() + "; acquisition_provider:eodhd"]
        for warning in warnings:
            with self.subTest(warning=warning):
                self.request = FakeRequest()
                self.request.payload["row_objects"][0]["warnings"] = warning
                self.assert_status(self.run_probe(), "withheld")

    def test_good_tuples_outside_tiny_price_scope_are_withheld(self):
        for symbol, price in (("SHIB-USD", 0.0001), ("DOGE-USD", 10.0)):
            with self.subTest(symbol=symbol):
                self.request = FakeRequest()
                rows = self.request.payload["row_objects"]
                index = SYMBOLS.index(symbol)
                rows[index] = sample_row(symbol, price)
                self.assert_status(self.run_probe(), "withheld")

    def test_zero_fraction_roi_with_genuine_unchanged_forecast_is_a_valid_tuple(self):
        for row in self.request.payload["row_objects"]:
            for horizon in HORIZONS:
                row["forecast_price_" + horizon] = row["current_price"]
                row["expected_roi_" + horizon] = 0.0
        self.assert_status(self.run_probe(), "verified")


class TinyQuoteCliTests(unittest.TestCase):
    def invoke(self, token="local-test-token", tiny=False, core=None, version_reader=None):
        request = FakeRequest()
        stream = io.StringIO()
        environment = {"BACKEND_TOKEN": token, "GITHUB_SHA": WORKFLOW_COMMIT,
                       "GITHUB_RUN_ID": "38210000000"}
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "artifact.json"
            args = ["readback", "--expected-commit", COMMIT, "--output", str(output)]
            if tiny:
                args.append("--tiny-quote-readback")
            with contextlib.ExitStack() as stack:
                stack.enter_context(patch.dict(os.environ, environment))
                stack.enter_context(patch("sys.argv", args))
                transport = stack.enter_context(patch.object(probe, "transport", return_value=request))
                verify = stack.enter_context(patch.object(probe, "verify", side_effect=core,
                                                         return_value={"ok": True}))
                if version_reader is not None:
                    stack.enter_context(patch.object(probe, "source_version", side_effect=version_reader))
                tiny_probe = stack.enter_context(patch.object(probe, "verify_tiny_quotes",
                                                               wraps=probe.verify_tiny_quotes))
                stack.enter_context(contextlib.redirect_stdout(stream))
                exit_code = probe.main()
            self.assertTrue(output.is_file(), "Every CLI failure must retain an artifact")
            artifact = json.loads(output.read_text())
            stdout = json.loads(stream.getvalue())
        return exit_code, artifact, stdout, request, transport, verify, tiny_probe

    def test_missing_backend_token_writes_failure_without_constructing_transport(self):
        code, artifact, stdout, request, transport, verify, tiny = self.invoke(token="", tiny=True)
        self.assertEqual(code, 1)
        self.assertIs(artifact["ok"], False)
        self.assertEqual(artifact["error"], "BACKEND_TOKEN is not configured")
        self.assertEqual(artifact["http_receipts"], [])
        self.assertEqual(artifact["tiny_quote_readback"]["status"], "failed")
        self.assertEqual(stdout, artifact)
        self.assertEqual(request.calls, [])
        transport.assert_not_called()
        verify.assert_not_called()
        tiny.assert_not_called()

    def test_quoted_and_backslashed_credential_is_redacted_recursively_before_encoding(self):
        token = 'quoted"\\credential-sentinel'
        nested = {"ok": True, "public": {"notes": [{"opaque": "before " + token + " after"}]}}
        code, artifact, stdout, _, _, _, _ = self.invoke(
            token=token, core=lambda *_args: copy.deepcopy(nested))
        self.assertEqual(code, 0)
        for payload in (artifact, stdout):
            text = payload["public"]["notes"][0]["opaque"]
            self.assertNotIn(token, text)
            self.assertIn("[REDACTED]", text)
            self.assertNotIn(json.dumps(token)[1:-1], json.dumps(payload))

    def test_optional_route_source_failure_retains_failed_artifact_and_prior_http_receipts(self):
        original = probe.source_version

        def source_version(path, name):
            if path == "routes/analysis_sheet_rows.py":
                raise probe.ReadbackError("private-source-location-must-not-leak")
            return original(path, name)

        def core(request, *_args):
            # A real local fake-response receipt exists before optional lookup.
            request("/health")
            return {"ok": True}

        code, artifact, stdout, request, _, _, tiny = self.invoke(
            tiny=True, core=core, version_reader=source_version)
        self.assertEqual(code, 1)
        self.assertIs(artifact["ok"], False)
        self.assertEqual(artifact["tiny_quote_readback"]["status"], "failed")
        self.assertEqual(artifact["http_receipts"], request.receipts)
        self.assertEqual(artifact["http_receipts"][0]["path"], "/health")
        self.assertEqual(len(artifact["http_receipts"][0]["response_sha256"]), 64)
        self.assertNotIn("private-source-location-must-not-leak", json.dumps(artifact))
        self.assertEqual(stdout, artifact)
        tiny.assert_not_called()

    def test_cli_provenance_matches_helper_and_workflow_while_tiny_probe_defaults_off(self):
        expected_hash = hashlib.sha256(Path(probe.__file__).read_bytes()).hexdigest()
        for enabled in (False, True):
            with self.subTest(tiny_quote_readback=enabled):
                code, artifact, stdout, request, _, _, tiny = self.invoke(tiny=enabled)
                self.assertEqual(code, 0)
                self.assertEqual(artifact["helper_source_sha256"], expected_hash)
                self.assertEqual(artifact["readback_version"], probe.SCRIPT_VERSION)
                self.assertEqual(artifact["workflow_source_sha"], WORKFLOW_COMMIT)
                self.assertEqual(artifact["expected_backend_commit"], COMMIT)
                self.assertEqual(artifact["workflow_run_id"], "38210000000")
                self.assertEqual(stdout, artifact)
                if enabled:
                    tiny.assert_called_once()
                    self.assertEqual(artifact["tiny_quote_readback"]["status"], "verified")
                    self.assertEqual([c[0] for c in request.calls], ["/health", PATH, "/health"])
                else:
                    tiny.assert_not_called()
                    self.assertNotIn("tiny_quote_readback", artifact)
                    self.assertEqual(request.calls, [])


if __name__ == "__main__":
    unittest.main()
