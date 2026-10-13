"""Deployment verification must not approve stale code or failed funding guards."""
import copy
import json
import unittest

from scripts import verify_core_repair_readback as probe


class ReadbackProtocolTests(unittest.TestCase):
    def setUp(self):
        self.calls = []
        self.portfolio_version = probe.source_version(
            "core/analysis/portfolio_actions.py", "PORTFOLIO_ACTIONS_VERSION")
        self.health = {"ready": True, "engine_version": "engine-test",
                       "deploy": {"render_git_commit": "commit-test"},
                       "engine_gates": {"margin_publish": "observe"}}
        self.zero_kpis = {"selected_count": 0, "expected_gain_12m_sar": 0,
                          "deployable_sar": 0, "capital_unallocated_sar": 0}
        self.snapshot = {"rows": [], "snapshot_id": "synthetic-signature",
                         "contract_version": 1, "builder_version": "builder-test"}
        self.research = {"version": "builder-test", "status": "ok", "selected": [], "kpis": copy.deepcopy(self.zero_kpis),
                         "alerts": [], "meta": {"board_funding": {
                             "stage": "research", "snapshot_id": "synthetic-signature",
                             "contract_version": 1, "snapshot_available": True, "snapshot": self.snapshot}}}
        self.allocated = {"version": "builder-test", "status": "no_candidates",
                          "selected": [], "kpis": copy.deepcopy(self.zero_kpis), "alerts": [],
                          "meta": {"execution_ready": False, "input_certification": {"funding_eligible": False},
                              "board_funding": {"stage": "allocate",
                              "contract_version": 1, "snapshot_id": "synthetic-signature",
                              "eligible_symbols": [],
                              "snapshot_available": True}}}
        self.protective = {"version": self.portfolio_version, "status": "ok", "sector_summary": [],
            "kpis": {"deployable_sar": 0, "adds_funded_sar": 0, "proceeds_pending_sar": 0,
                     "capital_unallocated_sar": 0, "portfolio_value_sar": None, "holdings_value_sar": None,
                     "cash_sar": None, "cash_pct": None, "cost_basis_sar": None, "pnl_sar": None, "pnl_pct": None},
            "actions": [{"symbol": "SYNTH.SR", "action": "BLOCK", "suggested_delta_sar": 0,
                         "suggested_delta_shares": 0, "proceeds_sar": 0,
                         **{key: None for key in ("market_value_sar", "cost_sar", "pnl_sar", "pnl_pct", "weight_pct",
                                                  "post_trade_weight_pct", "stop_sar", "tp1_sar", "tp2_sar", "funds_from")},
                         "detail": {"execution_ready": False, "position_evidence_matched": False, "sector_weight_pct": None}}],
            "alerts": [{"type": "portfolio_inputs_unverified"}],
            "meta": {"execution_ready": False, "input_certification": {"funding_eligible": False},
                     "versions": {"portfolio_actions": self.portfolio_version},
                     "route": {"portfolio_actions_version": self.portfolio_version}}}
        self.rejected = {"status": "board_funding_mismatch", "selected": [],
                         "kpis": copy.deepcopy(self.zero_kpis), "alerts": []}

    def request(self, path, body=None, authenticated=False):
        self.calls.append((path, copy.deepcopy(body), authenticated))
        if path == "/health":
            return copy.deepcopy(self.health)
        self.assertTrue(authenticated)
        if path == probe.PORTFOLIO_ACTIONS_PATH:
            return copy.deepcopy(self.protective)
        if body["criteria"]["Board Funding Stage"] == "research":
            return copy.deepcopy(self.research)
        if body["portfolio"]["cash_available_sar"] or body["fx_rates"]["USD"] != 3.75:
            return copy.deepcopy(self.rejected)
        return copy.deepcopy(self.allocated)

    def run_probe(self):
        return probe.verify(self.request, "commit-test", "engine-test", "builder-test",
                            expected_portfolio_actions=self.portfolio_version)

    def test_complete_protocol_is_blocked_zero_cash_and_does_not_publish_snapshot(self):
        result = self.run_probe()
        self.assertTrue(result["ok"])
        self.assertEqual(len(self.calls), 7)
        research_body = self.calls[1][1]
        self.assertEqual(research_body["rows"][0]["investability_status"], "BLOCKED")
        self.assertEqual(research_body["portfolio"]["cash_available_sar"], 0)
        self.assertEqual(research_body["criteria"], {"Board Funding Stage": "research"})
        self.assertEqual(self.calls[2][1]["criteria"]["Board Funding Symbols"], [])
        self.assertEqual(self.calls[2][1]["rows"], [])
        self.assertNotIn("synthetic-signature", json.dumps(result))
        portfolio_body = self.calls[-2][1]
        self.assertEqual(self.calls[-2][0], "/sheet-rows/portfolio-actions")
        self.assertTrue(self.calls[-2][2])
        self.assertEqual(portfolio_body["controls"]["Cash Available (SAR)"], 10_000)
        self.assertNotIn("reconciliation_evidence", portfolio_body)
        self.assertEqual(portfolio_body["rows"][0]["Recommendation"], "BUY")
        self.assertTrue(result["unreconciled_portfolio_blocked"])
        self.assertTrue(result["uncertified_replay_withheld"])
        self.assertEqual(result["portfolio_actions_version"], self.portfolio_version)

    def test_replay_and_portfolio_need_explicit_false_booleans(self):
        for payload in (self.allocated, self.protective):
            for section, key in ((payload["meta"], "execution_ready"),
                                 (payload["meta"]["input_certification"], "funding_eligible")):
                for bad in (None, True, 0, "false"):
                    with self.subTest(value=bad, key=key):
                        section[key] = bad
                        with self.assertRaisesRegex(probe.ReadbackError, "explicitly withhold"):
                            self.run_probe()
                del section[key]
                with self.assertRaisesRegex(probe.ReadbackError, "explicitly withhold"):
                    self.run_probe()
                section[key] = False

    def test_portfolio_version_and_protective_single_row_are_mandatory(self):
        for field, value in (("version", "1.14.0"), ("status", "disabled"), ("actions", []),
                             ("actions", self.protective["actions"] * 2)):
            with self.subTest(field=field):
                original = self.protective[field]
                self.protective[field] = value
                with self.assertRaises(probe.ReadbackError):
                    self.run_probe()
                self.protective[field] = original
        for key, value in (("symbol", "OTHER.SR"), ("action", "HOLD")):
            original = self.protective["actions"][0][key]
            self.protective["actions"][0][key] = value
            with self.assertRaises(probe.ReadbackError):
                self.run_probe()
            self.protective["actions"][0][key] = original
        self.protective["meta"]["route"]["portfolio_actions_version"] = "1.14.0"
        with self.assertRaisesRegex(probe.ReadbackError, "version attestation"):
            self.run_probe()

    def test_portfolio_money_requires_numeric_zero_and_unproven_values_require_null(self):
        kpis, action = self.protective["kpis"], self.protective["actions"][0]
        for section, keys in ((kpis, ("deployable_sar", "adds_funded_sar", "proceeds_pending_sar", "capital_unallocated_sar")),
                              (action, ("suggested_delta_sar", "suggested_delta_shares", "proceeds_sar"))):
            for key in keys:
                for bad in (True, "0", None, 1, float("nan"), float("inf")):
                    section[key] = bad
                    with self.subTest(key=key, value=bad), self.assertRaises(probe.ReadbackError):
                        self.run_probe()
                del section[key]
                with self.assertRaises(probe.ReadbackError):
                    self.run_probe()
                section[key] = 0
        for section, keys in ((kpis, ("portfolio_value_sar", "holdings_value_sar", "cash_sar", "cash_pct", "cost_basis_sar", "pnl_sar", "pnl_pct")),
                              (action, ("market_value_sar", "cost_sar", "pnl_sar", "pnl_pct", "weight_pct", "post_trade_weight_pct",
                                        "stop_sar", "tp1_sar", "tp2_sar", "funds_from"))):
            for key in keys:
                section[key] = 0
                with self.subTest(key=key), self.assertRaises(probe.ReadbackError):
                    self.run_probe()
                del section[key]
                with self.assertRaises(probe.ReadbackError):
                    self.run_probe()
                section[key] = None

    def test_portfolio_protection_metadata_and_alerts_cannot_be_missing_or_funding_advice(self):
        detail = self.protective["actions"][0]["detail"]
        for key in ("execution_ready", "position_evidence_matched"):
            detail[key] = 0
            with self.assertRaises(probe.ReadbackError):
                self.run_probe()
            detail[key] = False
        self.protective["alerts"] = [{"type": "capital_call", "private_backend_note": "private-sentinel"}]
        with self.assertRaises(probe.ReadbackError) as caught:
            self.run_probe()
        self.assertNotIn("private-sentinel", str(caught.exception))

    def test_success_artifact_excludes_backend_fields_and_private_echoes(self):
        self.protective["private_backend_note"] = "private-sentinel"
        self.protective["meta"]["upstream"] = {"account_id": "private-account-sentinel"}
        self.health["engine_gates"]["margin_publish"] = "private-health-sentinel"
        serialized = json.dumps(self.run_probe())
        for value in ("private-sentinel", "private-account-sentinel", "private-health-sentinel", "Unreconciled synthetic", "Position Qty"):
            self.assertNotIn(value, serialized)

    def test_explicit_expected_portfolio_version_preserves_old_verify_arguments(self):
        current = probe.verify(self.request, "commit-test", "engine-test", "builder-test")
        self.assertTrue(current["ok"])
        self.assertEqual(current["portfolio_actions_version"], self.portfolio_version)
        # An explicitly requested historical version still uses the original
        # positional API. It must not be accepted by the current-source default.
        self.protective["version"] = "1.15.0"
        self.protective["meta"]["versions"]["portfolio_actions"] = "1.15.0"
        self.protective["meta"]["route"]["portfolio_actions_version"] = "1.15.0"
        result = probe.verify(self.request, "commit-test", "engine-test", "builder-test", 1, "1.15.0")
        self.assertTrue(result["ok"])
        self.assertEqual(result["portfolio_actions_version"], "1.15.0")
        with self.assertRaisesRegex(probe.ReadbackError, "version or status differs from source"):
            probe.verify(self.request, "commit-test", "engine-test", "builder-test")

    def test_wrong_commit_stops_before_authenticated_call(self):
        self.health["deploy"]["render_git_commit"] = "older-release"
        with self.assertRaisesRegex(probe.ReadbackError, "deployed commit"):
            self.run_probe()
        self.assertEqual(len(self.calls), 1)

    def test_wrong_engine_or_not_ready_stops_before_probe(self):
        for key, value in (("ready", False), ("engine_version", "old-engine")):
            with self.subTest(key=key):
                original = self.health[key]
                self.health[key] = value
                with self.assertRaisesRegex(probe.ReadbackError, "expected ready"):
                    self.run_probe()
                self.health[key] = original

    def test_wrong_builder_or_missing_signature_is_not_a_success(self):
        self.research["version"] = "old-builder"
        with self.assertRaisesRegex(probe.ReadbackError, "builder version"):
            self.run_probe()
        self.research["version"] = "builder-test"
        self.snapshot["snapshot_id"] = ""
        with self.assertRaisesRegex(probe.ReadbackError, "empty signed snapshot"):
            self.run_probe()

    def test_research_authentication_cannot_be_bypassed_with_unsigned_snapshot(self):
        self.research["meta"]["board_funding"]["snapshot_available"] = False
        with self.assertRaisesRegex(probe.ReadbackError, "signed research snapshot unavailable"):
            self.run_probe()

    def test_selected_or_nonzero_gain_is_rejected(self):
        self.allocated["selected"] = [{"symbol": "AAPL.US", "suggested_sar": 1}]
        with self.assertRaisesRegex(probe.ReadbackError, "selected tickets"):
            self.run_probe()
        self.allocated["selected"] = []
        self.allocated["kpis"]["expected_gain_12m_sar"] = 100
        with self.assertRaisesRegex(probe.ReadbackError, "KPI"):
            self.run_probe()

    def test_funding_alert_is_rejected_even_with_empty_selected(self):
        self.allocated["alerts"] = [{"type": "capital_call", "value_sar": 100}]
        with self.assertRaisesRegex(probe.ReadbackError, "funding alert"):
            self.run_probe()

    def test_empty_kpis_are_not_evidence_of_zero_money(self):
        self.allocated["kpis"] = {}
        with self.assertRaisesRegex(probe.ReadbackError, "monetary KPIs missing"):
            self.run_probe()

    def test_wrong_replay_signature_or_contract_is_not_attested(self):
        board = self.allocated["meta"]["board_funding"]
        for key, value in (("snapshot_id", "wrong-snapshot"), ("contract_version", 99)):
            with self.subTest(key=key):
                original = board[key]
                board[key] = value
                with self.assertRaisesRegex(probe.ReadbackError, "replay rejected"):
                    self.run_probe()
                board[key] = original

    def test_accepted_changed_basis_is_a_failure_even_when_money_is_zero(self):
        self.rejected["status"] = "ok"
        with self.assertRaisesRegex(probe.ReadbackError, "basis was accepted"):
            self.run_probe()

    def test_deployment_change_during_readback_is_rejected(self):
        def changing_request(path, body=None, authenticated=False):
            result = self.request(path, body, authenticated)
            if path == "/health" and len(self.calls) > 1:
                result["deploy"]["render_git_commit"] = "next-release"
            return result
        with self.assertRaisesRegex(probe.ReadbackError, "changed during"):
            probe.verify(changing_request, "commit-test", "engine-test", "builder-test")
        self.assertEqual(len(self.calls), 7)
        self.assertEqual(self.calls[-2][0], probe.PORTFOLIO_ACTIONS_PATH)
        self.assertEqual(self.calls[-1][0], "/health")

    def test_authenticated_redirect_is_refused(self):
        with self.assertRaisesRegex(probe.ReadbackError, "redirect refused"):
            probe.NoRedirect().redirect_request(None, None, 302, "", {}, "https://elsewhere.example")


class PortfolioResearchViewLockstepTests(unittest.TestCase):
    """v1.2.0 (2026-10-13, P0-6): research-view amounts need a declared mode.

    Reuses the protocol fixture above (the v1.1.1 tests keep running there
    unchanged with the default off) and adds the declared observe / enforce
    contract for portfolio_actions 1.16.0.
    """

    setUp = ReadbackProtocolTests.setUp
    request = ReadbackProtocolTests.request
    run_probe = ReadbackProtocolTests.run_probe
    TAIL = (" | [custody-unverified] new-money verdict withheld until fresh positions and settled cash"
            " are reconciled")

    def serve_research_view(self, mode):
        """Turn the protective fixture into a served research-view page."""
        row = self.protective["actions"][0]
        prefix = "RECONCILIATION_REQUIRED: missing_evidence"
        if mode == "observe":
            row["action_reason"] = prefix + " | [research-observe] [custody-unverified] new-money verdict withheld"
        else:
            row["action_reason"] = prefix + self.TAIL + "; not executable"
            row.update({"market_value_sar": 1000.0, "cost_sar": 900.0, "pnl_sar": 100.0, "pnl_pct": 11.1,
                        "stop_sar": 92.0, "tp1_sar": 115.0, "tp2_sar": 130.0})
            self.protective["kpis"].update({"holdings_value_sar": 1000.0, "cost_basis_sar": 900.0,
                                            "pnl_sar": 100.0, "pnl_pct": 11.1})
        row["detail"]["research_view"] = {"mode": mode, "tier": "unverified", "verdict": "NEW_MONEY_WITHHELD"}
        self.protective["meta"]["research_view"] = {"mode": mode, "status": "ok"}

    def run_declared(self, mode):
        return probe.verify(self.request, "commit-test", "engine-test", "builder-test",
                            expected_portfolio_actions=self.portfolio_version,
                            expected_pf_research_view=mode)

    def test_declared_enforce_accepts_levels_and_holdings_kpis(self):
        self.serve_research_view("enforce")
        result = self.run_declared("enforce")
        self.assertTrue(result["ok"])
        self.assertEqual(result["pf_research_view"], "enforce")

    def test_declared_enforce_rejects_blanked_research_amounts(self):
        # v1.2.1 (PR #764 review): the probe has every input, so a served
        # enforce page that blanks any newly exposed amount is a regression.
        for key in ("market_value_sar", "cost_sar", "pnl_sar", "pnl_pct",
                    "stop_sar", "tp1_sar", "tp2_sar"):
            self.setUp()
            self.serve_research_view("enforce")
            self.protective["actions"][0][key] = None
            with self.assertRaisesRegex(probe.ReadbackError, "did not publish: " + key):
                self.run_declared("enforce")
        for key in ("holdings_value_sar", "cost_basis_sar", "pnl_sar", "pnl_pct"):
            self.setUp()
            self.serve_research_view("enforce")
            self.protective["kpis"][key] = None
            with self.assertRaisesRegex(probe.ReadbackError, "did not publish KPI: " + key):
                self.run_declared("enforce")

    def test_undeclared_research_amounts_are_rejected_even_when_the_page_says_enforce(self):
        self.serve_research_view("enforce")
        with self.assertRaisesRegex(probe.ReadbackError,
                                    "uncertified portfolio amount was published: holdings_value_sar"):
            self.run_probe()
        with self.assertRaisesRegex(probe.ReadbackError, "uncertified portfolio amount was published"):
            self.run_declared("observe")
        for key in ("holdings_value_sar", "cost_basis_sar", "pnl_sar", "pnl_pct"):
            self.protective["kpis"][key] = None
        with self.assertRaisesRegex(probe.ReadbackError, "execution level: market_value_sar"):
            self.run_probe()

    def test_declared_enforce_keeps_every_money_execution_weight_sector_and_alert_check(self):
        cases = (("kpis", "deployable_sar", 5, "withheld portfolio KPI"),
                 ("kpis", "cash_sar", 10_000, "published: cash_sar"),
                 ("kpis", "portfolio_value_sar", 1000, "published: portfolio_value_sar"),
                 ("kpis", "cash_pct", 10.0, "published: cash_pct"),
                 ("row", "suggested_delta_sar", 100, "execution money"),
                 ("row", "suggested_delta_shares", -10, "execution money"),
                 ("row", "proceeds_sar", 1000, "execution money"),
                 ("row", "weight_pct", 10.0, "execution level: weight_pct"),
                 ("row", "post_trade_weight_pct", 5.0, "execution level: post_trade_weight_pct"),
                 ("row", "funds_from", "cash", "execution level: funds_from"),
                 ("row", "action", "EXIT", "was not blocked"))
        for section, key, value, message in cases:
            with self.subTest(key=key):
                self.setUp()
                self.serve_research_view("enforce")
                target = self.protective["kpis"] if section == "kpis" else self.protective["actions"][0]
                target[key] = value
                with self.assertRaisesRegex(probe.ReadbackError, message):
                    self.run_declared("enforce")
        mutations = (
            (lambda: self.protective["actions"][0]["detail"].update(execution_ready=True), "not withheld"),
            (lambda: self.protective["actions"][0]["detail"].update(position_evidence_matched=True), "not withheld"),
            (lambda: self.protective["actions"][0]["detail"].update(sector_weight_pct=12.0), "not withheld"),
            (lambda: self.protective.update(sector_summary=[{"sector": "Energy"}]), "sector weights"),
            (lambda: self.protective.update(alerts=[{"type": "capital_call"}]), "protective alert"),
            (lambda: self.protective["meta"].update(execution_ready=True), "explicitly withhold"))
        for mutate, message in mutations:
            self.setUp()
            self.serve_research_view("enforce")
            mutate()
            with self.subTest(message=message), self.assertRaisesRegex(probe.ReadbackError, message):
                self.run_declared("enforce")

    def test_declared_enforce_rejects_invalid_research_amounts(self):
        for key, value in (("market_value_sar", 0), ("market_value_sar", -5.0), ("stop_sar", True),
                           ("tp1_sar", "115"), ("cost_sar", float("nan")), ("pnl_sar", float("inf"))):
            with self.subTest(key=key, value=value):
                self.setUp()
                self.serve_research_view("enforce")
                self.protective["actions"][0][key] = value
                with self.assertRaisesRegex(probe.ReadbackError, "execution level: " + key):
                    self.run_declared("enforce")
        self.setUp()
        self.serve_research_view("enforce")
        self.protective["kpis"]["holdings_value_sar"] = 0
        with self.assertRaisesRegex(probe.ReadbackError, "published: holdings_value_sar"):
            self.run_declared("enforce")

    def test_declared_mode_must_be_served_with_its_tag(self):
        with self.assertRaisesRegex(probe.ReadbackError, "research view mode not served: enforce"):
            self.run_declared("enforce")
        with self.assertRaisesRegex(probe.ReadbackError, "research view mode not served: observe"):
            self.run_declared("observe")
        self.serve_research_view("observe")
        self.assertTrue(self.run_declared("observe")["ok"])
        self.assertTrue(self.run_probe()["ok"])  # observe numbers equal off
        with self.assertRaisesRegex(probe.ReadbackError, "research view mode not served: enforce"):
            self.run_declared("enforce")
        self.protective["actions"][0]["action_reason"] = "RECONCILIATION_REQUIRED: missing_evidence"
        with self.assertRaisesRegex(probe.ReadbackError, "observe portfolio research view tag missing"):
            self.run_declared("observe")
        self.setUp()
        self.serve_research_view("enforce")
        prefix = "RECONCILIATION_REQUIRED: missing_evidence"
        for reason, message in (
                (prefix + self.TAIL, "enforce portfolio research view tag missing"),
                (prefix + " | [custody-matched] x; not executable", "enforce portfolio research view tag missing"),
                ("missing_evidence" + self.TAIL + "; not executable", "reconciliation prefix"),
                (prefix + " | [custody-unverified] research verdict ADD: x; not executable", "named an ADD verdict")):
            self.protective["actions"][0]["action_reason"] = reason
            with self.subTest(reason=reason), self.assertRaisesRegex(probe.ReadbackError, message):
                self.run_declared("enforce")
        self.protective["actions"][0]["action_reason"] = prefix + self.TAIL + "; not executable"
        self.protective["actions"][0]["detail"]["research_view"]["verdict"] = "ADD"
        with self.assertRaisesRegex(probe.ReadbackError, "named an ADD verdict"):
            self.run_declared("enforce")

    def test_declared_enforce_holdings_total_must_match_the_row(self):
        self.serve_research_view("enforce")
        self.protective["kpis"]["holdings_value_sar"] = 5000.0
        with self.assertRaisesRegex(probe.ReadbackError, "holdings total disagrees"):
            self.run_declared("enforce")
        # v1.2.1: a blank row value and a blank total no longer pass under a
        # declared enforce; the probe has every input (PR #764 review).
        self.protective["kpis"]["holdings_value_sar"] = None
        self.protective["actions"][0]["market_value_sar"] = None
        with self.assertRaisesRegex(probe.ReadbackError, "did not publish: market_value_sar"):
            self.run_declared("enforce")

    def test_unsupported_mode_is_refused_before_any_request(self):
        for mode in ("ENFORCE", "on", "", None):
            with self.subTest(mode=mode), self.assertRaisesRegex(probe.ReadbackError, "unsupported"):
                self.run_declared(mode)
        self.assertEqual(self.calls, [])
        with self.assertRaisesRegex(probe.ReadbackError, "unsupported"):
            probe.require_blocked_portfolio(self.protective, self.portfolio_version, "bogus")

    def test_cli_declares_the_mode_only_when_not_off(self):
        import contextlib
        import io
        import os
        import tempfile
        from pathlib import Path
        from unittest import mock
        commit = "a" * 40

        def invoke(*extra):
            with tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "artifact.json"
                argv = ["readback", "--expected-commit", commit, "--workflow-source-sha", commit,
                        "--output", str(output), *extra]
                with mock.patch.dict(os.environ, {"BACKEND_TOKEN": "local-test-token"}), \
                        mock.patch("sys.argv", argv), \
                        mock.patch.object(probe, "transport", return_value=lambda *_a, **_k: {}) as transport, \
                        mock.patch.object(probe, "verify", return_value={"ok": True}) as verify, \
                        contextlib.redirect_stdout(io.StringIO()):
                    code = probe.main()
                return code, json.loads(output.read_text()), transport, verify

        code, artifact, _transport, verify = invoke()
        self.assertEqual(code, 0)
        self.assertEqual(verify.call_args.kwargs, {})
        self.assertNotIn("expected_pf_research_view", artifact)
        code, artifact, _transport, verify = invoke("--expect-pf-research-view", "off")
        self.assertEqual(verify.call_args.kwargs, {})
        self.assertNotIn("expected_pf_research_view", artifact)
        for mode in ("observe", "enforce", " Enforce "):
            code, artifact, _transport, verify = invoke("--expect-pf-research-view", mode)
            self.assertEqual(code, 0)
            self.assertEqual(verify.call_args.kwargs, {"expected_pf_research_view": mode.strip().lower()})
            self.assertEqual(artifact["expected_pf_research_view"], mode.strip().lower())
        code, artifact, transport, verify = invoke("--expect-pf-research-view", "armed")
        self.assertEqual(code, 1)
        self.assertEqual(artifact["error"], "unsupported expected portfolio research view mode")
        self.assertEqual(artifact["expected_pf_research_view"], "unsupported")
        transport.assert_not_called()
        verify.assert_not_called()

    def test_workflow_passes_the_declared_mode_default_off(self):
        import yaml
        from pathlib import Path
        root = Path(probe.__file__).resolve().parents[1]
        workflow = yaml.safe_load((root / ".github/workflows/core_repair_readback.yml").read_text())
        step = next(s for s in workflow["jobs"]["readback"]["steps"]
                    if "verify_core_repair_readback.py" in s.get("run", ""))
        self.assertEqual(step["env"]["TFB_PF_RESEARCH_VIEW_EXPECT"],
                         "${{ vars.TFB_PF_RESEARCH_VIEW_EXPECT || 'off' }}")
        self.assertIn('--expect-pf-research-view "$TFB_PF_RESEARCH_VIEW_EXPECT"', step["run"])

    def test_off_result_is_unchanged(self):
        result = self.run_declared("off")
        self.assertNotIn("pf_research_view", result)
        result.pop("checked_at_utc")
        legacy = self.run_probe()
        legacy.pop("checked_at_utc")
        self.assertEqual(result, legacy)
        self.assertEqual(probe.SCRIPT_VERSION, "1.2.1")


if __name__ == "__main__":
    unittest.main()
