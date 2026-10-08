"""Deployment verification must not approve stale code or failed funding guards."""
import copy
import json
import unittest

from scripts import verify_core_repair_readback as probe


class ReadbackProtocolTests(unittest.TestCase):
    def setUp(self):
        self.calls = []
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
        self.protective = {"version": "1.15.0", "status": "ok", "sector_summary": [],
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
                     "versions": {"portfolio_actions": "1.15.0"}, "route": {"portfolio_actions_version": "1.15.0"}}}
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
        return probe.verify(self.request, "commit-test", "engine-test", "builder-test", expected_portfolio_actions="1.15.0")

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
        self.assertEqual(result["portfolio_actions_version"], "1.15.0")

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
        result = probe.verify(self.request, "commit-test", "engine-test", "builder-test", 1, "1.15.0")
        self.assertTrue(result["ok"])
        self.assertTrue(probe.verify(self.request, "commit-test", "engine-test", "builder-test")["ok"])

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

    def test_authenticated_redirect_is_refused(self):
        with self.assertRaisesRegex(probe.ReadbackError, "redirect refused"):
            probe.NoRedirect().redirect_request(None, None, 302, "", {}, "https://elsewhere.example")


if __name__ == "__main__":
    unittest.main()
