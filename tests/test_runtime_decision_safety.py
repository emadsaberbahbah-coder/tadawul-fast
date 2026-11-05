from __future__ import annotations

import unittest

from core.runtime_decision_safety import (
    MODE_ENV,
    SAFETY_DEFAULTS,
    apply_safety_defaults,
    build_plan,
)


class RuntimeDecisionSafetyTests(unittest.TestCase):
    def test_default_mode_is_off_and_does_not_mutate(self):
        env: dict[str, str] = {}
        plan = apply_safety_defaults(env)
        self.assertEqual(plan.mode, "off")
        self.assertEqual(env, {})
        self.assertFalse(plan.mutations_applied)

    def test_shadow_reports_without_mutating(self):
        env = {MODE_ENV: "shadow"}
        plan = apply_safety_defaults(env)
        self.assertEqual(plan.mode, "shadow")
        self.assertEqual(env, {MODE_ENV: "shadow"})
        self.assertEqual(plan.would_set, SAFETY_DEFAULTS)
        self.assertFalse(plan.mutations_applied)

    def test_enforce_sets_only_missing_defaults(self):
        env = {MODE_ENV: "enforce"}
        plan = apply_safety_defaults(env)
        self.assertEqual(plan.mode, "enforce")
        self.assertEqual(plan.mutations_applied, SAFETY_DEFAULTS)
        for key, expected in SAFETY_DEFAULTS.items():
            self.assertEqual(env[key], expected)

    def test_explicit_operator_value_always_wins(self):
        env = {
            MODE_ENV: "enforce",
            "TFB_PF_MAX_DATA_AGE_HOURS": "30",
            "TFB_PF_IDENTITY_GATE": "0",
        }
        plan = apply_safety_defaults(env)
        self.assertEqual(env["TFB_PF_MAX_DATA_AGE_HOURS"], "30")
        self.assertEqual(env["TFB_PF_IDENTITY_GATE"], "0")
        self.assertEqual(
            plan.explicit_preserved["TFB_PF_MAX_DATA_AGE_HOURS"], "30"
        )
        self.assertEqual(plan.explicit_preserved["TFB_PF_IDENTITY_GATE"], "0")
        self.assertNotIn("TFB_PF_IDENTITY_GATE", plan.mutations_applied)

    def test_invalid_mode_fails_closed_to_off(self):
        env = {MODE_ENV: "trade-everything"}
        plan = apply_safety_defaults(env)
        self.assertEqual(plan.mode, "off")
        self.assertEqual(env, {MODE_ENV: "trade-everything"})

    def test_contract_contains_only_existing_safety_controls(self):
        expected = {
            "TFB_PF_MAX_DATA_AGE_HOURS",
            "TFB_OPP_MAX_DATA_AGE_HOURS",
            "TFB_PF_IDENTITY_GATE",
            "TFB_PF_VF_CONFLICT_GUARD",
            "TFB_PF_BLOCK_THIN_COVERAGE",
            "TFB_PF_BLOCK_MISSING_COST_BASIS",
            "TFB_PF_ENGINE_ROI_DISPLAY",
            "TFB_OPP_ENGINE_ROI_DISPLAY",
            "TFB_OPP_MIN_TRUST_FIELDS",
            "TFB_PA_SUKUK_ASSET_CLASS",
            "TFB_PA_PROTECT_SUKUK",
            "TFB_PA_PRECEDENCE_GATE",
        }
        self.assertEqual(set(SAFETY_DEFAULTS), expected)
        self.assertTrue(all(value != "" for value in SAFETY_DEFAULTS.values()))

    def test_build_plan_is_pure(self):
        env = {MODE_ENV: "enforce"}
        before = dict(env)
        plan = build_plan(env)
        self.assertEqual(env, before)
        self.assertFalse(plan.mutations_applied)


if __name__ == "__main__":
    unittest.main()
