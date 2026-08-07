"""Runtime safety defaults for real-money decision surfaces.

This module does not calculate prices, rankings, recommendations, or trades.  It
only provides a controlled way to tighten safety switches that already exist in
the portfolio and opportunity engines.

Modes
-----
off
    No mutation and no behavioural change.
shadow
    Resolve and report the settings that would be applied, but do not mutate the
    process environment.
enforce
    Apply defaults only where the operator has not already supplied an explicit
    environment value.  Explicit Render settings always win.

The default mode is ``off``.  Production activation therefore requires the
operator to set ``TFB_DECISION_SAFETY_MODE=enforce`` after review.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import asdict, dataclass
from typing import Mapping, MutableMapping

VERSION = "1.0.0"
MODE_ENV = "TFB_DECISION_SAFETY_MODE"
REPORT_ENV = "TFB_DECISION_SAFETY_REPORT"
VALID_MODES = {"off", "shadow", "enforce"}

# Existing engine controls only.  No new BUY/SELL vocabulary is introduced.
SAFETY_DEFAULTS: dict[str, str] = {
    # A four-hour sync cadence makes a 168-hour decision allowance unsuitable
    # for real-money actions.  Twenty-four hours is intentionally fail-closed;
    # a weekend/holiday may block action, which is safer than acting blind.
    "TFB_PF_MAX_DATA_AGE_HOURS": "24",
    "TFB_OPP_MAX_DATA_AGE_HOURS": "24",
    # Holdings with insufficient identity proof must not emit a confident action.
    "TFB_PF_IDENTITY_GATE": "1",
    # A valuation-only EXIT/TRIM contradicted by a positive reliable engine view
    # is withheld for manual review.  Sell-tier and risk-cap rules still stand.
    "TFB_PF_VF_CONFLICT_GUARD": "1",
    # Missing/thin evidence and missing cost basis cannot produce an executable
    # portfolio action in the safety profile.
    "TFB_PF_BLOCK_THIN_COVERAGE": "1",
    "TFB_PF_BLOCK_MISSING_COST_BASIS": "1",
    # Make the engine forecast visible beside valuation upside.
    "TFB_PF_ENGINE_ROI_DISPLAY": "1",
    "TFB_OPP_ENGINE_ROI_DISPLAY": "1",
    # Raise the candidate evidence floor modestly from two to three secondary
    # fields.  The hard DQ=80 and reliability=70 gates remain authoritative.
    "TFB_OPP_MIN_TRUST_FIELDS": "3",
    # Treat sukuk as fixed income and preserve the existing anchor protection.
    "TFB_PA_SUKUK_ASSET_CLASS": "1",
    "TFB_PA_PROTECT_SUKUK": "1",
    # Keep the already-shipped allocator precedence guard explicitly armed.
    "TFB_PA_PRECEDENCE_GATE": "1",
}


def _mode(raw: object) -> str:
    value = str(raw or "off").strip().lower()
    return value if value in VALID_MODES else "off"


def _truthy(raw: object) -> bool:
    return str(raw or "").strip().lower() in {
        "1", "true", "yes", "y", "on", "t", "enabled", "enable"
    }


@dataclass(frozen=True)
class SafetyPlan:
    version: str
    mode: str
    would_set: dict[str, str]
    explicit_preserved: dict[str, str]
    effective: dict[str, str]
    mutations_applied: dict[str, str]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def build_plan(
    environ: Mapping[str, str] | None = None,
    *,
    mode: str | None = None,
) -> SafetyPlan:
    """Return the deterministic safety plan without mutating anything."""
    env = environ if environ is not None else os.environ
    resolved_mode = _mode(mode if mode is not None else env.get(MODE_ENV, "off"))
    would_set: dict[str, str] = {}
    explicit: dict[str, str] = {}
    effective: dict[str, str] = {}

    for key, default in SAFETY_DEFAULTS.items():
        current = env.get(key)
        if current in (None, ""):
            would_set[key] = default
            effective[key] = default
        else:
            explicit[key] = str(current)
            effective[key] = str(current)

    return SafetyPlan(
        version=VERSION,
        mode=resolved_mode,
        would_set=would_set,
        explicit_preserved=explicit,
        effective=effective,
        mutations_applied={},
    )


def apply_safety_defaults(
    environ: MutableMapping[str, str] | None = None,
    *,
    mode: str | None = None,
) -> SafetyPlan:
    """Apply missing defaults only in enforce mode; never override the operator."""
    env = environ if environ is not None else os.environ
    plan = build_plan(env, mode=mode)
    applied: dict[str, str] = {}

    if plan.mode == "enforce":
        for key, value in plan.would_set.items():
            if env.get(key) in (None, ""):
                env[key] = value
                applied[key] = value

    return SafetyPlan(
        version=plan.version,
        mode=plan.mode,
        would_set=plan.would_set,
        explicit_preserved=plan.explicit_preserved,
        effective=dict(plan.effective),
        mutations_applied=applied,
    )


def bootstrap_from_environment() -> SafetyPlan:
    """Safe startup hook used by ``sitecustomize.py``.

    The function is deliberately exception-safe and silent unless reporting is
    explicitly requested.  Default mode ``off`` means adding this module has no
    production behaviour change.
    """
    try:
        plan = apply_safety_defaults(os.environ)
        if _truthy(os.environ.get(REPORT_ENV)):
            logging.getLogger("tfb.decision_safety").warning(
                "[DECISION-SAFETY %s] %s",
                VERSION,
                json.dumps(plan.to_dict(), sort_keys=True),
            )
        return plan
    except Exception as exc:  # pragma: no cover - startup must never be sunk here
        logging.getLogger("tfb.decision_safety").exception(
            "Decision safety bootstrap failed without changing runtime: %s", exc
        )
        return SafetyPlan(
            version=VERSION,
            mode="off",
            would_set={},
            explicit_preserved={},
            effective={},
            mutations_applied={},
        )


__all__ = [
    "VERSION",
    "MODE_ENV",
    "REPORT_ENV",
    "SAFETY_DEFAULTS",
    "SafetyPlan",
    "build_plan",
    "apply_safety_defaults",
    "bootstrap_from_environment",
]
