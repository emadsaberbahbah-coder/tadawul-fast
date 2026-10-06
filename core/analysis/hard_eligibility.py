"""Current-row hard restrictions shared by every exposure-increasing planner.

WATCHLIST is a revisable research opinion and is deliberately outside this
contract. Historical opinions must be supplied separately from the current row.
Raw aliases are inspected before generic header normalization loses duplicates.
"""
from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


def _token(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


@dataclass(frozen=True)
class HardEligibility:
    blocked: bool
    reason_codes: tuple[str, ...]


def resolve_hard_eligibility(
    row: Any, *, include_shadow: bool = False
) -> HardEligibility:
    reasons: set[str] = set()
    if isinstance(row, Mapping):
        for key, value in row.items():
            key_token, value_token = _token(key), _token(value)
            if key_token in {"finalaction"} and value_token in {
                "blocked", "donotinvest"
            }:
                reasons.add("final_action:" + value_token)
            elif key_token in {
                "investability", "investabilitystatus", "investabilitygate",
                "gatestatus"
            } and value_token == "blocked":
                reasons.add("investability:blocked")
            elif (include_shadow and key_token == "shadowinvesteligible"
                  and value is False):
                reasons.add("shadow_invest_eligible:false")
    return HardEligibility(bool(reasons), tuple(sorted(reasons)))
