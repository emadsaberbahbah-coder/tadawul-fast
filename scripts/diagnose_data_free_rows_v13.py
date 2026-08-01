#!/usr/bin/env python3
"""Decision-eligibility policy v1.3 for the read-only data-free diagnostic.

This thin entry point leaves prices, scoring, ranking, portfolio arithmetic,
and workbook data untouched. It refines diagnostic interpretation and ensures
the production sync-integrity boundary is armed before the evidence collector
wraps the fetch function:

* ``SAU`` / ``XSAU`` / ``SA`` are accepted venue aliases for a valid numeric
  Tadawul ``.SR`` symbol, preventing a code-vs-display false conflict;
* an EODHD HTTP 404 remains visible in evidence, but it is not a decision block
  when the same row has a verified positive price, a non-blank name, and an
  explicit ``xprovider_verified:`` marker from an alternative provider;
* omitted backend rows remain visible as explicit, decision-blocked stubs in
  exact request order, never as missing symbols.

Missing facts, identity conflicts, HTTP 402, circuit-open states, timeouts and
unverified 404 rows remain blocked. No price, score, rank, forecast or
recommendation is created or changed.
"""
from __future__ import annotations

from typing import Any, Mapping, Sequence

from scripts import diagnose_data_free_rows as base
from scripts.sync_integrity_v13 import ensure_installed

POLICY_VERSION = "1.3.0"
__version__ = POLICY_VERSION

_INSTALLED = False
_ORIGINAL_EXCHANGE_MATCHES = base._exchange_matches
_ORIGINAL_REASON_CODES = base._reason_codes


def _exchange_matches(actual: str, expected: str) -> bool:
    actual_norm = base.sync._guard_norm(actual)
    expected_norm = base.sync._guard_norm(expected)
    if expected_norm == "tadawul" and actual_norm in {
        "sa",
        "sau",
        "xsau",
        "tadawul",
        "saudiexchange",
        "saudiexchangetadawul",
    }:
        return True
    return _ORIGINAL_EXCHANGE_MATCHES(actual, expected)


def _has_verified_alternative(
    row: Sequence[Any] | None,
    columns: Mapping[str, int],
) -> bool:
    if row is None:
        return False
    diagnostic_text = base._diagnostic_text(row, columns)
    if "xprovider_verified:" not in diagnostic_text:
        return False
    name = base._cell(row, columns["name"])
    price = base._cell(row, columns["price"])
    return not base._is_blank(name) and base._positive(price)


def _reason_codes(
    symbol: str,
    row: Sequence[Any] | None,
    columns: Mapping[str, int],
) -> list[str]:
    reasons = list(_ORIGINAL_REASON_CODES(symbol, row, columns))
    if (
        "provider_http_404" in reasons
        and _has_verified_alternative(row, columns)
    ):
        reasons = [
            "provider_http_404_alternate_verified"
            if reason == "provider_http_404"
            else reason
            for reason in reasons
        ]
    return list(dict.fromkeys(reasons))


def install_policy() -> None:
    global _INSTALLED
    if _INSTALLED:
        return
    base._exchange_matches = _exchange_matches
    base._reason_codes = _reason_codes
    base.DIAGNOSTIC_VERSION = POLICY_VERSION
    _INSTALLED = True


# run_dashboard_sync has completed importing by the time this wrapper loads.
# Install synchronously so the evidence collector observes the same completed
# response matrix that the production runner receives.
if not ensure_installed():
    raise RuntimeError("sync integrity v1.3.0 did not arm before diagnostics")
install_policy()


if __name__ == "__main__":
    raise SystemExit(base.main())
