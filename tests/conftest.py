#!/usr/bin/env python3
"""
tests/conftest.py
================================================================================
Pytest shared configuration and fixtures for Tadawul Fast Bridge.
================================================================================
Emad Bahbah -- Tadawul Fast Bridge

Auto-loaded by pytest before any test in tests/ runs. Single source of truth for:
  - sys.path bootstrap (tests can import core/, routes/, integrations/, etc.)
  - environment variable defaults for the test process
  - shared fixtures used across multiple test files

Why this file exists
--------------------
- FIX: tests previously scattered between repo root and tests/ caused
       inconsistent pytest discovery. Centralizing here ensures pytest finds
       and configures every test the same way regardless of invocation cwd.
- FIX: tests calling FastAPI routes used to 401 because REQUIRE_AUTH defaults
       to true. We force open-mode + auth-off here so route tests don't need
       real tokens.
- FIX: providers used to occasionally try real network I/O during unit tests
       depending on import order. We set DISABLE_NETWORK_AT_IMPORT so any
       provider checking that flag stays offline.

Notes
-----
- Production env vars on Render are NOT affected -- os.environ.setdefault only
  sets a value if it isn't already present.
- Add new fixtures below as the test suite grows. The _StubEngine pattern
  currently inlined inside test_schema_alignment.py belongs here long-term.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# =============================================================================
# Path bootstrap
# =============================================================================
# Ensures `from core import ...`, `from routes import ...`, `from integrations
# import ...` etc. resolve correctly when pytest is invoked from any directory
# (repo root, tests/, CI runner, IDE test runners).
_REPO_ROOT = Path(__file__).resolve().parent.parent
_REPO_ROOT_STR = str(_REPO_ROOT)
if _REPO_ROOT_STR not in sys.path:
    sys.path.insert(0, _REPO_ROOT_STR)

# 2026-10-11: several suites import the shared helper by its bare module name
# (`from decision_evidence_fixtures import ...`). That only resolved when an
# earlier file in the same process had put tests/ on sys.path itself
# (test_ob_quote_evidence.py, test_ob_portfolio_certification.py), so
# test_ob_board_funding.py and test_ob_research_diversify.py failed to collect
# when run alone or in a different order, and one collection error aborts a whole
# CI pytest step.
# Appending tests/ here gives every order the import path the forward order
# already had; the bare name keeps resolving to the same single module object.
_TESTS_DIR_STR = str(Path(__file__).resolve().parent)
if _TESTS_DIR_STR not in sys.path:
    sys.path.append(_TESTS_DIR_STR)


# =============================================================================
# Test environment defaults
# =============================================================================
# Auth / open mode -- route tests should not need real X-APP-TOKEN values.
os.environ.setdefault("TFB_OPEN_MODE", "true")
os.environ.setdefault("OPEN_MODE", "true")
os.environ.setdefault("REQUIRE_AUTH", "false")
os.environ.setdefault("TFB_REQUIRE_AUTH", "false")

# Environment markers
os.environ.setdefault("APP_ENV", "testing")
os.environ.setdefault("TFB_APP_ENV", "testing")

# Logging -- keep test output clean
os.environ.setdefault("LOG_LEVEL", "WARNING")
os.environ.setdefault("LOG_JSON", "false")

# Network safety -- providers that respect this flag will not hit external
# APIs (Yahoo, EODHD, Finnhub, Argaam, Tadawul) during unit tests.
os.environ.setdefault("DISABLE_NETWORK_AT_IMPORT", "true")
os.environ.setdefault("PROVIDERS_OFFLINE_MODE", "true")

# Engine init -- defer heavy bootstrapping during tests
os.environ.setdefault("INIT_ENGINE_ON_BOOT", "false")
os.environ.setdefault("DEFER_ROUTER_MOUNT", "false")

# Performance budgets -- relax for slower CI machines
os.environ.setdefault("TFB_TEST_SCORE_BUDGET_MS", "200")


# =============================================================================
# Shared fixtures (add as needed)
# =============================================================================
@pytest.fixture(autouse=True)
def _restore_policy_environment():
    """Legacy batteries may set policies directly; each test owns its changes.

    WHY (WP-11): whole-suite discovery must not let one test's off/enforce
    settings become another test's defaults. This also restores removed keys.
    Tests that use monkeypatch retain pytest's normal fixture teardown order.
    """
    with patch.dict(os.environ):
        yield


# 2026-11-15 (portfolio_actions 1.18.0 [CUSTODY-TIER], default enforce): the
# suites below were written for the v1.15-v1.17 protective contract ("a row
# without a certified packet is BLOCK") and prove TFB_PF_CUSTODY_TIER=off is
# that contract unchanged. The tier itself is tested in
# tests/test_pf_custody_tier.py. Every test also starts without a custody
# receipt left in the imported module by an earlier test.
_PF_PROTECTIVE_CONTRACT_SUITES = frozenset({
    "test_portfolio_action_reconciliation", "test_portfolio_reconciliation_route",
    "test_pf_research_view_p06", "test_portfolio_actions",
})


@pytest.fixture(autouse=True)
def _pf_custody_tier_scope(request, monkeypatch):
    module = sys.modules.get("core.analysis.portfolio_actions")
    if module is not None and hasattr(module, "_CUSTODY_RECEIPT"):
        monkeypatch.setattr(module, "_CUSTODY_RECEIPT", None)
    name = getattr(getattr(request, "module", None), "__name__", "")
    if name.rsplit(".", 1)[-1] in _PF_PROTECTIVE_CONTRACT_SUITES:
        monkeypatch.setenv("TFB_PF_CUSTODY_TIER", "off")
    yield


# opportunity_builder 1.31.0 [CAP-NO-NEW-MONEY] defaults to enforce. These suites were written before it and use
# invented names whose 12-month forecasts sit above the 34% line (50-60%); their contracts (sizing, research
# diversification, the board-funding route) are pinned with the rule off. tests/test_ob_cap_no_new_money.py tests
# the rule. Several of these suites clear every TFB_OPP_* variable in their own fixtures, so the reader itself is
# pinned rather than the environment.
_OB_PRE_CAP_RULE_SUITES = frozenset({
    "test_board_funding_route", "test_ob_conviction_sizing", "test_ob_research_diversify", "test_ob_risk_sizing",
})


@pytest.fixture(autouse=True)
def _ob_cap_rule_scope(request, monkeypatch):
    name = getattr(getattr(request, "module", None), "__name__", "")
    if name.rsplit(".", 1)[-1] in _OB_PRE_CAP_RULE_SUITES:
        module = sys.modules.get("core.analysis.opportunity_builder")
        if module is None:
            import importlib
            module = importlib.import_module("core.analysis.opportunity_builder")
        monkeypatch.setattr(module, "_env_cap_no_new_money", lambda: "off")
    yield


# Example placeholder -- uncomment and expand when shared fixtures are needed:
#
# import pytest
#
# @pytest.fixture(scope="session")
# def repo_root() -> Path:
#     """Absolute path to the repository root."""
#     return _REPO_ROOT
#
# @pytest.fixture
# def stub_engine():
#     """Lightweight engine stub for route tests that don't need live data."""
#     from tests._stubs import StubEngine  # extract from test_schema_alignment
#     return StubEngine()
