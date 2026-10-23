"""A manual recovery must honor the same row-safety controls as its producer.

WHY (2026-10-16): the third run_dashboard_sync writer omitted active persistence
sanity and quota enforcement, and could ignore an operator's quarantine or
retirement filters. The old mapping contract compared only the two jobs within
daily_sync. Check all writer controls here while allowing independent recovery
page selection and time budgets. No workflow, provider or workbook is invoked.
"""
from pathlib import Path

import pytest
import yaml

WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"
LANES = (
    ("daily_sync.yml", "sync-dashboard"),
    ("daily_sync.yml", "recover-missing-market-pages"),
    ("page_refresh_recovery.yml", "recover-page"),
)
CONTROL_PREFIXES = ("TFB_SYNC_", "TFB_WARNINGS_", "TFB_RANK_", "TFB_OPP_FX_")
CONTROL_NAMES = {
    "TFB_XPAGE_PRICE_CHECK", "TFB_PORTFOLIO_REBUILD",
    "TFB_PF_MINOR_UNIT_CCY_GUARD",
}
# These settings describe a lane's work and capacity, rather than which rows
# can safely survive publication. Recovery intentionally selects one page and
# has its own budget; changing those is outside this regression's contract.
LANE_SETTINGS = {
    "TFB_SYNC_PAGE_ORDER", "TFB_SYNC_TIME_BUDGET_SEC",
    "TFB_SYNC_SYMBOL_BATCH_SIZE", "TFB_SYNC_MAX_SYMBOLS_MARKET",
    "TFB_SYNC_MARKET_GATEWAY",
}


def _workflow(name):
    # BaseLoader preserves scalar strings exactly as the Actions runner sees
    # them and does not turn the workflow's `on` key into YAML 1.1 boolean True.
    return yaml.load((WORKFLOWS / name).read_text(), Loader=yaml.BaseLoader)


def _effective_writer_env(name, job_name):
    workflow = _workflow(name)
    job = workflow["jobs"][job_name]
    env = workflow.get("env", {}) | job.get("env", {})
    writer_steps = [step for step in job["steps"]
                    if "scripts/run_dashboard_sync.py" in step.get("run", "")]
    # Inline recovery delegates to the same runner and inherits the job env.
    if job_name != "recover-missing-market-pages":
        assert len(writer_steps) == 1, (name, job_name)
        env |= writer_steps[0].get("env", {})
    return env


def _controls(env):
    return {key: value for key, value in env.items()
            if (key.startswith(CONTROL_PREFIXES) or key in CONTROL_NAMES)
            and key not in LANE_SETTINGS}


@pytest.mark.parametrize("name,job", LANES)
def test_every_sync_writer_inherits_producer_row_safety_and_operator_controls(name, job):
    expected = _controls(_effective_writer_env(*LANES[0]))
    # Keep this test grounded in the previously missing protection rather than
    # passing vacuously if someone removes controls from all three YAML lanes.
    assert expected["TFB_SYNC_PERSIST_SANITY"] == "1"
    assert expected["TFB_SYNC_EODHD_QUOTA"] == "observe"
    assert expected["TFB_SYNC_EODHD_QUOTA_GUARD"] == "enforce"
    assert {
        "TFB_SYNC_QUARANTINE_SYMBOLS", "TFB_SYNC_KLG_CRYPTO_NAME_GATE",
        "TFB_SYNC_RETIRE_TAB", "TFB_OPP_FX_USX_SUBUNIT",
        "TFB_SYNC_WRITE_SENTINEL", "TFB_SYNC_OHLC_PREWRITE",
    } <= expected.keys()
    actual = _controls(_effective_writer_env(name, job))
    assert actual == expected, {
        key: (expected.get(key), actual.get(key))
        for key in sorted(expected.keys() | actual.keys())
        if expected.get(key) != actual.get(key)
    }


def test_recovery_quota_guard_receives_the_same_provider_key_as_the_producer():
    for lane in LANES:
        assert _effective_writer_env(*lane)["EODHD_API_KEY"] == "${{ secrets.EODHD_API_KEY }}"


def test_recovery_keeps_its_own_scope_budget_and_strict_readback():
    workflow = _workflow("page_refresh_recovery.yml")
    env = _effective_writer_env(*LANES[2])
    assert env["TFB_SYNC_PAGE_ORDER"] == "${{ matrix.page }}"
    assert env["TFB_SYNC_TIME_BUDGET_SEC"] == "3600"
    assert env["TFB_AUDIT_REQUIRE_FULL_FETCH"] == "1"
    assert env["TFB_AUDIT_MIN_FRESH_PCT"] == "95"
    assert "TFB_TRACK_SHADOW_COHORTS" not in env
    assert not any(key.startswith("TRACK_") for key in env)
    for job_name in ("regression-tests", "plan", "recovery-verdict"):
        job = workflow["jobs"][job_name]
        assert not _controls(workflow.get("env", {}) | job.get("env", {})), job_name


@pytest.mark.parametrize("flag,default", (
    ("TFB_SYNC_RETIRE_TAB", "off"),
    ("TFB_SYNC_KLG_CRYPTO_NAME_GATE", "off"),
    ("TFB_OPP_FX_USX_SUBUNIT", "0"),
    ("TFB_SYNC_OHLC_PREWRITE", "0"),
    ("TFB_SYNC_WRITE_SENTINEL", "0"),
    ("TFB_SYNC_QUARANTINE_SYMBOLS", ""),
))
def test_recovery_does_not_arm_previously_optional_controls(flag, default):
    expected = "${{ vars." + flag + " || '" + default + "' }}"
    assert _effective_writer_env(*LANES[2])[flag] == expected
