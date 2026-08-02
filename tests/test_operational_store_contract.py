from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "db" / "migrations" / "0001_operational_data_store.sql"
PLAN = ROOT / "docs" / "architecture" / "DATA_PLATFORM_MIGRATION_V1.md"

REQUIRED_TABLES = {
    "instruments",
    "provider_symbol_map",
    "data_class_policies",
    "provider_budget_policies",
    "sync_runs",
    "job_leases",
    "page_refresh_runs",
    "batch_refresh_runs",
    "instrument_observations",
    "latest_instrument_state",
    "identity_quarantine",
    "provider_usage_daily",
    "manual_overrides",
    "sheet_publish_runs",
    "recommendation_snapshots",
}


def _sql() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def test_operational_schema_contains_required_tables() -> None:
    sql = _sql().lower()
    found = set(re.findall(r"create\s+table\s+if\s+not\s+exists\s+tfb\.([a-z0-9_]+)", sql))
    assert REQUIRED_TABLES <= found


def test_schema_is_additive_and_transactional() -> None:
    sql = _sql().lower()
    assert "begin;" in sql
    assert "commit;" in sql
    assert "drop table" not in sql
    assert "truncate table" not in sql
    assert "delete from" not in sql


def test_freshness_identity_and_api_contract_is_persisted() -> None:
    sql = _sql()
    for token in ("FRESH", "PRESERVED", "STALE", "UNKNOWN", "QUARANTINED"):
        assert token in sql
    for column in (
        "requested_count",
        "fresh_count",
        "preserved_count",
        "stub_count",
        "identity_failure_count",
        "coverage_pct",
        "api_units",
    ):
        assert column in sql


def test_current_and_last_good_state_are_separate() -> None:
    sql = _sql().lower()
    assert "current_observation_id" in sql
    assert "last_good_observation_id" in sql


def test_provider_budget_has_operating_limit_and_reserve() -> None:
    sql = _sql().lower()
    assert "daily_limit_units" in sql
    assert "operating_limit_units" in sql
    assert "reserve_units" in sql
    assert "operating_limit_units + reserve_units <= daily_limit_units" in sql


def test_recommendations_default_to_not_authorized_and_require_owner_reference() -> None:
    sql = _sql().lower()
    assert "execution_authorized    boolean not null default false" in sql
    assert "owner_approval_ref" in sql
    assert "execution_authorized = false" in sql


def test_plan_preserves_runtime_kill_switch_and_orders_postgres_before_bigquery() -> None:
    plan = PLAN.read_text(encoding="utf-8")
    assert "`runtime_enabled=false` remains unchanged" in plan
    assert plan.index("Phase 1 — PostgreSQL foundation") < plan.index("Phase 6 — BigQuery analytical layer")
