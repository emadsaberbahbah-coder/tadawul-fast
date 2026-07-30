from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "db" / "migrations" / "0001_operational_data_store.sql"
PLAN = ROOT / "docs" / "architecture" / "DATA_PLATFORM_MIGRATION_V1.md"

REQUIRED_TABLES = {
    "instruments",
    "provider_symbol_map",
    "sync_runs",
    "page_refresh_runs",
    "batch_refresh_runs",
    "instrument_observations",
    "latest_instrument_state",
    "identity_quarantine",
    "provider_usage_daily",
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


def test_freshness_and_identity_contract_is_persisted() -> None:
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
    ):
        assert column in sql


def test_recommendations_are_non_executable_in_foundation_migration() -> None:
    sql = _sql().lower()
    assert "executable              boolean not null default false" in sql
    assert "check (executable = false)" in sql


def test_plan_preserves_runtime_kill_switch_and_orders_postgres_before_bigquery() -> None:
    plan = PLAN.read_text(encoding="utf-8")
    assert "`runtime_enabled=false` remains unchanged" in plan
    assert plan.index("Phase 1 — PostgreSQL foundation") < plan.index("Phase 6 — BigQuery analytical layer")
