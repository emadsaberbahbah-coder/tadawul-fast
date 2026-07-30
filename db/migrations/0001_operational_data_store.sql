-- TADAWUL FAST BRIDGE
-- Operational data store foundation v1.1
-- Additive schema only. No runtime consumer is switched by this migration.
-- Target compatibility: PostgreSQL 14+.

BEGIN;

CREATE SCHEMA IF NOT EXISTS tfb;

CREATE TABLE IF NOT EXISTS tfb.instruments (
    instrument_id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    canonical_symbol        text NOT NULL UNIQUE,
    display_symbol          text NOT NULL,
    instrument_type         text NOT NULL,
    market_code             text,
    exchange_code           text,
    currency_code           text,
    country_code            text,
    active                  boolean NOT NULL DEFAULT true,
    identity_status         text NOT NULL DEFAULT 'UNKNOWN'
        CHECK (identity_status IN ('VERIFIED', 'UNKNOWN', 'QUARANTINED', 'INACTIVE')),
    lifecycle_reason        text,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    CHECK (btrim(canonical_symbol) <> ''),
    CHECK (btrim(display_symbol) <> '')
);

CREATE TABLE IF NOT EXISTS tfb.provider_symbol_map (
    provider                text NOT NULL,
    provider_symbol         text NOT NULL,
    instrument_id           bigint NOT NULL REFERENCES tfb.instruments(instrument_id),
    mapping_status          text NOT NULL DEFAULT 'UNKNOWN'
        CHECK (mapping_status IN ('VERIFIED', 'UNKNOWN', 'QUARANTINED', 'INACTIVE')),
    verified_at             timestamptz,
    evidence_uri            text,
    created_at              timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (provider, provider_symbol)
);

CREATE TABLE IF NOT EXISTS tfb.data_class_policies (
    data_class              text PRIMARY KEY,
    ttl_seconds             integer NOT NULL CHECK (ttl_seconds > 0),
    portfolio_ttl_seconds   integer CHECK (portfolio_ttl_seconds IS NULL OR portfolio_ttl_seconds > 0),
    candidate_ttl_seconds   integer CHECK (candidate_ttl_seconds IS NULL OR candidate_ttl_seconds > 0),
    refresh_priority        smallint NOT NULL DEFAULT 100 CHECK (refresh_priority >= 1),
    heavy_enrichment        boolean NOT NULL DEFAULT false,
    policy_json             jsonb NOT NULL DEFAULT '{}'::jsonb,
    effective_from          timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tfb.provider_budget_policies (
    provider                text PRIMARY KEY,
    daily_limit_units       bigint NOT NULL CHECK (daily_limit_units > 0),
    operating_limit_units   bigint NOT NULL CHECK (operating_limit_units > 0),
    reserve_units           bigint NOT NULL DEFAULT 0 CHECK (reserve_units >= 0),
    warning_pct             numeric(5,2) NOT NULL DEFAULT 70
        CHECK (warning_pct > 0 AND warning_pct <= 100),
    restrict_pct            numeric(5,2) NOT NULL DEFAULT 80
        CHECK (restrict_pct > 0 AND restrict_pct <= 100),
    critical_only_pct       numeric(5,2) NOT NULL DEFAULT 90
        CHECK (critical_only_pct > 0 AND critical_only_pct <= 100),
    effective_from          timestamptz NOT NULL DEFAULT now(),
    updated_at              timestamptz NOT NULL DEFAULT now(),
    CHECK (operating_limit_units + reserve_units <= daily_limit_units),
    CHECK (warning_pct < restrict_pct AND restrict_pct < critical_only_pct)
);

CREATE TABLE IF NOT EXISTS tfb.sync_runs (
    sync_run_id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    external_run_id         text,
    trigger_type            text NOT NULL,
    run_mode                text NOT NULL,
    status                  text NOT NULL
        CHECK (status IN ('QUEUED', 'RUNNING', 'PARTIAL', 'SUCCEEDED', 'FAILED', 'CANCELLED')),
    runtime_version         text,
    policy_version          text,
    started_at              timestamptz,
    finished_at             timestamptz,
    config_json             jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_sync_runs_external
    ON tfb.sync_runs(external_run_id)
    WHERE external_run_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS tfb.job_leases (
    lease_key               text PRIMARY KEY,
    holder_id               text NOT NULL,
    sync_run_id             bigint REFERENCES tfb.sync_runs(sync_run_id),
    acquired_at             timestamptz NOT NULL,
    heartbeat_at            timestamptz NOT NULL,
    expires_at              timestamptz NOT NULL,
    metadata_json           jsonb NOT NULL DEFAULT '{}'::jsonb,
    CHECK (btrim(lease_key) <> ''),
    CHECK (btrim(holder_id) <> ''),
    CHECK (expires_at > acquired_at)
);

CREATE TABLE IF NOT EXISTS tfb.page_refresh_runs (
    page_refresh_id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sync_run_id             bigint NOT NULL REFERENCES tfb.sync_runs(sync_run_id) ON DELETE CASCADE,
    page_name               text NOT NULL,
    status                  text NOT NULL
        CHECK (status IN ('QUEUED', 'RUNNING', 'PARTIAL', 'SUCCEEDED', 'FAILED', 'SKIPPED')),
    requested_count         integer NOT NULL DEFAULT 0 CHECK (requested_count >= 0),
    fresh_count             integer NOT NULL DEFAULT 0 CHECK (fresh_count >= 0),
    preserved_count         integer NOT NULL DEFAULT 0 CHECK (preserved_count >= 0),
    stub_count              integer NOT NULL DEFAULT 0 CHECK (stub_count >= 0),
    identity_failure_count  integer NOT NULL DEFAULT 0 CHECK (identity_failure_count >= 0),
    provider_failure_count  integer NOT NULL DEFAULT 0 CHECK (provider_failure_count >= 0),
    rows_written            integer NOT NULL DEFAULT 0 CHECK (rows_written >= 0),
    api_units               bigint NOT NULL DEFAULT 0 CHECK (api_units >= 0),
    coverage_pct            numeric(7,4),
    oldest_source_at        timestamptz,
    newest_source_at        timestamptz,
    started_at              timestamptz,
    finished_at             timestamptz,
    reason                  text,
    details_json            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT now(),
    UNIQUE (sync_run_id, page_name),
    CHECK (
        coverage_pct IS NULL
        OR (coverage_pct >= 0 AND coverage_pct <= 100)
    ),
    CHECK (
        requested_count = 0
        OR fresh_count + preserved_count + stub_count <= requested_count
    )
);

CREATE TABLE IF NOT EXISTS tfb.batch_refresh_runs (
    batch_refresh_id        bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    page_refresh_id         bigint NOT NULL REFERENCES tfb.page_refresh_runs(page_refresh_id) ON DELETE CASCADE,
    batch_number            integer NOT NULL CHECK (batch_number >= 1),
    status                  text NOT NULL
        CHECK (status IN ('QUEUED', 'RUNNING', 'PARTIAL', 'SUCCEEDED', 'FAILED', 'SKIPPED')),
    requested_count         integer NOT NULL DEFAULT 0 CHECK (requested_count >= 0),
    fresh_count             integer NOT NULL DEFAULT 0 CHECK (fresh_count >= 0),
    failed_count            integer NOT NULL DEFAULT 0 CHECK (failed_count >= 0),
    retry_count             integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    api_units               bigint NOT NULL DEFAULT 0 CHECK (api_units >= 0),
    endpoint                text,
    started_at              timestamptz,
    finished_at             timestamptz,
    details_json            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT now(),
    UNIQUE (page_refresh_id, batch_number)
);

CREATE TABLE IF NOT EXISTS tfb.instrument_observations (
    observation_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    instrument_id           bigint NOT NULL REFERENCES tfb.instruments(instrument_id),
    data_class              text NOT NULL,
    provider                text NOT NULL,
    source_as_of            timestamptz,
    fetched_at              timestamptz NOT NULL,
    quality_state           text NOT NULL
        CHECK (quality_state IN ('FRESH', 'PRESERVED', 'STALE', 'UNKNOWN', 'QUARANTINED')),
    identity_valid          boolean NOT NULL DEFAULT false,
    source_hash             text NOT NULL,
    payload_json            jsonb NOT NULL,
    sync_run_id             bigint REFERENCES tfb.sync_runs(sync_run_id),
    created_at              timestamptz NOT NULL DEFAULT now(),
    UNIQUE (instrument_id, data_class, provider, source_hash)
);

CREATE INDEX IF NOT EXISTS ix_observations_lookup
    ON tfb.instrument_observations(instrument_id, data_class, fetched_at DESC);

CREATE TABLE IF NOT EXISTS tfb.latest_instrument_state (
    instrument_id           bigint NOT NULL REFERENCES tfb.instruments(instrument_id),
    data_class              text NOT NULL,
    current_observation_id  bigint NOT NULL REFERENCES tfb.instrument_observations(observation_id),
    last_good_observation_id bigint REFERENCES tfb.instrument_observations(observation_id),
    quality_state           text NOT NULL
        CHECK (quality_state IN ('FRESH', 'PRESERVED', 'STALE', 'UNKNOWN', 'QUARANTINED')),
    source_as_of            timestamptz,
    fetched_at              timestamptz NOT NULL,
    expires_at              timestamptz,
    updated_at              timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (instrument_id, data_class),
    CHECK (
        quality_state NOT IN ('FRESH', 'PRESERVED')
        OR last_good_observation_id IS NOT NULL
    )
);

CREATE TABLE IF NOT EXISTS tfb.identity_quarantine (
    quarantine_id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    instrument_id           bigint REFERENCES tfb.instruments(instrument_id),
    requested_symbol        text NOT NULL,
    returned_symbol         text,
    provider                text,
    failure_reason          text NOT NULL,
    seen_name               text,
    seen_exchange           text,
    seen_currency           text,
    seen_country            text,
    sync_run_id             bigint REFERENCES tfb.sync_runs(sync_run_id),
    detected_at             timestamptz NOT NULL DEFAULT now(),
    resolved_at             timestamptz,
    resolution_note         text,
    CHECK (btrim(requested_symbol) <> '')
);

CREATE INDEX IF NOT EXISTS ix_identity_quarantine_open
    ON tfb.identity_quarantine(detected_at DESC)
    WHERE resolved_at IS NULL;

CREATE TABLE IF NOT EXISTS tfb.provider_usage_daily (
    usage_date              date NOT NULL,
    provider                text NOT NULL,
    endpoint_class          text NOT NULL,
    request_count           bigint NOT NULL DEFAULT 0 CHECK (request_count >= 0),
    api_units               bigint NOT NULL DEFAULT 0 CHECK (api_units >= 0),
    failure_count           bigint NOT NULL DEFAULT 0 CHECK (failure_count >= 0),
    last_updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (usage_date, provider, endpoint_class)
);

CREATE TABLE IF NOT EXISTS tfb.manual_overrides (
    override_id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    instrument_id           bigint REFERENCES tfb.instruments(instrument_id),
    override_scope          text NOT NULL,
    field_name              text NOT NULL,
    value_json              jsonb NOT NULL,
    reason                  text NOT NULL,
    entered_by              text NOT NULL,
    approved_by             text,
    effective_from          timestamptz NOT NULL DEFAULT now(),
    expires_at              timestamptz,
    revoked_at              timestamptz,
    created_at              timestamptz NOT NULL DEFAULT now(),
    CHECK (btrim(override_scope) <> ''),
    CHECK (btrim(field_name) <> ''),
    CHECK (btrim(reason) <> ''),
    CHECK (expires_at IS NULL OR expires_at > effective_from)
);

CREATE INDEX IF NOT EXISTS ix_manual_overrides_active
    ON tfb.manual_overrides(instrument_id, override_scope, field_name)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS tfb.sheet_publish_runs (
    publish_run_id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    sync_run_id             bigint REFERENCES tfb.sync_runs(sync_run_id),
    page_name               text NOT NULL,
    status                  text NOT NULL
        CHECK (status IN ('STAGED', 'VALIDATED', 'PUBLISHED', 'FAILED', 'SKIPPED')),
    database_row_count      integer NOT NULL DEFAULT 0 CHECK (database_row_count >= 0),
    outgoing_row_count      integer NOT NULL DEFAULT 0 CHECK (outgoing_row_count >= 0),
    snapshot_hash           text,
    staged_at               timestamptz,
    published_at            timestamptz,
    reason                  text,
    details_json            jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at              timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tfb.recommendation_snapshots (
    recommendation_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    instrument_id           bigint NOT NULL REFERENCES tfb.instruments(instrument_id),
    recommendation_key      text NOT NULL UNIQUE,
    position_class          text NOT NULL,
    signal_strength         numeric(9,4),
    action_code             text NOT NULL,
    policy_version          text NOT NULL,
    model_version           text NOT NULL,
    market_snapshot_json    jsonb NOT NULL,
    no_action_json          jsonb NOT NULL,
    sell_hold_cash_json     jsonb NOT NULL,
    sell_redeploy_json      jsonb NOT NULL,
    full_cost_json          jsonb NOT NULL,
    invalidation_json       jsonb NOT NULL,
    unknown_fields_json     jsonb NOT NULL DEFAULT '[]'::jsonb,
    execution_authorized    boolean NOT NULL DEFAULT false,
    owner_approval_ref      text,
    created_at              timestamptz NOT NULL DEFAULT now(),
    CHECK (
        execution_authorized = false
        OR (owner_approval_ref IS NOT NULL AND btrim(owner_approval_ref) <> '')
    )
);

COMMIT;
