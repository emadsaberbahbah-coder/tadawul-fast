# Operational Data Platform Migration Plan v1.0

**Status:** Approved implementation baseline — shadow mode only  
**Decision date:** 2026-07-30  
**Repository baseline:** `main` at `d3d93e32fc0d26935f68542a977d0604b51d2656`  
**Owner mandate:** Real-money portfolio; return, risk, and sustainability; no recommendation activation during this migration  
**Runtime invariant:** `runtime_enabled=false` remains unchanged until all acceptance gates pass and the owner explicitly approves activation.

## 1. Executive decision

The project will not move directly from Google Sheets to BigQuery.

The approved target is:

```text
Market-data providers
        ↓
Dedicated background worker / controlled scheduler
        ↓
PostgreSQL operational source of truth
        ↓
Validation, identity firewall, freshness and cost accounting
        ↓
Google Sheets decision surfaces
        ↓
BigQuery analytical history (Phase 6, after the operational path is stable)
```

### Why this is the strongest current design

1. PostgreSQL is the correct first system of record for transactional state: symbol identity, latest values, job state, retries, last-good records, provider usage, manual overrides, and publication status.
2. BigQuery is retained for append-heavy analytical history, performance measurement, backtesting, cohort analysis, and BI. It is not the first write target for row-by-row operational recovery.
3. Google Sheets remains an operator interface only. It must not be the primary store for provider state, freshness, retry state, or historical truth.
4. The existing code already contains:
   - Redis-compatible worker scaffolding;
   - SQLAlchemy and `asyncpg` dependencies;
   - last-good, persistence, identity, and page-verdict concepts.
   These assets reduce migration risk, but production deployment and database usage are not yet complete.
5. The migration is incremental and reversible. The current workbook remains untouched during shadow-write phases.

## 2. Updated findings from current `main`

The following earlier defects are already repaired and must not be reimplemented:

- CI concurrency is isolated from the production write lease.
- Inline page-level recovery exists.
- Known critical symbol identities are canonicalized or removed through the critical identity policy.
- `PERSISTENCE-HARD-GUARD` and last-good preservation remain necessary safety controls.

The following material gaps remain:

1. **Refresh success is not freshness success.**  
   `scripts/audit_sync_outcome.py` currently parses only `page`, `status`, and `rows_written`. A `partial` result with one written row can pass.
2. **Primary market jobs still amplify concurrency.**  
   The workflow still has parallel `core-pages` and `global-markets` legs, and the core runner can execute several pages inside one process.
3. **The operational database is not implemented.**  
   SQLAlchemy and `asyncpg` are pinned, but there is no confirmed production operational schema or repository layer.
4. **The worker is scaffolded but deployment is unconfirmed.**  
   `Procfile` documents a Redis-backed worker, while Render deployment configuration is not committed in the repository.
5. **Google Sheets is still both universe input and persistence surface.**  
   This makes stale rows, partial reads, and page-level writes part of the data-control path.
6. **The current schedule is every four hours.**  
   The owner-approved future cadence is two market-aware light refreshes per trading day, with heavy enrichment on separate TTL schedules.
7. **Provider usage is not governed centrally by weighted API units.**  
   Per-process limits cannot enforce one portfolio-wide EODHD daily budget.

## 3. Execution order

### Phase 0 — Safety baseline and evidence contract

No provider logic, ranking, recommendation, or sheet layout changes.

Deliverables:

- immutable pre-migration workbook backup;
- secret rotation checklist (the previously exposed EODHD token must be replaced);
- formal freshness vocabulary:
  - `FRESH`
  - `PRESERVED`
  - `STALE`
  - `UNKNOWN`
  - `QUARANTINED`
- enhanced page verdict contract:
  - `requested`
  - `fresh`
  - `preserved`
  - `coverage_pct`
  - `stubs`
  - `identity_failures`
  - `oldest_source_at`
  - `newest_source_at`
  - `provider_failures`
  - `api_units`
- portfolio and decision-candidate pages require 100% critical-field freshness;
- market universes require at least 95% fresh coverage, with all non-fresh rows visibly classified.

**Exit gate:** The audit must fail closed on inadequate freshness even when rows were written.

### Phase 1 — PostgreSQL foundation

Create the operational schema and repository interface for:

- instrument identity and lifecycle;
- provider-symbol mappings;
- sync runs, page runs, and batch runs;
- immutable observations and current latest observations;
- last-good state;
- weighted provider usage;
- identity quarantine;
- sheet publication state;
- immutable recommendation snapshots.

Rules:

- blank is never converted to zero;
- every stored value has provider, source timestamp, fetch timestamp, and quality state;
- critical identity failure blocks publication;
- application writes are idempotent by natural keys or source hashes;
- migrations are additive and reversible.

**Exit gate:** Schema contract tests pass; no runtime consumer is switched.

### Phase 2 — Shadow dual-write

The current fetch path continues to produce the same sheet output.

For every successful batch:

1. write raw/normalized results to PostgreSQL;
2. keep the existing sheet path unchanged;
3. compare database and outgoing-sheet counts/hashes;
4. record differences without altering recommendations.

**Exit gate:** Three consecutive complete cycles with:
- zero unexplained identity differences;
- zero missing portfolio rows;
- database totals reconciling to outgoing page totals;
- no increase in destructive sheet skips.

### Phase 3 — Incremental refresh and shared provider budget

Before an external request, the engine checks PostgreSQL TTL state.

Data cadence:

| Data class | Target cadence |
|---|---|
| Portfolio and decision-candidate prices | market-aware light refresh |
| Full global/Saudi price universe | twice per trading day |
| Fundamentals for portfolio/candidates | after filing/event or configured TTL |
| Fundamentals for general universe | staggered weekly/monthly |
| Full history | initial load once |
| History thereafter | incremental new sessions |
| News | portfolio and qualified candidates |
| Mutual-fund NAV | once after provider publication |

Weighted EODHD budget is stored centrally and shared across all workers.

**Exit gate:** Repeated requests for still-fresh data are prevented and measured.

### Phase 4 — Worker-first orchestration

Move long-running provider work out of the web request path.

Preferred practical deployment:

- Render Postgres for operational storage, because the application already runs on Render;
- Render Key Value / existing Redis-compatible queue for job delivery;
- Render background worker for long-running batches;
- one production lease per market-refresh cycle;
- page and batch checkpoints in PostgreSQL;
- web API used for status and control, not as the long-running execution container.

Start with one active page worker. Test a maximum of two only after load evidence.

**Exit gate:** Web requests do not hold an analysis engine call for the duration of a full page refresh.

### Phase 5 — Source-of-truth switch and Sheets publisher

PostgreSQL becomes the operational source of truth.

Google Sheets receives only:

- `My_Portfolio`
- `Top_Investments`
- `Decision`
- `Risk_Monitor`
- `Income_Assets`
- `Sync_Status`
- explicit manual-override surfaces

Large universe sheets can remain as read-only exports or views during transition, but they are not used as provider state.

Publication rules:

- publish from one consistent database snapshot;
- never clear first;
- stage, validate, then replace;
- preserve operator-owned cells;
- reject publication when identity or freshness gates fail.

**Exit gate:** Sheets can be rebuilt from PostgreSQL without contacting a provider.

### Phase 6 — BigQuery analytical layer

Add only after Phases 0–5 are stable.

Export append-only history for:

- price/fundamental/signal snapshots;
- recommendation cards;
- no-action counterfactuals;
- performance horizons;
- provider-quality history;
- freshness and API-cost history;
- premature-winner-exit and long-loser-retention regret.

BigQuery is not required for the operational cutover.

**Exit gate:** Historical analytics reconcile to immutable PostgreSQL recommendation IDs and source timestamps.

## 4. Market-aware target schedule

The schedule change is applied only after incremental refresh is working.

### Saudi market

- pre-open light refresh: approximately 09:00 Asia/Riyadh;
- intraday light refresh: approximately 12:00 Asia/Riyadh;
- heavy enrichment: after market close or staggered overnight.

### International / US-led global cycle

Use `America/New_York` market-calendar logic, not a fixed Riyadh hour:

- approximately one hour before regular US open;
- approximately two hours after open / three hours before close;
- heavy enrichment outside the trading decision window.

Holiday and daylight-saving behavior must come from market calendars.

## 5. Acceptance gates before real-money use

The system remains non-executable until all are true:

1. `runtime_enabled=false` throughout migration.
2. Current portfolio: 100% required instrument facts and critical-field freshness.
3. Decision candidates: 100% required recommendation-card fields.
4. Market universes: at least 95% fresh coverage.
5. Identity failures: zero for published rows.
6. Unknown critical fields are explicit `UNKNOWN`, never zero.
7. Three consecutive scheduled cycles pass the new freshness audit.
8. Recovery re-fetches only missing/expired data.
9. Provider API units stay within the configured internal budget and reserve.
10. Sheet output is reproducible from a database snapshot.
11. Recommendation outcomes retain no-action and cost-adjusted counterfactuals.
12. Human review and explicit owner approval are recorded before activation.

## 6. Rollback

Each phase has one rollback boundary:

- Phase 1: database unused by runtime — drop/recreate only in non-production.
- Phase 2: disable shadow-write flag; Sheets path remains canonical.
- Phase 3: disable TTL/cache reads; provider path remains available.
- Phase 4: stop worker and restore scheduled runner.
- Phase 5: switch publisher back to sheet-source mode from the last approved release.
- Phase 6: stop analytical export; no operational effect.

No phase may delete last-good workbook data or change investment runtime state.

## 7. Immediate implementation backlog

### Started in this change

- [x] Architecture decision recorded.
- [x] Initial additive PostgreSQL schema drafted.
- [x] Schema contract test drafted.
- [x] Migration is isolated on a draft branch; no production behavior changes.

### Next code changes

- [ ] Extend `PAGE-VERDICT` emission with freshness fields.
- [ ] Upgrade `scripts/audit_sync_outcome.py` to require freshness thresholds.
- [ ] Add unit tests for partial-but-stale failure.
- [ ] Add a database repository interface behind `TFB_OPERATIONAL_STORE_SHADOW=0`.
- [ ] Add shadow-write implementation with idempotency and source hashes.
- [ ] Add weighted EODHD usage ledger and reserve thresholds.
- [ ] Add Render worker deployment configuration only after environment values are confirmed.
- [ ] Replace four-hour cron only after the incremental pipeline passes acceptance tests.

## 8. Non-goals of this migration

- no new BUY/SELL recommendation;
- no scoring-threshold change;
- no activation of speculative ideas;
- no direct trading integration;
- no removal of persistence or identity guards;
- no assumption that a provider response is current merely because HTTP status is 200.
