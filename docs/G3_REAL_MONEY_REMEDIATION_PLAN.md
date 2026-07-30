# Generation 3 — Real-Money Remediation Plan

Status: **Draft / dual-review required**  
Owner activation: **not authorized**  
Current implementation slice: **Part 1 — runtime decision-safety profile**

## 1. Objective

Turn the current investment platform from a broad screening and monitoring
system into a reproducible decision-support system suitable for real money.
The governing order is:

1. protect existing capital;
2. prove data identity and freshness;
3. make no-action a valid output;
4. separate instrument classes;
5. prove execution feasibility and cost;
6. only then improve ranking and expected return.

No phase may create an automatic order or authorize a trade.  A recommendation
remains advisory and must satisfy the complete recommendation card.

## 2. Current verified state

The July 30 workbook review found real engineering progress but insufficient
execution readiness:

- the four main universes are structurally complete and protected by persistence;
- market-page freshness was approximately 97%–100% after the Render restart;
- `My_Portfolio` was materially older than the universe pages;
- the cockpit correctly withheld all executable tickets when quote freshness
  failed;
- market pages still displayed some `INVEST` rows below reliability 70 and/or
  DQ 80;
- identity contamination remained visible for several global symbols;
- the sukuk holding still carried equity-style valuation fields;
- buy dates were not propagated into the portfolio decision surface, disabling
  the 90-day time-stop rule;
- mixed ROI units remained present;
- trigger contention and workbook formatting created unnecessary operational
  load;
- Render experienced a memory-limit restart.

Conclusion: the platform is useful for monitoring and shadow evidence, but it
must not be the sole authority for a real-money buy or sell.

## 3. Script-level review and ownership map

### Decision logic

- `core/analysis/portfolio_actions.py`
  - existing stale-quote, cost-basis, identity, precedence and model-conflict
    controls;
  - current weak defaults include 168-hour freshness, identity gate OFF,
    valuation/forecast conflict gate OFF and thin-data blocking OFF;
  - owns current-position ADD/HOLD/TRIM/EXIT/BLOCK actions.

- `core/analysis/opportunity_builder.py`
  - Top-10 hard gates, scoring, sizing and no-forced-fill behavior;
  - correctly carries default reliability 70 and DQ 80;
  - current freshness allowance is 168 hours and missing timestamps are not
    treated as proven stale;
  - owns executable candidate tickets, not the broad market-page `Final Action`.

- `core/analysis/top10_selector.py`
  - input collection, ranked pool and selector interaction;
  - must remain subordinate to opportunity-builder hard gates.

- `core/data_engine_v2.py`, `core/scoring.py`, `core/reco_normalize.py`
  - broad-universe score/recommendation production;
  - source of the remaining semantic problem where `INVEST` on a market page is
    not equivalent to a fundable ticket.

### Data movement and publication

- `scripts/run_dashboard_sync.py`
  - symbol batching, persistence, last-good substitution, identity firewalls,
    write-then-trim and page verdicts;
  - protects availability but can preserve old rows, so successful writes alone
    do not prove fresh decisions.

- `scripts/audit_sync_outcome.py`
  - Freshness Verdict v2 is being developed in Draft PR #23;
  - must ultimately consume row-level timestamps and API usage from PostgreSQL.

- `routes/advanced_analysis.py`, `routes/analysis_sheet_rows.py`
  - live API surfaces that bridge engine output to Sheets;
  - future snapshot-ID enforcement belongs here.

- `10_My_Portfolio.gs`, `16_Decision_Top10.gs` and related Apps Script files
  - cockpit rendering and operator controls;
  - must display snapshot age, unknowns and block reasons without inventing data.

### Runtime and infrastructure

- `scripts/start_web.sh`
  - Render web entrypoint;
  - supports one or more Gunicorn workers and max-request recycling;
  - Render settings live in the dashboard, not a repository Blueprint.

- `scripts/worker.py`
  - background-worker scaffolding exists but long-running scans are not yet fully
    moved off the web process.

- `db/migrations/0001_operational_data_store.sql` in Draft PR #23
  - PostgreSQL operational schema, immutable observations and separate current /
    last-good evidence.

## 4. Phased remediation

## Part 1 — Immediate decision-safety profile

Purpose: prevent confident actions when evidence is stale, thin, unidentified,
missing cost basis, or internally contradictory.

Implementation in this branch:

- `core/runtime_decision_safety.py`
- `sitecustomize.py`
- `scripts/print_decision_safety.py`
- `tests/test_runtime_decision_safety.py`
- `.github/workflows/decision_safety.yml`

Modes:

- `off`: no behavior change;
- `shadow`: report what would be tightened, without mutation;
- `enforce`: set missing defaults while preserving every explicit operator value.

Proposed safety defaults:

| Control | Safety value | Reason |
|---|---:|---|
| Portfolio maximum quote age | 24 hours | four-hour sync cadence; older means an outage or stale position surface |
| Opportunity maximum quote age | 24 hours | no new purchase from an old quote |
| Portfolio identity gate | ON | weak identity cannot emit a confident action |
| Valuation/forecast conflict guard | ON | contradictory models require manual review |
| Block thin portfolio data | ON | unknown core evidence is not a buy/sell signal |
| Block missing cost basis | ON | position economics and stop review require a valid basis |
| Engine ROI display | ON | show forecast separately from valuation target |
| Opportunity trust fields | minimum 3 | raise evidence floor modestly |
| Sukuk asset-class treatment | ON | exclude from equity-sector logic |
| Sukuk anchor protection | ON | prevent equity-cap mechanical selling |

Safety boundary:

- no new recommendation vocabulary;
- no trade execution;
- no provider-call changes;
- no Google Sheets write changes;
- explicit Render values override the profile;
- default mode is `off` until dual approval.

Acceptance:

1. contract tests pass;
2. shadow mode mutates nothing;
3. an explicit operator value is never overwritten;
4. the resolved setting report is archived;
5. owner and second reviewer approve before `enforce`.

## Part 2 — One snapshot for portfolio and decisions

Problem: `Portfolio_Decision` can be generated after `My_Portfolio` using older
prices, creating a false sense of recency.

Required changes:

- create a `snapshot_id` and `source_as_of` for every portfolio refresh;
- portfolio action route accepts only rows from one snapshot;
- decision page displays snapshot ID and oldest/newest source ages;
- portfolio requires 100% fresh critical fields;
- stale or mixed snapshots produce `BLOCK — REFRESH REQUIRED`;
- Top-10 and portfolio pages cannot claim a newer generation time than their
  source snapshot.

Files:

- `core/analysis/portfolio_actions.py`
- `routes/advanced_analysis.py`
- Apps Script portfolio renderer
- PostgreSQL `sync_runs`, `page_refresh_runs`, observations and latest state.

Acceptance:

- ten of ten holdings have valid price, identity, source timestamp and snapshot;
- no mixed snapshot can produce ADD/TRIM/EXIT;
- three consecutive scheduled cycles pass.

## Part 3 — One meaning for `INVEST`

Problem: broad market pages can display `INVEST` even when the candidate fails
Top-10 DQ/reliability/freshness rules.

Required contract:

`INVEST` means all of the following are true:

- POSITION_CLASS assigned;
- price and identity are valid;
- freshness policy passes;
- DQ >= 80;
- reliability >= 70;
- engine recommendation is not sell-class;
- no unresolved provider/engine conflict;
- ROI units are normalized;
- instrument-specific model is allowed;
- execution and minimum-ticket checks pass for a proposed transaction.

Otherwise the broad page must display `WATCH`, `HOLD`, `BLOCKED` or
`DO_NOT_INVEST` with the first binding reason.  It may not use `INVEST` as a
synonym for score rank.

Files:

- `core/data_engine_v2.py`
- `core/reco_normalize.py`
- `core/scoring.py`
- `core/sheets/schema_registry.py`
- validation and contract tests.

Acceptance:

- zero `INVEST` rows below DQ 80 or reliability 70;
- zero `INVEST` rows with stale/unknown critical identity;
- selector and market-page labels agree on eligibility.

## Part 4 — Instrument registry and identity quarantine

Problem: suffix heuristics and duplicated ticker rules allow symbol/name
contamination.

Required changes:

- one instrument registry with canonical ID, exchange, currency, asset class,
  issuer and provider symbols;
- all routes resolve through the registry;
- identity failures leave the usable universe and enter quarantine;
- quarantine requires fresh provider proof or an approved manual override;
- unknown identity is explicit, never inferred from a neighboring row.

Acceptance:

- zero published critical identity failures;
- no script owns a separate suffix-only eligibility rule;
- every investment candidate has canonical instrument ID and provider mapping.

## Part 5 — Separate sukuk/income engine

Problem: 5023.SR is classified as sukuk but still receives equity valuation,
synthetic ROI, stop and target fields.

Required income card:

- issuer and security type;
- coupon rate and frequency;
- maturity and call features;
- clean/dirty price basis;
- yield to maturity / yield to call where calculable;
- last actual trade and average transaction size;
- accrued income and expected cash flows;
- credit and liquidity risk;
- hold-to-maturity decision and review date.

Equity P/E, synthetic equity ROI and equity TP ladders are forbidden for income
positions.

Acceptance:

- no sukuk is scored by the equity model;
- missing coupon/maturity/trade facts produce `UNKNOWN — NO RECOMMENDATION`;
- income positions are assessed by yield and cash-flow sustainability.

## Part 6 — Buy-date and 90-day time stop

Required changes:

- propagate `Buy Date` from `_Portfolio_CostBasis` into normalized holdings;
- calculate holding days and loss status;
- a negative position older than 90 days requires a recorded EXIT or extension
  reason and new date;
- silence defaults to exit-review required, not indefinite hold;
- preserve the owner's correction: do not add rules that cut winners faster.

Acceptance:

- 100% of holdings have a valid buy date or an explicit UNKNOWN block;
- every negative >90-day position has a written decision.

## Part 7 — ROI and unit normalization

Required changes:

- canonical internal unit is decimal ratio, serialized percentage is explicit;
- values such as `0.35`, `35` and `85` cannot coexist without metadata;
- out-of-range values are quarantined, not clipped silently;
- forecast source and basis are preserved;
- synthetic forecasts cannot be presented as analyst or market consensus.

Acceptance:

- zero unexplained mixed-scale ROI rows;
- all ranking tests use one canonical unit;
- displayed ROI names its basis: engine forecast, valuation target or execution
  plan.

## Part 8 — Render memory and job isolation

Immediate operator settings to review before the next deploy:

- `WEB_CONCURRENCY=1` unless metrics prove two workers fit memory;
- `TFB_MAX_REQUESTS=250`;
- `TFB_MAX_REQUESTS_JITTER=50`;
- retain `/readyz` health check;
- capture memory before and after each heavy request.

Architecture changes:

- move long scans and publication work to `scripts/worker.py`;
- web process accepts requests and returns job IDs;
- shared provider budget and Redis/PostgreSQL leases;
- no full-universe dataframe retained across requests;
- bounded caches and explicit cache metrics.

Acceptance:

- no memory restart during three full cycles;
- peak memory remains below 75% of instance limit;
- worker failure cannot corrupt or clear a Sheet page.

## Part 9 — Google Sheets becomes a thin decision client

Required changes:

- remove formatting from unused rows/columns;
- publish only portfolio, decision cards, risks, income assets, sync status and
  overrides;
- full universe lives in PostgreSQL, with historical analytics later in BigQuery;
- batch writes and idempotent publication keys;
- Sheets can be reconstructed from a committed snapshot.

Acceptance:

- workbook size and styled-empty-cell count fall materially;
- decision pages remain responsive;
- a publication retry cannot duplicate or mix runs.

## Part 10 — PostgreSQL shadow and Freshness Verdict v2

Complete Draft PR #23 after this safety phase:

- dual-write observations in shadow mode;
- separate current and last-good rows;
- source-effective, known-at and fetched-at timestamps;
- provider usage and weighted API units;
- row-level stale/preserved/stub/identity evidence;
- transaction outbox for Sheet publication;
- Freshness Verdict v2 requires portfolio/candidates 100% and universe >=95%;
- hard enforcement only after three successful cycles and owner approval.

## 5. Activation sequence

1. Merge code with mode `off` only.
2. Deploy and confirm no behavior change.
3. Set `TFB_DECISION_SAFETY_MODE=shadow` and
   `TFB_DECISION_SAFETY_REPORT=1`.
4. Capture three cycles and list every would-be block.
5. Review false positives with the owner and second reviewer.
6. Correct data—not thresholds—where facts are missing.
7. Approve a versioned settings manifest.
8. Set mode to `enforce` during a supervised window.
9. Refresh portfolio and cockpit from one snapshot.
10. Verify zero unexpected ADD/TRIM/EXIT changes.
11. Roll back to `shadow` immediately on unexplained divergence.

## 6. Current real-money operating rule

Until Parts 1–3 are enforced and verified:

- do not execute a purchase solely because a market page says `INVEST`;
- do not execute a portfolio ADD/TRIM/EXIT from a stale or mixed snapshot;
- treat blank identity, timestamps, buy date, coupon or maturity as UNKNOWN;
- evaluate sukuk separately;
- compare every proposed action to doing nothing;
- selling and buying remain separate decisions;
- rank exits by exit-signal strength, never by profit or loss.
