# Render Single-Service Rollout — Provider Recovery and Batch Concurrency

## Purpose

This runbook deploys the provider-symbol recovery, truthful fail-soft behavior,
HTTP 402 health handling, and bounded-concurrency code to the single production
Render service without allowing concurrency to become active before the live
backend proves correctness.

The rollout does not authorize a change to scoring, ranking, recommendations,
portfolio arithmetic, Google Sheet layout, or Apps Script behavior.

## Non-negotiable safety state

Before deployment:

- keep pull request #28 in Draft;
- keep `TFB_SYNC_BATCH_CONCURRENCY=1` or leave it unset;
- keep `TFB_SYNC_SYMBOL_BATCH_SIZE=25` unchanged;
- do not disable identity, coherence, persistence, shrink, or Keep-Last-Good guards;
- do not run a live Sheet-write test to prove deployment readiness;
- do not convert a missing fact into zero, a placeholder, or a synthetic signal;
- perform the deployment outside an active decision window where practical.

`run_dashboard_sync.py` treats concurrency `1` as the exact sequential path.
The no-write benchmark CLI and GitHub workflow also default to `1`. Any value
greater than `1` requires explicit operator input.

## Phase 1 — Deploy code with concurrency disabled

1. Confirm the reviewed Draft commit and record its full SHA.
2. In the Render service environment, explicitly set:

   ```text
   TFB_SYNC_BATCH_CONCURRENCY=1
   ```

3. Deploy that exact commit through the existing Git-connected Render service.
4. Do not change the start command, health-check path, provider keys, Google
   credentials, or production Sheet settings.
5. Confirm both endpoints answer successfully:

   ```text
   /readyz
   /meta
   ```

6. Record from `/meta` at minimum:
   - service version;
   - engine version;
   - environment;
   - route health;
   - deployment timestamp or commit identifier when exposed.

If readiness does not stabilize, roll back the Render deployment before any
refresh or benchmark is attempted.

## Phase 2 — Prove the deployed provider-symbol capabilities

Run the GitHub workflow **Python Refresh Benchmark** against the deployed
production backend. Before loading Google credentials or reading the workbook,
the workflow runs three backend-only probes **sequentially**, each with its own
120-second timeout. This execution shape matches production concurrency `1` and
avoids creating a provider burst inside the deployment gate.

| Requested symbol | Accepted response | Required proof |
|---|---|---|
| `ADNOCDIST.AD` | `ADNOCDIST.AD` or `ADNOCDIST.ADX` | ADNOC Distribution live identity, or an explicitly truthful unavailable row from a real provider |
| `BPI.PS` | `BPI.PS` or `BPI.PSE` | Bank of the Philippine Islands live identity, or an explicitly truthful unavailable row from a real provider |
| `BNY.US` | `BNY.US` or `BNY` | Exact live Bank of New York Mellon identity with a positive price; truthful-unavailable is not accepted |

A placeholder, fallback, synthetic row, mixed partial fact row, blank provider,
provider error marker, missing required column, or transport failure is a failed
capability—not a pass. Unknown is preserved as unknown; it is never treated as
zero.

Required result:

```text
execution_mode=sequential
ready_for_full_benchmark=true
failed_capabilities=[]
no_workbook_reads=true
no_workbook_writes=true
```

If this phase fails, do not enable concurrency and do not bypass the gate by
starting the 1,000-symbol benchmark manually.

## Phase 3 — Establish the sequential 1,000-symbol baseline

After all three capabilities pass, the same workflow proceeds to the no-write
benchmark with the production-safe baseline:

- page: `Market_Leaders`;
- requested symbols: `1,000`;
- batch size: `25`;
- concurrency: `1`;
- outer retries: `1`;
- time budget: `2,100` seconds;
- workbook writes: exactly zero.

The benchmark must report all acceptance metrics and satisfy all of the
following:

- runner status is `success`;
- returned symbols = requested symbols;
- good-fresh symbols = requested symbols;
- data-free rows = `0`;
- missing symbols = `0`;
- unattempted symbols = `0`;
- HTTP 429 = `0`;
- HTTP 5xx = `0`;
- targeted recovery healed = targeted recovery requested;
- planned no-write output preserves the full requested universe;
- no identity or coherence failure;
- no Google Sheet clear or write call is executed.

Elapsed time is recorded at 25- and 35-minute checkpoints, but speed never
converts an incomplete result into a pass.

## Phase 4 — Run a separate concurrency-3 no-write canary

Only after the concurrency-1 baseline passes may an operator explicitly dispatch
another no-write benchmark with:

```text
concurrency = 3
```

The canary must use the same page, ordered universe, batch size, provider
configuration, retry policy, and acceptance rules as the baseline. Compare the
two evidence files directly.

Concurrency `3` is eligible only when it:

- preserves the exact requested universe and order;
- produces the same or better complete-fresh coverage;
- has zero missing and zero unattempted symbols;
- has zero critical identity/coherence failures;
- does not increase HTTP 429, HTTP 5xx, or timeout outcomes;
- does not create cross-batch identity bleed or header mismatch;
- provides a material elapsed-time improvement over concurrency `1`.

If completeness, provider pressure, or identity quality degrades, keep
production at concurrency `1` regardless of speed.

## Phase 5 — Activate bounded production concurrency

Only after Phases 2–4 pass:

1. Set the production sync environment to:

   ```text
   TFB_SYNC_BATCH_CONCURRENCY=3
   ```

2. Keep:

   ```text
   TFB_SYNC_SYMBOL_BATCH_SIZE=25
   ```

3. Run one controlled production sync.
4. Review its evidence before allowing normal cadence to continue:
   - requested, attempted, returned, fresh, data-free, missing, and unattempted counts;
   - targeted recovery requested/healed;
   - p95 batch latency;
   - HTTP 429/5xx/timeout counts;
   - identity, coherence, persistence, and shrink-guard verdicts;
   - final page verdict and rows written;
   - full-row coverage audit result;
   - decision-surface freshness result.

A faster run is not a successful rollout unless it outperforms the sequential
alternative on completeness, integrity, and provider stability.

## Immediate rollback

At the first material regression:

1. Set:

   ```text
   TFB_SYNC_BATCH_CONCURRENCY=1
   ```

2. Restart/redeploy the Render service if required for the environment change.
3. Re-run only readiness and the sequential three-symbol capability gate.
4. Keep all identity and persistence guards enabled.
5. Roll back the Render commit if provider-symbol capability remains broken under
   the sequential path.

Do not use any of the following as a rollback shortcut:

- disabling the critical identity registry;
- disabling persistence hard guard;
- disabling Keep-Last-Good;
- accepting placeholder or fallback rows as fresh data;
- reducing the requested universe to make coverage appear higher;
- writing benchmark output to the live workbook.

## Current deployment status

- pull request #28 remains Draft;
- production concurrency remains `1`;
- the workflow is fail-closed and no-write;
- local and CI tests do not prove that Render is running the Draft commit;
- the live capability gate must pass against `/meta` and the deployed analysis
  route before the full baseline can run;
- no production concurrency increase is authorized by this document alone.
