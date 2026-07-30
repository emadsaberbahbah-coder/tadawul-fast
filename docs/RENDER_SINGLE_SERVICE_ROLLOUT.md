# Render Single-Service Rollout — Python Batch Concurrency

## Purpose

This runbook deploys the provider-symbol fixes and bounded-concurrency code to the single production Render service without allowing concurrency to become active before the live backend proves that it can resolve the critical symbols correctly.

The rollout changes fetch scheduling and provider symbol normalization only. It does not change scoring, recommendation, portfolio sizing, Google Sheet layout, or Apps Script manual refresh behavior.

## Non-negotiable safety state

Before deployment:

- keep pull request #28 in Draft;
- keep `TFB_SYNC_BATCH_CONCURRENCY=1` or leave it unset;
- keep `TFB_SYNC_SYMBOL_BATCH_SIZE=25` unchanged;
- do not disable identity, coherence, persistence, shrink, or Keep-Last-Good guards;
- do not run a live write test to prove the deployment;
- perform the deployment outside an active decision window where practical.

`run_dashboard_sync.py` treats concurrency `1` as the exact sequential path. Concurrency greater than `1` is the activation switch.

## Phase 1 — Deploy code with concurrency disabled

1. Confirm the approved commit and record its full SHA.
2. In the Render service environment, explicitly set:

   ```text
   TFB_SYNC_BATCH_CONCURRENCY=1
   ```

3. Deploy the approved commit through the existing Git-connected Render service.
4. Do not change the start command, health-check path, provider keys, Google credentials, or production Sheet settings.
5. Confirm the deployment completes and that both endpoints answer:

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

If readiness does not stabilize, roll back the Render deployment before any data refresh is attempted.

## Phase 2 — Prove the deployed backend

Run the GitHub workflow **Python Refresh Benchmark** against the production backend.

The workflow must pass the deployment capability gate before it can load Google credentials or start the 1,000-symbol benchmark. The gate performs only three read-only backend probes, in parallel, with a 60-second wall-clock budget:

| Requested symbol | Required proof |
|---|---|
| `ADNOCDIST.AB` | Real provider row for ADNOC Distribution; `.AB` or `.ADX` accepted |
| `BPI.PS` | Real provider row for Bank of the Philippine Islands; `.PS` or `.PSE` accepted |
| `BK.US` | Real Bank of New York Mellon identity; `BK.US` or `BK` accepted |

A placeholder, fallback, synthetic, stub, blank identity, non-positive price, timeout, missing column, or transport failure is a failed capability—not a pass.

Required result:

```text
ready_for_full_benchmark=true
failed_capabilities=[]
no_workbook_reads=true
no_workbook_writes=true
```

If this phase fails, do not enable concurrency and do not run the full benchmark manually to bypass the gate.

## Phase 3 — Run the controlled 1,000-symbol benchmark

After all three capabilities pass, the same workflow may proceed to the no-write benchmark:

- page: `Market_Leaders`;
- requested symbols: `1,000`;
- batch size: `25`;
- concurrency: `3`;
- target elapsed time: no more than `25` minutes;
- workbook writes: exactly zero.

Activation requires all of the following:

- requested universe unchanged;
- zero unattempted symbols;
- zero missing symbols after the persistence/readback evaluation;
- zero critical identity failures, including `BK.US`;
- no material increase in HTTP 429, HTTP 5xx, or timeout outcomes;
- no cross-batch identity bleed accepted;
- no schema/header mismatch accepted;
- manual Apps Script refresh remains unaffected;
- decision/scoring logic remains unchanged.

Fresh coverage below 100%, any critical identity failure, or a persistence hard-guard trip means **do not activate** even when elapsed time is good.

## Phase 4 — Activate bounded concurrency

Only after Phase 3 passes:

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
   - batch count and attempted symbols;
   - returned, good-fresh, data-free, missing, and unattempted counts;
   - targeted recovery requested/healed;
   - p95 batch latency;
   - HTTP 429/5xx counts;
   - identity/coherence/persistence guard verdicts;
   - final page verdict and rows written.

A faster run is not a successful rollout unless it also outperforms the sequential alternative on completeness and safety.

## Immediate rollback

At the first material regression:

1. Set:

   ```text
   TFB_SYNC_BATCH_CONCURRENCY=1
   ```

2. Restart/redeploy the Render service if required for the environment change.
3. Re-run only the readiness and three-symbol capability gate.
4. Keep all identity and persistence guards enabled.
5. Roll back the Render commit if the provider-symbol capability remains broken under the sequential path.

Do not use any of the following as a rollback shortcut:

- disabling the critical identity registry;
- disabling persistence hard guard;
- disabling Keep-Last-Good;
- accepting placeholder or fallback rows as fresh data;
- reducing the requested universe to make coverage appear higher;
- writing the benchmark result to the live workbook.

## Current evidence before deployment

The pre-deployment gate against the currently deployed backend failed closed:

- `ADNOCDIST.AB`: probe time budget exceeded;
- `BPI.PS`: probe time budget exceeded;
- `BK.US`: placeholder/fallback identity, not Bank of New York Mellon;
- all three capabilities failed;
- gate elapsed approximately 60 seconds;
- Google credential configuration was skipped;
- the 1,000-symbol benchmark was skipped;
- no workbook read or write was performed by the capability gate.

This is the expected state until the approved provider-symbol fixes are actually deployed to Render.
