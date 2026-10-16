TFB Commit Sheet — scripts/run_dashboard_sync.py v6.71.0 [BATCH-CONC: bounded concurrent market-page batches]
Date: 2026-10-16 (Friday, evidence day) · Lane: GitHub/Python (daily_sync.yml, both writer lanes) · File status: OLD file, rebuilt · Nothing armed by this delivery · STACKED ON #787 (v6.70.0 FW-SKEW)

S0/S1 — Base pin
Item	Value
Base	`scripts/run_dashboard_sync.py` v6.70.0 at `origin/claude/gm-firewall-skew` c3fc33c (PR #787 head; this branch is stacked on it and must merge after it)
Delivered	sha256 `d58bf5377a5ed5f2…`, 15,297 lines, `SCRIPT_VERSION = "6.71.0"` (banner re-pinned)
Edits	5 anchored edits, count==1 each (banner · version + WHY · 3 helpers after `_batch_delay_ms` · `_fetch_market_rows_batched` docstring note · batch-loop region: shared closures + the sequential loop calling them + the k > 1 concurrent phase)
AST	functions 357 → 365 unique names (+3 top-level `_sync_batch_concurrency`, `_batch_conc_pct`, `_batch_conc_line`; +5 nested in `_fetch_market_rows_batched`: `_bc_payload`, `_bc_fetch`, `_bc_apply`, `_bc_budget_stop`, `_bc_worker`; 0 removed) · all defs 369 → 377 · ruff E9/F63/F7/F82 clean

S2 — Why (GitHub Actions logs, runs 38711216198, 38774823513, 38821487562)
Global_Markets (6,609 symbols) is fetched as a strictly serial chain of 268 `POST /v1/analysis/sheet-rows` calls (25 symbols each, median ~10.0 s per call): 50.9–55.1 min per pass, and the whole-page recovery replay repeats it (48.6–51.4 min). Together ~92% of every daily_sync run (108–116 min).
Every call returned HTTP 200; EODHD peaked at 32% of its daily quota. The wall clock is request latency in series, not throttling or quota.
Two overlapping legs earlier measured ~1.78x aggregate backend throughput with 2 streams, so 2 batches in flight should cut a GM pass to ~26–31 min (and the recovery replay alike).

S3 — What
`TFB_SYNC_BATCH_CONCURRENCY` = k. Unset, blank, unparseable or ≤ 1 → 1; values clamp to 1..4 ("" x 0 1 2 9 → 1 1 1 1 2 4).
k = 1: the v6.70.0 sequential loop runs. Its fetch, per-batch fold and TIME-BUDGET stop line now live in closures (`_bc_fetch`, `_bc_apply`, `_bc_budget_stop`) whose bodies are the old loop body moved without logic change; the loop still does the budget check, payload, candidate choice and `TFB_SYNC_BATCH_DELAY_MS` sleep exactly as before.
k > 1: the loop still runs every batch up to and including the one that resolves the endpoint (normally batch 0; if it fails on every candidate the next batch cycles them, serially, as today). The remaining batches launch in ascending order under `asyncio.Semaphore(k)` on the resolved endpoint. The time budget is checked when a slot frees, right before each launch, never mid-batch; the first refused batch ends the launches like the sequential `break`. Each slot keeps the `TFB_SYNC_BATCH_DELAY_MS` rest between its own batches.
Outcomes (headers, rows, answering endpoint, batch error) are keyed by batch index and folded by the same `_bc_apply` in ascending batch order, so identity-bleed drops, first-occurrence dedupe, positional order (identity off), `failed_batches`, `ok_batches`, `last_err`, the BATCH-IDENTITY line and the TIME-BUDGET line equal k = 1 for the same responses. A batch whose fetch raised is re-raised at its place in that order (launches stop once one raised; later outcomes are discarded, as the sequential loop never fetched them).
The v6.25.0 BATCH-RETRY pass runs after the fold, serial and unchanged.
k > 1 adds one line per batched fetch (res.warnings + log, no braces): `[BATCH-CONC v6.71.0] page=<p> k=<k> batches=<n> launched=<m> wall_s=<first-pass wall> p50_s=<..> p90_s=<..> failed=<first-pass failed batches>`. p50/p90 are nearest-rank over every first-pass request (serial and concurrent).
UNTOUCHED: batch size and composition (critical symbols still isolated), payloads and request ids, endpoint candidates, the retry pass, every guard, floor, persistence and write path, page-level `--workers`.
Workflow: `daily_sync.yml` maps `TFB_SYNC_BATCH_CONCURRENCY: ${{ vars.TFB_SYNC_BATCH_CONCURRENCY || '1' }}` in sync-dashboard AND recover-missing-market-pages (inert); `tests/test_workflow_flag_mappings.py` SYNC_DEFAULTS +1 (parity enforced).

S4 — Audits
Battery	Result
New `tests/test_sync_batch_concurrency.py` (34)	env parse/clamp 11/11 · line format (no braces, nearest-rank) · k unset == k = 1 (outputs, warnings, call sequence, no line) · k = 2/3/4 page == k = 1 with jittered latencies (completion order ≠ launch order asserted), max in flight == k, one line `batches=6 launched=6` · 20-batch page bound at k = 2 and 3 · first-occurrence + bleed follow batch order when the later duplicate answers first · identity-off positional order · failed batch (non-200, non-dict, missing headers) at k = 3: same outputs, same `last_err`, same retry calls `r3,r5`, same `recovered 1/2` · no-retry `last_err` is the last failure in batch order · raised batch propagates the same exception · endpoint resolution stays serial over 8 candidates · budget after 1/2/4 launches: same calls, same `exhausted after ok/6` and `budget ended` lines, `launched=` N · per-slot delay · REAL runner (`_run_one_task`, fake backend + writer, Global_Markets, batch size 2): k unset == k = 1; k = 3 publishes the k = 1 page with and without failed batches (S04/S05 restored from last-good alike); raised batch fails the page alike; budget after 3 launches alike — 34 passed ×3
Equivalence harness (scratch, not shipped)	v6.70.0 `_fetch_market_rows_batched` loaded from the base vs v6.71.0, 400 seeded random scenarios (1–5 symbol batches, duplicates, critical symbols, bleed/dup/blank/short rows, shuffled answers, http/non-dict/no-header failures in pass 1 and retry, raised batches, safe/unsafe gateways, budget cut at a random check, delay 0/1 ms, identity and retry on/off): k unset and k = 1 identical to v6.70.0 including the exact call sequence; k = 2/3/4 identical outputs, exceptions, warnings (minus the one line), call multiset and budget-check count. 47 scenarios raised, 55 hit the budget, 101 ran the retry pass
Mutation	fold in completion order → 3 failed (+ harness fails at seed 0) · `Semaphore(k + 1)` → 8 failed · launch-time budget check removed → 4 failed
Regression	every test naming run_dashboard_sync / daily_sync.yml + manifest pins + the new file: 1,562 passed, 15 skipped (GITHUB_EVENT_NAME unset) / 1,562 passed, 15 skipped (pull_request)
Workflow audit	`audit_repository_workflows.py` errors=0 (2 pre-existing TEMPORARY_OVERRIDE warnings)
Manifest	`verify_deployment.py` pin run_dashboard_sync 6.70.0 → 6.71.0 (pin line only)

S5 — Delivery
`scripts/run_dashboard_sync.py` · `.github/workflows/daily_sync.yml` · `scripts/verify_deployment.py` (pin) · `tests/test_sync_batch_concurrency.py` (NEW) · `tests/test_workflow_flag_mappings.py` · this sheet
Merge note: stacked on #787 (base c3fc33c). Merge #787 first; if anything else bumps run_dashboard_sync in between, this delivery takes the next version on the banner, SCRIPT_VERSION, the `[BATCH-CONC v…]` tag and the pin. The FW-SKEW line tag reads `v{SCRIPT_VERSION}`, so after this merges it prints `[FW-SKEW v6.71.0]`.

S6 — Arming / read-back (owner, GitHub Variables; NOT done by this delivery)
Step 1: set repo Variable `TFB_SYNC_BATCH_CONCURRENCY=2`. Read-back on the next daily_sync, job sync-dashboard, step "🚀 Execute Dashboard Sync": `[BATCH-CONC v6.71.0] page=Global_Markets k=2 batches=268 launched=268 wall_s=… p50_s=… p90_s=… failed=0`, and the GM leg duration (expected ~26–31 min against 50.9–55.1). The recovery job's "🧯 Recover only pages that did not refresh" step prints the same line for its GM replay.
Keep if: wall_s roughly halves, p50_s stays near 10–12 s, failed and the f429/f404/fetch_failed counters stay 0, EODHD quota peak stays well under 100% (same number of calls per day, compressed in time), and the GM PAGE-VERDICT fresh_pct is unchanged. If p50_s roughly doubles (the backend serialises) there is no gain: delete the Variable. Only consider 3 after a clean sitting at 2; aggregate in-flight calls are bounded by page workers × k.
Rollback: delete the Variable (→ 1, the sequential loop).
