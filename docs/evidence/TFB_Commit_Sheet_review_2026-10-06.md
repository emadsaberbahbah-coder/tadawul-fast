# TFB Commit Sheet — REVIEW DELIVERY 2026-10-06 [outcome lane + top-investment lane + B5 P-201 + B6 LOOPGUARD + register item 7 + hygiene]

Date: 2026-10-06 (Tuesday) · Lanes: GitHub Actions / Render (Python) / repo hygiene · Protocol: One-Pass, lane-by-lane review of the whole repository · Base HEAD `39977ad` (`main`, 2026-10-05 14:30Z)

**Operator complaint this delivery answers:** "I face an issue related to the outcome and the top investment recommendation."

---

## S0 — What was actually wrong (live evidence, not inference)

The two complaints are one failure chain, and its first link is **not in this codebase**.

| # | Finding | Evidence |
|---|---|---|
| 1 | **GitHub's scheduler is delivering every cron in this repository 3-7 h late, and drops slots.** | `daily_sync` `"17 4,12,20"`: the 04:17Z slot fired 09:55-11:36Z on 10-03/04/05 (+5h38 .. +7h19); the 12:17Z slot fired 16:28-19:42Z and **did not fire at all on 10-05**; `track_performance` `"0 14"` fired 17:44-21:20Z on its last 12 runs, never within 3 h of nominal; `intraday_refresh` `*/15 7-11` delivered **one** run per day against 21 nominal ticks. `created_at == run_started_at` on every run, so the delay precedes run creation (scheduler side, not the write lease). |
| 2 | **The 2026-10-05 20:17 sync concluded `failure` because its global-markets leg was cancelled before it ever got a runner.** | Job `111970695957`: `runner_id 0`, `runner_name ""`, **zero steps**, cancelled after 15m01s. Same run: preflight queued 11 min, mutual-funds queued 10.5 min. Every other scheduled run started all three legs within 2-40 s. Runner starvation, **not** the write-lease concurrency - no other workflow held `tadawul-production-write-*` (grid_capacity_repair last ran 08-31, page_refresh_recovery 09-22). |
| 3 | **So the outcome evidence for that Riyadh day was never recorded.** | `daily_sync.yml:1137` gates the tracker step on `success() && matrix.group == 'global-markets'`, and the recovery job carries no tracker step. The backstop clock then ran at **21:20:52Z = 00:20 Riyadh 10-06**, and the record key is the wall-clock Riyadh date (`key = symbol|horizon|YYYYMMDD` from `date_recorded = RiyadhTime.now()`), so it recorded under **20261006**. Riyadh day 10-05 has no `--record` from either lane. |
| 4 | **And the one audit that answers "did the pages refresh?" refused to look.** | `sync_outcome_audit` run `37381563177` died in its Resolve step: `Source Daily Sync run concluded 'failure'` - although core-pages and mutual-funds had both succeeded and uploaded logs, and the recovery job had re-written Global_Markets (6,609 rows, 21:28-22:17Z). `decision_surface_freshness` and `full_refresh_coverage` refused on the same rule. |
| 5 | **Two recommendation-path providers silently drop symbols on every cockpit build after the first.** | Reproduced zero-network on untouched copies of HEAD. `yahoo_chart` SingleFlight awaited its future **while holding** an `asyncio.Lock`: loop 1 `['v','v','v','v']`, loop 2 `['RuntimeError','v','RuntimeError','RuntimeError']`, and the batch path served `['CCC']` of `['CCC','DDD']` **with no exception raised**. `argaam`'s semaphore: loop 1 6/6, loop 2 **1/6** with 5 rows carrying `fetch_failed`. Same class as `yahoo_fundamentals` P-203, fixed 10-05; `top10_selector` calls `asyncio.run()` per build at lines 5391/5448. |
| 6 | **Three audits have been red on EVERY scheduled run since 10-01 for a reason that is not a defect.** | Row floors of 1025 Market_Leaders and 4496 Mutual_Funds against a measured universe of **255** and **2,474** - 4.0x and 1.8x. The real findings in those same reports (Global_Markets name coverage 97.93 %, My_Portfolio 80.00 %) were buried under the permanent red. |
| 7 | **Register item 7 still open: the two armed decision gates were invisible on `/health`.** | `main.py:1953-1967`: neither tuple named `TFB_T10_W52_TIMING` (the two-sided 52W entry-timing gate) nor `TFB_PF_ADD_LOSER_VETO`. A key absent from the tuple is not even reported as `unset`. |
| 8 | **67 of 91 test files are run by no workflow**, and four of the `track_performance` harnesses could not be added to CI even in principle. | The module registered three Prometheus metrics unguarded at import, so the second `importlib` load in one pytest process died at collection with `Duplicated timeseries in CollectorRegistry`. Two of those harnesses also pinned exact strings (`== "6.41.0"`, `"PASS 18/18"`) that go red on every release that adds a case. |

Method: seven parallel review lanes over the repository, every finding then put to three independent adversarial verifiers (reproduces / severity / fix-safety); 48 findings confirmed, 16 refuted and dropped. Each delivered build was then audited by an agent that did not write it; the **B5 audit returned FAIL** and its six findings are fixed below (P1-P6).

---

## S1 — Delivered

| File | Old | New |
|---|---|---|
| `core/providers/argaam_provider.py` | 6.1.0 | **6.2.0** [B6-a LOOPGUARD] |
| `core/providers/yahoo_chart_provider.py` | 8.14.0 | **8.15.0** [B6-b LOOPGUARD] |
| `scripts/track_performance.py` | 6.41.0 | **6.42.0** [P-201 zero-forecast baseline + double-import guard] |
| `main.py` | 8.14.0 | **8.14.1** [register item 7] |
| `scripts/verify_deployment.py` | 1.0.28 | **1.0.29** [manifest re-sync, 14 pins] |
| `core/analysis/top10_selector.py` | banner 4.21.0 | banner **4.31.0** (constant was already 4.31.0) |
| `.github/workflows/track_performance.yml` | - | two slots, shared write lease, env parity, action majors |
| `.github/workflows/sync_outcome_audit.yml` | - | audits a `failure` upstream, reads recovery evidence |
| `.github/workflows/daily_sync.yml` | - | run-level verdict notifier |
| `.github/workflows/decision_surface_freshness.yml`, `full_refresh_coverage.yml` | - | row floors re-based |
| `.github/workflows/ci.yml` | 1.0.5 | **1.0.6** (six orphaned suites join the blocking gate) |
| `scripts/audit_decision_surface_freshness.py`, `audit_full_refresh_coverage.py` | - | matching Python fallbacks |
| NEW `tests/test_argaam_loopguard_b6.py`, `tests/test_ycp_loopguard_b6.py`, `tests/test_tp_zero_baseline_p201.py`, `tests/test_verify_manifest_pins.py` | - | - |
| `tests/test_sync_outcome_audit.py`, `test_main_health_pf_gates_v8140.py`, `test_tp_target_unit_sentry_p158.py`, `test_track_force_coverage_p180.py`, `test_tp_dedup_matured_p189b.py` | - | extended / de-staled |
| NEW `docs/evidence/versions_2026-10-06.json` | - | HEAD-verified, AST-read |
| 23 renames | - | see S4 |

---

## S2 — Change, by lane

### Lane 1 — the outcome lane (findings 2, 3, 4)

**`track_performance.yml`**
- `concurrency.group`: `tfb-performance-clock` -> **`tadawul-production-write-${{ github.ref }}`**. The clock and the in-sync tracker step write the same `Performance_Log` and never saw each other; `daily_sync.yml:1131-1135` records what that already cost once ("overlapping trackers append and rewrite Performance_Log from different in-memory copies (23,799 duplicate cohorts by 08-31)"), and the clock's measured start (21:20:52Z) lands inside the window where a healthy 20:17 leg runs its tracker (~21:20-21:35Z). *Recorded cost:* GitHub keeps one pending run per group, so a clock run queued behind two syncs is dropped - acceptable because the sync leg records the same keys and the tracker is idempotent per Riyadh day.
- `schedule`: `"0 14"` -> **`"47 11"` + `"47 15"`**. Two slots, off the hour, both early enough that the observed drift still lands them inside the intended Riyadh day.
- **Env parity**: `TRACK_HORIZONS: "1W,2W,1M,3M"`, `TFB_TRACK_SHADOW_COHORTS: "1"`, `TFB_PERF_TARGET_UNIT_SENTRY: "observe"` mirrored verbatim from `daily_sync.yml` (1152 / 328 / 1164). Without them this lane recorded a different **cohort shape**: no 1W/2W checkpoints (and "days not sampled cannot be recovered later"), an empty `Entry Selected` stamp that lands in neither the champion nor the alternates bucket, and no unit-sentry block. Both blocks now carry a comment that they must stay identical.
- `checkout@v4 -> v6`, `setup-python@v5 -> v6` (the repo's own `audit_repository_workflows.py` flagged both; warnings 29 -> 27).

**`sync_outcome_audit.yml`** - `failure` is now audited (`case success|failure`); `cancelled`/`skipped`/`timed_out` still refuse, because those upload no evidence. A second best-effort download pulls `page-refresh-<run_id>-*` into `downloaded-sync-artifacts/zz-recovery/`. The prefix is load-bearing: `audit_sync_outcome.py` sorts log paths and keeps the **last** verdict per page, so `zz-recovery/` sorts after `tadawul-sync-logs-` and a recovered page's success replaces the failed leg's verdict. Two tests pin exactly that, including the negative (a recovery that also failed still blocks).

**`daily_sync.yml`** - new run-level `notify-run-verdict` job, `needs: [sync-dashboard, recover-missing-market-pages]`, `if: always() && (failure||cancelled)`. The existing notifier lives **inside** the matrix leg, so it cannot fire for a leg that never started: on 10-05 it reported `skipped` in the two legs that succeeded and did not exist for the cancelled one, and nobody was told. The new job runs on its own runner, emits an `::error::` annotation plus a step-summary table **with no secret configured**, and posts to Slack when the webhook exists.

### Lane 2 — the top-investment lane (findings 5, 7)

**B6 LOOPGUARD**, following the `yahoo_fundamentals` v6.9.0 template; no ENV and no kill switch, because the OFF state is the defect (the v6.9.0 / v4.18.0 precedent).
- `argaam` 6.2.0: loop-keyed `_get_semaphore()` (same cap, previous dropped); `_TTLCache`, `ArgaamProvider._lock` and `_PROVIDER_LOCK` to `threading.Lock` with plain `with` (every section audited await-free); plus a loop-keyed `httpx` pool guard (`_ensure_http_client` / `_new_http_client` / `_retire_http_client`) with a bounded graveyard, since the client was cached for the process. +3 methods, 0 removed.
- `yahoo_chart` 8.15.0: `SingleFlight` on a lazy `threading.Lock` held **only** for dict bookkeeping - `return await fut` moved outside the critical section, which is mandatory, not stylistic (a threading lock held across an await deadlocks the loop); a stored future whose `get_loop()` is not the running loop is treated as **absent**; every future gets `_observe_future` as a done-callback so an exception is always retrieved. `TokenBucket` / `CircuitBreaker` / `AdvancedCache` / `_PROVIDER_LOCK` ported for parity. +3 defs, 0 removed; banner corrected from v8.5.0.

**`main.py` 8.14.1** - `_OB_GATE_ENVS` += `TFB_T10_W52_TIMING`, `TFB_T10_W52_LOW_PCT`, `TFB_T10_W52_HIGH_PCT`, `TFB_T10_SHOCK_PCT`; `_PF_GATE_ENVS` += `TFB_PF_ADD_LOSER_VETO`, `TFB_PF_ADD_LOSER_PCT`, `TFB_PF_ADD_STOP_PROX_PCT`. Seven additive keys inside two existing blocks; the anonymous reduced view is untouched; an unset gate still reports the literal `unset`. **Closes register item 7.**

### Lane 3 — B5 / P-201 zero-forecast baseline

`track_performance` 6.42.0, gate **`TFB_S1_ZERO_BASELINE` = off (DEFAULT, byte-identical) | publish**. The computation is pure and ungated; only publication is gated. Armed, `_S1_Calibration` row 2 gains a `Zero MAE (pp)` column, the Detail cell gains `zero_mae=<x.xx>pp`, and the sentry block gains a per-basis row. This satisfies the consumer that **already shipped**: `run_shadow_scorer` v1.9.2's `parse_zero_mae`. Until it lands the scorer prints `zero_mae=n/a` and criterion 4 reads PENDING under enforce.

Why it matters: on the matured 1W+2W cohort MAE(model) 3.23 pp is **worse** than MAE(zero forecast) 3.07 pp, yet criterion 4 passes because 3.23 sits inside the 10 pp band. The band certifies forecasts with no skill.

**The six audit findings against this build, and their fixes:**
- **P1/P2 (honesty)** - the fixture's own numbers (model 2.50 vs zero 3.00) show the model *winning*, while the comment claimed it lost, and the key assertion read `model < zero or model > zero` - a tautology. The comment is corrected, the direction is now asserted explicitly, and the **missing case is added**: `T6` builds a model-loses cohort (model 5.00 pp vs zero 1.50 pp), drives it through the REAL publisher, and asserts the consumer's own rule on the values read back off the sheet - `in_band: true`, `consumer_beats_zero: false`. That pair is the P-201 defect in one line, and nothing drove it before. The embedded self-test's matching `!=` check is hardened the same way.
- **P3 (off-path purity, real violation)** - with the gate unset but `TFB_PERF_TARGET_UNIT_SENTRY=observe` (the LIVE setting in both lanes), the `_Run_Log` UNIT_SENTRY payload gained `zero_mae_pp` keys, because it serialises `rep["legacy"]` verbatim. New `_zb_strip()` removes them when the gate is off. **Verified differentially**: for all three sentry modes, every recorded write and stdout line is byte-identical to v6.41.0 apart from the version stamp, with no `zero_mae` anywhere.
- **P4** - arming writes the sentry block to `A4:D10`, disarming writes only `A4:D9`, so a stale row 10 is left on the sheet. Cosmetic (nothing reads rows 10-11), but it is a sheet an operator reads, so the rollback instruction now says to clear `A10:D10` by hand.
- **P5** - the version bump broke two harnesses that pinned `== "6.41.0"` and `"PASS 18/18"`. Both are now version-tuple floors and `k == k` assertions; `test_tp_target_unit_sentry_p158.py`'s stale `"PASS 14/14"` is fixed the same way.
- **P6** - the harness's version assertion lived in a function the `__main__` block never called; T6 is wired into both the script path and pytest.

Also in this build: `_metric()` wraps the three Prometheus registrations so a **second import in one process** reuses the registered collector instead of raising. That is what unblocks CI (finding 8).

### Lane 4 — chronically-red audits (finding 6)

Row floors re-based to ~95 % of the measured universe: Market_Leaders `1025 -> 240`, Mutual_Funds `4496 -> 2350`, in both workflows and both Python fallbacks. Global_Markets (6512 vs 6609) and Commodities_FX (453 vs 453) were already correct and are untouched. 255 and 2,474 are the **complete** pages - the 10-05 export counts 255 + 6,609 + 453 + 2,474 = 9,791 rows and the cockpit status reads `ML 255/255`; the universe was re-scoped by P-163b and the floors were never re-based with it. Repo Variables `EXPECTED_MIN_ROWS_*` still override.

> **OPERATOR DECISION REQUESTED.** This is the only change in this delivery that moves a **safety threshold**. The reasoning is that a floor which can never be met is the "gate that cries wolf" this repo's own `ci.yml` header warns about, and it was burying the real findings. Confirm the two numbers, or set the repo Variables to values you prefer; rollback is `1025` / `4496`.

### Lane 5 — hygiene (finding 8 and the rest)

- `verify_deployment.py` 1.0.29: 14 stale pins re-synced (opportunity_builder 1.15.1->1.23.0, portfolio_actions 1.9.0->1.14.0, data_engine_v2 5.133.0->5.151.0, track_performance 6.34.0->6.42.0, run_dashboard_sync 6.46.0->6.64.0, top10_selector 4.29.0->4.31.0, scoring 5.11.1->5.11.2, enriched_quote 4.10.0->4.11.0, compliance_gate 1.0.1->1.1.0, yahoo_chart 8.13.0->8.15.0, run_shadow_board 1.2.0->1.5.0, run_shadow_scorer 1.6.0->1.9.2, run_weekly_brief 1.0.2->1.0.3, send_digest 2.0.0->2.1.0). Evidence standard: **HEAD-verified re-sync** (pin == source constant, read by AST), not a per-module behavioural diff - the weaker standard, recorded rather than implied, per the v1.0.21 convention.
- NEW `tests/test_verify_manifest_pins.py`: reads every pinned constant out of the source by AST and asserts it equals the manifest. Nine previous releases of that file are apologies for forgetting a pin; this ends the cycle mechanically. **Proven to fail**: an injected stale pin produces `track performance (scripts/track_performance.py): manifest 6.34.0 != source 6.42.0`, exit 1.
- `ci.yml` 1.0.6: six orphaned suites join the blocking lean gate (the four `track_performance` harnesses, the sync-outcome audit and the manifest guard).
- 23 renames: the misnamed `tests/test_tp_dedup_matured_p189b.py please provide it` (never collected; passes once renamed); 21 `scripts/Harness <name>·PY` files - uploaded with a space and a U+00B7 instead of the extension dot, so invisible to `compileall`, pytest and grep-by-extension - moved to `scripts/harness_archive/harness_<name>.py` with a README recording that 15 of them hard-code `/home/claude` sandbox paths and are evidence, not a runnable suite; and one `docs/evidence/Tfb commit sheet ... · MD`.

---

## S3 — Audits (measured, this container, zero network)

| Battery | Result |
|---|---|
| CI `lean-unit`, the **new** list, in a venv matching CI's pins exactly (numpy + pytest only) | **266 passed in 2.85 s** |
| CI `compile` job (`compileall main.py core routes scripts` + `tests`) | **exit 0** both |
| `tests/test_argaam_loopguard_b6.py` | **6/6** with `ARGAAM_BASE`, 5/5 + 1 skip without. G1 on the base: `loop1 ok=6/6 calls=6 \| loop2 ok=1/6 calls=1`; delivered `loop2 ok=6/6`; semaphores loop-keyed, cap 3 preserved; pool `first_loop_keeps=True new_loop_rebuilds=True graveyard=1`; `+3/-0` defs, 4 threading-lock sections, **awaits_inside=0** |
| `tests/test_ycp_loopguard_b6.py` | **31/31** with `YCP_BASE`, 22/22 without. G1 on the base: `loop2 ['RuntimeError','v','RuntimeError','RuntimeError']`, `_lock._loop.is_closed()=True`, and `base batch served ['CCC'] of ['CCC','DDD'] with NO exception raised` |
| `tests/test_tp_zero_baseline_p201.py` | **6/6**. T6: `{model_mae 5.0, zero_mae 1.5, band_pp 10.0, in_band true, consumer_beats_zero false}` |
| P-201 off-path differential (base v6.41.0 vs delivered, sentry unset/observe/enforce) | **byte-identical** writes, `_Run_Log` payloads and stdout, version stamp aside; no `zero_mae` on any unarmed surface |
| Four `track_performance` harnesses in ONE pytest process | **8 passed in 0.77 s** (previously impossible - died at collection) |
| `tests/test_main_health_pf_gates_v8140.py` | **23/23** (was 21/22 on HEAD: a pre-existing stale pin of portfolio_actions 1.13.0 / opportunity_builder 1.22.1, now read from the modules) |
| `tests/test_sync_outcome_audit.py` | **9/9** (7 + the 2 new recovery-ordering cases) |
| `tests/test_verify_manifest_pins.py` | **3/3**, and 2/3 with an injected stale pin |
| Repo's own `audit_repository_workflows.py` | errors **0**, warnings 29 -> **27** |
| `audit_decision_surface_freshness.py --selftest` | **6/6** |
| CI `contract` job (lean venv) | 19 passed / 1 failed / 2 skipped - `test_top10_sheet_rows_includes_baseline_special_headers`. **Pre-existing and not from this delivery**: verified in a `git worktree` at `39977ad`, where the same full-file run gives the identical 1 failed / 19 passed, and the test passes in isolation on both trees. It is order- and network-dependent, and this container has no outbound network (proxy 403s on Yahoo). CI has network and this job is green there across 2,491 runs. |

---

## S4 — Operator steps

1. **Review the PR.** All 34 changed paths are in one branch; CI must show `📋 CI verdict: CLEAN`.
2. **Confirm or override the two row floors** (Lane 4). This is the one safety-threshold change.
3. **After merge, read back Render** (`main` auto-deploys): `/health` -> `entry_version 8.14.1`, `startup_warnings []`, and `pf_gates.opportunity_builder.TFB_T10_W52_TIMING` + `pf_gates.portfolio_actions.TFB_PF_ADD_LOSER_VETO` now PRESENT (register item 7 closed). On the next cockpit build (two `asyncio.run` loops in one worker life): **zero** `bound to a different event loop` and `Future exception was never retrieved` lines from `argaam` or `yahoo_chart`.
4. **Arm P-201 when you want it** (not armed by this commit): add `TFB_S1_ZERO_BASELINE: "publish"` to the tracker env in BOTH lanes. Read-back: `_S1_Calibration!K2` populated and the scorer's `zero_mae=` token no longer `n/a`. **Sequencing matters** - flip `TFB_PERF_TARGET_UNIT_SENTRY` to `enforce` BEFORE `TFB_S1_CRITERIA_V2`, or row 2 and its new zero column are compared on the legacy fraction-scale basis.
5. **One-off, no code**: dispatch `provider_target_coverage` once with `bootstrap=true` after a clean sync - its last-good baseline was never bootstrapped, so it has been exiting 2 on every scheduled run (`CH_BASELINE_EMPTY`).
6. **Render build filter** (still open from the 10-03 read-back): add ignored paths `docs/**`, `tests/**` so a docs-only commit stops redeploying the service and wiping the L1 fund cache.

## Known limits / deliberate cuts

- **Finding 1 is not fixed, and cannot be fixed in this repository.** The 3-7 h cron drift and the runner starvation are GitHub-platform behaviour. This delivery makes the consequences **survivable and visible** (two tracker slots, shared write lease, recovery evidence counted, a notifier that fires for a leg that never started) rather than pretending to prevent them. The durable fix is to stop trusting GitHub's scheduler for the time-critical trigger - a Render cron calling `POST /repos/.../dispatches` with a `repository_dispatch` trigger, keeping the cron as fallback. That is an operator-armed change with its own sitting; the scale of the drift is now documented here for that decision.
- The Riyadh-day key is still wall-clock. A `TRACK_DAY_KEY_MODE=slot` option (mirroring the scorer's P-176 day key) is specified but **not built** - it changes which day an outcome belongs to and deserves its own build and boundary note.
- **P-192** (7 crypto wrong-instrument names) stays open and stays a code change. The `crypto_pair_class` observe gate only tags class/exchange/currency and never inspects the name; the narrowest fix is the chart-meta name acceptance in `data_engine_v2`, with the curated map `scripts/tfb_export_audit.py` already carries.
- **P-158 / fc_tuple** arming is unchanged. The 10-03 read-back's hypothesis that enforce alone closes the 35 forecast/ROI pair mismatches was not re-tested here.
- The GAS cockpit (`16_Decision_Top10.gs` v1.11.13) is **unchanged**. Its in-flight detector computes the "GM leg missing" verdict and then discards it because the note is only set when `active` is true - so the cockpit went silent in exactly the 10-05 shape. The fix is a string-only disclosure, but the live source is the Apps Script editor and the repo is a mirror, so it belongs in a GAS build you paste, not in this Python/YAML delivery.
- The 21 archived harnesses are not runnable as-is and were not repaired; they are kept because ten commit sheets cite them.
- No live Google Sheets or Render write was exercised. Everything above is offline evidence.
