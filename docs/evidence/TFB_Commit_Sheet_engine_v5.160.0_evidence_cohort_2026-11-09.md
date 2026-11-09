# Commit sheet - core/data_engine_v2.py v5.160.0 [EVIDENCE-COHORT] - 2026-11-09

**Why.** Owner, 9 Nov 2026 11:44 Riyadh: "MODEL EVIDENCE 48.70% INVESTABLE hit rate, matured records. Still our system is less than 49% I need to improve it between 60% -55% at least so please check what is preventing that and let's correct it."

**The tile, reproduced.**
- Source: the Decision Pages v2 tile (`dp2ReadHitRates_`). It reads Performance_Log matured records with Outcome WIN or LOSS, grouped by Entry Investability; all four horizons (1W, 2W, 1M, 3M) are pooled.
- BigQuery `tfb_archive.performance_log`, snapshot 2026-11-09, recorded 28 Jun - 9 Nov:

| Label | Matured WIN/LOSS | Hit rate |
|---|---|---|
| INVESTABLE | 1,268 | 48.7% |
| WATCHLIST | 10,946 | 47.7% |
| BLOCKED | 682 | 27.1% |

**What pulls INVESTABLE down** (same records).

| Slice | n | Hit | Note |
|---|---|---|---|
| Tadawul, forecast confidence >= 0.75 (`SR:HIGH`) | 288 | 28.5% | 37.4% Jun-Aug (n=171), 15.4% Sep-Nov (n=117); 23% of all INVESTABLE records |
| Tadawul, confidence 0.50-0.75 (`SR:MEDIUM`) | 109 | 56.9% | |
| US HIGH / US MEDIUM | 95 / 603 | 58.9% / 54.9% | |
| Overall score 68-70 (just above today's floor) | about 400 | 44.1% | |
| 3M checkpoint | 143 | 37.8% | longer than the 45-session time stop |
| Entry month September | 321 | 32.1% | market drop: WATCHLIST outside Tadawul 28.0% |

- Within Tadawul, INVESTABLE won less often than WATCHLIST in Jul (44.6% vs 54.4%) and Aug (49.0% vs 55.7%).
- Forecast reliability is inversely related to the outcome: reliability >= 80 won 37.5% (n=336). This is consistent with H-28 (no edge).

**The tested correction** (same matured records, split into two halves).

| Rule | All | Jun-Aug | Sep-Nov |
|---|---|---|---|
| Today (INVESTABLE) | 48.7% (1,268) | 54.8% (662) | 41.9% (606) |
| Score floor 70 | 50.7% (869) | 56.8% | 44.1% |
| No `SR:HIGH` | 54.6% (980) | 60.9% | 48.3% |
| **Floor 70 + no `SR:HIGH`** | **58.8% (662)** | **65.4% (338)** | **51.9% (324)** |
| Market reference (WATCHLIST outside Tadawul) | 49.6% | 55.3% | 35.1% |

- The floor plateau is flat from 70 to 74 (58.6-59.2%), and every removed slice is below 50%. Removed records:
  - `SR:HIGH` at a score of 70 or more: 25.1% (n=207);
  - score below 70: 45.9% (n=318);
  - both: 37.0% (n=81).
- By horizon under the rule: 1W 60.3%, 2W 64.2%, 1M 60.8%, 3M 39.0%. The 1W-1M horizons together give 61.6%.
- By month under the rule: Jul 58.1%, Aug 82.5%, Sep 47.2%, Oct 54.7%. The rule is above today's label in every month.
- Today's universe (9 Nov archive) goes from 129 INVESTABLE rows to 65 (46 US).
  - Plan names EFC.US (73.8) and PFS.US (70.7) stay INVESTABLE.
  - MBIN, SPFI, SBLK and ZTO (69.3-69.8) move to WATCHLIST, as do four of the five Market_Leaders Tadawul INVESTABLE names (`SR:HIGH`).

**Change.**
- One more DEMOTE-only tier in `_apply_investability_gate`, after the conservative (v5.81.0) and thin-history tiers.
- `TFB_EVIDENCE_GATE` takes three values:
  - `off`: the default, also any unknown word. Byte-identical; only the mode is read.
  - `observe`: appends the substring-safe warning `evidence_cohort_watch:<cohort>`. The verdict is unchanged.
  - `enforce`: WATCHLIST / WATCH, with block_reason "Evidence gate: <cohort> cohort (market:confidence) is below a 50% hit rate on matured records".
- `TFB_EVIDENCE_DEMOTE_COHORTS`: a comma list of `MARKET:CONFIDENCE`, case and spaces ignored, default `SR:HIGH`. A blank value is an empty list, and the tier then cannot fire. Malformed items are skipped.
- Cohort keys:
  - MARKET is the symbol suffix; a bare four-digit code counts as SR. A symbol without a suffix has no cohort.
  - CONFIDENCE uses the tracker's `_confidence_bucket` thresholds on `forecast_confidence`: >= 0.75 HIGH, >= 0.50 MEDIUM, else LOW. A value above 1 is percent points. A missing value means no cohort.
- Never promotes; no column (schema 115).
- `surface_gate_states()` (/health "engine_gates") gains `evidence_gate`, `evidence_cohorts` and `conservative_floor`, the effective v5.81.0 floor.
- The second lever is the existing `TFB_CONSERVATIVE_GATE_SCORE_FLOOR` (default 68) and needs no code.

**Diff.**
- Helpers added: `_evidence_gate_mode`, `_evidence_demote_cohorts`, `_evidence_market_of`, `_evidence_confidence_bucket`, `_evidence_cohort_of` and `_evidence_cohorts_label`.
- Constants added: `_EVIDENCE_COHORTS_DEFAULT`, `_EVIDENCE_OBSERVE_TAG`, `_EVIDENCE_CONF_LEVELS` and `_EVIDENCE_TADAWUL_BARE`.
- One tier, three health keys, the WHY block and a version-history line. Nothing removed.
- Version 5.159.2 -> 5.160.0.
- Manifest pin `scripts/verify_deployment.py` (pin only).
- `docs/render_env_contract.md` regenerated (+3 engine_gates rows).
- Pins moved in `tests/test_core_repair_tiny_quotes.py` and `tests/test_eodhd_analyst_target.py`.

**Tests.** `tests/test_de_evidence_cohort.py` passes 18 of 18 through the real gate:
- E1 off / unset / junk is byte-identical, and the cohort list alone arms nothing.
- E2 observe tags once, keeps the verdict and stays reliability-scan safe.
- E3 enforce demotes and leaves scores alone.
- E4 covers 11 cohort-key cases.
- E5 covers list parsing.
- E6 demote-only checks:
  - the conservative and BLOCKED reasons are kept;
  - floor 70 moves 69.75 to WATCHLIST.
- E7 checks the health surface.
- E8 checks the version and the manifest pin.
- Engine, manifest and env-contract test set: 582 passed, 8 skipped.
- ci.yml 1.1.72 runs the suite next to the Fix P strict-gate suite in the blocking data engine job.

**Arm (after merge and deploy).**
1. On Render, set `TFB_CONSERVATIVE_GATE_SCORE_FLOOR=70` and `TFB_EVIDENCE_GATE=enforce` (cohort list default `SR:HIGH`), then deploy.
2. Read-back: /health `engine_gates` shows `evidence_gate: enforce`, `evidence_cohorts: SR:HIGH`, `conservative_floor: 70.0`. After the next sync, Market_Leaders `SR:HIGH` rows read "Evidence gate: SR:HIGH ..." and Global_Markets rows scored 68-70 read "Conservative gate: overall .. < 70".
3. The hit-rate tile counts every matured record since June, so it moves only as new records mature. Track the corrected cohort from the arming date.

**Rollback.** Set `TFB_EVIDENCE_GATE=off` and `TFB_CONSERVATIVE_GATE_SCORE_FLOOR=68` (or delete both).

Not a licensed financial adviser. These are label rules measured on past records; the owner decides and executes every trade.
