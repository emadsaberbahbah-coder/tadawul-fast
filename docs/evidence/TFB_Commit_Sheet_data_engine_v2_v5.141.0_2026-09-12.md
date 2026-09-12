# Commit Sheet — data_engine_v2 v5.141.0 "HORIZON COHERENCE"
**Date:** 2026-09-12 · **Protocol:** One-Pass (S0→S5 complete; S6 observe cycle pending arming)
**Register item:** **F-6** — the `Horizon Days 365` / `Invest Period Label 3M` / `horizon=month` three-way contradiction on live rows.

## Identity
| | |
|---|---|
| Destination | `core/data_engine_v2.py` |
| Base (pinned) | v5.140.0 · SHA256 `ffc3c1ec86c0cf67f55c…` · 17,486 lines — **live-fetched from GitHub main at build time** |
| Delivered | v5.141.0 · SHA256 `85222353e58511129fab4b37d36cb1bf1d1700a4bb19710fc78b594aa577d74f` · 17,587 lines |
| Companion pin | `core/scoring.py` v5.11.1 (`a0f559db…`, byte-identical to upload) — **read-only evidence, not modified** |
| Tests delivered | `tests/test_de_horizon_coherence_dualtree.py` |

## Root cause, proven end-to-end on live source
1. `scoring.py:2738` `detect_horizon(settings, working)` receives no horizon anywhere → falls to the MONTH default with `hdays=None` → `invest_period_label(MONTH, None)` = **"3M"**, reason text `horizon=month`, `horizon_days_effective=None`.
2. `data_engine_v2` then backfills the blank sheet field at **three sites** (`_apply_symbol_context_defaults` L12847; `_apply_page_row_backfill` L13291 and the fund branch L13351), each pairing `label→"1Y"` with `days→365` as **independent constants**. With the label already stamped "3M", only the 365 half fires → **"3M + 365"** manufactured on every scored row. The constant also creates the inverse (days present, label blank → "1Y" beside days=30).
3. Reproduced on the pinned base before any edit: `_apply_page_row_backfill("Global_Markets", {label:"3M"})` → `("3M", 365)`.

## What it does
The three sites now route through one shared helper `_horizon_coherence_fill(out)`, gated `TFB_HORIZON_COHERENT = off | observe | enforce` (**default OFF ⇒ the exact legacy constant fills, byte-identical**).
- **observe:** legacy values untouched; a countable `horizon_incoherent:<label>!=<days>:observe` warnings tag wherever the post-fill pair disagrees — this is the read-back population.
- **enforce:** the blank side is derived from the present side — days from label via `{1D:1, 1W:6, 1M:30, 3M:90, 1Y:365}` (every value round-trips `scoring.invest_period_label`'s own bucket edges exactly, harness J4), label from days via those same edges; both-blank keeps `1Y/365`; **both-present values are never rewritten** — legacy stamped pairs are tagged for counting and self-correct on their next fresh build.
- **Scope guard:** this is display/metadata coherence only. *Which horizon gets scored* is untouched — that is a model decision (F-1/F-5 family), explicitly out of scope, exactly as ruled in the code review.
- Tags avoid the reliability-scan substrings; `_v573_append_warning` provides idempotent tagging.

## Edits (anchored, count==1 asserted, zero removals)
E1 version bump + WHY block (line-start anchor; the version string also appears once in changelog prose) · E2 helpers `_HZC_LABEL_TO_DAYS` + `_horizon_coherence_mode` + `_hzc_label_for_days` + `_horizon_coherence_fill` before `_apply_page_row_backfill` · E3/E4/E5 the three paired-constant sites replaced by the helper call (the two identical 8-space blocks disambiguated by widened context: the `=F` setdefault prefix and the currency-infer prefix).

## Audit results (S4)
| Check | Result |
|---|---|
| `py_compile` (17,587 lines) | PASS |
| AST zero-removal proof | PASS — removed NONE; added the 3 helpers + table; all three remaining `=365` constants live inside the helper (L13353/13358/13379 within its 13333–13388 span) |
| Smart-quote / NBSP scan | CLEAN |
| **J1** off/off deep-equal vs pinned base on both entry points; defect `3M+365` intact when off | **PASS ×3** |
| **J2** observe log-only; both defect shapes (`3M!=365`, `1Y!=30`), the legacy stamped pair, and the `=F` site all tagged; coherent pair clean | **PASS ×3** |
| **J3** enforce: `3M→90`, `30→1M`, blank→`1Y/365` untagged, both-present preserved + tagged | **PASS ×3** |
| **J4** label↔days round-trip stable for all five labels | **PASS ×3** |
| Triple-run digest | `df6757987eca7085` — identical ×3 |

## Deploy + arming plan (Emad executes; one ENV per evidence run; sequence against the v1.11.0 and v4.11.0 armings — never two new flags in one run)
1. **Commit** the full file to `core/data_engine_v2.py` on main; re-verify SHA `85222353…` at HEAD. Deploy is behavior-identical (J1).
   https://github.com/emadsaberbahbah-coder/tadawul-fast/blob/main/core/data_engine_v2.py
2. **Observe run:** Render → Environment → `TFB_HORIZON_COHERENT=observe`. https://dashboard.render.com
   *Positive read-back:* `horizon_incoherent:3M!=365:observe` tags across essentially every scored equity row in the next full sync (the defect is universal on scored rows today), zero value changes.
3. **Enforce (separate run):** `TFB_HORIZON_COHERENT=enforce`.
   *Read-back:* freshly built rows show `Horizon Days = 90` beside `3M` (or a coherent pair wherever the label differs); the `horizon_incoherent` tag count collapses to the both-present legacy residue and then to ~0 as rows rebuild. That trend closes F-6.

**Rollback:** `git revert` (single file) or remove the env var — off-state proven byte-identical.

## Explicitly not in this build (queued)
Passing a *real* horizon into `detect_horizon` (changes which weights/thresholds score every row — a model decision for the F-1/F-5 discussion, not a coherence patch) · `upside_pct`/`percent_change` unit audit (v4.11.0 vNEXT) · P-134 cockpit cash cell (GAS-side) · Copper-row schema quarantine.
