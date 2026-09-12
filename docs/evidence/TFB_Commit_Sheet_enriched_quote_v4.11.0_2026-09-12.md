# Commit Sheet — enriched_quote v4.11.0 "ROI POINTS OUTPUT SENTRY"
**Date:** 2026-09-12 · **Protocol:** One-Pass (S0→S5 complete; S6 observe cycle pending arming)
**Register item:** **P-101 root fix** — the fraction-scale Expected-ROI *writer* (residual 138 → 167 → 256 across the 09-09 / 09-11 / 09-12 exports while only the cells were being reformatted).

## Identity
| | |
|---|---|
| Destination | `core/enriched_quote.py` |
| Base (pinned) | v4.10.0 · SHA256 `d17d5c024ae3002f34eef4d4cca3bddbf415b73fae6e9077bd87edc708d0de80` · 3,684 lines — **live-fetched from GitHub main at build time** (both project-folder copies are stale: 1,680 / 1,955 lines, non-matching SHAs — live pin was essential) |
| Delivered | v4.11.0 · SHA256 `c30f751b08061df6c50b5301f0aebc2897ca2b73d29bb07990e073886ab40441` · 3,770 lines |
| Tests delivered | `tests/test_eq_roi_unit_sentry_dualtree.py` |

## Root cause, re-proven on today's HEAD (not taken from the register)
The sheet's Expected-ROI columns carry a **percent-points** contract; this module's internal contract is **fraction**:
- Step 4 `_normalize_percent_units` (L1537) actively converts points → fraction for `expected_roi_*` using the fp/cp ground-truth discriminator.
- Step 5 `_derive_missing_fields` (L1373-1385) backfills missing ROI as `fp/cp − 1` — a fraction.
- **No ×100 exists anywhere before `schema_projection`** (verified: the only two `*100` sites in 3,684 lines are the confidence heuristic and the discriminator itself).

So every row served through `normalize_rows` lands on the sheet fraction-scale while engine-path rows land as points — two producers, one column, mixed units. That is P-101, and it explains why the GAS reformat could not stop the growth: the writer was never fixed.

## What it does
New pipeline step **8c** `_roi_points_output_sentry(row)`, gated `TFB_EQ_ROI_UNIT_SENTRY = off | observe | enforce` (**default OFF ⇒ byte-identical**), inserted after 8b and **before** `_normalize_warnings_field` so tags ride the canonical warnings channel.

- **Ground-truth-confirmed only:** a value is rescaled ×100 **only** when `forecast_price_*` / `current_price` prove it fraction-scale (err-as-fraction beats err-as-points — the same discriminator step 4 already trusts). This makes the inverse defect — inflating a genuine 0.55-**points** value ×100 (external review F01 class) — **structurally impossible**, proven by harness H5.
- `observe`: tag `roi_unit_points:eq:<field>:observe`, values untouched. `enforce`: value ×100 + `:enforce` tag. Idempotent (H3).
- **No ground truth ⇒ never scales** (fail-safe); magnitude-suspect values get a countable `roi_unit_ambiguous:eq:<field>:<mode>` tag (H6).
- Scope: `expected_roi_1m/3m/12m` only. `upside_pct` / `percent_change` are a vNEXT decision after their own residual count.

## Edits (anchored, count==1 asserted, zero removals)
E1 version bump + WHY block · E2 sentry function + field table + mode reader (before the v4.5.0 outlier section) · E3 pipeline docstring step 8c · E4 pipeline call after `_check_market_cap_currency_units`.

## Audit results (S4)
| Check | Result |
|---|---|
| `py_compile` | PASS |
| AST zero-removal proof | PASS — removed NONE; added `_eq_roi_sentry_mode`, `_roi_points_output_sentry` |
| Smart-quote / NBSP scan | CLEAN |
| **H1** off/off deep-equal vs pinned base, real `normalize_rows` | **PASS ×3** |
| **H2** observe log-only; tags survive `_normalize_warnings_field` **and** `_strip_stale_warnings` | **PASS ×3** |
| **H3** enforce: 0.34 → 34.0 + tag; idempotent when re-fed | **PASS ×3** |
| **H4** points input round-trips to 34.0 at the boundary | **PASS ×3** |
| **H5** 0.55-points value NOT inflated (F01-inverse guard) | **PASS ×3** |
| **H6** no-ground-truth value never scaled; ambiguous tag countable | **PASS ×3** |
| Triple-run digest | `6983cae040bc9d95` — identical ×3 |

## Architectural finding recorded during S4 (composed pipeline)
Step 4 fractionalizes **even correct points inputs** before step 8c. Consequence when enforced: every ground-truth row exits in points regardless of arrival unit — the uniform contract we want — and H4 proves the round-trip lands exactly on 34.0. Rows with no forecast price cannot be produced fraction-scale by the backfill in the first place (the backfill requires `fp`), so the untouched-ambiguous population is upstream-supplied only, and the tag makes it countable before any vNEXT decision.

## Deploy + arming plan (Emad executes; one ENV per evidence run; sequence AFTER the two v1.11.0 armings or interleave — never two new ENVs in one run)
1. **Commit** the full file to `core/enriched_quote.py` on main; re-verify SHA `c30f751b…` at HEAD. Render auto-deploys; nothing armed (H1 guarantee).
   https://github.com/emadsaberbahbah-coder/tadawul-fast/blob/main/core/enriched_quote.py
2. **Arming run (observe):** Render → Environment → `TFB_EQ_ROI_UNIT_SENTRY=observe`. https://dashboard.render.com
   *Positive read-back:* the next full sync's exports carry `roi_unit_points:eq:*:observe` tags, expected on roughly the current bare-fraction population (GM 196 / CFX 59 / MF 1) plus any points rows carrying ground truth (composed-pipeline note above) — tag presence, zero value changes, is the pass condition.
3. **Enforce (separate run)** after tag review: `TFB_EQ_ROI_UNIT_SENTRY=enforce`.
   *Read-back:* the daily sync rewrites all cells, so the **first enforced full sync** must collapse the bare-fraction residual counter (my per-export P-101 script) from 256 toward ~0 on pages served by this path, excluding tagged-ambiguous rows — and the counter must **stop growing** thereafter. That trend line is P-101 → CLOSED.

**Rollback:** `git revert` (single file) or remove the env var — off-state proven byte-identical.

## Explicitly not in this build (queued)
`upside_pct` / `percent_change` unit audit (vNEXT after residual count) · F-6 horizon contradiction (Build #3 candidate) · P-134 cockpit cash cell (GAS-side, needs Apps Script source) · Copper-row schema quarantine · the F-1/F-3/F-4/F-5 model-design decisions.
