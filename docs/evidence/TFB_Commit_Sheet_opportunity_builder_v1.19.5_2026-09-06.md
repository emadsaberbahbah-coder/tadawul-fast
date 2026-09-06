# Commit sheet — opportunity_builder v1.19.5 (build #2, 2026-09-06)

| Item | Value |
|---|---|
| Repo path (exact) | `core/analysis/opportunity_builder.py` |
| This sheet | `docs/evidence/TFB_Commit_Sheet_opportunity_builder_v1.19.5_2026-09-06.md` |
| Base pinned | main `d68be59f` · blob `1b5b4fa7` (v1.19.4, post-commit verified: only the module + its sheet changed vs `a26dfe8c`) |
| New blob (expected after commit) | `be6c4aa8` (v1.19.5) · 5,470 lines (5,428 + 45 − 3 replaced) |
| Change class | Wiring fix — the v1.18.1 rotation criteria (held ≥ 7 d, not within 3 % of TP1) now receive their fields |
| ENV required | **None.** No new gate; the existing knobs govern (`TFB_OPP_ROTATION_MIN_HELD_DAYS=0` disables the age rule, `TFB_OPP_ROTATION_TP1_PROXIMITY_PCT=0` the TP1 rule). |
| Deploy | Push to main ⇒ Render auto-deploy (restart harmless: fundamentals L2 armed). |
| Commit message | `opportunity_builder v1.19.5: carry buy_date/tp1_sar/price_sar into normalized holdings so the v1.18.1 rotation rule fires; meta.rotation_fields read-back` |

## Evidence
Found while proving v1.19.4 end-to-end: `_normalize_portfolio` rebuilt holdings as `{symbol, sector, market, value_sar}` only, so `_holding_rotation_eligible` never saw `buy_date` / `tp1_sar` / `price_sar` and failed open on every holding. Pinned-module harness: under v1.19.4 a holding bought 3 days ago **is** picked for rotation (E1 PASS = defect reproduced); under v1.19.5 it is not (E2).

## What changes (anchored edits, count==1 each)
1. Version 1.19.4 → 1.19.5 + changelog block.
2. `_normalize_portfolio`: carries `buy_date` (text), `tp1_sar`, `price_sar` (floats > 0) when present; absent/blank/unparseable ⇒ omitted (holding normalized byte-identically to v1.19.4).
3. `meta.rotation_fields = {holdings, buy_date, tp1_sar, price_sar}` counts — the read-back.

Functions added 0, removed 0. `_holding_rotation_eligible`, funding layer, gates, sizing untouched.

## Proofs
| Step | Result |
|---|---|
| py_compile | OK |
| AST zero-removal | removed 0, added 0 |
| Smart-quote scan | CLEAN |
| Lean CI (`test_opportunity_builder.py` + `test_top10_selector.py`, real module) | **47 passed** |
| Real-module harness ×3 (14 checks: normalizer carry/omit/byte-identity, age rule fires, TP1 rule fires, e2e pinned v1.19.4 rotates a 3-day holding vs v1.19.5 does not, 30-day holding rotates, knob `MIN_HELD_DAYS=0` restores the v1.19.4 pick, no-buy_date ⇒ kpis identical to v1.19.4, meta read-back) | **14/14 PASS ×3**, identical output |

## Read-back after deploy
- `meta.rotation_fields.buy_date` must equal `meta.rotation_fields.holdings` on the next cockpit run (the ledger carries ISO buy dates for all 7). If it reads 0, the GAS holdings payload does not send `buy_date` — that is a `16_Decision_Top10.gs` finding, not a builder one.
- With the fields flowing: HCI.US (bought 2026-09-03) cannot be a rotation source before 2026-09-10; a holding within 3 % below its TP1 is skipped with reason `within x.x% of TP1`.

## Post-commit verification (Claude)
HEAD re-fetched; blob of `core/analysis/opportunity_builder.py` == `be6c4aa8`; tree diff vs `d68be59f` shows only the module + this sheet.
