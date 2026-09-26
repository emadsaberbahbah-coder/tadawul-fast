# Commit Sheet — identity_guard v1.3.0 "SIGNATURE 0: COLUMN-SHIFT QUARANTINE"
**Date:** 2026-09-12 · **Protocol:** One-Pass (S0→S5 complete) · **Register item:** the Copper-class schema corruption (external review, adjudicated 09-11; also the named gap in Claude's own six-gate audit, which single-column checks passed).

## Identity
| | |
|---|---|
| Destination | `core/analysis/identity_guard.py` |
| Base (pinned) | v1.2.0 · SHA256 `ff6ae3e4e864f0c5c7e1…` — live-fetched, byte-identical to the upload; companion `symbol_dedup.py` also verified identical to live |
| Delivered | v1.3.0 · SHA256 `03a37440d6ab5b1710a9dcc24ad6e089ac20caf1ad21cead5c52a02746f006da` · re-verify at HEAD after commit |
| Tests | `tests/test_idg_schema_shift.py` (M1–M5; runs the repo's own 22-check standalone suite plus real-export cases) |

## Why signatures 1–4 could never catch it (proven on source)
The live row — `Symbol="Copper Futures" · Name="Commodity" · Asset Class="Futures" · Exchange="USD" · Currency="Global" · Country="Commodities"` — has **no venue suffix**, so `expected_currency_for()` → None, `currency_is_consistent()` → None, price cell holds text → price None, and no quote-failed marker is present. Every existing check answers "cannot verify," and the corpse survives run after run as a preserved row. The correct test is **cross-field**.

## What it does
New **signature 0**, first in the per-row loop, gate `TFB_IDG_SCHEMA_SHIFT = off | observe | enforce` (**default OFF ⇒ byte-identical**):
- `schema_shift_signals(row)` — four independent, pure signals: whitespace inside the symbol cell · currency cell not a 3-letter code · exchange cell holding a **known currency token** · country cell holding an **asset-class token**.
- `schema_shift_suspect()` — conservative conjunction: fires only on (symbol-whitespace AND ≥1 more) OR ≥3 signals. A legitimate FX row carrying `Currency="Global"` is one signal and can never be condemned — proven on the full real page (M5).
- **observe:** one case-tolerant warnings tag naming every signal; values, findings, and plan counts untouched.
- **enforce:** the row joins the **existing** `QUARANTINE_FIELDS` machinery unchanged — field clearing still governed by `TFB_IDENTITY_QUARANTINE_KEYS` exactly like signatures 1–4, BLOCKED status + block reason + refetch queue + the mass-destruction guard all inherited. **Repair is deliberately not attempted** — shifted values need a trustworthy re-fetch, not a guess (the external review's own conclusion).

## Audit results (S4)
| Check | Result |
|---|---|
| `py_compile` · AST zero-removal (4 helpers added) · smart-quote scan | PASS |
| **M1** the repo's own 22-check standalone suite | **ALL PASSED ×3 on v1.3.0** (and on the v1.2.0 base tree) |
| **M2** gate off: signals detected by the pure fn; guard output byte-untouched on the full real page | PASS ×3 |
| **M3** observe on the **real exported Copper row**: tag carries all four signals; values + plan untouched | PASS ×3 |
| **M4** enforce: exactly one SCHEMA_SHIFT finding → fields cleared, `Investability Status=BLOCKED`, symbol refetch-queued | PASS ×3 |
| **M5** false-positive battery on **real data**: CFX 453 rows → exactly the Copper row; Market_Leaders 255 rows → none | PASS ×3 |
| Triple-run digest | `6d45e989a23e3962` identical ×3 |

## Deploy + arming (one ENV per evidence run; Render env, engine lane)
1. Commit the full file; re-verify SHA at HEAD. Deploy is behavior-identical.
   https://github.com/emadsaberbahbah-coder/tadawul-fast/blob/main/core/analysis/identity_guard.py
2. Observe run: `TFB_IDG_SCHEMA_SHIFT=observe` — read-back = the schema_shift tag on the Copper row in the next CFX export, nothing else changed.
3. Enforce on a separate run — read-back = the Copper row reduced to a self-explaining BLOCKED stub (Symbol/Warnings/Block Reason kept) and listed in the guard's refetch queue. Full field-clearing on the snake_case path additionally needs `TFB_IDENTITY_QUARANTINE_KEYS` armed, exactly as for signatures 1–4 — unchanged doctrine.

## Still open on this thread
P-134 (cockpit stale cash cell) remains the one item needing GAS source — the repo's `apps_script/` folder holds only `11_Manual_Refresh_Coordinator.gs`; **paste `00_Config.gs` + `16_Decision_Top10.gs`** from the Sheet's script editor when convenient and that becomes the next build.
