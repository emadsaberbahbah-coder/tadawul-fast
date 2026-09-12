# Commit Sheet — portfolio_actions v1.11.1 "P-125: 'Position Qty' QUANTITY ALIAS"
**Date:** 2026-09-12 · **Protocol:** One-Pass (S0→S5 complete) · **Register item:** **P-125** (external adjudication 09-11; verified still open at HEAD before building).

## Identity
| | |
|---|---|
| Destination | `core/analysis/portfolio_actions.py` |
| Base (pinned) | **our own v1.11.0** · SHA256 `24e627a274d036f87376…` — live-fetched, confirmed at HEAD |
| Delivered | v1.11.1 · SHA256 `54a2f12f5f670f1c9cb0bae6284a2726687396feab707f72620feb87e196996a` · re-verify at HEAD after commit |
| Tests | `tests/test_pa_position_qty_alias.py` (Q1–Q4, dual-tree, real module + real `_ob._norm_token`) |

## Root cause
`_position_fields` resolves quantity via normalized tokens against `_QTY_KEYS`, which lacked `positionqty` — yet **"Position Qty" is the engine's own My_Portfolio schema header** (data_engine v5.85.4). Any caller handing this module rows under that header parsed `qty=None` and the holding silently vanished from the action ladder. Latent today (the live GAS payload's keys match), a silent hole at the input boundary tomorrow — harness Q2 makes it visible end-to-end: the base tree renders the holding invisible; the revised tree parses qty=10 and issues a real action.

## What it does
One tuple entry appended. **Provably additive:** the scan takes the first row key (row-iteration order) whose token is in the set, so every row that parses today parses identically; divergence requires a row carrying **two** quantity columns with the position-qty one first — a shape no schema produces, and Q4 documents its behavior anyway. **Ungated** by the alias-addition precedent (`CONTAMINATED_FIELD_ALIASES` class); rollback is `git revert`.

## Audit results (S4)
| Check | Result |
|---|---|
| `py_compile` · AST (function set identical — in-place tuple edit) · smart-quote scan | PASS |
| **Q1** `Quantity`-keyed row: full `build_portfolio_actions` output deep-equal | PASS ×3 |
| **Q2** `Position Qty` row: base `qty=None` (holding invisible) → revised `qty=10`, action issued | PASS ×3 |
| **Q3** the real normalizer maps `'Position Qty' → 'positionqty'` | PASS ×3 |
| **Q4** the only divergence class (two quantity columns, position-first) exhibited and documented; quantity-first identical | PASS ×3 |
| Triple-run digest | `c0b23db9888c3087` identical ×3 |

## Deploy
Commit the full file; re-verify SHA at HEAD. No env change, no visible board change expected.
https://github.com/emadsaberbahbah-coder/tadawul-fast/blob/main/core/analysis/portfolio_actions.py

## Queue after this build
**P-130** (investment_advisor TypeError retry label) is the last adjudicated-open item. Then: the GAS Reformat file, the read-backs, and F-1/F-3/F-4/F-5.
