# Commit Sheet — run_shadow_board v1.3.1 "D-2 ATOMIC BOARD WRITE"
**Date:** 2026-09-12 · **Protocol:** One-Pass (S0→S5 complete) · **Register item:** D-2 from the slot-#4 root-cause (non-atomic `write_board` — a reader in the clear()/update() gap sees an empty tab; the S-1 scorer is exactly such a reader and was observed racing the board on 2026-09-11).

## Identity
| | |
|---|---|
| Destination | `scripts/run_shadow_board.py` |
| Base (pinned) | v1.3.0 · SHA256 `5ac822d0ea8f1b9fb6ea…` — live-fetched at build time, byte-identical to the morning pin |
| Delivered | v1.3.1 · SHA256 `4502e9794b5d68e78b755dd8eccc988e45605e4bb1c11434b65ad9ba0861ee54` · re-verify at HEAD after commit |
| Tests | `tests/test_sb_atomic_write.py` (L1–L4, real board + the **landed** scorer v1.8.0) |
| HEAD verification performed this session | portfolio_actions v1.11.0, enriched_quote v4.11.0, data_engine_v2 v5.141.0, run_shadow_scorer v1.8.0, shadow_scorer.yml — **all five confirmed landed byte-identical at main** |

## What it does
`write_board()` now pads the body to a fixed **17-wide × ≥60-row** blank rectangle and writes it in **one** `update()` with **no `clear()`** — residue from any previous, longer body is overwritten by the pad, and a concurrent reader sees either the old full board or the new full board, never an empty one. End-state cell content is identical to v1.3.0 (trailing blanks instead of cleared cells; every consumer filters on the symbol cell).

**Default ON, deliberately — a stated deviation from the default-OFF doctrine, with the veto in your hands:** the OFF state *is* the defect (a race window), end-state content is unchanged, and this file's own convention ships mechanics fixes ON with a kill switch (`TFB_SB_COST_LEGACY` precedent). Kill: `TFB_SB_ATOMIC_WRITE=0` in `shadow_board.yml` restores the v1.3.0 clear-then-update byte-for-byte (proven, L3).

## Audit results (S4)
| Check | Result |
|---|---|
| `py_compile` · AST zero-removal (added `_sb_atomic_write` only) · smart-quote scan | PASS |
| **L1** full board selftest battery on v1.3.1 | **20/20 PASS ×3** |
| **L2** atomic path: single `update`, zero `clear`, 17×60 rectangle, legacy body verbatim at identical indices, pure-blank pad | PASS ×3 |
| **L3** kill switch: exact v1.3.0 call order and payload | PASS ×3 |
| **L4** the **landed scorer v1.8.0** extracts identical challengers (`[KRP.US]`) and `asof` from the padded rectangle | PASS ×3 |
| Triple-run digest | `f3283575ae2f937a` identical ×3 |

## Deploy
Commit the full file to `scripts/run_shadow_board.py`. No env change needed (default ON); rollback = `TFB_SB_ATOMIC_WRITE: "0"` in the workflow env or `git revert`.
https://github.com/emadsaberbahbah-coder/tadawul-fast/blob/main/scripts/run_shadow_board.py

## Queue state after this slot
Python-side mechanical queue: **EMPTY.** Remaining: (a) tonight's read-backs — first ~18:20 scorer run, then the observe armings one per evidence run; (b) **your 60-second Shadow_Board check** (Edge Verdict / Gen2 Eligible columns) deciding whether a NO_ROI→Engine-ROI fallback build is wanted (evidence-lane semantics, your call); (c) GAS-side P-134 + Copper quarantine (need the Apps Script source); (d) the model decisions F-1 / F-3 / F-4 / F-5.
