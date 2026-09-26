# TFB Commit Sheet — scripts/run_dashboard_sync.py v6.63.0 [P-162b KEEP-LAST-GOOD FOR FETCH-FAILED ROWS]

Date: 2026-09-26 (Saturday) · Lane: GitHub (Actions script; no Render deploy) · Build #4 of the day — **recorded deviation from the three-script cap** (operator-directed "next scripts"; the gate ships OFF, so no read-back collision with today's armings) · Protocol: One-Pass

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | `scripts/run_dashboard_sync.py` v6.62.0 at HEAD 3ef955e (2026-09-26 13:16 Riyadh) |
| Base SHA-256 | `51325da7db8a180d800e69501223ced5a08a8d2195e74878be2fbb1b45489125` (Build #2 delivery, zero drift) |

## S2 — Root (pinned on source + runs 36199188352 / 36231358321)
`_keep_last_good_rows` substitutes only DATA-FREE stubs ("all forms require NO positive price"). A `fetch_failed:HTTP 402` row carries the engine's last-known price, so it is never a stub: on 09-26 the guard swapped 7 priceless CFX crypto stubs and let 6,302 GM + 411 CFX + 2,472 MF price-carrying 402 rows overwrite clean rows. The armed v6.61.0 refusal covers storms ≥ 25%; the partial storm (09-20: 845 rows = 12.8%) still overwrites row by row.

## S3 — Change (8 anchored edits, each `count == 1`)
| # | Site | Edit |
|---|---|---|
| E1 | header | `SCRIPT_VERSION = "6.63.0"` + WHY block (ASCII only) |
| E2 | before `_keep_last_good_rows` | `_KLG_FF_TAG`, `_LAST_KLG_FF` counters, `_klg_fetchfail_mode()` (gate `TFB_SYNC_KLG_FETCHFAIL` off/observe/enforce), `_klg_ff_reset()`, `_klg_fetchfail_selftest()` (drives the REAL guard on a canned grid) |
| E3 | stub scan | form (d): a row whose Warnings match `_FG_FETCHFAIL_RE` is a candidate whatever its price cell says (gate off → the scan is byte-identical) |
| E4 | fast path / pending | the one old-grid read also fires when only candidates exist; candidates join the certification loop; `old_warn_i` located |
| E5 | certification tail | extra certificate for (d): the prior must not itself carry `fetch_failed`; observe counts (`good_prior` / `prior_fetchfailed`), enforce substitutes (header-aligned like a stub) and counts the symbol in `swapped` → `klg_kept` |
| E6 | `_run_one_task` KLG block | one `[KLG-FETCHFAIL v6.63.0] <page> \| mode=… \| candidates=… good_prior=… prior_fetchfailed=… substituted=…` line (warnings + logger + `::warning::`) when candidates exist; `_stamp_meta.klg_ff_cand/good/swapped` |
| E7 | startup | `[KLG-FETCHFAIL v6.63.0] selftest=… mode=…` beside the P-162 line |

Ordering fact (pinned): KLG → **v6.61.0 post-fetch refusal** → P-162 census → stamp. Under enforce, substituted rows no longer carry the tag, so a page with certified priors is WRITTEN (old good rows + fresh clean ones) instead of refused whole; rows without a prior still count toward the 25% refusal; `klg_kept` drives `fresh_cov` → PARTIAL / STALE_COV with no further gate.

Scope cuts (stated): forms (a)–(c) and their certification untouched; no substitution cap (the identity firewall's per-row gates stand; whole-page substitution is the intended outcome of a whole-page provider failure). Companion edit: `tests/test_sync_fetchfail_truth_p162.py` version pin loosened `== 6.62.0` → `>= (6, 62, 0)` (the 09-22 practice).

## S4 — Audits (×3 identical)
| Check | Result |
|---|---|
| Delivered SHA-256 | `2c6964219317a5028b88eb466e38ebd0a8d6dcbe1767b6fc91ba39cbc1323703` (618,265 bytes, 12,179 lines LF) |
| `py_compile` | PASS |
| AST | functions 292 → 297 (+`_klg_fetchfail_mode`, `_klg_ff_reset`, `_klg_fetchfail_selftest` and its inner `_run`; **0 removed**); 4 base lines not verbatim = version line + three lines widened (`if not stub_rows` → `and not ff_rows`; `pending` union; `stub_rows[t]` → `.get(t, [])`) |
| Non-ASCII | multiset identical to base (0 new); 0 smart quotes |
| Real-module self-test | `_klg_fetchfail_selftest() == PASS`, `_fetchfail_truth_selftest() == PASS` |
| Harness `tests/test_sync_klg_fetchfail_p162b.py` (dual-tree, REAL `_keep_last_good_rows` with a read-only Sheets double, REAL `TaskResult`) | T1–T8 PASS ×3, digest `ad14c9d90dc9` ×3; pytest 8/8 |
| Fixtures | off = byte-identical (fetch-failed rows untouched, zero-stub fast path still zero reads); observe = candidates 4, good_prior 1, prior_fetchfailed 1, substituted 0, one read; enforce = only the certified prior rides back (A.US 9.9 → 10.0 with its clean Warnings/stamp), poisoned prior / no prior / priceless prior keep the fresh row, classic stub path unchanged, idempotent on a second pass; forced-refetch symbol never rides back |
| Real 09-26 GM page vs a clean prior grid | candidates 6,349 → enforce substitutes **6,253**, 96 left (priors failing certification); stamp `preserved=6253 … data=PARTIAL`, cell PARTIAL_FRESH, feed STALE_COV — by the existing arithmetic |
| Seam order | KLG block < disclosure line < P-154d post-fetch seam < P-162 census (source assertion) |
| Base parity (v6.62.0) | gate off → identical matrix/swaps; base ignores the gate and lets the poisoned row overwrite |
| Existing sync batteries on the v6.63.0 tree | 38 passed, 1 skipped ×3 (P-154 / P-154d / P-162 / outcome audit / recovery plan + this battery) |

## S5 — Delivery
| File | Destination |
|---|---|
| `run_dashboard_sync.py` | `scripts/run_dashboard_sync.py` (full file) |
| `test_sync_klg_fetchfail_p162b.py` | `tests/` |
| `test_sync_fetchfail_truth_p162.py` | `tests/` (REPLACES the 09-26 file — pin loosened) |
| `TFB_Commit_Sheet_run_dashboard_sync_v6.63.0_2026-09-26.md` | `docs/evidence/` |

No Render change. No ENV change in this commit (gate defaults OFF).

## S6 — Arming + read-back (GitHub lane, separate sitting after the P-162 observe read-back)
- `TFB_SYNC_KLG_FETCHFAIL: "observe"` in both jobs of `daily_sync.yml` → read-back: `[KLG-FETCHFAIL v6.63.0] <page> | mode=observe | candidates=n good_prior=m …` lines on any leg with fetch-failed rows (≈ `candidates=47` on a healthy GM leg — the .MI/.NZ 404 rows, whose priors are themselves 404-tagged → `prior_fetchfailed`), zero substitutions.
- `enforce` (later sitting): a partial-storm leg stamps `preserved=<n>` with `[v6.22.3 KEEP-LAST-GOOD] substituted …` listing the symbols, `fresh_cov` honest, the page keeps its clean rows.
