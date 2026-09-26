# TFB Commit Sheet — scripts/run_dashboard_sync.py v6.62.0 [P-162 FETCH-FAILED STAMP TRUTH]

Date: 2026-09-26 (Saturday) · Lane: GitHub (Actions script; no Render deploy) · Build #2 of the day · Protocol: One-Pass

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | `scripts/run_dashboard_sync.py` v6.61.0 live-fetched from HEAD f66a38e (2026-09-26 11:14 Riyadh) |
| Base SHA-256 | `fab345b76829d6eb9a6566d4097391be2c13a9edf0e4eb5393237b8fbb45efd9` (595,070 bytes, 11,767 lines LF, 288 functions) |
| Drift vs the 2026-09-24 v6.61.0 delivery pin (`fab345b7…`) | **zero** |

## S2 — Root (pinned on source + run 36199188352)
`_status_stamp_row`: `fresh = pre_persist_rows − klg_kept`, `cov = fresh/requested` — every row the fetch RETURNED counts as fresh. `_uv_page_state` mirrors the arithmetic for the per-page feed token. A row the engine tagged `fetch_failed:HTTP 402` carries a last-known price (not a data-free stub), so on 2026-09-26 the recovery replay's 6,302 poisoned GM rows (+411 CFX) stamped `fresh=6444 fresh_cov=97.5% | data=COMPLETE`, feed `GM:OK`, Decision Feed `EXECUTABLE` — the P-162 blind spot (first specimen 2026-09-22: 561 stale rows under "100% fresh").

## S3 — Change (7 anchored edits, each `count == 1`)
| # | Site | Edit |
|---|---|---|
| E1 | header | `SCRIPT_VERSION = "6.62.0"` + v6.62.0 WHY block (ASCII only) |
| E2 | new helpers before `_status_data_verdict` | `_fetchfail_truth_mode()` (gate `TFB_SYNC_FETCHFAIL_TRUTH` off/observe/enforce; a failed self-test degrades enforce→observe), pure `_fetchfail_count_rows(headers, rows, t0)` (ff_new / ff_carried, the v6.60.0 120 s stamp-age rule, `_FG_FETCHFAIL_RE`), pure `_fetchfail_truth_apply(fresh, requested, meta, mode)` → (fresh, cov, note), `_fetchfail_truth_selftest()` (5 fixtures incl. the real 09-26 GM leg) |
| E3 | `_status_stamp_row` | one apply step after the original `cov` line; ` fetchfail=<new>/<carried>[ would_cov=x%]` inserted between `preserved=` and `fresh_cov=` |
| E4 | `_uv_page_state` | enforce mirrors the same apply (feed token STALE_COV/PARTIAL follows) |
| E5 | `_run_one_task` | census seam after the v6.61.0 post-fetch guard, before persistence verification; ranked market pages only; writes `res._stamp_meta["ff_new"/"ff_carried"]`; try/except, count only |
| E6 | startup | one `[FETCHFAIL-TRUTH v6.62.0] selftest=… mode=…` log line beside the FW/FG self-tests |

Semantics: **off** = byte-identical stamps/cells/tokens (version string apart). **observe** = values untouched, disclosure only. **enforce** = `fresh −= ff_new` → fresh_cov, PARTIAL_FRESH, the v6.51.0 data verdict, the Status cell and the feed token all follow from that one number. Zero counts → no disclosure (healthy legs keep their exact text). `data_status` stays binary (COMPLETE/PARTIAL).

Deliberate scope cut (stated): keeping a poisoned row from overwriting a clean one BELOW the v6.61.0 25% refusal threshold = a keep-last-good widening (a price-carrying `fetch_failed` row is not a stub under the v6.22.3 doctrine) → **register candidate P-162b** after this build's observe read-back shows the residual `ff_new` per leg.

## S4 — Audits (×3 identical)
| Check | Result |
|---|---|
| Delivered SHA-256 | `51325da7db8a180d800e69501223ced5a08a8d2195e74878be2fbb1b45489125` (607,945 bytes, 12,000 lines LF) |
| `py_compile` | PASS |
| AST | functions 288 → 292 (+4: `_fetchfail_truth_mode`, `_fetchfail_count_rows`, `_fetchfail_truth_apply`, `_fetchfail_truth_selftest`; **0 removed**); 1 base line not verbatim = the version line |
| Non-ASCII | multiset identical to base (571; 0 new); 0 smart quotes |
| Harness `tests/test_sync_fetchfail_truth_p162.py` (dual-tree, REAL module, REAL `TaskResult`/`TaskSpec`, real 09-26 GM export) | T1–T8 PASS ×3, digest `6194ca806570` ×3; pytest 8 passed |
| Real census (GM 09-26, t0 = 00:17:04Z) | ff_new **6,338** / ff_carried **11** (6,349 fetch_failed rows = 6,302 × 402 + 47 × 404) |
| Enforce stamp on the real leg | `fresh=106 preserved=165 fetchfail=6338/11 fresh_cov=1.6% \| data=PARTIAL`, Status cell PARTIAL_FRESH, feed `GM:STALE_COV` |
| Observe stamp | values unchanged + ` fetchfail=6302/0 would_cov=2.1%` (fixture) |
| Base parity | v6.61.0 stamp row == delivered off-mode row (same run id), feed OK/97.5 both; base says COMPLETE where enforce says PARTIAL |
| Healthy leg (ML 255/255) | identical text in all three modes; carried-only rows disclose without moving the number |
| Seam replay | the inserted block, extracted verbatim from the delivered source, placed after the P-154d post-fetch seam and before persistence verification; populates the REAL `TaskResult._stamp_meta` (off → no keys; non-market page → no keys); real GM page → 6,338/11 |
| Existing sync batteries on the delivered tree | `test_sync_eodhd_quota_guard_p154d`, `test_sync_eodhd_quota_p154`, `test_sync_outcome_audit`, `test_sync_recovery_plan`: 22 passed, 1 skipped ×3 — identical to base |

## S5 — Delivery
| File | Destination |
|---|---|
| `run_dashboard_sync.py` | `scripts/run_dashboard_sync.py` (full file) |
| `test_sync_fetchfail_truth_p162.py` | `tests/` (pytest; `TFB_TEST_SYNC_BASE` / `TFB_TEST_EXPORT_DIR` optional) |
| `TFB_Commit_Sheet_run_dashboard_sync_v6.62.0_2026-09-26.md` | `docs/evidence/` |

No Render change. No ENV change in this commit (gate defaults OFF).

## S6 — Arming + read-back
- Arming (GitHub lane, separate sitting): `TFB_SYNC_FETCHFAIL_TRUTH: "observe"` in **both** the sync-dashboard job env and the recovery job env of `.github/workflows/daily_sync.yml`.
- Observe read-back (first scheduled run): every GM stamp carries ` fetchfail=<n>/<m> would_cov=…` (the 47 persistent .MI/.NZ 404 rows alone give ≈ `fetchfail=47/0 would_cov=96.8%` on a healthy leg — enforce would NOT flip a healthy page); a storm leg shows `would_cov` under the 95% floor. Zero value changes; `[FETCHFAIL-TRUTH v6.62.0] selftest=PASS mode=observe` in the job log.
- Enforce (a later sitting, after one clean observe run): a storm leg stamps `data=PARTIAL`, Status cell PARTIAL_FRESH, `TFB Feed Global_Markets` STALE_COV, `TFB Decision Feed` NOT_ACTIONABLE(stale_cov:GM) → the cockpit WITHHOLDS instead of building on a poisoned page. Composes with v1.11.10 P-168 (clocks pause on the same rows).
