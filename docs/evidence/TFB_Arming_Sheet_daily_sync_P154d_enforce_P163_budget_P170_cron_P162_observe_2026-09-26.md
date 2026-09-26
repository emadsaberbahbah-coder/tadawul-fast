# TFB Arming Sheet — `.github/workflows/daily_sync.yml` (2026-09-26, GitHub lane)

Recorded deviation: four knobs in one commit (disjoint read-backs). Operator-requested full-file delivery; value-only edits, no step/permission/secret/command touched.

| Item | Value |
|---|---|
| Base | HEAD 7658a3b… file sha `d6becc099b3e44a0…` (the 09-24 P-163b/P-154d arming) |
| Armed sha-256 | `67a0a4e82313720da5991b8cbb742c83023c706c14606f499c49416bd6385701` (91,788 bytes) |
| YAML parse | OK — jobs `ci-tests`, `preflight-validation`, `sync-dashboard`, `recover-missing-market-pages` unchanged |
| Functional diff vs HEAD | 5 lines changed, 2 lines added, 0 removed (plus comment lines) |

## Changes
| Line (armed) | Key | Old → New | Item |
|---|---|---|---|
| 24 | `cron` | `"0 4,12,20 * * *"` → `"17 4,12,20 * * *"` | **P-170** schedule drift: the 20Z slot fired 22:59Z, the 04Z slot 09:00Z (minute 00 is GitHub's most-delayed schedule minute) |
| 314 / 1567 | `TFB_SYNC_EODHD_QUOTA_GUARD` | `"observe"` → `"enforce"` (both jobs) | **P-154d** — runs 36199188352 / 36231358321: observe said "would REFUSE" while GM, CFX and MF were written poisoned |
| 562 / 1551 | `TFB_SYNC_TIME_BUDGET_SEC` | `"3600"` → `"5400"` (both jobs) | **P-163** — the 61-min GM leg beat the ceiling by ~40 s → whole-page replay that wrote the poisoned page |
| 321 / 1570 | `TFB_SYNC_FETCHFAIL_TRUTH` | *(new)* `"observe"` (both jobs) | **P-162** — v6.62.0 stamp truth, disclosure only |
| 1581 | `TFB_INLINE_RECOVERY_QUOTA_GUARD` | unchanged `"observe"` | optional later flip; the enforce guard above already skips replay subprocess legs |

## Read-backs (each independent)
- **P-154d enforce:** while the counter is EXHAUSTED (until 03:00 Riyadh Sunday) every ranked page stamps `leg=skipped` with `[EODHD-QUOTA-GUARD] … verdict=skip`, no 402 rows written, `TFB Decision Feed` NOT_ACTIONABLE(skipped:…); after the reset, normal legs.
- **P-163 budget:** first full GM leg ends with `fresh_cov ≥ 95%` on the first pass, no `[TIME-BUDGET]` line, no `[FLOOR-MERGE]` replay, run ≈ 65–80 min.
- **P-170 cron:** run start within ~15 min of 07:17 / 15:17 / 23:17 Riyadh (`run_started_at` on the Actions page).
- **P-162 observe:** GM stamp carries ` fetchfail=<new>/<carried> would_cov=…` (≈ `fetchfail=47/0 would_cov=96.8%` on a healthy leg — the persistent .MI/.NZ 404 rows); zero value changes; job log `[FETCHFAIL-TRUTH v6.62.0] selftest=PASS mode=observe`.

Rollback: the old values per line (each independently).
