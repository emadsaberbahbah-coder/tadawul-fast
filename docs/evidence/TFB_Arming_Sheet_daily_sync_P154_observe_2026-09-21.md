# TFB Arming Sheet — daily_sync.yml · P-154 EODHD quota sentinel → observe

**Date:** 2026-09-21 · **Lane:** GitHub Actions (no Render change) · **Feature:** `run_dashboard_sync.py` v6.60.0 (landed byte-identical at HEAD; CI green on push runs #4276–#4281)

| | SHA-256 | Lines |
|---|---|---|
| Base `.github/workflows/daily_sync.yml` (live-fetched, `main`) | `c4b4eb4a105d97450408dcdc8c3b98e2b4e229f1e4c7eb4e7b7fc2c8073f31c5` | 1,627 |
| Armed | `ab760153d74d3796809c8f143f10ab06ebaa4d1c3facd4aad025081710d43aa0` | 1,648 |

Three anchored insertions (each `count == 1`); **21 lines added, 0 removed** — 4 functional, 17 comments. Both files parse as YAML; 4 jobs before and after.

| # | Where | Functional line(s) |
|---|---|---|
| 1 | `sync-dashboard` job `env:` (after `TFB_SYNC_PERSIST_SANITY`) | `TFB_SYNC_EODHD_QUOTA: "observe"` |
| 2 | step `execute_sync` `env:` (beside `BACKEND_TOKEN`) — **step-scoped on purpose** | `EODHD_API_KEY: ${{ secrets.EODHD_API_KEY }}` |
| 3 | `recover-missing-market-pages` job `env:` (that job's own convention keeps secrets at job level) | gate + key |

`ci-tests` never sees the key (verified on the parsed YAML). The sentinel never logs the token (harness K8).

**Why the recovery job too:** its retries launch fresh `run_dashboard_sync.py` processes, up to `TFB_INLINE_RECOVERY_MAX_CYCLES` (3) — a suspected burn amplifier. Each process logs `delta=first-sample`; the absolute counter plus the timestamp still give the curve.

**Schedule facts pinned from this file:** data-sync jobs run only on `schedule` (cron `0 */4 * * *` → 03:00 / 07:00 / 11:00 / 15:00 / 19:00 / 23:00 Riyadh) and `workflow_dispatch`. A push runs `ci-tests` only — commits do not spend provider calls.

**Read-back (S6 observe cycle):** first scheduled leg after the commit → one `[EODHD-QUOTA v6.60.0]` line per ranked market page in `_Run_Log`; `_Status` stamps read `v6.60.0`. The 23:00 Riyadh (20:00Z) leg covers the window in which the limit ran out on 09-20 (22:26–23:22Z).
What to read: `used / limit (pct)` by hour = the burn curve; the leg where the counter jumps = the burn source; `rows402 new` > 0 or `state=EXHAUSTED` = the outage, same hour, also as a `::warning::` on the run page.

**Rollback:** delete the four functional lines (or revert the commit).
