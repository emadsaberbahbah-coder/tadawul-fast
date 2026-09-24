# TFB Arming Sheet — .github/workflows/daily_sync.yml — P-163b CRON CUT + P-154d QUOTA GUARD observe (2026-09-24)

Lane: GitHub Actions (data-sync jobs run only on `schedule` / `workflow_dispatch`; commits never spend provider calls). Commit with `[skip render]`.
Deviation recorded: two knobs in one commit (a schedule policy + a tag-only observe gate) — read-backs are disjoint (slot count / run cadence vs `[EODHD-QUOTA-GUARD]` lines), the 09-23 precedent (MAX_CYCLES + RECOVERY quota guard).

## Base / armed
| | |
|---|---|
| Base | sha `68c3026b7f3ca11f…`, 1,680 lines — the 09-23 armed file, live-fetched twice today, zero drift |
| Armed | sha `d6becc099b3e44a0…`, 1,694 lines, +15 / −1 (the one removed line is the replaced cron string); YAML parses; both jobs carry the guard var |

## The three anchored edits
1. L24 `- cron: "0 */4 * * *"` → `- cron: "0 4,12,20 * * *"` (07:00 / 15:00 / 23:00 Riyadh).
   WHY: measured 09-23 GMT day — a scheduled run costs ~21–38% of the 400k EODHD budget (04Z run 83k calls), a cold replay up to 17%; four executed slots reached 100% at 00:54 Riyadh. With runs now ~2h15m (MAX_CYCLES=1) the 08Z/00Z slots would start executing too (the concurrency group only cancels a queued run when a third one queues), i.e. six runs/day ≈ 126% before any replay.
2. sync-dashboard job env, directly under `TFB_SYNC_EODHD_QUOTA: "observe"` → `TFB_SYNC_EODHD_QUOTA_GUARD: "observe"` (+ comment block).
3. recover-missing-market-pages job env, under `EODHD_API_KEY: ${{ secrets.EODHD_API_KEY }}` → the same line (the replay subprocesses inherit the job env).

Prerequisite for edits 2–3 to mean anything: `scripts/run_dashboard_sync.py` v6.61.0 committed first (v6.60.0 ignores the variable — harmless either way).

## Read-backs (falsifiable)
- Cron: exactly three `run_dashboard_sync` run starts per UTC day from the first full day after the commit (07:15 / ~15:xx / ~23:xx Riyadh in `_Run_Log`); no run starting ~11:00 or ~03:00 Riyadh; `[EODHD-QUOTA]` peak tomorrow < 85% (v5.150.0 observe does not yet save calls — the enforce sitting does).
- Guard observe: `[EODHD-QUOTA-GUARD v6.61.0]` `_Run_Log` lines ONLY when the counter is ≥ 97% / exhausted, or a leg's outgoing matrix carries fresh 402 rows on ≥ 25% of rows; on a healthy day zero lines (the run log shows the INFO `verdict=allow` lines per ranked page). Every export keeps its current 402 behaviour — observe changes no write.
- Enforce = separate sitting after ≥1 disclosed would_skip or ≥3 clean days: expected `SKIPPED` lines + `leg=skipped` stamps only under exhaustion, and zero fresh `fetch_failed:HTTP 402` rows in any export thereafter.

## Rollback
Revert the commit, or delete the two `TFB_SYNC_EODHD_QUOTA_GUARD` lines (= off) and restore `"0 */4 * * *"`.

## Steps
1. https://github.dev/emadsaberbahbah-coder/tadawul-fast/blob/main/.github/workflows/daily_sync.yml — paste the armed file (full file, not a diff).
2. Commit message: `arm: daily_sync cron 4,12,20 (P-163b) + TFB_SYNC_EODHD_QUOTA_GUARD observe (P-154d) [skip render]`.
3. Reply "done" → SHA re-verification at HEAD (`d6becc09…`), then the next scheduled run (15:00 Riyadh) is the first read-back.
