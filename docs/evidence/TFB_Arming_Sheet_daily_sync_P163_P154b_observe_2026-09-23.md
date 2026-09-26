# TFB Arming Sheet — daily_sync.yml: P-163 replay cap (3→1) + P-154b quota guard OBSERVE

Date: 2026-09-23 (Riyadh) · Lane: GitHub Actions (`.github/workflows/daily_sync.yml`) · Render: NO change, NO deploy
Operator: Emad · Engineer: Claude · Protocol: One-Pass arming (S1 live pin → anchored edits count==1 → YAML parse → structural equality → full-file delivery)

## 1. Base pinned (S1)

| Item | Value |
|---|---|
| Base file | `.github/workflows/daily_sync.yml` at `main`, live-fetched twice (zero drift between fetches) |
| Base SHA-256 | `b0baca62f2e9f8cc24270cfc163b36d6f87f43ed9ec70b591019dc638513e21e` (= the P-158 observe arming of 2026-09-22) |
| Base lines | 1,660 |
| Script consuming the env | `scripts/run_inline_page_recovery.py` v1.2.0 at `main`, sha `ed2af0025cd097bc…` (397 lines) — reads `TFB_INLINE_RECOVERY_MAX_CYCLES` (L59, clamp 1..6, script default "1"), `TFB_INLINE_RECOVERY_QUOTA_GUARD` (L67/L85, explicit words off\|observe\|enforce), `TFB_INLINE_RECOVERY_QUOTA_SKIP_PCT` (L68, default 90.0); parses `sync_execution.log` under `--source-root source-sync-artifacts` (L101), which the recovery job downloads at YAML L1552–1557 (`tadawul-sync-logs-<run_id>-*`). |

## 2. Delivered (S5)

| Item | Value |
|---|---|
| Armed file | `daily_sync.yml` → commit to `.github/workflows/daily_sync.yml` |
| Armed SHA-256 | `68c3026b7f3ca11f0aadf1412ba35a663f7fb84f11b8dc647e19f7a98ea88b15` |
| Armed lines | 1,680 (+21 / −1: the single `"3"` line becomes `"1"`; 19 added lines are WHY comments) |
| Functional diff | exactly 2 keys in the `recover-missing-market-pages` job env: `TFB_INLINE_RECOVERY_MAX_CYCLES: "3"` → `"1"` (L1519 armed) and NEW `TFB_INLINE_RECOVERY_QUOTA_GUARD: "observe"` (L1541 armed) |
| Structural proof | both files parse (PyYAML); reverting the two keys in the armed document makes it deep-equal to the base document; `sync-dashboard` job env identical; every other job/step identical |
| Anchors | edit 1 anchor `TFB_AUDIT_MIN_FRESH_PCT: "95"\n      TFB_INLINE_RECOVERY_MAX_CYCLES: "3"` count==1; edit 2 anchor = the P-154 WHY line + `TFB_SYNC_EODHD_QUOTA: "observe"` + `EODHD_API_KEY` + `steps:` in the recovery job, count==1 (the sync-dashboard copies of those lines are NOT touched) |
| Hygiene | non-ASCII count unchanged (451/451), no smart quotes, digest identical ×3 |

## 3. Why (measured evidence)

- `_Status` 09-23: Global_Markets leg Duration 3,663,450 ms = **61.1 min** vs `TFB_SYNC_TIME_BUDGET_SEC: "3600"` (recovery env L1520 armed); Mutual_Funds 52.0 min. Every GM pass is cut by the budget → `[TIME-BUDGET] batches_done < total` → the audit marks the page incomplete → the recovery job replays the WHOLE page (no symbol-subset mode) up to `MAX_CYCLES`.
- `_Run_Log` run 35817773571 (09-23): GM legs at 08:21 / 09:51 / 10:53 / 11:55 Riyadh — three replays ≈62 min each, +42.8k EODHD calls (23.8k / 10.4k / 8.6k), run length **4h35m**; final page still 3,950 fresh + 2,659 preserved (59.8%) → `TFB Decision Feed NOT_ACTIONABLE`.
- Run cadence 09-15 → 09-23 (SYNC-HOLD owners): 4 executed cron slots per day (04/12/16/20 UTC); the 00 UTC and 08 UTC slots never observed in 9 days; run lengths 0h56–4h35 (GM ×1–×4).
- Quota: 09-22 curve 0.5% → 90.9% CRIT (23:43) → **100% EXHAUSTED at 00:56 09-23** inside the 23:00-leg replays; cycles 2–3 then 402-blocked 6,274 GM + 2,474 MF + 418 CFX rows. Same mechanism on the 09-20 23:00 leg (845 GM rows 402, P-154).
- With MAX_CYCLES=1 the run is base (~1h) + one replay (~62 min) ≈ 2h15m (< the 4h cron interval); daily burn drops by ≈1–2 replays ×(9–50k) calls. The remaining structural fix (symbol-subset replay / longer per-page budget) is a separate build decision.

## 4. Deviation recorded

Two changes in one commit: a **policy knob** (replay cap) and an **observe-only gate** (quota guard annotations). Read-backs are disjoint — the cap is measured by GM leg count / run length / quota curve; the guard by `[RECOVERY-QUOTA v1.2.0 observe]` annotations and `summary.quota_guard[]` — so attribution stays clean. Emad approved with "GO cycles" after the deviation was named in the morning audit.

## 5. Read-back (positive proof required before this counts as armed)

1. First scheduled run after the commit (12 UTC ≈ 15:00–16:20 Riyadh): `_Run_Log` shows **≤2 `[STATUS-STAMP …] Global_Markets` legs** for that run id, and the run's SYNC-HOLD span is **< 2h30m**.
2. The recovery job log carries `[RECOVERY-QUOTA v1.2.0 observe] Global_Markets (cycle 1): would allow …` (or `would SKIP` once used% ≥ 90 / EXHAUSTED) and the `inline-recovery-<run_id>` artifact's `inline-recovery-summary.json` has a non-empty `quota_guard[]`. Runs page: https://github.com/emadsaberbahbah-coder/tadawul-fast/actions/workflows/daily_sync.yml
3. Tomorrow's 07:00 export: `[EODHD-QUOTA]` peak for 09-23 **< 90%** (prediction, falsifiable: today read 34.3% at 11:55 with three runs still to come), `rows402 new = 0` on every night leg.
4. P-163 hypothesis test: if run length was the cause, the 08 UTC / 00 UTC slots start appearing as separate run ids within 48 h; if they still do not, the cause is elsewhere (queue supersession or GitHub cron drops) and the Actions history decides.

Negative read-back (any of): a run with 3+ GM legs, run length > 3h, or zero `[RECOVERY-QUOTA` annotations while a replay ran → report, do not re-arm blind.

## 6. Rollback

`git revert` of the commit, or by hand: set `TFB_INLINE_RECOVERY_MAX_CYCLES: "1"` back to `"3"` and delete the `TFB_INLINE_RECOVERY_QUOTA_GUARD` line — v1.1.0 behaviour is restored byte-identically (the script's off state is the default).

## 7. Deploy notes

- GitHub lane only. Commit touches `.github/workflows/daily_sync.yml` (+ this sheet under `docs/evidence/`). **Do not click Render Manual Deploy** — the backend is unaffected (the red-team confirmed deploys are manual; the three 09-22 restarts were clicks after non-backend pushes).
- Push in ONE commit via https://github.dev/emadsaberbahbah-coder/tadawul-fast — then reply "done" and Claude re-verifies both files by SHA at HEAD (per-file check, both files).
- Next sitting on this item: `TFB_INLINE_RECOVERY_QUOTA_GUARD: "enforce"` after ≥1 observe run showing correct `would SKIP/allow` verdicts.

Files delivered with this sheet: `daily_sync.yml` (→ `.github/workflows/daily_sync.yml`).
