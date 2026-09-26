# TFB Commit Sheet — scripts/run_dashboard_sync.py v6.60.0 [EODHD QUOTA SENTINEL]

**Date:** 2026-09-21 · **Register item:** P-154 · **Protocol:** One-Pass (S0–S5 complete; S6 = first observe leg)
**GO:** Emad, "let's move to the next one" (Build #1 v6.39.0 verified byte-identical at HEAD first).

## 1. Pins

| | Version | SHA-256 | Lines |
|---|---|---|---|
| Base (live-fetched, `main`) | 6.59.0 | `054be415d2a108db99b66c5f7b0a12de850bb518b0fb6a5e717c9a0f7cb02c1c` | 11,086 |
| Delivered | 6.60.0 | `38d1662f19eccfceaed6c7f4e2c085312b4929c644338e15a34d44bef92c9d4c` | 11,453 |
| Test | — | `290ce6779c5725cebecbfb6d01b3ce6c666543f6c39a8d36d7ac43203169dcbc` | 314 |

LF, UTF-8, 0 CR. Harness tree also pinned the sibling `scripts/critical_symbol_identity.py` (`920309f80ed8…`, 550 lines) that the sync imports.

## 2. Why (measured on the 2026-09-21 export, run 35560901900)

- 845 Global_Markets rows (+1 Mutual_Funds) carry `fetch_failed:HTTP 402`, all stamped 22:26–23:22 UTC on 09-20. EODHD documents 402 as the daily API-call limit exhausted with no extra calls left; the limit resets at midnight GMT.
- The engine's false-green screen then behaved as designed (fetch_failed → DQ cap 55 → BLOCKED): a quota outage rewrote 845 rows as blocked, seat GLNG.US fell 76.5 → 65.1, the rows were preserved through the morning leg, GM coverage read 87.0 % and the decision feed went NOT_ACTIONABLE.
- Nothing in `_Run_Log` said why. The sync never logs a provider HTTP class, and the quota counter appears nowhere in the evidence trail.
- The burn source is still a hypothesis (fundamentals fallback = 10 calls per request on ~3,800 GM rows per leg; GM legs/day rose 5–7 → 9–10). A budget guard belongs in the backend and should be aimed by a measured burn curve. **This release measures; it guards nothing.**

## 3. Change — 3 anchored edits, each asserted `count == 1`

| Edit | Site | Change |
|---|---|---|
| E1 | `SCRIPT_VERSION` | 6.59.0 → 6.60.0 + WHY block |
| E2 | before `_ohlc_prewrite_runlog_enabled` | sentinel helpers + `_append_runlog_eodhd_quota` (FW-3 channel shape, RUN-META details) |
| E3 | `_run_one_task`, after the FW-3 verdict block | guarded best-effort call for ranked market pages |

AST: 268 → 279 def/class names, **0 removed**, +11; **exactly one pre-existing def touched (`_run_one_task`)**. 1 line changed, 368 added; added lines: 0 non-ASCII, 0 tabs. `py_compile` PASS on base and delivered.

## 4. Gate — `TFB_SYNC_EODHD_QUOTA` = off | observe

Explicit word only (`1`/`true`/`on`/`enforce` = off). **ENV lane: GitHub Actions.** There is no enforce mode in this release.

- **off (default):** returns before any work — no network call, no line. Identical to v6.59.0 apart from the version string inside the existing tags.
- **observe:** after each ranked market page is written, one `_Run_Log` line:

```
[EODHD-QUOTA v6.60.0] Global_Markets | used=321500/400000 (80.4%) date=2026-09-21 extra=0 | delta=+4321 in 1742s since Market_Leaders | rows402 new=0 carried=845 | f429=0 f404=47 fetch_failed=892 | state=WARN | selftest=PASS 5/5
```

| Field | Meaning |
|---|---|
| used / limit / extra | one GET of the provider usage endpoint `/api/user` — costs 0 API calls. A counter dated before today (UTC) is read as 0 used: the provider resets lazily |
| delta | change since this process' previous sample. **Account-wide** (matrix legs, backend and any other consumer share one counter) — read it as a burn curve over the day, not a per-page invoice |
| rows402 new / carried | 402 rows in the outgoing matrix stamped since this process started (120 s skew) vs older preserved rows still carrying the tag |
| state | OK · WARN (≥ `TFB_SYNC_EODHD_QUOTA_WARN_PCT`, 80) · CRIT (≥ `…_CRIT_PCT`, 90) · ON_EXTRA · EXHAUSTED · UNKNOWN. **Any new 402 row = EXHAUSTED regardless of the counter** |

WARN and worse are WARNING-level and raise a `::warning::` annotation on the run page — exhaustion becomes visible the same hour.

**Safety.** The token is never logged: the poll swallows its own exceptions and reports only the exception type plus an HTTP status code, because the URL carries the key. No key in the job env → `used=unknown (no_key)` and the row counts still publish. One attempt, ≤ `TFB_SYNC_EODHD_QUOTA_TIMEOUT_S` (5 s). The sentinel's own append failure is annotated but deliberately **not** counted into `_RUNLOG_APPEND_FAILS` — telemetry cannot flip the sync's exit code.

## 5. Evidence — real-module harness, ×3 identical

`tests/test_sync_eodhd_quota_p154.py` runs the real functions; the usage endpoint is emulated by a local HTTP server so the real urllib path executes end to end. Only double: the Sheets service recorder.

| Test | Result |
|---|---|
| K1 off | unset / `1` / `true` / `on` / `enforce` → 0 HTTP hits, 0 appends |
| K2 parse | doc-shaped payload → 80.4 %; stale counter date → used 0; junk → not ok |
| K3 **real 09-21 export rows** | Global_Markets: **845 carried**, 0 new from the morning leg's clock (845 new from the night leg's clock), f404 = 47, fetch_failed = 892 · Mutual_Funds 1 carried · Commodities_FX 0 (35 other fetch-failed) · Market_Leaders 0 — matches the morning audit to the row |
| K5 line | 10-column row, WARNING / WARN at 80.4 %, RUN-META `run_id` + `ts_utc` present |
| K6 delta | second sample → `delta=+4321 … since Market_Leaders` |
| K7 failure modes | HTTP 401 → `poll_failed:HTTPError:401`; slow endpoint with 1 s timeout returns in < 2.9 s; no key → `no_key` — none raise |
| K8 token hygiene | key absent from stdout, logging and every sheet payload (and the emulator confirms it was sent) |
| K9 | fresh 402 row → EXHAUSTED + WARNING with the counter at 0.3 % |
| K10 dual-tree AST | 0 removed, +11, touched = `_run_one_task` only |
| K11 | sentinel append failure → annotated, `_RUNLOG_APPEND_FAILS` unchanged |

Digest dual-tree + real export `06479b7dcf707a7a` ×3 · CI-shaped single-tree `e2789ea0e74d4e06`.
Not run: the repo's other sync tests could not be enumerated (GitHub tree API rate-limited); any test pinning the literal `6.59.0` will need its pin loosened — the CI run on the commit is the check.

## 6. Deploy

Commit three files — no Render action (the sync runs in GitHub Actions from HEAD):

- `scripts/run_dashboard_sync.py`
- `tests/test_sync_eodhd_quota_p154.py`
- `docs/evidence/TFB_Commit_Sheet_run_dashboard_sync_v6.60.0_2026-09-21.md`

Deploy read-back (gate off): next `_Status` stamps and guard lines read `v6.60.0`; zero `[EODHD-QUOTA]` lines.

## 7. Arming — separate sitting

`.github/workflows/daily_sync.yml`, sync-dashboard job `env:` block (after `TFB_SYNC_PERSIST_SANITY: "1"`):

```yaml
      TFB_SYNC_EODHD_QUOTA: "observe"
      EODHD_API_KEY: ${{ secrets.EODHD_API_KEY }}
```

One feature, two lines: without the key line the state reads UNKNOWN (no_key) but 402 counts still publish. Optional: the same two lines in the recovery job's `env:` block.
Observe read-back: one `[EODHD-QUOTA v6.60.0]` line per market page per leg. After one day the lines give the burn curve by hour and the leg(s) where the counter jumps — that aims the backend budget guard (next build).

## 8. Rollback

Remove the two env lines (→ off) or `git revert`. No data is written or changed in any mode except the telemetry line itself.

## 9. Deliberate cuts

- No enforce / no budget guard here — it belongs in the backend once the burn source is measured.
- Ranked market pages only (the FW-3 page set); Data_Dictionary / My_Portfolio legs are not sampled.
- The false-green screen (fetch_failed → DQ 55 → BLOCKED) is untouched: it is a working safeguard.
- The two per-venue coverage gaps found the same morning (`.MI` 32/32 and `.NZ` 15/15 rows HTTP 404 — and failed lookups are charged) are visible in the new `f404` count but not fixed here.
