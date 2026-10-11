# TFB Commit Sheet — tracker workflows ENV PARITY: TRACK_DEDUP_MATURED / TRACK_RISK_BLANK / TRACK_DEDUP_SIGNAL_HISTORY mapped in both tracker steps + tests/test_track_flags_mapped.py

Date: 2026-10-11 (Saturday) · Lane: GitHub Actions (two workflow files, one commit together with `scripts/track_performance.py` v6.44.0 — see `TFB_Commit_Sheet_track_performance_v6.44.0_2026-10-11.md`) · WP-7 pass 1 · Register: findings.json `track-flags-unmapped` (P-189b / P-188 kill-switch-without-mapping) + P-191

## S0/S1 — Base pin
| File | Base @ `a6668c235b526394158726a3e34c8ffaeb95b6b7` | Delivered | Change |
|---|---|---|---|
| `.github/workflows/track_performance.yml` | sha256 `0af8f6c91283335901d041e659acd0635f5fc2b997b080d3ecde0dd1a614f299` (374 ln) | sha256 `4cda243aaa9d60b996d18bdffb65d091c93d5f5f618568d526154e2d738a0cc2` (408 ln) | **+34 / −0**: dated correction block in the `WHY THIS EXISTS` header (before its closing rule) + three mapping lines with a WHY/read-back/rollback comment in the `📈 Run performance clock` env, right after `TRACK_DROP_DUPLICATE_ROWS` |
| `.github/workflows/daily_sync.yml` | sha256 `bb435f67db69cbd2a75513b4464c9498a0009840e1f5bf9b514f0b19993a4985` (1,990 ln) | sha256 `58f7b9ceb017c21773b78763d233685fec22469f82c8730bee1e18b9e3107c39` (2,006 ln) | **+16 / −0**: one dated comment appended to the tracker step's lead comment + the same three mapping lines (mirrored verbatim) between `TFB_PERF_TARGET_UNIT_SENTRY` and `BACKEND_TOKEN` in the `📈 Track Performance (record + audit)` env |
| `tests/test_track_flags_mapped.py` | — | sha256 `05df1ad57a061bec26370911083cd14bc032f4054cd085c616f4b395d89fd8fa` (132 ln) | NEW, YAML text only |

Last commits touching the files before this build: `track_performance.yml` → `2428552` (2026-10-06, #718); `daily_sync.yml` → `2698457a9f2ec5b1a1d92800b3ec8b2b25452b4c` (2026-10-10). Neither file has a version constant; the dated `(2026-10-11)` comments are the version marks.

## S2 — Why
`track_performance.py` v6.41.0 (2026-10-01) shipped `TRACK_DEDUP_MATURED` (off | observe | enforce) and `TRACK_RISK_BLANK` (0 | 1) and named the Performance Clock run step as their env lane — but neither `track_performance.yml` nor the mirrored tracker step in `daily_sync.yml` ever mapped them, so a repo Variable could never reach `os.getenv()`. The 2026-10-10 export still carries the same 27 MATURED/MATURED duplicate Keys (20 W/W, 7 L/L) and 0.0 in Volatility / Max Drawdown % / Sharpe Ratio on 11,791/11,791 matured rows, and none of the 284 `[PERF-VERDICT]` `_Run_Log` lines carries `dedup_matured`. v6.44.0 adds `TRACK_DEDUP_SIGNAL_HISTORY` (Signal_History same-day repeats: 21,670 of 29,471 rows, max 19 per key) and would have joined the same unmapped class.

**Why both blocks, verbatim (ENV PARITY):** `PerformanceStore.save_records` rewrites every row, so `TRACK_RISK_BLANK` armed in one lane only would be undone by the other lane's next save; the two steps run the same script against the same tabs 4-5 times a day. `track_performance.yml` already states the rule for the three settings mirrored on 2026-10-06; the new test pins it mechanically.

**Why `${{ vars.X || default }}` and not a hardcode:** these are default-OFF flags whose unset state is the safe one, so a silently unset Variable (the S-1 lesson of 2026-09-10) falls back to the script default; the hardcoded `"observe"` of `TFB_PERF_TARGET_UNIT_SENTRY` is the right pattern only for a flag that must stay armed.

**Header correction:** `track_performance.yml`'s `WHY THIS EXISTS` and the daily_sync tracker lead comment both claimed per-day idempotency for Signal_History snapshots; the prior text is kept and a dated correction is added (Performance_Log keys are idempotent via the primed `load_records` cache; Signal_History is not, until enforce).

## S3 — The three lines (identical in both files; file order)
```
TRACK_DEDUP_MATURED: ${{ vars.TRACK_DEDUP_MATURED || 'off' }}
TRACK_RISK_BLANK: ${{ vars.TRACK_RISK_BLANK || '0' }}
TRACK_DEDUP_SIGNAL_HISTORY: ${{ vars.TRACK_DEDUP_SIGNAL_HISTORY || 'off' }}
```
Fallbacks equal the script defaults: `_dedup_matured_mode` → `or "off"`, `_risk_blank_enabled` → `or "0"`, `_dedup_signal_mode` → `or "off"` (the test reads the literals out of `scripts/track_performance.py`).

## S4 — Audits
| Check | Result |
|---|---|
| Anchored edits | 4/4 with `count == 1` asserted (W1 header, W2 clock env, D1 daily_sync lead comment, D2 daily_sync env); every base line re-emitted verbatim (zero removals: `git diff --numstat` 34/0 and 16/0) |
| YAML parse (`yaml.safe_load`) of `track_performance.yml`, `daily_sync.yml`, `ci.yml` | PASS |
| Smart quotes / non-ASCII in added lines | 0 smart quotes; added lines ASCII |
| `tests/test_track_flags_mapped.py` (5 tests: mapped-in-clock, mapped-in-daily-sync tracker step (step slice by the six-space `- name:` form, challenge correction), parity of the ordered three lines, workflow fallbacks == script reader literals, dated WHY + `ROLLBACK: delete the three lines` present in both steps) | `python -m pytest -q tests/test_track_flags_mapped.py` → **5 passed** ×3 (venv311) ×3 (venv_full); `python tests/test_track_flags_mapped.py` → `RESULT 0 FAIL`; ruff `E9,F63,F7,F82` PASS |
| Against the base workflows | each `_declarations(...) == [...]` assertion is red (0 declarations found) — the mapping is what the test proves |
| Behaviour with the Variables unset | the script defaults — byte-identical to the base run (proved on the script side: `TFB_Commit_Sheet_track_performance_v6.44.0_2026-10-11.md` S4) |

## S5 — Delivery
`.github/workflows/track_performance.yml` · `.github/workflows/daily_sync.yml` · `tests/test_track_flags_mapped.py` (NEW) · this sheet (NEW). Not touched: `.github/workflows/ci.yml` — the integrator adds `            tests/test_track_flags_mapped.py \` after line 411 (with the p191 line; see the companion sheet S6 step 2).

## S6 — Arming / read-back (GitHub lane; one action each)
1. Commit. With no Variable set, the next tracker run in either lane behaves exactly as before (read-back: `[PERF-VERDICT v6.44.0] ... selftest=PASS 22/22` without `dedup_matured` / `dedup_signal` tokens).
2. Observe sitting (recommended first): repo Variables `TRACK_DEDUP_SIGNAL_HISTORY=observe` and, optionally in the same sitting, `TRACK_DEDUP_MATURED=observe`. Read-back on the next run of **either** lane: job log `[v6.44.0 DEDUP-SIGNAL] mode=observe would_skip=k/built ... backlog=21,7xx max_per_key=19` and `[v6.41.0 DEDUP-MATURED] mode=observe would_expire=27 (W 20 / L 7 / BE 0)`; `_Run_Log` tokens ` | dedup_matured observe 27 | dedup_signal observe would_skip k/built backlog 217xx`. Both lanes must show it (parity).
3. `TRACK_RISK_BLANK=1`: hold until `scripts/tfb_export_audit.py` treats blank ≠ zero in `performance_log_risk_columns_all_zero` (today it coerces blank to 0, so P-188 would not flip and the arming would look inert).
4. `TRACK_DEDUP_MATURED=enforce` / `TRACK_DEDUP_SIGNAL_HISTORY=enforce`: separate sittings after a positive observe read-back; note that the signal flag's enforce is **measure-only** until v6.45.0 and prints `(measure-only)`.
5. Rollback: unset the Variables (defaults = off / 0 / off), or delete the three lines in BOTH files (the parity test enforces both-or-neither).

## Known limits
- Mapping alone changes nothing: all three flags default off; this sheet makes them **reachable**, the owner arms them.
- GitHub's `||` fallback treats a Variable set to an empty string as unset — intended here (empty → default off).
- A Variable can be unset silently; for default-OFF flags that is the safe direction, which is why the hardcode pattern was not used.
