# Commit Sheet - `scripts/track_performance.py` v6.52.0 [OUTCOME-TALLY] - 2026-10-27

**Scope:** one observe line per audit pass saying why due records did or did not mature, per exchange calendar. For quotes refused as `quote_session_unknown`, it also says how many minutes after the session close they were stamped. Measures only. ENV-gated, default OFF (`TRACK_OUTCOME_TALLY`).

**Numbering:** 6.52.0, because open PRs #841 / #845 claim 6.50.0 / 6.51.0. The clock workflow header is 1.0.8 because they claim 1.0.6 / 1.0.7. Whichever merges later renumbers. A trial merge with each of them conflicts only on the docstring version line, the WHY block / `SCRIPT_VERSION` region, the clock workflow header and the `verify_deployment` pin; `daily_sync.yml`, `test_workflow_flag_mappings.py` and `test_outcome_price_evidence.py` merge cleanly.

**Files:**
- `scripts/track_performance.py` (6.49.0 -> 6.52.0)
- `.github/workflows/track_performance.yml` (header 1.0.8, one env line in the clock step)
- `.github/workflows/daily_sync.yml` (one env line in the tracker step; header unchanged, as in #827)
- `scripts/verify_deployment.py` (pin only)
- `tests/test_tracker_outcome_tally.py` (new harness)
- `tests/test_outcome_price_evidence.py` (four guards in the blocking lean lane)
- `tests/test_workflow_flag_mappings.py` (tracker default `off`)
- this sheet

## S1 - Gap (evidence)

- **The counter exists, the output does not.** `audit_active_records` counts every reason a due record stays unmatured (`_skipped_outcome_reasons`, stored as `_last_audit_stats['outcome_skipped_reasons']`). Nothing prints it. The `[PERF-VERDICT]` line and the job log carry only `matured N | pending M`.
- **What that cost on 26 Oct.** Both Sunday 25 Oct audits (jobs 121204298588 and 121254652958) matured 0 records, although many `.SR` records targeted that session. The proposed cause was the backend's `.SR` quote stamp: it is the closing-auction print (the one committed sample, 2286.SR, is 12:10:24Z), after XSAU's 12:00Z close. A `source_quote` gets zero close slack in `_outcome_quote_session`, so it is refused as `quote_session_unknown`. An independent refuter rated this PLAUSIBLE, not CONFIRMED, because no live reason tally exists.
- **Scale.** The 26 Oct 20:45Z audit (job 121837451879) left `pending-price=4404` with no breakdown by reason or market.

## S2 - Change (additive; zero removals)

| Mode | Behaviour |
|---|---|
| off (DEFAULT; unset or any other word, `'1'` and `enforce` included) | v6.49.0 exactly. The collectors stay `None`; no line. |
| observe | Measures only. One INFO line after the HIST-CLOSE block, then the collectors are disarmed. Records, receipts, `_last_audit_stats`, saves and every other line equal off. |

**How observe measures:**
- `_outcome_tally_begin()` arms two per-pass collectors at the top of `audit_active_records`. This is before the price fetch, so refusals made while absorbing the backend rows are seen.
- `_outcome_receipt` is the one place a due record's verdict is written. Before writing the unchanged receipt, it remembers that record's last `(symbol, state, reason)`.
- When `_outcome_quote_session` refuses a `source_quote` or `regular_market` quote as `quote_session_unknown`, it remembers the minutes between that quote and the last session close before it.

**The line:**

```
[v6.52.0 OUTCOME-TALLY] mode=observe evaluated=N matured=M expired=E pending=P | <CAL> n= matured= expired= pending= reasons=<reason:n,...> after_close_min=<0-15:a,15-20:b,20-60:c,60+:d> | ...
```

- There is one segment per exchange calendar, largest first.
- Expired reasons carry an `expired:` prefix.
- `after_close_min` counts distinct symbols.

**Env lane:** both tracker steps map the Variable (ENV PARITY).
- `track_performance.yml`: the `Run performance clock` step, on every scheduled audit.
- `daily_sync.yml`: the `Track Performance (record + audit)` step. It audits on a manual dispatch with `run_tracker=true`; it is inert on record-only schedules, because those never call `audit_active_records`.

**Additions:** `_outcome_tally_mode`, `_outcome_tally_begin`, `_outcome_tally_end`, `_outcome_tally_stamp`, `_outcome_tally_line`, `_OUTCOME_TALLY_TAG`, `_OUTCOME_TALLY`, `_OUTCOME_TALLY_STAMPS`. Zero top-level functions removed.

## S3 - Known limits

- `after_close_min` counts distinct symbols, not records. Only a quote refused as `quote_session_unknown` carries a stamp; another refusal (for example `quote_timestamp_unknown`) shows only its reason.
- The calendar is the symbol's own exchange calendar, or `UNKNOWN` when the tracker has none.
- It measures; it does not fix. The XSAU closing-auction allowance stays a separate, owner-gated build. Its prototype is in the 26 Oct review.
- **The full harness runs only in the advisory whole-suite job.** Four guards run in the blocking lean lane through `tests/test_outcome_price_evidence.py`: three off spellings, and observe on a Saudi refusal. `ci.yml` is not touched.

## S4 - Tests

**`tests/test_tracker_outcome_tally.py`:**
- PASS 8/8 as a script; 16 passed under pytest; identical under three hash seeds.
- With no git base (`GIT_DIR=/nonexistent`, CI's depth-1 checkout) and stray tracker variables set: 15 passed, 1 skipped (T7). T3 then checks `OFF_GOLDEN = 72db2ad8b4251afe`, the base digest.

| Case | What it pins |
|---|---|
| T0 | the parser takes the exact word `observe` only (`'1'`, `'true'`, `'on'`, `'enforce'`, `'observed'` -> off) |
| T1 | **observe, nine records.** The line reads `evaluated=7 matured=2 expired=0 pending=5`. XSAU: `n=5 matured=1 pending=4 reasons=quote_session_unknown:4 after_close_min=0-15:1,15-20:1,20-60:1,60+:1` (2286.SR at 12:10:24Z, 12:17Z, 12:45Z, 15:00Z; 7010.SR at the close matures). XNYS: `n=2 matured=1 pending=1 reasons=price_unavailable:1`. A not-yet-due and an already-MATURED record are not counted. The tally equals `_last_audit_stats`. |
| T2 | observe changes nothing else: records, receipts, stats and every other log line equal the off run |
| T3 | **off, nine spellings:** records, receipts, stats and logs equal the v6.49.0 copy, or `OFF_GOLDEN`; no collector is armed |
| T4 | one INFO line, after the MATURE-FRESH and HIST-CLOSE lines; collectors disarmed after |
| T5 | edges: a `regular_market` quote 16 min after close (past its 15 min slack) lands in 15-20; exactly 15 min is 0-15 and exactly 20 min is 15-20; an unpriced record past grace counts as `expired:price_unavailable`; an unknown calendar reads `UNKNOWN`; nothing due means no line; only live quote kinds are stamped; the helpers never raise |
| T6 | both tracker steps map `${{ vars.TRACK_OUTCOME_TALLY \|\| 'off' }}` exactly once; header 1.0.8; SCRIPT_VERSION >= 6.52.0 in lockstep with docstring line 5 and the `verify_deployment` pin |
| T7 | ASCII added lines; zero removals; on 6.52.0 exactly the five new functions are added |

**Golden negative:** module under test = `git show e8337a16:scripts/track_performance.py`.
- Result: 6 failed, 2 passed.
- T2 (off equals observe) and T3 (off identity) pass, as they must.
- T1 fails because no line is written.

**Blocking-lane guards** (`tests/test_outcome_price_evidence.py`): 81 passed. On the base tree the observe guard fails, and the three off guards pass.

**Mutations: 18/18 caught.**

| Mutation | Caught by |
|---|---|
| collectors always armed | T2, T3 |
| parser accepting `'1'` | T0, T3 |
| case-sensitive parser | T0 |
| line not logged | T1, T4, T5 |
| line at WARNING | T4 |
| line moved above the HIST-CLOSE block | T4 |
| collectors not disarmed | T4, T5 |
| receipt hook removed | T1, T4, T5 |
| stamp hook removed | T1, T5 |
| 15 min edge strict | T5 |
| 20 min edge strict | T5 |
| historical quote kinds stamped | T5 |
| `expired:` prefix dropped | T5 |
| expired counted as pending | T5 |
| not-yet-due records counted | T1, T5 |
| version not bumped | T6 |
| docstring not bumped | T6 |
| observe alters a receipt | T2 |

## S5 - Operator steps (nothing is armed here)

1. Merge. No Render deploy is needed: the tracker runs in GitHub Actions.
2. Set the repository Variable `TRACK_OUTCOME_TALLY=observe`.
3. Read the `[v6.52.0 OUTCOME-TALLY]` line on the next audits (the clock's two daily slots).
   - **For the Saudi question:** if the XSAU segment shows `quote_session_unknown` with most symbols in `0-15` / `15-20`, the closing-auction hypothesis is confirmed. The XSAU-only allowance is then the next build.
   - **For the US weekday question:** compare the XNYS segment between a slot that ran before 20:00Z and one that ran after.

**Rollback:** unset the Variable, or set it to `off`.

## S6 - Not done here

- No acceptance change: the XSAU closing-auction allowance is not built.
- No cron change, and no `ci.yml` change.
- No change to `_last_audit_stats`, `[PERF-VERDICT]` or `_Run_Log`.
