# Commit Sheet - `scripts/track_performance.py` v6.52.0 [OUTCOME-TALLY] - 2026-10-27

**Scope:** observe lines per audit pass saying why due records did or did not mature, per exchange calendar. For quotes refused as `quote_session_unknown`, they also say how many minutes after the session close they were stamped. Measures only. ENV-gated, default OFF (`TRACK_OUTCOME_TALLY`).

**Numbering:** 6.52.0, because open PRs #841 / #845 claim 6.50.0 / 6.51.0. The clock workflow header is 1.0.8 because they claim 1.0.6 / 1.0.7. Whichever merges later renumbers. A trial merge with each of them conflicts only on:
- the docstring version line;
- the WHY block / `SCRIPT_VERSION` region;
- the clock workflow header;
- the `verify_deployment` pin.

`daily_sync.yml`, `test_workflow_flag_mappings.py` and `test_outcome_price_evidence.py` merge cleanly.

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
  - Per-record receipts in the Performance_Log Notes column do keep each refusal reason. But nothing aggregates them, and the workbook is not visible from a job log.
- **What that cost on 26 Oct.** Both Sunday 25 Oct audits (jobs 121204298588 and 121254652958) matured 0 records, although many `.SR` records targeted that session.
  - **Proposed cause:** the backend's `.SR` quote stamp is the closing-auction print (the one committed sample, 2286.SR, is 12:10:24Z), after XSAU's 12:00Z close. A `source_quote` gets zero close slack in `_outcome_quote_session`, so it is refused as `quote_session_unknown`.
  - An independent refuter rated this PLAUSIBLE, not CONFIRMED, because no live reason tally exists.
- **Scale.** The 26 Oct 20:45Z audit (job 121837451879) left `pending-price=4404` with no breakdown by reason or market.

## S2 - Change (additive; zero removals)

| Mode | Behaviour |
|---|---|
| off (DEFAULT; unset or any other word, `'1'` and `enforce` included) | v6.49.0, apart from the `SCRIPT_VERSION` string itself (receipt `tracker_version`, version tags in log lines). The collectors stay `None`; no line. |
| observe | Measures only. INFO lines after the HIST-CLOSE block, then the collectors are disarmed. Records, receipts, `_last_audit_stats`, saves, stdout and every other log record equal off. |

**How observe measures:**
- `_outcome_tally_begin()` arms two per-pass collectors at the top of `audit_active_records`. This is before the price fetch, so refusals made while absorbing the backend rows are seen.
- `_outcome_receipt` is the one place a due record's verdict is written. Before writing the unchanged receipt, it remembers that record's last `(symbol, state, reason)`.
- When `_outcome_quote_session` refuses a `source_quote` or `regular_market` quote as `quote_session_unknown`, it remembers the minutes between that quote and the last session close before it.
  - Only the first such quote per symbol is kept: the backend's, which is absorbed before any Yahoo fallback.
- The calendar name comes from `_outcome_tally_calendar`. It uses `_outcome_sessions`' own suffix rules but builds no exchange calendar, so a dead-feed audit pays nothing extra.

**The lines:**

```
[v6.52.0 OUTCOME-TALLY] mode=observe evaluated=N matured=M expired=E pending=P calendars=K
[v6.52.0 OUTCOME-TALLY] <CAL> n= matured= expired= pending= reasons=<reason:n,...> after_close_min=<0-15:a,15-20:b,20-60:c,60+:d>
...
```

- Calendars are ordered largest first, ties by name. Reasons are ordered by count, then name.
- Expired reasons carry an `expired:` prefix.
- `n`, `matured`, `expired` and `pending` count records; `after_close_min` counts distinct symbols whose final reason includes `quote_session_unknown`.
- `grep "OUTCOME-TALLY] XSAU"` gives the Saudi line alone.

**Env lane:** both tracker steps map the Variable (ENV PARITY).
- `track_performance.yml`: the `Run performance clock` step, on every scheduled audit.
- `daily_sync.yml`: the `Track Performance (record + audit)` step. It audits on a manual dispatch with `run_tracker=true`; it is inert on record-only schedules, because those never call `audit_active_records`.

**Additions:**
- functions: `_outcome_tally_mode`, `_outcome_tally_begin`, `_outcome_tally_end`, `_outcome_tally_stamp`, `_outcome_tally_calendar`, `_outcome_tally_lines`;
- module names: `_OUTCOME_TALLY_TAG`, `_OUTCOME_TALLY`, `_OUTCOME_TALLY_STAMPS`.

Zero functions or methods removed at any depth.

## S3 - Known limits

- **Production reason strings are joined.** With the Yahoo fallback on (the production default: neither workflow maps `TFB_TRACK_PRICE_FALLBACK`, so the code default `1` applies), a backend refusal reads e.g. `quote_session_unknown;quote_http_429` or `quote_session_unknown;quote_rate_limit_breaker`. Such refusals are still bucketed.
- **What `after_close_min` measures.** It counts distinct symbols, not records, and it uses the first refused quote per symbol. Only a quote refused as `quote_session_unknown` carries a stamp; another refusal (for example `quote_timestamp_unknown`) shows only its reason.
- **Calendar.** The calendar is the symbol's own exchange calendar, or `UNKNOWN` when the tracker has none.
- **A raising pass.** If a pass raises, the collectors stay armed but unread until the next audit call resets them (an empty audit disarms them too).
- **It measures; it does not fix.** The XSAU closing-auction allowance stays a separate, owner-gated build; its prototype is in the 26 Oct review.
- **Test lanes.** The full harness runs only in the advisory whole-suite job. Four guards run in the blocking lean lane through `tests/test_outcome_price_evidence.py`: three off spellings, and observe on a Saudi refusal with two records of one symbol. `ci.yml` is not touched.

## S4 - Tests

**`tests/test_tracker_outcome_tally.py`:**
- PASS 10/10 as a script; 18 passed under pytest; identical under three hash seeds.
- With no git base (`GIT_DIR=/nonexistent`, CI's depth-1 checkout) and stray tracker variables set: 17 passed, 1 skipped (T7). T3 then checks `OFF_GOLDEN = ffa28067f021c852`, the base digest.

| Case | What it pins |
|---|---|
| T0 | the parser takes the exact word `observe` only (`'1'`, `'true'`, `'on'`, `'enforce'`, `'observed'` -> off) |
| T1 | **observe, 14 records.** The XSAU records come first, so it is name order that puts XNYS first. Results: `evaluated=12 matured=3 pending=9 calendars=2`. **XNYS:** `n=6 matured=2 pending=4 reasons=quote_session_unknown:2,entry_price_invalid:1,price_unavailable:1 after_close_min=20-60:2`. **XSAU:** `n=6 matured=1 pending=5 reasons=quote_session_unknown:5 after_close_min=0-15:1,15-20:1,20-60:1,60+:1`. XNYS covers: two AAA.US records matured, BBB.US / CCC.US at 20:30Z, ZERO.US stamped but `entry_price_invalid`, and MISS.US unpriced. XSAU covers: two 2286.SR records at 12:10:24Z counted once in the buckets, 2222.SR 12:17Z, 1150.SR 12:45Z, 2010.SR 15:00Z, and 7010.SR at 12:00Z, which matures. A not-yet-due and an already-MATURED record are not counted. The tally equals `_last_audit_stats`. |
| T1b | the production lane, with the Yahoo fallback on: `reasons=quote_session_unknown:1,quote_session_unknown;quote_http_429:1 after_close_min=0-15:1,15-20:1`. The backend's 12:10:24Z stamp wins over a refused Yahoo `regular_market` quote at 12:20Z. |
| T2 | observe changes nothing else over the three T3 passes: records, receipts, stats, every other log record (level, logger, text) and stdout equal the off run |
| T3 | **off, nine spellings, three passes.** The passes are: mixed + HIST-CLOSE observe; a dead feed with the fallback on (PRICE-FEED ERROR, PRICE-FALLBACK, MATURE-FRESH); and an audit with no ACTIVE record. Records, receipts, stats, root-logger records (level, logger, text) and stdout equal the v6.49.0 copy, or `OFF_GOLDEN`. Only `SCRIPT_VERSION` and the fallback's wall-clock duration are normalised. A spy on `_outcome_receipt` proves no collector is armed at any receipt. |
| T4 | the lines are INFO, from logger PerfTracker, in one contiguous block after the MATURE-FRESH and HIST-CLOSE lines. Collectors are disarmed after a pass; after a raising pass, they are disarmed by the next audit call, even one with no ACTIVE record. |
| T5 | edges: 15 / 20 / 21 / 60 / 61 minutes give `0-15:1,15-20:1,20-60:2,60+:1`; a `regular_market` quote 16 min after close (past its 15 min slack) lands in 15-20; an unpriced record past grace counts as `expired:price_unavailable`; an unknown calendar reads `UNKNOWN`; nothing due means no line; only live quote kinds are stamped; the first stamp per symbol wins; the helpers never raise |
| T6 | both tracker steps map `${{ vars.TRACK_OUTCOME_TALLY \|\| 'off' }}` exactly once; header 1.0.8; SCRIPT_VERSION >= 6.52.0 in lockstep with docstring line 5 and the `verify_deployment` pin |
| T7 | ASCII added lines. The one exception is the inherited U+2013 on docstring line 5, which comes from the base line. Zero removals at any depth (Class.method included). On 6.52.0, exactly the six new functions are added. |
| T8 | `_outcome_tally_calendar` agrees with `_outcome_sessions` on 17 symbols (share-class dots, no suffix, `^`, `=X`, `-USD`, unknown suffix, digits, lower case) without building any calendar. A dead-feed observe audit builds no calendar that off would not. |

**Golden negative:** module under test = `git show e8337a16:scripts/track_performance.py`.
- Result: 9 failed, 1 passed (T3, off identity), as a script.
- T1 fails because no line is written.

**Blocking-lane guards** (`tests/test_outcome_price_evidence.py`): 81 passed. On the base tree the observe guard fails, and the three off guards pass.

**Mutations: 36/36 caught.**

| Mutation | Caught by |
|---|---|
| collectors always armed | T2, T3 |
| parser accepting `'1'` | T0, T3 |
| case-sensitive parser | T0 |
| lines not logged | T1, T1b, T2, T4, T5, T8 |
| lines at WARNING | T4 |
| lines moved above the HIST-CLOSE block | T4 |
| collectors not disarmed after a pass | T1, T2, T4, T5 |
| empty audit does not disarm | T4 |
| receipt hook removed | T1, T1b, T2, T4, T5, T8 |
| counted by symbol instead of record | T1 |
| buckets counted per record | T1 |
| exact-match reason (joined fallback reasons dropped) | T1b |
| no reason filter on the buckets | T1 |
| last stamp wins (Yahoo over backend) | T1b, T5 |
| stamp hook removed | T1, T1b, T5 |
| historical quote kinds stamped | T5 |
| 15 min edge strict | T5 |
| 20 min edge strict | T5 |
| 20 min edge widened to 30 | T1, T5 |
| 60 min edge strict | T5 |
| 60 min edge widened to 120 | T5 |
| reasons in name order | T1 |
| reasons by ascending count | T1 |
| calendar ties in insertion order | T1 |
| `expired:` prefix dropped | T5 |
| expired counted as pending | T5 |
| not-yet-due records counted | T1, T5 |
| calendar name via `_outcome_sessions` (builds calendars) | T8 |
| share-class rule dropped from the calendar name | T8 |
| off prints to stdout | T2, T3 |
| off logs on the root logger | T2, T3 |
| off arms the collectors during the pass | T3 |
| observe alters a receipt | T2 |
| version not bumped | T6 |
| docstring not bumped | T6 |
| a method removed | T7 |

## S5 - Review (before the PR)

A four-lens adversarial review ran (off identity, semantics on production-shaped inputs, evidence, harness). Every non-nit finding went to an independent refuter.

**Confirmed and fixed:**
- A Yahoo fallback quote overwrote the backend's after-close stamp. Fixed: first stamp wins.
- The harness never ran the fallback, which is on in production. Added T1b.
- Record versus symbol counting was not distinguished. T1 now has two records for one symbol.
- T3 saw one log line, ignored levels, blanked every version tag, and did not capture stdout or the root logger. It now covers three passes, captures levels, loggers and stdout, normalises only `SCRIPT_VERSION`, and adds a receipt spy.
- The bucket edges were pinned on one side only. Both sides are now pinned.
- Reason order and the calendar tie-break were not pinned. Both are now pinned.
- T7 ignored methods. It now compares qualified names at any depth.

**Nits fixed:**
- The output is now one totals line plus one line per calendar, instead of one 7-27 KB line.
- The calendar name no longer builds exchange calendars (about 13 s on a dead feed).
- The empty-audit return now disarms the collectors.
- The wording now says "apart from the `SCRIPT_VERSION` string".
- T1's symbol list is corrected.
- The T7 exemption is stated.

**Refuted:**
- "Every XSAU refusal reads 0-15": the line reports what it measures.
- "Notes already hold the reasons": the claim is about printed output.
- "T2 cannot see saves or the `_Run_Log` inputs": the refuter judged it immaterial, because T2 compares the records, receipts and stats those surfaces are built from.

## S6 - Operator steps (nothing is armed here)

1. Merge. No Render deploy is needed: the tracker runs in GitHub Actions.
2. Set the repository Variable `TRACK_OUTCOME_TALLY=observe`.
3. Read the `[v6.52.0 OUTCOME-TALLY]` lines on the next audits (the clock's two daily slots).
   - **For the Saudi question:** look at `grep "OUTCOME-TALLY] XSAU"`. If it shows `quote_session_unknown` (alone or joined with a fallback reason) with most symbols in `0-15` / `15-20`, the closing-auction hypothesis is confirmed, and the XSAU-only allowance is the next build.
   - **For the US weekday question:** compare the XNYS line between a slot that ran before 20:00Z and one that ran after.

**Rollback:** unset the Variable, or set it to `off`.

## S7 - Not done here

- No acceptance change: the XSAU closing-auction allowance is not built.
- No change to the fallback's reason join. A joined reason also stops unpriced expiry; that is pre-existing and noted for the owner.
- No cron change, and no `ci.yml` change.
- No change to `_last_audit_stats`, `[PERF-VERDICT]` or `_Run_Log`.
