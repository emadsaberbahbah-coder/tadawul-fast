# TFB Commit Sheet - track_performance v6.47.0 [HISTORICAL TARGET-SESSION CLOSE]

Date: 2026-10-21 (daily improvement, one item, priority (a): a published outcome number is missing, for good).

- **Base:** main `df99bcf3` since this branch merged main on 2026-10-22 (see 'Merge of main' below). `scripts/track_performance.py` 6.46.0 (sha256 `f1f48ea3d0ff153c...`, 11,109 lines). Result: 6.47.0 (sha256 `3280d929c8d6a7c3...`, 11,560 lines). When the PR opened, the base was main `55b47f24` with 6.45.0 (sha256 `f58f2dafb2759b5d...`, 10,929 lines), and the result was sha256 `799e0aa74d2a34ee...`, 11376 lines.
- **Why 6.47.0:** when this PR opened, 6.45.1, 6.45.2 and 6.46.0 were claimed by open #785, #795 and #809. 6.46.0 has since landed on main through #816, which carries #809's learning-metric fixes. This branch merged it, so 6.47.0 now follows main directly. #785, #795 and #809 are still open; whichever of them lands after this PR re-numbers.
- **Lane:** GitHub Actions (`track_performance.yml`, the Performance Clock). Nothing in Render, the backend or Apps Script changes.
- **Gate:** repository Variable `TRACK_HIST_TARGET_CLOSE`, mapped in the 'Run performance clock' step with default `'off'`. The words `observe` and `enforce` are read case- and space-insensitively; any other value, `1` included, is off.
- **Off:** maturation is v6.46.0 (main `df99bcf3`) byte-identical apart from the version string; before the merge it was v6.45.0, proven the same way. This is proven against the base copy: every record field, the Notes receipt (with `tracker_version` normalised), `_last_audit_stats` and the absence of any pull.

## Why

A record matures only on a quote from its exact target session (v6.43.0, `_outcome_target_session`: "never use a later session"). Once that session has passed, every later audit holds a later session's quote, the reason is `quote_outside_target_session`, and the record stays ACTIVE with no realized ROI for good:

- only `price_unavailable` can EXPIRE;
- `_outcome_quote_session` refuses any older quote as `quote_session_stale`.

Live evidence:

```
TFB Acceptance job 119219926330 (2026-10-21)   D10-3b ... FAIL 3208 records=23529 active=11606 matured=11791
Performance Clock job 118841772740              Matured: 11791   (flat since about 10 Oct)
```

#805 (`TRACK_SCHEDULED_AUDIT=1`) only stops new losses when a run lands inside the target session's window. Its own commit sheet says the 12-20 Oct target sessions "stay unmeasured". The measured run start times (14:26-20:49Z) also miss the US close on some weekdays, so even after arming, US sessions keep being lost and the matured cohort is skewed by market.

`docs/reviews/witnessed-outcomes-20261009.md:88-92` names this follow-up: "a separately witnessed historical target-close price path. Corporate-action endpoints must remain bound to that same time."

## What changed

`scripts/track_performance.py` 6.45.0 -> 6.47.0 (6.46.0 -> 6.47.0 against main since the merge). A dated WHY block is added above `SCRIPT_VERSION`, and every prior block is kept verbatim. Since the merge it sits above #816's 6.46.0 block, newest first.

**Trigger.** In `audit_active_records`, the path runs only when all of these hold:
- the gate is observe or enforce;
- a current witnessed quote exists;
- after the entry-price and ROI checks, the reason is exactly `quote_outside_target_session`. The context of that reason already names the target session and its close.

`quote_session_stale` never triggers, because it carries no target-session context.

**`_hist_close_evidence`** (new) refuses with `hist_close_context_unknown` unless the current quote's session is LATER than the target session and the target close is in the past. It then:
1. refuses when the `_Corporate_Actions` ledger holds a confirmed action dated after the target session;
2. reads daily bars through `_hist_close_bars` (its own per-run cache and budget);
3. takes the close of exactly that session via `_hist_target_close`.

**`_hist_close_pull`** (new) makes ONE yfinance call: `history(period="1y", interval="1d", auto_adjust=False, actions=True, raise_errors=True)`, returning `{date, close, split}` per bar.
- `close` is Yahoo's Close. It is split back-adjusted but not dividend-adjusted, so a ROI against the raw entry print stays a price ROI.
- An empty answer returns `[]`. This covers Yahoo answering with no rows: `YFPricesMissingError` for an unknown or delisted symbol, an empty range, or a Yahoo error body.
- A failed call raises, so the caller can count it separately: a rate limit, a network error, a timeout, an outage page, or missing yfinance.
- `raise_errors=True` is what makes the failure raise. With the default, yfinance 0.2.66 logs every failure except a 429 and returns an empty frame, so an outage would read as "no bars" and never trip the breaker. Offline probe over the real library (stubbed transport, 0 requests sent), 7 symbols:

  | Mode | Before (default) | After |
  | --- | --- | --- |
  | connection error, timeout, 503, outage page | `ok=7 failed=0 breaker=closed` | `failed=3 breaker=open deferred=4` |
  | 429 | breaker open after 1 | breaker open after 1 |
  | delisted or a Yahoo status body | `ok=7` (empty) | `ok=7` (empty) |

**`_hist_close_bars`** (new) wraps the pull with its own per-run cache and budget.
- It counts `ok` and `failed` separately and keeps `first_error` (the exception class).
- A failed symbol is not retried in the same pass.
- After 3 failed pulls in a row, or any rate-limit error, a breaker opens and no further pull is made that pass. The remaining records are deferred, not refused.
- `_CA_BARS_CACHE` (ADJUSTED bars for the CA-GUARD) is never touched. The one contact with `_CA_BUDGET` is a credit: each pull's wall-clock time is added back to the CA-GUARD's window start (`t0`), so these pulls never spend the CA-GUARD's 240 s.

**`_hist_target_close`** (new, pure) refuses, and the record stays ACTIVE, on:

| Condition | Reason |
| --- | --- |
| no bars | `hist_close_unavailable` |
| no bar dated the target session | `hist_close_bar_missing` |
| more than one distinct close that day | `hist_close_bar_conflict` |
| a close that is not finite or not positive | `hist_close_invalid` |
| a Stock Splits event, or an unreadable split cell, on the target-session bar or any later bar (yfinance attaches an event dated on a day with no bar to the PREVIOUS bar, which can be the target session's own) | `hist_close_post_target_split` |
| a close 10x or more away from the current witnessed quote (currency subunit or wrong instrument) | `hist_close_scale_mismatch` |
| the pull failed | `hist_close_pull_error` |
| the breaker is open | `hist_close_breaker_open` (deferred) |
| the pull budget is spent | `hist_close_budget_exhausted` (deferred, retried next pass) |

**Modes:**
- **observe:** records, Notes, `_last_audit_stats` and every sheet are unchanged. The candidates are judged AFTER the loop, so observe's pulls never interleave with the CA-GUARD's own pulls (clock or Yahoo quota) in that pass. One stdout line is logged per audit pass:
  `[HIST-CLOSE v6.47.0] mode=observe candidates=N witnessed=K (1W:a 2W:b 1M:c 3M:d) matured=0 refused=M (reason:count ...) pulls=P ok=A failed=F [first_error=<class>] breaker=closed|open deferred=D`
- **enforce:**
  - The evidence becomes `OutcomePrice(quote_kind="historical_daily_close", provider="yfinance-history", quote_asof=<target-session close>)`.
  - `current_price` and `unrealized_roi` are recomputed from the bar close BEFORE the CA-GUARD reads them, so nothing matures on today's price.
  - The record then continues down the UNCHANGED path: the CA-GUARD (ledger factor and adjusted series both bound to the same target session), WIN/LOSS/BREAKEVEN, and the `[OUTCOME-WITNESS v1]` receipt.
  - The receipt also carries `hist_bar_date`, `hist_provider`, `current_quote_session/price/asof` (the quote it replaced) and `quote_session` set to the target session.
  - A refused record keeps its v6.45.0 reason and adds `hist_close_reason`.
  - A substituted record that the unchanged path then leaves unmatured (CA verification deferred, a CA refusal, or CORP_ACTION_SUSPECT) gets its live Current Price and Unrealized ROI back after the loop. The sheet never shows the old close as the live mark.
- **Budgets:**
  - `TRACK_HIST_CLOSE_BUDGET`: distinct symbol pulls per pass, default 60, `0` = unlimited. The 60 are a rotating daily window (`_hist_close_admit`) over the pass's candidate symbols in a stable hash order: window start = (day ordinal x budget) mod N. Every candidate symbol is reached within ceil(N / budget) days, even when some are refused on every pass. A fixed sheet order would spend the budget on the same first symbols for good (Codex P2 on this PR). Enforce finds its candidates with a pure pre-pass (`_hist_close_candidate`, the loop's own tests in the loop's order); observe uses the list it collects in the loop.
  - `TRACK_HIST_CLOSE_TIME_SEC`: default 240, `0` = unlimited.
- **No new EXPIRE path.**
- **Totals:** functions 0 removed, 10 added (`_hist_target_close_mode`, `_hist_close_budget`, `_hist_close_time_sec`, `_hist_close_pull`, `_hist_close_bars`, `_hist_target_close`, `_hist_close_evidence`, `_hist_close_judge`, `_hist_close_admit`, `_hist_close_candidate`), plus 3 module dicts and two constants (`_HIST_CLOSE_BREAKER_AFTER`, `_HIST_CLOSE_EMPTY_ERRORS`). Added lines are ASCII; the only exception is the docstring title, which keeps its existing en dash and changes only its version.
- **Embedded self-test:** unchanged at 22 cases, PASS 22/22.

`.github/workflows/track_performance.yml` 1.0.4:
- a dated WHY block above 1.0.3;
- the 'Run performance clock' step maps `TRACK_HIST_TARGET_CLOSE` (`'off'`), `TRACK_HIST_CLOSE_BUDGET` (`'60'`) and `TRACK_HIST_CLOSE_TIME_SEC` (`'240'`), which equal the script's own defaults.

`daily_sync.yml` is not changed:
- its tracker step audits only on manual dispatch;
- a maturity written by the clock lane is not undone by its record-only save;
- the parity tests cover only keys both lanes map (`TRACK_SCHEDULED_AUDIT` is already clock-only).

`scripts/verify_deployment.py`: the manifest pin `track_performance` goes 6.45.0 -> 6.47.0 (6.46.0 -> 6.47.0 against main, replacing #816's pin; #816's other pins are kept). This is a pin only: `SCRIPT_VERSION` stays 1.0.61, following main's own 2026-10-21 note, which keeps it clear of the open verifier bumps in #778, #779, #785, #795 and #809.

## Merge of main (2026-10-22)

main moved after this PR opened: #813, #815 and #816 (merged 2026-10-21 22:23Z, main `df99bcf3`). #816 shipped `track_performance` 6.46.0 (the direction-aware learning metrics carried from #809), its `verify_deployment.py` pins and `ci.yml` 1.1.31. This branch merged `origin/main` with a normal merge commit (no rebase, no force-push).

| File | Conflict | Resolution |
| --- | --- | --- |
| `scripts/track_performance.py` | docstring title (v6.47.0 vs v6.46.0) | v6.47.0 |
| `scripts/track_performance.py` | WHY blocks and `SCRIPT_VERSION` | both blocks kept, newest first: the 6.47.0 block (plus a 3-line MERGED note), a separator, #816's 6.46.0 block verbatim, then `SCRIPT_VERSION = "6.47.0"` |
| `scripts/verify_deployment.py` | `track_performance` pin (6.47.0 vs 6.46.0) | 6.47.0, plus one comment line; #816's other pins kept |

Merged without conflict: `ci.yml` is main's 1.1.31 (this PR never edited it, so there was no enrolment to carry), and `.github/workflows/track_performance.yml` keeps this PR's 1.0.4. The branch's diff against main is still exactly this PR's five files.

`tests/test_tracker_hist_target_close.py` is re-anchored on the new base: `BASE_SHA` 55b47f24 -> df99bcf3, the base version that N2 and N4 expect 6.45.0 -> 6.46.0, and N1 renamed `test_n1_off_is_byte_identical_to_v6_46_0`. No assertion is loosened. Before the re-anchor, N4 failed on the merged tree because its exact diff against 55b47f24 also saw the 50 lines #816 replaced. Against df99bcf3 its removals are exactly the two version lines again.

```
AST function names                     base 269 | main 274 | PR 279 | merged 284 (= 269 + 5 + 10); missing vs main 0, vs PR 0
3-way body check                       every def equals the side that changed it; none changed on both sides
lines of main not in the merged file   2 (the 6.46.0 title and SCRIPT_VERSION lines)
default off, static                    audit_active_records minus the _hist_* inits and the `_hist_mode != 'off'` blocks is AST-identical to main's
N1, off == main df99bcf3               PASS (also PASS with the unmodified test and TFB_TEST_TP_BASE=<main copy>)
pytest tests/test_tracker_hist_target_close.py   25 passed (requirements.txt env); numpy+pytest env 23 passed, 2 skipped (pandas, yfinance absent)
python3 tests/test_tracker_hist_target_close.py (x2)   PASS 25/25 digest 04e8597958ee9c30 (the N1 rename moves the digest)
golden negative: TFB_TEST_TP_DELIVERED=<main df99bcf3 copy>   23 failed, 1 passed (N1), 1 skipped (N4)
tracker-related suites (19 files)      merged 456 passed, 1 skipped | main 431 passed, 1 skipped (+25 = this suite)
embedded self-test _track_selftest_    PASS 22/22 (main PASS 22/22)
verify_deployment.py --selftests       track_performance 6.47.0 == pin, OK; every other module, script, selftest and flag identical to main
whole suite, pytest -q tests (requirements.txt env)   merged 5507 passed, 37 skipped, 0 failed | main 5482 passed, 37 skipped, 0 failed (+25 = this suite)
ci.yml lean-unit pytest steps (numpy + pytest)   LEAN FAILS: 0 (decision-layer suites 3795 passed, 26 skipped; main identical)
```

## Measured results (pre-merge tree, base 55b47f24)

```
pytest tests/test_tracker_hist_target_close.py (repo checkout, base via git show)   25 passed
python3 tests/test_tracker_hist_target_close.py   (x2)                    PASS 25/25 digest a67bec95d2b71769
golden negative: TFB_TEST_TP_DELIVERED=<main 55b47f24 copy>             23 failed, 1 passed, 1 skipped
   (N1 off == base passes on base, as it should; N4 skips by design, its exact
    diff checks apply to 6.47.0 only)
mutations: 29 of 30 caught; the miss is equivalent (list below)
pytest test_outcome_price_evidence + tracker/workflow/manifest + tracker unit suites   315 passed
python3 scripts/test_maturation.py                                        14/14
embedded self-test _track_selftest_                                       PASS 22/22 (unchanged)
compileall main.py core routes scripts tests                             ok
ci.yml lean list (every step)                                             LEAN FAILS: 0 (decision-layer suites 3772 passed, 19 skipped), on the final pre-merge tree
```

**Mutations (`-x` per mutation), 29 of 30 caught:**
1. the gate ignored;
2. ROI not recomputed (matures on today's price);
3. the post-target split check removed;
4. the split check back to strictly after the target session;
5. the CA cache shared;
6. the budget ignored;
7. a trigger on any reason when a quote exists;
8. observe mutating records;
9. observe judged inside the loop;
10. no CA clock credit;
11. no live-mark restore;
12. a failed pull counted as an empty answer;
13. the breaker disabled;
14. the scale check removed;
15. the ledger check removed;
16. `quote_session` not rebound in the receipt;
17. a bar conflict ignored;
18. the nearest bar used instead of the exact one;
19. the refusal reason dropped from the receipt;
20. `auto_adjust=True` in the pull;
21. observe bumping the CA deferred count;
22. an extra log line when off;
23. `raise_errors` dropped;
24. the empty-answer mapping dropped;
25. the wall-clock budget leg disabled;
26. a trigger widened to `target_session_not_complete`;
27. the admission window ignored;
28. a window that never rotates;
29. a pre-pass that admits every active record.

Missed, and equivalent: the pre-pass without its target-time test. A future target can never yield `quote_outside_target_session`, because `_outcome_target_session` returns `target_session_not_complete` first. The test is kept so the pre-pass mirrors the loop's order. Also equivalent inside the loop: the count leg of `_hist_close_bars`, because the window is never larger than the budget. H14b pins that leg on a direct call.

Mutations 21 and 22 were survivors of the first harness and were found by the review below. A trigger that also fires on `quote_session_stale` is an equivalent mutation: that reason never comes with evidence, so the branch cannot be reached.

## Adversarial review (before the PR)

A three-angle review (maturity semantics, byte identity, ops and CI) ran with an independent refute step for each finding. It confirmed six defects in the first cut, and all are fixed in this PR:

| # | Finding | Fix | Case |
| --- | --- | --- | --- |
| F0 | yfinance 0.2.66 attaches a split dated on a no-bar day (suspension, missing bar, weekend) to the PREVIOUS bar. That can be the target session's own bar, whose Close is already back-adjusted. Reproduced: entry 100, true close 104, a 1.1 bonus dated on the next no-bar day; the record matured as a LOSS at -5.45% instead of a WIN at +4%. | The split check covers bars on or after the target session. | H4 "event merged onto the target bar"; H12 end to end through the real pull |
| F1 | A substituted record that the CA path left PENDING kept the old close as its Current Price and Unrealized ROI. | The live mark is restored after the loop for every unmatured substituted record. | H11 (deferred and CORP_ACTION_SUSPECT) |
| F2 | Under observe, in-loop pulls could use up the CA-GUARD's 240 s wall-clock window, deferring records that off would mature. Observe was not a pure measurement. | Observe judges after the loop; each pull's time is credited back to the CA window. | H9 (observe == off with a 300 s pull; enforce keeps the later CA-verified record maturing; the pull order is asserted) |
| F3 | The harness never ran observe on the refusal path, never compared stats, and missed INFO log lines under pytest (root logger at WARNING). | H3b added; N1 compares stats and logs; the harness pins the tracker logger to INFO. | H3b, N1 |
| F4 | Failed pulls were silent, counted as pulls, and nothing stopped a throttled runner. | `ok`/`failed`/`first_error` are counted; a breaker opens after 3 failures or a rate-limit error; the read-back line carries them. | H10 |
| F5 | N3/N4 pinned 6.47.0 exactly, so the next tracker bump would break them. | Floors: a regex for the version line; N4 runs only on 6.47.0. | N3, N4 |

A second review, of the fixes themselves, ran the same way and confirmed four more items. All are fixed:

| # | Finding | Fix | Case |
| --- | --- | --- | --- |
| R1 | F4 was incomplete. With yfinance's default `raise_errors=False`, an outage (connection error, timeout, 5xx, outage page) comes back as an empty frame, counts as `ok`, and never trips the breaker. | `raise_errors=True`, with `YFPricesMissingError` mapped to an empty answer. | H13 (the real yfinance 0.2.66 stack over a stubbed transport), H12 |
| R2 | H5b never reached `target_session_not_complete`: its record matured on the live quote, so it passed trivially. | The harness takes a clock override; H5b asserts the reason, PENDING and `candidates == 0`. | H5b, mutation 26 |
| R3 | The `TRACK_HIST_CLOSE_TIME_SEC` leg was untested. | H6c added (a 150 s pull against a 100 s window), plus reader rows in H8. | H6c, mutation 25 |

It refuted three further claims:
- the CA-GUARD's pulls spending the hist window under enforce (only deferral, by design);
- the restore writing today's mark into an EXPIRED CORP_ACTION_SUSPECT row (the existing live-maturity behaviour);
- permanent refusals starving the budget (needs 60 or more of them).

After the PR opened, Codex (P2) reported that a fixed sheet order lets symbols refused on every pass take the whole budget, and that observe re-pulls the same first 60 symbols every pass. I traced it: a whole venue refused for one reason (no Yahoo bars, a unit mismatch) can exceed 60 symbols. It is fixed with the rotating daily window above, and covered by H14, H14b and H15 and by mutations 27-29.

The first review's four refuted claims:
- an EXPIRE path for entries over a year old (no such record exists for about 8 months);
- budget starvation by symbols refused on every pass (needs 60 or more such symbols);
- load->save window growth (save_records does not drop concurrent appends);
- a missing warning on record-only runs (the documented steps say to arm it with an audit pass).

## Harness cases

`tests/test_tracker_hist_target_close.py` drives the REAL `audit_active_records` with the lean calendar fixture from `tests/test_outcome_price_evidence.py`, an AsyncMock backend, an empty ledger index and fixture bars. No network.

- **H1:** enforce matures on the target-session close: WIN 104, LOSS 95, BREAKEVEN 100 against entry 100, while today's quote is 110. Checked: the ROI, `current_price`, every receipt key, one pull, and the read-back line.
- **H2:** off and 7 non-word spellings: no pull, no log line, no receipt key, read-back untouched.
- **H3:** observe gives the same record state as off, with one pull and one log line. H3b: observe on every refusal fixture equals off in record state, stats and log lines (bar its one line).
- **H4:** 13 refusal fixtures, including an event merged onto the target bar, each also at grace 0. The record stays ACTIVE with its v6.45.0 reason plus `hist_close_reason`, and is never EXPIRED. H4b: a confirmed ledger action after the target session refuses.
- **H5:** `price_unavailable`, `quote_session_stale` and `quote_before_target_close` never pull and equal off. H5b: with the clock 1 minute before the close, a session that has not completed is PENDING `target_session_not_complete`, with no candidate and no pull.
- **H6:** three horizons of one symbol take one pull; `_CA_BARS_CACHE` is untouched. H6b: a spent count budget defers the second symbol and never expires it. H6c: a spent wall-clock budget does the same.
- **H7:** a 2:1 split between entry and target (print -48%) goes through the unchanged CA-GUARD. `_ca_adjusted_roi` is called with the target session, and the record matures on the adjusted +4.0 with a CA-ADJUST note.
- **H8:** the pure helper table and the mode and budget readers.
- **H9:** a 300 s hist pull between two CA-verified records. Observe equals off (records and stats), with the pull after all CA work. Enforce matures all three with `ca_deferred=0`.
- **H10:** 3 failures open the breaker, and the 4th record is deferred. A rate-limit error opens it at once. A failed symbol is not retried, and one failure does not stop the next symbol.
- **H11:** a CA-deferred substitution, and a CORP_ACTION_SUSPECT one, both keep the live 150 mark instead of the 130 close.
- **H12:** the REAL `_hist_close_pull` over a fake yfinance frame (pandas): the exact call kwargs (`raise_errors=True`), the bare US ticker, parsing, an empty frame, a raised error, a `YFPricesMissingError` read as an empty answer, and end to end both an event merged onto the target bar (refused) and a clean frame (matures +4.0).
- **H14:** three symbols refused on every pass, budget 1. Across Thu, Fri and Tue the window moves, so each symbol is pulled exactly once, in observe and in enforce. The pure window table is checked too. H14b: a non-candidate beside a candidate never takes the window (two consecutive days), and the count leg still defers on a direct call.
- **H15:** the enforce pre-pass picks exactly the records the loop marks `quote_outside_target_session` on the mixed fixture plus a not-yet-due record.
- **H13:** the REAL yfinance 0.2.66 stack over a stubbed transport, with no network. A connection error, a timeout and the outage page each give 3 failed pulls, then an open breaker that defers the rest; a delisted answer gives 5 empty answers.
- **N1:** off (5 spellings) equals the v6.46.0 base copy (main `df99bcf3`; v6.45.0 before the merge) on a mixed fixture: record states, receipts, `_last_audit_stats` and INFO log lines. Observe equals it too, on witnessed and on missing-bar fixtures.
- **N2:** golden negative. The base copy under `TRACK_HIST_TARGET_CLOSE=enforce` leaves the H1 record ACTIVE with `quote_outside_target_session`, including past grace.
- **N3:** the workflow mapping (step-scoped, defaults equal to the script's), no mapping in daily_sync, the version floor (>= 6.47.0), the docstring version, the verifier pin, and the WHY block order (floors, no exact pin).
- **N4:** on 6.47.0 only, the added lines are ASCII, the only removals are the two version lines, and the function inventory is +10 / -0.

## Operator steps (owner) - nothing is armed by this PR

1. Merge. No deploy is needed; this runs in GitHub Actions.
2. **Proposed (owner decision):** set Variable `TRACK_HIST_TARGET_CLOSE = observe`, then run one audit pass. Use `track_performance.yml` with `run_audit=true`, or `TRACK_SCHEDULED_AUDIT=1` together with `TRACK_DEDUP_MATURED=observe` from #805.
3. Read the `[HIST-CLOSE v6.47.0]` line in the run log:
   - `witnessed` should be most of `candidates`;
   - `refused` names why the rest were refused;
   - `ok` and `failed` show whether yfinance answers from the runner (the CA-GUARD's own Yahoo fallback has seen HTTP 429), with `first_error` naming the class;
   - `breaker=open` means pulling stopped for that pass. Do not raise the budget then; wait for a later pass.
4. **Proposed (owner decision):** set it to `enforce`. Then read back:
   - matured rises above 11,791;
   - acceptance D10-3b's overdue count falls from 3,208;
   - the recovered receipts carry `"quote_kind":"historical_daily_close"` and `hist_bar_date`.
5. **Optional:** raise `TRACK_HIST_CLOSE_BUDGET` to clear the backlog faster. At the default of 60 symbols per pass it takes several passes.
6. **Rollback:** unset the Variable. Records already matured under enforce keep their receipts, and `hist_bar_date` marks them.

## Known limits

- **Coverage:** a record whose symbol has no current witnessed quote in that pass is not a candidate. With audits armed, those past the 5-day grace EXPIRE as UNPRICED under the existing behaviour, whether or not this lands.
- **Large moves:** a recovered record whose |print ROI| is 15pp or more goes through the existing CA-GUARD verification. As for any live maturity, it can mature on the adjusted series, stay PENDING, or EXPIRE as CORP_ACTION_SUSPECT. That is the existing path; this PR adds none.
- **Yahoo data error:** if Yahoo back-adjusts a Close but leaves the split out of its event list, only a confirmed `_Corporate_Actions` row (or, for a move of 15pp or more, the CA-GUARD) catches it. That is a provider data error and outside this code's reach; observe's refusal counts are the place to watch for it.
- **`maturity_date`** is the audit time, as for every maturity. This only affects plot ordering.
- **Provider:** the close comes from Yahoo through yfinance. It is a different provider from the current-quote witness but is the exact session's close. Observe is the step that shows whether the runner can reach it.
- **CI coverage:** the harness is not enrolled in the blocking lean list. `ci.yml` is edited by open #778, so enrolling here would collide with its version lines; enrolling is proposed once #778 lands. The advisory whole-suite job collects it. N1, N2 and N4 need the base copy (`git show df99bcf3` since the merge, `55b47f24` before it, or `TFB_TEST_TP_BASE`) and skip in a shallow checkout. They were run locally, as recorded above.

## Erratum (2026-10-24)

The **Coverage** bullet above says "those past the 5-day grace EXPIRE as UNPRICED under the existing behaviour". That is not what happens in production.

- **Why the expiry never fires:**
  - With the default `TFB_TRACK_PRICE_FALLBACK` on, the Yahoo fallback joins its own reason onto every symbol it leaves unpriced (`scripts/track_performance.py` around L9917-9919). `_yahoo_chart_fallback_prices` sets a reason on every exit.
  - The grace expiry fires only on the exact reason `price_unavailable` (around L10025).
  - The only expiry test forces the fallback off (`tests/test_outcome_price_evidence.py` autouse fixture).
- **Production logs:** every audited Performance Clock run since #742 (2026-10-09) logs `expired-unpriced=0`, while pending rose to 3,956 (job 120614987176, run 40012156458). Those records stay ACTIVE.
- **Status:** this is the behaviour main has today, not something v6.47.0 changed. `docs/reviews/witnessed-outcomes-20261009.md` (L62-65) lets only an actually absent price expire. Whether a rate-limited or breaker-skipped fallback miss counts as absent is an owner policy decision. No code change is proposed until it is made (daily review of 2026-10-24).
