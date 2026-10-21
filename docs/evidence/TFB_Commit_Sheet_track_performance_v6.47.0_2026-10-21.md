# TFB Commit Sheet - track_performance v6.47.0 [HISTORICAL TARGET-SESSION CLOSE]

Date: 2026-10-21 (daily improvement, one item, priority (a): a published outcome number is missing, for good).

- **Base:** main `55b47f24`. `scripts/track_performance.py` 6.45.0 (sha256 `f58f2dafb2759b5d...`, 10,929 lines). Result: 6.47.0 (sha256 `fb76ad22568c4e09...`, 11229 lines).
- **Why 6.47.0:** 6.45.1, 6.45.2 and 6.46.0 are claimed by open #785, #795 and #809. Whichever of these lands second re-numbers.
- **Lane:** GitHub Actions (`track_performance.yml`, the Performance Clock). Nothing in Render, the backend or Apps Script changes.
- **Gate:** repository Variable `TRACK_HIST_TARGET_CLOSE`, mapped in the 'Run performance clock' step with default `'off'`. The words `observe` and `enforce` are read case- and space-insensitively; any other value, `1` included, is off.
- **Off:** maturation is v6.45.0 byte-identical apart from the version string. This is proven against the base copy: every record field, the Notes receipt (with `tracker_version` normalised), `_last_audit_stats` and the absence of any pull.

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

`scripts/track_performance.py` 6.45.0 -> 6.47.0. A dated WHY block is added above `SCRIPT_VERSION`, and every prior block is kept verbatim.

**Trigger.** In `audit_active_records`, the path runs only when all of these hold:
- the gate is observe or enforce;
- a current witnessed quote exists;
- after the entry-price and ROI checks, the reason is exactly `quote_outside_target_session`. The context of that reason already names the target session and its close.

`quote_session_stale` never triggers, because it carries no target-session context.

**`_hist_close_evidence`** (new) refuses with `hist_close_context_unknown` unless the current quote's session is LATER than the target session and the target close is in the past. It then:
1. refuses when the `_Corporate_Actions` ledger holds a confirmed action dated after the target session;
2. reads daily bars through `_hist_close_bars` (its own per-run cache and budget);
3. takes the close of exactly that session via `_hist_target_close`.

**`_hist_close_pull`** (new) makes ONE yfinance call: `history(period="1y", interval="1d", auto_adjust=False, actions=True)`, returning `{date, close, split}` per bar.
- `close` is Yahoo's Close. It is split back-adjusted but not dividend-adjusted, so a ROI against the raw entry print stays a price ROI.
- `_CA_BARS_CACHE` and `_CA_BUDGET`, which hold ADJUSTED bars for the CA-GUARD, are never touched.

**`_hist_target_close`** (new, pure) refuses, and the record stays ACTIVE, on:

| Condition | Reason |
| --- | --- |
| no bars | `hist_close_unavailable` |
| no bar dated the target session | `hist_close_bar_missing` |
| more than one distinct close that day | `hist_close_bar_conflict` |
| a close that is not finite or not positive | `hist_close_invalid` |
| a Stock Splits event, or an unreadable split cell, dated after the target session | `hist_close_post_target_split` |
| a close 10x or more away from the current witnessed quote (currency subunit or wrong instrument) | `hist_close_scale_mismatch` |
| the pull budget is spent | `hist_close_budget_exhausted` (deferred, retried next pass) |

**Modes:**
- **observe:** records, Notes and every sheet are unchanged. One stdout line is logged per audit pass:
  `[HIST-CLOSE v6.47.0] mode=observe candidates=N witnessed=K (1W:a 2W:b 1M:c 3M:d) matured=0 refused=M (reason:count ...) pulls=P deferred=D`
- **enforce:**
  - The evidence becomes `OutcomePrice(quote_kind="historical_daily_close", provider="yfinance-history", quote_asof=<target-session close>)`.
  - `current_price` and `unrealized_roi` are recomputed from the bar close BEFORE the CA-GUARD reads them, so nothing matures on today's price.
  - The record then continues down the UNCHANGED path: the CA-GUARD (ledger factor and adjusted series both bound to the same target session), WIN/LOSS/BREAKEVEN, and the `[OUTCOME-WITNESS v1]` receipt.
  - The receipt also carries `hist_bar_date`, `hist_provider`, `current_quote_session/price/asof` (the quote it replaced) and `quote_session` set to the target session.
  - A refused record keeps its v6.45.0 reason and adds `hist_close_reason`.
- **Budgets:**
  - `TRACK_HIST_CLOSE_BUDGET`: distinct symbol pulls per pass, default 60, `0` = unlimited.
  - `TRACK_HIST_CLOSE_TIME_SEC`: default 240, `0` = unlimited.
- **No new EXPIRE path.**
- **Totals:** functions 0 removed, 7 added (`_hist_target_close_mode`, `_hist_close_budget`, `_hist_close_time_sec`, `_hist_close_pull`, `_hist_close_bars`, `_hist_target_close`, `_hist_close_evidence`), plus 3 module dicts. Added lines are ASCII; the only exception is the docstring title, which keeps its existing en dash and changes only its version.
- **Embedded self-test:** unchanged at 22 cases, PASS 22/22.

`.github/workflows/track_performance.yml` 1.0.4:
- a dated WHY block above 1.0.3;
- the 'Run performance clock' step maps `TRACK_HIST_TARGET_CLOSE` (`'off'`), `TRACK_HIST_CLOSE_BUDGET` (`'60'`) and `TRACK_HIST_CLOSE_TIME_SEC` (`'240'`), which equal the script's own defaults.

`daily_sync.yml` is not changed:
- its tracker step audits only on manual dispatch;
- a maturity written by the clock lane is not undone by its record-only save;
- the parity tests cover only keys both lanes map (`TRACK_SCHEDULED_AUDIT` is already clock-only).

`scripts/verify_deployment.py`: the manifest pin `track_performance` goes 6.45.0 -> 6.47.0. This is a pin only: `SCRIPT_VERSION` stays 1.0.61, following main's own 2026-10-21 note, which keeps it clear of the open verifier bumps in #778, #779, #785, #795 and #809.

## Measured results

```
pytest tests/test_tracker_hist_target_close.py (repo checkout, base via git show)   15 passed
python3 tests/test_tracker_hist_target_close.py   (x2)                    PASS 15/15 digest 7cedac026b7390d7
golden negative: TFB_TEST_TP_DELIVERED=<main 55b47f24 copy>             13 failed, 2 passed
   (passing on base, as they should: N1 off == base, H5b no pull before the target close)
mutations: 13 of 14 caught; the miss is equivalent (see below)
pytest test_outcome_price_evidence + tracker/workflow/manifest suites   268 passed
pytest tracker unit suites (test_tp_*, test_track*, test_tracker*)        93 passed
python3 scripts/test_maturation.py                                        14/14
embedded self-test _track_selftest_                                       PASS 22/22 (unchanged)
compileall main.py core routes scripts tests                             ok
ci.yml lean list (every step)                                             all steps OK, LEAN FAILS: 0
```

**Mutations (`-x` per mutation):**
- Caught:
  - the gate ignored;
  - ROI not recomputed (matures on today's price);
  - a post-target split allowed;
  - the CA cache shared;
  - the budget ignored;
  - a trigger on any reason when a quote exists;
  - observe mutating records;
  - the scale check removed;
  - the ledger check removed;
  - `quote_session` not rebound in the receipt;
  - a bar conflict ignored;
  - the nearest bar used instead of the exact one;
  - the refusal reason dropped from the receipt.
- Missed: a trigger that also fires on `quote_session_stale`. This one is equivalent: that reason never comes with evidence, so the branch cannot be reached.

**Harness cases** (`tests/test_tracker_hist_target_close.py`). They drive the REAL `audit_active_records` with the lean calendar fixture from `tests/test_outcome_price_evidence.py`, an AsyncMock backend, an empty ledger index and fixture bars. No network.

- **H1:** enforce matures on the target-session close: WIN 104, LOSS 95, BREAKEVEN 100 against entry 100, while today's quote is 110. Checked: the ROI, `current_price`, every receipt key, one pull, and the read-back line.
- **H2:** off and 7 non-word spellings: no pull, no log line, no receipt key, read-back untouched.
- **H3:** observe gives the same record state as off; one pull and one log line.
- **H4:** 12 refusal fixtures, each also at grace 0: the record stays ACTIVE with its v6.45.0 reason plus `hist_close_reason`, and is never EXPIRED. H4b: a confirmed ledger action after the target session refuses.
- **H5:** `price_unavailable`, `quote_session_stale` and `quote_before_target_close` never pull and equal off. H5b: a session that has not completed never pulls.
- **H6:** three horizons of one symbol take one pull; `_CA_BARS_CACHE` and `_CA_BUDGET` are untouched. H6b: a spent budget defers the second symbol and never expires it.
- **H7:** a 2:1 split between entry and target (print -48%) goes through the unchanged CA-GUARD. `_ca_adjusted_roi` is called with the target session, and the record matures on the adjusted +4.0 with a CA-ADJUST note.
- **H8:** the pure helper table and the mode and budget readers.
- **N1:** off (4 spellings) equals the v6.45.0 base copy on a mixed fixture (record states, receipts, `_last_audit_stats`), and observe equals it too.
- **N2:** golden negative. The base copy under `TRACK_HIST_TARGET_CLOSE=enforce` leaves the H1 record ACTIVE with `quote_outside_target_session`, including past grace.
- **N3:** the workflow mapping (step-scoped, defaults equal to the script's), no mapping in daily_sync, the version floor (>= 6.47.0), the docstring version, the verifier pin, and the WHY block order.
- **N4:** added lines are ASCII, the only removals are the two version lines, and the function inventory is +7 / -0.

## Operator steps (owner) - nothing is armed by this PR

1. Merge. No deploy is needed; this runs in GitHub Actions.
2. **Proposed (owner decision):** set Variable `TRACK_HIST_TARGET_CLOSE = observe`, then run one audit pass. Use `track_performance.yml` with `run_audit=true`, or `TRACK_SCHEDULED_AUDIT=1` together with `TRACK_DEDUP_MATURED=observe` from #805.
3. Read the `[HIST-CLOSE v6.47.0]` line in the run log:
   - `witnessed` should be most of `candidates`;
   - `refused` names why the rest were refused;
   - `pulls` and `deferred` show whether yfinance answers from the runner (the CA-GUARD's own Yahoo fallback has seen HTTP 429).
4. **Proposed (owner decision):** set it to `enforce`. Then read back:
   - matured rises above 11,791;
   - acceptance D10-3b's overdue count falls from 3,208;
   - the recovered receipts carry `"quote_kind":"historical_daily_close"` and `hist_bar_date`.
5. **Optional:** raise `TRACK_HIST_CLOSE_BUDGET` to clear the backlog faster. At the default of 60 symbols per pass it takes several passes.
6. **Rollback:** unset the Variable. Records already matured under enforce keep their receipts, and `hist_bar_date` marks them.

## Known limits

- **Coverage:** a record whose symbol has no current witnessed quote in that pass is not a candidate. With audits armed, those past the 5-day grace EXPIRE as UNPRICED under the existing behaviour, whether or not this lands.
- **Large moves:** a recovered record whose |print ROI| is 15pp or more goes through the existing CA-GUARD verification. As for any live maturity, it can mature on the adjusted series, stay PENDING, or EXPIRE as CORP_ACTION_SUSPECT. That is the existing path; this PR adds none.
- **`maturity_date`** is the audit time, as for every maturity. This only affects plot ordering.
- **Provider:** the close comes from Yahoo through yfinance. It is a different provider from the current-quote witness but is the exact session's close. Observe is the step that shows whether the runner can reach it.
- **CI coverage:** the harness is not enrolled in the blocking lean list. `ci.yml` is edited by open #778, so enrolling here would collide with its version lines; enrolling is proposed once #778 lands. The advisory whole-suite job collects it. N1, N2 and N4 need the base copy (`git show 55b47f24`, or `TFB_TEST_TP_BASE`) and skip in a shallow checkout. They were run locally, as recorded above.
