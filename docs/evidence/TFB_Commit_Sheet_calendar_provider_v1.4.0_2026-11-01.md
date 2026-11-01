# TFB Commit Sheet - calendar_provider v1.4.0 [SAUDI PAST REPORTS FROM YAHOO]

| Item | Value |
| --- | --- |
| Date | 2026-11-01 (daily improvement routine) |
| Branch | `claude/daily-2026-11-01` |
| Base | origin/main `953f84e2` (calendar_provider v1.3.2 since `4c2bd640`, #854) |
| Lane | GitHub Actions only (`calendar_sync.yml`); no Render deploy, no Apps Script paste |
| Gate | repository Variable `TFB_CAL_LAST_REPORT_YAHOO` = on / 1 / true / yes; default off = v1.3.2 byte for byte |
| Priority | (a) a published recommendation's input is missing: the post-earnings tag of KSA Top_10 seats (and D1b for any KSA holding's ADD) |

## S1 - Defect and evidence

`fetch_last_earnings_map` (v1.3.0) feeds the Last Earnings Date / EPS Surprise % columns of Calendar_Events (run_calendar_sync v1.2.0, `TFB_CAL_LAST_REPORT`). It sends only non-KSA symbols to EODHD (`_split_symbols`; EODHD has no Saudi coverage) and returns `{}` at once for a KSA-only list. No Yahoo path reads past reports: the v1.2.0 Yahoo tier (`_yf_earnings_dates_future`) keeps future dates only.

So a Saudi Top_10 seat or holding never gets a Last Earnings Date. The post-earnings quarantine reads exactly that column:

- `16_Decision_Top10` v1.18.0 tag (`dt10PostEarnMapFromValues_`);
- `10_My_Portfolio` `PD_POST_EARN` / Portfolio_Decision D1b, through the same tag.

The docs already list the gap:

| Doc | Line |
| --- | --- |
| `TFB_Commit_Sheet_calendar_last_report_v1.2.0_2026-10-27.md` | 28: "EODHD only: no Saudi coverage for past reports" (repeated in the v1.3.1 and v1.3.2 sheets) |
| `TFB_Commit_Sheet_pf_add_guards_d1_v1.17.0_2026-10-28.md` | 40: "the EODHD calendar does not cover KSA names (a KSA holding passes (b))" |
| `16_Decision_Top10.gs` v1.18.0 WHY | "only a past Next Earnings Date can tag one" |

Offline replay (survey verifier, real `apply_sticky` / `merge_last_report` / `last_report_cells` and the real `dt10PostEarnMapFromValues_`):

- 2222.SR is tagged for ONE render, the morning after its report, through the past Next Earnings Date.
- After the next 08:15 sync that past date is dropped (sticky keeps future dates) and the Last Earnings Date stays blank.
- The DT10 tag needs 5 sessions, or 10 after a miss; it gives an unknown surprise 5. Portfolio_Decision D1b (portfolio_actions v1.17.1) gives an unknown surprise the longer 10.

Why now: Aramco (2222.SR, a rated KSA name: views 1.4.0 sheet, `v_symbol_rating`) reports Q3 in the first week of November (4 Nov 2025, 5 Nov 2024).

The live book of 1 Nov is the 5023.SR sukuk (no earnings, outside the execution scope) and FRST.US (EODHD-covered), so no live holding loses a guard input today. KSA Top_10 seats do; D1b covers ADDs to a holding, so it gains the input once a KSA name with earnings is held. (An earlier draft of this sheet called 2222.SR a live holding; the independent review showed that was a misreading of the views sheet, see S7.)

## S2 - Change

| Site | Change |
| --- | --- |
| `core/providers/calendar_provider.py` | v1.4.0. WHY block in the module docstring above v1.3.2; title and `__version__` 1.4.0; ENV list gains the gate. Added `_last_report_yahoo_enabled`, `_yf_report_date`, `_yf_row_value`, `_yf_earnings_dates_past`, `_yahoo_last_report_fill` (five, module level). Zero removals. |
| `fetch_last_earnings_map` | Two gated seams. The early return becomes `if not code_map and not _ksa_fill`. After the EODHD batches (and the split-state line), `_yahoo_last_report_fill(_ksa, start, end, out)` runs when the gate is on. Off: `_ksa_fill` is False, so requests, results and log lines are v1.3.2's. |
| `_yf_earnings_dates_past` | yfinance `Ticker(sym).get_earnings_dates(limit=12)`, in a thread. Rows dated in `[today - TFB_CALENDAR_DAYS_BACK, today - 1]`, the date taken on the forward tier's basis (`_yf_report_date`: the index entry's own calendar date, New York on yfinance 0.2.66), so one Yahoo row passes from Next to Last with no day in neither. Skips a row whose Event Type is present and not `Earnings` / `Call` (or the raw codes 2 / 1), or whose Event Name reads as a meeting (meeting, assembly, AGM, EGM): 0.2.66 returns no Event Type, its scrape path keeps the page's own columns. Of the rows left, the LATEST with a numeric Reported EPS or Surprise(%) wins over a later row with neither dated at most 21 days after it (a call, an unnamed meeting); a figureless row further on is a newer report not filled in yet and wins (`TFB_CALENDAR_DAYS_BACK` can reach 120); with no figured row, the latest. A frame holding a NaT date raises ValueError (counted as an error): 0.2.66 drops a dateless row and then re-assigns the parsed dates by position, so every later row carries the next row's date and the last row NaT. `eps_surprise_pct` is `Surprise(%)` when numeric, else (actual - estimate) / abs(estimate) x 100, rounded to 0.1. The row carries `source: yahoo_earnings_dates` and `report_timing: None`. |
| `_yahoo_last_report_fill` | Bounded by `TFB_CALENDAR_CONCURRENCY` and `TFB_CALENDAR_TIMEOUT_SEC` like the forward tier. A bare 4-digit code is asked as `<code>.SR`. Fill-only: a row already in the map is never overwritten. A rate-limit-class exception is counted (`throttled=N`); any other error or a timeout drops only that symbol and is counted (`errors=N`). yfinance absent: one line, no fill. One summary line per call: `last-report yahoo fill: n of m KSA (throttled=t, errors=e): ...`. It goes through the logger when a handler takes INFO, else straight to stderr: `run_calendar_sync` configures no logging, so an INFO record alone never reached the job log. |
| `.github/workflows/calendar_sync.yml` | `TFB_CAL_LAST_REPORT_YAHOO: ${{ vars.TFB_CAL_LAST_REPORT_YAHOO \|\| 'off' }}` in the "Run calendar sync" step env, with a dated header comment. Not armed. |
| `scripts/verify_deployment.py` | pin calendar_provider 1.4.0 (pin only). |
| `.github/workflows/ci.yml` | 1.1.65 (1.1.62 / 1.1.63 claimed by open #778, 1.1.64 by open #882): the new harness in the contract lane, and a SHA fetch of `4c2bd640` for its golden leg. |
| `tests/test_calendar_split_404.py`, `tests/test_calendar_venue_batches.py`, `tests/test_calendar_last_report_a4.py` | exact `1.3.2` pins become `>= (1, 3, 2)` floors; one expected log line carries the module's own version. Every other assertion is kept. |

`run_calendar_sync.py` needs no change: `merge_last_report` reads `last_earnings_date` and `eps_surprise_pct` and ignores the extra `source` key. Non-KSA symbols, the forward map, `fetch_event_context` and the ex-dividend path are untouched in every mode.

## S3 - Tests

`tests/test_calendar_last_report_yahoo.py`: **PASS 82/82** as a script, 82 passed under pytest, the same under PYTHONHASHSEED 0 / 77.

The yfinance stub is duck-typed: a frame with `.index` and `.iloc[pos]`. The contract lane installs no PyYAML, pandas or yfinance. With those modules blocked (`sys.modules[m] = None`, plus bs4 and lxml): 78 passed, 4 skipped (Y4b needs pandas; Y12 and Y17b need yfinance).

| Case | Checks |
| --- | --- |
| Y1 | off under 8 spellings (unset, off, 0, blank, spaces, onn, enforce, observe) equals the untouched v1.3.2 (`git show 4c2bd640`) on a mixed list, a KSA-only list and a US-only list: results, EODHD requests, log lines, and zero yfinance calls. |
| Y1b / Y1c | the six on-spellings; a disabled layer stays `{}` with no yfinance call. |
| Y2 | on, mixed list: 2222.SR gets the 2 Nov New York row (-6.5 %). The future row, the meeting row and the older in-window row are not taken. The US rows and the EODHD requests equal the off run, `source` appears on the KSA row only, and the summary line reads `1 of 1 KSA (throttled=0, errors=0)`. |
| Y3 | on, KSA-only list (`2222.SR`, `5110`): filled with no EODHD request; the bare code is asked as `5110.SR`. |
| Y4 / Y4b | dates on the forward tier's basis: 23:30 New York stays that New York day; UTC, Riyadh, naive, date and text forms; NaT is no date. |
| Y5 | surprise rules (Surprise(%) wins, NaN is not a number, estimate 0 or no actual gives None, a negative estimate). |
| Y6 | throttles are counted (`throttled=1`); other errors are dropped and counted (`errors=1`). |
| Y7 | yfinance missing. |
| Y8 | a timeout drops only that symbol and is counted: `1 of 2 KSA (throttled=0, errors=1)`. |
| Y9 | fill-only. |
| Y10 / Y10b | Event Type labels, raw codes, absent and NaN; Event Name: a name reading as a meeting (meeting, assembly, AGM, EGM) never counts, an earnings release or call does. |
| Y11 | the REAL `run_calendar_sync` `merge_last_report` / `last_report_cells` put `2026-11-02 \| 3 \| -6.5` on the 2222.SR row. |
| Y12 | the REAL yfinance `get_earnings_dates` parser (only `cache_get` canned, no network) on a calendar page with and without an Event Name column. On 0.2.66 the frame has no Event Type column and a New York index; a later Extraordinary General Meeting row never replaces the 2 Nov report. |
| Y13 | window edges, both through `_yf_earnings_dates_past` and through `fetch_last_earnings_sync`: today out, yesterday in, the start day in, the day before it out; only out-of-window rows give nothing, and the summary reads `0 of 1 KSA (throttled=0, errors=0): -` (no report is not an error). |
| Y14 | hand-off over the syncs of 1-4 Nov: the same `November 2, 2026 at 11 PM EST` row is in Next Earnings Date (the REAL `_yf_earnings_dates_future`) on 1 and 2 Nov and in Last Earnings Date on 3 and 4 Nov: never in both, never in neither. |
| Y15 / Y15b | with no logging configured (as in `run_calendar_sync`), the summary line and the yfinance-missing line reach stderr exactly once; with an INFO handler they go to the logger only; a handler at WARNING, or level INFO with no handler, also means stderr. |
| Y17 / Y17b | a frame holding a NaT date is refused and counted (`errors=1`). Y17b runs the REAL 0.2.66 parser on Y12's page with the EGM row's date blanked: the Q3 report comes back under Q2's date and the last row as NaT, and nothing is filled. |
| Y16 / Y16b | a row with EPS figures beats a later call or unnamed no-figure row; with no such row, the latest row (surprise None). A figureless row 22 or 92 days after the figured one wins (a newer report is not lost under a 120-day look-back); 21 days after, the figured row wins. |
| G1 | Y2, Y3 and Y11 fail on the untouched v1.3.2 with the gate on (2222.SR stays absent; no yfinance call). |
| V1 / V1b | version floor, verifier pin, step-env mapping (text always, parsed when PyYAML exists; step env only), the contract-lane line in ci.yml, WHY order; against the base: ASCII added lines, exactly the five module-level functions added, none removed (nested included). |

Golden negatives (`TFB_TEST_CALENDAR_DELIVERED=<copy>`):

- against the untouched v1.3.2: **79 failed, 2 passed, 1 skipped**. Y1c passes because a disabled layer is `{}` on both; G1 compares against the base by construction; V1b applies to 1.4.0 only.
- against the first 1.4.0 push `579b1513` (before the first review's fixes): **28 failed, 54 passed**: the cases written for that review (new date basis, summary format, Event Name, real parser, hand-off, stderr, figured-row preference) plus Y13's summary line and Y17 / Y17b. Y13's window edges themselves pass there: the window check was right, only its tests were missing.
- against `57395ec3` (before the second review's fixes): **2 failed, 80 passed**: Y17 and Y17b, the NaT guard. The other second-review cases (Y8, Y13 and Y15b assertions) pin behaviour 57395ec3 already had.
- fourteen mutants of the delivered module are each killed: no lower bound (2 failed), today included (3), yesterday excluded (4), start day excluded (1), no EPS preference (4), no Event Name filter (5), no 21-day cap (2), summary through the logger only (3), `or` for `and` in the stderr condition (2), handler check only (1), level check only (1), timeouts not counted (1), a no-row result counted as an error (4), no NaT guard (2). Before Y13, the four window mutants passed every case; before Y8 / Y13 / Y15b asserted the summary, the last five passed too.

Other checks:

- calendar suites, verifier pins, flag mappings, env contract, PF confirm calendar / session, venue calendar ext (16 files): 446 passed, 2 skipped;
- `tests/test_dt10_calendar_completeness.js` passes;
- compileall clean; workflow audit errors=0; `render_env_contract --check` OK.

## S4 - Operator steps

1. Merge. GitHub Actions only: no deploy, no paste.
2. Set the repository Variable `TFB_CAL_LAST_REPORT_YAHOO=on`. `TFB_CAL_LAST_REPORT=on` must be set too; it already is if Calendar_Events shows the Last Earnings Date column.
3. Read back the next 08:15 calendar_sync run:
   - the "Run calendar sync" log shows `[calendar_provider v1.4.0] last-report yahoo fill: n of m KSA (throttled=t, errors=e): ...` (written to stderr, since the sync configures no logging);
   - after a Saudi report (Aramco Q3, first week of November), the 2222.SR row on Calendar_Events carries a Last Earnings Date (Yahoo's own date for the row, the same date the Next Earnings Date column showed), Days Since Earnings and EPS Surprise %;
   - the Top_10 / Portfolio_Decision post-earnings tag then holds it for its sessions.

Rollback: unset the Variable (or set it off).

## S5 - Known limits

- Coverage is not checkable offline. If Yahoo lists no past rows for a KSA name, the summary line reads `0 of m` and nothing changes, so the change is harmless.
- One extra Yahoo request per KSA symbol per run (3 KSA names in the current universe). On 0.2.66 that is one calendar-page fetch; there is no separate timezone lookup. Throttles and other errors are counted on the summary line.
- Whether the live calendar page carries an Event Name column, lists meetings, or ever has a dateless row, is not checkable offline. Y12 covers both shapes. Without a name, a meeting row with no EPS figures is taken only when no figured row lies in the 21 days before it; the DT10 tag then holds 5 sessions and D1b 10.
- The date is Yahoo's own date for the row (New York on 0.2.66), as for Next Earnings Date. For a Saudi report stamped the New York evening before, that is the day before the Riyadh date, so the Riyadh morning's session counts as session 1. A pre-open release stamped 08:00-10:00 Riyadh (00:00-02:00 New York) keeps the Riyadh date, so its count starts one session late, as for any date-only feed (EODHD's before-market reports too).
- "No day in neither" holds for the scheduled 05:15 UTC sync, where the provider's UTC today is the Riyadh day. A manual write dispatch between 21:00 and 24:00 UTC can leave a row in neither column until the next scheduled run.
- A fetch failure on the sync right after a report (throttle, timeout, DST-ambiguous stamp such as `November 1, 2026 at 1 AM EST`, which 0.2.66's parser rejects) leaves that symbol blank for that run; it is counted on the summary line, and the next run fills it. Once Last is on the tab, `merge_last_report` carries it through failed runs.
- The forward tier (`_yf_earnings_dates_future`, untouched) raises TypeError on a NaT frame and falls back to `Ticker.calendar`, and still takes a meeting or call row as Next Earnings Date (pre-existing).
- Yahoo report dates and EPS can be estimates or restated. The surprise feeds a display tag and an owner-armed guard; it moves no score.
- Not touched here:
  - codes EODHD rejects (`.T`, `.SI`, `IG.MI`), and the `X.IS.US` / `X.LS.US` Yahoo mapping;
  - the forward Yahoo tier, which still takes the earliest future row of any Event Type (pre-existing).

## S6 - Also found by the survey (not built here)

- **Daily brief, `_Status` read failure** (priority a). `read_pages_live` turns a failed `_Status` read into `[]`, the final-feed veto is lost, and the brief emails executable tickets. `send_digest` is fail-closed on the same read.
  - Codex PR #785 carries an ungated fix, but it is stale (53 commits behind) and conflicts with main.
  - A gated rebuild (run_daily_brief 1.22.0 over #882) is the next-day candidate.
- **`tests/test_fw2_stale_pe.py` w1b fails on main** whenever the git base is available. Its base `729a316e` predates #841's `daily_sync.yml` change; CI skips the leg on a shallow checkout, so nobody saw it.

## S7 - Independent review (before merge)

A five-lens review (off identity, real yfinance shapes, downstream guards, harness, docs) of `579b1513`. Each finding was checked by an adversarial verifier: 11 confirmed, 7 rejected. The confirmed ones reduce to seven issues, all fixed on this branch:

| Issue | Fix |
| --- | --- |
| V1 imported PyYAML, which the contract lane does not install (CI red) | `57a93fc0`: V1 checks the text form always and the parsed form only when PyYAML exists |
| The new lane dated rows in Riyadh time, the forward tier in New York time. For a report stamped the New York evening before, the report-day sync showed the event in neither column, so session 1 went untagged and D1b unguarded | `_yf_report_date` now uses the forward tier's basis; Y14 replays 1-4 Nov |
| The Event Type filter never runs on the pinned yfinance 0.2.66 (no such column), so a later meeting row could replace the report and cut the DT10 10-session miss window to 5 | Event Name filter, plus a row with EPS figures beats a later one without (within 21 days; see the last rejected item); Y10b, Y12 (real parser), Y16 / Y16b |
| The summary line was an INFO record and `run_calendar_sync` configures no logging, so the operator readback never saw it | the line goes to stderr when no handler takes INFO, and counts errors; Y15 |
| No fixture row on the window edges: four window mutants passed every case | Y13; the four mutants now fail |
| "2222.SR is one of the two live holdings" was false (the book is 5023.SR and FRST.US) | corrected in the WHY, the workflow comment, the harness and this sheet |
| Y12's "shaped like yfinance 0.2.66" frame and the "timezone lookup" cost were not what 0.2.66 does | Y12 rebuilt on the real parser; S5 corrected |

Rejected after verification:

- a NaT index entry: rejected as not shown on real data. The reason first given here ("0.2.66 drops rows without an Earnings Date") was backwards, as the second review showed (S8): that drop is what creates the NaT and shifts dates onto the wrong rows. A NaT frame is now refused;
- `TADAWUL:` prefixes and Arabic-Indic digits: they reach Yahoo only when the provider is called directly; no production input takes that path;
- newest-first fixtures, the one-line-per-call claim, and zero KSA symbols with the gate on: coverage notes with no wrong output;
- "a meeting row becomes the last report": rejected by one verifier because no real page has been shown to list meetings; another verifier confirmed the same defect, and it is fixed above;
- "the latest row wins even without EPS": its verifier argued that preferring the figured row could pick last quarter's report when the look-back is long (`TFB_CALENDAR_DAYS_BACK` up to 120) and Yahoo has not filled in a fresh report. That point is taken: the preference applies only within 21 days (Y16b).

## S8 - Second independent review (of `57395ec3`)

Three lenses (code regressions, downstream end to end, tests and claims), each finding checked by a skeptic: 3 confirmed (all minor), 6 rejected, 6 not verified (cap of three per lens).

It found the code clean on every point it checked:
- the off path is identical to v1.3.2 over 1,728 side-by-side cases;
- the hand-off has no gap for report stamps at every hour of a New York day, EST and EDT, through the real `run_calendar_sync.main`;
- DT10 and D1b session counts match Tadawul's Sun-Thu sessions;
- the real-parser shapes, house rules and every count in S3 reproduce.

| Confirmed | Fix |
| --- | --- |
| The errors counter's handling of timeouts and of "no report" was not asserted: two mutants passed | Y8 and Y13 assert the summary line |
| The stderr condition was tested only at its two corners: three one-condition mutants passed | Y15b (handler at WARNING; INFO with no handler) |
| S7's reason for rejecting the NaT finding was backwards: in 0.2.66 a dropped mid-table row shifts every later date onto the wrong row and leaves NaT on the last one | a NaT frame is refused and counted (Y17, Y17b on the real parser); S7 corrected |

Unverified items acted on:
- Y12 patched the process-wide yfinance `YfData` singleton without restoring it; it now uses `monkeypatch`.
- "KSA ADD candidates lose a guard input" overstated D1b, which only covers ADDs to a holding; the wording is corrected.
- The pre-open, UTC-evening dispatch and DST notes are added to S5.

Rejected:
- the 21-day cap's edge behaviour: it only narrows what 579b1513 did, and the realism is unverified;
- `_say` honouring a deliberately silenced logger;
- the UTC-evening and fetch-failure gaps: documented, pre-existing scope, now in S5;
- the forward tier's `Ticker.calendar` local-zone dating, and meeting rows becoming Next: pre-existing, untouched.
