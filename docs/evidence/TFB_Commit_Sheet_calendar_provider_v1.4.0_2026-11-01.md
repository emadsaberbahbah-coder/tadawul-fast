# TFB Commit Sheet - calendar_provider v1.4.0 [SAUDI PAST REPORTS FROM YAHOO]

| Item | Value |
| --- | --- |
| Date | 2026-11-01 (daily improvement routine) |
| Branch | `claude/daily-2026-11-01` |
| Base | origin/main `953f84e2` (calendar_provider v1.3.2 since `4c2bd640`, #854) |
| Lane | GitHub Actions only (`calendar_sync.yml`); no Render deploy, no Apps Script paste |
| Gate | repository Variable `TFB_CAL_LAST_REPORT_YAHOO` = on / 1 / true / yes; default off = v1.3.2 byte for byte |
| Priority | (a) a decision guard's input is missing for a live holding |

## S1 - Defect and evidence

`fetch_last_earnings_map` (v1.3.0) feeds the Last Earnings Date / EPS Surprise % columns of Calendar_Events (run_calendar_sync v1.2.0, `TFB_CAL_LAST_REPORT`). It sends only non-KSA symbols to EODHD (`_split_symbols`; EODHD has no Saudi coverage) and returns `{}` at once for a KSA-only list. No Yahoo path reads past reports: the v1.2.0 Yahoo tier (`_yf_earnings_dates_future`) keeps future dates only.

So a Saudi holding or seat never gets a Last Earnings Date. The post-earnings quarantine reads exactly that column:

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
- The quarantine needs 5 sessions, or 10 after a miss or an unknown surprise.

Why now: 2222.SR is one of the two live holdings (views 1.4.0 sheet, 1 Nov). Aramco reports Q3 in the first week of November (4 Nov 2025, 5 Nov 2024).

## S2 - Change

| Site | Change |
| --- | --- |
| `core/providers/calendar_provider.py` | v1.4.0. WHY block in the module docstring above v1.3.2; title and `__version__` 1.4.0; ENV list gains the gate. Added `_last_report_yahoo_enabled`, `_yf_report_date`, `_yf_row_value`, `_yf_earnings_dates_past`, `_yahoo_last_report_fill` (five, module level). Zero removals. |
| `fetch_last_earnings_map` | Two gated seams. The early return becomes `if not code_map and not _ksa_fill`. After the EODHD batches (and the split-state line), `_yahoo_last_report_fill(_ksa, start, end, out)` runs when the gate is on. Off: `_ksa_fill` is False, so requests, results and log lines are v1.3.2's. |
| `_yf_earnings_dates_past` | yfinance `Ticker(sym).get_earnings_dates(limit=12)`, in a thread. Takes the LATEST row dated (Riyadh date) in `[today - TFB_CALENDAR_DAYS_BACK, today - 1]`. Skips a row whose Event Type is present and not `Earnings` / `Call` (or the raw codes 2 / 1); a stockholders' meeting (11) is never a report. `eps_surprise_pct` is `Surprise(%)` when numeric, else (actual - estimate) / abs(estimate) x 100, rounded to 0.1. The row carries `source: yahoo_earnings_dates` and `report_timing: None`. |
| `_yahoo_last_report_fill` | Bounded by `TFB_CALENDAR_CONCURRENCY` and `TFB_CALENDAR_TIMEOUT_SEC` like the forward tier. A bare 4-digit code is asked as `<code>.SR`. Fill-only: a row already in the map is never overwritten. A rate-limit-class exception is counted (`throttled=N`); any other error or a timeout drops only that symbol. yfinance absent: one INFO line, no fill. One INFO line per call: `last-report yahoo fill: n of m KSA (throttled=t): ...`. |
| `.github/workflows/calendar_sync.yml` | `TFB_CAL_LAST_REPORT_YAHOO: ${{ vars.TFB_CAL_LAST_REPORT_YAHOO \|\| 'off' }}` in the "Run calendar sync" step env, with a dated header comment. Not armed. |
| `scripts/verify_deployment.py` | pin calendar_provider 1.4.0 (pin only). |
| `.github/workflows/ci.yml` | 1.1.65 (1.1.62 / 1.1.63 claimed by open #778, 1.1.64 by open #882): the new harness in the contract lane, and a SHA fetch of `4c2bd640` for its golden leg. |
| `tests/test_calendar_split_404.py`, `tests/test_calendar_venue_batches.py`, `tests/test_calendar_last_report_a4.py` | exact `1.3.2` pins become `>= (1, 3, 2)` floors; one expected log line carries the module's own version. Every other assertion is kept. |

`run_calendar_sync.py` needs no change: `merge_last_report` reads `last_earnings_date` and `eps_surprise_pct` and ignores the extra `source` key. Non-KSA symbols, the forward map, `fetch_event_context` and the ex-dividend path are untouched in every mode.

## S3 - Tests

`tests/test_calendar_last_report_yahoo.py`: **PASS 53/53** as a script, 53 passed under pytest, the same under PYTHONHASHSEED 0 / 77.

The yfinance stub is duck-typed: a frame with `.index` and `.iloc[pos]`. The contract lane installs no pandas or yfinance. One case uses a real pandas frame when pandas is present.

| Case | Checks |
| --- | --- |
| Y1 | off under 8 spellings (unset, off, 0, blank, spaces, onn, enforce, observe) equals the untouched v1.3.2 (`git show 4c2bd640`) on a mixed list, a KSA-only list and a US-only list: results, EODHD requests, log lines, and zero yfinance calls. |
| Y1b / Y1c | the six on-spellings; a disabled layer stays `{}` with no yfinance call. |
| Y2 | on, mixed list: 2222.SR gets the 3 Nov row (-6.5 %). The future row, the meeting row, the older in-window row and the out-of-window row are not taken. The US rows and the EODHD requests equal the off run, and `source` appears on the KSA row only. |
| Y3 | on, KSA-only list (`2222.SR`, `5110`): filled with no EODHD request; the bare code is asked as `5110.SR`. |
| Y4 | Riyadh dates: 23:30 New York gives the next day; 20:59Z and 21:00Z fall either side of Riyadh midnight; naive, date and text forms. |
| Y5 | surprise rules (Surprise(%) wins, NaN is not a number, estimate 0 or no actual gives None, a negative estimate). |
| Y6 | throttles are counted and other errors are dropped. |
| Y7 | yfinance missing. |
| Y8 | timeout. |
| Y9 | fill-only. |
| Y10 | Event Type labels, raw codes, absent and NaN. |
| Y11 | the REAL `run_calendar_sync` `merge_last_report` / `last_report_cells` put `2026-11-03 \| 2 \| -6.5` on the 2222.SR row. |
| Y12 | a real pandas frame shaped like yfinance 0.2.66 (tz-aware index, integer event codes). |
| G1 | Y2, Y3 and Y11 fail on the untouched v1.3.2 with the gate on (2222.SR stays absent; no yfinance call). |
| V1 / V1b | version floor, verifier pin, step-env mapping (step env only), the contract-lane line in ci.yml, WHY order; against the base: ASCII added lines, exactly the five module-level functions added, none removed (nested included). |

Golden negative of the whole file (`TFB_TEST_CALENDAR_DELIVERED=<v1.3.2 copy>`): **50 failed, 2 passed, 1 skipped**. Y1c passes because a disabled layer is `{}` on both; G1 compares against the base by construction; V1b applies to 1.4.0 only.

Other checks:

- calendar suites, verifier pins, flag mappings, env contract, PF confirm calendar, venue calendar ext: 399 passed, 1 skipped;
- `tests/test_dt10_calendar_completeness.js` passes;
- compileall clean; workflow audit errors=0; `render_env_contract --check` OK.

## S4 - Operator steps

1. Merge. GitHub Actions only: no deploy, no paste.
2. Set the repository Variable `TFB_CAL_LAST_REPORT_YAHOO=on`. `TFB_CAL_LAST_REPORT=on` must be set too; it already is if Calendar_Events shows the Last Earnings Date column.
3. Read back the next 08:15 calendar_sync run:
   - the "Run calendar sync" log shows `last-report yahoo fill: n of m KSA (throttled=t): ...`;
   - after a Saudi report (Aramco Q3, first week of November), the 2222.SR row on Calendar_Events carries a Last Earnings Date, Days Since Earnings and EPS Surprise %;
   - the Top_10 / Portfolio_Decision post-earnings tag then holds it for its sessions.

Rollback: unset the Variable (or set it off).

## S5 - Known limits

- Coverage is not checkable offline. If Yahoo lists no past rows for a KSA name, the INFO line reads `0 of m` and nothing changes, so the change is harmless.
- One extra Yahoo request per KSA symbol per run (3 KSA names in the current universe), plus yfinance's timezone lookup. A throttle is counted and visible.
- Yahoo report dates and EPS can be estimates or restated. The surprise feeds a display tag and an owner-armed guard; it moves no score.
- Not touched here:
  - codes EODHD rejects (`.T`, `.SI`, `IG.MI`), and the `X.IS.US` / `X.LS.US` Yahoo mapping;
  - the forward Yahoo tier, which still takes the earliest future row of any Event Type (pre-existing).

## S6 - Also found by the survey (not built here)

- **Daily brief, `_Status` read failure** (priority a). `read_pages_live` turns a failed `_Status` read into `[]`, the final-feed veto is lost, and the brief emails executable tickets. `send_digest` is fail-closed on the same read.
  - Codex PR #785 carries an ungated fix, but it is stale (53 commits behind) and conflicts with main.
  - A gated rebuild (run_daily_brief 1.22.0 over #882) is the next-day candidate.
- **`tests/test_fw2_stale_pe.py` w1b fails on main** whenever the git base is available. Its base `729a316e` predates #841's `daily_sync.yml` change; CI skips the leg on a shallow checkout, so nobody saw it.
