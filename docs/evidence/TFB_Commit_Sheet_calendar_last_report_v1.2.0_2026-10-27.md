# Commit Sheet - `scripts/run_calendar_sync.py` v1.2.0 + `core/providers/calendar_provider.py` v1.3.0 [LAST REPORT DATE] - 2026-10-27

**Scope:** program register **A4, part 1** (the post-earnings quarantine data lane; project doc `TFB_Self_Correcting_Recommendation_Program_2026-10-27.md`, post-mortem P1). `Calendar_Events` learns each decision symbol's most recent PAST earnings report and its EPS surprise. Workflow gate `TFB_CAL_LAST_REPORT` (repo Variable), default off = the v1.1.4 seven-column table byte for byte.
**Files:** `core/providers/calendar_provider.py` (1.2.2 -> 1.3.0, additive: `fetch_last_earnings_map`, `fetch_last_earnings_sync`, `_num`), `scripts/run_calendar_sync.py` (1.1.4 -> 1.2.0; selftest 9 -> 13), `.github/workflows/calendar_sync.yml` (maps `vars.TFB_CAL_LAST_REPORT || 'off'`), `scripts/verify_deployment.py` (pins only), `.github/workflows/ci.yml` 1.1.42, `tests/test_calendar_last_report_a4.py` (new), this sheet.

## S1 - WHY
FRST.US (first funded ticket, 26 Oct) was bought two sessions after a 22 % EPS miss (Q3 reported 22 Oct after the close: EPS 0.35 vs 0.45; -4.1 % and -3.7 % on heavy volume the next sessions). `Calendar_Events` held only its NEXT report (2027-01-28); the cockpit's earnings tag and the owner's 7-day rule both look forward (`0 <= days <= N`). Nothing in the system could see that the name had just reported and missed. The October review and the research note (post-earnings drift continues in the direction of the surprise for weeks, strongest in small, low-coverage names) make a short post-report quarantine cheap insurance.

## S2 - WHAT
`calendar_provider` v1.3.0 (pure addition; `fetch_event_context` untouched):
- `fetch_last_earnings_map(symbols, days_back=None)` - one batched EODHD `/calendar/earnings` call per `TFB_CALENDAR_EARNINGS_CHUNK` symbols over `[today - TFB_CALENDAR_DAYS_BACK (default 21, 1..120), today - 1]`; per symbol the LATEST report in the window with `eps_surprise_pct` (payload `percent` when numeric, else `(actual - estimate) / |estimate| x 100` when both numeric and estimate != 0; rounded 0.1), `eps_actual`, `eps_estimate`, `report_timing`. A same-day report stays the forward path's (next earnings = today). KSA symbols skipped like the forward path; a failed batch drops only itself; disabled -> `{}`.

`run_calendar_sync` v1.2.0, only when `TFB_CAL_LAST_REPORT` is `on` (or 1/true/yes):
- three columns appended AFTER Source: `Last Earnings Date | Days Since Earnings | EPS Surprise %` (the GAS cockpit reads A:G; the tracker and the brief read by header - no reader changes);
- fill-only carry: a harvested symbol whose fresh fetch has no report keeps a prior row's report while it is still inside the window; a carried report never resurrects a symbol and expires with the window;
- the prior read widens to `A1:J` only when the tab already has 10 columns (a wider read would leave the grid); the grid grows to 10 columns before the single atomic `A1:J` write;
- one log line `last_report: with=n fresh=f carried=c window=Nd`.
Off (unset / off / any other word): reads, the update request and every log line equal v1.1.4 (harness L3, golden against `53c8544e`).

## S3 - Tests
- `tests/test_calendar_last_report_a4.py` -> **11 passed**: L1 provider (latest report in window, bounds incl. today and day -22 excluded, surprise from `percent` / computed / estimate 0, KSA never sent, request window `2026-10-06..2026-10-26` for 27 Oct, `DAYS_BACK=7` window, chunk 1 batching, a 429 batch dropped alone, disabled -> `{}`); L2 the forward context never requests a past window; L3 OFF golden x3 (unset / off / junk: same reads, update request, resizes and stderr as v1.1.4 with the version tag normalised); L4 ON first run on a 7-column tab (read `A1:G1000`, resize to 10 columns, one `A1:J1000` write, 10-column header, FRST `2026-10-22 | 5 | -22.2`, forward columns unchanged, log line); L5 ON carry (inside the window kept, 37-day-old report dropped, unharvested symbol not resurrected); L6 ON with the fetch or the import raising (WARN, carried report kept, publication still written); selftest 13/13.
- Existing calendar suites + manifest pins + workflow mapping/audit + render env contract: **179 passed, 4 skipped**. `py_compile` PASS.

## S4 - Rollback
Unset the repo Variable `TFB_CAL_LAST_REPORT` (or `off`): the next run writes `A1:G` exactly as v1.1.4. Columns H:J keep their last values until cleared by hand; nothing reads them while the gate is off. `git revert` restores both modules.

## S5 - Known limits
- EODHD only: no Saudi coverage for past reports (the forward path's Yahoo fallback is not extended to past reports in this version).
- Release-day price reaction is not computed here (no prices in this job); the quarantine rule's "negative reaction" leg uses the EPS surprise sign for now. A price-reaction leg can come from the engine (it has the bars).
- Scope is the harvested decision symbols (Top_10_Investments + My_Portfolio), exactly like the forward columns.
- Consumers come next: A4 part 2 = a cockpit advisor tag (`reported Nd ago, EPS -22%`) in `16_Decision_Top10` (stacks on v1.17.0, PR #849), observe first; a gate only after it.

## S6 - Operator steps
1. Merge. Nothing changes until the Variable is set.
2. Set the repository Variable `TFB_CAL_LAST_REPORT=on` (Settings -> Variables -> Actions), then dispatch `calendar_sync.yml` with `write=false` once (dry run prints the table incl. the three columns), then `write=true` (or wait for the daily schedule).
3. Verify `Calendar_Events`: header has 10 columns; FRST.US shows `2026-10-22 | <days> | -22.2`; the job log shows `last_report: with=n fresh=f carried=c window=21d`.

Not a licensed financial adviser; the owner decides and executes every trade.
