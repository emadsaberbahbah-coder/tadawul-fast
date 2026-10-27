# Commit Sheet - `core/providers/calendar_provider.py` v1.3.1 [SPLIT A 404 BATCH] + `calendar_sync.yml` + `ci.yml` 1.1.45 - 2026-10-27

**Scope:** program register **A4** (post-earnings quarantine data lane), a fix found on the first armed run of the last-report columns. A `/calendar/earnings` batch that answers HTTP 404 no longer loses every symbol in it. Gate `TFB_CALENDAR_SPLIT_404`: default off in the module (the v1.3.0 requests and results byte for byte), armed in the calendar workflow.
**Files:** `core/providers/calendar_provider.py` (1.3.0 -> 1.3.1; six helpers added, zero functions removed), `tests/test_calendar_split_404.py` (new), `tests/test_redaction_boundaries.py` (the calendar harness extracts the new helpers; new leg: the split path never logs the key), `tests/test_calendar_last_report_a4.py` (version pin moved), `scripts/verify_deployment.py` (manifest pin 1.3.0 -> 1.3.1, comment only otherwise), `.github/workflows/calendar_sync.yml` (arms the gate), `.github/workflows/ci.yml` 1.1.45, this sheet.

## S1 - WHY (measured 27 Oct)
- calendar_sync run `40489000970` (13:30 Riyadh, first run with `TFB_CAL_LAST_REPORT=on`): `Calendar_Events` gained the three columns, but **3 of 327 rows** carry a last report (WDP.BR, WTFC.US, UNTY.US).
- The step log: `last-earnings batch failed (50 syms): HTTPStatusError: Client error '404 Not Found'` x5 and `earnings batch failed (31 syms): ... 404` (the forward batch of the decision symbols). The forward batch failed the same way on 26 Oct (run `40312198932`, v1.2.2), so the board names' next-earnings dates have come from Yahoo estimates and the sync's prior-row carry (`resurrected=295/298`), not from EODHD.
- EODHD answers 404 for the WHOLE request when it does not know one code in `symbols`. The failing batches mix venues whose suffixes EODHD spells differently from the sheet (e.g. `PHP.L`, `1605.T`, `1015.KL`) and one malformed code (`AEFES.IS.US`); `to_eodhd_symbol` passes these through unchanged. One unknown code costs the whole batch of 31-50 names.

## S2 - WHAT (only when `TFB_CALENDAR_SPLIT_404` is on/1/true/yes)
- `_earnings_items_split`: a batch that answers 404 is split in halves and each half retried, recursively, so every code EODHD accepts keeps its rows; a single code that answers 404 is recorded as rejected. Any other error propagates to the caller's v1.3.0 handling (error logged, that top-level batch skipped).
- Request cap `TFB_CALENDAR_SPLIT_MAX_CALLS` (default 200 per call, 1..1000), checked **before every request**: once it is reached nothing more is requested in that call - pending halves and later batches alike - and their codes are counted.
- One WARNING line per call when anything was split: `<label>: 404 batches split N time(s), M request(s); EODHD rejected K code(s): ...` (first 40 codes), plus the cap note when hit. The line carries codes and counts only - never the URL or the key (new redaction leg).
- Applies to both batched earnings paths: `fetch_earnings_map` (forward) and `fetch_last_earnings_map` (last report). The ex-dividend path (per symbol) and the Yahoo fallbacks are unchanged. With the gate off the request dicts, results and log lines are v1.3.0's.
- Cost: with k unknown codes in a batch of 50 the split adds about 2k x log2(50) requests (≈ 12 per unknown code); EODHD calendar requests are a small share of the daily budget.

## S3 - Gate / arming
| Env | Where | Value | Effect |
|---|---|---|---|
| `TFB_CALENDAR_SPLIT_404` | module default | off | v1.3.0 byte for byte (Render never calls this module) |
| | `calendar_sync.yml` step env | `"on"` | the next scheduled calendar run splits 404 batches |
| `TFB_CALENDAR_SPLIT_MAX_CALLS` | module default | 200 | request cap per call |

No owner arming step: merge is enough. Kill: set the workflow value to `"off"`.

## S4 - Tests
- `tests/test_calendar_split_404.py` (19 cases): S1 gate off / `off` / `0` / blank / junk - requests, results and log lines equal the untouched v1.3.0 module (`git show aba4e5eb`) on both paths; S2 last report - one unknown code among eight: 7 calls (8 -> 4+4 -> 2+2 -> 1+1), seven reports kept (FRST-like -22.2 % surprise exact), one WARNING line exact; S3 forward - earliest upcoming kept, KSA never sent; S4 four unknown codes named (`AEFES.IS.US, X1.T, X2.KL, X3.L`), nine known kept; the cap (2) stops at exactly two requests and counts 13 codes as dropped; Codex's case (cap 2, two all-404 batches of 4 and 2 codes) makes exactly two requests and counts 6; a 429 inside a split skips that top-level batch exactly as v1.3.0 and the next batch still lands; S5 gate spellings, `_is_http_404` on real httpx errors, the workflow arms the gate.
- `tests/test_redaction_boundaries.py` 21 passed (new: the split path's WARNING line carries the rejected code and no URL, no key).
- Calendar suites (A4, partial fill, sticky, completeness, atomic publication, confirm fail-closed, venue calendar): 236 passed, 1 skipped. Contract lane list: 389 passed, 10 skipped. Workflow regressions 309 + manifest pins 17 passed; `scripts/audit_repository_workflows.py` errors 0, warnings 0.

## Review fix (27 Oct, Codex P2 on PR #853)
- **Finding:** the cap was checked only after a request had answered 404, so pending right halves and later top-level batches still issued requests past `TFB_CALENDAR_SPLIT_MAX_CALLS` (cap 2 with two all-404 batches of four and two codes made 4 requests).
- **Fix:** the budget is checked before every request; past it the codes are counted as capped and nothing is requested. Both cases are pinned in S4 (they fail on the pre-fix code: 3 and 4 requests).

## S5 - Rollback
`TFB_CALENDAR_SPLIT_404: "off"` in the workflow (v1.3.0 behaviour on the next run), or revert.

## S6 - Known limits / next
- The split recovers what EODHD knows; it does not translate the suffixes it rejects. The first armed run's WARNING line lists them; a calendar-side suffix map (e.g. `.L` -> `.LSE`) is a separate change made from that evidence, not guessed here (the shared `to_eodhd_symbol` also serves prices and fundamentals).
- No Yahoo fallback for past reports yet (v1.3.0 limit, unchanged).

## S7 - Operator steps
1. Merge. Nothing to arm.
2. Read the next `📅 Calendar Sync` run (05:15Z schedule; recent slots ran 09:30-11:05Z): the step log shows `last-earnings: 404 batches split ...; EODHD rejected K code(s): ...`, and `Calendar_Events` H:J fill for the names that reported in the last 21 days (FRST.US 2026-10-22, -22 % EPS surprise, is the reference case).

Not a licensed financial adviser; the owner decides and executes every trade.
