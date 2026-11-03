# Commit sheet: `calendar_provider` 1.4.0 + `run_calendar_sync` 1.3.0 add the Dividend_Events tab (3 Nov 2026)

**Gate:** `vars.TFB_CAL_DIVIDENDS`.
- **Off (default):** unset, `off` or any other word runs the 1.2.0 sync exactly, apart from the version tag. No new tab is read, created or written.
- **On:** `on`, `1`, `true` or `yes` publishes the new tab.

**Owner request (3 Nov 14:05 Riyadh):** "make the required update to avoid what happened". This is part A of the corporate-action guard: the data lane.

## 1. Evidence
All reads were read-only: the workbook, IBKR and the web. Times are Riyadh.

| When | Fact |
|---|---|
| 2 Nov 16:28 | `Calendar_Events` reads `INDV.US \| 2027-02-25 \| 115 \| 2026-11-03 \| 1`: the next ex-dividend date is tomorrow |
| 2 Nov 08:49 | The cockpit issues six tickets, including INDV.US for 12,335 SAR |
| 2 Nov 17:30 | The owner buys 95 INDV at 35.43 |
| 3 Nov | IBKR shows the position as SUPN on a new contract (849142054), last 28.15. IBKR's corporate-action record shows a cash dividend of 8.13 USD: ex-date 3 Nov, announced 17 Sep, paid 6 Nov |
| 3 Nov | 36.28 − 8.13 = 28.15. The 8.13 USD dividend is about 22 % of the price. A 30 % US withholding applies to a Saudi-resident holder (no US–Saudi treaty), about 2.44 USD a share |

**The defect:**
- The date was in the workbook; the amount never was.
- `fetch_next_exdiv_map` keeps only the date from EODHD `/div/`, so no reader could tell a 22 % special dividend from a 0.7 % quarterly one.
- FRST.US (ex-dividend 6 Nov, a regular quarterly payment) looked exactly the same.

## 2. What changed

**`core/providers/calendar_provider.py` 1.3.2 → 1.4.0**
- New `fetch_next_exdiv_detail(symbols, days_ahead)` and its `_sync` wrapper.
  - It makes the same per-symbol `/div/<code>` request as `fetch_next_exdiv_map`, over [today, today + `TFB_CALENDAR_DIV_DAYS_AHEAD` (90)]. KSA symbols are skipped, and one failed symbol drops only itself.
  - It returns, for the EARLIEST future ex-date: `ex_date`, `amount`, `currency`, `pay_date`, `record_date`, `declaration_date`, `period` and `items`.
- How `amount` is read:
  - It is `value`, else `unadjustedValue`.
  - Several declarations on the same day in one currency are summed.
  - Two currencies on the same day give `MIXED` with no amount.
  - A missing or negative amount gives `None`.
- New pure helpers: `_div_amount` and `_div_detail_of`.
- Everything else is 1.3.2 byte for byte (test D3 against `git show 8478029d`): `fetch_next_exdiv_map`, `fetch_event_context` and every other request and log line.

**`scripts/run_calendar_sync.py` 1.2.0 → 1.3.0**
- With `TFB_CAL_DIVIDENDS=on`, the new lane runs AFTER the unchanged `Calendar_Events` publication.
  - It sends the same decision symbols to `fetch_next_exdiv_detail_sync`.
  - It replaces a SEPARATE tab in one atomic A1:J update. The tab is named by `TFB_CAL_DIVIDENDS_SHEET` (default `Dividend_Events`).
  - Columns: `Symbol | Ex-Div Date | Days To ExDiv | Amount | Currency | Pay Date | Record Date | Period | Updated At (Riyadh) | Source`.
- **Why a separate tab:** the GAS cockpit reads `Calendar_Events` A:G, the tracker and the brief read it by header, and the A4 lane owns H:J. None of them can see a difference.
- **Fill-only carry:**
  - A harvested symbol whose fresh fetch is empty keeps its prior row while the ex-date is still in the future. The row is marked `+carried` and keeps its own as-of.
  - Expired rows die.
  - A symbol outside the harvest is never added.
- **Fail-safe:**

| Condition | Result |
|---|---|
| Provider disabled or not importable | Tab left as is, rc 0 |
| Prior tab unreadable | Tab preserved, rc 2 |
| Fetch fails | Prior rows only, still published |
| Write fails | rc 3, after the calendar publication already succeeded |

- One new summary line: `dividends: with=n fresh=f carried=c -> Dividend_Events`. A dry run prints the rows and writes nothing.

**`.github/workflows/calendar_sync.yml`**
- One run-step env line: `TFB_CAL_DIVIDENDS: ${{ vars.TFB_CAL_DIVIDENDS || 'off' }}`.
- Header notes updated.

**`scripts/verify_deployment.py`:** pins only. `calendar_provider` 1.3.2 → 1.4.0 and `run_calendar_sync` 1.2.0 → 1.3.0.

**Not touched:** `Calendar_Events` (its layout, carry and readers), the cockpit, the backend and the sync.

## 3. Verification
- `tests/test_calendar_dividends.py`: 16 passed (D1–D10).
- The other calendar suites: 151 passed, with only their version pins moved. That includes the A4 golden-off comparison with the 1.1.4 base, whose log-tag normalisation moved to v1.3.0.
- Manifest pins, workflow audit and flag-mapping suites: all green (201 passed together).
- `python scripts/audit_repository_workflows.py --root .` → errors 0, warnings 0.
- `python scripts/run_calendar_sync.py --selftest` → 13/13.
- `test_security_pins_libsmoke_20261013.py::test_l17` fails in this sandbox with and without this change. It is an environment library, not this change.

## 4. Operator step (owner)
1. Merge.
2. Repository → Settings → Secrets and variables → Actions → Variables: `TFB_CAL_DIVIDENDS` = `on`.
3. The next Calendar Sync (08:15 Riyadh, or a manual run with `write` ticked) creates `Dividend_Events`.

**Rollback:** delete the variable. The tab stays as the last ON run left it, and readers decide by its as-of.

## 5. What uses it next
Part B: the cockpit's ticket guard (`16_Decision_Top10`) will read `Dividend_Events` and hold a new ticket whose ex-dividend date is within N days when the dividend is at least P % of the price, with the reason on the ticket.

Part C: holdings will show the dividend and dividend-adjusted levels.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order was created, modified or cancelled.
