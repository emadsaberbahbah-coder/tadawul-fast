# Commit Sheet - `apps_script/10_My_Portfolio.gs` v1.5.20 [EXIT-PLAN-STOP] - 2026-11-16

**Scope:** the last page that showed a stop other than the stop of record. Owner, 16 Nov: "each stock must end with ONE final decision shown the same on every page"; 14:41 Riyadh: "for any approval required from me take the best available option".
- **Gate:** a new Script Property `PD_EXIT_PLAN_STOP`, **default ON**. `0` / `off` / `false` / `no` (trimmed, any case) = every cell byte-identical to v1.5.19, apart from that one property read per rendered refresh.
- **Display only:** nothing is sent, weighted, blocked, unblocked or actioned differently. The payload is not changed.

**Files:**
- `apps_script/10_My_Portfolio.gs` 1.5.19 -> 1.5.20: 6,102 -> 6,286 lines; 5 hunks; every added line ES5 and ASCII. SHA-256 `199989e9b8aa4bba39f1a8651eead0c85711f6955956723053a29940b007a6cc`.
- `tests/test_pd_v1520_exit_plan_stop.js` (new; X0-X7 x2, `PASS 8/8`, mutants 11/11) and `tests/test_pd_v1520_exit_plan_stop.py` (pytest wrapper).
- `tests/js/pd_edits_v1520.json` (new, the 5 hunks) and `tests/js/pd_history.js` (new: rebuilds the v1.5.19 file by reversing the hunks, SHA-256 `4f246be1...` pinned).
- Pins moved:
  - `tests/test_pd_v1519_plain_block.js` and `tests/test_my_portfolio_v1518_cash_snapshot.js` read the rebuilt v1.5.19 file through `pd_history.js`.
  - `tests/js/gas_ledger_fx_failed_state.test.js`: version pin.
  - `tests/test_pf_allcash_ok.py`: version pin, the 6 new functions, +184 lines.
- This sheet.

## S0 - Evidence (16 Nov 2026, read-only)
- `Consistency_Check` C9 has read MISMATCH every hour: the `Stop SAR` column of Portfolio_Decision is the backend engine's stop, while the stop the owner places is the Exit Plan's (per stock, reviewed daily, never lowered).
- The Today Action Plan, the FINAL DECISION column and Decision Pages v2 1.4.3 already lead with the Exit Plan's stop. Portfolio_Decision was the last page showing another one.
- The live `_Exit_Plan` header was read back before the build: `Date | Run At (Riyadh) | Symbol | Lot | Entry | Shares | Last | Highest Close | ATR | R | Stop | Target | TP1 Reached | Action | Tighten | Bars To | Basis`. Every holding in today's ledger has one row.

## S1 - WHAT
1. **The ledger map.** `pdExitStopsFromValues_` (pure) turns the ledger grid into `{SYMBOL: {stop, day}}`.
   - Each symbol's newest day wins; the day must be 0 to `PD_EXIT_STOP_MAX_DAYS` (4) days old and never in the future.
   - A symbol whose lots carry different stops on that day is left out: the engine's stop stays.
   - Rows without a symbol, a day or a positive stop are skipped. A ledger without the `Date`, `Symbol` or `Stop` header gives an empty map.
2. **The overlay.** `pdExitStopsApply_` (pure) works on the **rendered rows only**.
   - `Stop SAR` = stop x (`Price SAR` / `Price`), 2 decimals. A SAR row without both prices uses rate 1. Any other row without a usable rate is unchanged.
   - The Advisor Note gains ` | Stop SAR: Exit Plan <stop> <ccy> of <day>; engine stop <x> SAR`, so the engine's figure is still on the page.
3. **The read.** `pdExitStopsRead_` reads the header row and the newest 400 rows of `_Exit_Plan` (two range reads). No tab, an empty tab or any fault gives `{}`.
4. **The call.** `pdRenderPayload_` applies the overlay between building the rows and writing the table, inside a try / catch. One Logger line: `exit plan stop on <n> of <m> row(s)`.
5. **Self-test.** `pdSelfTest` gains one line: the gate state and `core: ok`.

## S2 - What does not change
- The POST body, B2, the KPI strip, `_Run_Log`, the evidence publication and `_Portfolio_Action_Log` (it keeps the engine's own stop for tracking).
- The 26 headers and the header row position: no row is added, so every reader (`consistency_check`, `run_daily_brief`, the archive) finds the table where it was.
- `consistency_check` C9 converts `Stop SAR` back with `Stop SAR x Price / Price SAR`, so it now reads the Exit Plan's stop back within rounding.

## S3 - Tests (invented figures only)
- `node tests/test_pd_v1520_exit_plan_stop.js` x3: `run 1/2: 8/8`, digest `898451345defe5b8` each time, **mutants killed 11/11**, **`PASS 8/8`**.
  - X1 golden negative: gate off (5 spellings) with a ledger present = the v1.5.19 refresh; the ledger is never read.
  - X2 on: only `Stop SAR` and the Advisor Note of the holdings in the ledger change; B2, the POST body, `_Run_Log` and the payload are the base's.
  - X3 edges: no tab, a fault, an empty or headerless ledger, 5 days old, a future day, lots that disagree = the base's cells; 4 days old is used; a Date cell; a long ledger reads its newest 400 rows.
  - X4 readers: the C9 formula reads back the Exit Plan stop; headers unchanged; the note is one ASCII line.
  - Mutants: off does not disarm; the payload mutated; no rate applied; the engine stop not named; an old ledger used; a future day used; lots that disagree used; an older day wins; a USD row without a rate changed; a ledger fault thrown; the whole ledger read.
- Older portfolio harnesses on the rebuilt v1.5.19 file: `test_pd_v1519_plain_block.js` 9/9, `test_my_portfolio_v1518_cash_snapshot.js` 10/10, `gas_ledger_fx_failed_state.test.js` 11/11.
- `pytest` on every test that reads the script plus the consistency-check tests: **187 passed, 2 skipped**.
- `python3 scripts/audit_repository_workflows.py`: `errors=0 warnings=0`.

## S4 - Deploy and rollback
- **Deploy:** paste `apps_script/10_My_Portfolio.gs` (1.5.20) into the Apps Script project. No property is needed: the overlay is on by default.
- **Verify:** run `pdSelfTest` (line `exit plan stop (PD_EXIT_PLAN_STOP): on ... core: ok`); after the next refresh the Advisor Note of each holding ends `Stop SAR: Exit Plan ...`, and the next hourly `Consistency_Check` C9 reads MATCH.
- **Rollback:** set `PD_EXIT_PLAN_STOP` = `off` (no re-paste), or re-paste v1.5.19.

## S5 - Known limits
- A refresh before the day's first Exit Plan run shows the previous trading day's stop (named with its day in the note) until the next refresh.
- A holding that is not in the Exit Plan ledger keeps the engine's stop, with no note.
