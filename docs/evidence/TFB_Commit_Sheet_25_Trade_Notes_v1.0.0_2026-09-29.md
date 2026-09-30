# TFB Commit Sheet — 25_Trade_Notes.gs v1.0.0 [PROGRAM v2 TRADE NOTES + DIVERGENCE LEDGER]

Date: 2026-09-29 (Tuesday) · Lane: GAS (Apps Script editor paste; repo mirror under `apps_script/`) · Build #4 of the day — **tomorrow's GAS slot pulled forward on the operator's "move to the next"** (three-lane cap exceeded by one, disclosed) · Protocol: One-Pass (new file — no base to pin)

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Repo HEAD at build | `b246fd2` (#625) |
| Base file | none — NEW file; no existing function, tab or cell touched. The announced name `25_Divergence_Log.gs` became `25_Trade_Notes.gs`: Program v2 names the target tab `_Trade_Notes` ("in the ledger Notes column until the `_Trade_Notes` tab exists"); the divergence ledger is the YES-rows of the same tab |
| Program v2 anchors | discipline #2 (5-line note per fill: thesis, evidence, benchmark, exit rule, size rationale — Appendix B); 09-14 governance answer (every divergence = register event, reason attributed to P-item / F-item / judgment, scored against outcome) |

## S2 — Root
09-28: AER.US 14 @ 148.18 and KRP.US 130 @ 14.68 filled outside the weekly board; 09-29 export: both ledger `Notes` cells empty; the divergence events D-1/D-2 have no tab. A divergence without written numbers is the failure mode the governance answer named.

## S3 — Change (new file, 443 lines, ES5, 0 non-ASCII)
| Function | Role |
|---|---|
| `tbTradeNotesSetup()` | creates `_Trade_Notes` (28-column header, frozen row) and `_Trade_Notes_Input` (label/value form, 21 note fields + 4 SCORE fields, dropdowns for Type / Side / Origin / Reason Class / Outcome); idempotent; a non-empty existing header is never overwritten |
| `tbLogTradeNoteFromInput()` | reads the form → `tbLogTradeNote(rec)` → on success clears the 21 note fields |
| `tbLogTradeNote(rec)` | validation (9 required fields; FILL needs side/qty/price; a divergence needs reason class + system verdict; date `YYYY-MM-DD`; enum checks) → Note ID `TN-YYYYMMDD-nnn` (per-trade-date counter from column A) → ONE appended row → `_Run_Log` stamp; returns `OK:<id>` / `FAILED:<reasons>`; nothing is written on a validation failure |
| `tbScoreTradeNoteFromInput()` / `tbScoreTradeNote(id, outcome, pnlSar, by)` | fills exactly the four Outcome cells (Outcome, P&L SAR, Scored At, Scored By) of one Note ID; unknown id / bad outcome → FAILED |
| `tbSeedD1_AER()` / `tbSeedD2_KRP()` | pre-fill the form with the MEASURED facts of the 09-28 fills (fill time/price/range, commissions, engine + PF verdicts from the 09-29 export, benchmark line, the REPLACED stop/TP levels as the exit rule, size % NAV, ledger ref); lines 1 Thesis and 5 Size Rationale are left blank on purpose — the note is the operator's |
| `tbTradeNotesSelfTest()` | pure: header shape, id counter, date part, validation matrix, row builder, divergence flag, Riyadh clock |
| pure helpers | `tbTrim_`, `tbPad3_`, `tbNowRiyadh_`, `tbIdDatePart_`, `tbNextNoteId_`, `tbIsYes_`, `tbValidate_`, `tbBuildRow_` |

Columns: Note ID · Logged At (Riyadh) · Type · Trade Date · Symbol · Side · Qty · Price · Ccy · Venue · Board Ref · Origin · Divergence · Reason Class · Reason Ref · System Verdict At Time · 1 Thesis · 2 Evidence · 3 Benchmark · 4 Exit Rule · 5 Size Rationale · Size % NAV · Outcome · Outcome P&L SAR · Scored At (Riyadh) · Scored By · Ledger Ref · Writer Version. Append-only by construction (scoring touches only the four Outcome cells). Script Properties `TFB_TRADE_NOTES_TAB` / `TFB_TRADE_NOTES_INPUT_TAB` rename the tabs. No ENV, no trigger.

Deliberate scope cuts: no menu entry (01_Menu.gs is paste-blocked); no automatic pull of fills from IBKR or the ledger (the note must be written by the human who diverged); Performance_Log outcome recording (exit price/fees/FX/dividends) stays the 10-06–10-09 build.

## S4 — Audits (×3 identical; 5 runs)
| Check | Result |
|---|---|
| Delivered SHA-256 | `03ead22a04e87f890c40e401948b045c864ecb50ba47fd5ca41364d9b6b95198` (443 lines, LF) |
| `node --check` (on a `.js` copy) + vm parse | PASS |
| ES5 / non-ASCII / smart quotes | 0 / 0 / 0 |
| Harness `tests/test_gas_trade_notes_v100.js` (REAL file in a vm with an in-memory Sheets model: ranges, values, appendRow, validations, frozen rows) | **25/25 PASS ×5, digest `e28934d7aed2`** |
| T1 | embedded self-test `trade notes core: ok`; 28 columns |
| T2 | setup creates both tabs (header + frozen row; 25 form rows + title; 5 dropdowns); second setup changes nothing; two `_Run_Log` rows |
| T3 | seed D-1 → form carries AER.US / 14 / 148.18; logging with blank lines 1/5 → `FAILED:missing thesis; missing size_rationale`, nothing appended; after filling both → `OK:TN-20260928-001`, row content verified (type, date, symbol, side, qty, price, origin OVERRIDE, divergence YES, JUDGMENT, D-1, thesis, 8.3 % NAV, empty Outcome, version), form cleared; seed D-2 → `OK:TN-20260928-002`, KRP system verdict carried |
| T4 | scoring `TN-20260928-002` → LOSS / −249 / timestamp / "rule: stop 13.95"; the other row untouched; SCORE block cleared, note block untouched; unknown id and bad outcome rejected |
| T5 | `_Run_Log` trail: one FAILED, two OK logs, one score OK |
| T6 | programmatic API to renamed tabs (`Notes_X`), upper-casing of symbol/side/origin, divergence NO; a Sheets Date cell for the trade date resolves to the Riyadh date (`2026-10-06`); a NO-TRADE divergence logs with empty qty |
| T7 | header guard: an empty pre-existing tab receives the header; a non-empty header is never overwritten |

## S5 — Delivery
| File | Destination |
|---|---|
| `apps_script/25_Trade_Notes.gs` | Apps Script editor — NEW file (Files ＋ → Script → `25_Trade_Notes`), paste the FULL file; also the repo mirror |
| `tests/test_gas_trade_notes_v100.js` | repo `tests/` (node) |
| `docs/evidence/TFB_Commit_Sheet_25_Trade_Notes_v1.0.0_2026-09-29.md` | repo `docs/evidence/` |

## S6 — Operator steps (today, after the paste)
1. Run `tbTradeNotesSelfTest` → `trade notes core: ok`.
2. Run `tbTradeNotesSetup` → two new tabs.
3. Run `tbSeedD1_AER` → open `_Trade_Notes_Input`, write **line 1 Thesis** (one falsifiable sentence) and **line 5 Size Rationale**, review the pre-filled facts → run `tbLogTradeNoteFromInput` → expect `OK:TN-20260928-001`.
4. Run `tbSeedD2_KRP` → same two lines → `tbLogTradeNoteFromInput` → `OK:TN-20260928-002`.
5. From now on: every fill → form → `tbLogTradeNoteFromInput` (Type FILL, Origin BOARD, Divergence NO for board tickets); every declined board ticket or non-board action → Type DIVERGENCE. Outcomes are scored at the exit rule with the SCORE block.
Read-back: two rows on `_Trade_Notes` with IDs TN-20260928-001/-002 and two `_Run_Log` rows `tbLogTradeNote OK`.
