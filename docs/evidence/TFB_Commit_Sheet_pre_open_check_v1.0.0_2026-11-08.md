# Commit Sheet: the pre-open check (pre_open_check 1.0.0 + workflow), 2026-11-08

**Scope.** Owner, 8 Nov 2026 (verbatim: "there is last week big loss happend how we can avoid that in the future"; "ok let's do the requirment and implement it the project"). Item 2 of the post-mortem plan (`TFB_PostMortem_Week_2026-11-02_to_06.md`, section 4b): a re-check of the executable tickets against S&P futures and each name's pre-market quote about 30 minutes before the US open. Data lane only; the cockpit hold is a later, default-off change.

## Why
| Finding (2-6 Nov, measured) | Why it matters | Fix |
|---|---|---|
| SNDA.US and IBOC.US were bought at the 17:30 Riyadh open of 5 Nov, a day the futures had already marked down (SPY closed -1.1 %, VIX +15 %); SNDA hit its stop 2 h 15 min later. Archive: USD seats taken the day after a >= 1.5 % SPY drop lost 3.5 % at +5 sessions, 1 win in 7 (thin, one-sided). | The tickets are computed the night before and cannot see the open. Nothing between the night build and the 17:30 buy reads the market. | `scripts/pre_open_check.py` v1.0.0 writes the `Pre_Open_Check` tab inside [open - 75 min, open + 15 min] of the NYSE session: ES=F vs settlement (WAIT at <= -1.0 %), each executable US ticket's pre-market last vs its planned price (WAIT at a 3 % gap either way), GO inside the band, CHECK when a quote is missing or unread - never a silent GO; SKIP for another session or a non-executable seat. |
| Winter and summer time move the US open between 17:30 and 16:30 Riyadh; a GitHub cron cannot follow DST. | A fixed 17:00 Riyadh firing lands after the open half the year. | The workflow fires at 13:00 and 14:00 UTC; the script acts only inside the window (exchange_calendars XNYS, the pinned 4.13.2), so exactly one firing lands in each season and a US holiday writes nothing. |

## Changes
| Change | Tests | Arm |
|---|---|---|
| `scripts/pre_open_check.py` (new, v1.0.0): `read_tickets` (header below the banner; executable = Ticket SAR > 0 and Shares > 0; planned price = Price), `parse_prepost` (Yahoo v8 chart, pre / regular / post session, the last print at or before now), `market_verdict`, `ticket_verdict`, `build_rows`, `preopen_window`, `Quotes` (query1 then query2, 404 final, 2-failure breaker reported as CHECK rows), `write_tab` (whole-table replace, bounded 200, fail-closed on an oversized prior tab); knobs `TFB_PREOPEN_ES_WAIT_PCT` (0.25..5, default 1.0), `TFB_PREOPEN_GAP_PCT` (0.5..10, default 3.0), `TFB_PREOPEN_SHEET`, `TFB_PREOPEN_PAGE`, `TFB_PREOPEN_TIMEOUT_S` | `tests/test_pre_open_check.py` P1-P6 (6 passed; 8 one-line mutants each caught: one-sided gap, exclusive WAIT threshold, missing quote reading GO, unread market ignored, late window dropped, prints after now, zero-shares seat sized) | workflow schedule |
| `.github/workflows/pre_open_check.yml` (new): cron `0 13,14 * * 1-5` + dispatch (writes only with write=true; ignore_window=true rehearses); `contents: read`; knobs from repository variables | P6, test_repository_workflow_audit | - |
| `scripts/verify_deployment.py` pins pre_open_check 1.0.0; `.github/workflows/ci.yml` 1.1.70 runs the suite in the lean lane (1.1.68 / 1.1.69 claimed by #935 / #936) | manifest pins, workflow audits | - |

## Pre-merge checks
- **Golden negative (P3, P5): the 5 Nov shape.** ES=F -1.20 % -> market WAIT; every executable US ticket reads WAIT whatever its gap; the Tadawul seat and the research seat read SKIP. A calm market (+0.20 %): SNDA -1.97 % GO, IBOC -3.60 % WAIT, BRK.B no pre-market print CHECK.
- **Rules.** The gap band holds both ways (a +3.54 % gap up buys above the plan: WAIT); the WAIT threshold is inclusive at -1.00 %; the band and the threshold are knobs and move the verdicts; a print after the regular open is a regular-session print (a late runner still reads the real open); prints after `now` are ignored.
- **Never a silent GO.** A missing print, an unread quote (Yahoo failure or the breaker) and an unread ES=F all read CHECK (a gap WAIT stays WAIT; a market WAIT still holds).
- **Window (P4).** Winter 9 Nov: 14:00 UTC inside, 13:00 outside, 14:44 inside (late runner), 14:46 outside; summer 6 Jul: 13:00 inside, 14:00 outside; Thanksgiving: no session.
- **Kill.** Outside the window nothing is read from Yahoo and nothing is written; a dry run writes nothing; a broken page refuses (exit 2); an oversized prior tab is preserved.
- **Not done here.** The cockpit does not read the tab yet (a `DT10_GUARD_PREOPEN` hold in the ticket guard is the follow-up once 16_Decision_Top10 v1.27.0 is pasted); the decision-feed release is untouched. Pre-market prints on thin names are often absent - those rows read CHECK, which is the honest answer.

## Post-merge order (lead)
1. Dispatch `pre_open_check.yml` once with write=false, ignore_window=true (a rehearsal; prints the verdicts, writes nothing) and read the log.
2. Let the 13:00 / 14:00 UTC schedule write the tab from the next US session; read `Pre_Open_Check` at ~17:00 Riyadh before any buy: WAIT = skip the day's new buys (the tickets recompute at the next sync), CHECK = read the quote yourself, GO = proceed as planned.
3. Optional knobs (repository variables): `TFB_PREOPEN_ES_WAIT_PCT`, `TFB_PREOPEN_GAP_PCT`. Rollback: disable the workflow.

Not a licensed financial adviser; the owner decides and executes every trade. No broker order; no executable count increases (the tab only adds a WAIT / CHECK reading).
