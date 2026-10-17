# TFB Commit Sheet — opportunity_builder.py v1.28.0: earnings-proximity gate, dark (T5, builder half)

Date: 2026-10-17 (Saturday) · Lane: Render · Brief: Codex Brief 2026-10-17 v2 §3 **T5** (new, observe first) · Cockpit half (`earn-held n` token + FT-suspend on `sizing_hold` seats) ships with the DT10 bump

## S0/S1 — Base pin
Branch `claude/new-session-twoifp` on `1615775` (T1) ← `d6a9548` ← `cf8e875` ← `4e88ae6` ← `fa42a55` ← main `11ef134`.

| File | Base | Delivered | Change |
|---|---|---|---|
| `core/analysis/opportunity_builder.py` | sha256 `39ee6a565bb0adb249869c1c0bd71cdc29a6dba682507e2c485d9e78f11093c4` (v1.27.0) | sha256 `be800fbee067802312f6bd5675376e0e0d173756a9163d8fb081c4018f407ed3` (v1.28.0) | **+158 / −2** (12 anchored edits, `count == 1`); removed lines = the two version literals |
| `tests/test_ob_earnings_gate_t5.py` | — | sha256 `283110e57e0755eca79932a6d30858faf73dea67ba66580e5e4e41968818903b` | NEW, 38 tests |
| `.github/workflows/ci.yml` | — | — | +1 lean-list line |

## S2 — Why
`earn ⚠4/10` on the 17-Oct board (VICI ≤11d, GLPI ≤12d, EPRT ≤4d, AX ≤12d) is advisory text only — the cockpit's v1.6.0 earnings tag prefixes the Advisor Note and nothing stops a sized ticket from being issued into an earnings print. The brief asks for a gate: observe appends `[earn-observe] Nd`; enforce **suspends sizing** (FT-suspend vocabulary, P-145 class) until Days To Earnings > N (suggested N = 5).

## S3 — What
- `TFB_T10_EARNINGS_GATE = off (default) | observe | enforce`; `TFB_T10_EARNINGS_MIN_DAYS` (default 5, clamped 0..60).
- Row aliases `days_to_earnings` (`daystoearnings`, …) and `next_earnings_date` (`nextearningsdate`, …): the same two Calendar_Events fields the tracker merges onto Signal_History rows. `normalize_candidate` carries `cand["days_to_earnings"]` (explicit days win; else ISO date − today; else None).
- Gate `Earnings Proximity` (registered after `Timing (52W)` in `GATE_ORDER`) — **always PASSED** in both modes. Reason: an earnings hold is a pause, not an exit; a failed gate would de-qualify the seat and the stability clocks would read it as a gate removal (the T4 clock question). The gate carries the trace: `[earn-<mode>] 4d <= 5d` inside the window, `12d ok` outside, `n/a (no earnings date)` unknown; `earnings_detail {days, min_days, within}`.
- F-1b seam (after selection, before KPIs): in-window **audit rows** get one countable tag beside the legacy failure_reason (observe and enforce); under enforce the in-window **tickets** are marked `sizing_hold="earnings"`, `earnings_hold_days=N`. The builder does not re-size (capital math byte-identical); the cockpit applies the dash vocabulary to those seats (`dt10FastTrackSuspend_` class) and prints `earn-held n`.
- `meta["earnings_gate"]` read-back (`mode, min_days, evaluated, unknown, within, held`) on the certified and the withheld path, only when armed.
- Default off: gate absent, no tag, no meta key — byte-identical to v1.27.0 save the version stamp.

### Data reality, stated
The live route assembles the candidate pool from the market pages (the cockpit POSTs criteria and portfolio, not rows), and those rows carry **no earnings field** — so on today's route every row reads `n/a` until one of: (a) the sync publishes `Days To Earnings` onto the market pages from Calendar_Events; (b) the route merges `Calendar_Events` (the tracker's `_load_calendar_context` + `_merge_calendar_context` pattern, 60 lines, sheet read already available to the sync); (c) the route calls `core.providers.calendar_provider.fetch_event_context` for the **qualified** rows only (24 today; the full pool is too large per request). (b) is the recommended follow-up and is a route change, listed separately. The cockpit already holds the earnings map (`dt10EarningsMap_`), so the cockpit half can enforce the hold on its own data today.

## S4 — Audits
| Check | Result |
|---|---|
| Anchored edits 12/12; `ast.parse` | PASS |
| New tests: mode/min-days readers incl. clamps; 9 row-parsing cases (explicit days, header spelling, ISO/ISO-T dates, past dates, blanks, junk, precedence); gate note/detail table incl. HOLD wording and n/a; GATE_ORDER position; off byte-identical (no gate, no tag, no meta, no `sizing_hold`); observe tags only in-window rows (incl. a WATCHLIST row, after its legacy reason), verdicts unchanged, tickets untouched, meta counters exact; enforce marks exactly the in-window tickets with days, seats unchanged, `suggested_sar` unchanged, counters; wider window via env; withheld path read-back | **38 passed** |
| Existing builder suites (`test_opportunity_builder`, blocked invariant, every `test_ob_*`, selector, KPI labels, rel-cluster tag) | **all passed** |
| ruff | PASS |

## S5 — Delivery
`core/analysis/opportunity_builder.py` · `tests/test_ob_earnings_gate_t5.py` (NEW) · `.github/workflows/ci.yml` (+1) · this sheet.

## S6 — Arming / read-back (Emad's ENV, §8)
1. Deploy (nothing changes). 2. `TFB_T10_EARNINGS_GATE=observe` on Render: `meta.earnings_gate` appears; with the data path in place, `[earn-observe] Nd` on VICI/GLPI/EPRT/AX rows and `within=4`; no sizing change (acceptance). 3. `enforce` after one observe cycle: those seats carry `sizing_hold=earnings` and the cockpit shows `earn-held 4` with dashes on the sizing fields. 4. Rollback: unset.

## Known limits
- Days are calendar days as Calendar_Events publishes them; the brief's "5 trading days" maps to `TFB_T10_EARNINGS_MIN_DAYS=7` if a strict trading-day reading is wanted.
- Until the route carries the calendar fields, the builder half is a dormant contract; the cockpit half is the live lever.
