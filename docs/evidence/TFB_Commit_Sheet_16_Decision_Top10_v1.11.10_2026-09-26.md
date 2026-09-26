# TFB Commit Sheet — 16_Decision_Top10.gs v1.11.10 [P-168 OUTAGE-EPOCH CLOCK PAUSE]

Date: 2026-09-26 (Saturday) · Lane: GAS (Apps Script editor paste) · Build #1 of the day · Protocol: One-Pass

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base file | Emad's live paste of `16_Decision_Top10.gs` (v1.11.9), 2026-09-26 ~10:56 Riyadh |
| Base SHA-256 | `5295762e4e43d1fbab9f91e8a0ff01f3b881ffd5765cd7cde98cac275bb9ed3b` |
| Drift vs the 2026-09-16 v1.11.9 delivery | **zero** (byte-identical; 275,330 bytes, 5,323 lines pure CRLF, last line unterminated, 108 top-level functions) |
| Version stamp proof | Run_Log 2026-09-19 08:07:43 cockpit stamp `{"version":"1.11.9"}`; dt10SelfTest paste 2026-09-22 (three ok lines) |

## S2 — Root (pinned on source + the 2026-09-26 export)
`dt10StabCore_` counter pass (base L2160–2173): on a day-advance (UTC date key) a member absent from `rawSet` and outside `T10 rank_buffer` takes `co += 1`; `hardOut` fires on `structural_block`/verdict regex. Nothing distinguishes "the row's fetch failed" from "the name missed on merit". 2026-09-26: the recovery replay wrote 6,302 GM rows `fetch_failed:HTTP 402` (DQ 55, BLOCKED); the 06:41 day-advance run found the seats absent from the audited 500 → ITRN 0→1/3, NVDA 0→1/3, PINFRA 1→2/3, 2222.SR 1→2/3. Same class exited ITRN/CRC/PINFRA/ADAM on the 2026-09-24 02:01 board.

## S3 — Change (17 anchored edits, each `count == 1`)
| # | Site | Edit |
|---|---|---|
| E1 | header | version 1.11.9 → 1.11.10 (lockstep) + v1.11.10 WHY block (ASCII only) |
| E2 | `DT10_VERSION` | `'1.11.10'` |
| E3 | constants | `DT10_P168_MAX_PAUSE_DAYS = 5`, `DT10_P168_OUTAGE_RE = /fetch_failed/i` |
| E4 | `dt10StabParseState_` | reads `op` (consecutive paused day-advances); written only when > 0 |
| E5 | new pure `dt10OutageMapFromPool_(rows)` | `{SYM:true}` for pool rows whose `Warnings` matches the regex; never throws. `dt10StabCore_` gains optional 7th arg `outage` |
| E6 | counter pass | PAUSE leg: symbol with a clock (member or `ci>0`), absent from raw, marked outage, `op < 5` → ci/co untouched, no score point, `op += 1`; else legacy statements verbatim (re-indented); non-outage day-advance drops `op` |
| E7 | membership pass | paused member → survivor (no hardOut / soft-exit) |
| E8 | `heldByGrace` | additive `outage_pause` key |
| E9 | GRACE label | `' - outage pause n/5'` suffix (paint + SelLog marker key on the `GRACE` prefix), ghost note reason, countable `_p168_paused` |
| E10 | note/audit | `'N outage-paused'` token; `audit.outage_paused` |
| E11 | kill switch | `dt10OutagePauseLegacy_()` — Script Property `DT10_P168_OUTAGE_PAUSE_LEGACY='1'` (mirrors `dt10GraceSizingLegacy_`, never throws) |
| E12 | `dt10ApplyStability_(payload, panel, outage)` | threads the map as the 7th core arg; null under kill or no sheet pool |
| E13 | `refreshDecisionTop10` | `dt10Outage = dt10OutageMapFromPool_(pool.rows)` once after the pool collect; passed at the apply call |
| E14 | `dt10SelfTest` | `'outage pause core: ok'` (pure; map / pause / cap / legacy / same-day frozen) |

Deliberate scope cuts: ADD-confirmation counter half of P-168 = portfolio_actions (Python, v1.13.0 scope); feed-level aged/withheld epochs = P-144 (not folded in — pausing every clock on every raced morning would stall hysteresis); no rows/columns/schema changes; `Max Per Sector` 2→3 is a panel cell, not code.

DEFAULT ON (the OFF state is the defect — P-145/P-142 precedent). Kill = the Script Property above; rollback = paste v1.11.9 (sha 5295762e…).

## S4 — Audits (×3 identical)
| Check | Result |
|---|---|
| Delivered SHA-256 | `9edaf9f22d3ca2bbf2633d118aed1e1f0102dca5376e350796e0610a60783601` (287,330 bytes, 5,530 lines pure CRLF, tail unterminated like base) |
| `node --check` | PASS (base and delivered) |
| Functions | 108 → 110 (+`dt10OutageMapFromPool_`, +`dt10OutagePauseLegacy_`; **0 removed**) |
| Line audit | 18 base lines not verbatim = 3 version/signature bumps + 15 counter-pass lines re-indented inside the new else (statements identical); all WHY blocks carried |
| Non-ASCII | multiset identical to base (1,435 chars; 0 new); 0 smart quotes |
| ES5 | 0 `let/const`, 0 arrow functions outside comments |
| Harness `tests/test_dt10_p168_outage_pause.js` (dual-tree, REAL module in vm with service stubs, real 2026-09-26 pool rows 9,791 + real audit 500) | 24/24 PASS ×3, digest `88c35745393f` ×3 |
| Golden negative (base) | reproduces the live 06:41 board: ITRN 0→1, PINFRA 1→2, NVDA 0→1, 2222 1→2; day 2 soft-exits PINFRA + 2222.SR |
| Delivered on the same inputs | ITRN/NVDA/PINFRA paused (op 1, clocks unchanged, `GRACE (n/3 missed) - outage pause 1/5`, `_p168_paused`), 2222.SR (clean yahoo row) 1→2 on merit; note `3 outage-paused` |
| Kill path | tickets + state blob byte-identical to base (no `op` keys written); note identical |
| Bounded pause | `op=5` → the miss counts (co 1, op 6); a clean day-advance drops `op` |
| Same-day re-run | frozen (state identical, no token) |
| Embedded self-test | replayed in node on both trees: delivered prints `outage pause core: ok` + the eight prior `core: ok` lines; base unchanged |

## S5 — Delivery
| File | Destination |
|---|---|
| `16_Decision_Top10.gs` | Apps Script editor — paste the FULL file over v1.11.9 |
| `tests/test_dt10_p168_outage_pause.js` | repo `tests/` (node; `--export-dir` points at a browser export folder) |
| `docs/evidence/TFB_Commit_Sheet_16_Decision_Top10_v1.11.10_2026-09-26.md` | repo `docs/evidence/` |

Deploy proof: run `dt10SelfTest` → `[DT10 v1.11.10]` header + `outage pause core: ok` + `grace sizing core: ok` + `funding containment core: ok` + `cash source core: ok`.

## S6 — Read-back (first day-advance run after the paste)
- If the pool is still 402-poisoned at that run: the three GM seats print `GRACE (n/3 missed) - outage pause 1/5` with n unchanged, status note carries `3 outage-paused`, `_Selection_Log` Stability column carries the suffix, 2222.SR clocks move on merit.
- If the 04Z sync healed the pool first: no `outage pause` labels, clocks move normally — the read-back then lands on the next storm (the token is countable; absence is the correct output).
- Kill read-back: property `DT10_P168_OUTAGE_PAUSE_LEGACY=1` → v1.11.9 labels and clocks.
