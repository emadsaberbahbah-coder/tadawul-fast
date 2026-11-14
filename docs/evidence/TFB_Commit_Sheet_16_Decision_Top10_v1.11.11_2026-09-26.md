# TFB Commit Sheet — 16_Decision_Top10.gs v1.11.11 [P-144 EPOCH-KEYED STABILITY CLOCKS]

Date: 2026-09-26 (Saturday) · Lane: GAS (Apps Script editor paste) · Build #3 of the day · Protocol: One-Pass

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | the v1.11.10 delivery of 2026-09-26 (Build #1, P-168), sha `9edaf9f22d3ca2bbf2633d118aed1e1f0102dca5376e350796e0610a60783601`, 5,530 lines CRLF, 110 functions |
| Lineage | v1.11.10 is byte-built on Emad's live v1.11.9 paste (sha 5295762e…, zero drift vs the 09-16 delivery). **One paste of v1.11.11 deploys both P-168 and P-144** (v1.11.10 is contained verbatim) |

## S2 — Root (pinned on source + runs 36199188352 / 36231358321)
`dt10ApplyStability_` handed `dt10StabToday_()` (wall-clock UTC date) to the core; `dayAdvance = state.date !== today`. GitHub fired the 20Z slot at 22:59Z (3 h late) and the 04Z slot at 09:00Z (5 h late), so the first cockpit run after 03:00 Riyadh advanced every clock on a 9-hour-old Global_Markets epoch while the board was withheld as `aged:GM` — the P-144 mixed-epoch class, now daily.

## S3 — Change (8 anchored edits, each `count == 1`)
| # | Site | Edit |
|---|---|---|
| E1/E2 | header, `DT10_VERSION` | 1.11.11 (lockstep) + WHY block (ASCII only) |
| E3 | constants | `DT10_P144_FEED_KEY = 'TFB Feed Global_Markets'`, `DT10_P144_EPOCH_STATES = /^(OK\|PARTIAL\|PARTIAL_FRESH\|STALE_COV)$/` |
| E4 | new pure helpers after `dt10OutagePauseLegacy_` | `dt10EpochKeyLegacy_()` (kill property `DT10_P144_EPOCH_KEY_LEGACY='1'`), `dt10StabStampUtcDate_(s)` (UTC date of a stamp; explicit offset or `Z` honoured, naive = Riyadh), `dt10StabResolveEpoch_(feedRaw, stateDate, todayKey)` → `{key, src: feed\|held\|clock, held}` — rows-written states date the epoch; SKIPPED/FAILED/missing/unreadable hold the state's date; never backwards, never in the future; `held` = this run will not advance |
| E5 | new `dt10UvReadKeyed_(ss, key)` | keyed variant of the `_Status` L1:M60 reader (`dt10UvRead_` byte-identical) |
| E6 | `dt10ApplyStability_` | resolves the key (fail-open to wall-clock on any service error), passes it as the core's `today`, appends `epoch=<key>/<src>[(frozen)]` to the status note |
| E7 | `dt10SelfTest` | `'epoch key core: ok'` (12 resolver cases, parser cases, frozen vs advancing core replay) |

Semantics: aged feed → clocks frozen (the same-day re-run path, byte-identical); first run on a fresh epoch → one advance; three legs in one UTC day → one advance; a day with no refreshed epoch → no observation recorded. Composes with v1.11.10 (poisoned rows on a fresh epoch pause per symbol). DEFAULT ON; kill = the property above; rollback = paste v1.11.10 (9edaf9f2…) or v1.11.9 (5295762e…).

Scope cuts (stated): trigger time (08:10/08:30) and feed-age withholding untouched; the payload release-epoch stamp (P-144 full form) stays a backend item.

## S4 — Audits (×3 identical)
| Check | Result |
|---|---|
| Delivered SHA-256 | `0112efbdb1e779025095181dc74ca4a3e4c55c9804935997f9ed21def402ee17` (297,940 bytes, 5,732 lines pure CRLF, tail unterminated like base) |
| `node --check` | PASS |
| Functions | 110 → 114 (+`dt10EpochKeyLegacy_`, `dt10StabStampUtcDate_`, `dt10StabResolveEpoch_`, `dt10UvReadKeyed_`; **0 removed**); 3 base lines not verbatim = version line ×2 + the one core call (`dt10StabToday_()` → `epoch.key`) |
| Non-ASCII | multiset identical to base (0 new); 0 smart quotes; ES5 (0 let/const, 0 arrow functions) |
| Harness `tests/test_dt10_p144_epoch_key.js` (dual-tree, REAL module in vm, REAL `dt10ApplyStability_` end-to-end with the property store + `_Status` rows stubbed) | 24/24 PASS ×3, digest `dcaddd326ac6` ×3 |
| Golden negative (base v1.11.10) | on the 22:59Z (aged) epoch at wall-clock 09-26: PINFRA 1→2, 2222.SR 1→2 |
| Delivered, same inputs | key `2026-09-25/feed(frozen)`: no counter, hist or state.date change; then the real 04:18+03 stamp → one advance with ITRN/PINFRA/NVDA paused (P-168) and 2222.SR 1→2 on merit; a third run on the same epoch → frozen |
| SKIPPED / FAILED / missing feed | held on the state date (`epoch=2026-09-25/held(frozen)`) |
| Kill property | delivered tickets + state blob + note **identical to base** (wall-clock key, no token) |
| Regression | the P-168 harness re-run against v1.11.11: 23/24 identical (the only difference is its version-literal check, 1.11.11 vs 1.11.10); embedded `dt10SelfTest` prints the nine prior `core: ok` lines + `epoch key core: ok` |

## S5 — Delivery
| File | Destination |
|---|---|
| `16_Decision_Top10.gs` | Apps Script editor — paste the FULL file (supersedes the v1.11.10 paste; both builds ride) |
| `tests/test_dt10_p144_epoch_key.js` | repo `tests/` |
| `docs/evidence/TFB_Commit_Sheet_16_Decision_Top10_v1.11.11_2026-09-26.md` | repo `docs/evidence/` |

Deploy proof: run `dt10SelfTest` → `[DT10 v1.11.11]` + `epoch key core: ok` + `outage pause core: ok` + the three earlier ok lines.

## S6 — Read-back
- Any cockpit run while `TFB Feed Global_Markets` still carries yesterday's UTC date: status note ends `| epoch=<yesterday>/feed(frozen)`, `_Selection_Log` Run Info carries it, no Stability counter moves.
- First run after the fresh GM stamp: `| epoch=<today>/feed`, counters advance once; later runs that day: `(frozen)`.
- Under enforce-armed skipped legs: `| epoch=<state date>/held(frozen)`.
- Kill read-back: property set → no `epoch=` token, v1.11.10 behaviour.
