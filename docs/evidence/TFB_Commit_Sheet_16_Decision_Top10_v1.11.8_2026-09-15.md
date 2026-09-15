# TFB Commit Sheet — 16_Decision_Top10.gs v1.11.8 — 2026-09-15

**Item:** P-142 — FUNDING-NARRATIVE CONTAINMENT under WITHHELD (red-team F01, adjudicated ACCEPT 2026-09-15; operative strings re-verified by Claude on the 2026-09-15 08:08 export before adjudication).

## Base pin (S1)
| | |
|---|---|
| Base | 16_Decision_Top10.gs **v1.11.7** — Emad's live-editor paste 2026-09-15 |
| Base sha256 | `4b379071ae438133…` — **byte-identical to the 2026-09-12 v1.11.7 delivery** (zero drift since deploy) |
| Base shape | 5,009 lines, pure CRLF, 101 top-level functions, ES5 |

## Defect (WHY)
With `FEED NOT ACTIONABLE — SIZING WITHHELD` on the 08:08 board, the page still rendered: ALERTS `capital_call` ("Deposit ≥ 37,650 SAR … IGG.L, GBCI.US, VNOM.US") and `unfunded_candidates`, plus NEAR-MISS Funding rows carrying "CAPITAL_CALL: deposit ≥ 5,000 SAR … (cash 50 SAR)" — figures the backend's internal seat-funding pass computed against the very allocations the gate suppressed (SEAT-CHECK "kpi 2 funded / gain 17,395 vs board 0 exec" is the same leak). The v1.11.5/6 withheld-truth pass covers the KPI strip only; these strings arrive in `payload.alerts` / `payload.near_miss` and were rendered verbatim.

## Fix (WHAT)
Render-side containment gate at the alerts/near-miss seam, applied only when `dt10OutputStatus_(payload) === 'WITHHELD'`:
- alerts of type `capital_call` / `unfunded_candidates` dropped;
- NEAR-MISS funding rows (gate `Funding`, or text carrying CAPITAL_CALL/deposit — belt for wording drift) keep symbol/gate/verdict; `current` → `—`, `required` → "WITHHELD — funding not evaluated (feed not actionable)", `improve_note` → "Re-evaluated when the feed is actionable";
- ONE disclosure alert `funding_withheld` with the suppressed count appended — the **countable read-back**; absent on clean days.
Pure copies only — the payload is never mutated; idempotence hardened (already-transformed rows uncounted; disclosure never duplicated).

**Gate:** DEFAULT ON; kill switch Script Property `DT10_FUNDING_WITHHOLD_LEGACY = '1'` restores v1.11.7 rendering verbatim (OFF state IS the defect — P-127/P-130 default-ON precedent; operator veto preserved). Rollback = set the property (or re-paste v1.11.7).

## Deliberate scope cuts (recorded)
1. `HELD` / `QUALIFIED_UNFUNDED` states render **unchanged** — actionable-feed states where funding info is legitimate operator input.
2. Status-line KPI-CHECK / SEAT-CHECK internal disclosures kept — diagnostic truth, not solicitation.
3. Compute-level containment of the backend seat-funding pass itself = **F02a/F15 scope** (release-epoch work), not this build.
4. Backend payload and Selection Log untouched.

## Edits (all anchored, count==1 asserted)
E1 header version · E2 changelog WHY-block (inserted above v1.11.7, all prior blocks verbatim) · E3 `DT10_VERSION` → '1.11.8' · E4 five new functions after `dt10AlertToRow_` (`dt10FundingWithholdLegacy_`, `dt10IsFundingAlert_`, `dt10IsFundingNearMiss_`, `dt10ContainFundingCore_`, `dt10ContainFunding_`) · E5 render integration (3 lines + comment, immediately after `alerts` extraction) · E6 `dt10SelfTest` gains the pure check → line **`funding containment core: ok`**.

## Audits (S4)
| Check | Result |
|---|---|
| Anchors | 6/6 applied, each count==1 |
| `node --check` | PASS |
| Functions | 101 → **106** (+5 named, **0 removed** — set-diff proof) |
| Smart quotes | 0; **no new non-ASCII chars introduced** (em-dash only, house style) |
| ES5 | no `const`/`let`/arrow/template-literal in code (word "let" in 2 prose comments only) |
| CRLF | 5,199 lines, CR == LF == 5,199 (pure CRLF preserved) |
| Delivered sha256 | `cb86852dfdb6bbd1…` |

## Harness (real-module, ×3)
`tests/test_dt10_p142_containment.js` — extracts the **shipped** functions verbatim from the delivered file (no stand-ins) and runs the **verbatim 2026-09-15 fixtures** (4 real alerts incl. the 37,650 capital_call; 13 real near-miss rows: GBCI/VNOM/IBOC Funding + 10 Diversification):
- T1 WITHHELD: suppressed **5** (2 alerts + 3 rows), disclosure `funding_withheld count=5`, missing_fx/missing_valuation pass through, 10 Diversification rows byte-untouched
- T2 EXECUTABLE / T3 HELD / T4 kill-switch: **original array references returned** (identity — byte-identical by construction)
- T5 empty inputs: no disclosure invented · T6 purity: inputs unmutated · T7 idempotence: second pass changes nothing
**T1–T7 PASS ×3, identical digest `031a756796c15e96`.** Embedded self-test additionally replayed in node against the shipped file: `funding containment core: ok`.

## Deploy (operator)
1. Apps Script editor → open `16_Decision_Top10.gs` → select-all → paste the delivered file → Save.
2. Run `dt10SelfTest` → expect **`funding containment core: ok`** AND **`cash source core: ok`** (v1.11.7 checks all carried).
3. No property needed (default ON). Kill switch only if vetoing: Script Property `DT10_FUNDING_WITHHOLD_LEGACY = 1`.

## Read-back (S6)
Next **WITHHELD** board after deploy: ALERTS contains **zero** `capital_call`/`unfunded_candidates` rows and **one** `funding_withheld` row with the suppressed count; NEAR-MISS Funding rows show the WITHHELD text. On an EXECUTABLE/HELD day nothing changes — the read-back waits for the next withheld day (state-dependent by design). P-142 → CLOSED on that export.
