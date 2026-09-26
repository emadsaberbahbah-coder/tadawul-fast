# Commit Sheet — 16_Decision_Top10.gs v1.11.7 "P-134: COCKPIT CASH SOURCE-OF-TRUTH"
**Date:** 2026-09-12 · **Protocol:** One-Pass adapted for GAS (CRLF-exact anchors, node syntax check, function-inventory proof, extracted-function harness) · **Register item:** **P-134** — the cockpit sized its CAPITAL_CALL against 9,026 SAR while Portfolio_Decision carried 23,242.50 the same day (Δ 14,216).

## Identity
| | |
|---|---|
| Destination | Apps Script editor → `16_Decision_Top10.gs` (paste full file; also mirror to repo `apps_script/` if you version it) |
| Base | v1.11.6 · SHA256 `0750f862ca88d9837f9f…` (your paste, 4,858 lines, CRLF) |
| Delivered | v1.11.7 · SHA256 `4b379071ae4381339ba579aa1c2f1f16…` · 5,005 lines · **CRLF integrity proven 5,005/5,005** |
| Tests | `test_dt10_cash_source.js` (N1–N4, node, runs the extracted REAL functions) + `dt10SelfTest` extended in-file |
| Companion | `00_Config.gs` v1.12.6 read, **not modified** — it holds no format registry, so the vNEXT percent-format extension still needs the Reformat-owning file (05_Refresh or similar) |

## Root cause — REVISED on source (supersedes the layout-reset theory)
The v1.11.3 P-76 preserve fix (`dt10PanelPreserve_`) works exactly as designed — which is the problem: it **faithfully preserves a STALE operator entry** across every rebuild. The cockpit and the PF page each keep their own operator-maintained cash cell, nothing reconciles them, and the cockpit's copy is the one that sizes tickets and issues deposit calls (`cash_available_sar: Number(panel['Cash Available (SAR)'])`, payload line ~4050). On 2026-09-12 that meant a CAPITAL_CALL computed against 9,026 while real cash was 23,242.50.

## What it does
Script Property **`DT10_CASH_SOURCE` = panel (absent/default, byte-identical) | observe | portfolio**:
- `dt10CashScan_` (pure) finds the PF page's `PF: Cash Available SAR` label in the top 15×30 block and reads the value cell to its right — comma-tolerant, and **the KPI header `Cash (SAR)` cannot match** (normalized-contains test on "cashavailable").
- **observe:** panel value still sent; the status line gains `| cash=panel 9026 (PF 23243, Δ14217 observe)` — the divergence becomes visible on every refresh.
- **portfolio:** the PF value is sent; panel becomes the disclosed fallback whenever the PF cell is unreadable (`| cash=panel … (PF unreadable: sheet-missing…)`), fail-open by construction.
- `dt10CashChoose_` (pure) owns the decision + note, so the whole money path is unit-tested; `dt10SelfTest` gains a `cash source core` check built on the live exported row shape.

## Harness catch worth recording
The N2 negative battery caught a real defect in my first scanner: **a label with a blank/missing neighbor cell parsed as cash = 0** — in portfolio mode that would have sized against phantom-zero cash. Fixed in-module (blank neighbor ⇒ keep scanning), re-proven ×3. The harness paid for itself before the file ever reached the Sheet.

## Audit results
| Check | Result |
|---|---|
| `node --check` (V8 syntax) | PASS |
| Function inventory | removed NONE; added `dt10CashSourceMode_`, `dt10CashScan_`, `dt10CashFromPortfolio_`, `dt10CashChoose_` |
| CRLF integrity | 5,005/5,005 lines CRLF (base 4,858/4,858) |
| **N1** mode reader: default/junk→panel; observe/portfolio bind case-insensitively | PASS ×3 |
| **N2** scan on the **live exported PF rows** → 23,242.5 @ row 5; KPI header rejected; blank-neighbor + empty-matrix negatives clean | PASS ×3 |
| **N3** choose: panel mode byte-identical (empty note); observe log-only with Δ; portfolio switches; unreadable falls back to panel | PASS ×3 |
| **N4** default-mode payload value === legacy `Number(panel)||0` exactly | PASS ×3 |
| Triple-run digest | `0ca6bb7b5d95c93a` identical ×3 |

## Deploy + arming (extends the Runbook; GAS lane = Script Properties, not Render)
1. **Paste the full file** over `16_Decision_Top10.gs` in the Sheet's script editor; run `dt10SelfTest` once — the report must include `cash source core: ok`.
2. **Observe run:** Project Settings → Script Properties → add `DT10_CASH_SOURCE = observe`. Read-back = the `| cash=panel … (PF …, Δ… observe)` segment on the next refresh's status line.
3. **Enforce (separate sitting):** set `DT10_CASH_SOURCE = portfolio`. Read-back = the CAPITAL_CALL and deployable math finally agreeing with the PF page's cash; the stale-panel Δ disclosed until you clear the old panel entry.
4. Rollback anywhere: delete the property (mode falls back to `panel`, byte-identical).

## Still queued for GAS
The vNEXT percent-display-format extension (`Upside %`, `Percent Change`, `Upside/Downside %`) — needs the file that owns **Reformat All Sheets** (00_Config carries only the menu label). Paste that file and it's a 10-minute follow-up.
