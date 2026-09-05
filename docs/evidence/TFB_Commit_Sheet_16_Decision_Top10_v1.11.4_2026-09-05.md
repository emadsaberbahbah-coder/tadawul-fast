# Commit sheet — 16_Decision_Top10.gs v1.11.4 (P-78 holdings reach the gate in SAR · P-80 note) — 2026-09-05

| File | Where | Exists? | Delivered | Version |
|---|---|---|---|---|
| 16_Decision_Top10.gs | Apps Script project — replace the file body | replace | `16_Decision_Top10.txt` · CRLF · ES5 (acorn) OK · 0 smart quotes | v1.11.3 (`67a0442e33dd4d93…`) → **v1.11.4** |

Repo copy of this sheet (optional, new): `docs/evidence/TFB_Commit_Sheet_16_Decision_Top10_v1.11.4_2026-09-05.md`.

## Evidence
Alerts 14:36 / 15:03: "RPC.US is fundable by exit of **PFS 119 SAR**" — PFS is USD 118.60 = SAR 445. My_Portfolio has no SAR-labelled value column, so `dt10CollectHoldings_` sent the raw Position Value as `value_sar`: every USD holding reached the builder's Portfolio gate at 1/3.75 of its SAR value (HCI 5,629 instead of 21,126) — sector-cap context, rotation proceeds and the proposal text all skewed. Symbol-only exclusion never affected.

## Edits (10 anchored, each matched exactly once; 91/95 functions byte-identical; 0 added; 0 removed; 3 old lines altered)
- **P-78** `dt10HoldingFromRow_` (pure): optional `extra.iCcy` + `extra.fx` → a native value in a non-SAR currency × SAR-per-unit; SAR rows, rows without a currency, currencies missing from the map stay native (counted as unconverted). `dt10CollectHoldings_`: locates a `Currency`/`CCY` column and hands over `dt10FxRates_(ss)` (the `TFB_FX_LOOKUP` named range the ticket sizer already uses) **only when the value column is native**; Logger line `holdings -> gate: N row(s), M converted to SAR, K unconverted`. Payload schema unchanged. Toggle `DT10_V1114_HOLDINGS_FX` (default ON).
- **P-80** `dt10StabCore_`: "N ft-carried" counts only ft seats live in today's raw list with co=0 (grace-held ft seats read as GRACE; audit list unchanged).
- `dt10SelfTest()` +2 pure lines (26 total).

## Verification (node, GAS globals stubbed)
- 09-05 replay unchanged (re-run FAST-TRACK 1/3 suspended / HELD / day 3 ACTIVE); self-test 26 lines, 0 FAIL.
- End-to-end collector on a fake My_Portfolio (Currency column) + fake `TFB_FX_LOOKUP` (USD 3.7528, SGD 2.9420): HCI.US 5,629 → **21,125 SAR**, PFS 118.6 → **445**, 5023.SR 10,250 (SAR untouched), T82U.SI 5,852 → 17,217; kill-switch → v1.11.3 payload byte-for-byte.
- Existing `holdings row mapper: ok` fixture (566) unchanged.

## Post-paste checks (Emad)
1. Paste → `dt10SelfTest()` → `holdings value_sar via FX … : ok`, `stab note: grace-held ft seat … : ok`.
2. `refreshDecisionTop10()` → execution log shows `holdings -> gate: 7 row(s), 6 converted to SAR, 0 unconverted`; any rotation alert now quotes the holding in SAR (PFS ≈ 445 SAR). Note: TFB_FX_LOOKUP's USD cell must read 3.7528 (Run 2's cell fix) — with 3.8090 the conversion carries that error.
