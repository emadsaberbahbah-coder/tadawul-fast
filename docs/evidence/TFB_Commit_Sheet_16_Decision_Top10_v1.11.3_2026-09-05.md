# Commit sheet — 16_Decision_Top10.gs v1.11.3 (P-76 · P-77) — 2026-09-05

| File | Where | Exists? | Delivered | Version |
|---|---|---|---|---|
| 16_Decision_Top10.gs | Apps Script project — replace the file body | replace | `16_Decision_Top10.txt` · CRLF · ES5 (acorn ecmaVersion 5 OK) · 0 smart quotes | v1.11.2 (`72e3ccfeceba06f3…`) → **v1.11.3** |

Repo copy of this sheet (optional, new): `docs/evidence/TFB_Commit_Sheet_16_Decision_Top10_v1.11.3_2026-09-05.md`.

## Evidence (live board 14:32–14:36)
- `buildDecisionTop10Layout()` ("layout built", 14:32:01) `sheet.clear()`s and re-seeds every panel cell from `_Lists_Config`/built-in defaults → `Cash Available (SAR)` 3,825 → **100,000** (built-in `def`). The two refreshes that followed sized against phantom capital: Deployable 100,000 · Selected 5/10 · Capital Call 34,007 · four ~19.5k tickets fast-track-seated · AEFES.IS pushed to GRACE.
- Same board: the four suspended seats show Ticket/Shares `—` but **"Funds From: Cash 19,544 SAR"** — `dt10FastTrackSuspend_` never blanked `detail.funds_from` (BC-4 leak class; Python closed it in v4.28.0/v4.29.0).

## Edits (10 anchored, each matched exactly once; 90/93 functions byte-identical; 2 added; 0 removed; 3 old lines altered — the seed expression replaced by the pure helper call)
- **P-76** new `dt10PanelPreserve_(sheet)` (raw snapshot of non-blank operator values BEFORE the clear; label-checked; fresh sheet → `{}`) + pure `dt10PanelSeedValue_(item, defaults, preserved)` = preserved > `_Lists_Config` default > built-in; the builder logs `layout: preserved N operator input(s): …`.
- **P-77** `dt10FastTrackSuspend_` blanks `detail.funds_from` on a copy of `detail` (raw payload ticket untouched); board row col 23 and `_Selection_Log` col 26 both read `—`.
- Toggles (default ON): `DT10_V1113_LAYOUT_PRESERVE`, `DT10_V1113_SUSPEND_FUNDS`.
- `dt10SelfTest()` +2 pure lines.

## Verification (node, GAS globals stubbed)
- 09-05 replay unchanged from v1.11.2 (re-run FAST-TRACK 1/3 suspended / HELD / day 3 ACTIVE); self-test 0 FAIL; new lines `ok`.
- Fake-sheet test: existing panel → preserved `{Cash Available: 3825, Min R/R: 2}`; fresh sheet → `{}` → Cash seeds 100,000 exactly as before.
- Suspended seat: board Funds From `—`, log Funds From `—`, raw payload detail still `Cash 19,544 SAR`.

## Post-paste checks (Emad)
1. Paste → `dt10SelfTest()` → `suspension blanks Funds From (copy-on-write): ok` and `layout panel seed (preserved > default > built-in): ok`.
2. Type **3,825** into `Cash Available (SAR)` (still 100,000 from the 14:32 rebuild) → `refreshDecisionTop10()` → status line shows Deployable 3,825 and the suspended seats' Funds From reads `—`.
3. Never needed again, but if you ever re-run `buildDecisionTop10Layout()`, the Logger line `layout: preserved N operator input(s)` is the read-back.
