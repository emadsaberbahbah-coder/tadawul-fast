# TFB Commit Sheet — GAS 21_Portfolio_Ledger v1.5.0 — 2026-09-13

## Register item
**P-135** — ledger status-row totals (T2/U2) don't reconcile to column sums. **CLOSES P-135.**

## Root cause (proven, not assumed)
The paste of v1.4.1 contains **no code path that writes T2/U2** — no SUM, no
setFormula; `buildPortfolioLedgerLayout` writes only Title/Status/Legend/Headers;
`refreshPortfolioLedger` writes data rows + B2 status only. The 2,236 / 3,174
values are **orphaned manual entries** embedding a stale snapshot; nothing
maintained them as rows changed.

## Measured truth (evidence correction included)
Three parsers × two same-day exports, per-cell identical; frozen closed-row
cells byte-stable across exports (finalize-once verified in the wild):
- Realized, 30 closed (incl. div): **+1,075 SAR**
- Active (evening file): unrealized **+75**, total return **+880** (5023.SR moved in Sunday's Tadawul session; morning basis was +134 / +937)
- Lifetime: **+1,955 SAR** evening (+2,012 at morning prices)
- Orphan Δ: T2 +1,161 / U2 +1,162 — consistent with U2 = T2 + a once-current active total
- ⚠️ **Correction of record**: the morning brief reported realized **+1,157** / lifetime **+2,094** — a bug in my one-off measurement script (both my independent hand-sum and every subsequent parser agree on 1,075). The corrected figures govern. This incident is itself the argument for script-owned, tested totals.

## Fix (v1.4.1 → v1.5.0)
- `plTotalsFromGrid_(vals)` — pure, node-tested: Active rows sum UNRL + TOT;
  finalized Inactive rows sum the **frozen** REAL cell (finalize-once truth is
  READ, never recomputed); lifetime = closed realized + active total return.
- Post-refresh: one batched grid read → write `S2 "TOTALS →"`, `T2` realized
  closed, `U2` lifetime (formatted, script-owned; **first run replaces the
  orphans by design**); status line gains
  `totals: closed X · active Y · lifetime Z SAR` (export-visible read-back).
- Kill switch: Script Property `PL_TOTALS = off` restores v1.4.1 byte-path.
- Functions 53 → 55 (added `plTotalsFromGrid_`, `plTotalsOn_`); removed 0.

## Pins
- Base (operator paste) SHA256: `a912693ff627ad45…` (1,758 lines, CRLF)
- Delivered v1.5.0 SHA256: `e222fb380128e382…` (LF-normalized; Script Editor is line-ending agnostic)

## Audits
- 6 anchored edits, each `count==1` · `node --check` parse PASS · smart-quotes 0
- Dual-load harness (base + v1.5.0 in isolated VMs), ×3 identical digest `85fe7e24…`:
  - base genuinely lacks the totals path (regression guard)
  - **today's real evening export** → exactly {1,075 / 75 / 880 / 1,955 / 30 closed / 7 active}
  - golden-negatives: orphans 2,236/3,174 **and** the bad interim 1,157/2,094 both rejected
  - synthetic edge battery: blank-symbol skip, dash cells, pending-close (REAL=—) excluded, mixed grid

## Deploy (operator — 3 steps)
1. Script Editor → open `21_Portfolio_Ledger` → select-all → paste the delivered file → Save
2. Run `refreshPortfolioLedger` once (menu or editor ▶)
3. Read-back: T2/U2 flip from 2,236/3,174 to **1,075 / ~1,955**, and the status line shows the `totals:` bit. Rollback: Script Property `PL_TOTALS=off` (or re-paste base)
