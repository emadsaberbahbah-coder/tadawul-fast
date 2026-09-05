# Universe hygiene worklist — 2026-09-05 (P-70 / P-63) — DECISION SHEET, no action taken

**Where the universe lives:** the pages' own `Symbol` column is the request universe (`run_dashboard_sync` doc: "the sheet Symbol column is the symbol source"). Hygiene = editing rows in the pages, i.e. operator sheet edits. **Freeze note:** every deletion shifts the distribution baselines (row counts, `fresh_cov`, provider-target share) that W1B reads on scheduled runs — apply in ONE sitting, log the before/after counts, and tag the day as a baseline boundary in the register.

| Class | Page | Rows | Proposed action | Risk |
|---|---|---|---|---|
| DEAD_SYMBOL | Global_Markets | 113 | delete rows still blank (no price, no name) on the 09-06 export — **2-run rule** before deleting | low: all BLOCKED today; benefit: GM leg time (48 min) and pool noise |
| BARE_TWIN | Global_Markets | 3 (YUM, OTIS, FCX) | delete the bare rows; keep `.US` | none (bare rows carry the degraded synthetic forecast) |
| STALE_1D | Global_Markets | 9 | monitor (WSR.US, EEX.US, INHD.US, Z74.SI, GARAN.IS, SON.LS, 8725.T, 2531.T, FTFT.US — FTFT is ID-FIREWALL out_stripped) | — |
| SHIFTED_ROW | Commodities_FX | 1 | replace the "Copper Futures" list row with symbol `HG=F` (row is shifted one column; stale since 08-13, HTTP 422) | none |
| UNPRICED | Commodities_FX | 18 | delete, or replace with priced aliases (8 crypto `-USD`, `DX=F`, `XAUUSD=X`, 8 exotic crosses) | none |
| DEAD_SYMBOL | Mutual_Funds | 1 (MUAA.US, matured 2012) | delete | none |

Not in this sheet (needs code, not rows): holdings symbols bare in My_Portfolio/ledger (P-63) — engine-side canonicalization; do **not** rename ledger keys before `21_Portfolio_Ledger.gs` is reviewed. Taxonomy (294 CFX rows and all MF rows as "Equity") is a scoring-router item (P-70, W-series), not a row edit.

Full row-level list with sheet row numbers and evidence: `TFB_Universe_Hygiene_Worklist_2026-09-05.csv` (145 rows; one of the 114 unpriced GM rows still carries a name and is left out).
