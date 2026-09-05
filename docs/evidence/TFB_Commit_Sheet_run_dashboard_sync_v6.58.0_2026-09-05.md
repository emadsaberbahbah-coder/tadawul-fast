# Commit sheet — scripts/run_dashboard_sync.py v6.58.0 (P-83a: KLG stub-swap covers priceless 'history' rows) — 2026-09-05

| File | Repo path | Exists? | Delivered | Version |
|---|---|---|---|---|
| run_dashboard_sync.py | `scripts/run_dashboard_sync.py` | replace | SHA256 `2b95fb2dae8d7e8a…` · 11041 lines | v6.57.0 (`a64602671dc52e06`) → **v6.58.0** |

Repo copy of this sheet (optional, new): `docs/evidence/TFB_Commit_Sheet_run_dashboard_sync_v6.58.0_2026-09-05.md`.
**Commit message:** `sync v6.58.0: KLG stub-swap covers priceless 'history' rows (P-83a) — 25 .SR prices lost on the 15Z run`

## Evidence (evening Market_Leaders export, run 14:59–15:48Z, vs the 04:xxZ morning export)
25 .SR rows returned with Data Provider = `history`, Last Updated 2026-09-03T07:00, **no Current Price**, BLOCKED (`recommendation_forced_hold_missing_price`); the same symbols were yahoo_chart-priced at 08:19Z (2310.SR = 13.19). INVESTABLE 9 → 4. Mechanism: `_keep_last_good_rows` recognises a stub only as (a) provider in the error set or (b) blank Name; a `history` row has a Name and a non-error provider → never swapped → the priceless row overwrote the last-good row → NULL-CLEAR scope=all blanked the derived cells.

## What changed (5 anchored edits, each matched exactly once; 236/237 defs byte-identical; 2 added; 0 removed)
- New `_klg_stub_providers()` — `TFB_SYNC_KLG_STUB_PROVIDERS` (CSV, default `history`; **empty string = kill switch** → v6.57.0 byte-identical).
- New `_klg_provider_is_stub_eligible()` — same normalisation as the error-marker check.
- `_keep_last_good_rows`: stub predicate widened to `is_err or is_bare or is_hist`; the price check still runs FIRST (a row with a fresh positive price is never a stub); the old row must still be priced, non-error, pass the FW-1 identity gate and the forced-refetch veto — unchanged certification.
- Docstring + header changelog.
**ENV required: none** (default ON; the cure needs no arming — it only fires on priceless rows).

## Verification (real module, real data)
- py_compile / compileall OK; `tests/test_sync_outcome_audit.py` + `tests/test_sync_recovery_plan.py`: 14 passed.
- Replay of the evening Market_Leaders matrix (255 rows, 25 `history`) against the morning grid as last-good:
  - v6.57.0: swapped **0**, 25 `history` rows left, 230/255 priced (defect reproduced).
  - v6.58.0: swapped **25**, 0 `history` rows left, **255/255 priced**; 2310.SR → yahoo_chart 13.19 @ 2026-09-05T08:19:50 (morning timestamp kept = honest age).
  - Kill switch (empty CSV): swapped 0. A priced `history` row is never swapped. `eodhd`/blank never eligible.

## What this fixes / does not fix
- Fixes: price loss on rows the engine serves as `history` (no price) — the whole last-good row (incl. investability/forecast) rides back with its own timestamp.
- Does NOT fix: fundamentals blanked on **priced** rows (GM: Market Cap lost on 1,925 symbols). That is the engine's in-process `_FUND_LKG_STORE` (TTL 72h) being wiped by the day's four Render restarts → **P-83 (engine): make the fund LKG store Redis-backed like the target KLG** — tomorrow's Build #1. Until then: avoid restarts (P-75 build filters) and run the sync off-peak.

## Post-commit checks (Emad)
1. `SCRIPT_VERSION = "6.58.0"` on main. The daily_sync workflow needs no change.
2. Next scheduled run (04:00Z): `_Run_Log` shows `[v6.25.1 FW-KEEP]` / KLG swap lines only if priceless rows appear; Market_Leaders back to 255/255 priced; INVESTABLE back near the morning count.
