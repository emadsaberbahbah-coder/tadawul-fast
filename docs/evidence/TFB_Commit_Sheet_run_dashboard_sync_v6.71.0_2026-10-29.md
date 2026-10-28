# TFB Commit Sheet - scripts/run_dashboard_sync.py v6.71.0 [FW2-STALE-PE: a witnessed stale P/E clears P/E and EPS, not the row]

Date: 2026-10-29 (Thursday, Riyadh). Base: `origin/main` 729a316e (#871).
Lane: GitHub Actions (the sync runs in daily_sync.yml; no Render deploy). Gate: repository Variable
`TFB_SYNC_FW2_STALE_PE` = off | observe | enforce, default off = the v6.70.2 FW-2 call. Nothing is armed by this change.

Delivered: `scripts/run_dashboard_sync.py` 6.70.2 -> 6.71.0, `.github/workflows/daily_sync.yml` 1.0.7 -> 1.0.8 (env
mapping only), `.github/workflows/ci.yml` 1.1.55 -> 1.1.56 (lean-lane entry), `scripts/verify_deployment.py` (pin
6.71.0, SCRIPT_VERSION unchanged by the manifest-pin rule), `tests/test_fw2_stale_pe.py` (NEW), version / mapping pins in
`tests/test_sync_status_stamp_retry.py`, `tests/test_sync_hk_reporting_fx.py`, `tests/test_daily_sync_stamp_budget.py`,
`tests/test_workflow_flag_mappings.py`, this sheet.
Not edited: `_row_identity_firewall` (FW-2), `_klg_old_row_identity_ok` (FW-1), `_keep_last_good_rows` (FW-KEEP), the 5 %
tolerance, the GBX and HK-FX unit bands, the coverage audit, the finalizer, the release scripts, the engine, every Apps
Script file. A1 pins the FW-1 / FW-2 / FW-KEEP sources byte-identical to the base.

## Why (verbatim from the v6.71.0 header block)

FW-2 (v6.24.0) strips every outgoing row whose stated P/E differs from
Price/EPS by 5 % or more (outside the GBX and HK-FX unit bands); its design
note expects 0 strips on a healthy backend. _Run_Log: Market_Leaders strips
were 0-3 per write from 10 to 24 Oct, then 19 (25 Oct), 9 / 5 (26 Oct),
21 / 24 / 7 (27 Oct) and 6 / 16 / 13 / 10 (28 Oct); Global_Markets 4-377 per
write all month. The 12 KSA strips of 28 Oct had implied/stated P/E ratios
1.05-1.10 (one 0.91) on a day when many Tadawul names rose 4-10 %. 244 of
the 255 Market_Leaders rows carry yahoo_enrichment_applied and are coherent
to 0.6 %; the 11 testable rows without it were all off-identity. Render
logged "[YF-CB v6.9.1] OPEN reason=info_empty status=429 ... EODHD fallback
carries fundamentals" every minute from 15:30 to 15:45: such rows carry
EODHD's P/E, priced at EODHD's last snapshot, against the live quote. FW-KEEP
then restores yesterday's row, which is not acquired: Market_Leaders
acquisition read 243/255 (95.3 %, a pass by one row) and the 13:30
dispatched run failed at 94.9 %.

## What

- A row FW-2 would strip is a STALE VALUATION only when ALL of these hold:
  1. its implied/stated P/E ratio (Price/EPS over P/E) is inside the FIXED band [0.80, 1.25] (no env widens it);
  2. the symbol's own last-good sheet row exists and is good: positive price, provider not an error marker, not
     fetch-failed;
  3. that row passes the FW-1 identity rules (`_klg_old_row_identity_ok`: a Name that is not blank or fabricated, and
     P/E == Price/EPS when testable), applied whatever `TFB_SYNC_KLG_IDENTITY_GATE` says;
  4. the fresh Name equals the last-good Name (case, punctuation and the legal-form words of
     `core.analysis.symbol_dedup.normalise_company_name` such as Inc or PLC ignored; a blank, fabricated or unusable Name
     never matches);
  5. the same non-blank Currency, compared exactly (so `GBp` never equals `GBP`);
  6. today's price within +/-25 % of the last-good price (inclusive).
  Forced symbols (`TFB_SYNC_FORCE_REFETCH_SYMBOLS`), fetch-failed rows, error-provider rows and non-ticker symbols never
  qualify.
- off (default, and any word other than observe / enforce): the call site makes the v6.70.2 call verbatim; no new code
  runs, no read, no log line, no _Run_Log change (E1: rows, warnings, _Run_Log, stamp meta, stdout and logs equal the
  untouched base 729a316e, version string aside).
- observe: strips exactly as off and logs one INFO line per page that had an FW-2 strip. The only other difference is one
  extra read of the page (the block FW-KEEP reads), made only when a band candidate exists (E1b).
- enforce: each witnessed row keeps its fresh values; ONLY `P/E (TTM)` and `EPS (TTM)` are written as blank cells and
  `pe_stale_cleared:fw2:v6.71.0` is appended to Warnings. The untouched FW-2 then finds the row untestable. Every other
  mismatch is stripped and FW-KEEP restores it exactly as today. The line goes to WARNING and to the leg's warnings when
  a row was kept; the FW-3 `[ID-FIREWALL]` _Run_Log verdict gains ` | stale_pe_kept N` and a `stale_pe_kept` Details key.
- How: `_fw2_stale_pe_firewall` runs a probe FW-2 pass on a shallow copy of the matrix (FW-2 replaces a stripped row with
  a new list and never mutates the row it read, so `probe[i] is not rows[i]` is exactly its decision), recomputes the
  ratio with the same two divisions, reads the last-good grid once, judges each band row, and then calls the real FW-2.
  A planning error keeps nothing.
- The tag is not an invalid-acquisition token (`core.data_validity._INVALID_WARNINGS`) and contains none of the engine's
  Warnings substring gates (cap, forecast, target, roi, drop, reject), so a kept row counts as acquired (E1c:
  `row_acquisition` SUCCESS, against INVALID for the restored row under off).

## Exact lines (from the end-to-end harness on Market_Leaders)

```
[FW2-STALE-PE observe] 'Market_Leaders' would keep 1 row(s): 1120.SR ratio=1.0974 px/lkg=1.07 yf=0 | band 1 of 2 FW-2 strip(s)
[FW2-STALE-PE enforce] 'Market_Leaders' kept 1 row(s) with P/E and EPS cleared: 1120.SR ratio=1.0974 px/lkg=1.07 yf=0 | band 1 of 2 FW-2 strip(s)
[ID-FIREWALL v6.71.0] Market_Leaders | klg_kept=0 | klg_suspect_dropped=0 | out_stripped=1 (7010.SR) | name_dup=0 | dedup_mode=observe | stale_pe_kept 1 | selftest=PASS 9/9
```

A rejected band row is listed after the band count, e.g. `| band 2 of 3 FW-2 strip(s); still stripped 1: 1010.SR(name)`,
and a failed last-good read ends the line with `; last-good READ FAILED`. Reasons: forced, domain, fetch_failed,
provider, no_last_good, last_good_price, last_good_provider, last_good_fetch_failed, last_good_identity, name, currency,
price, px_jump, error. `yf=1` means the fresh row carries `yahoo_enrichment_applied` (diagnostic only; it tests the
stale-EODHD hypothesis during observe).

## Rollout (owner)

1. Set the repository Variable `TFB_SYNC_FW2_STALE_PE=observe`. Read the `[FW2-STALE-PE observe]` lines of the next
   Market_Leaders and Global_Markets legs: which rows would be kept, their ratios, `px/lkg`, `yf`, and the reasons the
   others stay stripped.
2. If the would-keep rows are the stale-P/E names (expected `yf=0`), set `enforce`. Read-back: the enforce line,
   ` | stale_pe_kept N` on the `[ID-FIREWALL]` _Run_Log line, `out_stripped` down by N, and the page's acquired count up
   by N on the _Status stamp.
3. Rollback: `off` (one Variable; no deploy).

## Residual and decisions

- Only P/E (TTM) and EPS (TTM) are cleared, as decided. `P/E (Forward)`, `PEG`, `P/B`, `P/S` and `Valuation Score` stay
  as the backend sent them; when the P/E came from the EODHD snapshot those may share its price basis. Open PR #773
  (FW-2 SCOPE) also cleared PEG and set BLOCKED / DO_NOT_INVEST on kept rows; this change does neither.
- Overlap: open PRs #773 (FW-2 SCOPE, drift band [0.5, 2.0]) and #787 (FW-SKEW, 52-week-range witness) gate the same
  strip decision and are stale against main (both still say v6.70.0). #790 (BATCH-CONC, titled v6.71.0) is stacked on
  #787. Only one FW-2 keep rule should merge; whichever lands second renumbers.
- The witness needs a last-good row: a symbol whose previous row is already a stub stays a stub, as today.

## Evidence

- `tests/test_fw2_stale_pe.py`: 84 cases (P1-P12 pure, E1/E1b/E1c/G1 through the real `_run_one_task`, A1 source
  identity and zero removals, W1 workflow mapping, V1 pins). All pass locally (Python 3.11.17).
- Golden negative: the same file run against the untouched 729a316e tree fails 77 cases and errors 1 (6 base-dependent
  cases skip there); inside the repo G1 shows the base stripping the witnessed row and FW-KEEP restoring yesterday's row
  (INVALID) even with the variable set to enforce.
- Whole suite: 6,434 passed, 48 skipped, 10 failed; the 10 are `tests/test_framework_security_20261024.py` (9) and
  `tests/test_security_pins_libsmoke_20261013.py::test_l17` (1), which fail identically on the untouched base tree in
  this environment (installed framework versions).
- Lean lane (numpy + pytest + PyYAML only, the 123 files of ci.yml 1.1.56): 4,298 passed, 26 skipped, 0 failed.
- AST: 11 functions added (`_fw2_stale_pe_mode`, `_fw2_name_key`, `_fw2_names_match`, `_fw2_ccy_col`, `_fw2_row_cols`,
  `_fw2_read_last_good`, `_fw2_last_good_index`, `_fw2_stale_pe_witness`, `_fw2_stale_pe_plan`,
  `_fw2_stale_pe_firewall`, `_fw2_stale_pe_line`), 0 removed. Added lines are ASCII except the title line, which keeps
  its existing em dash.

Not a licensed financial adviser; this changes data hygiene only and places, changes or cancels no order.
