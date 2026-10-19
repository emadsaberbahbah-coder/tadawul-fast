# TFB Commit Sheet - scripts/run_dashboard_sync.py v6.70.0 [Hong Kong reporting-currency band in the identity checks]

Date: 2026-10-19 (Monday, 14:30 to 16:30 Riyadh). Base: `origin/main` 6ebe737 (#798).
Delivered: `scripts/run_dashboard_sync.py` 6.69.0 -> 6.70.0, `scripts/repair_stores.py` 1.0.0 -> 1.0.1 (its B1 copy of
the identity check takes the same band), `tests/test_sync_hk_reporting_fx.py` (NEW, H1-H12),
`tests/test_sheet_payload_digest.py` (clock pin, see "Time-bomb test"), `scripts/verify_deployment.py` (pin 6.70.0),
`.github/workflows/ci.yml` 1.1.25, review document section 11, this sheet.
Not edited: the coverage audit, the finalizer, the release script and its acceptance rule, the engine, every Apps Script file,
the 5% identity tolerance, the GBX/GBP band, FW-KEEP's last-good certification.

## Why

The Top_10 board stays withheld because the decision feed reads NOT_ACTIONABLE(final_publication) and no release is ever
accepted. The armed release attempt of 19 Oct 11:33 UTC (run 39245386428, source sync 39234358605) was refused with
`coverage_summary_not_zero` among its reasons, and it wrote nothing. The coverage audit returned exit code 1 on one
warning, Global_Markets `7 blank/unparseable/date-only timestamp(s)`; the 18 Oct final audit had the same warning with 8.
`accepted()` requires both audit summary exit codes to be 0, so this warning alone refuses every release.

The 7 rows are identity-firewall stubs. FW-2 (`_row_identity_firewall`) quarantined 47 Global_Markets rows in run
39234358605 (log: `2601.HK, 0728.HK, 2359.HK, 1766.HK, SGH.AX, 0998.HK, 0285.HK, HKIT.US, SIM.US, 0384.HK, 1398.HK,
0386.HK, 9618.HK, 0857.HK, 0883.HK...`). FW-KEEP restored 40 from last-good (frozen, INVALID, ageing) and left 7 as
blank stubs ("7 had no last-good (left as stub)"). A stub has both Last Updated cells blank, which the audit counts as a
bad stamp. It never heals: FW-2 strips the fresh row again every run, and FW-KEEP never certifies a stub predecessor.

FW-2, FW-1 (`_klg_old_row_identity_ok`) and the L3b tripwire (`_coherence_scan`) test Price/EPS against the stated P/E
with one unit exemption, the 50-200x GBX/GBP band. Twelve of the fifteen named rows are Hong Kong listings, mostly
mainland issuers (H-shares and red chips) that quote in HKD and report EPS in CNY, so Price/EPS reads the stated P/E times
the HKD-per-CNY rate (about 1.07 to 1.10): a 7-10% gap over the 5% tolerance. This is a reporting-currency convention, the
same class as the pence/pound one, not an identity break. The other three named rows are not `.HK` listings: SGH.AX,
HKIT.US (a US listing, likely USD price over CNY EPS) and SIM.US (an ADR, likely MXN EPS). The band does not reach them,
and the remaining 32 stripped rows were not named in the log.

## What

- `_coh_ratio_is_unit_convention(ratio, symbol)`: the GBX/GBP band, unchanged and unconditional, or (new) a symbol ending
  `.HK` whose ratio lies in the HKD-per-CNY band (default 1.02-1.18) or the HKD-per-USD band (default 7.40-8.25, the
  7.75-7.85 peg widened by the as-of skew). FW-1, FW-2 and L3b all call it, so the three layers still cannot disagree.
  FW-1 receives the symbol from its call site; without one it applies only the GBX band, exactly as v6.69.0.
- `scripts/repair_stores.py` 1.0.1: the manual repair workflow carries its own copy of the identity check (B1). It now
  mirrors the same band, gate and overrides (mirrored, not imported: that workflow installs only gspread and google-auth),
  so an `apply=true` run no longer blanks the `.HK` rows the sync publishes. H12 pins the two verdicts equal on every
  fixture, gate on and off.
- Scope: `.HK` only. Every other venue, every other ratio and the 5% tolerance are unchanged; a `.HK` row outside both
  bands is still stripped (H4).
- Diagnostics: FW-2 records each stripped row's symbol, ratio and Currency; the call site logs
  `[ID-FIREWALL v6.70.0] FW-2 detail '<page>': SYM ratio=R ccy=C, ...` (first 20) and
  `[ID-FIREWALL v6.70.0] HK-FX '<page>': N row(s) kept as the Hong Kong reporting-currency convention ...`; FW-KEEP names
  the symbols it leaves as stubs.
- Gate `TFB_SYNC_COH_HK_REPORTING_FX` (default on; 0/false/off/no = v6.69.0 decisions byte-identical, H1b). Bands
  `TFB_SYNC_COH_HK_CNY_BAND` / `TFB_SYNC_COH_HK_USD_BAND` as `lo,hi`; anything invalid falls back to the default (H8b).

## Policy note (owner)

No release guard changes: the bad-stamp rule, the warning, the exit-code ladder, the finalizer and `accepted()` are
untouched, and a blank, unparseable or date-only stamp still blocks a release. What changes is which rows FW-2 judges
identity-broken: a `.HK` row at the HKD/CNY or HKD/USD ratio now publishes as a normal, fresh row instead of a stub or a
frozen last-good copy. The residual risk is a genuinely transposed `.HK` row whose ratio happens to land inside a band;
the name, fabrication and duplicate firewalls still apply to it.

## Expected effect and what it does not fix

- The `.HK` rows among the 47 quarantined Global_Markets rows should publish fresh. How many that is, and how many of the
  7 stubs it heals, is not known: v6.69.0 logged only the first 15 stripped symbols and never named its stubs. Read-back on
  the next sync: the `HK-FX` line lists the kept rows, `out_stripped` falls, and FW-KEEP now names what is left.
- Any non-`.HK` stub stays a stub: SGH.AX, possibly HKIT.US and SIM.US, and any unnamed non-`.HK` row. While one does,
  Global_Markets keeps a bad stamp and the coverage audit still warns, so a release stays refused. The new FW-2 detail
  line prints each stripped row's ratio and Currency, which tells the next cycle whether it is another reporting-currency
  case, a stale EPS, a forward P/E or a real identity break. The owner may also remove a residual stub symbol from
  Global_Markets (6612 rows clear the 6512 floor with margin).
- The hypothesis rests on the ticker list; no row values were readable offline (the backend needs the app token and the
  public quote API refused). If the ratios printed by the next sync are not near 1.08 or 7.8, the band does nothing and
  the rows stay quarantined exactly as today.

## Time-bomb test fixed in the same PR

`tests/test_sheet_payload_digest.py::test_receipt_captures_the_actual_post_name_rank_and_presentation_submission` read the
committed caption registry (`data/verified_name_labels.json`) against the wall clock. The registry's bindings carry a
seven-day review window; the oldest ended on 19 Oct 11:11 and 12:29 UTC, and the test started failing on main and every
PR. It now pins the registry clock inside the reviewed window, exactly as `tests/test_verified_name_labels.py` already
does. Production behaviour is unchanged and intended: expired captions are no longer filled.

## Audits

| Battery | Result |
| --- | --- |
| `tests/test_sync_hk_reporting_fx.py` H1-H12 (H12: repair tool parity) | 28 passed |
| Every test importing the sync, plus coverage, finalizer, manifest pins, release (v1.2.0 + auto source), workflow audits | 1500 passed, 12 skipped; the one failure was the time-bomb test above, reproduced on unmodified main, fixed here |
| `tests/test_sheet_payload_digest.py` + `tests/test_verified_name_labels.py` after the clock pin | 127 passed |
| ruff E9/F63/F7/F82, py_compile | clean |

Rollback: `TFB_SYNC_COH_HK_REPORTING_FX=0` in the sync workflow env, or revert.
