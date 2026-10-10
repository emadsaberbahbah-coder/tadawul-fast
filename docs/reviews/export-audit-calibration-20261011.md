# WP-06 / WP-14 offline reporting review

Date: 2026-10-11. Repository changes only; no live Sheet/provider/Render calls,
no production flag activation, and no owner policy decision inferred.

`scripts/tfb_export_audit.py` is v1.2.0. `--margin-receipts` (or
`TFB_AUDIT_MARGIN_RECEIPTS=1`) requires exact current-value receipts and rejects
all receipts for a field if any are malformed, stale, conflicting or nonfinite.
Matching fractions above 100% and negative margins below -100% are separate
WARNs; unknown positive units retain the existing 5%-of-populated FAIL rule.
Receipts establish display units, not supplier plausibility or authenticity.
`--eodhd-fail-pct` / `TFB_AUDIT_EODHD_FAIL_PCT` binds the recorded plan to each
daily usage maximum. Equal maxima use the smaller recorded plan. The threshold
is inclusive; invalid zero plans fail. Above-target usage becomes WARN while a
separate plan-threshold check determines FAIL. Both switches default off;
explicit `--no-margin-receipts --eodhd-fail-pct 0` rolls back env opt-ins.
Blank risk cells now mean unavailable measurement; numeric all-zero columns
remain P-188 failures. This intended reporting correction and the version
stamp are the only unconditional output changes.

The embedded selftest is **50/50 PASS**, cases digest `e4a1dd8650ad62f9`.
The subprocess/version harness and adversarial/default-payload/calibration
tests pass **36/36**. Two pinned payload hashes were captured directly from
v1.1.1's clean/defective synthetic fixtures; v1.2.0 reproduces every finding,
metric, parameter, lane and fix list with switches off. The crypto registry
now imports the shared WP-05 registry without changing reviewed tokens.

Available real export SHA-256:
`9c8b34812eee7d5554a3586f1f86a232e79159f24e228a4eac070e2436441ef4`.
Although its file metadata claims 2026-10-10, the actual Global_Markets dominant
update date and latest Signal_History date are **2026-10-06**. Its market/quota
default checks match v1.1.1 exactly. The older export has no matching published
margin receipts, so opt-in reporting correctly retains unknown-positive unit
failures: Global_Markets 2,253 unknown positives, 172 negative margins, zero
certified positive fractions. A demonstration threshold of 75% preserves all
seven above-90k days as cost WARNs with no plan-threshold FAIL. The threshold is
an offline example, not an owner-selected cost policy. These results are in
`docs/evidence/export_audit_v120_checks_2026-10-06.json`; this is a scoped
market/quota replay, not a six-gate or production validation claim. The
brief's 2026-10-10 213/114 receipt split needs that newer source export.

`scripts/calibration_report.py` reads exports with `read_only=True,
data_only=True`, records the source SHA/date ranges, preserves the intentional
Performance_Log row-5 header, and emits reproducible JSON/Markdown per horizon
and origin page. It keeps the first finite-outcome matured record per Key,
retains the only matured outcome after an active first record, excludes
nonfinite/missing outcomes, and omits missing/invalid/out-of-range entry
reliability from Brier rather than inventing probabilities. Risk blanks and
all-zero triplets contribute no risk evidence; their genuine realized
outcomes remain in win rate/Brier. Signal_History supplies entry-date coverage
only and cannot replace predictions through ambiguous or later snapshots.

Real evidence in `docs/evidence/calibration_export_2026-10-06.{json,md}`:
19,637 records; 11,218 raw matured rows; 27 duplicate matured rows excluded;
11,191 retained matured rows; 5,130 wins / 5,857 losses (204 breakeven),
**46.6915%** decided win rate; Brier **0.28402366** over **10,987** decided
records. All 11,191 risk triplets were unavailable zero placeholders.
Signal_History has 28,376 rows and 8,161 ambiguous entry-date matches, disclosed
without outcome/probability imputation. The source predates the brief's
11,791-record October-10 corpus; these numbers cannot validate that corpus.

Reproduce with:

```sh
python scripts/tfb_export_audit.py --selftest
python -m pytest -q tests/test_export_audit_v1.py tests/test_export_audit_v120.py tests/test_calibration_report.py
python scripts/calibration_report.py export.xlsx --json calibration.json --md calibration.md
```

Owner decisions remain open: EODHD daily cost target and failure percent,
handling implausible receipt-certified supplier margins, and whether Brier
above the 0.25 baseline should block promotion. No enforcement is added here.
The separate purged-walk-forward embargo review belongs to the root agent's
validation work, not these reporting files.
