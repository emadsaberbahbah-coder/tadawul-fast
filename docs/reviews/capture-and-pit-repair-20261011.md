# Custodian capture and PIT repair review — 11 October 2026 (Riyadh)

WP-01 now publishes the additive `_Cash_Snapshot` export template and provides
`scripts/tfb_capture_evidence.py` for operator-mapped JSON/CSV captures. The
builder requires explicit custody coverage, exact holding links, settled cash,
reserved cash, reservation completeness and caller FX. It uses the existing
pure certifier at the chosen `--now` with the same 900-second limit. Optional
`--max-age-check` includes every account's cash timestamp. A valid packet is
written atomically with `0600` permissions; invalid inputs preserve an existing
destination, and stdout contains only nonidentifying status/counts. Duplicate
JSON fields and ambiguous CSV mappings fail. No source authenticity, broker
credential, missing balance or reservation is manufactured.

Cash regression tests cover the actual seven-column v1.0.1 layout with appended
account/currency/balance-type fields: SAR certification succeeds with an actual
Time and fails with a missing Time; USD requires its native balance and FX
vintage, and inconsistent recorded SAR fails. Export-audit cash certification
remains separate from backend account reconciliation and does not unblock a
decision surface by itself.

WP-07 adds `scripts/repair_pit_legacy_rows.py`. Its pure planner requires the
exact current header, exact legacy dates, a populated width of 16 and a complete
original capture timestamp before moving the two timestamp values to W:X.
Uncaptured diagnostic fields remain blank. Only the eleven enumerated
2026-07-22 artifact identities with Source Page `-` qualify for deletion.
Artifact deletion and timestamp realignment can overlap. A short explicit
artifact needs no timestamp reconstruction to qualify for its own deletion;
other malformed legacy rows remain anomalies.

Default mode and `--scan` are read-only, including `_Run_Log`. `--input-json`
supports an entirely offline scan and refuses `--apply`. The operator's apply
path checks ceilings of 78 realigns and 11 deletions, refuses anomalies, re-reads
the grid to detect drift, realigns before bottom-up deletion, and appends one
`[PIT-REPAIR v1.0.0]` log entry. Transport/logging failures surface as an error
requiring review of possible partial I/O. `pit_snapshot` is now version 1.1.1;
its documentation correctly describes historical timestamp displacement, while
its append and duplicate-guard logic is retained.

Offline checks passed:

- 151 tests across `test_capture_evidence.py`, `test_pit_repair.py` and
  `test_cash_snapshot_certification.py`, covering exact freshness boundaries,
  malformed account/FX inputs, private output paths, strict repair predicates,
  count ceilings, grid drift, bottom-up deletion and idempotence.
- Full configured Ruff rules for the four new Python files.
- `pit_snapshot.py --selftest`: 6/6.
- Read-only/data-only inspection of the available uploaded workbook: 1,804 PIT
  data rows, exact 24-column header, 78 historical rows with populated width 16;
  the planner returned 78 realigns, 11 deletions and zero anomalies. These are
  measurements of that export, distinct from the brief's later 1,847-row count.

No workbook, document property, broker, Render service or live ledger was
modified. Authorized operator capture/property forwarding and a workbook backup
before an operator-run PIT repair remain external actions. Native Top10's
9,000-character property limit is unchanged: the builder reports packet size
for review and never truncates or installs evidence.
