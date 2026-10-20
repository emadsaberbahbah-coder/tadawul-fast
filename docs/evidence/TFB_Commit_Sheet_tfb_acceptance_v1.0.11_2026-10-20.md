# TFB Commit Sheet - tfb_acceptance v1.0.11 [RELOCATED AUDIT GRID]

Date: 2026-10-20. This is the follow-up to v1.0.10 (#807, merged as `97fa9915`) and answers Codex P2 on PR #808.

- **Base:** main `97fa9915`. `scripts/tfb_acceptance.py` 1.0.10 (sha256 `bd187c7a78a398d0...`, 1,397 lines). Result: 1.0.11 (sha256 `67e5a2ef15a92ad2...`, 1,447 lines).
- **Lane:** GitHub Actions (`acceptance_check.yml`), read-only. Nothing in Render, the backend or Apps Script changes. The workflow is unchanged.
- **Gate:** the same repository Variable, `TFB_ACCEPTANCE_LIVE_WINDOW` (default `'0'`). Off is unchanged: v1.0.9 byte-identical, as in v1.0.10.

## Why

`16_Decision_Top10.gs` supports `DT10_CAND_TAB=enforce` (v1.13.4 [WP-8]). In that mode, `dt10CandTabWrite_` moves the candidate audit grid to the hidden tab `_T10_Candidates`:
- row 1 holds the run stamp, with the receipt JSON (`state` PENDING -> COMMITTED, `rows`, `cols`, `checksum`) in column 2;
- row 2 holds the headers, and data runs from row 3.

The cockpit keeps only the CANDIDATES section header, the column header and one pointer line (`dt10CandPointerText_`: "Full audit (N rows) written to hidden tab _T10_Candidates ...").

Under that mode the v1.0.10 live window still gives A2a no USD rows, so A2a stays a hard NA with the gate on. Codex found this on #808. The mode is not armed today: the 14 Oct export carries the grid in the cockpit.

## What changed

`scripts/tfb_acceptance.py` 1.0.10 -> 1.0.11. A v1.0.11 WHY block is added; the v1.0.10 and earlier blocks are verbatim.

- **`_cand_tab_pointer(body)`** (new, pure): True when the first audit body row carries "written to hidden tab _T10_Candidates".
- **`_cand_tab_audit(src)`** (new): returns `(header, body)` of `_T10_Candidates`, read through `integrity_tab`, only when the receipt parses, says `COMMITTED`, and its `rows` equals the data rows. Otherwise it returns None.
- **`_top10_sections`**: with the gate on, if the audit body is the pointer and `_cand_tab_audit` returns a grid, that grid becomes the audit section.
  - Anything else keeps the cockpit section: a failed-open publication leaves the grid in the cockpit, and a pointer with a PENDING or mismatched grid stays NA.
  - A missing tab is recorded in `.errors` by `tab()`, so it fails closed.
- **Totals:** functions 54 -> 56 (2 added, 1 changed, **0 removed**). Added lines are ASCII.

## Measured results

```
pytest tests/test_acceptance_live_window.py (TFB_TEST_ACC_BASE=v1.0.9 + TFB_TEST_ACC_EXPORT)   13 passed
python3 tests/test_acceptance_live_window.py  (x3, same env)          PASS 13/13 digest 17f1218827b64e3d
python3 tests/test_acceptance_live_window.py  (CI: no base, no export) PASS 10/13 (skipped 3), same digest
golden negative vs v1.0.10 (main 97fa9915) as the delivered module:   W7 fails, 11 pass, 1 skip
golden negative vs v1.0.9 as the delivered module:                    9 failed, 1 passed (N4), 3 skipped
mutations 6/6 caught: relocation ungated, receipt state not checked, row count not checked,
  pointer never detected, moved grid ignored, gate ignored
python3 scripts/tfb_acceptance.py --selftest   PASS 8/8 (unchanged)
compileall scripts tests                        ok
```

**New harness case W7.** With the gate on, over the relocated layout (cockpit header + pointer, hidden grid with a receipt):
- COMMITTED grid: A2a PASS, n=320, median 3.7505;
- PENDING, or a receipt row count of 399 against 400 data rows: A2a NA ("USD rows with both prices: 0");
- missing tab: A2a NA, with `live tab _T10_Candidates unreadable` recorded;
- off: the v1.0.9 reading ("audit section not found"), and the hidden tab is never opened;
- export path on: the same A2a row as live.

**Existing cases extended.** N1 (off == v1.0.9 byte-identical: rows, errors, call log) and the digest now also run on the relocated layout.

## Operator steps (owner)

- Unchanged from v1.0.10: optional arming via the Variable `TFB_ACCEPTANCE_LIVE_WINDOW = 1`.
- The fallback only acts once the cockpit runs `DT10_CAND_TAB=enforce`. That is a separate owner decision with its own prerequisites, listed in the 16_Decision_Top10 v1.13.4 header.
- Rollback: unset the Variable, or revert to v1.0.10.

## Known limits

- A COMMITTED receipt is trusted as written. The harness does not recompute the `dt10CandTabChecksum_` checksum, a GAS digest, from Python.
- Run identity is not matched. The cockpit pointer carries the meta line, not the receipt `run_id`, so a COMMITTED grid from an older run would be read. A stale grid would show as stale prices, which is what A2a measures, not a structural failure.
- Everything else in the v1.0.10 sheet still applies: D10-1b/1c, the 3.742 audit FX observation, and the harness not being in the blocking lane.
