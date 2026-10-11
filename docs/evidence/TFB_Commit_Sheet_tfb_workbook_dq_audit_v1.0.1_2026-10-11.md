# TFB Commit Sheet — scripts/tfb_workbook_dq_audit.py v1.0.1 [WP-2bc integration: DQ-37 reads the settle_unresolved receipt]

Integration change made while landing WP-2 (b)(c) (engine 5.153.0). Not a work-package
build of its own; it closes the WP-0 parser gap the WP-2bc builder reported.

## S0/S1 — Base pin

| Item | Value |
|---|---|
| Base | v1.0.0 as published at `18a8416` (branch `claude/pensive-hypatia-smnxqe`) |
| Base file | sha256 `d84ef01738ef680a…`, 4,043 lines |
| Delivered file | sha256 `6b5b424f8b5c1e52…`, 4,071 lines |
| Test file | `tests/test_workbook_dq_audit.py` sha256 `cb600f91e02d80e6…` (P13 added; version pins 1.0.0 → 1.0.1) |
| Fixture | `tests/fixtures/dq_baseline_2026-10-10.json` unchanged (still `"version": "1.0.0"`, the generator of record) |

## S2 — Design (WHY on the file header)

Engine 5.153.0 (WP-2b, gate `TFB_SETTLE_UNRESOLVED`) publishes a non-converged row under
enforce without the `f7_settle_failed:non_converged` receipt. The row keeps its pass-1
scores as WATCHLIST and carries `settle_unresolved:enforce:<reason>:p<n>:ov=..:rc=..`.
v1.0.0 counted non-convergence only by the `f7_settle_failed` prefix, so arming enforce
would have dropped DQ-37's non-converged count to 0 while the rows were still unresolved.

- `F7_NONCONV_PREFIXES = ("f7_settle_failed", "settle_unresolved:")`, matched as a tag
  prefix. The bare substring `unresolved` is never used, because other tokens contain it:
  the engine's enforce-only margin receipt `margin_publish:<field>:unresolved` (since
  engine 5.151.4; none on 2026-10-10 because margin publication runs in observe) and the
  provider token `name_unresolved` (1 Global_Markets row on 2026-10-10).
- The same predicate feeds the per-row DQ-37 flag.
- An INVESTABLE row among the non-converged ones is listed FIRST in the DQ-37 examples
  (`<SYM> INVESTABLE non-converged (<page>)`), ahead of the INVESTABLE rc-flip examples.
  On the real export those flips fill all six example slots before 28 of the 46
  non-converged rows are reached, so a shared cap would hide such a row (independent
  review finding). There are none on 2026-10-10, and none can exist by construction
  under the engine's gate tier; examples are not part of the committed fixture, so the
  schema is unchanged.

## S4 — Audits

| Check | Result |
|---|---|
| AST zero-removal vs v1.0.0 | top-level defs 141 → 141, nested 12 → 12, names 78 → 79; removed none; added `F7_NONCONV_PREFIXES` |
| `--selftest` x3 | `[DQ-SELFTEST v1.0.1] 97/97 PASS  cases-digest=c1bdd1a2cc1c3927` (cases-digest unchanged, label-based) |
| Real export x3 vs baseline (2026-10-10, sha `f5d327afc9fb409c`) | `checks=81 pass=10 warn=19 fail=44 … decision_critical_fail=11 regressions=0 critical_regressions=0 digest=93f66f9a03260292` x3 (v1.0.0 printed `a92e4b7c9a54807f`; only the version token and the DQ-37 detail text differ) |
| Reduced checks vs committed fixture | identical (`reduce_baseline(report)["checks"] == fixture["checks"]`); DQ-37 FAIL, count 2,618, non_converged_total 46 |
| `pytest tests/test_workbook_dq_audit.py` (full requirements, export set) | 13 passed |
| `pytest tests/test_workbook_dq_audit.py` (no export) | 12 passed, 1 skipped (P12) |
| P13 against v1.0.0 | v1.0.0 counts 2 of the 3 non-converged rows (misses the enforce receipt) — the test discriminates |
| ruff E9/F63/F7/F82 | clean |
| Privacy | no figures added; P13 rows are synthetic symbols and tags only |

## S5 — Delivery

Integration commit on `claude/pensive-hypatia-smnxqe` together with the WP-2bc merge, the
verify_deployment 1.0.49 pins and the ci.yml 1.1.6 lines. The script is not pinned in
`scripts/verify_deployment.py` (precedent: `tfb_export_audit`).

## S6 — Operator steps

1. Nothing to arm for this change. The nightly backup's audit line now reads
   `[DQ-AUDIT v1.0.1] …` once `TFB_DQ_AUDIT_MODE` is observe or enforce.
2. After `TFB_SETTLE_UNRESOLVED` is armed (observe or enforce), DQ-37's
   `non_converged_total` must stay at the true count (46 on the 2026-10-10 shape), not
   drop to 0. Under enforce, check the DQ-37 examples for any
   `INVESTABLE non-converged` line; there should be none.
3. Rollback: re-publish v1.0.0 of this file. The fixture needs no change either way.

## Known limits

- The challenge's suggested separate check "DQ-37b: 0 of them INVESTABLE" is not added as
  a new record. A new check id would change the record count (81), the fixture and every
  digest. The INVESTABLE case surfaces in the DQ-37 examples instead.
- The single `f7_settle:observe:stx` tag (UNI-USD, keep-last-good residue) is still counted
  with the other `f7_settle:` tags. Classifying it separately is left for a later version.
