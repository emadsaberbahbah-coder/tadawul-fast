# Workflow flag mappings — 11 October 2026

The handoff reports that its 10 October export contained 27 duplicate matured keys, zero-valued
unavailable risk placeholders on 11,791 matured rows, criterion 5 PASS on an
empty corporate-actions register, and 198 unavailable symbols fetched again
every sync. The existing script guards could not receive Actions Variables
because their workflow env mappings were absent. These counts are attributed
to the handoff; the available older workbook cannot independently verify
its 10 October population.

The Performance Clock and in-sync tracker now both map
`TRACK_DEDUP_MATURED` (`off`) and `TRACK_RISK_BLANK` (`0`). The primary and
recovery sync jobs both map `TFB_SYNC_RETIRE_TAB` (`off`),
`TFB_SYNC_RETIRE_TAB_NAME` (`_Retired_Symbols`), and `TFB_SYNC_RETIRE_MAX_PCT`
(`20`). Both sync jobs also map the WP-05 crypto restored-row name guard,
`TFB_SYNC_KLG_CRYPTO_NAME_GATE` (`off`). The scorer maps
`TFB_S1_CRITERIA_V2` (`off`), preserving the strict default-off rule until
the owner chooses an arming mode. Each mapping uses its repository Variable
with the stated fallback; unset Variables preserve script defaults.

Arming remains an operator action. Before S-1 v2 enforce, record the Register
section 5 evidence-window boundary. Retirement enforce requires a reviewed
operator retirement list and retains the script's count ceiling. Dedup
observe should produce a DEDUP-MATURED receipt; S-1 observe should annotate
criterion 5 with the v2 NOT_EVALUABLE diagnosis. Unsetting mode Variables
restores off; unsetting `TRACK_RISK_BLANK` restores `0`.

The regression suite parses the three real workflow YAML files and verifies
each mapping, defaults, CI isolation, and all shared effective environment
keys across both tracker lanes and both sync writer lanes. Workflow, job and
step precedence is checked so inherited cohort settings are included.

Validation: `python -m pytest -q tests/test_workflow_flag_mappings.py
tests/test_tp_dedup_matured_p189b.py tests/test_s1_criteria_v2_p204.py` passed
12 tests. `python scripts/audit_repository_workflows.py` reported zero errors
and the two pre-existing force-refetch override warnings. Whitespace checks
passed. No Variables, live workbook or Render settings were changed.
