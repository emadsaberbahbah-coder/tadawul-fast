# Structure, validation and source coverage — 11 October 2026

WP-12 begins with two pure leaves: Wilson confidence intervals and calibration
shrinkage move from the 10k-line performance tracker into
`scripts/performance_math.py`. The tracker keeps the original public names
and `_Z95` constant as re-exports; formula operations, rounding and boundary
handling are unchanged. Tracker version is 6.43.1. Broader decomposition of
the engine and sync modules is deferred until each seam has independent
behavioral coverage. Root compatibility modules remain in place because
active consumers still depend on them; the importer findings are recorded
in `docs/root-compatibility-modules.md`.

`scripts/flag_registry.py` statically reads Python ASTs, literal/aliased env
names and helper readers, the verifier's `FLAGS`, and workflow mappings,
without importing production code. It generates `docs/flags.md`, recording
all observed defaults and mapping occurrences. Dynamic names and computed
defaults are disclosed rather than inferred as live state. Indirect launch
lanes and computed families still require manual reconciliation.

The initial pyright scope covers `core/sheets/`, `core/symbols/`, portfolio
reconciliation, execution accounting and the new math leaf. Its only error
was the execution reference dictionary inferred as string-only before its
list-valued `execution_keys` member was added. Execution accounting 1.0.1
adds one local mixed-value type annotation, with no runtime change. Pyright
1.1.405 now reports zero errors. The new dependency/typing workflow remains
advisory and does not weaken existing required CI lanes.

The requested pip-audit 2.9.0 check reported 89 advisory entries across 13
pinned packages. Its direct-wheel `httpcore` requirement was skipped because
the auditor cannot identify the version from that URL. This is an incomplete
static requirements audit, not a deployment vulnerability assessment. No
dependency pins were changed: upgrading the framework must preserve the
supported contract stack and needs its own validation.

WP-14's apparent unused embargo was not a leakage defect. Every walk-forward
fold trains strictly before the test and purges training labels that could
touch it, so the entire future region already excludes every post-test
embargo interval. Validation 1.0.1 corrects that contract and removes the
unused local. The compatibility argument still validates as before. A
deterministic grid covers dataset sizes, horizons, fold counts and embargo
fractions, asserts disjoint label windows and post-test exclusion, and
includes nonempty folds so the proof cannot pass only vacuously.

WP-13 adds VM syntax compilation for every authentic versioned `.gs` file.
All three current sources compile. The five missing bound-project sources
were not found in any locally available git history; prerequisites and
unresolved behavioral/lockstep tests are recorded in
`docs/apps-script-source-prerequisites.md`. No replacement live code was
invented.

Validation: 377 targeted tests passed, covering the registry, extracted math,
embargo property grid, execution accounting, provider capability routing,
recovery planner and inline recovery. Both extracted math function ASTs match
the handoff baseline exactly. Core validation's existing selftest passed
31/31; Apps Script syntax compiled three files; pyright reported zero errors;
Ruff and compile checks passed. The repository workflow audit reported zero
errors with two existing one-run override warnings. The dependency audit's
complete returned result, including the skipped direct-wheel requirement, is
in `docs/evidence/dependency-audit-20261011.json`. No live Sheet, Render or
GitHub settings changed.
