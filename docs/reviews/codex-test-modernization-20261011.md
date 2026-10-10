# Offline test discovery and framework drift

WP-08, WP-11 and WP-15 are implemented without arming policy flags or changing
production dependency pins.

The seven original runnable zero-collection batteries now expose pytest cases.
P-143 exposes six individual cases; the other batteries execute their original
assertions in subprocesses so module patches, clocks and environment changes
cannot leak to sibling tests. Five further script-style modules discovered by
the import audit were isolated the same way. Global per-test environment
restoration also protects existing direct environment mutations.

Three harnesses needing uncommitted second trees were preserved under
`scripts/harness_archive/`; that directory's README lists their missing inputs
and active replacements. The replacements assert the original reproducible
safety properties against real modules. The current portfolio funding contract
requires certified broker evidence, so drawdown and fee boundaries use the
actual pure guard/funding seams; existing certified builder integration remains
in `tests/test_portfolio_exact_funding.py`.

The enriched-quote sentry's original enforce-mode property (fraction to points,
ground-truth required, tiny points unchanged and idempotence) still holds at its
real seam. The newer sheet presentation contract expects fractions and withholds
the resulting conflicting points tuple. Tests cover both facts; this package
does not change that policy or enable the flag.

`tests/test_no_import_side_effects.py` imports every `tests/test_*.py` module in a
subprocess, asserts unchanged environment and root logger after each import,
and blocks socket connections. It initializes the application's intentional
startup logging before taking its baseline. The live endpoint smoke tool is
now `scripts/smoke_endpoints.py`; the old tests path remains a CLI shim with an
offline pytest help check. Pytest configuration lives in `pyproject.toml`.

The whole-suite CI lane installs the production pins on Python 3.11 and runs
`python -m pytest -q tests`. It is advisory during the requested first green
week; making it required is a later GitHub settings action. Existing blocking
lanes retain their verdict behavior. Their explicit lists now cover the new
capture/PIT repair, workflow mappings, rollout verification, USX conversion,
pure performance math, flag registry and embargo properties in the lean lane;
mounted-route compatibility in the pinned contract lane; and price precision,
currency lockstep, crypto identity, export audit, calibration and decision
policy regressions in the production-dependency lane. The lean lane retains
only numpy, pytest and pinned PyYAML; the crypto registry-sharing test runs in
the heavy lane because its audit import needs openpyxl. Every versioned Apps
Script source also gets a blocking syntax check without service execution.

WP-15 was reproduced on Python 3.13.5, FastAPI 0.143.0, Starlette 1.7.0,
httpx 0.28.1 and Pydantic 2.14.0: two schema tests failed and every business
route family was reported missing. FastAPI now stores included routers as lazy
branch nodes without path or methods; the endpoints were mounted successfully.
`main.py` v8.14.3 inspects the branch's effective contexts by feature detection
for diagnostics, counts, owner checks and duplicate detection. The pinned eager
route representation follows the original path, and router inclusion behavior
is unchanged. Schema endpoint discovery uses the same effective-route traversal.
The real modern contract lane passed 16 tests with two existing schema capability
skips, and a separate nonblocking CI job observes Python 3.13/latest framework.

Production `requirements.txt`, operator settings, live sheets and external
services were not changed by these packages. Owner decisions on horizon and
strict INVEST semantics remain open; tests establish off/observe/enforce
behavior without selecting the policy.
