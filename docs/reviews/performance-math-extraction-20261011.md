# Performance math extraction — 11 October 2026

WP-12 moves the pure Wilson interval and calibration shrinkage leaves into
`scripts/performance_math.py`; `track_performance` re-exports both names and
`_Z95`. The two function ASTs are identical to the 9a98d5a baseline. Known
small-sample/boundary regressions and the tracker integration suite pass.
Tracker version 6.43.1 and the new module version 1.0.0 are pinned in the
verifier. This extraction changes no arithmetic, I/O, flags or policy.

The isolated extraction tree passed the full offline suite: 3,319 tests and
131 subtests, with seven skips. Collection initially exposed an existing
YF harness that imported the provider under a second module name and
registered the same Prometheus metrics twice. The extraction commit includes
the subprocess isolation used by the final test modernization package;
behavioral checks remain intact, and stale exact version checks become
minimum supported versions. No provider code changed in this commit.
