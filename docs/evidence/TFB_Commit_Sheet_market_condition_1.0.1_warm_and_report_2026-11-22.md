# Commit sheet - the market read is warmed at boot and reported on /health (market_condition 1.0.1, 22 Nov 2026)

Branch `fix/market-condition-warm-readback`, against main after #1052. The engine stays 5.161.0: no rule, label or verdict changes.

| File | Version |
|---|---|
| `core/market_condition.py` | 1.0.0 -> **1.0.1** |
| `scripts/verify_deployment.py` | pin core.market_condition 1.0.1 |
| `tests/test_market_condition.py` | M8 (2 tests); two fakes take the new `missing` argument; pin 1.0.1 |

## Why

The owner deployed 973a77a on 22 Nov 2026 at 06:12 UTC with `TFB_MARKET_GATE=enforce` and `TFB_SELECTION_GATE=enforce` and pasted the deploy log. `/health` at 06:28 UTC read both gates on enforce and `market_labels {}`. A worker reads the labels only when an INVESTABLE row reaches the market tier; on a Sunday with no page read since the boot none had, so the read-back could not tell a quiet worker from a read that fails on Render.

## What changes

- `state()` - what the engine shows as `engine_gates.market_labels` on `/health` and in its startup line - also starts a refresh when one is due, in the background (it never waits). Each worker therefore reads at its startup line, and `/health` shows the labels within a minute of a boot.
- It reports the last read under `_read`: how many minutes ago, which markets were read, and why each other one was not - per host `query1: HTTP 429`, `no bars`, `last session ... is more than 7 days old`, `not enough closes for a label (n)`, or the fault's type.
- The Yahoo request carries the browser User-Agent of `yahoo_chart_provider`'s raw chart transport (`_RAW_CHART_UA`), the transport the engine already reads from Render.
- `read()` gains the optional `missing` argument; the labels and their rules are unchanged.

## Checked

`tests/test_market_condition.py` + `tests/test_de_market_and_selection_gate.py`: 108 passed (M8: the first health line starts one read; the report after a read with one market read, one 404, one too old and one too short; notes per host for faults and empty answers; the User-Agent). `docs/render_env_contract.md` is current. `test_verify_manifest_pins`, `test_render_env_contract`, `test_de_evidence_cohort`, `test_exit_plan`: passed.

## Read-back after the deploy (owner)

`/health` -> `engine_gates.market_labels`: five markets with their label and `_read.read = ["cn", "eu", "jp", "sa", "us"]`; any market in `_read.missing` says why it was not read.
