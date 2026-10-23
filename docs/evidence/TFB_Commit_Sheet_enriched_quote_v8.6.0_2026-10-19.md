# TFB Commit Sheet - routes/enriched_quote.py v8.6.0 [the bridge binds the analysis signature; the instrument fallback is gated]

Date: 2026-10-19 (Monday, 12:40 to 13:40 Riyadh). Base: `origin/main` fbd0176 (#797), stacked on PR #798 after the Apps Script review commit.
Delivered: `routes/enriched_quote.py` 8.5.2 -> 8.6.0, `tests/test_enriched_bridge_binding.py` (NEW, B1-B10),
`tests/test_auth_wall_scope_path.py` (W15 pin 8.6.0), `tests/test_ob_research_diversify.py` (clock-field scrub, see "CI flake"),
`.github/workflows/ci.yml` 1.1.24, review document section 10, this sheet.
Not edited: the engine, the analysis router, the sync runner, every Apps Script file, the schema registry, every gate or threshold.
Not pinned in `scripts/verify_deployment.py` (precedent: `routes/analysis_sheet_rows` is not a pinned module either).

## Why (the owner's blank `Rel` / `DQ` on Portfolio_Decision)

Every Portfolio_Decision export since the cockpit was certified shows `Rel` and `DQ` blank for the US holdings and the
backend writing `Low confidence (reliability ?) capped TRIM -> HOLD` and `Reliability ? below add minimum 70`: with no
reliability on the holding row, no action can leave HOLD. The cockpit sends those two columns from My_Portfolio by exact
header (`PD_HOLDING_FIELDS`), so the question was whether My_Portfolio carried them. Evidence, in the order it was read:

1. **Sync run #4918 (18 Oct, 22:29 UTC), core-pages job.** My_Portfolio was served by exactly one
   `POST /v1/enriched/sheet-rows` (the market pages go to `/v1/analysis/sheet-rows`). The runner then logged
   `[DECISION-RECONCILE] page=My_Portfolio reco_col=71 action_col=113 block_col=114 rows=4 changed=3
   distinct=[('ACCUMULATE', ''), ('BUY', ''), ('HOLD', '')]`: column 113 (`Final Action`) was blank on all four rows,
   and three buy-family rows were rewritten to WATCH / HOLD by invariant 2, which fires on a non-empty Block Reason.
   A row whose gate ran carries `""` there; `str(None)` is `"None"`, which is non-empty. The response carried `None`
   in the gate columns, so the sheet got `None` where it did not get the reconciler's WATCH / HOLD.
2. **Workbook DQ audit, 17 Oct (run 38990567104).** DQ-51 `weight_basis` FAIL: `actual_weight`, `target_weight`,
   `weight_gap` and `rebalance_action` blank on 4/4 holdings; DQ-34 counts 0 My_Portfolio rows. Those columns are filled
   by `_compute_portfolio_fields`, which only runs on the engine's `get_sheet_rows` path.
3. **Offline reproduction of the engine path.** `DataEngineV5.get_sheet_rows("My_Portfolio")` with a fake sheet reader
   (holdings with blank or absent gate cells) and fake live quotes populates all eight gate columns, under the default
   guards and under the production `[GUARDS]` line (settle observe, fc_tuple observe, settle_unresolved enforce, surface
   blocked/fetchfail on). The engine was not the gap.
4. **The route.** `/v1/enriched/sheet-rows` is bridge-first: `_resolve_bridge_impl` returns
   `routes.analysis_sheet_rows._analysis_sheet_rows_impl` for every instrument page, and `_call_with_tolerant_signatures`
   tries eight fixed keyword sets. That implementation has nine parameters and no defaults (`x_api_key` since #719,
   6 Oct). `inspect.signature(...).bind(**attempt)` raises `TypeError` on all eight: the first two miss `x_api_key`,
   the rest miss `include_matrix_q` / `mode` / `request`. The helper returned `None` with
   `all_signatures_typed_mismatch` and no log line, and `_sheet_rows_handler` served the page from
   `_build_instrument_rows`: `get_enriched_quotes_batch` rows normalised onto the contract keys and never passed through
   `core.data_engine_v2._strict_project_row`. The factory does not run the investability gate (the engine says so at its
   Top_10 call site), so the eight decision columns and the portfolio block left this route blank.

My_Portfolio is the only page the dashboard sync reads through this route (the four market pages take the analysis
gateway), which is why the gap was invisible on Global_Markets and visible on every holding.

## What

### FIX 1 [BRIDGE-SIGBIND] (`TFB_ENRICHED_BRIDGE_SIGBIND`, default ON)

- `_bind_kwargs_to_signature(fn, payload_kwargs)`: reads the callable's own signature; each named parameter takes the
  matching bridge kwarg (None values included, the implementation types them Optional), a required parameter absent from
  the kwargs takes None, nothing else is passed. A `**kwargs` callable or a required positional-only parameter returns
  None, so the old first attempt keeps serving those.
- `_call_with_tolerant_signatures`: the bound set is attempt 0, sent as-is (`"bound": True` in `call_summary`); the
  eight v8.5.2 attempts follow unchanged (None values still dropped). Outcome labels unchanged.
- `_delegate_sheet_rows_via_bridge`: `all_signatures_typed_mismatch` logs one WARNING naming module, callable and
  attempt count. The handler's own `meta.dispatch` stays `bridge_sheet_rows`; the implementation's label travels as
  `meta.upstream_dispatch` (before, the upstream meta overwrote it in the envelope spread).
- `=0`: the ladder is the v8.5.2 one byte for byte (B4 pins eight typed-mismatch attempts, no `bound` key).
- Pagination (Codex P2 on #798, verified): with the bridge working, a nonzero `offset` was applied by the implementation
  (body wins over the query string there) and again by `_sheet_rows_handler`, so `limit=10&offset=10` would have turned
  the upstream rows 10-19 into an empty page. The bridge now asks for the unsliced range `[0, offset+limit)` and the
  handler keeps the one slice (B11). No live caller sends an offset today (the sync sends `limit` only).

### FIX 2 [FALLBACK-BOUNDARY] (`TFB_ENRICHED_FALLBACK_BOUNDARY`, default ON)

- `_publish_fallback_row`: deep-copies the engine-direct row (a cached quote is never mutated), lays the alias-resolved
  contract values under the raw engine fields, and projects through `_strict_project_row` (reconcile, investability
  gate, reco coherence, analyst/trend block, settle holdback, presentation): the boundary the analysis router already
  uses in `_finalize_raw_instrument_row`. On any failure the bare `_normalize_row` projection is kept and a WARNING names
  the symbol.
- `_build_instrument_rows` meta gains `publication_boundary` (the function name, or None) and `publication_boundary_rows`.
- `=0`: the bare projection, gate columns None (B8 pins the OFF state as the v8.5.2 behaviour).

The fallback does not run the cross-sectional `_compute_portfolio_fields` (weights, drift decision); the bridge path does.

## Audits

| Battery | Result |
| --- | --- |
| `tests/test_enriched_bridge_binding.py` B1 real callable binds, B2 the v8.5.2 ladder cannot (kept as the negative), B3 bound call reaches a no-default implementation, B4 kill switch, B5 `**kwargs` callable, B6 handler dispatch `bridge_sheet_rows` + `upstream_dispatch`, B7 gated fallback + cached quote untouched, B8 boundary kill switch, B9 one WARNING on mismatch and none when bound, B10 version + CI, B11 a nonzero offset applied once | 11 passed |
| Route neighbours: `test_route_error_redaction`, `test_auth_wall_scope_path` (W15 pin moved), `test_board_funding_route`, `test_route_mount_compat`, `test_redaction_boundaries`, `test_schema_alignment`, `test_current_schema_contract`, `test_adv_typeerror_retry_p130` | see the PR CI receipt (local: 94 passed, 9 skipped after the pin move) |
| `tests/test_ob_research_diversify.py` after the clock-field scrub | 13 passed |
| ruff E9/F63/F7/F82, py_compile | clean |

## CI flake fixed in the same push

PR #798's push-event run (39230946600) failed the lean lane on
`tests/test_ob_research_diversify.py::test_off_explicit_zero_equals_unset` while the pull_request run of the same commit
passed: the two builds it compares were a few seconds apart and the quote-freshness detail `age_min` (minutes since the
quote instant, rounded to 0.1) read 4.3 on one and 4.4 on the other. `_strip_volatile` now scrubs `age_min` wherever it
sits; no production code involved.

## Arming and read-back (owner)

1. Nothing to set: both switches default ON. After merge, deploy `main` on Render (manual deploys).
2. Next scheduled sync: the core-pages log line `[DECISION-RECONCILE] page=My_Portfolio ... changed=0` with real tokens in
   the second position of each pair (INVEST / WATCH / DO_NOT_INVEST), not `''`.
3. Portfolio_Decision after its next refresh: `Rel` and `DQ` carry numbers for the US holdings and the
   `Low confidence (reliability ?)` cap line disappears; whether a holding then reads ADD or TRIM is the backend's call
   on real numbers.
4. Render logs: `[enriched_quote v8.6.0] bridge typed mismatch` must not appear. If it does, the bridge is failing for a
   new reason and the gated fallback is serving; report the line.
5. Next workbook backup's DQ artifact: DQ-51 `weight_basis` should pass (weights come back with the bridge path).
6. The 07:40 Apps Script My_Portfolio auto-refresh writes through `page.primaryEndpoint` (00_Config.gs, not mirrored).
   If that is `/v1/enriched/sheet-rows`, this fix covers it; if it is another route, the `_Status` endpoint column for
   My_Portfolio says which. Worth one look.

Rollback: `TFB_ENRICHED_BRIDGE_SIGBIND=0` and `TFB_ENRICHED_FALLBACK_BOUNDARY=0` (v8.5.2 behaviour), or revert.

## Known limits

- With the bridge working, a My_Portfolio request runs the analysis implementation, which calls the engine's
  `get_sheet_rows` up to twice (root proxy, then adapter) instead of one engine-direct batch. Bounded by
  `TFB_ENRICHED_BRIDGE_TIMEOUT_SEC` (25 s); a timeout now lands on the gated fallback, not on blank columns.
- The route cannot be exercised from outside without the app token; the proof here is the signature binding on the real
  callable plus the handler-level tests with a fake implementation and a fake engine.
