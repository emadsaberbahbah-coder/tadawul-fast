# Commit Sheet - EODHD analyst target: `eodhd_provider` v4.19.0 + `data_engine_v2` v5.159.0 (`TFB_EODHD_ANALYST_TARGET`) - 2026-10-28

**Scope:** weekly-review defect D2 (28 Oct): 23 of 31 board candidates held on `analyst_lkg:<age>h` ("Held target - no new money"). Root cause: the Yahoo fundamentals breaker has been open on HTTP 429 during every sync since 21 Oct, the EODHD fallback carries fundamentals but no analyst target, so the engine restores the last known target and the opportunity builder holds the row. This change lets EODHD supply the target, behind a default-off flag. Nothing is armed by this PR.

**Files:**
- `core/providers/eodhd_provider.py`: 4.18.5 -> 4.19.0. WHY block prepended; earlier text kept verbatim. New `_eodhd_analyst_target_enabled()` and `_analyst_target_fields()`; `fetch_fundamentals` adds `target_mean_price` and `analyst_count` to the patch and the segment `at1:` to the fundamentals cache key, only when armed.
- `core/data_engine_v2.py`: 5.158.0 -> 5.159.0. WHY block prepended. New `_eodhd_analyst_target_enabled()`, `_row_has_eodhd_analyst_tag()`, `_EODHD_ANALYST_TAG`, `_EODHD_ANALYST_SNAPSHOT_FIELDS`; anchored edits in `_apply_eodhd_fundamentals_fallback`, `_fund_lkg_capture`, `_fund_cache_lookup`, `_fund_cache_decide`, `_fund_lkg_redis_set`, `_fund_lkg_redis_get`.
- `scripts/verify_deployment.py`: both pins; FLAGS row `TFB_EODHD_ANALYST_TARGET` (default `0`, not required).
- `docs/render_env_contract.md`: regenerated; one row added.
- New: `tests/test_eodhd_analyst_target.py`, 24 cases.
- `.github/workflows/ci.yml` 1.1.53: the new test runs in the full-dependency work-brief lane next to `tests/test_yahoo_id_alias.py`.
- This sheet.

## S1 - Evidence
**The board (28 Oct, weekly review D2).** 23 of 31 candidates carried `analyst_lkg:<age>h` and the builder's reason "Held target (analyst_lkg:...) - no new money" (`TFB_OPP_HELD_TARGET_NO_NEW_MONEY=1`).

**The provider (Render logs).** `[YF-CB v6.9.1] OPEN reason=info_empty status=429 ... yfinance fundamentals suspended for cooldown; EODHD fallback carries fundamentals`, repeating every 45-90 s during every sync since 2026-10-21 09:28Z: 100+ events on 23 Oct, dozens on 28 Oct 00:00-00:52Z, none on 15-16 or 20 Oct.

**The code.**
- `eodhd_provider.fetch_fundamentals` (v4.18.5) emitted no target key, although the EODHD payload carries `AnalystRatings.TargetPrice` and `Highlights.WallStreetTargetPrice`.
- `_apply_eodhd_fundamentals_fallback` filters the canonical patch through `_YAHOO_FUNDAMENTAL_FIELDS`, which already admits `target_mean_price`, `forecast_price_12m` and `forecast_source` (v5.77.8), fill-only.
- `_canonicalize_provider_row` maps `target_mean_price` to `forecast_price_12m` and tags `forecast_source = provider_target` behind the v5.77.18 plausibility gate - but the gate judges against the patch's own price, and this patch has none (v5.77.19 Fix 1 found the same hole on the Yahoo path).
- `_phase_ii_quality_forecast` runs after the fallback (`_apply_phase_dd_enhancements`): a row that already carries `provider_target` is captured into the target LKG, not restored from it, so no `analyst_lkg` tag is written.
- The fund cache (`TFB_EODHD_FUND_CACHE=enforce` live, TTL 24 h, three syncs a day at 04:17 / 12:17 / 20:17 UTC) asks EODHD once per symbol per day. On the other two syncs the hit fills ratios from the fund-LKG snapshot and, by P-154c design, forgoes the target (`:nt`); Phase-II then restores `analyst_lkg:<age>h` and the row is held again. The L2 layer whitelists `_FUND_LKG_FIELDS` on write and read, so a per-worker memory snapshot alone would not reach the other workers.

## S2 - WHAT
**Provider (armed only).**
- `target_mean_price`: `AnalystRatings.TargetPrice`, else `Highlights.WallStreetTargetPrice`; a positive finite number only.
- `analyst_count`: the sum of `StrongBuy + Buy + Hold + Sell + StrongSell` when at least one bucket is a number and the sum is positive. (The engine drops this key at canonicalization today; it is carried for the Target Price column work and costs nothing.)
- Cache key `f:eps_basis_v1:<margin_version>:at1:<SYM>`, so a cached unarmed patch is never served armed and the other way round.
- Off: `_clean_patch` drops the two `None` keys; patch and key byte-identical to v4.18.5.

**Engine (the seam; dead code while the patch carries no target).**
1. Miss (provider called): a patch with `target_mean_price` is canonicalized from a COPY with the row's known price injected (Fix 1), the gate's `provider_target_implausible_dropped_12m` tag is carried back (Fix 2), and a 12M leg that lands is tagged `analyst_consensus_eodhd` (substring-safe: no cap / forecast / target / roi / drop / reject).
2. Capture: with the flag on, a row whose 12M target came from EODHD on this pass (tag present, `forecast_source = provider_target`) also stores `target_mean_price` / `forecast_price_12m` / `forecast_source` in its fund-LKG snapshot, in memory and in L2. The AZ-2 degraded-block restore filters to `_FUND_LKG_FIELDS` and never sees these keys.
3. Hit (enforce): a fill that carries the target is judged against the row's price of the day by the same gate before it lands (refused: drop tag, ratios still fill; accepted: provenance tag); the `:nt` suffix is omitted when the fill carries the target.
4. Off after an armed capture: `_fund_cache_lookup` and the L2 read strip the block, so off stays byte-identical to v5.158.0 for the life of any armed-era snapshot.

**Disclosure on the row.** `analyst_consensus_eodhd` (12M from EODHD, this pass or from a snapshot at most 24 h old; the snapshot's age is in `fund_cache:hit:<age>h:<n>`), `provider_target_implausible_dropped_12m` (refused), `eodhd_fundamentals_fallback_applied` as before.

## S3 - Tests (`tests/test_eodhd_analyst_target.py`, 24 passed; offline, real provider and engine modules)
| Case | Proves |
|---|---|
| P1 | Off (unset, `0`, `off`, `false`): no target keys, v4.18.5 cache key. |
| P2 | On: target from `AnalystRatings.TargetPrice`, count 20 from the five buckets, cache key carries `at1`. |
| P3 | `WallStreetTargetPrice` fallback; zero, negative, `bad`, `nan` add nothing; a count alone; a zero sum is no count; strings parse. |
| P4 | An unarmed cached patch is not served armed, and the other way round. |
| E1 | A patch without the key takes the v5.158.0 path, armed or not. |
| E2 | Miss, plausible (125 on 100): the three fields land, provenance tag, injected price never travels, Phase-II derives 3M / 1M, captures into the target LKG, never writes `analyst_lkg`. |
| E3 | Miss, implausible (417 on 146): dropped against the row price, drop tag, ratios still fill, snapshot carries no target. |
| E4 | Capture carries the block only when armed, tagged and `provider_target`; a Yahoo-sourced target or an overwritten source does not. |
| E5 | Hit (enforce): target filled from the snapshot, no `:nt`, provider not called, Phase-II never writes `analyst_lkg`; a snapshot without the block keeps `:nt`. |
| E6 | Hit, 125 on today's 20: dropped, ratios still fill, drop tag. |
| E7 | Off after an armed capture: `:nt`, no target served, lookup strips the block. |
| E8 | L2 round trip: the block is written and read only while armed. |
| E9 | Versions, pins, FLAGS row, env-contract row, ci lane, tag substring safety. |

Golden negative on `18ab6e38` (main): 15 of 24 cases fail (P2-P4, E2-E9); the 9 that pass are the off-path invariants. E3 fails on main because main tags `provider_target` on an EODHD target with no gate - the Fix 1 hole this seam closes.

Neighbouring suites pass: fund cache-first P-154c, EPS basis contract, margin unit cache / producer / scoring contracts, fund unit sentry, fund sentry repair, Yahoo id alias, crypto provider identity, identity guard, recent fixes, render env contract, flag registry, optional rollout flags, activation rollout evidence, data engine v2, engine cache expiry, F-7 settle v2, forecast score basis (681 cases). Manifest pins pass. Workflow audit: errors 0, warnings 0.

## S4 - Arming (owner, one step, outside a sync)
1. Render -> Environment -> add `TFB_EODHD_ANALYST_TARGET` = `1` -> **Save, rebuild, and deploy**. Not while a `daily_sync` is running and not within 15 min of a Top-10 slot. The deploy restarts the workers, so the provider's in-memory fundamentals cache starts empty under the new key.
2. The first sync after arming asks EODHD once per symbol (the fund cache's daily miss) and the target lands with `analyst_consensus_eodhd`; the next two syncs fill it from the snapshot.
3. Claude reads back: on `Global_Markets`, rows carrying `analyst_consensus_eodhd`; the share of board candidates carrying `analyst_lkg:` (28 Oct: 23 of 31); Render logs for `[engine_v2 v5.159.0 ...` and the absence of new startup warnings.

**Expected effect and its limit.** Targets return for every name EODHD covers with a consensus (US and most developed markets; Saudi names depend on EODHD coverage, and a name without `AnalystRatings` keeps the LKG path). The gate still refuses an out-of-band target, so a crashed name shows the drop tag rather than a stale upside.

## S5 - Rollback
Unset `TFB_EODHD_ANALYST_TARGET` and restart: provider patch, cache key, snapshot fill and tags are byte-identical to v4.18.5 / v5.158.0, and an armed-era snapshot is served without its target block. Or revert the two files to `18ab6e38`.

## S6 - Follow-up after merge: `data_engine_v2` v5.159.1 (PR after #865)
#865 merged at `cc450da7` with one red CI job and one open review finding. Both are fixed here; nothing is armed.

| Item | Cause | Fix | Proof |
|---|---|---|---|
| CI: *Whole suite (pinned Python 3.11)*, 1 failed / 6216 passed - `tests/test_core_repair_tiny_quotes.py::TinyQuoteCliTests::test_cli_provenance_matches_helper_and_workflow_while_tiny_probe_defaults_off[tiny_quote_readback=True]`, exit 1 != 0 | The test's fake `/health` pins `ENGINE = "5.158.0"` and the readback CLI compares it with the source constant; #865 moved the constant. My pre-push run covered the neighbouring suites, not the whole suite. | Pin moves in place to `5.159.1` (the same rule the file states for `ROUTE`). | Reproduced locally on `cc450da7`; passes after the pin; whole suite run locally before this push. |
| Codex P2 on #865: *gate cached forecasts independently of target-mean publication* | With `TFB_TARGET_MEAN_PUBLISH=0` (supported kill-switch) Fix AI never publishes `target_mean_price`, so the armed capture stores `forecast_price_12m` / `forecast_source` only, and the hit gate was entered on `target_mean_price` alone: 125 captured at 100 and replayed at 20 landed as `provider_target`, unjudged. Production runs with the switch on (default; not in the Render list), so the path was not live. | `_fill_carries_eodhd_target(fill)`: `forecast_price_12m` OR `target_mean_price`. The hit gate is entered on it and judges `forecast_price_12m` first; the `:nt` suffix uses the same test. | New `test_e10_publish_off_snapshot_is_still_gated_on_the_12m_forecast`: fails on `cc450da7` at the replay at 20 (the forecast lands), passes here; refused at 20, accepted at 100 without `:nt`. |

Also: the test fixture now clears the real gate name `TFB_PROVIDER_TARGET_GATE_ENABLED` (it named `TFB_PROVIDER_TARGET_GATE`, which nothing reads) and `TFB_TARGET_MEAN_PUBLISH`.

Pins: `data_engine_v2` 5.159.1 in `scripts/verify_deployment.py`, `tests/test_eodhd_analyst_target.py` (E9) and `tests/test_core_repair_tiny_quotes.py`. Arming (S4) is unchanged; arm after this follow-up merges.

Not a licensed financial adviser; the owner decides and executes every trade.
