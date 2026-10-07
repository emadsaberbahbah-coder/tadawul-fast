# TFB Commit Sheet - data_engine_v2 v5.151.4 unpriced-patch fill - 2026-10-07

This sheet closes review finding N1 from
`docs/evidence/TFB_Review_Codex_PRs_722_723_724_2026-10-07.md`.

- **Gate:** Render env `TFB_ENGINE_UNPRICED_FILL`. It is off by default; set
  it to `1` to arm. Unset or `0` is byte-identical to v5.151.3.

## Evidence
- **What #722 changed:** v5.151.2 stopped merging any provider quote patch
  without a positive price. The aim was correct: a failed quote must not lend
  its identity, timestamp or error to a row priced by a later provider.
- **The side effect:** it also discarded that patch's name, sector and
  fundamentals, which v5.151.0 kept.
- **Synthetic AAPL.US** (EODHD quote unpriced with HTTP 429, Yahoo priced):
  - 5.151.0: name, sector, market_cap, pe_ttm and eps_ttm all present.
  - 5.151.3: all of them None.
- **Live symptom, 2026-10-07 evening Full Refresh Coverage run 37680095784:**
  name coverage was 97.93% on Global_Markets and 98.02% on Mutual_Funds,
  against a 99% minimum. A causal link to N1 is plausible but unproven,
  because the workbook was not read from this container.

## What changed (one site, the quote factory)
- **Collecting the patches:** unpriced patches are collected while the
  provider loop runs. Nothing else in the loop changes.
- **When the fill runs:** after the priced merge, and only if the row has a
  positive live price and the gate is on.
- **What gets filled:** each unpriced patch fills *blank* fields from
  `_UNPRICED_FILL_FIELDS` only. That list covers display identity (name,
  asset_class, exchange, country, sector, industry) and fundamentals
  (market_cap ... avg_volume_30d).
- **What is never taken:** price, OHLC, volume, currency, timestamps,
  data_provider, analyst targets, 52-week levels, errors and warnings.
- **Conflicts:** the priced provider wins every conflict, because the fill
  runs after it and only fills blanks.
- **Identity check:** a patch whose declared identity is disjoint from the
  requested symbol (the AU-1 `_engine_patch_identity_mismatch` check) is
  skipped entirely.
- **Tagging:** each provider that filled anything adds the tag
  `unpriced_fill:<provider>`. The token is substring-safe against the
  acquisition INVALID regex and the repeat-gate words.
- **Version pin:** the `verify_deployment` pin is moved to 5.151.4.
- **Removals and ASCII:** no function was removed, and all added lines are
  ASCII.

## Validation
`tests/test_engine_unpriced_fill.py` passes 7/7. With the gate off, the
5.151.3 loss is reproduced exactly. With it on, the harness covers:
- restore
- no leak of price, currency, provider or error
- priced provider wins conflicts
- crossed identity is skipped
- no fill when nothing priced the row

Golden negative against the base engine: 4/7, failing the restore,
conflict-fill and version cases.

Other results:
- **Lean CI list (33 files):** 826 passed, 5 skipped.
- **Engine and provider suites (#722, manifest pins, schema):** 183 passed.
- **Engine suite:** 39 passed.

## Known limits
- The 99% name-coverage bar may still miss for rows where no provider
  returns a name at all.
- The fill does not re-run the AW-2 EODHD fundamentals fallback. When the
  fill has supplied a name, that fallback simply no longer fires, which
  saves a call.

## Operator steps
1. Merge. Behaviour does not change until the gate is set.
2. Set the Render env `TFB_ENGINE_UNPRICED_FILL=1`. Render redeploys.
3. After the next scheduled sync, read the Full Refresh Coverage audit and
   compare name coverage on Global_Markets and Mutual_Funds against 97.93% and
   98.02%. Rows that were filled carry `unpriced_fill:eodhd` in Warnings.
4. Rollback: unset the env.
