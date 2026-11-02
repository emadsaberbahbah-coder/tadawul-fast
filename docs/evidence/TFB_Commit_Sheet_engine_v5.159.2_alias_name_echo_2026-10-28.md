# Commit Sheet - data_engine_v2 v5.159.2 + normalize v5.7.1: the name rescue accepts the verified Yahoo alias it requested - 2026-10-28

**Scope:** readiness priority 1 (owner attachment, 28 Oct 14:47 Riyadh): the one coverage failure that keeps the 28 Oct cohort from final publication. Behaviour changes only while `TFB_YAHOO_ID_ALIAS=1` (armed since the 09:48 deploy); unset is byte-identical. No threshold, gate, floor, universe or other page is touched.

**Files:**
| File | Version | Change |
|---|---|---|
| `core/symbols/normalize.py` | 5.7.0 -> 5.7.1 | `yahoo_id_alias(symbol)`: the verified id that `to_yahoo_symbol()` sends for one of the nine page symbols while the flag is on, else `''` (read at call time). `to_yahoo_symbol` and every route unchanged |
| `core/data_engine_v2.py` | 5.159.1 -> 5.159.2 | `_bb1_echo_matches` also accepts an echo equal to that alias; new `_bb1_alias` helper |
| `scripts/verify_deployment.py` | pins | normalize 5.7.1, data_engine_v2 5.159.2 |
| `tests/test_yahoo_id_alias.py` | +A10-A12 | 10 new cases (A12 drives the real `_apply_identity_rescue`) |
| `tests/test_core_repair_tiny_quotes.py`, `tests/test_eodhd_analyst_target.py` | pins | stacked engine pin 5.159.2; every other assertion kept |

## S1 - Evidence
- Final audit of sync run `40673830237` (scheduled, 28 Oct 13:41-15:48 Riyadh), job summary: `coverage FAIL COVERAGE_GATE Commodities_FX - name coverage 98.45% below 99.00%`. It is the only coverage failure; Market_Leaders 96.1 %, Global_Markets 97.5 %, Commodities_FX fresh 100 %, Mutual_Funds 97.7 % all pass. The decision failures in the same summary are the expected pre-render ones (Top-10 HELD while the sync ran).
- `Commodities_FX` read-back 15:56: 453 rows, 7 without a Name - exactly the seven coins aliased by normalize v5.7.0 (#864): SUI-USD 0.94, GRT-USD 0.02, APT-USD 0.70, UNI-USD 7.15, COMP-USD 21.58, IMX-USD 0.14, STX-USD 0.30, provider `yahoo_chart`, updated 14:43, warning `... identity_echo_refused ...`. 446 / 453 = 98.45 %.
- Before #864 the same rows had a name but no price (fresh 94.9 %); the alias fixed the price and exposed the name refusal.
- After the sync the Top-10 rendered at 15:52 with `status: ok`, `source readiness VERIFIED (run 40673830237)`, disclosed pool counts and `pool_reconciliation_v1`; Portfolio_Decision at 15:45 with `source readiness VERIFIED (run 40673830237)`. So the name floor is the last blocker of this cohort.

## S2 - Root cause and change
- `DataEngineV5._apply_identity_rescue` (BA-1, v5.118.0) fills a blank name from the Yahoo chart meta, and BB-1 (v5.119.0) accepts the meta name only when the echoed symbol matches what was requested. The provider sends Yahoo the verified alias (`SUI-USD` -> `SUI20947-USD`, normalize v5.7.0), so the meta echoes `SUI20947-USD`; `_bb1_echo_matches` compares raw, canonical and base forms of `SUI-USD` only and refuses the id we asked for as a borrowed identity. The #864 tests covered the adapter guard (YC-1) and the patch guard (AU-1) but not this rescue.
- Fix: `_bb1_echo_matches(requested, echo)` returns True when the old comparison matches (unchanged), or when the echo equals `normalize.yahoo_id_alias(requested)` - exactly one verified id per page symbol, only while the flag is on. A crossed coin (`SUI-USD` answered as `GRT6719-USD`), every control pair, the class guard, the curated crypto-name screen and the BA-2 fallback are unchanged.
- Names that will land (from the #863 read-back): Sui USD, The Graph USD, Aptos USD, Uniswap USD, Compound USD, Immutable USD, Stacks USD - each passes the curated screen (`crypto_name_matches`).

## S3 - Tests
| Test | Proves |
|---|---|
| A10 `test_a10_yahoo_id_alias_follows_the_flag` | `''` off; the nine ids on (any case); `''` for controls, the targets themselves and junk input |
| A11 `test_a11_echo_gate_accepts_only_the_verified_alias` | off: the live refusal for all nine; on: each echo of its own alias matches; all 72 crossed alias pairs and 8 control pairs are unchanged |
| A12 `test_a12_real_rescue_names_the_aliased_coins` x7 | the real rescue: off -> blank name + `identity_echo_refused` (the live defect); on -> the verified name, `name_from_chart_meta`, price untouched |
| A12 `test_a12_real_rescue_still_refuses_a_crossed_coin` | on: a crossed coin stays refused; a plain echo of the page symbol is unchanged |
| A9 + stacked pins | normalize 5.7.1 / engine 5.159.2 pinned in the manifest and the two engine-pin tests |

Golden negative: the new file on `0c3d3e21` fails 10 of its 23 cases (A10, A11, A12 x7, A9); the crossed-coin case passes there, as it should. CI lane: unchanged (`tests/test_yahoo_id_alias.py` already runs in the full-deps step, ci.yml 1.1.52); no ci.yml change.

## S4 - Owner steps (after merge)
1. Render -> Save, rebuild, and deploy (latest main) **outside a sync** (check GitHub Actions: no "Tadawul Dashboard Advanced Sync" in progress or queued). The same deploy can carry `TFB_EODHD_ANALYST_TARGET=1` (D2, pending) and #869/#870 (default off; their envs stay unset until their own evidence run).
2. No env change for this fix: `TFB_YAHOO_ID_ALIAS=1` is already armed.
3. Read-back after the next full sync: `/health` shows data_engine_v2 5.159.2; `Commodities_FX` has 453/453 names (the seven coins tagged `name_from_chart_meta`); the final job summary has no `COVERAGE_GATE Commodities_FX` row.

## S5 - Rollback
Revert the PR (or unset `TFB_YAHOO_ID_ALIAS`, which also returns the seven rows to no price).

## Correction record
| Defect | Root cause | Change | Tests | Deployed/installed version | Effective mode | Live acceptance | Remaining dependency |
|---|---|---|---|---|---|---|---|
| Final publication blocked: Commodities_FX name coverage 98.45 % < 99 % (run 40673830237) | BB-1 name rescue refuses the chart-meta echo of the verified alias the provider itself requested (`SUI20947-USD` vs page `SUI-USD`); #864 tests missed this path | Echo gate accepts exactly `normalize.yahoo_id_alias(requested)` while `TFB_YAHOO_ID_ALIAS=1` | 10 new (A10-A12), golden negative 10/23 fail on `0c3d3e21`; full suite | Merged: pending. Render: pending (live engine 5.159.1 at `18ab6e38`) | Alias flag armed (1); fix has no own switch | Pending: next full sync after deploy shows 453/453 names and no CFX coverage row; then release of that cohort | Owner merge + deploy outside a sync; then one full sync and the release slot (or a dispatched release) |

Not a licensed financial adviser; the owner decides and executes every trade.
