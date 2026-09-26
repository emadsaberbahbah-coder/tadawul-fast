# TFB Commit Sheet — core/data_engine_v2.py v5.150.0 [P-154c EODHD FUNDAMENTALS CACHE-FIRST + NEGATIVE CACHE + PAGE SKIP]

Date: 2026-09-24 (Riyadh) · Author: Claude (One-Pass Script Protocol) · Operator: Emad
Register: P-154c (child of P-154 EODHD quota exhaustion; pairs with P-163b cron over-subscription)

## 1. Base pin (S1)
| | |
|---|---|
| Repo / branch | emadsaberbahbah-coder/tadawul-fast · main |
| File | core/data_engine_v2.py |
| Base version | 5.149.0 (`__version__` at L3773) |
| Base SHA-256 | `b425b479172f5bf5…` — live-fetched twice (raw, cache-busted) at build time, byte-identical (zero drift vs the 2026-09-23 Build #3 record) |
| Base lines | 18,339 |

## 2. Delivered (S5)
| File | Repo path | SHA-256 (prefix) | Lines |
|---|---|---|---|
| data_engine_v2.py | core/data_engine_v2.py | `ef9ad4fde067e7c2…` | 18,670 (+333 / −2; the two removed lines are the replaced `__version__` string and the replaced `[GUARDS]` format string) |
| test_de_fund_cache_first_p154c.py | tests/test_de_fund_cache_first_p154c.py | `3a76b259ee84b8da…` | 491 |
| this sheet | docs/evidence/TFB_Commit_Sheet_data_engine_v2_v5.150.0_2026-09-24.md | — | — |

## 3. Evidence that justifies the build (2026-09-24 export + _Run_Log)
- EODHD 400,000/400,000 EXHAUSTED at 00:54 Riyadh (third time in four days); the 20Z run ran blind (402 on 5,789 GM + 2,393 MF + 336 CFX rows); the 02:01 cockpit hard-exited four seats on that epoch (P-168).
- The 04Z run cost 83k calls (37,645 → 120,665). On the same export the fundamentals fallback fired on **3,774 GM + 2,373 MF + 223 CFX rows in one pass** (≈68k calls at 10/request ≈ 78% of the run); 611 rows are paid no-ops (empty answer, re-fetched every pass).
- HEAD source (v5.149.0): `_apply_eodhd_fundamentals_fallback` tests only the debt_to_equity / free_cash_flow_ttm gap, then fetches — no cache, no memory of an empty answer. The fund-LKG (v5.117.0 + v5.138.0 Redis L2, 120h in production) already holds the provider-completed 24-field block per symbol but is consulted only AFTER a fetch on a DEGRADED block.
- Cacheable share on the real export: GM 3,771 of 3,774 fallback rows (100%) satisfy the LKG capture rule (≥8 of 24 fields + anchor); Mutual_Funds 13%, Commodities_FX 4% — hence the page-skip leg. MF/CFX have never seated a candidate (0 of 1,772 _Selection_Log rows).

## 4. Design (S2) — one gate, three legs, all inside the gap branch
Gate `TFB_EODHD_FUND_CACHE` = off (default, byte-identical) | observe | enforce — explicit words only ("1"/"true"/"on" read as off), read at call time, printed in the `[GUARDS]` boot line (`fund_cache=`) and `/health engine_gates.eodhd_fund_cache` (+ `fund_cache_stats` per-worker counters).
Parameters: `TFB_EODHD_FUND_CACHE_TTL_H` (default 24, floor 1, ceiling = fund-LKG TTL), `TFB_EODHD_FUND_NEG_TTL_H` (default 168, floor 1), `TFB_EODHD_FUND_FALLBACK_SKIP_PAGES` (csv page names, default empty).

Order inside `_apply_eodhd_fundamentals_fallback`, only after the existing gap test says a request would be spent:
1. **page skip** — page in the skip list → `fund_cache:skip_page` (enforce: no request; observe: `would_skip_page`, request still made).
2. **negative cache** — a live mark (memory, then L2 key `tfb:fund_neg:v1:<SYM>`) → `fund_cache:neg:<age>h` (enforce: no request). Marks are written ONLY under enforce, when a request lands nothing: `neg_mark:empty` (empty payload), `neg_mark:refused` (AW-1 identity refusal), `neg_mark:nofill` (no whitelisted field filled). observe counts the same events as `would_neg:<reason>`.
3. **cache-first** — the fund-LKG snapshot (memory, then L2 via the existing v5.138.0 client/breaker) younger than the cache TTL that fills ≥1 still-missing whitelisted field → enforce: FILL-ONLY through the same `_filter_patch_to_missing_fields(row, fields, _YAHOO_FUNDAMENTAL_FIELDS)` as the provider patch, tag `fund_cache:hit:<age>h:<n>` (+`:nt` when the row lacks target_mean_price), no request, and NO `eodhd_fundamentals_fallback_applied` tag (honest: no provider call); observe: `would_hit:<age>h:<n>`, request still made, values untouched.
4. **capture guard** — `_fund_lkg_capture` refuses a row carrying `fund_cache:hit:` (the `fundamentals_lkg` precedent), so a hit can never refresh its own TTL: each symbol is asked of the provider again once per cache TTL.

Tags are substring-safe for `_apply_investability_gate` (no cap/forecast/target/roi/drop/reject/provider_target/price_bar_stale/xprovider_price_conflict) and never match an LKG taint substring. Every helper is pure/fail-open: any exception → "miss" → the provider path runs exactly as v5.149.0.

## 5. Edits (S3) — nine anchored edits, each anchor asserted count==1
| # | Site | Change |
|---|---|---|
| E1 | `__version__` | 5.149.0 → 5.150.0 + WHY v5.150.0 header block |
| E2 | `_fund_lkg_capture` | cache-hit guard after the taint check |
| E3 | before the v5.131.0 target-LKG section | new block: 9 constants + 12 helpers (`_fund_cache_mode/_ttl_h/_neg_ttl_h/_fb_skip_pages/_bump/_stats/_row_is_hit/_lookup`, `_fund_neg_lookup/_mark`, `_fund_cache_decide/_note_empty`) |
| E4 | `_apply_eodhd_fundamentals_fallback` pre-fetch seam | mode read + decide + short-circuit; empty-payload note |
| E5 | AW-1 refusal branch | `_fund_cache_note_empty(..., "refused")` |
| E6 | before `if filtered:` | `nofill` note |
| E7/E8 | `[GUARDS]` boot line | `fund_cache=%s` + arg |
| E9 | `surface_gate_states()` | `eodhd_fund_cache`, `fund_cache_stats` |

Proofs: `py_compile` PASS; AST 505 → 517 names (+12 helpers, **0 removed**); top-level constants +9 / −0; non-ASCII delta 0 (323 → 323), smart quotes 0; additions ASCII-only.

## 6. Harness (S4) — REAL module, dual tree, separate processes, ×3 identical digests
Fixtures: 340 engine-shaped rows built from the REAL 2026-09-24 export — 120 GM "hit" class (organically complete rows with the D/E+FCF gap re-created; the provider fixture answers with the export's own values), 100 GM paid-no-op class (empty / already-present-only answers), 60 GM no-gap controls, 40 Mutual_Funds, 20 Commodities_FX. The REAL `DataEngineV5._apply_eodhd_fundamentals_fallback` and the REAL `_fund_lkg_capture` run on every row; the only double is a counting provider module registered in the REAL `ProviderRegistry` (external wire). Sentry off, LKG on, Redis off except J6.

| Leg | Result (×3) | Digest |
|---|---|---|
| J1 default off, base v5.149.0 vs delivered | row-level output byte-identical (340 rows × 3 passes), provider called on every gap row every pass, zero `fund_cache` tags; base `972920dd…` / delivered `175280a4…` (version/stats fields differ), **row digest identical `5169bb00…`** | ✓ |
| J2 observe (+skip list) | values identical to base on 1,020/1,020 row-passes, calls identical 1,020/1,020, only `fund_cache:would_*` tags added: would_hit 240 (P2+P3), would_neg 360, would_skip_page 180, neg writes 0 | `5b6f4cbb…` |
| J3 enforce (+skip list) | P1: 220 requests (120 hit-class + 100 no-op), 100 `neg_mark`, 60 `skip_page`, 0 requests on MF/CFX; P2/P3: **0 requests**, 120 `hit` (39 `:nt`), 100 `neg`, 60 `skip_page`; hit rows' 24 fields + target equal to P1 on 120/120; capture guard 120 checked / 0 ts moved; tag violations 0 | `6d0617a1…` |
| J4 enforce, no skip list | as J3 on GM; **Mutual_Funds: 33 of 40 rows re-fetched on P2** (fund rows are too sparse to enter the LKG — cache-first cannot serve them; the skip list is required for the MF/CFX saving); CFX rows negative-cached after P1 | `14b6f909…` |
| J6 restart / cold worker (fakeredis L2, enforce) | P1 writes 290 L2 entries (190 snapshots + 100 marks); module reloaded (empty memory) → P2/P3: 0 requests, 120 hits served from L2 (l2 hits 120 / misses 0 / errors 0), 100 neg from L2, capture guard 0 moved | `6c3abf6e…` |

## 7. Battery
`tests/test_de_fund_cache_first_p154c.py` T1–T9 (six embedded real GM rows): **9 passed ×3** on v5.150.0; all 9 error on the v5.149.0 base (golden negative). Existing engine batteries fetched from HEAD (P-164, P-102, F-7, P-151, P-146, unit sentry, P-115b, P-143, F-6): **64 passed + 1 skipped** on base, **73 passed + 1 skipped ×3** on v5.150.0 (real core/scoring.py, core/symbols/normalize.py, core/reco_normalize.py, core/schemas.py from HEAD in the tree).

## 8. Deploy proof (owed on paste)
Render boot log: `[engine_v2 v5.150.0] module loaded`; `[v5.150.0 GUARDS] … fund_unit_sentry=enforce scoring_settle=observe fc_tuple=off w52_ceiling=on fund_cache=off`; `/health` `engine_version 5.150.0`, `engine_gates.eodhd_fund_cache="off"`, `fund_cache_stats` all zero; `startup_warnings []`, both workers clean. With the gate unset the live behaviour is v5.149.0-identical.

## 9. Arming plan (Render lane — every env change is a restart = one cold pass ≈ 17% of the day's quota, so bundle with the deploy)
- **Slot A (rides the v5.150.0 deploy):** `TFB_EODHD_FUND_CACHE=observe` + `TFB_EODHD_FUND_FALLBACK_SKIP_PAGES=Mutual_Funds,Commodities_FX` (the list does nothing without the gate; under observe it only tags). Read-back on the next full sync export: `fund_cache:would_hit` on ~3,000–3,700 GM rows (production L2 already holds v5.149.0 snapshots ≤24h old, so hits fire on the first pass), `would_neg:*` ≈ 600 (the paid no-op class), `would_skip_page` on 2,474 MF + 453 CFX rows; **zero value changes**, `[EODHD-QUOTA]` curve unchanged; `/health fund_cache_stats` counts.
- **Slot B (next evidence run):** `TFB_EODHD_FUND_CACHE=enforce`. Read-back = the quota curve: per-run cost falling from ~83k toward ~20–45k after the first enforce pass, `fund_cache:hit` tags, `neg_mark` → `neg`, `skip_page` on MF/CFX, `eodhd_fundamentals_fallback_applied` collapsing on GM. Falsifiable prediction: with the cron cut to three slots AND enforce, the daily peak stays below 60%.
- Kill / rollback: unset the gate (= off, byte-identical), or `git revert`. Marks and snapshots in L2 expire on their own TTLs.

## 10. Deliberate cuts / disclosures
- Cache-first serves the 24 canonical fundamentals only; the fallback whitelist also carries analyst-target and forecast keys — a hit forgoes a same-pass EODHD target fill (disclosed per row by `:nt`). On the real export 1,634 of the 3,774 GM fallback rows had no Target Price even AFTER the provider call, and the v5.131.0 target LKG covers the rest; the provider is still asked once per cache TTL per symbol.
- Under enforce with the skip list, MF/CFX rows keep whatever Yahoo delivered (no D/E/FCF fill). They are never decision inputs (0 INVESTABLE, 0 seats ever); their DQ/reliability display may shift on those pages — accepted, disclosed.
- Values served from the cache are the values captured after the FUND-SENTRY ran (engine units) — no re-conversion, no double scaling.
- Not built: a symbol-subset replay in the sync (P-163), the cron change (arming, GitHub lane), tiering by page (policy).

## 11. Register / next
- P-154c BUILT — awaiting commit + deploy proof + Slot A.
- P-163b (cron over-subscription) — arming pending (`0 4,12,20 * * *`).
- P-168 (stability/ADD clocks consume outage epochs) — GAS build next on the live 16_Decision source.
