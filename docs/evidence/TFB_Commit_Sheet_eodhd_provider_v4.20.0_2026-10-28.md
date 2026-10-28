# TFB Commit Sheet - eodhd_provider v4.20.0 [FUND-SUFFIX-MAP] (2026-10-28)

eodhd_provider 4.19.0 -> 4.20.0 - verify_deployment pin - tests/test_eodhd_fund_suffix_map.py (NEW) - tests/test_eodhd_analyst_target.py (two guards in the blocking lane, e9 pin floor)

Daily improvement 2026-10-28, one item, priority (a): a published fundamentals number is missing for a class of rows.

## S0 - Base and gate
| Item | Value |
|---|---|
| Base | main `0c3d3e21` (eodhd_provider v4.19.0, #865 / #867) |
| Gate | `TFB_EODHD_FUND_SUFFIX_MAP` = `off` (default; unset, blank or any other word) / `observe` / `enforce`; Render env, read per call |
| Scope | the fundamentals endpoint only (`EODHDClient.fetch_fundamentals`, the one `fundamentals/{code}` request site) |
| Off | byte-identical to v4.19.0: request path, cache key, patch and log lines |

## S1 - Evidence
- **Codes.** `normalize_eodhd_symbol` delegates to `core.symbols.normalize.to_eodhd_symbol`, then `_canonicalize_eodhd_suffix` (`.L`, `.XETR`, `.ETR`, `.TASE` only):
  - `1295.KL` stays `1295.KL`: `.KL` is an exchange suffix there but `_YAHOO_TO_EODHD_SUFFIX` has no `KL -> KLSE`.
  - `SON.LS` becomes `SON.LS.US`: `.LS` is not in `EXCHANGE_SUFFIXES`, so the default exchange is appended.
- **EODHD rejects both shapes.** calendar_sync run 40498374271 (27 Oct): "EODHD rejected 33 code(s)", among them the `.KL` and `X.LS.US` codes (calendar_provider v1.3.2 WHY).
- **EODHD accepts the venue codes.** calendar_sync run 40678905106, job 122784262698 (28 Oct 11:19Z), with `TFB_CALENDAR_SUFFIX_MAP` armed: "EODHD rejected 19 code(s): AEFES.IS, KCHOL.IS, TUPRS.IS, IG.MI, TIT.MI, T82U.SI, 1605.T, ... ; sent as EODHD exchange codes: IS.US->IS x3, KL->KLSE x2, L->LSE x13, LS.US->LS x3". No KLSE or LS code is rejected; the IS codes still are.
- **Why it matters now.** The Yahoo fundamentals breaker has been open since 2026-10-21 (v4.19.0 WHY: "EODHD fallback carries fundamentals"), so EODHD is these rows' fundamentals source.
- **Scale.** In `tests/fixtures/wp3ab_tuple_rows_2026-10-10.json`, 13 of the 2,033 Global_Markets rows are affected: 10 `.KL` (1295, 3816, 4197, 4715, 4863, 5183, 6742, 6888, 7084, 7113) and 3 `.LS` (COR, NOS, SON).
- **Routing.** `.KL` and `.LS` are not Yahoo-only (`provider_capabilities.yahoo_primary_reason` lists only `=F`, `^TASI.SR`, `.MI`, `.NZ`), so the engine's EODHD fundamentals fallback does run for them.

## S2 - What changed
- `_fund_suffix_map_mode()`: `observe` / `enforce` (trimmed, any case), else `off`.
- `_eodhd_fund_venue_code(sym)` (PURE): `X.KL -> X.KLSE`, `X.LS.US -> X.LS`; everything else unchanged.
- `_fund_suffix_map(sym)`: off returns `sym` without any other work; observe returns `sym` and logs the translation; enforce returns the venue code and logs it. One INFO line per (mode, code) per process, `[eodhd v4.20.0] FUND-SUFFIX-MAP <mode>: fundamentals for <code> requested as|would be requested as <venue code>`; no URL, no key.
- `fetch_fundamentals`: one line after the empty-symbol check, `sym = _fund_suffix_map(sym)`. Under enforce, the request path, the cache key (`f:eps_basis_v1:<margin version>:[at1:]<venue code>`), the identity guard and `symbol_normalized` use the venue code. The patch's `symbol` stays the row's own.
- WHY block above the v4.19.0 block; all prior blocks verbatim. Additions: 3 functions and 1 module set (`_FUND_SUFFIX_MAP_SEEN`); 0 removed.
- Not mapped: `.IS` (still rejected as `X.IS`), `.T`, `.SI`, `.MI`, `.BH`, `.SN`, `.JO` and every other venue (no established code). Quotes, history and every other endpoint are unchanged in every mode.

## S3 - Tests
`tests/test_eodhd_fund_suffix_map.py`: **PASS 8/8** (script), 8 passed (pytest), PASS 8/8 with `PYTHONHASHSEED=7`. On a tree with no git (as on CI): 7 passed, 1 skipped (G1).

| Case | What it pins |
|---|---|
| W1 | the code map on 21 shapes; only `X.KL` and `X.LS.US` move |
| W2 | enforce: `fundamentals/1295.KLSE`, `fundamentals/SON.LS`; cache keys; `symbol` / `symbol_normalized`; identity accepted; exactly two INFO lines, none repeated |
| W3 | observe (` Observe `): the v4.19.0 request, one INFO line per code per process, none for an untranslated code |
| W4 | the REAL engine fallback (`_apply_eodhd_fundamentals_fallback` -> `get_client` -> `fetch_fundamentals`) with a supplier that answers only the codes EODHD knows: off fills nothing; enforce fills P/E and market cap, tags `eodhd_fundamentals_fallback_applied`, keeps the row's symbol and name, no `identity_patch_refused` |
| N1 | off, 11 spellings (`None`, `''`, `off`, `OFF`, `0`, `1`, `on`, `true`, `yes`, `observe-ish`, `enforced`), every `TFB_*` / `EODHD*` variable unset: request paths, cache keys, patches (time fields dropped) and log records over all 2,033 fixture Global_Markets symbols equal the v4.19.0 copy (`git show 0c3d3e21`); without git, the digests `55ed1fd874aeaace` (2,033 x unset) and `df4e10c1742e1f7a` (613 x 11 spellings) of that copy's output; each spelling reads `off` |
| N2 | enforce changes no quote or history request and no other venue's fundamentals request (`.IS`, `.T`, `.MI`, `.SI`, `.L`, `.SR`, US, FX, crypto, index, `.VN`, `.AX`, an already-KLSE code) |
| N3 | version floor 4.20.0, manifest pin = module, the three added functions, zero removed (AST), ASCII WHY block and helpers |
| G1 | W2, W3 and W4 fail on the v4.19.0 copy |

- **Golden negative** (provider swapped for `git show 0c3d3e21:core/providers/eodhd_provider.py`): 5 failed (W1, W2, W3, W4, N3), N1 and N2 pass (identity and scope), G1 skipped.
- **Mutations: 13/13 caught.** IS mapped too; KL at any arity; enforce does not translate; observe translates; off translates; no log dedupe; WARNING level; no trim/case; `on`/`1` arm (caught after the spellings digest was added: the first run let it survive because, without git, only the unset spelling was digested); hook removed; mapping in the shared normalizer (every endpoint); observe logs nothing; version not bumped.
- **Blocking lane:** `tests/test_eodhd_analyst_target.py` (heavy job, "Work-brief publication, policy and workbook regressions") gains `test_g20_fund_suffix_map_off_keeps_the_v4190_request` (6 spellings) and `test_g20_fund_suffix_map_enforce_requests_the_eodhd_codes`; its e9 provider pin becomes a 4.19.0 floor and the manifest check reads the module's version. 32 passed; on the v4.19.0 provider the enforce guard fails.

## S4 - Owner steps (nothing is armed here)
1. Merge, then Render **Manual Deploy** outside a sync; read back the served `eodhd_provider` version.
2. Optional: set `TFB_EODHD_FUND_SUFFIX_MAP=observe` first. The Render log then names each `.KL` / `.LS` code once per process ("would be requested as").
3. Set `TFB_EODHD_FUND_SUFFIX_MAP=enforce`. On the next sync, the `.KL` / `.LS` Global_Markets rows should carry P/E, EPS and market cap from EODHD with `eodhd_fundamentals_fallback_applied` in Warnings, and the log names each translated code once per process.

**Rollback:** unset the variable (or set it to `off`). The translated codes use their own cache keys, so nothing cached under them is served after a rollback.

## S5 - Known limits
- The calendar run proves EODHD knows the KLSE and LS codes, not that it carries fundamentals for every name. A name without data answers with an empty or error patch, exactly as today.
- Prices and history for these rows still use the v4.19.0 codes; Yahoo carries them. Mapping other endpoints needs its own read-back first.
- `.IS`, `.T`, `.SI`, `.MI` and the other rejected venues are unchanged until their EODHD codes are established.
- With `TFB_EODHD_ANALYST_TARGET=1` as well, these rows can also receive an EODHD analyst target through the v4.19.0 path; the plausibility gate is unchanged.

## S6 - Rollback
Unset `TFB_EODHD_FUND_SUFFIX_MAP`, or revert this commit.
