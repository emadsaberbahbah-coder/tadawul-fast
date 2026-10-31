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
- **EODHD's earnings calendar rejects both shapes.** calendar_sync run 40498374271 (27 Oct): "EODHD rejected 33 code(s)", among them the `.KL` and `X.LS.US` codes (calendar_provider v1.3.2 WHY). No log shows the fundamentals endpoint's answer for these codes; that it rejects them too is inferred from EODHD using the same codes across endpoints.
- **EODHD accepts the venue codes.** calendar_sync run 40678905106, job 122784262698 (28 Oct 11:19Z), with `TFB_CALENDAR_SUFFIX_MAP` armed: "EODHD rejected 19 code(s): AEFES.IS, KCHOL.IS, TUPRS.IS, IG.MI, TIT.MI, T82U.SI, 1605.T, ... ; sent as EODHD exchange codes: IS.US->IS x3, KL->KLSE x2, L->LSE x13, LS.US->LS x3". No KLSE or LS code is rejected; the IS codes still are.
- **Why it matters now.** The Yahoo fundamentals breaker has re-tripped in every sync since 2026-10-21 (v4.19.0 WHY: 100+ OPEN events on 23 Oct, dozens per sync since; "EODHD fallback carries fundamentals"), so EODHD is often these rows' only fundamentals source.
- **Scale.** In `tests/fixtures/wp3ab_tuple_rows_2026-10-10.json`, 13 of the 2,033 Global_Markets rows are `.KL` or `.LS`: 10 `.KL` (1295, 3816, 4197, 4715, 4863, 5183, 6742, 6888, 7084, 7113) and 3 `.LS` (COR, NOS, SON). That fixture predates the breaker, so those rows still had Yahoo fundamentals on 10 Oct. The calendar runs name further codes (e.g. 1015.KL, 5285.KL, EDP.LS).
- **Routing.** `.KL` and `.LS` are not Yahoo-only (`provider_capabilities.yahoo_primary_reason` lists only `=F`, `^TASI.SR`, `.MI`, `.NZ`), so the engine's EODHD fundamentals fallback does run for them.

## S2 - What changed
- `_fund_suffix_map_mode()`: `observe` / `enforce` (trimmed, any case), else `off`.
- `_eodhd_fund_venue_code(sym)` (PURE): `X.KL -> X.KLSE`, `X.LS.US -> X.LS`; everything else unchanged.
- `_fund_suffix_map(sym)`: off returns `sym` without any other work; observe returns `sym` and logs the translation; enforce returns the venue code and logs it. One INFO line per (mode, code) per process, `[eodhd v4.20.0] FUND-SUFFIX-MAP <mode>: fundamentals for <code> requested as|would be requested as <venue code>`; no URL, no key.
- `fetch_fundamentals`: one line after the empty-symbol check, `sym = _fund_suffix_map(sym)`. Under enforce, the request path, the cache key (`f:eps_basis_v1:<margin version>:[at1:]<venue code>`), the identity guard and `symbol_normalized` use the venue code. The patch's `symbol` stays the row's own.
- WHY block above the v4.19.0 block; all prior blocks verbatim. Additions: 3 functions and 1 module set (`_FUND_SUFFIX_MAP_SEEN`); 0 removed.
- Not mapped: `.IS` (still rejected as `X.IS`), `.T`, `.SI`, `.MI`, `.BH`, `.SN`, `.JO` and every other venue (no established code). Quotes, history and every other endpoint are unchanged in every mode.

## S3 - Tests
`tests/test_eodhd_fund_suffix_map.py`: **PASS 9/9** (script), 9 passed (pytest), PASS 9/9 with `PYTHONHASHSEED=11`; about 26 s. On a tree with no git (as on CI's shallow checkout): `PASS 8/8 (1 skipped)`, and the run prints that N1's live comparison and N3's AST check did not run (a skip is no longer counted as a pass).

| Case | What it pins |
|---|---|
| W1 | the code map on 21 shapes; only `X.KL` and `X.LS.US` move |
| W2 | enforce: `fundamentals/1295.KLSE`, `fundamentals/SON.LS`; cache keys; `symbol` / `symbol_normalized`; identity accepted; exactly two INFO lines, none repeated |
| W3 | observe (` Observe `): the v4.19.0 request, one INFO line per code per process, none for an untranslated code |
| W4 | the REAL engine fallback (`_apply_eodhd_fundamentals_fallback` -> `get_client` -> `fetch_fundamentals`) with a supplier stub that rejects the old codes (the calendar evidence; the premise, not a fundamentals measurement): off fills nothing; enforce fills P/E and market cap, tags `eodhd_fundamentals_fallback_applied`, keeps the row's symbol and name |
| W5 | the engine fund cache as in production (`TFB_EODHD_FUND_CACHE=enforce`): the empty answer under the old code leaves a negative mark on the ROW symbol; while it lives, enforce never reaches the provider (no request, no log line, tag `fund_cache:neg...`); once it is older than 168 h, enforce asks `fundamentals/1295.KLSE` and the row fills |
| N1 | off, 11 spellings (`None`, `''`, `off`, `OFF`, `0`, `1`, `on`, `true`, `yes`, `observe-ish`, `enforced`) x all 2,033 fixture Global_Markets symbols, every `TFB_*` / `EODHD*` / `NORMALIZE_*` variable unset: request paths, cache keys, patches (time fields dropped) and log records equal the v4.19.0 copy (`git show 0c3d3e21`); without git, the digest `dd8d68d6d0971802` of that copy's output; each spelling reads `off` |
| N2 | enforce changes no quote or history request and no other venue's fundamentals request (`.IS`, `.T`, `.MI`, `.SI`, `.L`, `.SR`, US, FX, crypto, index, `.VN`, `.AX`, an already-KLSE code) |
| N3 | version floor 4.20.0, manifest pin = module, the three added functions, zero removed (AST), ASCII WHY block and helpers |
| G1 | W2, W3, W4 and W5 fail on the v4.19.0 copy |

- **Golden negative** (provider swapped for `git show 0c3d3e21:core/providers/eodhd_provider.py`): 6 failed (W1-W5, N3); N1 and N2 pass (identity and scope); G1 skipped.
- **Mutations: 14/14 caught.** IS mapped too; KL at any arity; enforce does not translate; observe translates; off translates; no log dedupe; WARNING level; no trim/case; `on`/`1` arm; `on` reads observe; hook removed; mapping in the shared normalizer (every endpoint); observe logs nothing; version not bumped. (A first run let `on`/`1` arm survive: without git only the unset spelling was digested. N1 now digests every spelling.)
- **Blocking lane:** `tests/test_eodhd_analyst_target.py` (heavy job, "Work-brief publication, policy and workbook regressions") gains `test_g20_fund_suffix_map_off_keeps_the_v4190_request` (6 spellings: the v4.19.0 requests, no FUND-SUFFIX-MAP log record, v4.19.0 cache keys, mode reads off) and `test_g20_fund_suffix_map_enforce_requests_the_eodhd_codes`; its e9 provider pin becomes a 4.19.0 floor and the manifest check reads the module's version. 32 passed. It catches the reviewer's `on reads observe` mutant. On the v4.19.0 provider 8 fail: the 6 off guards and the enforce guard call the new helpers, and e9 checks this branch's pin.
- **Lean lane and heavy step:** LEAN FAILS 0; the heavy job's work-brief step 241 passed.

## S4 - Owner steps (nothing is armed here)
1. Merge, then Render **Manual Deploy** outside a sync; read back the served `eodhd_provider` version.
2. Set `TFB_EODHD_FUND_SUFFIX_MAP=enforce` (or `observe` first, to see the log lines without changing a request).
3. **Expect a delay of up to 7 days.** The engine's fund cache (`TFB_EODHD_FUND_CACHE=enforce`, live) keeps a negative mark per ROW symbol for `TFB_EODHD_FUND_NEG_TTL_H` (default 168 h, memory plus Redis key `tfb:fund_neg:v1:<SYM>`). It checks the mark before calling this provider, and today's empty answers have marked these rows. Each row is asked again only when its own mark expires (W5). Neither observe's log line nor enforce's request appears for a row until then.
   - To apply on the next sync instead: delete the Redis keys `tfb:fund_neg:v1:<SYM>` for the `.KL` / `.LS` rows, then Manual Deploy (which clears the per-worker memory mirror). A one-sync `TFB_EODHD_FUND_NEG_TTL_H=1` also works but re-asks EODHD for EVERY negatively-marked symbol, so it costs extra EODHD calls; it is an owner choice, not proposed here.
4. Read-back, per row once its mark has gone: the Render log names the translated code once per process (`[eodhd v4.20.0] FUND-SUFFIX-MAP enforce: fundamentals for 1295.KL requested as 1295.KLSE`), and the row carries P/E, EPS and market cap from EODHD with `eodhd_fundamentals_fallback_applied` in Warnings. A name EODHD has no data for gets a new negative mark, as today.

**Rollback:** unset the variable (or set it to `off`). New provider requests return to the v4.19.0 codes at once. Values already filled under enforce stay in the engine's caches, which are keyed by the ROW symbol: up to 24 h as fund-cache hits and up to 72 h as AZ-2 last-good restores, with no EODHD request.

## S5 - Known limits
- The calendar run proves EODHD's calendar knows the KLSE and LS codes, not that the fundamentals endpoint carries data for every name. A name without data answers with an empty or error patch, exactly as today.
- Rollout delay of up to 168 h per row through the engine's negative marks (S4 step 3). Re-keying those marks by EODHD code would remove it, but needs a data_engine_v2 change; three open PRs are renumbering that file, so it is a follow-up.
- `scripts/diag_eodhd_fundamentals.py` (read-only diagnostic) still checks the plan-restricted cache of the untranslated endpoint and does not print the new variable; with enforce armed it can name the wrong cause for a `.KL` / `.LS` symbol.
- Prices and history for these rows still use the v4.19.0 codes; Yahoo carries them. Mapping other endpoints needs its own read-back first.
- `.IS`, `.T`, `.SI`, `.MI` and the other rejected venues are unchanged until their EODHD codes are established.
- With `TFB_EODHD_ANALYST_TARGET=1` as well, these rows can also receive an EODHD analyst target through the v4.19.0 path; the plausibility gate is unchanged.

## S6 - Rollback
Unset `TFB_EODHD_FUND_SUFFIX_MAP`, or revert this commit (cached fills age out as in S4).

## S7 - Adversarial review (before the PR)
Five lenses (semantics, off identity and harness, evidence, production risk) with an independent refuter for each non-nit finding. Confirmed and fixed in the second commit:
- **Major:** the rollout delay through the engine's negative marks (S4 step 3, W5); the first sheet promised a fill "on the next sync".
- **Minor:** the rollback note ignored the engine's row-keyed caches; N1's coverage was overstated (it now runs all 2,033 x 11); the script runner counted a skip as a pass and dropped two base comparisons silently; the blocking off guard checked request paths only; `_hermetic` missed `NORMALIZE_*`.
- **Nits:** "breaker open" -> "re-trips in every sync"; the fixture rows still had Yahoo fundamentals on 10 Oct; the rejection evidence is the calendar endpoint's; W4's "no identity refusal" check cannot fail (dropped from the sheet's claims); the diagnostic script (S5).
Checked and holding: the translated code is used consistently (cache and single-flight key, plan-restricted cache, identity guard with realistic KLSE / LS declarations); no other production path reaches EODHD fundamentals; AW-2 quarantines the patch's exchange / currency / country, so nothing overwrites row fields; the off identity re-proven by an independent probe.
