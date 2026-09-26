# TFB Commit Sheet — core/data_engine_v2.py v5.145.0 [CRYPTO-PAIR SHAPE] (P-151)

**Date:** 2026-09-20 (Sunday, Riyadh) · **Item:** P-151 — 49 Commodities_FX crypto pairs (`<ROOT>-USD`) exported with Asset Class "Equity", FLOW-USD with Exchange "NASDAQ/NYSE" (red-team P2-11, re-executed 49/49 on the 09-20 export) · **Build #4 of 2026-09-20** on Emad's "done, let's go next" · **Engineer:** Claude · **Protocol:** One-Pass S0→S6.

## S0 / S1 — Freeze and pin

- HEAD verified before the build: `67814a6fb5baa5a181b884088c5a50a2456d3796`. `core/data_engine_v2.py` (v5.144.0, sha `b6784d21…`) and `tests/test_de_rel_path_tag_p115b.py` (sha `a92118ca…`) byte-identical at branch AND commit. **`docs/evidence/TFB_Commit_Sheet_data_engine_v2_v5.144.0_2026-09-20.md` is NOT at HEAD (404)** — the v5.144.0 commit leg stays half-open until the sheet lands (same class as the 09-19 lesson: commit sheets travel with the code).
- Base = v5.144.0 at that commit, **17,826 lines, sha256 `b6784d21a159c658…`** (zero drift).

## S2 — Design (gate `TFB_SYM_CRYPTO_PAIR_CLASS` off|observe|enforce, read per call)

Root: both symbol-shape inferrers treat a dot-less symbol as a US equity (`_infer_asset_class_from_symbol` → "Equity", `_infer_exchange_from_symbol` → "NASDAQ/NYSE"); Yahoo's quoteType overrides only when its meta is present on that fetch. `_yf_asset_class_ok` already excludes `-USD` from the equity contract, so the decision surface never consumed them (0/49 in the 09-20 audit strip) — the defect is identity/display and every consumer keyed on Asset Class or venue.

| Mode | Behaviour |
|---|---|
| `off` (default) | Byte-identical v5.144.0 (measured: 453/453 real CFX rows identical base vs delivered; inferrers unchanged). |
| `observe` | Inferrers unchanged; a shaped row (`<ROOT>-<QUOTE>`, QUOTE ∈ USD/USDT/USDC/EUR/GBP/JPY/BTC/ETH, no suffix, no `=`, no `^`) whose class is missing or equity-like gets ONE countable `crypto_pair_shape:observe` tag; values untouched. |
| `enforce` | The three inferrers answer Crypto / Crypto / `<QUOTE>` for the shape, and `_crypto_pair_shape_apply` repairs a shaped row whose class is missing or equity-like: asset_class "Crypto", exchange "NASDAQ/NYSE"/blank → "Crypto", blank currency → `<QUOTE>`, tag `crypto_pair_shape:enforce`. A provider-declared non-equity class (CRYPTOCURRENCY / FX / Commodity) is never rewritten. |

Applied at the Commodities_FX / `=F` / `=X` identity block of `_apply_symbol_context_defaults` (where the 49 live). Share-class dashes never match by construction (BRK-B, AKO-B.US, GRT-UN.TO). Mode disclosed in `/health engine_gates.crypto_pair_class`.

## S3 — Build (7 anchored edits, every replacement asserted `count == 1`)

E1 header WHY · E2 `__version__` 5.145.0 · E3 helpers (`_crypto_pair_class_mode`, `_crypto_pair_shape`, `_crypto_pair_class_like_equity`, `_crypto_pair_shape_apply`, regex + tags) and the class-inferrer branch · E4 exchange-inferrer branch · E5 currency-inferrer branch · E6 `_crypto_pair_shape_apply(out, sym)` after the CFX identity setdefaults · E7 `crypto_pair_class` in `surface_gate_states()`.

Delivered: **17,928 lines**, sha256 `e2de0cb46770a3acdc38cf1ce4e9587531ccf4bb8d484c7da0a1e4c6531d1557`. `py_compile` PASS. AST defs 489 → 493 (**+4, 0 removed**). Smart quotes 0. Net-new non-ASCII outside comments 0.

## S4 — Internal audits ×3

**Repo harness** `tests/test_de_crypto_pair_shape_p151.py` (191 lines, sha256 `636acc96d7ffa888179e7d8f362130d99f2a8778a84d6a85b5606345cb1fe3c7`), REAL module on `_apply_symbol_context_defaults` + the inferrers: T1 helpers (shape edges: BRK-B, AKO-B.US, GRT-UN.TO, =X, =F, ^, 1-char root, >12-char root, `BTC-USD.X`, `BTC_USD` all refused) · T2 off identity · T3 observe (values untouched, one tag on shaped equity-like rows, nothing on declared/non-shaped rows) · T4 enforce (FLOW/SHIB/bare repaired; DOT-USD declared CRYPTOCURRENCY untouched; equities/FX/futures identical) · T5 page scope (equity-page rows untouched) + idempotence in every mode · T6 substring safety · T7 wiring + `/health`. **ALL PASS ×3.**

**Dual-tree on the REAL 09-20 export** (`replay_p151.py`): all 453 Commodities_FX rows with their exported labels + all 81 dash-symbol rows of GM/ML/MF: off identical 453/453 · observe values identical 453/453, **tagged exactly 49** · enforce **repaired exactly 49** (FLOW-USD Equity/NASDAQ/NYSE → Crypto/Crypto/USD), 404 untouched-identical · equity-page dash symbols changed under enforce: **0/81** · digest `f06c17860d321e44` **identical ×3**.

Prior batteries on this file: `test_de_fund_sentry_repair_p146.py` 8/8 PASS; `test_de_rel_path_tag_p115b.py` ALL PASS ×3 after loosening its exact `5.144.0` pin to a `>= 5.144.0` floor (delivered again, sha now `5c6660f0…`).

## S5 — Delivery (full files, convention paths)

- `core/data_engine_v2.py` (v5.145.0 — contains v5.144.0 verbatim)
- `tests/test_de_crypto_pair_shape_p151.py` (new)
- `tests/test_de_rel_path_tag_p115b.py` (version floor only)
- `docs/evidence/TFB_Commit_Sheet_data_engine_v2_v5.145.0_2026-09-20.md` (this sheet) — **plus the still-missing v5.144.0 sheet**

## ENV (Emad applies; Render lane; ONE per evidence run)

| Var | Default | Purpose |
|---|---|---|
| `TFB_SYM_CRYPTO_PAIR_CLASS` | off | `observe` first (read-back: 49 tags on CFX, 0 elsewhere), `enforce` after |

Deploy is behaviour-identical (gate unset). Note the sync's preserved-row semantics: rows not re-normalised in a run keep their prior labels — the enforce read-back counts only the rows the run rebuilt.

## S6 — Read-back

Observe: `crypto_pair_shape:observe` on exactly the 49 CFX rows, none on GM/ML/MF. Enforce: those rows read Asset Class "Crypto", Exchange "Crypto", the identity firewall's out-of-universe strips unchanged, 0 changes on any equity page.

## Rollback

Unset `TFB_SYM_CRYPTO_PAIR_CLASS` (no deploy) or `git revert`.
