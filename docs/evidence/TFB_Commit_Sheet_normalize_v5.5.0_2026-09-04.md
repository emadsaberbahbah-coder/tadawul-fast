# COMMIT SHEET — core/symbols/normalize.py v5.5.0 (bare root is an equity — identity-collision fix)

Date: 2026-09-04 (Friday, evening session) · Builder: Claude · Executor: Emad (GitHub web UI)

## 1. Target
| Item | Value |
|---|---|
| Repo | `emadsaberbahbah-coder/tadawul-fast`, branch `main` |
| **Exact repo path** | `core/symbols/normalize.py` (full-file replace) |
| Base pinned | HEAD `848f699eeb2453b781b681964f44cd439100dca0`, blob `a9e01e2bd3895492e6b02260e4e0239baff9cd89`, 1,566 lines, 47,764 bytes, `__version__ = "5.4.0"` |
| Delivered | blob `650e30456856b722897769ee6d12cce4265f43a4`, 1,607 lines, 50,453 bytes, `__version__ = "5.5.0"` |
| Net diff | +41 lines; 2 lines replaced (docstring title, version string); 0 removed |
| Commit sheet path | `docs/evidence/TFB_Commit_Sheet_normalize_v5.5.0_2026-09-04.md` (this file) |
| Commit message | `normalize v5.5.0 — bare root is an equity (TFB_SYM_BARE_ROOT_EQUITY, default OFF); fixes KE/NG/SI/PL/HG/LINK identity collisions` |

## 2. Mechanism (re-executed on the real modules — W7-b)
1. Engine `_yahoo_symbol_for("KE.US")` → `to_yahoo_symbol("KE.US")` → `KE` (correct).
2. Chart provider `_yc_yahoo_symbol("KE")` re-normalizes the bare ticker: `to_yahoo_symbol("KE")` → `is_commodity_future("KE")` is True because the bare root is in `COMMODITY_CODES` → **`KE=F`** (wheat). Same for NG/SI/PL/HG/CC/KC/CT/SB/OJ/LB/RB/HO and, via `is_crypto`, LINK/SOL/UNI/DOT/FIL/VET/ADA/ATOM/TRX/BCH → `XXX-USD`.
3. Yahoo's chart meta for the contract is grafted as the row name (`name_from_chart_meta`), sector blank.
Fires only on rows that needed name enrichment; today six rows showed it (KE, NG, SI, PL, HG, LINK). The workbook never spells a commodity or coin as a bare root (Commodities_FX 453/453 rows use `=F`/`=X`; crypto is `XXX-USD`), so the heuristic can only misfire on equities.

## 3. What changed (additions only)
| # | Anchor (count == 1) | Edit |
|---|---|---|
| E1 | docstring title + `v5.4.0 (over v5.3.2)` block | v5.5.0 changelog entry (evidence, mechanism, fix, cache note, vNEXT belt) |
| E2 | `__version__ = "5.4.0"` | → `5.5.0` |
| E3 | `CUSTOM_COMMODITY_MAP = _env_dict(…)` | `_BARE_ROOT_EQUITY = _env_bool("TFB_SYM_BARE_ROOT_EQUITY", False)` (import-time; the classifiers are `lru_cache`d, Render restarts on ENV change — same semantics as `CUSTOM_*_MAP`) |
| E4 | `is_commodity_future` bare-root lookup | when ON: explicit notation only (`XX=F`, `.COMM/.COM/.FUT`) |
| E5 | `is_crypto` bare-root lookup | when ON: explicit notation only (dash pair with known base, `.CRYPTO/.CC/.C`) |

## 4. Proofs
| Gate | Result |
|---|---|
| `py_compile` | OK |
| AST zero-removal | functions 54 → 54 (none removed, none added); 1 module constant added |
| Real-module harness (`normalize` + `yahoo_chart_provider` 8.14.0 + `data_engine_v2` 5.136.0) | **14 checks × OFF/ON × 3 runs = 84/84 PASS**: OFF reproduces the collision at the provider seam (`KE→KE=F`, `LINK→LINK-USD`) and is byte-identical to v5.4.0 across a 29-symbol battery; ON gives `KE→KE`, `LINK→LINK` at the provider while `GC=F`, `GC.COMM`, `BTC-USD`, `LINK-USD`, `EURUSD=X`, `SAR=X`, `2222.SR`, `VOD.L`, `RELIANCE.NSE→.NS`, `^GSPC` are unchanged; `EUR-USD` stays FX; currency inference unchanged |
| Lean CI (`test_data_engine_v2`, `test_learning_guards`, `test_symbol_dedup`, `test_critical_symbol_identity`, `test_identity_guard`) | **173 / 173** on v5.5.0 — identical on base |

Pre-existing quirk noted, not touched: `to_yahoo_symbol("LINK.CRYPTO")` returns `LINK.CRYPTO-USD` on both v5.4.0 and v5.5.0 (Register, cosmetic — the workbook has no `.CRYPTO` symbols).

## 5. ENV (Render Web `tadawul-fast-bridge`)
| Variable | Value | Arming rule |
|---|---|---|
| `TFB_SYM_BARE_ROOT_EQUITY` | `1` | own evidence run; read-back = next GM sync: the six rows' names resolve to their issuers (Kimball Electronics, NovaGold, Planet Labs, …), sectors fill, and `tfb_acceptance` A3-Global_Markets goes FAIL 6 → PASS. Kill `0`. Also arm `TFB_SYNC_IDENTITY_REFETCH` / `TFB_SYNC_FORCE_REFETCH_SYMBOLS=KE.US,NG.US,SI.US,PL.US,HG.US,LINK.US` on the same run if the names are kept-last-good rather than re-fetched. |

## 6. Post-commit verification (Emad → Claude)
- [ ] Exactly two changed files: `core/symbols/normalize.py` + this sheet; no harness in `scripts/`.
- [ ] Blob on main == `650e30456856b722897769ee6d12cce4265f43a4`.
- [ ] Render deploy OK before arming.

## 7. Register
- vNEXT belt in `data_engine_v2`: refuse a chart-meta name when `instrumentType` ∉ {EQUITY, ETF} for an equity-page row.
- Correction of an earlier claim: `yahoo_chart_provider` v8.14.0 IS on main (the file carries two `PROVIDER_VERSION` strings; the first is a legacy 8.11.0 constant). Whether Render runs 8.14.0 is still unverified.
