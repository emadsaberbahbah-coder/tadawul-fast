# COMMIT SHEET — core/data_engine_v2.py v5.137.0 (chart-meta class guard — belt for the bare-root collision)

Date: 2026-09-04 (Friday, evening session) · Builder: Claude · Executor: Emad (GitHub web UI)

## 1. Target
| Item | Value |
|---|---|
| Repo | `emadsaberbahbah-coder/tadawul-fast`, branch `main` |
| **Exact repo path** | `core/data_engine_v2.py` (full-file replace) |
| Base pinned | HEAD `36ad452f0b312877121e7afa96cbd8b6ca1131c7`, blob `83d676306e1521b165f5b1e7eabd3c065e371c3c` (= v5.136.0 as committed), 16,918 lines |
| Delivered | blob `61fe269e38f1383c9c9301cb4b598ff1e085f384`, 16,992 lines, 874,770 bytes, `__version__ = "5.137.0"` |
| Net diff | +74 lines; 1 line replaced (version string); 0 removed |
| Commit sheet path | `docs/evidence/TFB_Commit_Sheet_data_engine_v2_v5.137.0_2026-09-04.md` (this file) |
| Commit message | `data_engine_v2 v5.137.0 — chart-meta class guard (TFB_ENGINE_CHART_META_CLASS_GUARD, default OFF)` |

## 2. Why a belt (mechanism re-executed on the real module)
The BA-1 identity rescue (`_apply_identity_rescue`) takes `fetch_chart_meta(sym)`; the provider's `_ensure_shape` stamps the **requested** symbol onto the payload, so the YC-4 / BB-1 identity checks (symbol token sets) pass even when Yahoo answered for `KE=F` — the contract's identity is visible only in `meta.instrumentType = FUTURE` / `CRYPTOCURRENCY`, which nothing consulted. Root cause is fixed in `normalize` v5.5.0 (bare root → equity); this guard makes the engine refuse a class contradiction regardless of how the symbol got there.

## 3. What changed (additions only)
| # | Anchor (count == 1) | Edit |
|---|---|---|
| E1 | `__version__ = "5.136.0"` | v5.137.0 changelog block + version bump |
| E2 | `def _identity_rescue_enabled()` | `_CHART_META_NON_EQUITY_TYPES`, `_chart_meta_class_guard_enabled`, `_chart_meta_declared_type`, `_chart_meta_class_refused` (uses the existing v5.132 equity-only contract `_yf_asset_class_ok`) |
| E3 | BA-1 block, after `echo = …`, before the BB-1 echo gate | when the guard is ON and the row is Equity and the meta declares FUTURE / CRYPTOCURRENCY / CURRENCY / INDEX / OPTION / COMMODITY → tag `identity_class_refused:<TYPE>`, drop the meta (name / exchange / currency left blank for BA-2 static map). EQUITY / ETF / MUTUALFUND / absent type → v5.136.0 behaviour |

NOT CHANGED: BB-1 echo gate, YC-4, BA-2 static map, both enrichment passes, target-LKG (v5.136.0), providers.

## 4. Proofs
| Gate | Result |
|---|---|
| `py_compile` | OK |
| AST zero-removal | functions 454 → 457 (removed **none**); 1 constant added |
| Real-module harness (`harness_eng_v5_137_0`: the REAL `DataEngineV5._apply_identity_rescue` runs; only the remote Yahoo meta is a fixture injected through `_pick_provider_callable`) | **17 checks × OFF/ON × 3 = 102/102 PASS** — OFF reproduces the graft byte-identically (KE.US ← "KC HRW Wheat Futures,Dec-2026", LINK.US ← "Chainlink USD"); ON refuses with `identity_class_refused:FUTURE` / `:CRYPTOCURRENCY`, fills nothing; EQUITY / ETF / absent-type metas accepted both ways; a genuine `GC=F` row still takes its own FUTURE meta (guard is equity-only); banner intact |
| Lean CI (7 engine test files) | **154 / 154**, identical on base |
| R-6b regression (v5.136.0 harness re-run on v5.137.0 against a real redis-server) | H1/H2A/H2B/H3 behavioural checks all PASS (the only differing line is the harness's own version assert 5.136.0 vs 5.137.0) |

## 5. ENV (Render Web `tadawul-fast-bridge`)
| Variable | Value | Arming rule |
|---|---|---|
| `TFB_ENGINE_CHART_META_CLASS_GUARD` | `1` | own evidence run; read-back = `identity_class_refused:*` tags on the six rows while `TFB_SYM_BARE_ROOT_EQUITY` is still OFF (with the root fix ON the provider never fetches the contract and the guard stays silent — that silence is the expected end state). Kill `0` |

## 6. Post-commit verification (Emad → Claude)
- [ ] Exactly two changed files: `core/data_engine_v2.py` + this sheet; no harness in `scripts/`.
- [ ] Blob on main == `61fe269e38f1383c9c9301cb4b598ff1e085f384`.
- [ ] Render deploy OK before any arming.
