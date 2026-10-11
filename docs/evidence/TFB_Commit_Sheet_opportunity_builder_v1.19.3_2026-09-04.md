# COMMIT SHEET — core/analysis/opportunity_builder.py v1.19.3 (FX peg guard + held-target NO_NEW_MONEY)

Date: 2026-09-04 (Friday, evening session) · Builder: Claude · Executor: Emad (GitHub web UI)

## 1. Target
| Item | Value |
|---|---|
| Repo | `emadsaberbahbah-coder/tadawul-fast`, branch `main` |
| **Exact repo path** | `core/analysis/opportunity_builder.py` (full-file replace) |
| Base pinned | HEAD `246d055ee8e56b08e0b9ba35e61655b09b468d2e`, blob `b063eb7b33a6cbdd72bc3842234ceb40a3172b08`, 5,205 lines, 271,390 bytes, `OPPORTUNITY_BUILDER_VERSION = "1.19.2"` |
| Delivered | blob `824c473047043252ccd6945b0d3089b0c9af299a`, 5,304 lines, 276,379 bytes, `OPPORTUNITY_BUILDER_VERSION = "1.19.3"` |
| Net diff | +99 lines; 3 lines replaced (version string; the two `_resolve_fx` provided-rate returns now pass through `_fx_peg_guard`, identical when OFF); 0 lines removed |
| Commit sheet path | `docs/evidence/TFB_Commit_Sheet_opportunity_builder_v1.19.3_2026-09-04.md` (this file) |
| Commit message | `opportunity_builder v1.19.3 — FX peg guard + held-target no-new-money (env-gated, default OFF)` |
| Versioning note | built from main 1.19.2; any earlier un-merged "v1.19.3 bundle" must be rebased on this file and renumbered |

## 2. What changed (additions only)
| # | Anchor (count == 1 verified) | Edit |
|---|---|---|
| E1 | `# v1.19.2 (2026-09-02) - ELIGIBILITY (VENUE)…` / `OPPORTUNITY_BUILDER_VERSION = "1.19.2"` | v1.19.3 changelog block (EVIDENCE/CHANGE/NOT CHANGED) + version bump |
| E2 | `def _venue_floor(symbol):` | `_env_fx_sanity`, `_env_held_target_no_new_money`, `_FX_PEG_BAND`, `_FX_PEG_STATIC`, `_FX_PEG_REJECTS`, `_HELD_TARGET_TAG`, `_fx_peg_guard`, `_held_target_age` |
| E3 | the two `return … "provided"` lines in `_resolve_fx` | routed through `_fx_peg_guard(code, rate, source)` |
| E4 | `"forecast_source": …` in `normalize_candidate` | `"warnings": _to_text(_field(view, "warnings")) or ""` |
| E5 | loop head of `_select_and_size` | held-target deferral before any capital use |

Behaviour when armed:
- `TFB_OPP_FX_SANITY=1`: provided `USD` outside [3.74, 3.77] → 3.75, source `static(peg-guard)`; provided `SAR` ≠ 1.0 → 1.0; all other currencies and the subunit path untouched; rejections counted in `_FX_PEG_REJECTS` (count + last value per code).
- `TFB_OPP_HELD_TARGET_NO_NEW_MONEY=1`: candidate whose warnings contain `analyst_lkg:` → verdict/gates/audit/near-miss unchanged; never funded; `deferral = "Held target (analyst_lkg:<age>h) — no new money until the provider leg returns"`.

NOT CHANGED: gate list, GATE_ORDER, `derive_verdict`, sizing math, venue floors/lots, the v1.19.2 venue gate, static FX table.

## 3. Proofs
| Gate | Result |
|---|---|
| `py_compile` | OK |
| AST zero-removal | functions 140 → 144 (removed **none**, added 4); classes unchanged; module assigns 59 → 63 (removed none, added 4) |
| Real-module harness (`harness_ob_v1_19_3.py`) | **35 checks × 3 runs = 105/105 PASS** — FX OFF pass-through (provided/subunit/static), FX ON matrix (USD 3.8088 → 3.75; SAR 1.02 → 1.0; EUR/GBp untouched; 3.7549 / 3.769 / 3.74 in band; 3.7399 rejected; counters), end-to-end `build_opportunity_payload` audit row `price_sar = 375.00` with fx `{"USD": 3.8088}`; held-target OFF → both rows selected (byte-identical), ON → held row INVEST but unfunded with the deferral text, fresh row selected |
| Lean CI (`test_opportunity_builder.py`, `test_top10_selector.py`, `test_portfolio_actions.py`, `test_investment_policy.py`) | **71 passed / 0 failed** on v1.19.3 — identical on base v1.19.2 |

## 4. ENV (Render Web `tadawul-fast-bridge`) — applied by Emad
| Variable | Value | Arming rule |
|---|---|---|
| `TFB_OPP_FX_SANITY` | `1` | own evidence run; read-back = Price SAR / Price == 3.75 on every USD audit row; kill `0`. Complements (does not replace) fixing the `_Lists_Config` USD cell |
| `TFB_OPP_HELD_TARGET_NO_NEW_MONEY` | `1` | arm together with the engine master gate `TFB_ENGINE_TARGET_KLG=1` (it is inert until `analyst_lkg:` tags exist); read-back = audit "Why Not Selected" shows the held-target deferral on carried rows |
| `TFB_T10_VENUE_ALLOWLIST` | `US,SR,T,HK,L,PA,AS,BR,DE,MI,MC,LS,VI,SW,TO,AX,OL,SI,MX` | existing v1.19.2 gate, no code in this build; own evidence run; read-back = `.NS/.BA/.AT/.JK` rows fail "Eligibility (Venue)" and the board seats mapped venues only |

IR-020 guard: values exactly `1` (or the CSV above); anything else collapses silently to OFF.

## 5. Post-commit verification (Emad → Claude)
- [ ] Exactly two changed files: `core/analysis/opportunity_builder.py` + this sheet under `docs/evidence/`; **no harness in `scripts/`**.
- [ ] Blob on main == `824c473047043252ccd6945b0d3089b0c9af299a` (Claude re-fetches with cache-bust).
- [ ] Render deploy OK; board status line reports `builder v1.19.3`.

## 6. Register
- F2 (negative-news gate) closed as by-design: pool has no news field; News_Archive overlay is display-only until the hypothesis clears backtest (`routes/advanced_analysis.py` L830–831). Cosmetic: panel label could read "gate input Unknown".
- F1 (venue): both mechanisms exist (`TFB_T10_VENUE_ALLOWLIST` builder gate; `TFB_TOP10_TRADABILITY_GATE` selector gate) — arming, not code. Selector fail-open branch (`top10_selector.py` L4801) remains a vNEXT item.
- Repo hygiene: 20 `scripts/Harness …·PY/·py/·JS` files (file-card display names) are not importable; batch delete pending.
