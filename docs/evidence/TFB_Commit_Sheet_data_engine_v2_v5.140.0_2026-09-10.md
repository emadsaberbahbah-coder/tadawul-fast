# TFB Commit Sheet — core/data_engine_v2.py v5.140.0 "FUND-SENTRY" (P-115)
Date: 2026-09-10 · Builder: Claude (One-Pass Script Protocol) · Operator: Emad

## 1. Identity
| Item | Value |
|---|---|
| Register item | **P-115** (red-team MR-03, ACCEPTED 2026-09-10) — fundamentals unit contracts + margin coherence |
| Base | `core/data_engine_v2.py` v5.139.0 @ HEAD `bff7d869270665241a41ee269aa98ce6199fbbd1` — sha256 `2e2524cd557ceec3…` |
| Delivered | v5.140.0 — sha256 `ffc3c1ec86c0cf67…` (17,486 lines; +182 / −2 vs base) |
| New test | `tests/test_fund_unit_sentry.py` — sha256 `95569eb739767f3a…` (119 lines, 12 tests) |
| Gate | `TFB_FUND_UNIT_SENTRY` = *(unset/off)* / `observe` / `enforce` — **default OFF ⇒ rows byte-identical to v5.139.0** |
| ENV placement | Render (backend/provider setting per placement rule). Zero GitHub-workflow ENV. |
| Rollback | Unset the env (no deploy needed) or `git revert` of the single commit. |

## 2. Production evidence driving the build
Two live My_Portfolio snapshots, 2026-09-10, DDI.US, price unchanged at 12.72, 13 minutes apart:
- **08:45:11** (warnings: `eodhd_fundamentals_fallback_applied`): `profit_margin = 0.33` — the raw **fraction** under the engine's percent contract (provider computes `_safe_div(net_income, revenue)`, a fraction by construction; truth 32.91).
- **08:58:11** (warnings: `yahoo_enrichment_applied`): `debt_to_equity = 3.70` — Yahoo's **percent** figure under the engine's ratio contract (`yahoo_fundamentals_provider.py` L2195 raw passthrough; truth ≈0.037; same 100× class as the SBGI.US `1,151` note at enriched_quote.py L262).
- Side effect: `forecast_reliability_score` 54.30 → 70.40, crossing the PF `Min Reliability to Add = 70.0` gate. Reproduced arithmetically: `0.7×conf + 0.3×DQ` − 5 (soft-cap) [− 15 iff `fallback` in `opportunity_source`]. The reliability flip itself is **path-dependence, not the corrupt values** → carved out as **P-115b** (see §7).

## 3. Changes (8 anchored edits, each count==1 asserted)
| # | Site | Change |
|---|---|---|
| E1 | Top docstring | Version line → 5.140.0 + full WHY v5.140.0 block |
| E2 | L3462 | `__version__ = "5.140.0"` |
| E3 | Before `_yahoo_enrichment_enabled` | New section: `_fund_unit_sentry_mode()`, `_fund_unit_contract_apply()`, `_fund_coherence_sentry()` + 6 constants |
| E4 | `_apply_yahoo_enrichment_pass` (fundamentals branch, post-canonicalize, pre-filter) | `debt_to_equity ÷ 100` on the canonical Yahoo patch (percent → ratio contract) |
| E5 | Same, inside `if filtered:` | Disclosure tag `fund_unit_contract:yahoo:<key>` (`:observe` suffix in observe) — only for keys that survived the missing-field filter |
| E6 | `_apply_eodhd_fundamentals_fallback` (post-canonicalize, pre-filter) | Margin-family `× 100` when `|v| ≤ 1.5` (fraction → percent contract) |
| E7 | Same, inside `if filtered:` | Disclosure tag `fund_unit_contract:eodhd:<key>` |
| E8 | `_get_enriched_quote_impl`, immediately **before** the Fix-AZ LKG block | Margin/PE coherence tripwire: `implied = 100×(market_cap/pe_ttm)/revenue_ttm`; ≥8× divergence with `|implied| ≥ 2pp` ⇒ enforce quarantines `profit_margin` + tag `fund_coherence_quarantined:profit_margin`; observe tags only |

Design decisions carried from the signed-off S2: deterministic **contract** conversion (not value-sniffing) on the two proven-defective paths only; the **D/E leg of the tripwire was deliberately cut** (an absolute D/E plausibility rule misfires on legitimately leveraged REIT/utility names — the Yahoo ÷100 contract fix closes the observed 100× class instead); coherence runs **before** LKG capture so a quarantined margin is never snapshotted as clean. All tags substring-safe (no cap/forecast/target/roi/drop/reject).

## 4. Audits (S4)
| Audit | Result |
|---|---|
| Anchored edits | 8/8 applied, every anchor count==1 asserted |
| `py_compile` | PASS |
| AST zero-removal (top-level + class methods) | PASS — additions only: 3 functions, 6 constants |
| Smart-quote scan on added lines | PASS |
| Real-module harness | **12/12 PASSED × 3 runs, timing-normalized output identical**; module imports clean, `__version__ == "5.140.0"` |

## 5. Harness coverage (goldens = the two production DDI rows, verbatim)
off-inert byte-identity · mode parsing · Yahoo 3.70→0.037 · SBGI 1,151→11.51 · EODHD 0.3291→32.91 (D/E ratio untouched) · percent-points pass-through (38.14 > 1.5 bound) · observe-no-mutation · coherence fires on the 08:45 shape (0.33 vs implied 33.0, 100×) · coherence quiet on the 08:58 shape · quiet on incomplete/`|implied|<2pp` · tag substring-safety · wiring-present-in-source.

## 6. Destinations + arming plan (operator)
Commit paths: `core/data_engine_v2.py` · `tests/test_fund_unit_sentry.py` · `docs/evidence/TFB_Commit_Sheet_data_engine_v2_v5.140.0_2026-09-10.md`.
1. Commit + deploy — behavior-identical while the gate is unset (proven OFF-inert). Post-deploy check: `/health` shows engine 5.140.0.
2. **Arm `TFB_FUND_UNIT_SENTRY=observe`** in Render (one ENV per evidence run). Read-back = next full sync's export carries `fund_unit_contract:*:observe` tags (expected classes: `yahoo:debt_to_equity` broadly on Yahoo-enriched rows; `eodhd:profit_margin` on fallback rows; any `fund_coherence_quarantined:*:observe`).
3. Flip to `enforce` after the observe read-back is reviewed. Sequencing vs the queued `TFB_PF_SWITCH_SCAN=1` is the operator's call (recommendation: sentry observe first).

## 7. Residuals → register
- **P-115b** — reliability path-dependence: the −15 penalty keys on `opportunity_source` containing "fallback", so the enrichment path (not the data) moves reliability across the 70 gate. Scoring-semantics change; own slot.
- Scope note: the Yahoo D/E contract fix covers the enrichment pass (the proven path). If `yahoo_fundamentals` ever supplies D/E through the primary provider loop, that path is not converted (deliberate S2 blast-radius scope).
- Bound residual: a genuine sub-1.5-percent-point EODHD gross/operating margin shipped in percent units would misconvert; observe mode surfaces any such case before enforce.
- ENV inventory: production numbers only reproduce with **`TFB_RELIABILITY_RECALIBRATION` ON** — not in the recorded arming history; operator to confirm its presence in Render and log it.
