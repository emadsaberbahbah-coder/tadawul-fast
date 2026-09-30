# TFB Commit Sheet — core/analysis/portfolio_actions.py v1.13.1 [P-153 SUKUK ROW: NO EQUITY LADDER ON A FIXED-INCOME HOLDING]

Date: 2026-09-30 (Wednesday) · Lane: Render/Python (Portfolio_Decision route) · **Build #6 of the day — on Emad's 16:07 "if any script need to be build or rebuild just do that and deliver"; three over the three-lane cap, disclosed here** · Protocol: One-Pass

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | `core/analysis/portfolio_actions.py` v1.13.0 at HEAD `cc4340a` — sha `211f8c4c409cc976…` (3,633 lines) = the 09-28 P-168b delivery, live on Render since 09-28 12:47 (`/health` `portfolio_actions_version` 1.13.0 at 16:02 today) |
| Production facts used | `TFB_PA_PROTECT_SUKUK` default 1 (never set); `TFB_PA_SUKUK_ASSET_CLASS` on (the "Sukuk (fixed income)" bucket prints on the page); `TFB_FORECAST_BASIS=observe`; `TFB_PF_ENGINE_ROI_DISPLAY=1`; `TFB_PF_NULL_LEVELS` default 1 (B-7: a None level renders blank); 5023.SR = Arabian Centres sukuk, `compliance_gate.EXPLICIT_ASSET_CLASS` → SUKUK on the symbol |
| Environment golden (base, this workspace, pytest 9.1.1 installed for the run) | `test_portfolio_actions.py` 11 OK · `test_pf_dd_guard_sukuk_exempt.py` PASS T1–T4 digest `fb1b5d8662edc743` · pytest set p165 + p168b + sukuk_exempt + portfolio_actions = **26 passed, 2 skipped** (the two integration tests skip without `TFB_TEST_MP_TSV`; with today's export they FAIL on base too — fixture-anchored to the 09-28 book where DDI qualified for ADD) · `test_pf_f1_plan_basis_dualtree.py` / `test_pf_dd_fee_guards_local_dualtree.py` need base copies / pin 1.11.0 (stale) · `test_main_health_pf_gates_v8140.py` needs fastapi |

## S2 — Root (pinned on source + the 09-30 exports)
Every RULE in the module already stands down for a SUKUK-class holding: D-9 (v1.2.1) never a SELL leg · RULE-1b (v1.7.3) never a position-cap TRIM · v1.3.0 its own sector bucket · F-2 (v1.12.1) drawdown/time guard `[dd-exempt]`. The ROW does not: `_action_row` writes `stop_sar/tp1_sar/tp2_sar` from `_level_sar(cand.stop/tp1/tp2)` for every holding (L3152–3154), `_advisor_sentence` appends "stop x / TP1 y / TP2 z SAR" for every ADD/HOLD row with a stop (L2910–2919), and `_apply_f1_observe_tag` prints the plan-3M pair (TP1/price − 1, an equity valuation test) on every row (L3026–3048). The ladder itself is derived upstream by `opportunity_builder.normalize_candidate` from `price × (1 − stop_pct)` and the target — so a par-100 sukuk gets stop 92.23 (8 %) and TP 102.70 / 105.16. Today's 08:15 Portfolio_Decision row for 5023.SR: `Stop SAR 92.23 · TP1 102.70 · TP2 105.16`, note "…[f1-observe] plan 3M ROI 2.4% vs 3.0%; legacy basis kept; stop 92.2 / TP1 102.7 / TP2 105.2 SAR…". A printed stop on a sukuk invites the one action the rules refuse to take. Display class only — register **P-153**, scope-cut from v1.13.0 to keep one mechanism per build.

## S3 — Change (7 anchored edits, each `count == 1`)
| # | Site | Edit |
|---|---|---|
| E1 | header | v1.13.1 WHY block + `PORTFOLIO_ACTIONS_VERSION = "1.13.1"` |
| E2 | after `_null_levels_enabled` | `SUKUK_LADDER_NOTE` ("sukuk / fixed income (D-9): held for income - no equity stop/TP ladder (exit by maturity, issuer event or operator decision)"); `_env_sukuk_ladder_legacy()` (kill: `TFB_PA_SUKUK_LADDER_LEGACY` = 1/true/on/yes); `_sukuk_display_active(cand)` = `_protect_sukuk_enabled() and not kill and _is_sukuk_holding(cand)`, fail-open False |
| E3 | `_advisor_sentence` | ADD/HOLD + sukuk → the note bit replaces the ladder bit; the v1.7.2 B-7 branch becomes the `elif` (byte-identical text for equities, incl. the "no TP ladder" form) |
| E4 | `_apply_f1_observe_tag` | under observe, a sukuk row gets `[f1-observe] n/a - sukuk / fixed income (D-9): equity valuation basis not applied` — the observe token stays (per-row count complete), no basis pair; legacy / plan3m untouched |
| E5a/E5b | `_action_row` | `_sk_disp = _sukuk_display_active(cand)`; `stop_sar / tp1_sar / tp2_sar = None` when set (rendered blank — the B-7 contract) |
| E5c | `_action_row` | `detail.ladder_display = "sukuk_na"` on that row only (machine-readable witness) |

**DEFAULT ON** — the OFF state IS the defect (the v1.22.2 near-miss-text precedent of this morning). Kill switch `TFB_PA_SUKUK_LADDER_LEGACY=1` (call-time read, no redeploy) restores the v1.13.0 row byte-for-byte; `TFB_PA_PROTECT_SUKUK=0` (the sukuk master switch) also makes it inert. **Equity rows are byte-identical in every mode; verdicts, `capped_from`, KPIs, alerts, counts, `roi_pct` and the engine-ROI cells are untouched on every row including the sukuk.**

Deliberate cuts: `decide_action` is untouched — the sukuk's action reason still reads "Upside 4.9% below add threshold 12.0%; within all caps" (decision logic, not display; under `plan3m` the plan leg still runs on the sukuk); whether a sukuk should be excluded from the ADD ladder altogether is a Saturday parameter question; `detail.stop_pct / rr / mos_pct` stay (not on the sheet); the GAS 17_Portfolio_Decision writer is untouched (a None level already renders blank since B-7).

## S4 — Audits (×3 identical)
| Check | Result |
|---|---|
| Delivered SHA-256 | `1b88905a0194b3e08ce9112b6730d9030e0f5bfa26d6c6acd5a28b9b1d39f0c6` (3,714 lines) |
| `py_compile` | PASS |
| AST | functions 98 → 100 (+2: `_env_sukuk_ladder_legacy`, `_sukuk_display_active`; **0 removed**) |
| Line audit | **5** base lines not verbatim = version constant · the B-7 `if` → `elif` · the three ladder assignments; 86 lines added |
| Non-ASCII | 0 new characters vs base; 0 smart quotes (new code pure ASCII) |
| Harness `tests/test_pf_sukuk_ladder_p153.py` (sha `466fdaa039097950…`; REAL module end-to-end via `build_portfolio_actions` on the real 09-30 `My_Portfolio` export — 6 holdings incl. 5023.SR — with the production display env; REAL `compliance_gate` classifier; dual-tree vs the v1.13.0 file via `PA_BASE`) | **S0–S8: 29/29 PASS ×3, digest `1ab8c257a4b34631` ×3** (plain invocation without base/export: 23 PASS, S2–S6 real-export and S5 dual-tree SKIP; pytest form: 1 passed) |
| S2 (default, real book) | 5023.SR: cells `None/None/None`, `detail.ladder_display = sukuk_na`, note carries the sukuk note and the engine-forecast sentence, action reason carries the f1 n/a tag; the five equity rows (incl. CWBC/KRP's B-7 "no TP ladder" form) byte-identical to the kill-switch run; verdicts `[YUM HOLD (capped EXIT), DDI HOLD, 5023 HOLD, CWBC HOLD, AER HOLD, KRP HOLD (capped TRIM)]`, KPIs, alerts, counts identical across default / kill / protect=0 |
| S3 / S4 | kill switch reproduces `92.23 / 102.70 / 105.16` + "plan 3M ROI 2.4% vs 3.0%" + "stop 92.2 / TP1 102.7 / TP2 105.2 SAR"; `TFB_PA_PROTECT_SUKUK=0` == kill |
| S5 dual-tree | base reproduces the defect; **legacy payload == base payload** (versions masked) under observe / legacy / plan3m; default differs from base **only** in `actions[5023.SR].{stop_sar, tp1_sar, tp2_sar, action_reason, advisor_note, detail.ladder_display}` (observe) / the five non-f1 fields (legacy, plan3m) |
| S7 | equity row with target below price keeps the B-7 line (the `elif` is intact); a sukuk BLOCK row prints neither bit (ADD/HOLD only, as v1.13.0) |
| Existing batteries (delivered tree) | pytest set **27 passed, 2 skipped** (= base 26 + the new file); `test_pf_dd_guard_sukuk_exempt.py` digest **`fb1b5d8662edc743` = base**; `test_portfolio_actions.py` 11 OK; the two export-anchored integration tests fail identically on base |

## S5 — Delivery
| File | Destination |
|---|---|
| `core/analysis/portfolio_actions.py` | repo (full file) |
| `tests/test_pf_sukuk_ladder_p153.py` | repo `tests/` (new; `PA_BASE=<v1.13.0 file>` enables S5, `TFB_TEST_MP_TSV=<My_Portfolio export>` enables the real-book legs) |
| `docs/evidence/TFB_Commit_Sheet_portfolio_actions_v1.13.1_2026-09-30.md` | repo `docs/evidence/` |

## S6 — Deploy / read-back (Render lane)
1. Commit the three files. **Do not Manual-Deploy for this alone** — it is a one-row display change and each deploy wipes the L1 fund cache (six wipes today already). It rides the next deploy that is needed anyway (or the Saturday 10-03 sitting). No ENV change.
2. Read-back after the first deployed Portfolio_Decision run: status line `actions v1.13.1`; the 5023.SR row shows blank `Stop SAR / TP1 SAR / TP2 SAR`, its Advisor Note reads "…sukuk / fixed income (D-9): held for income - no equity stop/TP ladder…", its action reason ends "[f1-observe] n/a - sukuk / fixed income (D-9)…"; every other row byte-identical to the previous run's text apart from prices; Sector Summary and Alerts unchanged.
3. Rollback: `TFB_PA_SUKUK_LADDER_LEGACY=1` on Render (call-time read, no redeploy) or `git revert`.
