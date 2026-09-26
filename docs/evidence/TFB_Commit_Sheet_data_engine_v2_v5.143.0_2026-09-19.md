# TFB Commit Sheet — core/data_engine_v2.py v5.143.0 — 2026-09-19
## P-146: FUND-SENTRY repair leg (enforce three-way verdict) + sentry-mode disclosure

## Base pin (S1)
- Repo cloned at HEAD `5ff9d066dfb46b04ab46d9279ef45c4b2fee3c5e` (2026-09-17, #515). `core/data_engine_v2.py` sha256 `8e53a8f8b523d4204eb7977f27e46e7834c6fe7e20068b7fb6236f5b9177d427` = the 2026-09-16 v5.142.0 delivery, **zero drift**; 17,652 lines (wc -l).
- `tests/test_fund_unit_sentry.py` base sha256 `95569eb739767f3a48df5c73333199ae12dbc4b95864cf993227098235ed8709` (= the 2026-09-10 delivery), 12/12 PASS on base.

## Delivered
- `core/data_engine_v2.py` sha256 `78b8f5e8444a65bf32349fc53d37b854cba3f0ca8386cb93c98e748e8914f788`, 17,723 lines (+71).
- 7 anchored edits, each asserted count==1: E1 WHY block + `__version__` 5.143.0; E2 five constants + two tags; E3 tripwire docstring; E4 tripwire enforce branch (three-way verdict); E5 `surface_gate_states()["fund_unit_sentry"]`; E6 `[GUARDS]` boot-line format; E7 boot-line argument.
- AST functions 476 → 476 (**0 removed, 0 added** — every edit lives inside existing functions or the constants block); py_compile PASS; 74 added lines, 0 non-ASCII; LF line endings; every existing tag string unchanged.
- `tests/test_fund_unit_sentry.py` — **ONE expectation updated** (declared change): `test_coherence_fires_on_0845_shape`, enforce leg only — the DDI 100x golden is now `fund_coherence_repaired:profit_margin:x100` with `profit_margin == 33.0` instead of quarantine/None; the observe leg of the same test is untouched and still asserts `...:observe` + no mutation. Delivered sha256 `f18dbeaf9e1ea453d10be80b645a327b1e45992595546fcae4f0830144320810`, 12/12 PASS x3 on the delivered engine.
- `tests/test_de_fund_sentry_repair_p146.py` (NEW, 208 lines) sha256 `f1e629bcbecb305b4a32426433e41b59e8802a0005f46795b0e11f63ede94433`.

## Root (P-146) — adjudicated on the 2026-09-19 export + HEAD source
- Render env `TFB_FUND_UNIT_SENTRY` holds **`enforce`** (operator paste 2026-09-19 12:5x Riyadh); the 2026-09-10 arming record said `observe`. Source discriminator: observe tags carry `:observe` (L4068, L16040, L16309); every `fund_*` tag on the export is suffix-less. Corroboration by value scale: DDI D/E prints 0.04 (Yahoo raw 3.70 ÷ 100), 1,879 Yahoo-tagged GM rows have median D/E 0.46.
- Effect measured: `fund_coherence_quarantined:profit_margin` on **3,213 Global_Markets rows (48.6%) + 150 Market_Leaders rows + 5/5 US holdings**, and Profit Margin blank on 100% of them (3,213/3,213 GM). Implied margins recomputed from the export's own market_cap / pe_ttm / revenue_ttm: **3,075 of 3,197** computable GM quarantines are plausible (2–100pp); 72 in 100–1,000pp and 47 above 1,000pp (unit-inconsistent inputs, e.g. ESSA.JK IDR cap vs thousands-scale revenue, GDHG.US pe 0.0061). Holdings' implied: DDI 33.25 / CWBC 30.45 / CARE 45.42 / YUM 25.25 / SBAC 33.78 — the stored side was the off-scale one, the DDI 08:45 100x class.
- Source of the fraction: `core/providers/yahoo_fundamentals_provider.py` L2179 `profit_margin = _as_fraction(_pick(info, "profitMargins", "netMargins"))` on the PRIMARY provider path — outside both v5.140.0 contracts (Yahoo enrichment patch converts only D/E; EODHD fallback patch only fills missing keys). The tripwire therefore met a correct-but-fraction value and destroyed it.
- Not fully explained from the export (carried as an open sub-item): 1,929 of the 3,213 GM quarantines also carry `fund_unit_contract:eodhd:profit_margin`; `_filter_patch_to_missing_fields` proves that tag means the converted value LANDED, so the tripwire saw something other than that value or than the export's inputs at run time. Needs one live provider-payload golden (vNEXT P-146b). The repair leg is source-agnostic, so the outcome is handled regardless.
- Correction owned: the 09-13 / 09-14 / 09-16 read-backs counted `fund_*` substrings and labeled them observe tags; the suffix was never checked.

## Fix (enforce branch only; off and observe byte-identical to v5.140.0)
- `_fund_coherence_sentry`, after the unchanged v5.140.0 guards (inputs present, pe/mc/rev > 0, |implied| >= 2pp, divergence >= 8x):
  1. `|implied| > _FUND_SENTRY_IMPLIED_MARGIN_MAX_PCT (100.0)` → **fail OPEN**, value untouched, tag `fund_coherence_skipped:profit_margin:implied_oob`.
  2. divergence ratio in `[_FUND_SENTRY_REPAIR_RATIO_LO, _HI] = [90, 110]` (the 100x signature) → **repair** by x100 (stored < implied) or /100 (stored > implied), rounded to 4 dp, accepted only if the repaired value coheres (< 8x); tag `fund_coherence_repaired:profit_margin:x100|d100`.
  3. otherwise → quarantine exactly as v5.140.0 (`fund_coherence_quarantined:profit_margin`, value None).
- Invariant: a repair can only land inside [2, 100]pp (the two guards bound it); values outside that window are never produced by this leg.
- Disclosure: `[v… GUARDS] … fund_unit_sentry=off|observe|enforce` in the boot banner and `surface_gate_states()["fund_unit_sentry"]` → `/health` `engine_gates.fund_unit_sentry` — an arming is provable at boot and in every health paste instead of only by tag-suffix forensics.

## Gate / env
- **No ENV change.** `TFB_FUND_UNIT_SENTRY` stays `enforce` (Option A, adopted 2026-09-19 "go with the best option"); this deploy is the activation of the repair leg. Rollback = `git revert` (restores the quarantine behavior; env untouched).

## Evidence
- Existing battery `tests/test_fund_unit_sentry.py`: 12/12 on base; 12/12 x3 on the delivered engine.
- Repo battery `tests/test_de_fund_sentry_repair_p146.py` T1–T9 **PASS x3, identical digest `1e9f36ef804521ef`** (goldens = v5.140.0 DDI snapshots + verbatim 2026-09-19 export inputs BRK-B.US, BNY.US, DDI.US, ESSA.JK, HQH.US, GDHG.US). Golden-negative proven: the same file against base v5.142.0 fails at T3 (base quarantines the 100x golden).
- Dual-tree real-module harness H1–H5 **PASS x3, identical digest `b97c33566715d88f`** over **3,347 real export rows** (every quarantined GM+ML row with computable market_cap/pe_ttm/revenue_ttm; stored margin modeled as implied/100 = the pinned fraction class) plus the 5 holdings: H1 off A==B deep-equal (3,347, no tags); H2 observe A==B deep-equal, 3,344 `:observe` tags, zero value drift; H3 enforce — base quarantines 3,344 / delivered **repairs 3,224 + skips 120 + quiet 3**, every repair within 2% of its implied benchmark, every skip untouched with |implied| > 100pp; H4 repaired rows idempotent (second pass quiet), skipped rows re-tag each run by design (120); H5 holdings repaired to DDI 33.25 / CWBC 30.45 / CARE 45.42 / YUM 25.25 / SBAC 33.78.
- Harness discovery kept: `filtered` in `_filter_patch_to_missing_fields` contains only keys MISSING on the row, so a `fund_unit_contract:*` tag proves the converted value landed — which is what makes the 1,929 dual-tag quarantines an open question rather than a settled mechanism.

## Deploy & read-back
- Destinations: `core/data_engine_v2.py`, `tests/test_fund_unit_sentry.py` (updated), `tests/test_de_fund_sentry_repair_p146.py` (new), `docs/evidence/TFB_Commit_Sheet_data_engine_v2_v5.143.0_2026-09-19.md`.
- Render **auto-deploy is OFF** — Manual Deploy after commit; verify `/health`: `engine_version` **5.143.0**, `engine_gates.fund_unit_sentry` **"enforce"**, boot log line `… fund_lkg=on fund_unit_sentry=enforce`, both workers clean, startup_warnings empty.
- Positive read-back = next full sync export: `fund_coherence_repaired:profit_margin:x100` tags on the order of 3,000 (GM-heavy) with Profit Margin **repopulated at percent scale**; `fund_coherence_skipped:profit_margin:implied_oob` ≈ 120; `fund_coherence_quarantined:profit_margin` collapsing from 3,363 toward the non-signature residual; the five US holdings print Profit Margin ≈ 33 / 30 / 45 / 25 / 34 % on My_Portfolio. Zero change expected on any cell the tripwire does not touch.

## Residuals / vNEXT (registered, not built here)
- P-146b: convert Yahoo margin fractions at the PRIMARY canonicalization (provider L2179) so the tripwire has nothing to repair; needs a live provider-payload golden and a blast-radius pass on every consumer of `profit_margin`/`net_margin`.
- The v5.140.0 `|implied| >= 2pp` guard still lets a x100-inflated sub-2% margin pass unjudged (e.g. a true 1.2% margin stored as 120); unchanged.
- Acceptance class carried from v5.140.0's ≤1.5 bound: a true sub-1% margin beside a benchmark that is itself 100x off would be mis-repaired into [2,100]pp — bounded by the window, not eliminated.
- Module docstring title still reads v5.139.0 (header convention untouched since v5.140.0; every WHY block carried verbatim).
