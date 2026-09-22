# TFB Commit Sheet — core/data_engine_v2.py v5.148.0 [P-102 FC-TUPLE COHERENCE]

Date: 2026-09-22 (Riyadh) · Register item: **P-102** (re-issue of the never-committed v5.146.0 build, same design, on the v5.147.0 base — supersede lane) · Operator GO: "done next" after the v5.147.0 commit verification.

## Base pin (live)

| | |
|---|---|
| Base `core/data_engine_v2.py` | v5.147.0 at HEAD `main` (raw read 2026-09-22 ~13:05 Riyadh) · SHA-256 `017a20d7c98452747c900e7393507f30854999f35eeb9cd95c2359ee1ed53c05` · 18,153 lines · byte-identical to the v5.147.0 delivery |
| Also verified at HEAD | `tests/test_de_f7_scoring_settle.py` `3d0663b2…` ✓ · `tests/test_de_crypto_pair_shape_p151.py` `4c437d7c…` ✓ · commit sheet v5.147.0 `23d13424…` ✓ · P-158 arming sheet `acc101d3…` ✓ (landed) |
| **Mis-paste found at HEAD** | `tests/test_de_fund_sentry_repair_p146.py` = `4c437d7c…` = the **P-151 test's content** (the P-146 battery is gone from main). Correct file (`035ceffb…`) re-attached in this delivery |

## Mechanism (pinned on source)

`expected_roi_*` is derived once (Phase-II honor branch / synthesis / the eq_roi backfill); afterwards the R-6 keep-last-good restore re-plants `forecast_price_12m` and every price refresh replaces `current_price`, and nothing re-derives the ROI — the published (fp, cp, roi) triple is a vintage mix. Measured on the same tolerance: 173 rows (09-20) → 157 (09-21) → **180 today (ML 34 / GM 146, all 12M)**; board seat 2286.SR stored 26.74% vs implied 26.03%; 4142.SR −20.62% vs −17.81%; 4001.SR −23.83% vs −21.36%.

## Change

Gate `TFB_FC_TUPLE_COHERENT` = off (default, byte-identical) | observe | enforce, explicit words only, read at call time. `_fc_tuple_coherence(row)` runs immediately **before** `_apply_investability_gate` at all three publish boundaries (`_strict_project_row`, `get_page_rows`, the direct Top_10 path). Per leg 12m/3m/1m with cp>0, fp>0 and a fraction-domain stored ROI (|roi| ≤ 1.5): implied = (fp−cp)/cp, tol = max(0.0005, 2·0.005/cp + 0.0002).
- observe → ONE tag per incoherent leg `fctuple_vintage:<h>:observe`, values untouched.
- enforce → `expected_roi_<h> = round(implied, 6)` + `fctuple_vintage:<h>:enforce`; prices never touched; percent-domain ROI never scaled (units belong to P-101/P-143); missing ROI left to the backfill; idempotent.
- Mode in `[GUARDS]` boot line (`fc_tuple=`) and `/health engine_gates.fc_tuple_coherent`.

Nine anchored edits, each `count == 1`: E1 version · E2 WHY block (before WHY v5.147.0) · E3 three helpers placed directly above `_apply_investability_gate` · E4/E5/E6 the three seams · E7 engine_gates · E8/E9 GUARDS boot line.

| | |
|---|---|
| Delivered `core/data_engine_v2.py` | v5.148.0 · SHA-256 `e6df3ef9181d4308e3690f5342adc7d58fef674e1155091ac304ea5fa125803c` · 18,258 lines (+105 / −0) |
| AST | 500 → 503 (+3: `_fc_tuple_mode`, `_fc_tuple_tol`, `_fc_tuple_coherence`), **0 removed** · `py_compile` PASS · non-ASCII delta 0 · smart quotes 0 |

## Evidence

**Replay harness on the real 2026-09-22 export** (`ds/h_p102.py`; 7,317 rows across Market_Leaders / Global_Markets / Commodities_FX; (fp, cp, roi) triples parsed from the published cells; delivered function vs an independent spec implementation) — **PASS ×3, digest `4c9f448cab0a1520`**:

| Mode | Result |
|---|---|
| off | 0 legs; every row byte-identical |
| observe | 180 legs on 180 rows (ML 34 / GM 146 / CFX 0; 12m 180, 3m 0, 1m 0), values untouched, tags == golden set, no forbidden substring in any added tag |
| enforce | 180 repaired == golden values; prices touched 0; percent-domain legs touched 0 (0 such legs exist in today's export); **second pass residual 0** |

Specimens (stored → implied): 4017.SR −10.70% → −11.05%; 4142.SR −20.62% → −17.81%; 8020.SR −18.79% → −17.93%; seat 2286.SR 26.74% → 26.03%; holdings coherent within tolerance (SBAC 28.43/28.43, DDI 33.95/33.97, KRP 32.24/32.20, CARE 14.28/14.27, YUM 24.08/24.09, CWBC 14.25/14.23).

**Repo battery** `tests/test_de_fc_tuple_coherence_p102.py` (SHA-256 `c4bc7322ae92fd8d70ecb6045e356f8a096b00400196366f90f8ad58125d4f4d`, 172 lines): T1 gate words · T2 off inert · T3 observe tag-only · T4 enforce repair + idempotent · T5 guards (percent domain, missing ROI, zero/missing price, non-dict) · T6 tolerance boundary · T7 wiring (three seams each within 200 chars before the gate call, health key, boot leg, version floor) — **7/7 ×3**.

**All engine batteries on the delivered tree: 52/52 ×3** (P-102, F-7, P-151, P-146, P-143, fund-unit sentry, P-115b). `tests/test_de_f7_scoring_settle.py` T8 loosened from the full boot-line literal to the `scoring_settle=%s` leg (the literal now grows per gate) — SHA `b619119a8a74fd50407e65b88a1dd26143b1e20f9e683ae4dd23dd99ed01d25d`. CI's four: 70/5/2 — the same five endpoint tests fail on the base tree in this sandbox (no app); not attributable.

## Deploy

1. Commit **five files in ONE push**: `core/data_engine_v2.py` (v5.148.0), `tests/test_de_fc_tuple_coherence_p102.py`, `tests/test_de_f7_scoring_settle.py` (updated), `tests/test_de_fund_sentry_repair_p146.py` (**the correct P-146 file, repairing the mis-paste**), `docs/evidence/TFB_Commit_Sheet_data_engine_v2_v5.148.0_2026-09-22.md`.
2. One Render Manual Deploy (absorbs the still-unproven v5.147.0 deploy: v5.148.0 contains v5.147.0 verbatim). Proof: boot log `[engine_v2 v5.148.0] module loaded`, `[GUARDS] … scoring_settle=<mode> fc_tuple=off`; `/health` engine_version 5.148.0, `engine_gates.scoring_settle` and `engine_gates.fc_tuple_coherent` present; startup_warnings [].

## Arming (Render lane, one per evidence run)

Order unchanged: `TFB_SCORING_SETTLE=observe` first (F-7 read-back on the next full-sync export), then `TFB_FC_TUPLE_COHERENT=observe` on the following run (read-back = exactly the rows the audit counts: ~180 `fctuple_vintage:12m:observe` tags, ML+GM, 0 on CFX/MF, zero value changes). Enforce for either = separate sitting; P-102 enforce is a value change on ~180 rows incl. a board seat → S-1 boundary note.

## Rollback

env unset (no deploy) or `git revert`.
