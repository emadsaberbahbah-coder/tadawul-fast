# TFB Commit Sheet — core/analysis/portfolio_actions.py v1.12.1 [F-2 HONOURS D-9]

**Date:** 2026-09-21 · **Register:** F-2 follow-up (own-build defect, found before `TFB_PF_DD_EXIT=enforce`) · **Protocol:** One-Pass
**Why this item now:** the engine lane is blocked (v5.146.0 / P-102 still uncommitted at HEAD), so P-115b, P-159, P-160 and the `.MI`/`.NZ` routing cannot be built on a pinned base. This is the highest-value fix in a free lane.

## 1. Pins

| | Version | SHA-256 | Lines |
|---|---|---|---|
| Base (live-fetched, `main`) | 1.12.0 | `a764aeb8a2095ef9fdbc69b896919cda4128a4031ad54609cb364ae63c358086` | 3,250 |
| Delivered | 1.12.1 | `6a378895143584eecb825d111be4f778179045340f779f6159996cce9a4b9aeb` | 3,279 |

Base equals the 2026-09-14 delivery (zero drift). Harness tree also pinned `core/analysis/opportunity_builder.py` v1.22.0 (`529a0864…`) and `core/compliance_gate.py` (`1bb0d5fb27e5…`).

## 2. Defect (reproduced on the pinned base)

`_apply_drawdown_guard` (v1.11.0, F-2) has no asset-class test. Its basis is `pnl_sar / cost_sar` — **price only** — and its time rule exits anything "still negative after `TFB_PF_DD_TIME_D` (45) days".

5023.SR (sukuk, bought at par, 8.5 % coupon, ~300 days held, recorded income 637.50 SAR, 52W low 100.05) is above water on any honest basis. One print below par makes its price-only return negative with days ≫ 45:

- under `enforce` (the planned last Wave-2 arming) → **forced full EXIT of the income anchor**;
- under today's `observe` arming → the page would print "would EXIT under enforce".

Every other rule in the file already stands down for a sukuk: D-9 (never a switch-scan SELL leg, v1.2.1) and RULE 1b (never position-cap trimmed, v1.7.3). F-2 missed it.

## 3. Change — 2 anchored edits, each asserted `count == 1`

| Edit | Site | Change |
|---|---|---|
| E1 | `PORTFOLIO_ACTIONS_VERSION` | 1.12.0 → 1.12.1 + WHY block |
| E2 | `_apply_drawdown_guard`, after a trigger is confirmed | SUKUK-class holding → action and proceeds stand; countable `[dd-exempt]` tag discloses the trigger and the price-only basis |

The sukuk test runs **only after a trigger fired**, so every non-triggering row — and every equity — is byte-identical to v1.12.0 in all three modes.

AST: 87 → 87 defs, **0 added, 0 removed, exactly one touched (`_apply_drawdown_guard`)**. 30 lines added; 0 non-ASCII, 0 tabs. `py_compile` PASS.

**Gate:** rides on the existing `TFB_PA_PROTECT_SUKUK` (default ON) — the v1.7.3 precedent, no new environment variable. `TFB_PA_PROTECT_SUKUK=0` restores v1.12.0 exactly. *Stated deviation from default-OFF:* the OFF state is the defect, and the live output is unchanged today (see §4).

## 4. Evidence — real module, repo-shaped trees, base vs delivered in separate processes

**Seam truth table** — 36 cases (3 modes × protect on/off × 6 holdings). Exactly 4 differ, all sukuk-trigger cases with protection ON:

| Case | v1.12.0 | v1.12.1 |
|---|---|---|
| enforce · sukuk −0.5 %, 302 d | **EXIT** "still negative … full exit (was HOLD)" | HOLD + `[dd-exempt]` |
| enforce · sukuk −9.0 % | **EXIT** "drawdown −9.0 % breaches −8.0 %" | HOLD + `[dd-exempt]` |
| observe · both | `[dd-observe] … would EXIT under enforce` | `[dd-exempt]` |

Equities (drawdown and time triggers), the no-trigger sukuk, the EXIT pass-through, OFF mode and the kill switch: identical on both trees.

**Integration — today's real My_Portfolio rows (2026-09-21 export) under the live panel** (cash 24,763.73, 10 / 20 / 30 / 70 / 80, Advisory) through the real `build_portfolio_actions`:

| Book | off | observe | enforce |
|---|---|---|---|
| live (sukuk 100.90) | base == new | base == new | base == new |
| sukuk nudged to 99.50 | base == new | tag differs | **base EXITs 5023.SR · new HOLDs + `[dd-exempt]`** |

Deploying changes nothing on today's book. Digests: base `1941901203463195`, delivered `a51e2307cb29b37b`, each ×3 identical (one earlier base-tree invocation returned empty stdout; not reproduced in 7 further runs).

`tests/test_pf_dd_guard_sukuk_exempt.py` T1–T4 PASS ×3 on the delivered tree (digest `fb1b5d8662edc743`) and **fails on the base tree** (golden negative). Uses the real `core.compliance_gate` classifier — no doubles.

## 5. Deploy

Backend module → commit, then **Render Manual Deploy** (auto-deploy is off).

- `core/analysis/portfolio_actions.py`
- `tests/test_pf_dd_guard_sukuk_exempt.py`
- `docs/evidence/TFB_Commit_Sheet_portfolio_actions_v1.12.1_2026-09-21.md`

Read-back: boot log binds `portfolio_actions v1.12.1`; the Portfolio_Decision status line reads `actions v1.12.1`; the six rows are otherwise unchanged. The `[dd-exempt]` tag appears only if the sukuk ever triggers the guard.

## 6. Rollback

`TFB_PA_PROTECT_SUKUK=0` (also disables D-9 and RULE 1b — prefer `git revert`).

## 7. Deliberate cuts

- The guard's price-only return basis is unchanged for equities (income-inclusive basis = a model decision).
- The sukuk still receives equity-style stop / TP levels and the `[f1-observe]` tag on the page (P-153 family, display).
- No ADD-ladder exemption: the sukuk cannot qualify today (reliability 26.2, DQ 70.6).
