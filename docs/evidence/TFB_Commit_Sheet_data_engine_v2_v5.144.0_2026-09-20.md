# TFB Commit Sheet — core/data_engine_v2.py v5.144.0 [REL-PATH-TAG] (P-115b)

**Date:** 2026-09-20 (Sunday, Riyadh) · **Item:** P-115b — path-dependent forecast_reliability_score (upgraded 2026-09-19; decision-relevant specimen 2026-09-20: DDI.US 70.4 advisor vs 63.1 sync with an INVEST/DO_NOT_INVEST split, YUM 76.5 vs 58.1, CWBC 76.5 vs 59.3) · **Build #3 of 2026-09-20** on Emad's "done, let's move to the next" · **Engineer:** Claude · **Protocol:** One-Pass S0→S6 · **Scope:** observe-only instrumentation — the semantics decision (which legs are path artifacts) is an F-item for Emad after one tagged export.

## S0 / S1 — Freeze and pin

- HEAD verified before the build: `f22d05597164a9dfa6b2f6e3dfe99930627d013c`; the four v1.22.0 files byte-identical at branch AND commit (builder `529a0864…`, tests `8a197c48…` / `fef262ce…`, sheet `075d8da4…`) — v1.22.0 commit leg CLOSED (Render `/health` 1.22.0 paste still owed).
- Base = data_engine_v2 v5.143.0 at that commit, **17,723 lines, sha256 `78b8f5e8444a65bf…`** — identical to the 2026-09-19 delivery (zero drift).

## S2 — Design (gate `TFB_REL_PATH_TAG` off|observe, read per call, no restart)

The score is a sum of independent legs: base (0.7·fc + 0.3·dq under `TFB_RELIABILITY_RECALIBRATION`, else fc), −60 no price (NP), −40 no forecast (NF), −5/−20 soft cap (SC5/SC20), −15 provider-target drop/reject (PD), −30 bar stale (BS, own gate), −25 cross-provider conflict (XC, own gate), −15 opportunity-source fallback/momentum (OS), −15 forecast-source synthetic/fallback/momentum (FS); then the DISPLAY calibration factor by bucket (v5.91.0, decision-neutral).

| Mode | Behaviour |
|---|---|
| `off` (default/unset/other) | Byte-identical v5.143.0 — measured on 1,628 base-vs-delivered cases: 1,628 identical rows. |
| `observe` | ONE substring-safe tag appended to `warnings` per scored row: `rel_path:b=B\|F:fc=..:dq=..:pen=<codes\|none>:os=<src>:fs=<src>:raw=..:cf=<factor\|none>:fin=..`. Values, verdicts and every other warning untouched. Mode disclosed in `/health engine_gates.rel_path_tag`. |

Substring safety: the gate itself tests `warnings` for cap / forecast / target / roi / drop / reject / provider_target / price_bar_stale / xprovider_price_conflict — and preserved rows are re-read next run. Source tokens are rewritten (provider_target→pt, fallback→fb, momentum→mo, synthetic→sy, forecast→fc, target→tg, conflict→cf, reject→rj, stale→st, drop→dp, cap→cp, roi→ri), the builder re-checks the finished tag and suppresses it rather than emit a forbidden substring.

## S3 — Build (16 anchored edits, every replacement asserted `count == 1`)

E1 header WHY · E2 `__version__` 5.144.0 · E3 helpers after `_reliability_recalibration_enabled` (`_rel_path_tag_mode`, `_rel_path_tag_enabled`, `_rel_path_src_code`, `_rel_path_tag`, rewrite/forbidden tables) · E4 `_rp = []` after the base · E5–E12 one `_rp.append(<code>)` line inside each of the eight penalty branches (single source of truth — no condition duplicated) · E13 `_rp_raw, _rp_cal` stash after the clamp · E14 `_rp_cal = _cal_factor` inside the calibration branch · E15 tag emit immediately before `row["forecast_reliability_score"] = rel` · E16 `rel_path_tag` in `surface_gate_states()`.

Delivered: **17,826 lines**, sha256 `b6784d21a159c658e0206916d59d22df7c5896b0839be523ef3099022acfda68`. `py_compile` PASS. AST defs 485 → 489 (**+4, 0 removed**). Smart quotes 0. Net-new non-ASCII outside comments 0.

## S4 — Internal audits ×3

**Repo harness** `tests/test_de_rel_path_tag_p115b.py` (261 lines, sha256 `a92118ca4ed12cb172bdbf0675e01ce27d4a012f6d339f944ebf2f4a79afea7a`), REAL module, `_apply_investability_gate` on rows shaped like the 09-20 cross-surface specimens plus adversarial sources: T1 helpers/rewrites/builder defensiveness · T2 off identity · T3 observe (values identical, one tag, penalty codes exactly the legs that fired, raw/fin/dq agree with the row, b=B/F by recalibration) · T4 display-calibration factor disclosed and reproduces fin = raw × factor · T5 substring safety incl. adversarial tokens · T6 preserved-row replay inert (a tagged row re-gated: same values, still one tag) · T7 idempotence · T8 wiring + `/health` disclosure. **ALL PASS ×3.**

**Dual-tree** (`replay_dualtree_p115.py`): base v5.143.0 vs delivered on 7 specimens + 400 seeded variants × 4 env combos (recalibration on/off × display calibration on/off) = **1,628 cases**: off rows identical 1,628/1,628; observe ok 1,628/1,628 (values identical, exactly one tag, remaining warnings identical, no forbidden substring); the tag's legs reconstruct the raw score within 0.1 in every case; digest `096aebe4e835fe56` **identical ×3**. Penalty histogram covers none/FS/OS/NF/NP/PD/SC5/SC20 and their combinations (BS/XC legs only fire under their own gates, off in the harness).

Existing battery `tests/test_de_fund_sentry_repair_p146.py` re-run on this file: **8/8 PASS**.

## S5 — Delivery (full files, convention paths)

- `core/data_engine_v2.py` (v5.144.0)
- `tests/test_de_rel_path_tag_p115b.py`
- `docs/evidence/TFB_Commit_Sheet_data_engine_v2_v5.144.0_2026-09-20.md` (this sheet)

## ENV (Emad applies; Render lane; ONE per evidence run)

| Var | Default | Purpose |
|---|---|---|
| `TFB_REL_PATH_TAG` | off | **the arming**: `observe` |

No other variable. Deploy is behaviour-identical (gate unset); `/health engine_gates.rel_path_tag` shows the mode at boot.

## S6 — Read-back and the decision it enables

First observe export: `rel_path:` on every scored row of GM/ML/CFX/MF and on the 6 My_Portfolio rows. For each holding, place the two surfaces' tags side by side — the leg that differs (dq under b=B, a penalty code, or `cf=`) is the attributed cause of the split. Expected on the DDI specimen: a `cf=` difference (INVESTABLE 0.781 vs BLOCKED 0.693 on a near-identical raw) and/or an OS/FS code on the sync path. That table is the evidence for the F-item: which legs are path artifacts to neutralise before the ADD gate reads the number.

## Rollback

Unset `TFB_REL_PATH_TAG` (no deploy) or `git revert`.
