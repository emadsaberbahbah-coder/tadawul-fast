# TFB Commit Sheet — routes/investment_advisor.py v2.18.0 — 2026-09-13

## Register item
**P-130** — advisor twin except-TypeError retry sites (adjudicated ACCEPTED 2026-09-12,
external-patch HEAD audit; last adjudicated-open Python item). **CLOSES P-130.**

## Root cause (re-proven on live HEAD, not taken from the register)
`routes/investment_advisor.py` v2.17.0 — the live owner of `/v1/advanced/*`
(Insights_Analysis daily refresh):

- **Site 1 — `_auth_passed`** (base L1189): `except TypeError: continue` walks all
  7 auth_ok kwargs variants on ANY TypeError, including one raised inside auth_ok.
- **Site 2 — `_call_candidate`** (base L1405): ANY TypeError → outcome `"typeerror"`
  → next kwargs variant. A TypeError raised INSIDE the bridge function body (a
  real data-shape bug) is re-executed up to 9× against progressively FEWER
  kwargs — variant 9 is `{}`, so a degraded call can "succeed" while silently
  dropping page/limit/offset/schema_only — and on exhaustion the outcome label
  lies (`all_signatures_typed_mismatch` for a body bug). Same quiet-degradation
  class as the P-110 secondary finding.

Sibling modules already carry the discriminator: `routes/advisor.py`
`_looks_like_signature_type_error` (L920) and `core/investment_advisor_engine.py`
`_signature_typeerror_is_retryable` (L1968). This file was the unfixed twin.

## Fix
- New `_signature_typeerror_is_retryable(exc)` — marker union of both sibling
  helpers, dedup'd ("positional argument" subsumes the required/too-many forms).
- New `_p130_typeerror_retry_legacy()` — kill switch, env read at CALL TIME.
- Site 2: signature-mismatch → walk (unchanged, per-attempt record gains
  additive `"signature_retryable": true`); real body TypeError → stash
  `_last_call_summary` and **raise** — rides the EXISTING "raised" machinery
  (caller catch at `_execute_via_bridge` L1723 already recovers the summary,
  logs, returns the partial response). No new caller path.
- Site 1: signature-mismatch → walk; real TypeError → `return False`
  (same terminal as the except-Exception arm).
- Docstring outcome contract updated; WHY v2.18.0 header block added;
  `INVESTMENT_ADVISOR_VERSION` 2.17.0 → 2.18.0.

## Gate
**DEFAULT ON** with kill switch `TFB_ADV_TYPEERROR_LEGACY=1` (restores legacy
retry-all at both sites, read per call — no restart). Stated deviation from the
default-OFF doctrine, per the v1.19.6 ROI-TRUTH / P-129 precedent: the OFF
state IS the defect; the happy path (signature walk → success) is
behavior-identical; only real body-TypeErrors change route, and they ride
pre-existing handling. Operator may veto via the kill switch or git revert.

## Base pin (S1)
- Source: https://raw.githubusercontent.com/emadsaberbahbah-coder/tadawul-fast/main/routes/investment_advisor.py
- Base v2.17.0 SHA256: `97c2af4a1cc1acac7a90fe269aaedd189179ee61603b734ab826025555813eb0` (2,396 lines)
- Note: GitHub REST API was rate-limited this session; pin taken via raw fetch.
- Sibling references pinned same session: routes/advisor.py `832eca4a…`,
  core/investment_advisor_engine.py `7039130c…`.

## Delivered
- `routes/investment_advisor.py` v2.18.0 — SHA256
  `9eb143bca0e8a464aca2ca54ca3e6ffc012b50c1af1e1b4a25de3e854239119b` (2,486 lines)
- `tests/test_adv_typeerror_retry_p130.py` — T1–T6 battery, repo-layout import
  with file fallback, no network.
- This sheet → `docs/evidence/`.

## Audits (S4)
- 6 anchored edits, every anchor `count==1` asserted.
- `py_compile` PASS. Smart-quote scan: 0.
- AST zero-removal proof: 49 → 51 functions; added only
  `_p130_typeerror_retry_legacy`, `_signature_typeerror_is_retryable`; 0 removed.
- Dual-tree REAL-module harness (base tree vs delivered tree), R1–R5 ×3,
  identical digest `5b5d7edbe4ee7b7a`:
  - R1 signature walk → success identical across base / fixed / fixed+kill
    (additive flag only; flags all `true` on typeerror attempts).
  - R2 body TypeError: base = 9 degraded retries + `typed_mismatch` (defect
    reproduced); fixed = raise after 1 attempt, flag `false`; kill = deep-equal
    to base.
  - R3 mixed (2 sig misses then body bug): base lies at 9/`typed_mismatch`;
    fixed raises at attempt 3, flags `[true,true,false]`; kill = base.
  - R4 ValueError branch byte-identical (raise at attempt 1, all trees).
  - R5 `_auth_passed`: sig walk identical (True @ 6 calls all trees); body bug
    base 7 calls / fixed 1 call / kill 7 calls, all False.
- Standalone battery T1–T6 PASS ×3 against the delivered file.

## Deploy (operator)
1. Commit the three files at their convention paths (above) to `main`.
2. Render auto-deploys; verify
   https://tadawul-fast-bridge.onrender.com/v1/advanced/health → `"version":"2.18.0"`.
3. No ENV action needed (default ON). Kill/rollback:
   `TFB_ADV_TYPEERROR_LEGACY=1` in Render, or `git revert`.

## Read-back (S6)
- Positive: health/meta report 2.18.0 AND the next Insights_Analysis refresh
  stays green (53 rows, HTTP 200) — happy path proven identical in production.
- Honesty signature (event-driven): any future body-TypeError surfaces as
  `bridge_call_outcome="raised"` with the full message in meta/logs instead of
  a silent `typed_mismatch` — per-attempt records carry `signature_retryable`.
