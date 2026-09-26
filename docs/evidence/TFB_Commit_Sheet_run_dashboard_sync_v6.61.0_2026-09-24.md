# TFB Commit Sheet — scripts/run_dashboard_sync.py v6.61.0 [P-154d EODHD QUOTA GUARD: PRE-FETCH SKIP + POST-FETCH POISON REFUSAL]

Date: 2026-09-24 (Riyadh) · Author: Claude (One-Pass Script Protocol) · Operator: Emad
Register: P-154d (containment leg of P-154; breaks the chain quota → 402 storm → BLOCKED rows → seat hard-exits = P-168's trigger)

## 1. Base pin (S1)
| | |
|---|---|
| File | scripts/run_dashboard_sync.py |
| Base | SCRIPT_VERSION 6.60.0, sha `38d1662f19eccfce…`, 11,453 lines — live-fetched from main (raw, cache-busted); equals the 09-21 Build #2 delivery |
| Sibling | scripts/critical_symbol_identity.py (hard import) fetched from HEAD for the harness tree |

## 2. Delivered (S5)
| File | Repo path | SHA-256 (prefix) | Lines |
|---|---|---|---|
| run_dashboard_sync.py | scripts/run_dashboard_sync.py | `fab345b76829d6eb…` | 11,767 (+315 / −1 — the only removed line is the replaced version string) |
| test_sync_eodhd_quota_guard_p154d.py | tests/test_sync_eodhd_quota_guard_p154d.py | `b22e6eef43aa4a63…` | 313 |
| this sheet | docs/evidence/TFB_Commit_Sheet_run_dashboard_sync_v6.61.0_2026-09-24.md | — | — |
| daily_sync.yml (ARMING, separate sitting) | .github/workflows/daily_sync.yml | `d6becc099b3e44a0…` (base `68c3026b…`, +15/−1) | 1,694 |

## 3. Evidence (2026-09-23/24 _Run_Log + exports)
- `[EODHD-QUOTA v6.60.0]` reached **400,000/400,000 EXHAUSTED at 00:54 Riyadh** (third time in four days); the 20Z run fetched every page blind: 5,789 GM + 2,393 MF + 336 CFX rows written as `fetch_failed:HTTP 402` → BLOCKED; the 02:01 cockpit hard-exited ITRN/CRC/PINFRA/ADAM on that epoch (P-168); ITRN/ADAM returned at 06:06 as day-1 seats.
- The sentinel logged 96.5% CRIT at 23:38 — the sync had the information and no brake. The v1.2.0 recovery guard covers only replays.

## 4. Design (S2) — one gate, two seams, ranked market pages only
Gate `TFB_SYNC_EODHD_QUOTA_GUARD` = off (default, byte-identical: no poll, no line) | observe | enforce (explicit words). Knobs: `TFB_SYNC_EODHD_QUOTA_GUARD_PCT` 97 (clamped 50..100), `TFB_SYNC_EODHD_QUOTA_GUARD_POISON_PCT` 25 (1..100), `TFB_SYNC_EODHD_QUOTA_GUARD_ALLOW_EXTRA` (ON_EXTRA treated as exhausted unless 1).
- **PRE-FETCH** (after the v6.6.0 decision-owned guard, before `_read_symbols`): one 0-cost `/api/user` poll (the v6.60.0 helper — same key, timeout, token never logged). EXHAUSTED / ON_EXTRA / used% ≥ PCT → enforce: `status="skipped"`, rows 0/0, `return res` — nothing fetched, cleared or written; the F-09 early-exit stamp records `leg=skipped … data=PARTIAL` so the decision feed withholds truthfully on the last-good page. UNKNOWN (no key, poll failed, unparseable) always allows — fail-open.
- **POST-FETCH** (at the v6.60.0 sentinel site, before the write): fresh 402 rows (stamped since process start, the sentinel's own count) on ≥ POISON_PCT of the outgoing matrix → enforce: refuse the write exactly like the persistence-hard / OHLC-enforce guards (`status="skipped"`, `fail_result_on_identity`, `return res`); last-good rows preserved.
- **observe**: identical decisions, disclosed only — one `[EODHD-QUOTA-GUARD v6.61.0]` `_Run_Log` line (WARNING, `::warning::` annotation, Status `WOULD_SKIP`) per page per leg when a skip would fire; fetch and write proceed exactly as v6.60.0. enforce lines carry Status `SKIPPED`. Allow decisions go to the run log only (INFO).
- Telemetry failures annotated, never counted into `_RUNLOG_APPEND_FAILS`. A quota-skipped page raises none of the audit's replay signals, so the recovery job leaves it for the next scheduled leg after the GMT reset.

## 5. Edits (S3) — four anchored edits, each anchor asserted count==1
E1 version + WHY v6.61.0 block · E2 helpers block (7 defs + tag/selftest constants) before `_ohlc_prewrite_runlog_enabled` · E3 pre-fetch seam in `_run_one_task` · E4 post-fetch seam after the sentinel block.
Proofs: `py_compile` PASS; AST 279 → 286 names (+7, **0 removed**); **exactly one pre-existing def touched (`_run_one_task`)**; non-ASCII delta 0 (571 → 571); smart quotes 0; additions ASCII-only.

## 6. Harness / battery (S4) — REAL script, ×3 identical
`tests/test_sync_eodhd_quota_guard_p154d.py` T1–T8, the K-battery loader (real module, `scripts/` on sys.path), a local usage-endpoint emulator (`TFB_SYNC_EODHD_QUOTA_URL`) and the FW-3 Sheets boundary recorder; the REAL async `_run_one_task` is driven to the seam with a control-flow probe on `_read_symbols` (the first step after the guard):

| Leg | Result |
|---|---|
| T1 vocabulary / clamps / selftest `PASS 8/8` | ✓ |
| T2 pure decision matrix — pre: 30.2%/96.9% allow, 97.0% skip (`used>=97%`) / would_skip, 100% `exhausted`, ON_EXTRA skip unless allowed, every UNKNOWN shape allows; post on the REAL counts: 6,071/6,609 (the 00:54 replay) → `poison:6071/6609(91.9%)` skip, 0/6,609 (+43×404, the 08:17 leg) allow, 47/6,609 allow, 2,393/2,474 (the 20Z MF leg) would_skip | ✓ |
| T3 `_Run_Log` line shape (WARNING, page, SKIPPED/WOULD_SKIP, meta JSON with version/skip_pct), no line on allow, key never serialised, dead Sheets → annotated, not counted | ✓ |
| T4 REAL `_run_one_task`, enforce, emulator 100% → `status=skipped`, rows 0/0, note in warnings, one SKIPPED line, exactly one poll, `_read_symbols` never reached; 97.5% → skipped; 96.9% → proceeds; 401 from the endpoint (UNKNOWN) → proceeds, no note | ✓ |
| T5 observe at 100% → WOULD_SKIP line + note, leg proceeds | ✓ |
| T6 off at 100% → no poll, no line, no note, leg proceeds | ✓ |
| T7 decision-owned page (Top_10) → decision guard first, no poll | ✓ |
| T8 dual version: v6.60.0 base at 100% with the gate set → no guard, proceeds (control-flow parity with v6.61.0 off) | ✓ |

**8 passed ×3**; golden negative on the v6.60.0 base: 5 failed / 2 passed / 1 skipped. Existing `tests/test_sync_eodhd_quota_p154.py` (K1–K11) **passes ×3 on v6.61.0** (its `>= "6.60.0"` floor holds; the recovery guard's `[EODHD-QUOTA v[\d.]+]` regex is version-agnostic — verified at HEAD, no test loosening needed). Full sync batteries 9/9 ×3.

## 7. Deploy / read-back
No Render deploy (GitHub lane). Deploy read-back (gate unset): existing sentinel tags read `v6.61.0`, zero `[EODHD-QUOTA-GUARD]` lines. Arming = the separate YAML sitting (sheet attached): observe read-back = `[EODHD-QUOTA-GUARD v6.61.0] <page> | phase=… verdict=would_skip …` lines only when the counter is ≥ 97% / exhausted (on a healthy day: zero lines, and that is the positive read-back — the INFO allow lines are in the run log); enforce (separate sitting) = `SKIPPED` lines + `_Status` `leg=skipped` stamps on the affected pages and **zero fresh 402 rows in any export**.

## 8. Deliberate cuts / disclosures
- The pre-fetch poll costs one HTTP GET per ranked page per leg (4/run); the post-fetch leg costs nothing.
- A skipped page keeps its last-good rows and an old engine epoch; the feed withholds (truthful) and the cockpit's stability layer keeps its seats — the intended trade.
- Not built: symbol-subset replay, time-budget change (policy knob), backend-side 402 handling (the engine's false-green screen is untouched by design).

## 9. Register / next
P-154d BUILT — awaiting commit (3 files) + the YAML arming sitting. Still owed: v5.150.0 Render deploy proof + Slot A; the v1.0.7 acceptance commit sheet (404 at HEAD); P-168 needs the live 16_Decision source.
