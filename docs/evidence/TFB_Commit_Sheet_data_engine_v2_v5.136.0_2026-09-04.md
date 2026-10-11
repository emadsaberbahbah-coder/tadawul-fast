# COMMIT SHEET — core/data_engine_v2.py v5.136.0 (R-6b: target-LKG Redis L2)

Date: 2026-09-04 (Friday, evening session) · Builder: Claude · Executor: Emad (GitHub web UI)

## 1. Target
| Item | Value |
|---|---|
| Repo | `emadsaberbahbah-coder/tadawul-fast`, branch `main` |
| **Exact repo path** | `core/data_engine_v2.py` (full-file replace) |
| Base pinned | HEAD `199327c613ec468c27c611b76eeb0049e485ff33`, blob `881a5903cac54b845699192f8a499036ac314944`, 16,701 lines, 861,391 bytes, `__version__ = "5.135.0"` |
| Delivered | blob `83d676306e1521b165f5b1e7eabd3c065e371c3c`, sha256 `b8b57231ef1a3b01…`, 16,918 lines, 870,693 bytes, `__version__ = "5.136.0"` (line 3363) |
| Net diff | +217 lines added, 1 line replaced (the version string), 0 lines removed |
| Commit message | `data_engine_v2 v5.136.0 — R-6b target-LKG Redis L2 (default OFF, byte-identical when off)` |

## 2. What changed (additions only)
| # | Anchor (count == 1 verified) | Edit |
|---|---|---|
| E1 | `__version__ = "5.135.0"` | v5.136.0 changelog block (WHY/FIX/ENV/NOT CHANGED) + version bump |
| E2 | `def _tgt_lkg_names_compatible(` | 5 module constants (`_TGT_LKG_REDIS_*`, `_TGT_LKG_REDIS_STATE`) + 10 helpers: `_tgt_lkg_redis_enabled/_url/_key/_state_label/_stats/_note_error/_note_ok/_client/_set/_get` |
| E3 | `_TGT_LKG_STORE[s] = entry` (inside `_tgt_lkg_capture`) | write-through `_tgt_lkg_redis_set(s, entry)` after the in-memory write |
| E4 | `entry = _TGT_LKG_STORE.get(s)` (inside `_tgt_lkg_restore`) | on in-memory miss: `_tgt_lkg_redis_get(s)` → hydrate memory → fall through to the UNCHANGED TTL/taint/identity guards |
| E5 | `"engine_target_klg": _tgt_lkg_enabled(),` (boot banner `surface_gate_states`) | + `engine_target_klg_redis`, `tgt_lkg_redis_state`, `tgt_lkg_redis_stats` |

Design: key `tfb:tgt_lkg:v1:<SYMBOL>`, `SETEX` ttl = `_tgt_lkg_ttl_h()`; lazy client, 250 ms connect/socket timeouts, `decode_responses=True`; circuit breaker 3 consecutive errors → memory-only 300 s; counters hits/misses/writes/errors/breaker_trips; local `import json` / `import redis` per module convention; every failure path degrades to v5.131.0 memory-only behaviour; nothing raises into the seam.

NOT CHANGED: `_phase_ii_quality_forecast` seam logic, restore guards, reliability formula, entry schema, TTL default (72 h), fund-LKG, providers, SAI contract.

## 3. Proofs
| Gate | Result |
|---|---|
| `py_compile` | OK |
| AST zero-removal | functions 444 → 454 (removed **none**, added 10); classes 9 → 9; module assigns 224 → 229 (removed none). Top-level bodies changed: `_tgt_lkg_capture`, `_tgt_lkg_restore`, `surface_gate_states` — insertions only. Unified diff removed lines: only `__version__ = "5.135.0"` |
| Real-module harness (`harness_r6b_v5_136_0.py`, REAL `core.data_engine_v2` import, real `redis-server` 7.x on :6390) | **28 checks × 3 runs = 84/84 PASS** |
| Lean CI (`tests/test_data_engine_v2.py`, `test_identity_guard.py`, `test_scoring_engine_contract.py`, `test_quality_gates.py`, `test_recent_fixes.py`, `test_critical_symbol_identity.py`, `test_sai_falsegreen_fixtures.py`) | **154 passed / 0 failed** on v5.136.0 — identical result on base v5.135.0 |

Harness coverage:
- **H1** layer OFF (default): no client constructed, `state=off`, capture/restore byte-identical memory-only; master gate OFF → capture/restore return False (dead code preserved).
- **H2A/H2B** cross-process: process A captures → key present, TTL = 604,800 s (168 h), schema `{ts,name,fp12,tmp}`; fresh process B (empty memory) restores via L2 with tag `analyst_lkg:0h`, `hits=1`; identity guard still refuses a different name; **the REAL seam `_phase_ii_quality_forecast` restores through L2** (`forecast_source=provider_target`, `fp12=123.4`, `expected_roi_12m=0.234`); unknown symbol → clean miss.
- **H3** dead port: 3 errors → breaker open (`trips=1`, client dropped), capture still True, restore memory-only, no further attempts while open, one retry after the window, banner shows `degraded` + counters.
- **H3B** non-routable host: 3 attempts complete in 0.75 s (timeout bound holds), breaker opens.

## 4. ENV (Render Web `tadawul-fast-bridge`) — applied by Emad, in this order
| Step | Variable | Value | Note |
|---|---|---|---|
| with the deploy (inert) | `TFB_ENGINE_TARGET_KLG_REDIS` | `1` | new in v5.136.0; default `0` = byte-identical |
| with the deploy (inert) | `TFB_ENGINE_TARGET_KLG_TTL_H` | `168` | spec R-6 max age 5 trading days |
| prerequisite | `REDIS_URL` | must already exist | absent → banner `nourl`, memory-only, never fails |
| **the one arming, own evidence run** | `TFB_ENGINE_TARGET_KLG` | `1` | spelled **K-L-G**; kill-switch `0` (restores v5.130.3 seam behaviour) |

IR-020 guard: values must be exactly `1` / `168`; any other non-empty string collapses silently to OFF.

## 5. Read-back proof (mechanism verdict — valid on a manual run)
1. Render log/`/health` banner after deploy: `engine_target_klg_redis: true`, `tgt_lkg_redis_state: idle` (before first command) → `ok` after the first sync; `tgt_lkg_redis_stats.writes` climbing.
2. After the master arming: rows whose target leg failed carry `analyst_lkg:<age>h` in Warnings while keeping `forecast_source=provider_target`; the GM↔MP forecast-source split for the same symbol shrinks. Instrument: W1B-1 `provider_target_coverage` workflow before/after.
3. Distribution baseline (share of `phase_ii_synthetic` in GM) — scheduled-run-only, per the MECHANISM ≠ DISTRIBUTION rule.

## 6. Post-commit verification (Emad → Claude)
- [ ] GitHub shows exactly one changed file: `core/data_engine_v2.py`; **no unexpected new files** (do not commit the harness — keep `scripts/` clean; it is evidence only).
- [ ] Blob SHA on main == `83d676306e1521b165f5b1e7eabd3c065e371c3c` (Claude re-fetches raw with cache-bust and verifies).
- [ ] Render deploy succeeds; banner keys present.
- [ ] Only then: master arming on its own evidence run.

## 7. Register (not in this build)
- Builder-side NO_NEW_MONEY for rows tagged `analyst_lkg:` — W1B-1 spec: a held target may preserve a seat, never fund a new one (`opportunity_builder`, build #2 candidate).
- Record correction: R-6 was implemented on 2026-08-21 (v5.131.0), never armed; the Register entry "R-6 pending build" is closed by this sheet.
