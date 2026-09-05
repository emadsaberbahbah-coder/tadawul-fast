# Commit sheet — data_engine_v2 v5.138.0 (R-6c / P-83: Redis L2 for the fundamentals LKG) — 2026-09-05

| File | Repo path | Exists? | Delivered | Version |
|---|---|---|---|---|
| data_engine_v2.py | `core/data_engine_v2.py` | replace | SHA256 `9ff95024aad08e1b…` | v5.137.0 (`c2766bc30eb8a386`) → **v5.138.0** |
| test_recent_fixes.py | `tests/test_recent_fixes.py` | replace | SHA256 `ed33af7399bc9bc4…` | +3 tests (27 total) |

Repo copy of this sheet (optional, new): `docs/evidence/TFB_Commit_Sheet_data_engine_v2_v5.138.0_2026-09-05.md`.
**Commit message:** `engine v5.138.0: Redis L2 for the fundamentals LKG (R-6c, P-83) — default OFF`

## ⚠ ENV (surfaced first — Emad applies in Render; NOT needed for the commit)
- **Deploy is inert.** `TFB_ENGINE_FUND_LKG_REDIS` DEFAULT OFF → v5.137.0 byte-identical (no client constructed).
- **Arming (a separate evidence run, your GO):** Render Web → `TFB_ENGINE_FUND_LKG_REDIS=1`. Effective immediately (the fund-LKG master `TFB_ENGINE_FUND_LKG` is ON by default). Optional: `TFB_ENGINE_FUND_LKG_TTL_H=120` (5 trading days; default 72h). Reads the existing `REDIS_URL`.
- Read-back: `/health` → `engine_gates.engine_fund_lkg_redis: true`, `fund_lkg_redis_state: idle→ok`, `fund_lkg_redis_stats.writes` climbing on the first sync leg; after the NEXT restart the first degraded rows carry `fundamentals_lkg:<age>h` again (the count that went 320 → 0 today).
- Kill: unset/0.

## Evidence
Evening GM run 14:59–15:48Z: Yahoo enrichment ~3% during 15:00–15:30Z; `fundamentals_lkg` tags 320 (morning) → **0** (evening) because `_FUND_LKG_STORE` is per-worker memory and the service restarted four times today; with NULL-CLEAR scope=all the sheet then blanked Market Cap on 1,925 symbols, Target on 1,028; INVESTABLE 49 → 20.

## What changed (5 anchored edits, each matched exactly once; 375/378 defs byte-identical; 10 added; 0 removed; 3 modified additive-only)
- Mirror of the v5.136.0 target-LKG L2: `_FUND_LKG_REDIS_*` state, `_fund_lkg_redis_{enabled,url,key,state_label,stats,note_error,note_ok,client,set,get}`; key `tfb:fund_lkg:v1:<SYMBOL>`, SETEX ttl = `_fund_lkg_ttl_h`, payload `{ts,name,fields}` (whitelisted fields only); 250 ms timeouts; breaker 3 errors → memory-only 300 s.
- `_fund_lkg_capture`: write-through after the in-memory write. `_fund_lkg_restore`: on an in-memory miss, L2 GET (schema-checked: ts>0, whitelisted non-missing fields, ≥1 anchor) → hydrate memory → the UNCHANGED TTL / min-fields / fill-only guards decide.
- `surface_gate_states` (health/boot banner): `engine_fund_lkg_redis`, `fund_lkg_redis_state`, `fund_lkg_redis_stats`.
- NOT changed: taint rules, min-fields, the 24-field whitelist, anchor rule, tag text, target LKG, providers, SAI contract.

## Verification (real module)
- py_compile / compileall OK · AST: capture/restore/surface_gate_states additive-only.
- Harness with an injected fake Redis: OFF → no client, writes 0 · ON → capture SETEX `tfb:fund_lkg:v1:HCI.US` ttl 259,200 s, 13 fields · cold store → restore returns `fundamentals_lkg:0h`, 13 fields back, memory rehydrated · expired L2 entry (80 h) refused · anchor-less payload refused · 3 errors → `breaker`, capture keeps working memory-only.
- New tests: **fail 3/3 on v5.137.0, pass 3/3 ×3 on v5.138.0**; lean CI **241 passed**; `tests/test_data_engine_v2.py` 39 passed.

## Post-commit checks (Emad)
1. `__version__ = "5.138.0"` on main; Render auto-deploy; `/health` shows `engine_fund_lkg_redis: false`, `fund_lkg_redis_state: off` (inert).
2. Arm on an evidence run of your choosing (tomorrow's plan slot is fine): `TFB_ENGINE_FUND_LKG_REDIS=1` → `/health` state `idle` → after one sync leg `writes` > 0 → after the next restart `hits` > 0 and the `fundamentals_lkg:<age>h` tags reappear on degraded rows.
