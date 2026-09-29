# TFB Commit Sheet — main.py v8.14.0 [/health TRUTH: pf_gates + deploy provenance]

Date: 2026-09-29 (Tuesday) · Lane: Render/Python (service entrypoint) · Build #3 of the day · Protocol: One-Pass

## S0/S1 — Base pin
| Item | Value |
|---|---|
| Base | `main.py` v8.13.2 at HEAD `5b1ea5e` (#623) — file unchanged since the IR-096 fix (2026-08-23) |
| Base SHA-256 | `main_base_v8.13.2.py` = the HEAD bytes (2,593 lines) |
| Live proof of the gap | Render deploy log 2026-09-29 13:14 Riyadh + `/health` 13:29: `engine_gates` shows the engine ENVs, but neither the portfolio_actions/opportunity_builder gate ENVs (`TFB_PF_CONFIRM_SESSION`, `TFB_FORECAST_BASIS`, …) nor the running commit appear anywhere — the arming of the day could not be verified from the paste, and the deploy that wiped the fund cache (`fund_cache_stats` all zero) could not be tied to a commit |

## S2 — Root
`_runtime_meta()` reports the engine's boot-line gates via `_engine_gates_snapshot()`, but the two decision modules read their ENVs **per call** and print no boot line, so their state was never on the payload; nothing on the payload names the deployed commit or the worker boot time. Every prior additive key (engine_version 8.11.3, global_auth_enforcement 8.13.0, engine_gates 8.13.1/8.13.2) shipped for exactly this reason: "one browser hit on /health is the acceptance test".

## S3 — Change (7 anchored edits, each `count == 1`)
| # | Site | Edit |
|---|---|---|
| E1 | docstring | title `(v8.14.0)` + "Why this revision (v8.14.0 vs v8.13.2)" block |
| E2 | version block | v8.14.0 note + `APP_ENTRY_VERSION = "8.14.0"` (SERVICE_VERSION follows) |
| E3 | after `SERVICE_VERSION` | `_PROCESS_BOOT_UTC` — per-worker boot stamp (module import time, UTC) |
| E4 | before `_runtime_meta` | `_PF_GATE_ENVS` (21 names), `_OB_GATE_ENVS` (5), `_DEPLOY_ENVS` (6); helpers `_env_or_unset` (raw stripped value or the literal `"unset"`), `_module_version_no_import` (sys.modules read, never imports), `_pf_gates_snapshot()`, `_deploy_provenance()` (RENDER_* + `worker_pid` + `worker_boot_utc`) |
| E5 | `_runtime_meta` return | two additive keys after `engine_gates`: `"pf_gates"`, `"deploy"` |

Contract: additive only; fail-open at the leaf (a raising `os.getenv` degrades every value to `"unset"`, the payload never breaks); `_PUBLIC_STATUS_META_KEYS` (the anonymous reduced view) untouched, so the new keys appear only where the full payload already does; no route, auth, middleware, mount-plan or engine-lifecycle change; no ENV to arm. Rollback = `git revert`.

Payload shape:
```
"pf_gates": {"portfolio_actions_version": "1.13.0", "opportunity_builder_version": "1.22.1",
             "portfolio_actions": {"TFB_PF_CONFIRM_SESSION": "observe"|"unset"|…, … 21 keys},
             "opportunity_builder": {"TFB_FORECAST_BASIS": …, … 5 keys}},
"deploy":   {"render_git_commit": "<sha>", "render_git_branch": "main", "render_git_repo_slug": …,
             "render_service_id": …, "render_service_name": …, "render_instance_id": …,
             "worker_pid": 111, "worker_boot_utc": "2026-09-29T10:14:22.3+00:00"}
```

## S4 — Audits (×3 identical)
| Check | Result |
|---|---|
| Delivered SHA-256 | `5c94e4a71ce5f06753bf1ade7af4ebed318976dfd75f4b9be27f1886af90cd0d` (2,709 lines) |
| `py_compile` | PASS |
| AST | functions/classes 93 → 97 (+`_env_or_unset`, `_module_version_no_import`, `_pf_gates_snapshot`, `_deploy_provenance`; **0 removed**) |
| Line audit | 2 base lines not verbatim = the docstring title + the version constant; every WHY block carried |
| Non-ASCII | multiset identical to base (11 lines; 0 new); 0 smart quotes |
| Harness `tests/test_main_health_pf_gates_v8140.py` (REAL delivered `main.py` imported in-process — app created, 6 route modules mounted — plus the REAL base in a subprocess via `MAIN_BASE`, same ENV) | **27/27 PASS ×3, digest `4112ee696e2f` ×3** |
| H2 | base payload keys all present and equal (timestamp/version masked); exactly two additive keys `['deploy','pf_gates']` |
| H3 | 21 + 5 gate names; explicit `observe`/`off` values read; unset → `"unset"`; whitespace-only → `"unset"`; module versions `1.13.0` / `1.22.1` resolved from sys.modules |
| H4 | RENDER_* set → echoed, unset → `"unset"`; pid int; boot stamp ISO-UTC and stable within a process |
| H5 | raising `os.getenv` → full shape with every value `"unset"`; missing module → `""` |
| H6 | `_StrictJSONResponse.render` (allow_nan=False) accepts the payload |
| H7 | `_PUBLIC_STATUS_META_KEYS` unchanged |

## S5 — Delivery
| File | Destination |
|---|---|
| `main.py` | repo (full file) |
| `tests/test_main_health_pf_gates_v8140.py` | repo `tests/` (set `MAIN_BASE=<v8.13.2 file>` for the dual-tree legs) |
| `docs/evidence/TFB_Commit_Sheet_main_v8.14.0_2026-09-29.md` | repo `docs/evidence/` |

## S6 — Deploy / read-back
Commit → Render deploy (the deploy itself is the cost: it restarts both workers and wipes the fund cache again — pair it with the next ENV change you intend anyway, or take it at a quiet slot before a full sync). Read-back = `/health` shows `"entry_version":"8.14.0"`, `pf_gates.portfolio_actions.TFB_PF_CONFIRM_SESSION` = whatever is armed (`"observe"` if today's Render item was done, else `"unset"`), and `deploy.render_git_commit` = the merge SHA; two worker pids answer alternately with the same `worker_boot_utc` minute. Startup warnings must stay `[]`.
