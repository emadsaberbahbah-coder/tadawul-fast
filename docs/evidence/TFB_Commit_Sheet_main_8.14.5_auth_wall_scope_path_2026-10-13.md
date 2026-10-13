# TFB Commit Sheet - main.py 8.14.5: the global auth wall decides on the routed path (SECURITY)

Date: 2026-10-13 (Tuesday). Base: branch claude/auth-path-scope-2026-10-13 at 0fc8d6c0 (the stacked dt10-cohort-binding head: main.py 8.14.4, ci.yml v1.1.17, verify_deployment 1.0.58). main.py was also compared against dda1211 (8.14.3), whose wall code is identical. Lane: Render/Python service entrypoint, plus one verifier pin, one CI enrolment and one new harness. main.py 8.14.4 -> 8.14.5, verify_deployment 1.0.58 -> 1.0.59, ci.yml v1.1.17 -> v1.1.18. A review-fix follow-up commit on top of 9a73f48c (the 8.14.5 commit) closes the same hole in the route-layer auth: see Review fixes at the end.

## Why

GlobalAuthEnforcementMiddleware (main.py v8.13.0 to v8.14.4) let a request skip the global token wall when `str(request.url.path)` was in `_GLOBAL_AUTH_EXEMPT_PATHS` (the probe/meta paths: /, /health, /livez, /readyz, the /v1 aliases, docs and /_debug/routes).

In Starlette 0.41.3 `request.url` is not the routed path. `URL(scope=...)` builds the string `f"{scheme}://{host_header}{scope['path']}"` from the raw Host header and re-parses it with urlsplit. The router matches `scope["path"]` (after `get_route_path` strips root_path). A Host header that carries "/", "?" or "#" moves the start of the parsed path into the Host:

- Host `x/health?x=` with request target `/v1/advanced/sheet-rows` gives the URL `http://x/health?x=/v1/advanced/sheet-rows`, so `request.url.path == "/health"`, and the router still dispatches to /v1/advanced/sheet-rows.
- The wall exempted that request, so an anonymous caller passed it.

Production /health shows `global_auth_enforcement: true`, so the wall is up and this hole is live in the application. Whether Render's edge forwards such a Host header to the service was not tested; no request was sent to Render.

The per-route auth did not stop the request either. Five of the six mounted route modules check their own public-path lists against the same `request.url.path` (details under Audit). On the base, the crafted Host therefore passed both layers.

## Evidence

All runs are offline, with /home/user/tfb-venv/bin/python 3.11.17 (FastAPI 0.115.14, Starlette 0.41.3, uvicorn 0.32.1, httptools 0.6.4, h11 0.16.0). The base copy is `git show 0fc8d6c0:main.py`; the dda1211 copy is `git show dda1211:main.py`.

1. Bypass on the base, through the real app (TestClient; wall armed: REQUIRE_AUTH=true, OPEN_MODE=false, APP_TOKEN set). From the harness W12 case:

       base 8.14.4: Host 'testserver/health?x=' -> /v1/schema/pages 200 (non-401), tripwire REACHED;
       W3 FAILs there (490/558 crafted requests passed the wall, 68 walled with a Host-shaped path)

2. The server does not clean the Host. Raw bytes were fed to uvicorn 0.32.1's HttpToolsProtocol and H11Protocol with a fake transport (no socket). The ASGI scope the app receives keeps the crafted Host, and uvicorn decodes %3F in the path:

       httptools ... {"headers": [["host", "x/health?x="]], "path": "/v1/advanced/sheet-rows", ...}
       h11       ... {"path": "/v1/advanced/sheet-rows", ..., "headers": [["host", "x/livez#"]]}
       httptools ... {"headers": [["host", "tfb.example"]], "path": "/health?", "raw_path": "/health%3F"}

3. Which mounted routes rely only on the wall. With the wall dropped (TFB_GLOBAL_AUTH_ENFORCE=0), REQUIRE_AUTH on and a normal Host, 100 route signatures were sent anonymous GET/POST requests:
   - 54 answer 401 from their own route auth.
   - 13 are the meta routes.
   - 33 answer 200 and are protected only by the wall: /v1/config, /v1/config/, /v1/config/health; /v1/schema and /schema with /pages, /sheet-spec (GET and POST), /data-dictionary, /provider-health and /v1/schema/health; /v1/analysis/health; /v1/advanced GET and POST, /v1/advanced/health, /meta and /metrics; /v1/advisor/ping, /healthz, /health, /meta and /metrics; the three /v1/enriched*/health and /headers families.
   - POST /v1/advanced is the most sensitive of these: it runs the Top-10 build through the bridge, and routes/investment_advisor lists "/v1/advanced" as public.

4. The per-route auth can be spoofed the same way. The auth functions were called directly on Starlette Request objects (no handler ran), with REQUIRE_AUTH on and no token:

       Host 'testserver'            -> investment_advisor False, analysis_sheet_rows False, advanced_analysis False, config False
       Host 'testserver/health?x='  -> investment_advisor True,  analysis_sheet_rows True,  advanced_analysis True,  config True
       Host 'h/static?'             -> all four True (core.config's public prefix /static)
       enriched_quote.auth_guard: 401 with a normal Host, passes with 'testserver/health?x='
       advisor._require_auth_or_401: 401 with both (it never passes a path)
       core.config.auth_ok(request=r): False with a normal Host, True with the crafted Host

## What changed

1. main.py 8.14.4 -> 8.14.5, with a dated WHY in the module docstring, a note at APP_ENTRY_VERSION and a v8.14.5 paragraph in the middleware docstring.
   - New `_request_scope_path(request)` returns the ASGI scope path, or "" (fail-closed) when there is none. It never raises.
   - New `_scope_route_path(scope_path, root_path)` repeats the steps of Starlette 0.41.3 `get_route_path`. It is kept local, and the harness checks it against Starlette's own function.
   - New `_global_auth_exempt_request(request)` is True only when the scope path AND the path the router matches after its root_path step are both exempt. Any error returns False.
   - The wall now exempts with `_global_auth_exempt_request(request)` instead of `str(request.url.path) in _GLOBAL_AUTH_EXEMPT_PATHS`.
   - The wall's 401 body "path" now echoes the scope path.
   - The four NoResponseGuard "path" fields (log extra and body) use `_request_scope_path`. The nested unhandled-exception handler reads the scope path inline (two fields), so that it stays self-contained; tests/test_redaction_boundaries.py runs that handler in a fixed namespace.
   - `_call_auth_ok_flexible` hands config.auth_ok the scope path. core.config.auth_ok checks its public-path list against that value whenever main falls back to it.
   - main.py has no `request.url` read left (AST check, harness W1).
   - Unchanged: `_global_auth_enforcement_active` (REQUIRE_AUTH, OPEN_MODE, TFB_GLOBAL_AUTH_ENFORCE), the exempt set, OPTIONS handling, the fail-closed `except`, middleware order and all route modules.
2. New tests/test_auth_wall_scope_path.py (HARNESS_VERSION 1.0.0, dated WHY). It runs as a script ("PASS k/k", exit 0/1) and under pytest (one parametrized test per case).
   - It loads the real main.py in a subprocess with the wall armed by ENV and drives it with TestClient. Data routes only ever reach a tripwire: the module's real GlobalAuthEnforcementMiddleware wrapped around a stub that answers "REACHED".
   - The subprocess blocks Python sockets and points every proxy variable at 127.0.0.1:9.
   - Cases W1 to W13 are listed in its docstring. W9 (dual-tree identity), W12 (golden negative) and W13 (zero removals) need `TFB_AUTH_WALL_BASE` (the pre-change main.py, or a directory holding it) and skip when it is unset.
3. scripts/verify_deployment.py 1.0.58 -> 1.0.59 (dated WHY): MODULES pins main APP_ENTRY_VERSION 8.14.5. This is a pin only; no FLAGS entry and no verifier logic changed.
4. .github/workflows/ci.yml v1.1.17 -> v1.1.18 (header block with a dated WHY). The harness joins the blocking contract job's pytest list, before tests/test_route_mount_compat.py.
   - Why the contract job: it installs FastAPI 0.115.14, Starlette 0.41.3 and httpx 0.27.2 at the production pins, which the harness needs. The lean job installs numpy and pytest only.
   - The push paths already cover main.py, scripts/** and tests/**.
5. This sheet.

## Why it is inert for valid traffic (and why there is no ENV gate)

A valid Host is `uri-host [ ":" port ]`: a reg-name (unreserved, pct-encoded, sub-delims), an IPv4 address, or a bracketed IPv6 / IPvFuture literal, in any case. It cannot contain "/", "?" or "#". urlsplit therefore keeps the whole Host in the netloc, and the parsed path is exactly `scope["path"]`. The one exception is that urlsplit truncates the path at "?" or "#" and drops TAB, LF and CR.

So, for a valid Host:
- The old and new decisions are identical for every decoded path without those five characters. The 401 "path" field is byte-identical too.
- Decoded paths with those characters, for example `/health%3F` or `/%3F`, were exempted on the truncated url.path, yet no meta route serves them (the router 404'd them). They now get the same 401 as any other unknown path.

The proof is in W8 and W9:
- W8 checks 34,132 valid-Host Request objects in process, and `_scope_route_path` against Starlette `get_route_path` in 3,904 cases.
- W9 runs 1,023 requests through the old and new walls and both real apps.

Requests that change behaviour:
- The crafted-Host bypass.
- Paths no route serves.
- An unparseable Host such as an unbalanced "[". Before, `request.url` raised inside the wall and again in the 401 builder, so the request got a bare 500 even on /health. Now it gets the normal answer: the JSON 401 when anonymous, the route when exempt or when a token is sent.
- Operator-set root_path values that would route an exempt scope path to a non-meta route. Production sets no root_path (start_web.sh: gunicorn + UvicornWorker, no --root-path).

An off-by-default ENV gate would leave the bypass open in production, and it would add a flag only to keep a security defect available. TFB_GLOBAL_AUTH_ENFORCE keeps its exact semantics (W7, and W9 against the base for 11 values).

## Measured results with exact commands and outputs

B=<scratchpad>/build/auth-path/main_base_v8.14.4.py (`git show 0fc8d6c0:main.py`); D=<scratchpad>/build/auth-path/main_dda1211_v8.14.3.py (`git show dda1211:main.py`).

`python tests/test_auth_wall_scope_path.py`:

    PASS W1 v8.14.5, helpers present, no request.url reads | v8.14.5; helpers ['_global_auth_exempt_request', '_request_scope_path', '_scope_route_path']; 0 request.url reads; verify_deployment pin matches
    PASS W2 anonymous protected requests get 401 | 17/17 anonymous protected requests get the wall's 401 with the routed path
    PASS W3 crafted Host and decoded-path shapes still get 401 | 558/558 crafted Host / decoded-path requests get 401 (routed path echoed)
    PASS W4 exempt paths stay exempt | 17 exempt paths pass the wall (real app statuses [200, 401, 404])
    PASS W5 valid token passes, wrong token 401 | X-APP-TOKEN, Bearer and X-API-Key pass (normal and crafted Host); a wrong token gets 401
    PASS W6 OPTIONS preflight unchanged | OPTIONS passes the wall (real app answers 405 without CORS configured)
    PASS W7 kill switch, OPEN_MODE and REQUIRE_AUTH semantics | falsy 5 values drop the wall, 6 others keep it; OPEN_MODE=true and REQUIRE_AUTH=false keep it down
    PASS W8 valid-Host decision identity (in process) | 34132 valid-Host requests: old and new decisions identical, url.path == scope path; _scope_route_path == get_route_path in 3904/3904
    SKIP W9 dual-tree identity against the base | TFB_AUTH_WALL_BASE unset
    PASS W10 root_path follows the router | root_path follows the router: /api/health walled, /v1/health under root /v1 exempt, /_debug/routes under root /_debug walled (routed to /routes)
    PASS W11 unparseable Host fails closed without a crash | Host '[' -> JSON 401 with the routed path; a valid token still passes
    SKIP W12 golden negative on the base | TFB_AUTH_WALL_BASE unset
    SKIP W13 zero removals against the base | TFB_AUTH_WALL_BASE unset
    PASS 10/10 (3 skipped: TFB_AUTH_WALL_BASE unset)

W4's real-app 401 is /_debug/routes answering with its own production 401 (no "path" key), and its 404s are the docs paths, which are off in production. Neither is the wall.

`TFB_AUTH_WALL_BASE=$B python tests/test_auth_wall_scope_path.py` and `TFB_AUTH_WALL_BASE=$D ...` both end in `PASS 13/13`. With D the dual-tree, golden-negative and zero-removal lines read:

    PASS W9 dual-tree identity against the base | 1023/1023 valid-Host requests, kill-switch values, modes and root_paths identical to 8.14.3
    PASS W12 golden negative on the base | base 8.14.3: Host 'testserver/health?x=' -> /v1/schema/pages 200 (non-401), tripwire REACHED; W3 FAILs there (490/558 crafted requests passed the wall, 68 walled with a Host-shaped path)
    PASS W13 zero removals against the base | 96 -> 99 defs: 0 removed, added ['_global_auth_exempt_request', '_request_scope_path', '_scope_route_path']

Each case evaluated directly on the base results (B):

    W1  FAIL helpers missing ...; APP_ENTRY_VERSION '8.14.4' != '8.14.5'; request.url still read at lines [1080, 1359, 1373, 2678, 2689, 1296, 1307, 1319, 1330]
    W2  PASS   W4 PASS   W6 PASS
    W3  FAIL 490/558 crafted requests passed the wall, 68 walled with a Host-shaped path
    W5  FAIL wrong token crafted not walled
    W7  FAIL (a crafted Host passes the wall at every truthy kill-switch value)
    W8  FAIL the module under test has no _global_auth_exempt_request
    W10 FAIL debug_split: reached != walled; equal_root: reached != walled
    W11 FAIL tripwire anon: [500, '']; tripwire token: [500, '']; real anon: 500

Under pytest: `python -m pytest -q tests/test_auth_wall_scope_path.py` gives "10 passed, 3 skipped in 17.54s"; with `TFB_AUTH_WALL_BASE=$B` it gives "13 passed in 32.66s".

The ci.yml contract step, extracted from the YAML and run locally with the pinned stack (proxies pointed at a closed local port): "249 passed, 5 skipped in 68.49s". The same step on the 0fc8d6c0 tree gives "239 passed, 2 skipped"; the difference is this harness (10 passed, 3 skipped).

Related suites, `python -m pytest -q` over test_main_health_pf_gates_v8140, test_no_import_side_effects, test_verify_manifest_pins, test_render_env_contract, test_ci_required_verdict, test_runner_pin_ubuntu2404, test_repository_workflow_audit, test_verify_optional_rollout_flags and test_flag_registry: "52 passed, 4 skipped". Also:
- `python tests/test_verify_manifest_pins.py`: "[MANIFEST PIN GUARD] 3/3 PASS".
- `python scripts/render_env_contract.py --check`: "OK ... is current", rc 0.

Whole-suite discovery: see "Whole suite" below.

Static checks:
- `py_compile` of main.py, verify_deployment.py and the harness: clean.
- `ruff 0.16.8 check --select E9,F63,F7,F82 .`: "All checks passed!".
- `ruff check --select F,E9` on main.py and the harness: "All checks passed!".

Zero removals:
- main.py defs and classes: 96 -> 99, 0 removed (W13, against both B and D). The 12 deleted diff lines in main.py are the title, the version constant, one docstring closing line and the nine `request.url` reads replaced in place.
- verify_deployment.py: two lines changed in place (SCRIPT_VERSION, the main pin); no def changed.
- ci.yml: 0 lines removed. With the added line taken out, the parsed YAML equals the base (yaml.safe_load: "added line present once: True; contract run equal without the line: True; everything else equal: True").

ASCII: every added line is ASCII. The only exception in the diff is the SCRIPT_VERSION line of verify_deployment.py: just its "1.0.58" -> "1.0.59" changed, and the line's pre-existing trailing history comment, carried over unchanged, contains em dashes.

## Whole suite

`python -m pytest -q tests` on this tree, with every proxy variable pointed at 127.0.0.1:9 and NO_PROXY empty: "3 failed, 4873 passed, 31 skipped, 3 warnings, 243 subtests passed in 480.39s".

The three failures are tests/test_sync_eodhd_quota_guard_p154d.py t4 and t5 and tests/test_sync_eodhd_quota_p154.py. Each starts a loopback HTTPServer, which the empty NO_PROXY sent to the dead proxy.
- Under that same environment they fail identically on the 0fc8d6c0 tree.
- With NO_PROXY=127.0.0.1,localhost they pass on this tree ("8 passed, 1 skipped").

None of them imports main.py.

## Audit of the other Host- and URL-derived uses

main.py, all fixed in this commit:
- The wall exemption: the defect.
- The wall's 401 "path", the NoResponseGuard "path" (log and body) and the unhandled-exception "path" (log and body): echoes only. They now carry the routed path, so logs can no longer show "/health" for a request that hit a data route, and an unparseable Host no longer crashes the error paths.
- `_call_auth_ok_flexible` path kwarg: root config.auth_ok ignores it, but core.config.auth_ok (main's fallback) checks it against its public-path list.

main.py, nothing to change:
- Docs gating is fixed at app construction from settings (docs_url, redoc_url and openapi_url), not from the request.
- There is no inbound rate limiter: MAX_REQUESTS_PER_MINUTE is a setting that nothing enforces.
- main.py issues no redirects.
- _request_id_from_request reads headers only.

Framework:
- Starlette's redirect_slashes builds its Location from the Host header (`URL(scope=redirect_scope)`). That only reflects the caller's own Host back to the caller. With the wall up, an anonymous caller gets the wall's 401 before the router can redirect.
- `--forwarded-allow-ips "*"` in scripts/start_web.sh lets any client set X-Forwarded-For/Proto. Nothing in main.py or routes/ keys on client IP or scheme.

routes/, NOT changed in the 8.14.5 commit, all reading `getattr(request.url, "path")` there. The review-fix follow-up commit fixes the five mounted modules and core/config.py (see Review fixes); the three unmounted modules are unchanged:
- routes/investment_advisor.py `_auth_passed` (L1214-1218) and its `_is_public_path`, which includes /v1/advanced, /v1/advanced/health, /meta and /metrics.
- routes/analysis_sheet_rows.py `_auth_passed` (L1012-1013).
- routes/advanced_analysis.py `_auth_passed` (L2246, passed as `path` to core.config.auth_ok).
- routes/config.py `_auth_ok` (L322).
- routes/enriched_quote.py `_Service.auth_guard` (L1005).
- routes/data_dictionary.py (L515, L530) and routes/routes_argaam.py (L336): optional modules, not mounted (main.py mounts only `_CONTROLLED_ROUTE_PLAN`). Unchanged by the follow-up.
- routes/advanced_sheet_rows.py (L823-824): not mounted. Unchanged by the follow-up.
- core/config.py auth_ok reads `request.url.path` first (L2002) when no path is given.
- routes/advisor.py passes no path, so it cannot be spoofed.

With the wall up, none of these is reachable by an anonymous caller after this commit.

## Known limits

- With the 8.14.5 commit alone the route layer stays Host-spoofable: only the wall protects business routes from a crafted Host. With TFB_GLOBAL_AUTH_ENFORCE falsy, REQUIRE_AUTH on and OPEN_MODE off, the per-route auth is the only check, and on the base it let a crafted Host through to /v1/advanced/sheet-rows. The review-fix follow-up commit closes this for the five mounted modules and core.config (see Review fixes). Dropping the wall still opens the wall-only routes (Evidence 3: 33 signatures, POST /v1/advanced included) to anonymous callers, so the kill switch stays an emergency control.
- Render's edge was not tested. Whether the edge forwards a Host such as `x/health?x=` to the service is unknown: no request was sent to Render.
- The harness proves the decision against Starlette 0.41.3, the production pin. `_scope_route_path` is a local copy of `get_route_path`. W8 compares it with Starlette's function and reports, without failing, when that function is unavailable, as it would be in an unpinned future stack.
- W9, W12 and W13 need a pre-change copy and skip in CI, like every other golden negative in this repo.
- Reconnaissance note. One scratch probe, not the harness, sent an anonymous request with the wall off and a crafted Host to /v1/advanced/sheet-rows. The spoofable route layer let it through to the engine, and yfinance (libcurl, outside Python's socket module) tried fc.yahoo.com. The session's egress proxy rejected the CONNECT and the process was stopped at once. After that, every run points HTTP(S)_PROXY and ALL_PROXY at 127.0.0.1:9 as well as blocking Python sockets, and the harness never sends a data route to the real app.

## Operator steps (including rollback)

1. Review and merge. No ENV change is needed. The push run of ci.yml runs the harness in the contract job.
2. After Render deploys, check with one GET /health that entry_version reads 8.14.5 and global_auth_enforcement reads true. Run verify_deployment: main 8.14.5 should report OK; until the deploy lands it reports BEHIND, the normal manifest-ahead state.
3. Do not test the bypass against production from a browser or script. The harness is the acceptance test.
4. Keep TFB_GLOBAL_AUTH_ENFORCE unset (or truthy). See Known limits.
5. Rollback: `git revert` this commit. main.py returns to 8.14.4, verify_deployment to 1.0.58 and ci.yml to v1.1.17, and the harness is removed. Rolling back reopens the bypass. Revert the review-fix follow-up commit first (see Review fixes).

## Owner decisions

- Fix the route layer: the five mounted modules plus core/config.py `auth_ok` should read the scope path, with version bumps, verifier pins and a golden negative of their own. This commit deliberately stays main.py-only. Done in the review-fix follow-up commit (see Review fixes).
- Whether routes/investment_advisor should keep "/v1/advanced" (POST runs the Top-10 build) on its route-level public list. It is protected only by the wall today.
- Whether to ask Render support (or test on a staging service) whether the edge forwards Host values that contain "/", "?" or "#".

## Review fixes (follow-up commit, 2026-10-13)

Base: 9a73f48c, the 8.14.5 commit above and the branch head before this fix (its routes/ and core/ are identical to 0fc8d6c0). Lane: route-layer auth and core.config, one verifier pin pair, two in-place test pins and harness 1.1.0. main.py and ci.yml are not changed.

### Finding (minor): the route-layer auth still read the Host-derived path

Re-verified offline on 9a73f48c. The auth functions were called directly (no handler ran), with REQUIRE_AUTH on and no token:

    Host 'testserver'             -> investment_advisor False, analysis_sheet_rows False, advanced_analysis False, config False
    Host 'testserver/health?x='   -> all four True; enriched_quote.auth_guard lets it through; core.config.auth_ok(request=) True
    Host 'h/livez#', 'h/static?', 'h/v1/advanced/health?' -> all four True

End to end on the 9a73f48c tree, with the wall dropped by the documented kill switch (TFB_GLOBAL_AUTH_ENFORCE=0), the real app answered GET /v1/config/settings (the masked settings; no engine, no provider) with 200 to an anonymous request with Host 'testserver/health?x=' and to five other crafted shapes. A normal Host got the route's 401. With the wall up the full stack answers every crafted Host with the wall's 401 (W3), so 8.14.5 alone is not exploitable in the production posture. The finding is real; this commit fixes it.

The finding's second point is also confirmed: routes/investment_advisor `_is_public_path` lists "/v1/advanced" as public even with a normal Host. It is not changed here:
- It is a route-level policy, not a Host defect.
- With the wall up, an anonymous /v1/advanced gets the wall's 401 (it is not an exempt path).
- Removing it changes what a tokenless caller gets when the wall is dropped. It stays an owner decision (Owner decisions, second bullet).

### What changed

Six modules now read the ASGI scope path at their auth site instead of `getattr(request.url, "path")`:

- routes/investment_advisor.py 2.18.0 -> 2.18.1: `_auth_passed` (public-path check and the path handed to auth_ok). The stale v2.17.0 title now reads v2.18.1.
- routes/analysis_sheet_rows.py 4.8.3 -> 4.8.4: `_auth_passed`.
- routes/advanced_analysis.py 4.18.1 -> 4.18.2: `_auth_passed` (the path handed to core.config.auth_ok). A new top banner carries the WHY, as tests/test_learning_guards.py reads the first title line; the v4.18.1 banner follows it.
- routes/config.py 5.9.0 -> 5.9.1: `_auth_ok`.
- routes/enriched_quote.py 8.5.1 -> 8.5.2: `_Service.auth_guard`.
- core/config.py 5.8.0 -> 5.8.1: `auth_ok`, the request fallback used when a caller passes a request but no path. An explicit path is used exactly as before. `Mapping` joins its typing import.

Each module gets:
- a dated WHY in its docstring;
- a comment at its version constant;
- a module-level `_request_scope_path(request)`, the same helper as main.py's. It returns the ASGI scope path, or "" when the request carries no scope. "" matches no built-in public path, so such a request still needs a token.

The value is the full scope path, root_path included, which is exactly what request.url.path held for a valid Host. The route layer has no root_path step to mirror, so none is added. The helper is copied into each module, not imported, so no route module gains an import dependency.

Other files:
- scripts/verify_deployment.py 1.0.59 -> 1.0.60 (dated WHY) pins advanced_analysis 4.18.2 and analysis_sheet_rows 4.8.4. The other four modules are not in MODULES. No entry was added and no verifier logic changed.
- tests/test_analysis_sheet_rows_rank_withheld.py (R3) and tests/test_core_repair_tiny_quotes.py (`ROUTE`): the analysis_sheet_rows pin moves 4.8.3 -> 4.8.4 in place, with a dated comment. The tiny-quote CLI compares that fixture with the source constant, so without the move its tiny-readback subtest failed ("AssertionError: 1 != 0", found by the related-suite run).
- tests/test_auth_wall_scope_path.py 1.0.0 -> 1.1.0 adds W14-W19 (below). core.config's token-list and protected-path ENV names join the dropped ENV list, so a developer shell cannot change the answers.
- Not changed: main.py and ci.yml. The harness already runs in the contract job, and the push paths already cover routes/** and core/**.

### Why it is inert for valid traffic (no ENV gate)

The proof is the same as for the wall. A valid Host cannot contain "/", "?" or "#", so request.url.path equals scope["path"] byte for byte, and the path is the only input that changed.
- W15 checks each of the six helpers against request.url.path for 111,936 valid-Host requests. It is always on.
- W16 compares 11,451 route-layer decisions with the 9a73f48c tree: valid Hosts, anonymous and with a token, public paths and the full stack.

An off switch would only keep the bypass available, so there is no ENV gate.

Not changed, with the reason:
- The "path" fields of the analysis_sheet_rows and advanced_analysis health and provider-health payloads are echoes to the caller and decide nothing.
- routes/data_dictionary.py, routes/routes_argaam.py and routes/advanced_sheet_rows.py are not mounted: main.py mounts only `_CONTROLLED_ROUTE_PLAN`, and `_OPTIONAL_ROUTE_MODULES` only affects failure accounting. They keep request.url.path. A future mount should take the same helper.
- routes/advisor.py passes no path or request, so it cannot be spoofed.

### Harness 1.1.0 cases

- W14: the six auth sites are called directly. They refuse anonymous protected requests with a normal Host and with 896 crafted Host shapes per site, built on every public path any layer knows. A valid token passes with a normal or a crafted Host, and a wrong token is refused.
- W15: identity, always on. The helper equals request.url.path for valid Hosts and returns "" without a scope. There is no request.url read in the six auth functions (AST check). The versions and verify_deployment pins match.
- W16 [TFB_ROUTE_AUTH_BASE]: dual-tree identity.
- W17 [TFB_ROUTE_AUTH_BASE]: golden negative. On the base a crafted Host passes all six sites, and the real app (wall dropped) serves /v1/config/settings with 200. W14 and W19 FAIL there.
- W18 [TFB_ROUTE_AUTH_BASE]: zero removals.
- W19: full stack with the wall dropped. /v1/config/settings gives the route's own 401 to a normal Host and to six crafted Hosts, 401 to a wrong token and 200 to a valid one.

### Measured results

B = `git show 0fc8d6c0:main.py` (as above). T = `git archive 9a73f48c` extracted to <scratchpad>/build/auth-path/fix/base_tree_9a73f48c.

`python tests/test_auth_wall_scope_path.py` (no base) ends with:

    PASS W14 route-layer auth refuses crafted Hosts | 5596/5596 route-layer decisions right: 5376 crafted-Host requests refused at all 6 sites; a token passes with normal and crafted Hosts
    PASS W15 route-layer valid-Host identity (in process) | 111936 valid-Host requests over 6 modules: scope path == url.path; no-scope objects give ''; no request.url read in the six auth functions; versions and verify_deployment pins match
    SKIP W16 / W17 / W18 | TFB_ROUTE_AUTH_BASE unset
    PASS W19 full stack with the wall dropped | wall dropped: /v1/config/settings gives the route's own 401 to a normal Host and 6 crafted Hosts, 401 to a wrong token, 200 to a valid one
    PASS 13/13 (6 skipped: TFB_AUTH_WALL_BASE or TFB_ROUTE_AUTH_BASE unset)

`TFB_AUTH_WALL_BASE=$B TFB_ROUTE_AUTH_BASE=$T python tests/test_auth_wall_scope_path.py` gives `PASS 19/19`. Its W16 to W18 lines:

    PASS W16 route-layer dual-tree identity against the base | 11451/11451 route-layer decisions (valid Hosts, anonymous and token, public paths, full stack) identical to the pre-change tree
    PASS W17 route-layer golden negative on the base | base: Host 'testserver/health?x=' passes all 6 auth functions and the real app (wall dropped) serves /v1/config/settings 200 to it; W14 FAILs there (2572/5596 route-layer decisions wrong, 2500 crafted-Host requests passed; per site of 896 crafted: investment_advisor 468, analysis_sheet_rows 496, advanced_analysis 384, config 384, enriched_quote 384, core.config 384) and W19 FAILs
    PASS W18 route-layer zero removals against the base | 6 modules, 463 -> 469 defs: 0 removed, added _request_scope_path in each

The same run with the dda1211 main.py and the 0fc8d6c0 tree also gives `PASS 19/19`.

The new cases evaluated directly on T:
- W14 FAIL, as above.
- W15 FAIL: 20 problems, e.g. "INVESTMENT_ADVISOR_VERSION '2.18.0' != '2.18.1'", "has no module-level _request_scope_path" and "_auth_passed reads getattr(..., 'url') at line 1214".
- W19 FAIL: "anon Host='testserver/health?x=' -> 200; anon Host='h/livez#' -> 200; anon Host='h/static?' -> 200; anon Host='h/v1/advanced/health?' -> 200; anon Host='testserver/?x=' -> 200; anon Host='u@h/readyz?' -> 200".

Under pytest: "13 passed, 6 skipped in 20.99s"; with both bases, "19 passed in 39.56s".

Related suites: 24 files, all with every proxy variable at 127.0.0.1:9 and NO_PROXY=127.0.0.1,localhost.
- Files: every test that imports a changed module, main or verify_deployment, plus the route, redaction, schema, health, CI-verdict and workflow-audit suites.
- Result: "327 passed, 13 skipped, 74 subtests passed".
- Script mode: `tests/test_verify_manifest_pins.py` "[MANIFEST PIN GUARD] 3/3 PASS"; `tests/test_analysis_sheet_rows_rank_withheld.py` "RESULT PASS 4/4 route 4.8.4"; `tests/test_adv_typeerror_retry_p130.py` "T1-T6 PASS | module version 2.18.1".

CI lists, extracted from ci.yml by yaml.safe_load and run locally with the pinned stack (Python 3.11.17, FastAPI 0.115.14, Starlette 0.41.3, Node 22):
- Contract job: "252 passed, 8 skipped in 72.80s". On the 9a73f48c tree it gave "249 passed, 5 skipped"; the difference is W14, W15 and W19 passing and W16 to W18 skipping.
- Lean job: all 11 steps rc 0. The main pytest step gives "3525 passed, 18 skipped, 162 subtests passed"; tests/test_learning_guards.py is in it and reads the advanced_analysis title.

Whole suite: `python -m pytest -q tests` (proxies at 127.0.0.1:9, NO_PROXY=127.0.0.1,localhost) gives "4879 passed, 34 skipped, 3 warnings, 243 subtests passed in 475.87s". There are no failures. The three loopback suites listed under Whole suite pass with that NO_PROXY.

Static checks:
- `py_compile` of the six modules, verify_deployment and the harness: clean.
- `ruff 0.16.8 check --select E9,F63,F7,F82 .`: "All checks passed!".
- `ruff check --select F,E9` on the six modules and the harness: "All checks passed!".
- `python scripts/render_env_contract.py --check`: "OK ... is current".

Zero removals: W18 counts 463 -> 469 defs in the six modules, with 0 removed. The 26 deleted code lines are all replaced in place:
- six title versions and six version constants;
- six path reads and core.config's typing import;
- SCRIPT_VERSION and the two verifier pins;
- the two test pins;
- HARNESS_VERSION and one harness summary line.

ASCII: every added line is ASCII except two kinds of in-place change, where only digits changed:
- the six module title lines, whose em dash already existed (only the version digits changed);
- the SCRIPT_VERSION line, whose trailing history comment is carried over unchanged, as in 1.0.59.

### Operator steps for the follow-up

1. Merge the follow-up together with the 8.14.5 commit. No ENV change is needed.
2. After Render deploys, verify_deployment should report advanced_analysis 4.18.2 and analysis_sheet_rows 4.8.4 OK (BEHIND until the deploy lands).
3. The route layer is no longer Host-spoofable. Dropping the wall still opens the wall-only routes to anonymous callers, so TFB_GLOBAL_AUTH_ENFORCE stays an emergency control (Known limits).
4. Rollback: `git revert` the follow-up commit. The six modules, verify_deployment, the two test pins and the harness return to their 8.14.5-commit state, and the wall fix stays. Revert the 8.14.5 commit after it only if the wall itself must go.

## CI follow-up (harness 1.1.1, 2026-10-13)

The blocking contract job failed W19 on the first push of PR #769. Root cause, reproduced in a venv with exactly that job's packages (numpy, pytest, fastapi 0.115.14, starlette 0.41.3, httpx 0.27.2, pydantic 2.13.2, anyio 4.13.0): orjson is not installed there, routes/config renders through ORJSONResponse, which raises "orjson must be installed", and GlobalAuthEnforcementMiddleware's fail-closed `except Exception` (which also wraps `call_next`) turns that exception into a 401 for every request, valid token included. The same 401 appears on the pre-change tree in that venv, so it is not a regression. W19 now SKIPs with that reason when orjson is not importable; W14/W15 still prove the route layer in the contract job, and W19 runs wherever the full requirements are installed (whole-suite lane, Render has orjson 3.11.8). No assertion was weakened. Measured: contract list in that venv 251 passed, 9 skipped; harness there PASS 12/12 (7 skipped); full venv PASS 13/13 (6 skipped).

Pre-existing behaviour noted, not changed here (owner item): because the wall's `try/except Exception` encloses `call_next`, any exception raised downstream is answered as 401 "unauthorized", even while the wall is inactive. A server error is therefore reported as an auth failure. Narrowing the except to the decision only would be a separate, versioned change.
