"""tests/test_auth_wall_scope_path.py - global auth wall routed-path harness.

HARNESS_VERSION 1.0.0 (2026-10-13) for main.py v8.14.5.
HARNESS_VERSION 1.1.1 (2026-10-13, CI fix on PR #769): W19 skips, with its
reason, when orjson is not importable. The contract job installs only
fastapi/starlette/httpx/pydantic/anyio/numpy/pytest; routes/config renders
through ORJSONResponse, which then raises, and the wall's fail-closed
except turns that into a 401 for EVERY request (token or not), so W19 could
neither pass nor discriminate there. W14/W15 still prove the route layer in
that job; W19 runs wherever the full requirements (orjson) are installed,
e.g. the whole-suite lane. No assertion was weakened.
HARNESS_VERSION 1.1.0 (2026-10-13) adds W14-W19 for the route-layer
follow-up: routes/investment_advisor 2.18.1, analysis_sheet_rows 4.8.4,
advanced_analysis 4.18.2, config 5.9.1, enriched_quote 8.5.2 and core.config
5.8.1 read the ASGI scope path too. WHY (2026-10-13, review of 8.14.5): their
per-route auth still matched its public-path lists against the Host-derived
request.url.path. With the wall up an anonymous request never reaches them,
but with TFB_GLOBAL_AUTH_ENFORCE falsy they are the only check, and a Host
such as "x/health?x=" passed all six (the masked settings at
/v1/config/settings were served to an anonymous caller, W17/W19 on the base).

WHY (2026-10-13): GlobalAuthEnforcementMiddleware (main.py v8.13.0 to
v8.14.4) exempted a request from the global token wall when
str(request.url.path) was a probe/meta path. Starlette 0.41.3 builds
request.url as f"{scheme}://{Host header}{scope['path']}" and re-parses it,
while the router dispatches on scope["path"]. A crafted Host such as
"testserver/health?x=" therefore made request.url.path read "/health" for a
request routed to a protected path, and an anonymous caller passed the wall.
main.py v8.14.5 decides the exemption on the ASGI scope path (and on the
path the router matches after its root_path step). This harness builds the
REAL app (main.py loaded from disk, six route modules mounted, no server)
in a subprocess with the wall armed by ENV and drives it with Starlette's
TestClient.

Data routes are never executed: every case that targets one goes through a
"tripwire" app -- the real GlobalAuthEnforcementMiddleware class of the
module under test wrapped around a stub that answers 200 "REACHED". The real
app is only asked for probe/meta paths and for wall-only routes that build
static payloads (/v1/schema/pages, /v1/config, /v1/advanced/meta). The
subprocess also blocks Python sockets and points every proxy variable at a
closed local port, so nothing can leave the machine.

Cases:
  W1 delivered main.py is v8.14.5, defines the routed-path helpers, and no
     request.url access is left in main.py code (AST)
  W2 (1) anonymous requests to protected paths get the wall's 401, whose
     "path" is the routed path; the tripwire is never reached
  W3 (2) every exempt path in every crafted Host shape ("h/health?x=",
     "h/health#", "h/health", "h/livez?", port, userinfo, fragment) and the
     decoded-path shapes (/health%3F, /%3F, /livez%23x, /health%09, LF, CR)
     still get 401 on protected targets
  W4 (3) exempt paths with a normal Host are exempt (tripwire reached for
     each; the real app answers them without the wall's 401)
  W5 (4) a valid token (X-APP-TOKEN, Bearer, X-API-Key) passes, with a
     normal or a crafted Host; a wrong token gets 401
  W6 (5) OPTIONS preflight is not walled, with a normal or a crafted Host
  W7 (6) kill switch: TFB_GLOBAL_AUTH_ENFORCE falsy (0/false/off/no) drops
     the wall; truthy, empty or unknown values keep it; OPEN_MODE=true and
     REQUIRE_AUTH=false keep it down
  W8 (7a) decision identity, always on: for a corpus of valid Host values
     (with and without port, IPv6 literals, uppercase, sub-delims, empty,
     absent) and plain paths, the v8.14.4 expression
     str(request.url.path) in _GLOBAL_AUTH_EXEMPT_PATHS equals the new
     decision, and request.url.path equals the echoed scope path;
     _scope_route_path equals Starlette's own get_route_path
  W9 (7b) [needs TFB_AUTH_WALL_BASE] dual tree: the same corpus through the
     pre-change wall and the new wall gives byte-identical status and body
     (ts_utc masked); the real apps agree on every benign path; the
     kill-switch, OPEN_MODE and REQUIRE_AUTH answers are identical
  W10 root_path: decisions with a root_path follow the router (Starlette
     get_route_path); identical to v8.14.4 except where an exempt scope path
     would be routed to a non-meta route
  W11 an unparseable Host ("[") gets the wall's JSON 401 (no crash), and a
     valid token still passes
  W12 (8) [needs TFB_AUTH_WALL_BASE] golden negative: on the pre-change
     main.py the crafted-Host request returns non-401 from the real app
     (bypass reproduced) and W3 FAILS
  W13 [needs TFB_AUTH_WALL_BASE] zero removals: every def/class of the base
     is still defined; the additions are exactly the three helpers
  W14 route layer (auth functions called directly, no handler runs, wall
     not involved): investment_advisor._auth_passed,
     analysis_sheet_rows._auth_passed, advanced_analysis._auth_passed,
     config._auth_ok, enriched_quote svc.auth_guard and core.config.auth_ok
     (request only) refuse an anonymous protected request with a normal Host
     and with every crafted Host shape built on every public path any layer
     knows; a valid token passes with a normal or a crafted Host; a wrong
     token is refused
  W15 route-layer identity, always on: each module's _request_scope_path
     equals request.url.path for the valid-Host corpus (the path is the only
     input that changed, so every decision is unchanged) and returns "" for
     a request object without a scope; the six auth functions read no
     request.url (AST); versions bumped; verify_deployment pins
     advanced_analysis and analysis_sheet_rows
  W16 [needs TFB_ROUTE_AUTH_BASE] dual tree: every route-layer decision for
     the valid-Host corpus (anonymous and with a token) and for the public
     paths is identical to the pre-change tree
  W17 [needs TFB_ROUTE_AUTH_BASE] golden negative: on the pre-change tree a
     crafted Host passes all six auth functions and the real app serves
     /v1/config/settings to an anonymous crafted-Host request with the wall
     dropped; W14 and W19 FAIL there
  W18 [needs TFB_ROUTE_AUTH_BASE] zero removals in the six modules; the only
     addition in each is _request_scope_path
  W19 full stack with the wall dropped (TFB_GLOBAL_AUTH_ENFORCE=0): the real
     app answers GET /v1/config/settings (masked settings; no engine, no
     provider) with the route's own 401 for a normal Host and for crafted
     Hosts, 401 for a wrong token and 200 for a valid one

Env (optional):
  TFB_AUTH_WALL_BASE=<file or dir>  the pre-change main.py (or a directory
      holding it), e.g.
        git show 0fc8d6c0:main.py > /tmp/main_base_v8.14.4.py
      or the dda1211 copy (v8.14.3; same wall code). Unset: W9, W12, W13
      SKIP cleanly.
  TFB_ROUTE_AUTH_BASE=<dir>  a pre-change TREE (routes/, core/, main.py),
      e.g.  mkdir /tmp/base && git archive 9a73f48c | tar -x -C /tmp/base
      (0fc8d6c0 has the same routes/ and core/). Unset: W16, W17, W18 SKIP.

Run:  python tests/test_auth_wall_scope_path.py   -> "PASS k/k", exit 0/1
      python -m pytest -q tests/test_auth_wall_scope_path.py
"""
from __future__ import annotations

import ast
import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

try:  # pytest is optional in script mode
    import pytest
except ImportError:  # pragma: no cover - script mode without pytest
    pytest = None


HARNESS_VERSION = "1.1.1"
ROOT = Path(__file__).resolve().parents[1]
DELIVERED = ROOT / "main.py"
ENV_BASE = "TFB_AUTH_WALL_BASE"
ENV_ROUTE_BASE = "TFB_ROUTE_AUTH_BASE"
EXPECTED_VERSION = "8.14.5"
NEW_HELPERS = ["_global_auth_exempt_request", "_request_scope_path", "_scope_route_path"]
TOKEN = "harness-wall-token-7q2"
RID = "harness-rid-1"

# Wall-only routes that build static payloads (no engine, no provider).
BENIGN_PROTECTED = ["/v1/schema/pages", "/v1/config", "/v1/advanced/meta"]
# Data routes: only ever sent to the tripwire, never to the real app.
DATA_PROTECTED = ["/v1/advanced/sheet-rows", "/v1/analysis/sheet-rows", "/sheet-rows",
                  "/quote", "/quotes", "/v1/enriched/quote", "/v1/advisor/run"]
UNKNOWN = ["/no-such-path", "/v1/no-such-path"]

VALID_HOSTS = [
    "testserver", "localhost", "localhost:8000", "127.0.0.1", "127.0.0.1:10000",
    "[::1]", "[::1]:8080", "[2001:db8::1]:443", "[v1.fe]", "EXAMPLE.COM",
    "Tadawul-Fast-Bridge.OnRender.com", "a.b-c.example:65535", "xn--mgbh0fb.example",
    "sub.delims!$&'()*+,;=", "pct%20enc.example", "h:", "",
]
NEAR_MISS = ["/health/", "/HEALTH", "//health", "/health/x", "/v1/health/", "/healthz/",
             "/docs/oauth2-redirect", "/openapi.json/", "/_debug/routes/", "/health;x",
             "/health%20", "/health.json", "/v1", "/v1/", "/static/x"]


def _host_shapes(exempt: str) -> list:
    return [f"testserver{exempt}?x=", f"h{exempt}#", f"h{exempt}", f"h{exempt}?",
            f"h:8080{exempt}?a=1&b=", f"u@h{exempt}?", f"h{exempt}#frag?x", f"H{exempt}?"]


PATH_SHAPES = ["/health%3F", "/%3F", "/livez%23x", "/health%09", "/v1/health%0A", "/health%0D",
               "/readyz%3F/v1/advanced/sheet-rows"]


RUNNER = r'''
import importlib.util, json, os, socket, sys
ROOT, MAIN, MODE = sys.argv[1], sys.argv[2], sys.argv[3]
SPEC = json.loads(sys.stdin.read())
sys.path.insert(0, ROOT)

def _blocked(*a, **k):
    raise OSError("network blocked by tests/test_auth_wall_scope_path.py")
socket.socket.connect = _blocked
socket.socket.connect_ex = _blocked
socket.create_connection = _blocked
socket.getaddrinfo = _blocked

sp = importlib.util.spec_from_file_location("main", MAIN)
m = importlib.util.module_from_spec(sp)
sys.modules["main"] = m
sp.loader.exec_module(m)

from starlette.testclient import TestClient
from starlette.requests import Request

TOKEN, RID = SPEC["token"], SPEC["rid"]

async def _stub(scope, receive, send):
    await send({"type": "http.response.start", "status": 200, "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": b"REACHED"})

def _mask(resp):
    body = resp.text
    try:
        obj = json.loads(body)
        if isinstance(obj, dict) and "ts_utc" in obj:
            obj["ts_utc"] = "<ts>"
        body = json.dumps(obj, sort_keys=True)
    except Exception:
        pass
    return [resp.status_code, body]

def _req(client, method, path, host=None, headers=None):
    h = {"X-Request-ID": RID}
    if host is not None:
        h["Host"] = host
    h.update(headers or {})
    return _mask(client.request(method, path, headers=h))

out = {"entry_version": getattr(m, "APP_ENTRY_VERSION", ""), "mode": MODE,
       "exempt": sorted(m._GLOBAL_AUTH_EXEMPT_PATHS),
       "wall_active": bool(m._global_auth_enforcement_active())}

trip = TestClient(m.GlobalAuthEnforcementMiddleware(_stub), raise_server_exceptions=False)
real = TestClient(m.app, raise_server_exceptions=False)
T, R = {}, {}
for key, method, path, host, hdrs in SPEC["trip"]:
    T[key] = _req(trip, method, path, host, hdrs)
for key, method, path, host, hdrs in SPEC["real"]:
    R[key] = _req(real, method, path, host, hdrs)
out["trip"], out["real"] = T, R

# root_path: the router's own view, via TestClient(root_path=...)
RP = {}
for key, root, path in SPEC["root"]:
    RP[key] = _req(TestClient(m.GlobalAuthEnforcementMiddleware(_stub), root_path=root, raise_server_exceptions=False), "GET", path)
out["root"] = RP

# kill switch: TFB_GLOBAL_AUTH_ENFORCE is read per request
KS = {}
saved = os.environ.get("TFB_GLOBAL_AUTH_ENFORCE")
for value in SPEC["kill_values"]:
    if value is None:
        os.environ.pop("TFB_GLOBAL_AUTH_ENFORCE", None)
    else:
        os.environ["TFB_GLOBAL_AUTH_ENFORCE"] = value
    KS[repr(value)] = {"active": bool(m._global_auth_enforcement_active()),
                       "plain": _req(trip, "GET", "/v1/advanced/sheet-rows"),
                       "crafted": _req(trip, "GET", "/v1/advanced/sheet-rows", "testserver/health?x=")}
    if value == "0":
        KS[repr(value)]["real_benign"] = _req(real, "GET", "/v1/schema/pages")
if saved is None:
    os.environ.pop("TFB_GLOBAL_AUTH_ENFORCE", None)
else:
    os.environ["TFB_GLOBAL_AUTH_ENFORCE"] = saved
out["kill"] = KS

# W8: in-process decision identity (new helpers only)
if hasattr(m, "_global_auth_exempt_request"):
    import random
    rng = random.Random(20261013)
    alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~!$&'()*+,;=:@/% "
    paths = list(SPEC["corpus_paths"])
    for _ in range(600):
        paths.append("/" + "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 14))))
    hosts = list(SPEC["valid_hosts"]) + [None]
    n = mism = echo = 0
    examples = []
    for host in hosts:
        for path in paths:
            # A request with neither a Host header nor a server address only
            # exists in hand-built ASGI scopes (uvicorn always sets server for
            # TCP), so that pair is not part of the valid-traffic corpus.
            servers = [("http", ("testserver", 80)), ("https", ("10.0.0.1", 8443))]
            if host is not None:
                servers.append(("http", None))
            for scheme, server in servers:
                headers = [] if host is None else [(b"host", host.encode("latin-1"))]
                scope = {"type": "http", "method": "GET", "scheme": scheme, "path": path, "root_path": "",
                         "query_string": b"x=/health", "headers": headers, "server": server}
                old = str(Request(scope).url.path or "") in m._GLOBAL_AUTH_EXEMPT_PATHS
                new = m._global_auth_exempt_request(Request(scope))
                n += 1
                if old != new:
                    mism += 1
                    if len(examples) < 5:
                        examples.append([host, path, old, new])
                if Request(scope).url.path != m._request_scope_path(Request(scope)):
                    echo += 1
    out["identity"] = {"n": n, "mismatch": mism, "echo_mismatch": echo, "examples": examples}
    try:
        from starlette._utils import get_route_path
        roots = ["", "/", "/api", "/v1", "/health", "/_debug", "/v1/health", "/a/b"]
        par = bad = 0
        for root in roots:
            for path in paths[:200] + SPEC["corpus_paths"]:
                for p in (path, root + path):
                    par += 1
                    if m._scope_route_path(p, root) != get_route_path({"path": p, "root_path": root}):
                        bad += 1
        out["route_path_parity"] = {"n": par, "mismatch": bad}
    except Exception as exc:
        out["route_path_parity"] = {"skip": repr(exc)}

print("@@JSON@@" + json.dumps(out, sort_keys=True))
'''

_BASE_ENV = {
    "APP_ENV": "production",
    "TFB_REQUIRE_AUTH": "true", "REQUIRE_AUTH": "true", "APP_REQUIRE_AUTH": "true",
    "TFB_OPEN_MODE": "false", "OPEN_MODE": "false", "APP_OPEN_MODE": "false",
    "APP_TOKEN": TOKEN,
    "INIT_ENGINE_ON_BOOT": "false", "DEFER_ROUTER_MOUNT": "false",
    "DISABLE_NETWORK_AT_IMPORT": "true", "PROVIDERS_OFFLINE_MODE": "true",
    "LOG_LEVEL": "ERROR", "LOG_JSON": "false", "PYTHONDONTWRITEBYTECODE": "1",
    # belt and braces: native HTTP clients (libcurl) honour these, so any
    # stray outbound call dies on a closed local port instead of leaving.
    "HTTP_PROXY": "http://127.0.0.1:9", "HTTPS_PROXY": "http://127.0.0.1:9",
    "ALL_PROXY": "http://127.0.0.1:9", "http_proxy": "http://127.0.0.1:9",
    "https_proxy": "http://127.0.0.1:9", "all_proxy": "http://127.0.0.1:9",
    "NO_PROXY": "", "no_proxy": "",
}
_DROP_ENV = (
    "BACKEND_TOKEN", "BACKUP_APP_TOKEN", "X_APP_TOKEN", "AUTH_TOKEN", "TOKEN",
    "TFB_APP_TOKEN", "TFB_TOKEN", "API_TOKEN", "TFB_BEARER_TOKEN", "BEARER_TOKEN",
    "TFB_API_KEY", "API_KEY", "X_API_KEY", "TFB_GLOBAL_AUTH_ENFORCE", "ALLOW_QUERY_TOKEN",
    "ENABLE_CORS_ALL_ORIGINS", "CORS_ORIGINS", "CORS_ALLOW_CREDENTIALS", "DEBUG",
    "STATUS_DETAILS_REQUIRE_AUTH", "PUBLIC_PATHS", "AUTH_PUBLIC_PATHS",
    "PUBLIC_EXACT_PATHS", "PUBLIC_PATH_PREFIXES", "AUTH_HEADER_NAME", "TFB_ENV", "ENV",
    # 1.1.0: core.config reads these token lists (before APP_TOKEN) and
    # protected-path lists, so a developer shell cannot change the answers
    "ALLOWED_TOKENS", "TFB_ALLOWED_TOKENS", "APP_TOKENS", "PROTECTED_EXACT_PATHS",
    "PROTECTED_PATH_PREFIXES",
)
_MODES = {
    "armed": {},
    "open": {"TFB_OPEN_MODE": "true", "OPEN_MODE": "true", "APP_OPEN_MODE": "true"},
    "noauth": {"TFB_REQUIRE_AUTH": "false", "REQUIRE_AUTH": "false", "APP_REQUIRE_AUTH": "false"},
}


def _spec() -> dict:
    trip, real, root = [], [], []
    tok = {"X-APP-TOKEN": TOKEN}
    # W2 anonymous protected
    for p in DATA_PROTECTED + BENIGN_PROTECTED + UNKNOWN:
        trip.append([f"anon|{p}", "GET", p, None, {}])
    for p in BENIGN_PROTECTED + UNKNOWN:
        real.append([f"anon|{p}", "GET", p, None, {}])
    # W3 crafted Host shapes on protected targets
    exempt = sorted(_exempt_paths())
    for e in exempt:
        for shape in _host_shapes(e):
            for p in ("/v1/advanced/sheet-rows", "/quote", "/v1/schema/pages"):
                trip.append([f"shape|{shape}|{p}", "GET", p, shape, {}])
            real.append([f"shape|{shape}|/v1/schema/pages", "GET", "/v1/schema/pages", shape, {}])
    for p in PATH_SHAPES:
        trip.append([f"pathshape|{p}", "GET", p, None, {}])
        real.append([f"pathshape|{p}", "GET", p, None, {}])
    # W4 exempt paths with a normal Host
    for e in exempt:
        trip.append([f"exempt|{e}", "GET", e, None, {}])
        real.append([f"exempt|{e}", "GET", e, None, {}])
    # W5 tokens
    for name, hdrs in (("xapp", tok), ("bearer", {"Authorization": "Bearer " + TOKEN}),
                       ("xapikey", {"X-API-Key": TOKEN}), ("wrong", {"X-APP-TOKEN": "nope"})):
        trip.append([f"token|{name}|plain", "GET", "/v1/advanced/sheet-rows", None, hdrs])
        trip.append([f"token|{name}|crafted", "GET", "/v1/advanced/sheet-rows", "testserver/health?x=", hdrs])
        real.append([f"token|{name}|plain", "GET", "/v1/schema/pages", None, hdrs])
    # W6 OPTIONS preflight
    pre = {"Origin": "https://sheets.example", "Access-Control-Request-Method": "POST"}
    trip.append(["options|plain", "OPTIONS", "/v1/advanced/sheet-rows", None, pre])
    trip.append(["options|crafted", "OPTIONS", "/v1/advanced/sheet-rows", "h/health?", pre])
    trip.append(["options|bare", "OPTIONS", "/v1/advanced/sheet-rows", None, {}])
    real.append(["options|plain", "OPTIONS", "/v1/schema/pages", None, pre])
    # W9 corpus (valid Hosts) through the tripwire, and benign paths through the real app
    for host in VALID_HOSTS:
        for p in exempt + DATA_PROTECTED + BENIGN_PROTECTED + UNKNOWN + NEAR_MISS:
            trip.append([f"corpus|{host}|{p}", "GET", p, host, {}])
        for q in ("?x=/health", "?a=1&b=#f"):
            trip.append([f"corpusq|{host}|{q}", "GET", "/v1/advanced/sheet-rows" + q, host, {}])
        trip.append([f"corpuspost|{host}", "POST", "/v1/advanced/sheet-rows", host, {}])
        trip.append([f"corpusopt|{host}", "OPTIONS", "/health", host, pre])
    for host in ("testserver", "localhost:8000", "[::1]:8080", "EXAMPLE.COM", "127.0.0.1"):
        for p in exempt + BENIGN_PROTECTED + UNKNOWN:
            real.append([f"corpus|{host}|{p}", "GET", p, host, {}])
    # W11 unparseable Host
    trip.append(["badhost|anon", "GET", "/v1/advanced/sheet-rows", "[", {}])
    trip.append(["badhost|token", "GET", "/v1/advanced/sheet-rows", "[", tok])
    real.append(["badhost|anon", "GET", "/v1/schema/pages", "[", {}])
    # W10 root_path
    for key, rp, p in (("api_full", "/api", "/api/health"), ("api_bare", "/api", "/health"),
                       ("v1_alias", "/v1", "/v1/health"), ("debug_split", "/_debug", "/_debug/routes"),
                       ("equal_root", "/health", "/health"), ("none", "", "/health"),
                       ("api_protected", "/api", "/api/v1/advanced/sheet-rows")):
        root.append([key, rp, p])
    return {"trip": trip, "real": real, "root": root, "token": TOKEN, "rid": RID,
            "kill_values": [None, "", "1", "true", "on", "maybe", "0", "false", "off", "no", "FALSE"],
            "valid_hosts": VALID_HOSTS,
            "corpus_paths": exempt + DATA_PROTECTED + BENIGN_PROTECTED + UNKNOWN + NEAR_MISS}


def _exempt_paths() -> set:
    """The delivered _BUILTIN_META_PATHS literal, read by AST (no import)."""
    tree = ast.parse(DELIVERED.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", "") == "_BUILTIN_META_PATHS" for t in node.targets):
            return set(ast.literal_eval(node.value))
    raise AssertionError("_BUILTIN_META_PATHS literal not found in main.py")


_CACHE: dict = {}


def _run(main_path: Path, mode: str) -> dict:
    key = (str(main_path), mode)
    if key in _CACHE:
        return _CACHE[key]
    env = dict(os.environ)
    for name in _DROP_ENV:
        env.pop(name, None)
    env.update(_BASE_ENV)
    env.update(_MODES[mode])
    proc = subprocess.run([sys.executable, "-c", RUNNER, str(ROOT), str(main_path), mode],
                          input=json.dumps(_spec()), cwd=str(ROOT), env=env,
                          capture_output=True, text=True, timeout=600)
    lines = [ln for ln in proc.stdout.splitlines() if ln.startswith("@@JSON@@")]
    if proc.returncode != 0 or not lines:
        raise AssertionError(f"runner failed for {main_path.name} [{mode}] rc={proc.returncode}: "
                             f"{proc.stdout[-600:]} {proc.stderr[-1500:]}")
    _CACHE[key] = json.loads(lines[-1][len("@@JSON@@"):])
    return _CACHE[key]


def _base_path():
    raw = os.environ.get(ENV_BASE, "").strip()
    if not raw:
        return None
    p = Path(raw)
    if p.is_dir():
        p = p / "main.py"
    return p


def _walled(rec, path=None) -> bool:
    """The wall's own 401: JSON with error 'unauthorized' and a 'path' key."""
    status, body = rec
    if status != 401:
        return False
    try:
        obj = json.loads(body)
    except Exception:
        return False
    if not (isinstance(obj, dict) and obj.get("error") == "unauthorized" and "path" in obj):
        return False
    return path is None or obj.get("path") == path


def _unquote(p: str) -> str:
    from urllib.parse import unquote
    return unquote(p.split("?", 1)[0])


# ------------------------------------------------- route layer (1.1.0)
# The six route-layer auth sites, in the order the cases report them.
ROUTE_SITES = ["investment_advisor._auth_passed", "analysis_sheet_rows._auth_passed",
               "advanced_analysis._auth_passed", "config._auth_ok",
               "enriched_quote.auth_guard", "core.config.auth_ok"]
# file -> (version constant, expected version, auth function qualname)
ROUTE_FILES = {
    "routes/investment_advisor.py": ("INVESTMENT_ADVISOR_VERSION", "2.18.1", "_auth_passed"),
    "routes/analysis_sheet_rows.py": ("ANALYSIS_SHEET_ROWS_VERSION", "4.8.4", "_auth_passed"),
    "routes/advanced_analysis.py": ("ADVANCED_ANALYSIS_VERSION", "4.18.2", "_auth_passed"),
    "routes/config.py": ("ROUTER_VERSION", "5.9.1", "_auth_ok"),
    # 8.5.2 -> 8.6.0 (2026-10-19): bridge signature binding + gated instrument
    # fallback; _Service.auth_guard and _request_scope_path untouched.
    "routes/enriched_quote.py": ("ROUTER_VERSION", "8.6.0", "_Service.auth_guard"),
    "core/config.py": ("__version__", "5.8.1", "auth_ok"),
}
ROUTE_HELPER = "_request_scope_path"
# Every path some route-layer list treats as public (investment_advisor,
# analysis_sheet_rows "*/health", core.config exact and prefixes); the wall's
# exempt set is added at run time.
ROUTE_PUBLIC = ["/", "/health", "/readyz", "/livez", "/meta", "/v1/health", "/x/health",
                "/openapi.json", "/docs", "/docs/x", "/redoc", "/favicon.ico", "/static",
                "/static/x", "/v1/advanced", "/v1/advanced/health", "/v1/advanced/meta",
                "/v1/advanced/metrics", "/v1/advisor/health", "/v1/schema/health"]
# Protected under every route-layer list.
ROUTE_TARGETS = ["/v1/advanced/sheet-rows", "/v1/analysis/sheet-rows", "/v1/config/settings", "/quote"]
ROUTE_TOKEN_HOSTS = ["testserver", "testserver/health?x=", "h/static?", "h/v1/advanced/health?"]
FULL_STACK_HOSTS = ["testserver/health?x=", "h/livez#", "h/static?", "h/v1/advanced/health?",
                    "testserver/?x=", "u@h/readyz?"]

ROUTE_RUNNER = r"""
import importlib.util, json, os, random, socket, sys
TREE = sys.argv[1]
SPEC = json.loads(sys.stdin.read())
sys.path.insert(0, TREE)

def _blocked(*a, **k):
    raise OSError("network blocked by tests/test_auth_wall_scope_path.py")
socket.socket.connect = _blocked
socket.socket.connect_ex = _blocked
socket.create_connection = _blocked
socket.getaddrinfo = _blocked

from starlette.requests import Request
from fastapi import HTTPException
import core.config as cc
import routes.investment_advisor as ia
import routes.analysis_sheet_rows as asr
import routes.advanced_analysis as aa
import routes.config as rc
import routes.enriched_quote as eq

TOKEN = SPEC["token"]

def req(path, host, token=None, server=("testserver", 80)):
    headers = [] if host is None else [(b"host", host.encode("latin-1"))]
    if token:
        headers.append((b"x-app-token", token.encode("latin-1")))
    return Request({"type": "http", "method": "GET", "scheme": "http", "path": path,
                    "raw_path": path.encode("utf-8"), "root_path": "", "query_string": b"",
                    "headers": headers, "client": ("203.0.113.9", 50000), "server": server})

def _settings(mod):
    fn = getattr(mod, "get_settings_cached", None)
    try:
        return fn() if callable(fn) else None
    except Exception:
        return None

def s_ia(r, tok):
    return ia._auth_passed(request=r, token_query=None, x_app_token=tok, authorization=None)
def s_asr(r, tok):
    return asr._auth_passed(request=r, settings=_settings(asr), auth_token=tok or "", authorization=None)
def s_aa(r, tok):
    return aa._auth_passed(request=r, settings=_settings(aa), auth_token=tok or "", authorization=None)
def s_rc(r, tok):
    return rc._auth_ok(r, query_token=None)
def s_eq(r, tok):
    try:
        eq.svc.auth_guard(r, None, tok, None)
        return True
    except HTTPException as exc:
        if exc.status_code == 401:
            return False
        raise
def s_cc(r, tok):
    return cc.auth_ok(request=r)

SITES = dict(zip(SPEC["sites"], (s_ia, s_asr, s_aa, s_rc, s_eq, s_cc)))
MODS = {"routes/investment_advisor.py": ia, "routes/analysis_sheet_rows.py": asr,
        "routes/advanced_analysis.py": aa, "routes/config.py": rc,
        "routes/enriched_quote.py": eq, "core/config.py": cc}

def call(fn, r, tok):
    try:
        return bool(fn(r, tok))
    except Exception as exc:
        return "EXC " + type(exc).__name__

os.environ.pop("TFB_GLOBAL_AUTH_ENFORCE", None)
out = {"plain": {}, "spoof": {}, "token": {}, "wrong": {}, "public": {}, "corpus": {}}
for site, fn in SITES.items():
    for p in SPEC["targets"]:
        out["plain"][site + "|" + p] = call(fn, req(p, "testserver"), None)
        for shape in SPEC["shapes"]:
            out["spoof"][site + "|" + shape + "|" + p] = call(fn, req(p, shape), None)
        for host in SPEC["token_hosts"]:
            out["token"][site + "|" + host + "|" + p] = call(fn, req(p, host, TOKEN), TOKEN)
            out["wrong"][site + "|" + host + "|" + p] = call(fn, req(p, host, "nope"), "nope")
    for p in SPEC["public"]:
        out["public"][site + "|" + p] = call(fn, req(p, "testserver"), None)
    for host in SPEC["valid_hosts"] + [None]:
        for p in SPEC["corpus_paths"]:
            for tok in (None, TOKEN):
                out["corpus"]["%s|%r|%s|%s" % (site, host, p, bool(tok))] = call(fn, req(p, host, tok), tok)

# helper identity (delivered tree only: the base has no helper)
rng = random.Random(20261013)
alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~!$&'()*+,;=:@/% "
paths = list(SPEC["corpus_paths"])
for _ in range(300):
    paths.append("/" + "".join(rng.choice(alphabet) for _ in range(rng.randint(0, 14))))

class _Duck:
    url = type("U", (), {"path": "/health"})()
    headers = {}

helpers = {}
for name, mod in MODS.items():
    fn = getattr(mod, "_request_scope_path", None)
    if not callable(fn):
        helpers[name] = None
        continue
    n = mism = 0
    examples = []
    for host in SPEC["valid_hosts"] + [None]:
        servers = [("testserver", 80), ("10.0.0.1", 8443)] + ([None] if host is not None else [])
        for p in paths:
            for server in servers:
                r = req(p, host, server=server)
                n += 1
                if fn(r) != r.url.path:
                    mism += 1
                    if len(examples) < 4:
                        examples.append([host, p, fn(r), r.url.path])
    helpers[name] = {"n": n, "mismatch": mism, "examples": examples, "duck": fn(_Duck()),
                     "none": fn(None)}
out["helpers"] = helpers

# full stack, wall dropped: the route layer is the only check
os.environ["TFB_GLOBAL_AUTH_ENFORCE"] = "0"
sp = importlib.util.spec_from_file_location("main", os.path.join(TREE, "main.py"))
m = importlib.util.module_from_spec(sp)
sys.modules["main"] = m
sp.loader.exec_module(m)
from starlette.testclient import TestClient
client = TestClient(m.app, raise_server_exceptions=False)
FS = {"wall_active": bool(m._global_auth_enforcement_active()), "entry_version": getattr(m, "APP_ENTRY_VERSION", "")}
for key, host, tok in SPEC["full_stack"]:
    h = {"X-Request-ID": "harness-rid-2"}
    if host is not None:
        h["Host"] = host
    if tok:
        h["X-APP-TOKEN"] = tok
    resp = client.get("/v1/config/settings", headers=h)
    try:
        obj = resp.json()
    except Exception:
        obj = {}
    obj = obj if isinstance(obj, dict) else {}
    # status and envelope keys only: the masked settings body is never kept
    FS[key] = [resp.status_code, obj.get("error"), obj.get("route_owner"), "path" in obj]
out["full_stack"] = FS
print("@@JSON@@" + json.dumps(out, sort_keys=True))
"""


def _route_spec() -> dict:
    public = sorted(set(ROUTE_PUBLIC) | _exempt_paths())
    shapes = sorted({s for p in public for s in _host_shapes(p)})
    full = [["anon|testserver", "testserver", None], ["wrong|testserver", "testserver", "nope"],
            ["token|testserver", "testserver", TOKEN], ["token|testserver/health?x=", "testserver/health?x=", TOKEN]]
    full += [["anon|" + h, h, None] for h in FULL_STACK_HOSTS]
    return {"sites": ROUTE_SITES, "token": TOKEN, "targets": ROUTE_TARGETS, "shapes": shapes,
            "token_hosts": ROUTE_TOKEN_HOSTS, "public": public, "valid_hosts": VALID_HOSTS,
            "corpus_paths": sorted(set(public + ROUTE_TARGETS + NEAR_MISS + UNKNOWN + DATA_PROTECTED)),
            "full_stack": full}


_ROUTE_CACHE: dict = {}


def _run_routes(tree: Path) -> dict:
    key = str(tree)
    if key in _ROUTE_CACHE:
        return _ROUTE_CACHE[key]
    env = dict(os.environ)
    for name in _DROP_ENV:
        env.pop(name, None)
    env.update(_BASE_ENV)
    proc = subprocess.run([sys.executable, "-c", ROUTE_RUNNER, str(tree)],
                          input=json.dumps(_route_spec()), cwd=str(tree), env=env,
                          capture_output=True, text=True, timeout=600)
    lines = [ln for ln in proc.stdout.splitlines() if ln.startswith("@@JSON@@")]
    if proc.returncode != 0 or not lines:
        raise AssertionError(f"route runner failed for {tree} rc={proc.returncode}: "
                             f"{proc.stdout[-600:]} {proc.stderr[-1500:]}")
    _ROUTE_CACHE[key] = json.loads(lines[-1][len("@@JSON@@"):])
    return _ROUTE_CACHE[key]


def _route_base():
    raw = os.environ.get(ENV_ROUTE_BASE, "").strip()
    return Path(raw) if raw else None


def _route_base_problem(base: Path):
    if not base.is_dir():
        return f"{ENV_ROUTE_BASE}={base} is not a directory"
    missing = [f for f in list(ROUTE_FILES) + ["main.py"] if not (base / f).is_file()]
    if missing:
        return f"{ENV_ROUTE_BASE}={base} lacks {missing}"
    return None


def _route_401(rec) -> bool:
    """The config route's own 401 (the wall's 401 carries a "path" key)."""
    return rec[0] == 401 and rec[1] == "unauthorized" and rec[2] == "config" and not rec[3]


# ----------------------------------------------------------------- cases
def case_w1(main_path: Path = DELIVERED):
    src = main_path.read_text(encoding="utf-8")
    tree = ast.parse(src)
    problems = []
    names = {n.name for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
    missing = [h for h in NEW_HELPERS if h not in names]
    if missing:
        problems.append(f"helpers missing: {missing}")
    version = None
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", "") == "APP_ENTRY_VERSION" for t in node.targets):
            version = ast.literal_eval(node.value)
    if version != EXPECTED_VERSION:
        problems.append(f"APP_ENTRY_VERSION {version!r} != {EXPECTED_VERSION!r}")
    url_uses = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.attr == "url" and isinstance(node.value, ast.Name) and node.value.id == "request":
            url_uses.append(node.lineno)
        if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "getattr"
                and len(node.args) >= 2 and isinstance(node.args[1], ast.Constant) and node.args[1].value == "url"):
            url_uses.append(node.lineno)
    if url_uses:
        problems.append(f"request.url still read at lines {url_uses}")
    vd = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    if f"('main', 'APP_ENTRY_VERSION', '{EXPECTED_VERSION}'" not in vd:
        problems.append("verify_deployment.py does not pin main APP_ENTRY_VERSION " + EXPECTED_VERSION)
    if problems:
        return "FAIL", "; ".join(problems)
    return "PASS", f"v{version}; helpers {NEW_HELPERS}; 0 request.url reads; verify_deployment pin matches"


def case_w2(res=None):
    res = res or _run(DELIVERED, "armed")
    bad = []
    if not res["wall_active"]:
        bad.append("wall not active in armed mode")
    for p in DATA_PROTECTED + BENIGN_PROTECTED + UNKNOWN:
        if not _walled(res["trip"][f"anon|{p}"], p):
            bad.append(f"tripwire {p}: {res['trip'][f'anon|{p}']}")
    for p in BENIGN_PROTECTED + UNKNOWN:
        if not _walled(res["real"][f"anon|{p}"], p):
            bad.append(f"real {p}: {res['real'][f'anon|{p}'][0]}")
    if bad:
        return "FAIL", "; ".join(bad[:6])
    n = len(DATA_PROTECTED + BENIGN_PROTECTED + UNKNOWN) + len(BENIGN_PROTECTED + UNKNOWN)
    return "PASS", f"{n}/{n} anonymous protected requests get the wall's 401 with the routed path"


def case_w3(res=None):
    res = res or _run(DELIVERED, "armed")
    passed, echo, n = [], [], 0

    def check(side, label, rec, path):
        nonlocal n
        n += 1
        if not _walled(rec):
            passed.append(f"{side} {label} -> {rec[0]} {rec[1][:30]}")
        elif not _walled(rec, path):
            echo.append(f"{side} {label} echoed {json.loads(rec[1]).get('path')!r}")

    for e in sorted(res["exempt"]):
        for shape in _host_shapes(e):
            for p in ("/v1/advanced/sheet-rows", "/quote", "/v1/schema/pages"):
                check("tripwire", f"Host={shape!r} {p}", res["trip"][f"shape|{shape}|{p}"], p)
            check("real", f"Host={shape!r} /v1/schema/pages",
                  res["real"][f"shape|{shape}|/v1/schema/pages"], "/v1/schema/pages")
    for p in PATH_SHAPES:
        for side in ("trip", "real"):
            check(side, f"path {p}", res[side][f"pathshape|{p}"], _unquote(p))
    if passed or echo:
        return "FAIL", (f"{len(passed)}/{n} crafted requests passed the wall, {len(echo)} walled with a "
                        f"Host-shaped path: " + "; ".join((passed + echo)[:4]))
    return "PASS", f"{n}/{n} crafted Host / decoded-path requests get 401 (routed path echoed)"


def case_w4(res=None):
    res = res or _run(DELIVERED, "armed")
    bad = []
    for e in sorted(res["exempt"]):
        if res["trip"][f"exempt|{e}"] != [200, "REACHED"]:
            bad.append(f"tripwire {e}: {res['trip'][f'exempt|{e}'][0]}")
        if _walled(res["real"][f"exempt|{e}"]):
            bad.append(f"real {e}: wall 401")
    if bad:
        return "FAIL", "; ".join(bad[:6])
    statuses = sorted({res["real"][f"exempt|{e}"][0] for e in res["exempt"]})
    return "PASS", f"{len(res['exempt'])} exempt paths pass the wall (real app statuses {statuses})"


def case_w5(res=None):
    res = res or _run(DELIVERED, "armed")
    bad = []
    for name in ("xapp", "bearer", "xapikey"):
        for host in ("plain", "crafted"):
            if res["trip"][f"token|{name}|{host}"] != [200, "REACHED"]:
                bad.append(f"tripwire {name}/{host}: {res['trip'][f'token|{name}|{host}'][0]}")
        if res["real"][f"token|{name}|plain"][0] != 200:
            bad.append(f"real {name}: {res['real'][f'token|{name}|plain'][0]}")
    for host in ("plain", "crafted"):
        if not _walled(res["trip"][f"token|wrong|{host}"], "/v1/advanced/sheet-rows"):
            bad.append(f"wrong token {host} not walled")
    if not _walled(res["real"]["token|wrong|plain"], "/v1/schema/pages"):
        bad.append("real wrong token not walled")
    if bad:
        return "FAIL", "; ".join(bad)
    return "PASS", "X-APP-TOKEN, Bearer and X-API-Key pass (normal and crafted Host); a wrong token gets 401"


def case_w6(res=None):
    res = res or _run(DELIVERED, "armed")
    bad = [k for k in ("options|plain", "options|crafted", "options|bare") if res["trip"][k] != [200, "REACHED"]]
    if _walled(res["real"]["options|plain"]):
        bad.append("real OPTIONS walled")
    if bad:
        return "FAIL", f"OPTIONS walled: {bad}"
    return "PASS", f"OPTIONS passes the wall (real app answers {res['real']['options|plain'][0]} without CORS configured)"


_KILL_OFF = ("'0'", "'false'", "'off'", "'no'", "'FALSE'")
_KILL_ON = ("None", "''", "'1'", "'true'", "'on'", "'maybe'")


def case_w7(armed=None, open_=None, noauth=None):
    armed = armed or _run(DELIVERED, "armed")
    open_ = open_ or _run(DELIVERED, "open")
    noauth = noauth or _run(DELIVERED, "noauth")
    bad = []
    for k in _KILL_OFF:
        rec = armed["kill"][k]
        if rec["active"] or rec["plain"] != [200, "REACHED"] or rec["crafted"] != [200, "REACHED"]:
            bad.append(f"kill {k} did not drop the wall")
    if armed["kill"]["'0'"]["real_benign"][0] != 200:
        bad.append("kill 0: real wall-only route not reachable")
    for k in _KILL_ON:
        rec = armed["kill"][k]
        if not rec["active"] or not _walled(rec["plain"]) or not _walled(rec["crafted"]):
            bad.append(f"kill {k} did not keep the wall")
    for name, res in (("OPEN_MODE=true", open_), ("REQUIRE_AUTH=false", noauth)):
        if res["wall_active"] or res["trip"]["anon|/v1/advanced/sheet-rows"] != [200, "REACHED"]:
            bad.append(f"{name}: wall still up")
    if bad:
        return "FAIL", "; ".join(bad)
    return "PASS", (f"falsy {len(_KILL_OFF)} values drop the wall, {len(_KILL_ON)} others keep it; "
                    "OPEN_MODE=true and REQUIRE_AUTH=false keep it down")


def case_w8(res=None):
    res = res or _run(DELIVERED, "armed")
    ident = res.get("identity")
    if not ident:
        return "FAIL", "the module under test has no _global_auth_exempt_request"
    par = res.get("route_path_parity", {})
    problems = []
    if ident["mismatch"] or ident["echo_mismatch"]:
        problems.append(f"decision mismatches {ident['mismatch']}, echo mismatches {ident['echo_mismatch']}: {ident['examples']}")
    parity = "starlette get_route_path unavailable (%s)" % par.get("skip")
    if "skip" not in par:
        parity = f"_scope_route_path == get_route_path in {par['n'] - par['mismatch']}/{par['n']}"
    if par.get("mismatch"):
        problems.append(f"_scope_route_path differs from get_route_path in {par['mismatch']}/{par['n']}")
    if problems:
        return "FAIL", "; ".join(problems)
    return "PASS", (f"{ident['n']} valid-Host requests: old and new decisions identical, "
                    f"url.path == scope path; {parity}")


def _dual(base: Path):
    return {m: (_run(base, m), _run(DELIVERED, m)) for m in ("armed", "open", "noauth")}


def case_w9():
    base = _base_path()
    if base is None:
        return "SKIP", f"{ENV_BASE} unset"
    if not base.is_file():
        return "FAIL", f"{ENV_BASE}={base} is not a file"
    pairs = _dual(base)
    old, new = pairs["armed"]
    diffs, n = [], 0
    for key in old["trip"]:
        if not key.startswith(("corpus", "anon|", "exempt|", "token|", "options|")) or key.endswith("|crafted"):
            continue
        n += 1
        if old["trip"][key] != new["trip"][key]:
            diffs.append(f"tripwire {key}: {old['trip'][key]} vs {new['trip'][key]}")
    for key in old["real"]:
        if not key.startswith(("corpus", "anon|", "exempt|", "token|", "options|")):
            continue
        n += 1
        a, b = old["real"][key], new["real"][key]
        same = a == b if (a[0] == 401 or b[0] == 401) else a[0] == b[0]
        if not same:
            diffs.append(f"real {key}: {a[0]} vs {b[0]}")
    for k in old["kill"]:
        # the crafted-Host record is the bypass itself; it may only match
        # where the wall is down in both trees
        fields = ["active", "plain", "real_benign"] + (["crafted"] if k in _KILL_OFF else [])
        for f in fields:
            if f not in old["kill"][k] and f not in new["kill"][k]:
                continue
            n += 1
            if old["kill"][k].get(f) != new["kill"][k].get(f):
                diffs.append(f"kill {k} {f}: {old['kill'][k].get(f)} vs {new['kill'][k].get(f)}")
    for m in ("open", "noauth"):
        a, b = pairs[m]
        n += 1
        if a["wall_active"] != b["wall_active"] or a["trip"]["anon|/v1/advanced/sheet-rows"] != b["trip"]["anon|/v1/advanced/sheet-rows"]:
            diffs.append(f"mode {m} differs")
    for key in ("api_full", "api_bare", "v1_alias", "none", "api_protected"):
        n += 1
        if old["root"][key] != new["root"][key]:
            diffs.append(f"root {key}: {old['root'][key]} vs {new['root'][key]}")
    if old["exempt"] != new["exempt"]:
        diffs.append("exempt path set differs")
    if diffs:
        return "FAIL", f"{len(diffs)}/{n} differ: " + "; ".join(diffs[:5])
    return "PASS", f"{n}/{n} valid-Host requests, kill-switch values, modes and root_paths identical to {old['entry_version']}"


def case_w10(res=None):
    res = res or _run(DELIVERED, "armed")
    rp = res["root"]
    expect = {"api_full": "walled", "api_bare": "reached", "v1_alias": "reached", "none": "reached",
              "debug_split": "walled", "equal_root": "walled", "api_protected": "walled"}
    bad = []
    for key, want in expect.items():
        got = "reached" if rp[key] == [200, "REACHED"] else ("walled" if _walled(rp[key]) else f"other {rp[key][0]}")
        if got != want:
            bad.append(f"{key}: {got} != {want}")
    if bad:
        return "FAIL", "; ".join(bad)
    return "PASS", ("root_path follows the router: /api/health walled, /v1/health under root /v1 exempt, "
                    "/_debug/routes under root /_debug walled (routed to /routes)")


def case_w11(res=None):
    res = res or _run(DELIVERED, "armed")
    bad = []
    if not _walled(res["trip"]["badhost|anon"], "/v1/advanced/sheet-rows"):
        bad.append(f"tripwire anon: {res['trip']['badhost|anon']}")
    if res["trip"]["badhost|token"] != [200, "REACHED"]:
        bad.append(f"tripwire token: {res['trip']['badhost|token']}")
    if not _walled(res["real"]["badhost|anon"], "/v1/schema/pages"):
        bad.append(f"real anon: {res['real']['badhost|anon'][0]}")
    if bad:
        return "FAIL", "; ".join(bad)
    return "PASS", "Host '[' -> JSON 401 with the routed path; a valid token still passes"


def case_w12():
    base = _base_path()
    if base is None:
        return "SKIP", f"{ENV_BASE} unset"
    if not base.is_file():
        return "FAIL", f"{ENV_BASE}={base} is not a file"
    old = _run(base, "armed")
    bypass = old["real"]["shape|testserver/health?x=|/v1/schema/pages"]
    trip = old["trip"]["shape|testserver/health?x=|/v1/advanced/sheet-rows"]
    status, detail = case_w3(old)
    problems = []
    if bypass[0] == 401:
        problems.append("base real app answered 401 to the crafted Host (bypass not reproduced)")
    if trip != [200, "REACHED"]:
        problems.append(f"base wall did not let the crafted Host reach the tripwire: {trip}")
    if status != "FAIL":
        problems.append("W3 did not FAIL on the base")
    if problems:
        return "FAIL", "; ".join(problems)
    return "PASS", (f"base {old['entry_version']}: Host 'testserver/health?x=' -> /v1/schema/pages {bypass[0]} "
                    f"(non-401), tripwire REACHED; W3 FAILs there ({detail.split(': ')[0]})")


def case_w13():
    base = _base_path()
    if base is None:
        return "SKIP", f"{ENV_BASE} unset"

    def defs(path):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        return {n.name for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
    b, d = defs(base), defs(DELIVERED)
    removed, added = sorted(b - d), sorted(d - b)
    if removed or added != NEW_HELPERS:
        return "FAIL", f"removed {removed}, added {added}"
    return "PASS", f"{len(b)} -> {len(d)} defs: 0 removed, added {added}"


def case_w14(res=None):
    res = res or _run_routes(ROOT)
    bad, n = [], 0
    passed_by_site = {}
    for group, want in (("plain", False), ("spoof", False), ("wrong", False), ("token", True)):
        for key, got in sorted(res[group].items()):
            n += 1
            if got is not want:
                site = key.split("|", 1)[0]
                if group == "spoof" and got is True:
                    passed_by_site[site] = passed_by_site.get(site, 0) + 1
                bad.append(f"{group} {key} -> {got}")
    for key in ("investment_advisor._auth_passed|/v1/advanced/health", "analysis_sheet_rows._auth_passed|/health",
                "core.config.auth_ok|/health", "config._auth_ok|/static/x"):
        n += 1
        if res["public"].get(key) is not True:
            bad.append(f"public {key} -> {res['public'].get(key)}")
    if bad:
        spoof = sum(passed_by_site.values())
        return "FAIL", (f"{len(bad)}/{n} route-layer decisions wrong, {spoof} crafted-Host requests passed; "
                        f"per site {dict(sorted(passed_by_site.items()))}; e.g. " + "; ".join(bad[:4]))
    n_spoof = len(res["spoof"])
    return "PASS", (f"{n}/{n} route-layer decisions right: {n_spoof} crafted-Host requests refused at all "
                    f"{len(ROUTE_SITES)} sites; a token passes with normal and crafted Hosts")


def _route_ast_problems(root: Path) -> list:
    problems = []
    for rel, (const, want, qual) in ROUTE_FILES.items():
        tree = ast.parse((root / rel).read_text(encoding="utf-8"))
        version = None
        for node in tree.body:
            if isinstance(node, ast.Assign) and any(getattr(t, "id", "") == const for t in node.targets):
                version = ast.literal_eval(node.value)
        if version != want:
            problems.append(f"{rel} {const} {version!r} != {want!r}")
        if not any(isinstance(n, ast.FunctionDef) and n.name == ROUTE_HELPER for n in tree.body):
            problems.append(f"{rel} has no module-level {ROUTE_HELPER}")
        body = tree.body
        fn = None
        for part in qual.split("."):
            fn = next((n for n in body if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name == part), None)
            body = getattr(fn, "body", [])
        if fn is None:
            problems.append(f"{rel} has no {qual}")
            continue
        for node in ast.walk(fn):
            if isinstance(node, ast.Attribute) and node.attr == "url" and isinstance(node.value, ast.Name) and node.value.id == "request":
                problems.append(f"{rel} {qual} reads request.url at line {node.lineno}")
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "getattr"
                    and len(node.args) >= 2 and isinstance(node.args[1], ast.Constant) and node.args[1].value == "url"):
                problems.append(f"{rel} {qual} reads getattr(..., 'url') at line {node.lineno}")
    vd = (root / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    for mod, rel in (("routes.advanced_analysis", "routes/advanced_analysis.py"),
                     ("routes.analysis_sheet_rows", "routes/analysis_sheet_rows.py")):
        const, want, _ = ROUTE_FILES[rel]
        if f'("{mod}", "{const}", "{want}",' not in vd:
            problems.append(f"verify_deployment.py does not pin {mod} {want}")
    return problems


def case_w15(res=None):
    res = res or _run_routes(ROOT)
    problems = _route_ast_problems(ROOT)
    total = 0
    for rel, h in sorted(res["helpers"].items()):
        if h is None:
            problems.append(f"{rel}: no {ROUTE_HELPER} at run time")
            continue
        total += h["n"]
        if h["mismatch"]:
            problems.append(f"{rel}: {h['mismatch']}/{h['n']} differ from url.path {h['examples']}")
        if h["duck"] != "" or h["none"] != "":
            problems.append(f"{rel}: no-scope request gave {h['duck']!r} / {h['none']!r}, not ''")
    if problems:
        return "FAIL", "; ".join(problems[:6])
    return "PASS", (f"{total} valid-Host requests over {len(res['helpers'])} modules: scope path == url.path; "
                    "no-scope objects give ''; no request.url read in the six auth functions; versions and "
                    "verify_deployment pins match")


def case_w16():
    base = _route_base()
    if base is None:
        return "SKIP", f"{ENV_ROUTE_BASE} unset"
    problem = _route_base_problem(base)
    if problem:
        return "FAIL", problem
    old, new = _run_routes(base), _run_routes(ROOT)
    diffs, n = [], 0
    for group in ("corpus", "plain", "public"):
        for key in sorted(set(old[group]) | set(new[group])):
            n += 1
            if old[group].get(key) != new[group].get(key):
                diffs.append(f"{group} {key}: {old[group].get(key)} vs {new[group].get(key)}")
    for key in sorted(old["token"]):
        if "|testserver|" in key:
            n += 1
            if old["token"][key] != new["token"][key]:
                diffs.append(f"token {key}: {old['token'][key]} vs {new['token'][key]}")
    for key in ("anon|testserver", "wrong|testserver", "token|testserver"):
        n += 1
        if old["full_stack"][key] != new["full_stack"][key]:
            diffs.append(f"full stack {key}: {old['full_stack'][key]} vs {new['full_stack'][key]}")
    if diffs:
        return "FAIL", f"{len(diffs)}/{n} differ: " + "; ".join(diffs[:5])
    return "PASS", (f"{n}/{n} route-layer decisions (valid Hosts, anonymous and token, public paths, "
                    f"full stack) identical to the pre-change tree")


def case_w17():
    base = _route_base()
    if base is None:
        return "SKIP", f"{ENV_ROUTE_BASE} unset"
    problem = _route_base_problem(base)
    if problem:
        return "FAIL", problem
    old = _run_routes(base)
    problems = []
    key = "testserver/health?x=|/v1/advanced/sheet-rows"
    reached = {site: old["spoof"].get(f"{site}|{key}") for site in ROUTE_SITES}
    not_spoofed = [site for site, got in reached.items() if got is not True]
    if not_spoofed:
        problems.append(f"base not spoofed at {not_spoofed}")
    w14, w14_detail = case_w14(old)
    w19, _ = case_w19(old)
    if w14 != "FAIL":
        problems.append("W14 did not FAIL on the base")
    if w19 != "FAIL":
        problems.append("W19 did not FAIL on the base")
    fs = old["full_stack"].get("anon|testserver/health?x=")
    if not fs or fs[0] != 200:
        problems.append(f"base real app did not serve /v1/config/settings to the crafted Host: {fs}")
    if problems:
        return "FAIL", "; ".join(problems)
    per_site = {site: sum(1 for k, v in old["spoof"].items() if k.startswith(site + "|") and v is True)
                for site in ROUTE_SITES}
    return "PASS", (f"base: Host 'testserver/health?x=' passes all {len(ROUTE_SITES)} auth functions and the "
                    f"real app (wall dropped) serves /v1/config/settings {fs[0]} to it; W14 FAILs there "
                    f"({w14_detail.split('; per site')[0]}; per site of {len(old['spoof']) // len(ROUTE_SITES)} "
                    f"crafted: {per_site}) and W19 FAILs")


def case_w18():
    base = _route_base()
    if base is None:
        return "SKIP", f"{ENV_ROUTE_BASE} unset"
    problem = _route_base_problem(base)
    if problem:
        return "FAIL", problem

    def defs(path):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        return {n.name for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
    bad, nb, nd = [], 0, 0
    for rel in ROUTE_FILES:
        b, d = defs(base / rel), defs(ROOT / rel)
        nb, nd = nb + len(b), nd + len(d)
        removed, added = sorted(b - d), sorted(d - b)
        if removed or added != [ROUTE_HELPER]:
            bad.append(f"{rel}: removed {removed}, added {added}")
    if bad:
        return "FAIL", "; ".join(bad)
    return "PASS", f"{len(ROUTE_FILES)} modules, {nb} -> {nd} defs: 0 removed, added {ROUTE_HELPER} in each"


def case_w19(res=None):
    # 1.1.1: the full-stack route renders through ORJSONResponse; without
    # orjson every request becomes the wall's fail-closed 401, so this case
    # cannot discriminate. Skip with the reason instead of passing or failing.
    if importlib.util.find_spec("orjson") is None:
        return "SKIP", ("orjson not installed in this interpreter: /v1/config/settings cannot render, "
                        "so every request is a fail-closed 401 and the full-stack case cannot "
                        "discriminate (W14/W15 cover the route layer here)")
    res = res or _run_routes(ROOT)
    fs = res["full_stack"]
    bad = []
    if fs["wall_active"]:
        bad.append("wall still active with TFB_GLOBAL_AUTH_ENFORCE=0")
    for host in ["testserver"] + FULL_STACK_HOSTS:
        if not _route_401(fs[f"anon|{host}"]):
            bad.append(f"anon Host={host!r} -> {fs[f'anon|{host}'][0]}")
    if not _route_401(fs["wrong|testserver"]):
        bad.append(f"wrong token -> {fs['wrong|testserver'][0]}")
    for key in ("token|testserver", "token|testserver/health?x="):
        if fs[key][0] != 200:
            bad.append(f"{key} -> {fs[key][0]}")
    if bad:
        return "FAIL", "; ".join(bad)
    return "PASS", (f"wall dropped: /v1/config/settings gives the route's own 401 to a normal Host and "
                    f"{len(FULL_STACK_HOSTS)} crafted Hosts, 401 to a wrong token, 200 to a valid one")


CASES = (
    ("W1 v8.14.5, helpers present, no request.url reads", case_w1),
    ("W2 anonymous protected requests get 401", case_w2),
    ("W3 crafted Host and decoded-path shapes still get 401", case_w3),
    ("W4 exempt paths stay exempt", case_w4),
    ("W5 valid token passes, wrong token 401", case_w5),
    ("W6 OPTIONS preflight unchanged", case_w6),
    ("W7 kill switch, OPEN_MODE and REQUIRE_AUTH semantics", case_w7),
    ("W8 valid-Host decision identity (in process)", case_w8),
    ("W9 dual-tree identity against the base", case_w9),
    ("W10 root_path follows the router", case_w10),
    ("W11 unparseable Host fails closed without a crash", case_w11),
    ("W12 golden negative on the base", case_w12),
    ("W13 zero removals against the base", case_w13),
    ("W14 route-layer auth refuses crafted Hosts", case_w14),
    ("W15 route-layer valid-Host identity (in process)", case_w15),
    ("W16 route-layer dual-tree identity against the base", case_w16),
    ("W17 route-layer golden negative on the base", case_w17),
    ("W18 route-layer zero removals against the base", case_w18),
    ("W19 full stack with the wall dropped", case_w19),
)


# ----------------------------------------------------------- pytest glue
if pytest is not None:
    @pytest.mark.parametrize("name,case", CASES, ids=[name.split()[0] for name, _ in CASES])
    def test_auth_wall_scope_path_case(name, case):
        status, detail = case()
        if status == "SKIP":
            pytest.skip(f"{name}: {detail}")
        assert status == "PASS", f"{name}: {detail}"


def main() -> int:
    passed = failed = skipped = 0
    for name, case in CASES:
        try:
            status, detail = case()
        except Exception as exc:  # noqa: BLE001 - a crashing case is a failing case
            status, detail = "FAIL", f"{type(exc).__name__}: {exc}"
        print(f"{status} {name} | {detail}")
        passed += status == "PASS"
        failed += status == "FAIL"
        skipped += status == "SKIP"
    ran = passed + failed
    note = f" ({skipped} skipped: {ENV_BASE} or {ENV_ROUTE_BASE} unset)" if skipped else ""
    if failed:
        print(f"FAIL {failed}/{ran} (PASS {passed}/{ran}){note}")
        return 1
    print(f"PASS {passed}/{ran}{note}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
