"""tests/test_auth_wall_downstream_errors.py - main.py v8.14.6: the global auth
wall answers 401 only for requests it REFUSES.

WHY (2026-10-21)
----------------
Up to v8.14.5 GlobalAuthEnforcementMiddleware.dispatch ran `await
call_next(request)` inside the same try/except that keeps the auth decision
fail-closed. Any exception raised downstream of an ALLOWED request (a route
error, Starlette's "No response returned" after a dropped request) was
swallowed and answered with the wall's 401 "unauthorized". Render log
2026-10-20 23:16:14Z: each "Caught downstream no-response runtime error" is
followed by "POST /v1/analysis/sheet-rows 401", and daily_sync run
39513589733 reported 60 backend 401s ("check TFB_TOKEN") although the token
was valid. v8.14.6 decides inside the try and calls downstream outside it.

Cases (the delivered main.py runs in a subprocess, wall armed by ENV, no
network; data routes are never executed - only stub ASGI apps and one
harness route that raises):
  D1 valid token + downstream exception      -> 500, never the wall's 401
  D2 valid token + downstream sends nothing  -> 500 ("No response returned"
     propagates), never the wall's 401
  D3 anonymous / wrong token + any downstream -> the wall's own 401 with the
     routed path (the stub is never reached)
  D4 valid token + healthy downstream        -> 200 REACHED (unchanged)
  D5 real app: a protected route that raises -> 500 from the app's own
     exception handler with a token, the wall's 401 without one
  D6 [needs TFB_AUTH_WALL_BASE=<pre-change main.py>] golden negative: the
     base answers 401 in D1/D2/D5 (bug reproduced)

Run:  python -m pytest -q tests/test_auth_wall_downstream_errors.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DELIVERED = ROOT / "main.py"
ENV_BASE = "TFB_AUTH_WALL_BASE"
TOKEN = "harness-downstream-token-5k1"
PATH = "/v1/analysis/sheet-rows"
REAL_PATH = "/v1/__harness_downstream_raise"

RUNNER = r'''
import importlib.util, json, socket, sys
ROOT, MAIN = sys.argv[1], sys.argv[2]
SPEC = json.loads(sys.stdin.read())
sys.path.insert(0, ROOT)

def _blocked(*a, **k):
    raise OSError("network blocked by tests/test_auth_wall_downstream_errors.py")
socket.socket.connect = _blocked
socket.socket.connect_ex = _blocked
socket.create_connection = _blocked
socket.getaddrinfo = _blocked

sp = importlib.util.spec_from_file_location("main", MAIN)
m = importlib.util.module_from_spec(sp)
sys.modules["main"] = m
sp.loader.exec_module(m)
from starlette.testclient import TestClient

TOKEN = SPEC["token"]

async def ok(scope, receive, send):
    await send({"type": "http.response.start", "status": 200,
                "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": b"REACHED"})

async def boom(scope, receive, send):
    raise RuntimeError("synthetic downstream failure")

async def silent(scope, receive, send):
    return None  # no response: BaseHTTPMiddleware raises "No response returned."

def rec(client, path, token=None, method="POST"):
    h = {"X-Request-ID": "harness-dse"}
    if token:
        h["X-APP-TOKEN"] = token
    r = client.request(method, path, headers=h)
    try:
        body = r.json()
        body = {k: body.get(k) for k in ("status", "error", "path")} if isinstance(body, dict) else str(body)
    except Exception:
        body = r.text[:60]
    return [r.status_code, body]

W = m.GlobalAuthEnforcementMiddleware
out = {"version": getattr(m, "APP_ENTRY_VERSION", ""),
       "wall_active": bool(m._global_auth_enforcement_active())}
for name, app in (("ok", ok), ("boom", boom), ("silent", silent)):
    client = TestClient(W(app), raise_server_exceptions=False)
    out[name + "|token"] = rec(client, SPEC["path"], TOKEN)
    out[name + "|anon"] = rec(client, SPEC["path"])
    out[name + "|wrong"] = rec(client, SPEC["path"], "nope")

def _harness_raise():
    raise RuntimeError("synthetic route failure")

m.app.add_api_route(SPEC["real_path"], _harness_raise, methods=["GET"])
real = TestClient(m.app, raise_server_exceptions=False)
out["real|token"] = rec(real, SPEC["real_path"], TOKEN, "GET")
out["real|anon"] = rec(real, SPEC["real_path"], None, "GET")
print("@@JSON@@" + json.dumps(out, sort_keys=True, default=str))
'''

_BASE_ENV = {
    "APP_ENV": "production",
    "TFB_REQUIRE_AUTH": "true", "REQUIRE_AUTH": "true", "APP_REQUIRE_AUTH": "true",
    "TFB_OPEN_MODE": "false", "OPEN_MODE": "false", "APP_OPEN_MODE": "false",
    "APP_TOKEN": TOKEN,
    "INIT_ENGINE_ON_BOOT": "false", "DEFER_ROUTER_MOUNT": "false",
    "DISABLE_NETWORK_AT_IMPORT": "true", "PROVIDERS_OFFLINE_MODE": "true",
    "LOG_LEVEL": "ERROR", "LOG_JSON": "false", "PYTHONDONTWRITEBYTECODE": "1",
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
    "ALLOWED_TOKENS", "TFB_ALLOWED_TOKENS", "APP_TOKENS", "PROTECTED_EXACT_PATHS",
    "PROTECTED_PATH_PREFIXES",
)
_CACHE: dict = {}


def _run(main_path: Path) -> dict:
    key = str(main_path)
    if key in _CACHE:
        return _CACHE[key]
    env = dict(os.environ)
    for name in _DROP_ENV:
        env.pop(name, None)
    env.update(_BASE_ENV)
    spec = {"token": TOKEN, "path": PATH, "real_path": REAL_PATH}
    proc = subprocess.run([sys.executable, "-c", RUNNER, str(ROOT), str(main_path)],
                          input=json.dumps(spec), cwd=str(ROOT), env=env,
                          capture_output=True, text=True, timeout=600)
    lines = [ln for ln in proc.stdout.splitlines() if ln.startswith("@@JSON@@")]
    if proc.returncode != 0 or not lines:
        raise AssertionError(f"runner failed for {main_path} rc={proc.returncode}: "
                             f"{proc.stdout[-600:]} {proc.stderr[-1500:]}")
    _CACHE[key] = json.loads(lines[-1][len("@@JSON@@"):])
    return _CACHE[key]


def _walled(rec, path=None) -> bool:
    status, body = rec
    return (status == 401 and isinstance(body, dict) and body.get("error") == "unauthorized"
            and "path" in body and (path is None or body.get("path") == path))


@pytest.fixture(scope="module")
def res():
    out = _run(DELIVERED)
    assert out["wall_active"] is True, "the wall must be armed for this harness"
    return out


def test_d1_allowed_request_downstream_exception_is_500(res):
    assert res["boom|token"][0] == 500, res["boom|token"]
    assert not _walled(res["boom|token"])


def test_d2_allowed_request_without_response_is_500(res):
    assert res["silent|token"][0] == 500, res["silent|token"]
    assert not _walled(res["silent|token"])


@pytest.mark.parametrize("app", ["ok", "boom", "silent"])
@pytest.mark.parametrize("who", ["anon", "wrong"])
def test_d3_refused_requests_still_get_the_wall_401(res, app, who):
    assert _walled(res[f"{app}|{who}"], PATH), res[f"{app}|{who}"]


def test_d4_healthy_allowed_request_unchanged(res):
    assert res["ok|token"] == [200, "REACHED"]


def test_d5_real_app_route_error_is_500_not_401(res):
    status, body = res["real|token"]
    assert status == 500 and not _walled(res["real|token"]), res["real|token"]
    assert isinstance(body, dict) and body.get("status") == "error"
    assert _walled(res["real|anon"], REAL_PATH), res["real|anon"]


def test_d5b_version(res):
    assert res["version"] == "8.14.6"


def test_d6_golden_negative_on_base():
    raw = os.environ.get(ENV_BASE, "").strip()
    if not raw:
        pytest.skip(f"{ENV_BASE} unset (point it at the pre-change main.py, e.g. git show 97fa991:main.py)")
    base = Path(raw)
    if base.is_dir():
        base = base / "main.py"
    old = _run(base)
    assert old["version"] == "8.14.5"
    assert _walled(old["boom|token"], PATH), old["boom|token"]
    assert _walled(old["silent|token"], PATH), old["silent|token"]
    assert _walled(old["real|token"], REAL_PATH), old["real|token"]
