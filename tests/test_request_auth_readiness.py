"""Mounted auth and readiness checks use local ASGI calls and dummy credentials."""

from dataclasses import replace
import importlib
import socket
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from starlette.requests import Request

from core.utils.request_security import invalid_host_authority, request_path


@pytest.fixture
def secured(monkeypatch):
    for name in ("OPEN_MODE", "TFB_OPEN_MODE", "APP_OPEN_MODE"):
        monkeypatch.setenv(name, "false")
    for name in ("REQUIRE_AUTH", "TFB_REQUIRE_AUTH", "APP_REQUIRE_AUTH"):
        monkeypatch.setenv(name, "true")
    monkeypatch.setenv("APP_TOKEN", "local-audit-token")
    monkeypatch.setenv("TFB_GLOBAL_AUTH_ENFORCE", "true")
    monkeypatch.setenv("INIT_ENGINE_ON_BOOT", "false")
    monkeypatch.delenv("TFB_READYZ_STRICT", raising=False)
    monkeypatch.delenv("TFB_HEALTH_STRICT", raising=False)

    def forbid_network(*args, **kwargs):
        raise AssertionError("auth/readiness tests must not open network sockets")

    monkeypatch.setattr(socket.socket, "connect", forbid_network)
    core_config = importlib.import_module("core.config")
    settings = core_config.Settings(require_auth=True, open_mode=False)
    monkeypatch.setattr(core_config, "get_settings_cached", lambda *a, **k: settings)
    root_config = importlib.import_module("config")
    root_settings = root_config.TFBSettings(
        require_auth=True, open_mode=False, app_token="local-audit-token",
    )
    monkeypatch.setattr(root_config, "get_settings_cached", lambda *a, **k: root_settings)
    main = importlib.import_module("main")
    monkeypatch.setattr(main, "_SETTINGS", replace(
        main._SETTINGS, APP_ENV="production", REQUIRE_AUTH=True, OPEN_MODE=False,
        INIT_ENGINE_ON_BOOT=False, PRESTART_MOUNT_ROUTES=True,
        ENABLE_CORS_ALL_ORIGINS=True, CORS_ALLOW_CREDENTIALS=False,
        READY_REQUIRE_ENGINE=False, READY_REQUIRE_ROUTES=False,
        READY_REQUIRE_AUTH=False,
    ))
    app = main.create_app()
    return main, core_config, app, settings


@pytest.mark.parametrize("host", [
    b"example.com", b"localhost:8000", b"127.0.0.1:10000",
    b"[::1]", b"[2001:db8::1]:8443", b"xn--mgbh0fb.xn--kgbechtv",
])
def test_valid_authorities(host):
    assert invalid_host_authority({"headers": [(b"host", host)]}) is False


@pytest.mark.parametrize("host", [
    b"", b"bad host", b"bad\thost", b"bad\rhost", b"bad\nhost",
    b"bad/host", b"bad\\host", b"bad?host", b"bad#host", b"bad@host",
    b"bad\x7fhost", b"bad\xffhost",
])
def test_invalid_authorities(host):
    assert invalid_host_authority({"headers": [(b"host", host)]}) is True


def test_duplicate_host_and_http10_compatibility():
    assert invalid_host_authority({"headers": [(b"host", b"a"), (b"Host", b"a")]})
    assert invalid_host_authority({"headers": []}) is False


def test_scope_path_overrides_url_and_explicit_public_hint(secured):
    _, config, _, settings = secured
    request = SimpleNamespace(scope={"path": "/v1/config/settings"},
                              url=SimpleNamespace(path="/health"), headers={})
    assert request_path(request) == "/v1/config/settings"
    assert config.auth_ok(request=request, path="/health", settings=settings) is False
    assert config.auth_ok(path="/health", settings=settings) is True
    request.scope = {}
    assert config.auth_ok(request=request, path="/health", settings=settings) is False


@pytest.mark.parametrize("wall", [True, False])
def test_actual_protected_config_route_ignores_reconstructed_url(secured, monkeypatch, wall):
    _, _, app, _ = secured
    monkeypatch.setenv("TFB_GLOBAL_AUTH_ENFORCE", str(wall))
    # Simulate a reconstructed URL disagreeing with routing, without relying
    # on a particular framework-version payload or making an external call.
    monkeypatch.setattr(Request, "url", property(lambda self: SimpleNamespace(path="/health")))
    response = TestClient(app).get("/v1/config/settings")
    assert response.status_code == 401
    assert response.json()["error"] == "unauthorized"


@pytest.mark.parametrize("host", ["bad host", "bad/host"])
@pytest.mark.parametrize("path", ["/v1/config/settings", "/health"])
def test_authority_guard_precedes_auth_and_public_routes(secured, host, path):
    _, _, app, _ = secured
    response = TestClient(app).get(path, headers={"Host": host})
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_host"
    assert response.headers["X-Request-ID"]


def test_duplicate_authority_rejected_by_actual_middleware(secured):
    _, _, app, _ = secured
    response = TestClient(app).get("/v1/config/settings", headers=[("Host", "a"), ("Host", "b")])
    assert response.status_code == 400
    assert response.json()["error"] == "invalid_host"


@pytest.mark.parametrize("headers", [
    {"X-APP-TOKEN": "local-audit-token"},
    {"Authorization": "Bearer local-audit-token"},
    {"X-API-Key": "local-audit-token"},
])
def test_valid_token_can_access_mounted_config_route(secured, headers):
    _, _, app, _ = secured
    response = TestClient(app).get("/v1/config/settings", headers=headers)
    assert response.status_code == 200
    assert "settings" in response.json()


def test_child_mount_keeps_route_auth_when_outer_wall_disabled(secured, monkeypatch):
    _, _, _, _ = secured
    monkeypatch.setenv("TFB_GLOBAL_AUTH_ENFORCE", "false")
    child = FastAPI()
    child.include_router(importlib.import_module("routes.config").router)
    parent = FastAPI()
    parent.mount("/nested", child)
    monkeypatch.setattr(Request, "url", property(lambda self: SimpleNamespace(path="/health")))
    client = TestClient(parent)
    assert client.get("/nested/v1/config/settings").status_code == 401
    assert client.get("/nested/v1/config/settings", headers={"X-APP-TOKEN": "local-audit-token"}).status_code == 200


def test_health_ipv6_port_and_cors_preflight_remain_public(secured):
    _, _, app, _ = secured
    client = TestClient(app)
    assert client.get("/health", headers={"Host": "[::1]:8000"}).status_code == 200
    response = client.options("/v1/config/settings", headers={
        "Origin": "https://example.com", "Access-Control-Request-Method": "GET",
    })
    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "*"


@pytest.mark.parametrize("module_name", [
    "routes.analysis_sheet_rows", "routes.advanced_sheet_rows",
    "routes.advanced_analysis", "routes.investment_advisor", "routes.data_dictionary",
    "routes.routes_argaam", "routes.enriched_quote",
])
def test_route_auth_helpers_use_routing_scope(secured, monkeypatch, module_name):
    _, config, _, settings = secured
    module = importlib.import_module(module_name)
    monkeypatch.setattr(module, "get_settings_cached", lambda *a, **k: settings, raising=False)
    monkeypatch.setattr(module, "_core_get_settings_cached", lambda *a, **k: settings, raising=False)
    monkeypatch.setattr(module, "is_open_mode", lambda: False, raising=False)
    request = Request({"type": "http", "method": "GET", "path": "/v1/private",
                       "headers": [], "query_string": b""})
    monkeypatch.setattr(Request, "url", property(lambda self: SimpleNamespace(path="/health")))
    if module_name.endswith(("analysis_sheet_rows", "advanced_sheet_rows", "advanced_analysis")):
        assert module._auth_passed(request=request, settings=settings, auth_token="", authorization=None) is False
    elif module_name.endswith("investment_advisor"):
        assert module._auth_passed(request=request, token_query=None, x_app_token=None, authorization=None) is False
    elif module_name.endswith("data_dictionary"):
        assert module._auth_passed(request=request, token=None, x_app_token=None, authorization=None) is False
    elif module_name.endswith("routes_argaam"):
        assert module._auth_ok_flexible(request, token_q=None, x_app_token=None, x_api_key=None, authorization=None) is False
    else:
        service = module._Service()
        with pytest.raises(HTTPException) as error:
            service.auth_guard(request, None, None, None)
        assert error.value.status_code == 401


def test_readiness_reports_structural_failures_with_legacy_flags_off(secured, monkeypatch):
    main, _, app, _ = secured
    monkeypatch.setattr(main, "_SETTINGS", replace(main._SETTINGS, INIT_ENGINE_ON_BOOT=True))
    app.state.routes_mounted = False
    app.state.routes_snapshot = {"failed_count": 3}
    response = TestClient(app).get("/readyz")
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ready"
    assert payload["ready"] is False
    assert {"routes_not_mounted", "route_module_failure", "engine_not_ready"} <= set(payload["readiness_reasons"])
    health = TestClient(app).get("/health")
    assert health.status_code == 200
    assert health.json()["status"] == "healthy"
    assert health.json()["ready"] is False


def test_strict_readiness_and_health_labels_do_not_change_liveness(secured, monkeypatch):
    _, _, app, _ = secured
    app.state.routes_mounted = False
    monkeypatch.setenv("TFB_READYZ_STRICT", "true")
    monkeypatch.setenv("TFB_HEALTH_STRICT", "true")
    client = TestClient(app)
    assert client.get("/readyz").status_code == 503
    assert client.head("/v1/readyz").status_code == 503
    assert client.get("/health").json()["status"] == "degraded"
    response = client.get("/livez")
    assert response.status_code == 200
    assert response.json()["status"] == "live"


def test_healthy_mounted_app_ready_and_disabled_engine_is_not_required(secured, monkeypatch):
    _, _, app, _ = secured
    app.state.routes_snapshot["failed_count"] = 0
    monkeypatch.setenv("TFB_READYZ_STRICT", "true")
    response = TestClient(app).get("/readyz")
    assert response.status_code == 200, response.json()
    assert response.json()["ready"] is True
    assert response.json()["readiness_reasons"] == []


def test_missing_auth_is_reported_in_production_with_legacy_flag_off(secured, monkeypatch):
    main, _, app, _ = secured
    monkeypatch.setattr(main, "_SETTINGS", replace(main._SETTINGS, REQUIRE_AUTH=False))
    _, reasons = main._readiness_evaluation(app)
    assert "auth_not_enforced" in reasons
    monkeypatch.setattr(main, "_SETTINGS", replace(main._SETTINGS, APP_ENV="testing"))
    _, reasons = main._readiness_evaluation(app)
    assert "auth_not_enforced" not in reasons
