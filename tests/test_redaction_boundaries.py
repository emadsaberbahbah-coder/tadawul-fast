"""Exercise real diagnostic boundaries with synthetic secrets and no network.

Source definitions are loaded without application/configuration bootstrap. The
real BackendClient, main logging/error handler, calendar request, and httpx
transports execute; only external services and settings are replaced.
"""
from __future__ import annotations

import ast
import asyncio
import dataclasses
import datetime as dt
import enum
import importlib
import io
import json
import logging
import math
import os
from pathlib import Path
from types import SimpleNamespace
import typing
from urllib.parse import quote, quote_plus
import uuid

import pytest

httpx = pytest.importorskip("httpx")
ROOT = Path(__file__).resolve().parents[1]


def _source(relative):
    root = Path(os.getenv("TFB_REDACTION_SOURCE_ROOT", str(ROOT)))
    return root / relative


def _definitions(relative, names, namespace, *, nested=()):
    path = _source(relative)
    tree = ast.parse(path.read_text())
    selected = [n for n in tree.body
        if isinstance(n, ast.ImportFrom) and n.module == "core.secret_redaction"]
    selected.extend(n for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
        and n.name in names)
    selected.extend(n for n in ast.walk(tree)
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        and n.name in nested)
    namespace.update(vars(typing))
    exec(compile(ast.Module(body=selected, type_ignores=[]), str(path), "exec"), namespace)
    return SimpleNamespace(**namespace)


@pytest.fixture
def credentials(monkeypatch):
    values = tuple("synthetic_" + uuid.uuid4().hex for _ in range(3))
    try:
        helper = importlib.import_module("core.secret_redaction")
    except ModuleNotFoundError:
        pass  # Baseline source must exercise its real unprotected boundaries.
    else:
        monkeypatch.setattr(helper, "configured_secret_values", lambda: values)
    return values


def _diagnostic(values):
    return ("upstream https://service.invalid/quote?api_token=" + values[0]
        + " Authorization: Bearer " + values[1] + "\nraw echo " + values[2])


def _assert_no_credentials(value, values):
    rendered = value if isinstance(value, str) else json.dumps(value)
    variants = [variant for secret in values
        for variant in (secret, quote(secret, safe=""), quote_plus(secret))]
    leaked = any(variant in rendered for variant in variants)
    assert leaked is False, "synthetic credential survived a diagnostic boundary"


def test_backend_get_success_remains_exact_and_does_not_retry(credentials):
    cls, sleeps = _backend()
    observed = []
    expected = {"status": "success", "request_id": "req-synthetic", "meta": {"ready": True}}

    async def respond(request):
        observed.append(request)
        return httpx.Response(200, json=expected, request=request)

    async def exercise():
        client = cls("https://backend.invalid", token=credentials[2])
        client._client = httpx.AsyncClient(transport=httpx.MockTransport(respond),
            headers=client._headers())
        try:
            return await client.get_json("/meta")
        finally:
            await client.close()

    assert asyncio.run(exercise()) == (expected, None, 200)
    assert len(observed) == 1 and sleeps == []


@pytest.fixture
def logging_state():
    names = ("", "main", "httpx", "uvicorn", "uvicorn.error", "uvicorn.access",
        "gunicorn", "gunicorn.error", "calendar.boundary.offline")
    state = [(logging.getLogger(name), list(logging.getLogger(name).handlers),
        logging.getLogger(name).level, logging.getLogger(name).propagate,
        logging.getLogger(name).disabled) for name in names]
    yield
    for logger, handlers, level, propagate, disabled in state:
        logger.handlers = handlers
        logger.setLevel(level)
        logger.propagate = propagate
        logger.disabled = disabled


def _backend():
    sleeps = []

    async def sleep(delay):
        sleeps.append(delay)

    namespace = {"asyncio": SimpleNamespace(sleep=sleep),
        "random": SimpleNamespace(uniform=lambda a, b: 0), "json": json,
        "os": SimpleNamespace(getenv=lambda name, default=None: default)}
    source = _definitions("scripts/run_dashboard_sync.py", {"BackendClient"}, namespace)
    return source.BackendClient, sleeps


@pytest.mark.parametrize("method", ["get", "post"])
@pytest.mark.parametrize("failure", ["http_body", "transport", "json_parse"])
def test_backend_real_error_paths_redact_and_preserve_auth_status_retry(credentials, method, failure):
    cls, sleeps = _backend()
    observed = []
    diagnostic = _diagnostic(credentials)

    class BadJsonResponse(httpx.Response):
        def json(self, **kwargs):
            raise ValueError(diagnostic)

    async def respond(request):
        observed.append(request)
        if failure == "transport":
            raise httpx.ConnectError(diagnostic, request=request)
        if failure == "json_parse":
            return BadJsonResponse(200, request=request)
        return httpx.Response(403, text=diagnostic, request=request)

    async def exercise():
        client = cls("https://backend.invalid", token=credentials[2])
        client._client = httpx.AsyncClient(transport=httpx.MockTransport(respond),
            headers=client._headers())
        try:
            if method == "get":
                return await client.get_json("/meta")
            return await client.post_json("/sheet-rows", {"symbols": ["SYNTHETIC.US"]})
        finally:
            await client.close()

    payload, error, status = asyncio.run(exercise())
    assert payload is None and isinstance(error, str)
    assert status == (0 if failure == "transport" else 200 if failure == "json_parse" else 403)
    expected_attempts = 3 if method == "post" and failure == "transport" else 1
    assert len(observed) == expected_attempts
    assert len(sleeps) == expected_attempts - 1
    auth_ok = all(request.headers.get("Authorization") == "Bearer " + credentials[2]
        and request.headers.get("X-APP-TOKEN") == credentials[2] for request in observed)
    assert auth_ok is True
    assert all(request.method == method.upper() for request in observed)
    if method == "post":
        assert all(json.loads(request.content) == {"symbols": ["SYNTHETIC.US"]}
            for request in observed)
    if failure == "http_body":
        assert error.startswith("HTTP 403:")
    elif failure == "json_parse":
        assert "JSON parse error" in error
    _assert_no_credentials(error, credentials)


@pytest.mark.parametrize("statuses", [(429, 503, 200), (503, 503, 503)])
def test_backend_post_retry_success_and_exhausted_body_are_preserved(credentials, statuses):
    cls, sleeps = _backend()
    observed = []
    expected = {"status": "success", "rows": [{"symbol": "SYNTHETIC.US", "value": 7}]}

    async def respond(request):
        observed.append(request)
        status = statuses[len(observed) - 1]
        return httpx.Response(status, json=expected, request=request) if status == 200 else (
            httpx.Response(status, text=_diagnostic(credentials), request=request))

    async def exercise():
        client = cls("https://backend.invalid", token=credentials[2])
        client._client = httpx.AsyncClient(transport=httpx.MockTransport(respond),
            headers=client._headers())
        try:
            return await client.post_json("/sheet-rows", {"request_id": "req-synthetic"})
        finally:
            await client.close()

    payload, error, status = asyncio.run(exercise())
    assert len(observed) == 3 and sleeps == [1.0, 2.0]
    assert status == statuses[-1]
    if status == 200:
        assert payload == expected and error is None
    else:
        assert payload is None and error.startswith("HTTP 503:")
        _assert_no_credentials(error, credentials)


def test_backend_body_redacts_known_bare_key_before_clipping(credentials):
    cls, _ = _backend()
    body = "x" * 185 + credentials[2] + " trailing diagnostic"

    async def respond(request):
        return httpx.Response(403, text=body, request=request)

    async def exercise():
        client = cls("https://backend.invalid", token=credentials[2])
        client._client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        try:
            return await client.get_json("/meta")
        finally:
            await client.close()

    payload, error, status = asyncio.run(exercise())
    assert payload is None and status == 403 and error.startswith("HTTP 403:")
    fragment_survived = credentials[2][:15] in error
    assert fragment_survived is False, "credential prefix survived truncation"
    _assert_no_credentials(error, credentials)


def _main(*, json_logs=False, expose=True, app=None):
    fastapi = pytest.importorskip("fastapi")
    from fastapi.encoders import jsonable_encoder
    from fastapi.responses import JSONResponse
    from decimal import Decimal

    env = {"LOG_LEVEL": "INFO", "LOG_JSON": "1" if json_logs else "0"}
    namespace = {"logging": logging, "sys": __import__("sys"), "json": json,
        "datetime": dt.datetime, "date": dt.date, "dt_time": dt.time,
        "timezone": dt.timezone, "math": math, "Decimal": Decimal,
        "Enum": enum.Enum, "uuid": uuid, "re": __import__("re"),
        "is_dataclass": dataclasses.is_dataclass, "asdict": dataclasses.asdict,
        "jsonable_encoder": jsonable_encoder, "JSONResponse": JSONResponse,
        "Request": fastapi.Request, "_REQUEST_ID_MAX_LEN": 128,
        "_REQUEST_ID_UNSAFE_RE": __import__("re").compile(r"[^A-Za-z0-9._:/-]+"),
        "_env_str": lambda name, default="": env.get(name, default),
        "_env_bool": lambda name, default=False: env.get(name, "1" if default else "0") == "1",
        "_SETTINGS": SimpleNamespace(DEBUG=False, EXPOSE_ERROR_DETAILS=expose, APP_ENV="production")}
    names = {"_scrub_text", "_json_safe", "_StrictJSONResponse", "_err_to_str",
        "_is_production_env", "_expose_error_details", "_public_error_text",
        "_JsonFormatter", "_setup_logging", "_request_id_from_request", "_normalize_request_id"}
    if app is not None:
        namespace.update(app=app, logger=logging.getLogger("main"))
    return _definitions("main.py", names, namespace,
        nested={"unhandled_exception_handler"} if app is not None else ())


@pytest.mark.parametrize("json_logs", [False, True])
@pytest.mark.parametrize("preinstalled", [False, True])
def test_main_actual_logging_setup_covers_message_traceback_existing_handlers(
        credentials, logging_state, json_logs, preinstalled):
    source = _main(json_logs=json_logs)
    root = logging.getLogger()
    root.handlers = []
    captured = io.StringIO()
    if preinstalled:
        handler = logging.StreamHandler(captured)
        handler.setFormatter(source._JsonFormatter() if json_logs else
            logging.Formatter("existing|%(levelname)s|%(message)s"))
        root.addHandler(handler)
    logger = source._setup_logging()
    logger.handlers = []
    logger.propagate = True
    if not preinstalled:
        assert len(root.handlers) == 1
        root.handlers[0].setStream(captured)
    try:
        raise RuntimeError(_diagnostic(credentials))
    except RuntimeError:
        logger.error("upstream diagnostic: %s", _diagnostic(credentials), exc_info=True,
            extra={"request_id": "req-synthetic", "path": "/synthetic", "status_code": 503})
    rendered = captured.getvalue()
    _assert_no_credentials(rendered, credentials)
    assert "RuntimeError" in rendered and "upstream diagnostic" in rendered
    if json_logs:
        payload = json.loads(rendered)
        assert payload["level"] == "ERROR" and payload["request_id"] == "req-synthetic"
        assert payload["path"] == "/synthetic" and payload["status_code"] == 503
        assert "RuntimeError" in payload["exc"]
    elif preinstalled:
        assert rendered.startswith("existing|ERROR|")


def test_main_installs_redaction_on_existing_named_handler(credentials, logging_state):
    source = _main()
    root = logging.getLogger()
    root.handlers = [logging.NullHandler()]
    captured = io.StringIO()
    named = logging.getLogger("uvicorn.error")
    handler = logging.StreamHandler(captured)
    handler.setFormatter(logging.Formatter("named|%(message)s"))
    named.handlers = [handler]
    named.propagate = False
    source._setup_logging()
    named.error("credential diagnostic: %s", _diagnostic(credentials))
    _assert_no_credentials(captured.getvalue(), credentials)
    assert captured.getvalue().startswith("named|credential diagnostic:")


@pytest.mark.parametrize("expose", [False, True])
def test_main_actual_public_error_handler_keeps_status_request_id_and_success(
        credentials, logging_state, expose):
    fastapi = pytest.importorskip("fastapi")
    app = fastapi.FastAPI()
    source = _main(expose=expose, app=app)
    root = logging.getLogger()
    captured = io.StringIO()
    handler = logging.StreamHandler(captured)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root.handlers = [handler]
    source._setup_logging()
    logger = logging.getLogger("main")
    logger.handlers = []
    logger.propagate = True

    @app.get("/synthetic-error")
    async def error_route():
        raise RuntimeError(_diagnostic(credentials))

    expected = {"status": "success", "currency": "USD", "nested": {"count": 7}}

    @app.get("/synthetic-success")
    async def success_route():
        return expected

    async def exercise():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,
                raise_app_exceptions=False), base_url="https://app.invalid") as client:
            error = await client.get("/synthetic-error", headers={"X-Request-ID": "req-synthetic"})
            success = await client.get("/synthetic-success")
            return error, success

    error, success = asyncio.run(exercise())
    assert error.status_code == 500 and error.headers["X-Request-ID"] == "req-synthetic"
    payload = error.json()
    assert payload["status"] == "error" and payload["request_id"] == "req-synthetic"
    assert payload["path"] == "/synthetic-error"
    _assert_no_credentials(payload, credentials)
    _assert_no_credentials(captured.getvalue(), credentials)
    if expose:
        assert "RuntimeError" in payload["error"]
    else:
        assert payload["error"] == "internal_server_error"
    assert success.status_code == 200 and success.json() == expected


def _calendar(credentials, captured, respond):
    logger = logging.getLogger("calendar.boundary.offline")
    logger.handlers = [logging.StreamHandler(captured)]
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    namespace = {"httpx": httpx, "_dt": dt, "asyncio": asyncio,
        "__version__": "synthetic-offline", "logger": logger,
        "is_enabled": lambda: True, "_api_key": lambda: credentials[0],
        "_base_url": lambda: "https://calendar.invalid/api",
        "_split_symbols": lambda symbols: ({"SYNTHETIC.US": "SYNTHETIC"}, []),
        "_today": lambda: dt.date(2026, 10, 8),
        "_env_int": lambda name, default, **kwargs: default,
        "_client": lambda: httpx.AsyncClient(transport=httpx.MockTransport(respond))}
    return _definitions("core/providers/calendar_provider.py",
        {"_get_json", "fetch_earnings_map", "_iso", "_parse_date"}, namespace)


@pytest.mark.parametrize("status", [401, 503])
def test_calendar_actual_httpx_status_error_cannot_log_query_key(
        credentials, logging_state, status):
    captured = io.StringIO()
    requests = []

    async def respond(request):
        requests.append(request)
        return httpx.Response(status, text="synthetic upstream failure", request=request)

    source = _calendar(credentials, captured, respond)
    result = asyncio.run(source.fetch_earnings_map(["SYNTHETIC"]))
    assert result == {} and len(requests) == 1
    query_ok = requests[0].url.params.get("api_token") == credentials[0]
    assert query_ok is True
    assert "earnings batch failed" in captured.getvalue()
    assert str(status) in captured.getvalue()
    _assert_no_credentials(captured.getvalue(), credentials)


def test_calendar_success_keeps_earliest_future_mapping_and_one_request(credentials, logging_state):
    requests = []

    async def respond(request):
        requests.append(request)
        return httpx.Response(200, json={"earnings": [
            {"code": "SYNTHETIC.US", "report_date": "2026-10-07"},
            {"code": "SYNTHETIC.US", "report_date": "2026-10-12"},
            {"code": "SYNTHETIC.US", "report_date": "2026-10-10"}]}, request=request)

    source = _calendar(credentials, io.StringIO(), respond)
    assert asyncio.run(source.fetch_earnings_map(["SYNTHETIC"])) == {"SYNTHETIC": "2026-10-10"}
    assert len(requests) == 1
