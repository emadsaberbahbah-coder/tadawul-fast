"""Real caught-route handlers/envelopes must not return credential diagnostics.

AST loading avoids application/configuration bootstrap. Both handlers and
their production envelope implementations execute through offline ASGI.
"""
from __future__ import annotations

import ast
import asyncio
import datetime as dt
from decimal import Decimal
import io
import inspect
import json
import logging
import math
import os
from pathlib import Path
import re
import time
from types import MethodType, SimpleNamespace
import typing
import uuid

import pytest

httpx = pytest.importorskip("httpx")
fastapi = pytest.importorskip("fastapi")
from core import secret_redaction

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def secret(monkeypatch):
    value = "synthetic_" + uuid.uuid4().hex
    monkeypatch.setattr(secret_redaction, "configured_secret_values", lambda: (value,))
    return value


def _load(kind):
    root = Path(os.getenv("TFB_REDACTION_SOURCE_ROOT", str(ROOT)))
    relative = "routes/enriched_quote.py" if kind == "enriched" else "routes/analysis_sheet_rows.py"
    path = root / relative
    tree = ast.parse(path.read_text())
    function = "_sheet_rows_handler" if kind == "enriched" else "_analysis_sheet_rows_impl"
    names = {function, "_json_safe", "_strip", "_bool_from_any", "_int_from_any",
        "_fetch_analysis_rows", "_call_engine", "_maybe_await", "_call_maybe_async",
        "_dict_is_symbol_map", "_rows_to_matrix"}
    if kind == "enriched":
        names.update({"_single_quote_handler", "_normalize_row", "_extract_from_raw", "_key_variants"})
    if kind == "analysis":
        names.add("_payload_envelope")
    nodes = [n for n in tree.body
        if isinstance(n, ast.ImportFrom) and n.module == "core.secret_redaction"]
    nodes.extend(n for n in tree.body
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name in names)
    for n in tree.body:
        if isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in
                {"ROUTER_VERSION", "ANALYSIS_SHEET_ROWS_VERSION"} for t in n.targets):
            nodes.append(n)
    if kind == "enriched":
        nodes.extend(n for n in ast.walk(tree)
            if isinstance(n, ast.FunctionDef) and n.name == "envelope")
    namespace = dict(vars(typing))
    namespace.update({"datetime": dt.datetime, "date": dt.date, "dt_time": dt.time,
        "Decimal": Decimal, "math": math, "time": time, "uuid": uuid,
        "asyncio": asyncio, "inspect": inspect, "re": re, "_FIELD_ALIAS_HINTS": {},
        "Request": fastapi.Request, "HTTPException": fastapi.HTTPException,
        "CORE_ENGINE_SOURCE": "synthetic-offline", "_request_id": lambda request, x: x,
        "_enriched_debug_enabled": lambda: False,
        "_page_from_body": lambda body: body.get("page"),
        "_merge_body_with_query": lambda body, request: dict(body),
        "_normalize_page_flexible": lambda page: page,
        "_pick_page_from_body": lambda body: body.get("page"),
        "_route_family_flexible": lambda page: "instrument",
        "_resolve_contract": lambda page: (["Symbol"], ["symbol"], None, "offline-contract"),
        "_maybe_bool": lambda value, default: default if value is None else bool(value),
        "_requested_symbols_from_body": lambda body: [],
        "_canonical_owner_hint": lambda page, family: {"canonical_owner": "offline"},
        "_apply_route_coherence": lambda *args: None, "_TOP10_PAGE": "Top_10"})
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)
    return namespace, function


async def _exercise(kind, secret, *, failure, success=None):
    namespace, function = _load(kind)
    captured = io.StringIO()
    logger = logging.getLogger("route.boundary.offline." + kind)
    prior = logger.handlers, logger.level, logger.propagate
    handler = logging.StreamHandler(captured)
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logger.handlers = [handler]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    secret_redaction.install_redaction_on_handlers(logger, secret_values=(secret,))
    namespace["logger"] = logger

    def guard(*args, **kwargs):
        if failure is not None:
            raise failure

    async def core(*args, **kwargs):
        if failure is not None:
            raise failure
        return success

    if kind == "enriched":
        service = SimpleNamespace(auth_guard=guard, normalize_page=lambda page: page,
            route_family=lambda page: "instrument", contract=lambda page: (["Symbol"], ["symbol"]))
        service.envelope = MethodType(namespace["envelope"], service)
        namespace["svc"] = service
    else:
        namespace["_analysis_sheet_rows_impl_core"] = core
    app = fastapi.FastAPI()

    @app.post("/synthetic")
    async def route(request: fastapi.Request):
        kwargs = dict(request=request, body={"page": "Market_Leaders", "schema_only": True},
            mode="full", include_matrix_q=False, token=None, x_app_token=None,
            authorization=None, x_request_id="req-offline")
        if kind == "analysis":
            kwargs["x_api_key"] = None
        return await namespace[function](**kwargs)

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,
                raise_app_exceptions=False), base_url="https://route.invalid") as client:
            response = await client.post("/synthetic", json={})
        return response, captured.getvalue()
    finally:
        logger.handlers, level, logger.propagate = prior
        logger.setLevel(level)


def _assert_no_credential(value, secret):
    rendered = value if isinstance(value, str) else json.dumps(value)
    leaked = secret in rendered
    assert leaked is False, "synthetic credential survived a caught-route boundary"


def _assert_error_contract(response, kind):
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == ("error" if kind == "enriched" else "partial")
    assert payload["request_id"] == "req-offline"
    assert payload["page"] == "Market_Leaders" and payload["route_family"] == "instrument"
    assert payload["headers"] == ["Symbol"] and payload["keys"] == ["symbol"]
    assert payload["row_objects"] == [] and payload["rows"] == [] and payload["count"] == 0
    assert payload["meta"]["_engine_error_class"] == "RuntimeError"
    assert "RuntimeError" in payload["error"] and "RuntimeError" in payload["meta"]["_engine_error"]
    return payload


@pytest.mark.parametrize("kind", ["enriched", "analysis"])
@pytest.mark.parametrize("form", ["bare", "query", "header"])
def test_actual_caught_route_envelopes_redact_all_diagnostic_fields(secret, kind, form):
    message = secret if form == "bare" else (
        "https://service.invalid/quote?api_token=" + secret if form == "query" else
        "Authorization: Bearer " + secret)
    response, log_text = asyncio.run(_exercise(kind, secret, failure=RuntimeError(message)))
    payload = _assert_error_contract(response, kind)
    for diagnostic in (payload["error"], payload["detail"], payload["meta"]["_engine_error"]):
        _assert_no_credential(diagnostic, secret)
    _assert_no_credential(log_text, secret)
    assert "RuntimeError" in log_text


@pytest.mark.parametrize("kind", ["enriched", "analysis"])
def test_route_redacts_before_clipping_a_known_bare_key(secret, kind):
    response, _ = asyncio.run(_exercise(kind, secret,
        failure=RuntimeError("x" * 280 + secret + " trailing")))
    payload = _assert_error_contract(response, kind)
    diagnostics = (payload["error"], payload["detail"], payload["meta"]["_engine_error"])
    fragment_survived = any(secret[:6] in value for value in diagnostics)
    assert fragment_survived is False, "credential prefix survived route clipping"
    for diagnostic in diagnostics:
        _assert_no_credential(diagnostic, secret)


@pytest.mark.parametrize("kind", ["enriched", "analysis"])
def test_route_http_exception_still_propagates_exact_status_detail(secret, kind):
    status = 401 if kind == "enriched" else 409
    response, log_text = asyncio.run(_exercise(kind, secret,
        failure=fastapi.HTTPException(status_code=status, detail="offline-control")))
    assert response.status_code == status and response.json() == {"detail": "offline-control"}
    assert log_text == ""


@pytest.mark.parametrize("kind", ["enriched", "analysis"])
def test_route_success_contract_is_unchanged(secret, kind):
    expected = {"status": "success", "request_id": "req-offline",
        "headers": ["Symbol"], "keys": ["symbol"], "rows": [["SYNTHETIC.US"]],
        "meta": {"count": 1}, "ordinary_business_text": secret}
    response, log_text = asyncio.run(_exercise(kind, secret, failure=None, success=expected))
    assert response.status_code == 200 and log_text == ""
    payload = response.json()
    if kind == "analysis":
        assert payload == expected
    else:
        assert payload["status"] == "success" and payload["request_id"] == "req-offline"
        assert payload["headers"] == ["Symbol"] and payload["keys"] == ["symbol"]
        assert payload["rows"] == [] and payload["row_objects"] == []
        assert payload["error"] is None and payload["meta"]["schema_only"] is True


@pytest.mark.parametrize("kind", ["enriched", "analysis"])
def test_actual_engine_batch_failure_and_per_symbol_shell_redact_diagnostics(secret, kind):
    namespace, _ = _load(kind)
    captured = io.StringIO()
    logger = logging.getLogger("route.engine.boundary.offline")
    prior = logger.handlers, logger.level, logger.propagate
    logger.handlers = [logging.StreamHandler(captured)]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    secret_redaction.install_redaction_on_handlers(logger, secret_values=(secret,))
    namespace["logger"] = logger
    calls = []
    healthy = {"symbol": "SYNGOOD.US", "ordinary_business_text": secret}

    class Engine:
        def get_analysis_rows_batch(self, symbols, **kwargs):
            calls.append(("batch", tuple(symbols)))
            raise RuntimeError(secret)

        def get_enriched_quote_dict(self, symbol, **kwargs):
            calls.append(("single", symbol))
            if symbol == "SYNBAD.US":
                raise RuntimeError(secret)
            return healthy

    async def exercise():
        kwargs = {"mode": "full", "page": "Market_Leaders"}
        if kind == "analysis":
            kwargs.update(settings=None, schema=None, body={})
        return await namespace["_fetch_analysis_rows"](Engine(), ["SYNBAD.US", "SYNGOOD.US"], **kwargs)

    try:
        rows, meta = asyncio.run(exercise())
    finally:
        logger.handlers, level, logger.propagate = prior
        logger.setLevel(level)
    assert calls == [("batch", ("SYNBAD.US", "SYNGOOD.US")),
        ("single", "SYNBAD.US"), ("single", "SYNGOOD.US")]
    assert rows["SYNBAD.US"]["symbol"] == "SYNBAD.US"
    assert rows["SYNBAD.US"]["error"].startswith("RuntimeError:")
    _assert_no_credential(rows["SYNBAD.US"]["error"], secret)
    assert rows["SYNGOOD.US"] == healthy
    batch = next(item for item in meta["engine_method_summary"]
        if item["method"] == "get_analysis_rows_batch")
    assert batch["outcome"] == "raised" and batch["error_class"] == "RuntimeError"
    assert "RuntimeError:" not in batch["error_message"]
    _assert_no_credential(batch["error_message"], secret)
    per_symbol = meta["engine_method_summary"][-1]
    assert per_symbol["outcome"] == "partial" and per_symbol["failures"] == 1
    assert per_symbol["total"] == 2
    _assert_no_credential(captured.getvalue(), secret)


async def _single_exercise(secret, failure):
    namespace, _ = _load("enriched")
    logger = logging.getLogger("route.single.boundary.offline")
    captured = io.StringIO()
    prior = logger.handlers, logger.level, logger.propagate
    logger.handlers = [logging.StreamHandler(captured)]
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    secret_redaction.install_redaction_on_handlers(logger, secret_values=(secret,))
    namespace["logger"] = logger

    def guard(*args, **kwargs):
        if failure is not None:
            raise failure

    service = SimpleNamespace(auth_guard=guard, normalize_page=lambda page: page,
        route_family=lambda page: "instrument",
        contract=lambda page: (["Symbol", "Error"], ["symbol", "error"]))
    service.envelope = MethodType(namespace["envelope"], service)
    namespace["svc"] = service
    namespace["_normalize_symbol_token"] = lambda value: value
    healthy = {"symbol": "SYNGOOD.US", "error": None, "ordinary_business_text": secret}

    async def build(*args, **kwargs):
        return [healthy], 0, {"engine_method_used": "offline"}

    namespace["_build_instrument_rows"] = build
    app = fastapi.FastAPI()

    @app.post("/synthetic-single")
    async def route(request: fastapi.Request):
        return await namespace["_single_quote_handler"](request=request,
            body={"symbol": "SYNGOOD.US"}, page_q="Market_Leaders", mode_q="full",
            token_q=None, x_app_token=None, authorization=None, x_request_id="req-offline")

    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app,
                raise_app_exceptions=False), base_url="https://route.invalid") as client:
            response = await client.post("/synthetic-single", json={})
        return response, captured.getvalue(), healthy
    finally:
        logger.handlers, level, logger.propagate = prior
        logger.setLevel(level)


@pytest.mark.parametrize("form", ["bare", "query", "header"])
def test_actual_single_quote_handler_redacts_envelope_and_normalized_error_row(secret, form):
    message = secret if form == "bare" else (
        "https://service.invalid/quote?api_token=" + secret if form == "query" else
        "Authorization: Bearer " + secret)
    response, log_text, _ = asyncio.run(_single_exercise(secret, RuntimeError(message)))
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "error" and payload["request_id"] == "req-offline"
    assert payload["headers"] == ["Symbol", "Error"] and payload["keys"] == ["symbol", "error"]
    assert payload["meta"]["_engine_error_class"] == "RuntimeError"
    assert payload["row"] == payload["quote"] == payload["row_objects"][0]
    assert payload["row"]["symbol"] == "" and payload["row"]["error"].startswith("RuntimeError:")
    _assert_no_credential(payload, secret)
    _assert_no_credential(log_text, secret)


def test_single_quote_http_exception_still_propagates(secret):
    response, log_text, _ = asyncio.run(_single_exercise(secret,
        fastapi.HTTPException(status_code=401, detail="offline-control")))
    assert response.status_code == 401 and response.json() == {"detail": "offline-control"}
    assert log_text == ""


def test_single_quote_success_preserves_business_payload_and_aliases(secret):
    response, log_text, healthy = asyncio.run(_single_exercise(secret, None))
    assert response.status_code == 200 and log_text == ""
    payload = response.json()
    assert payload["status"] == "success" and payload["request_id"] == "req-offline"
    assert payload["row"] == payload["quote"] == payload["row_objects"][0] == healthy
    assert payload["rows"] == [["SYNGOOD.US", None]]
