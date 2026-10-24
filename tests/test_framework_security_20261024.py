"""Offline behavioral regressions for the FastAPI/Starlette security migration.

The unchanged application auth/middleware, route ownership, root_path and
OpenAPI contracts live in their existing suites. These cases exercise the
library fixes themselves: GHSA-86qp-5c8j-p5mr / GHSA-jp82-jpqv-5vv3 URL
construction, and GHSA-82w8-qh3p-5jfq urlencoded form limits through FastAPI.
No production form endpoint or vulnerability exposure is implied by this
isolated test app. Bodies are bounded to just over the default 1 MiB limit.
"""

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from starlette.requests import Request as StarletteRequest


_PART_LIMIT = 1024 * 1024


@pytest.mark.parametrize(
    "host",
    ["x/health?x=", "x/health#", "user@attacker.example", "["],
)
def test_request_url_ignores_host_injection_and_preserves_scope_path(host):
    request = StarletteRequest({
        "type": "http",
        "scheme": "https",
        "server": ("origin.example", 443),
        "path": "/v1/analysis/sheet-rows",
        "query_string": b"page=1",
        "headers": [(b"host", host.encode("ascii"))],
    })
    assert request.url.hostname == "origin.example"
    assert request.url.path == "/v1/analysis/sheet-rows"
    assert request.url.query == "page=1"
    assert request.url.fragment == ""


def test_request_url_relative_scope_path_cannot_change_hostname():
    # Invalid request targets can reach ASGI apps on some server stacks. URL
    # assembly must keep path bytes from being parsed as userinfo/netloc.
    request = StarletteRequest({
        "type": "http",
        "scheme": "https",
        "server": ("origin.example", 443),
        "path": "@attacker.example/private",
        "query_string": b"",
        "headers": [(b"host", b"origin.example")],
    })
    assert request.url.hostname == "origin.example"
    assert request.url.path == "/@attacker.example/private"


@pytest.fixture
def form_client():
    pytest.importorskip("python_multipart")
    app = FastAPI()

    @app.post("/bounded-form")
    async def bounded_form(request: Request):
        # max_fields existed in the baseline; it used to be ignored for
        # urlencoded bodies. The default max_part_size is 1 MiB.
        async with request.form(max_fields=3) as form:
            return {"fields": [
                {"name_bytes": len(name.encode()), "value_bytes": len(value.encode())}
                for name, value in form.multi_items()
            ]}

    with TestClient(app) as client:
        yield client


def _post_form(client, body):
    return client.post(
        "/bounded-form",
        content=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )


def test_urlencoded_limit_accepts_boundary_and_preserves_duplicate_fields(form_client):
    response = _post_form(form_client, "a=1&a=2&b=ok")
    assert response.status_code == 200
    assert response.json() == {"fields": [
        {"name_bytes": 1, "value_bytes": 1},
        {"name_bytes": 1, "value_bytes": 1},
        {"name_bytes": 1, "value_bytes": 2},
    ]}


@pytest.mark.parametrize("body", ["a=1&b=2&c=3&d=4", "a=1&a=2&a=3&a=4"])
def test_urlencoded_rejects_excess_fields_including_duplicate_names(form_client, body):
    response = _post_form(form_client, body)
    assert response.status_code == 400
    assert "Too many fields" in response.json()["detail"]


def test_urlencoded_accepts_exact_default_part_size(form_client):
    response = _post_form(form_client, "a=" + "x" * (_PART_LIMIT - 1))
    assert response.status_code == 200
    assert response.json() == {"fields": [
        {"name_bytes": 1, "value_bytes": _PART_LIMIT - 1},
    ]}


@pytest.mark.parametrize("oversized_name", [False, True])
def test_urlencoded_rejects_oversized_field_names_and_values(form_client, oversized_name):
    body = ("x" * _PART_LIMIT + "=a") if oversized_name else ("a=" + "x" * _PART_LIMIT)
    response = _post_form(form_client, body)
    assert response.status_code == 400
    assert "Field exceeded maximum size" in response.json()["detail"]
