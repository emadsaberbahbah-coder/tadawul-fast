"""Exercise intraday HTTP authentication with captured requests, never a server."""
from __future__ import annotations

import ast
import os
from pathlib import Path
import socket
from types import SimpleNamespace
import urllib.error
import urllib.parse
import urllib.request

import pytest
import yaml

from scripts import intraday_quote_refresh as iqr


ROOT = Path(__file__).resolve().parents[1]
TOKEN_ENV = ("TFB_TOKEN", "X_APP_TOKEN", "APP_TOKEN", "BACKEND_TOKEN")
PRIVATE_TOKEN = "synthetic-private-backend-token"


class Response:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return b'{"data": []}'


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    for name in TOKEN_ENV:
        monkeypatch.delenv(name, raising=False)

    def fail_network(*_args, **_kwargs):
        raise AssertionError("unexpected network in intraday auth regression")

    monkeypatch.setattr(socket, "create_connection", fail_network)
    monkeypatch.setattr(socket.socket, "connect", fail_network)
    monkeypatch.setattr(socket.socket, "connect_ex", fail_network)
    monkeypatch.setattr(urllib.request, "urlopen", fail_network)


def capture_requests(monkeypatch, error=None):
    calls = []

    def urlopen(request, *, timeout):
        assert isinstance(request, urllib.request.Request)
        calls.append((request, timeout))
        if error is not None:
            raise error
        return Response()

    monkeypatch.setattr(urllib.request, "urlopen", urlopen)
    return calls


def fetch():
    return iqr.fetch_quotes("https://synthetic.invalid/", "Market_Leaders",
                            ["SYNTH.US", "SYNTH.SR"], timeout=17)


def daily_auth_headers():
    """Run the actual daily client's two pure helpers without its import graph."""
    tree = ast.parse((ROOT / "scripts/run_dashboard_sync.py").read_text())
    loader = next(node for node in tree.body
                  if isinstance(node, ast.FunctionDef) and node.name == "_env_token")
    client = next(node for node in tree.body
                  if isinstance(node, ast.ClassDef) and node.name == "BackendClient")
    headers = next(node for node in client.body
                   if isinstance(node, ast.FunctionDef) and node.name == "_headers")
    module = ast.Module(body=[loader, headers], type_ignores=[])
    namespace = {"os": os, "Dict": dict}
    exec(compile(ast.fix_missing_locations(module), "<daily auth contract>", "exec"), namespace)
    return namespace["_headers"](SimpleNamespace(token=namespace["_env_token"]()))


@pytest.mark.parametrize("values", [
    {"BACKEND_TOKEN": PRIVATE_TOKEN},
    {"APP_TOKEN": PRIVATE_TOKEN},
    {"X_APP_TOKEN": PRIVATE_TOKEN},
    {"TFB_TOKEN": PRIVATE_TOKEN},
    {name: name + "-synthetic-token" for name in TOKEN_ENV},
    {"TFB_TOKEN": "  ", "X_APP_TOKEN": "  ", "APP_TOKEN": "  ",
     "BACKEND_TOKEN": "  " + PRIVATE_TOKEN + "  "},
])
def test_request_matches_daily_token_precedence_and_headers(monkeypatch, capsys, values):
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    calls = capture_requests(monkeypatch)

    assert fetch() == {}

    assert len(calls) == 1
    request, timeout = calls[0]
    headers = dict((key.lower(), value) for key, value in request.header_items())
    assert headers == {key.lower(): value for key, value in daily_auth_headers().items()}
    assert request.get_method() == "GET" and timeout == 17
    parsed = urllib.parse.urlsplit(request.full_url)
    assert parsed.path == "/v1/analysis/sheet-rows"
    assert urllib.parse.parse_qs(parsed.query) == {
        "page": ["Market_Leaders"], "symbols": ["SYNTH.US,SYNTH.SR"], "limit": ["2"],
    }
    assert all(value.strip() not in request.full_url for value in values.values() if value.strip())
    captured = capsys.readouterr()
    assert PRIVATE_TOKEN not in captured.out + captured.err


def test_anonymous_development_request_stays_supported(monkeypatch):
    calls = capture_requests(monkeypatch)
    assert fetch() == {}
    request, _ = calls[0]
    assert {key.lower(): value for key, value in request.header_items()} == {
        "accept": "application/json",
    }


@pytest.mark.parametrize("error", [
    urllib.error.HTTPError("https://synthetic.invalid/?private=" + PRIVATE_TOKEN,
                           401, "private response " + PRIVATE_TOKEN, {}, None),
    urllib.error.URLError("private transport response " + PRIVATE_TOKEN),
])
def test_authenticated_failure_remains_sanitized_without_retry(monkeypatch, capsys, error):
    monkeypatch.setenv("BACKEND_TOKEN", PRIVATE_TOKEN)
    calls = capture_requests(monkeypatch, error)

    with pytest.raises(iqr.QuoteFetchError) as raised:
        fetch()

    assert str(raised.value) == type(error).__name__
    assert raised.value.quotes == {} and len(calls) == 1
    captured = capsys.readouterr()
    assert PRIVATE_TOKEN not in str(raised.value) + captured.out + captured.err


def test_empty_symbol_list_does_not_make_an_authenticated_request(monkeypatch):
    monkeypatch.setenv("BACKEND_TOKEN", PRIVATE_TOKEN)
    calls = capture_requests(monkeypatch)
    assert iqr.fetch_quotes("https://synthetic.invalid", "Market_Leaders", []) == {}
    assert calls == []


def test_workflow_passes_the_existing_backend_token_secret_to_refresh():
    workflow = yaml.safe_load((ROOT / ".github/workflows/intraday_refresh.yml").read_text())
    step = next(item for item in workflow["jobs"]["refresh"]["steps"]
                if item["name"] == "Refresh decision-symbol quotes")
    assert step["env"]["BACKEND_TOKEN"] == "${{ secrets.BACKEND_TOKEN }}"
