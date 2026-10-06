"""Offline publication readiness regressions, independent of FastAPI/httpx."""
from __future__ import annotations

import json
from pathlib import Path
from urllib.error import HTTPError

import pytest

from scripts import check_backend_readiness as readiness


VERSIONS = {"entry_version": "8.14.2", "service_version": "8.14.2", "engine_version": "5.151.0"}
OWNERS = {"/v1/analysis/sheet-rows": "analysis_sheet_rows", "/sheet-rows": "advanced_analysis"}


def healthy():
    return {
        "ready": True, "engine_present": True, "engine_ready": True,
        "routes_mounted": True, "routes_failed_count": 0,
        "readiness_reasons": [], "missing_required_keys": [],
        "canonical_path_owner_mismatches": {},
        "canonical_path_owners": {path: f"routes.{owner}" for path, owner in OWNERS.items()},
        **VERSIONS,
    }


def validate(payload):
    return readiness.validate_readiness(payload, expected_versions=VERSIONS, expected_owners=OWNERS)


def test_healthy_complete_contract_passes():
    assert validate(healthy()) == []


@pytest.mark.parametrize("field,value", [
    ("ready", False), ("ready", "true"), ("ready", 1),
    ("engine_present", False), ("engine_ready", False), ("routes_mounted", False),
    ("routes_failed_count", 1), ("routes_failed_count", "0"), ("routes_failed_count", False),
    ("readiness_reasons", ["engine_not_ready"]), ("readiness_reasons", {}),
    ("missing_required_keys", ["analysis"]), ("missing_required_keys", ""),
    ("canonical_path_owner_mismatches", {"/sheet-rows": {"actual_owner": "wrong"}}),
    ("canonical_path_owner_mismatches", []),
    ("entry_version", "old"), ("service_version", "old"), ("engine_version", "old"),
])
def test_http200_with_broken_or_ambiguous_diagnostics_is_rejected(field, value):
    payload = healthy()
    payload[field] = value
    assert validate(payload)


@pytest.mark.parametrize("field", list(healthy()))
def test_missing_or_redacted_required_diagnostic_is_rejected(field):
    payload = healthy()
    del payload[field]
    assert validate(payload)


def test_reported_empty_mismatches_cannot_hide_missing_or_wrong_owner():
    for owner in (None, "routes.wrong_family"):
        payload = healthy()
        payload["canonical_path_owners"]["/sheet-rows"] = owner
        assert "canonical_owner_mismatch:/sheet-rows" in validate(payload)


def test_optional_advisor_spelling_aliases_need_correct_owner_if_mounted():
    owners = {**OWNERS, "/v1/investment_advisor": "investment_advisor"}
    payload = healthy()
    assert readiness.validate_readiness(payload, expected_versions=VERSIONS, expected_owners=owners) == []
    payload["canonical_path_owners"]["/v1/investment_advisor"] = "routes.wrong_family"
    assert "canonical_owner_mismatch:/v1/investment_advisor" in readiness.validate_readiness(
        payload, expected_versions=VERSIONS, expected_owners=owners,
    )


@pytest.mark.parametrize("payload", [None, [], "ready", {"status": "alive"}])
def test_liveness_and_unstructured_responses_cannot_replace_readiness(payload):
    assert validate(payload)


def test_source_contract_uses_literals_without_application_imports():
    versions, owners = readiness.source_contract(Path(__file__).resolve().parents[1])
    assert versions["entry_version"] == versions["service_version"]
    assert versions["engine_version"]
    assert owners["/v1/analysis/sheet-rows"] == "analysis_sheet_rows"
    assert owners["/sheet-rows"] == "advanced_analysis"


class Response:
    status = 200

    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def read(self, size):
        return json.dumps(self.payload).encode()


@pytest.mark.parametrize("ready,expected", [(True, []), (False, ["ready_not_true"])])
def test_probe_validates_http200_json(monkeypatch, ready, expected):
    payload = healthy()
    payload["ready"] = ready

    class Opener:
        def open(self, request, timeout):
            return Response(payload)

    monkeypatch.setattr(readiness, "build_opener", lambda *args: Opener())
    assert readiness.probe_readiness(
        "https://backend.invalid", expected_versions=VERSIONS, expected_owners=OWNERS,
    ) == (expected, VERSIONS["entry_version"])


def test_probe_authenticates_only_readyz_and_rejects_redirects(monkeypatch):
    seen = []

    class Opener:
        def open(self, request, timeout):
            seen.append(request)
            assert request.get_header("X-app-token") == "private-token"
            raise HTTPError(request.full_url, 302, "redirect", {}, None)

    def build(handler):
        assert handler.redirect_request(None, None, 302, "", {}, "https://other.invalid") is None
        return Opener()

    monkeypatch.setattr(readiness, "build_opener", build)
    reasons, version = readiness.probe_readiness(
        "https://backend.invalid", expected_versions=VERSIONS, expected_owners=OWNERS,
        token="private-token",
    )
    assert reasons == ["readiness_http_302"] and version == ""
    assert [request.full_url for request in seen] == ["https://backend.invalid/readyz"]


def test_failed_cli_guard_does_not_emit_healthy_or_leak_token(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(readiness, "source_contract", lambda root: (VERSIONS, OWNERS))
    monkeypatch.setenv("BACKEND_TOKEN", "private-token")
    monkeypatch.setattr(readiness, "probe_readiness", lambda *args, **kwargs: (["ready_not_true"], ""))
    output = tmp_path / "github-output"
    assert readiness.main([
        "--backend", "https://backend.invalid", "--attempts", "1", "--github-output", str(output),
    ]) == 1
    assert output.read_text() == "health_status=not_ready\n"
    assert "private-token" not in capsys.readouterr().out


def test_healthy_cli_guard_emits_verified_version(monkeypatch, tmp_path):
    monkeypatch.setattr(readiness, "source_contract", lambda root: (VERSIONS, OWNERS))
    monkeypatch.setattr(readiness, "probe_readiness", lambda *args, **kwargs: ([], VERSIONS["entry_version"]))
    output = tmp_path / "github-output"
    assert readiness.main([
        "--backend", "https://backend.invalid", "--attempts", "1", "--github-output", str(output),
    ]) == 0
    assert output.read_text() == "health_status=healthy\nhealth_version=8.14.2\n"


@pytest.mark.parametrize("backend", ["file:///tmp/private", "https://user:password@backend.invalid", "https://backend.invalid?token=secret", "https://[invalid"])
def test_invalid_backend_url_is_rejected_before_io(backend, monkeypatch):
    def forbid(*args, **kwargs):
        pytest.fail("Invalid URL attempted I/O")
    monkeypatch.setattr(readiness, "build_opener", forbid)
    assert readiness.probe_readiness(
        backend, expected_versions=VERSIONS, expected_owners=OWNERS,
    )[0] == ["invalid_backend_url"]
