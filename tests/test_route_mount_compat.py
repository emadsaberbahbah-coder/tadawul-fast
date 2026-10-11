"""Mounted-route truth on eager and lazy FastAPI router representations."""
from types import SimpleNamespace

import pytest


def test_route_traversal_preserves_eager_routes_and_expands_effective_contexts():
    from main import _iter_effective_routes
    eager = SimpleNamespace(path="/probe", methods={"GET"})
    context = SimpleNamespace(path="/prefix/sheet-rows", methods={"POST"})
    lazy = SimpleNamespace(effective_route_contexts=lambda: iter([context]))
    assert list(_iter_effective_routes([eager, lazy])) == [eager, context]


def test_actual_route_families_owners_and_mount_idempotence():
    import main
    app = main.create_app()
    assert main._verify_required_route_families(app) == []
    owners = main._canonical_path_owners_from_routes(app)
    assert main._canonical_path_owner_mismatches(owners) == {}
    for path in ("/v1/advisor/sheet-rows", "/v1/advanced/sheet-rows", "/v1/analysis/sheet-rows",
                 "/v1/schema/sheet-spec", "/v1/enriched/sheet-rows", "/sheet-rows"):
        owner = main._CONTROLLED_CANONICAL_OWNER_MAP[path]
        assert path in owners, path
        assert owners[path].rsplit(".", 1)[-1] == owner
    before = main._app_route_signature_set(app)
    snapshot = main._mount_routes_once(app, phase="repeat-test")
    assert main._app_route_signature_set(app) == before
    assert snapshot["missing_required_keys"] == []
    assert snapshot["route_signature_count_after_mount"] == len(before)


def test_price_precision_snapshot_reaches_health_without_gate_allowlist(monkeypatch):
    import main
    from core import data_engine_v2 as de
    from fastapi.testclient import TestClient
    monkeypatch.setenv("TFB_OPEN_MODE", "true")
    monkeypatch.setenv("OPEN_MODE", "true")
    monkeypatch.setattr(de, "surface_gate_states", lambda: {"price_precision": "on"})
    response = TestClient(main.create_app()).get("/health")
    assert response.status_code == 200
    assert response.json()["engine_gates"]["price_precision"] == "on"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
