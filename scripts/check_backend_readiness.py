#!/usr/bin/env python3
"""Fail closed before publishing decisions from an unready backend.

Uses authenticated /readyz diagnostics and source literals without importing
the application or providers. HTTP 200 alone is insufficient: nonstrict
readiness intentionally returns 200 while exposing ready=false.
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import sys
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

SCRIPT_VERSION = "1.0.0"
# main protects ownership of these optional advisor spelling aliases if they
# are mounted; required-family presence accepts the canonical /v1/advisor.
_OPTIONAL_CANONICAL_ALIASES = frozenset({"/v1/investment_advisor", "/v1/investment-advisor"})


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        # Do not forward the backend token to a redirect destination.
        return None


def _source_literal(path: Path, name: str) -> Any:
    for node in ast.parse(path.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, ast.AnnAssign):
            targets = [node.target]
        else:
            continue
        if any(isinstance(target, ast.Name) and target.id == name for target in targets):
            return ast.literal_eval(node.value)
    raise ValueError(f"Missing source contract: {path.name}:{name}")


def source_contract(root: Path) -> tuple[dict[str, str], dict[str, str]]:
    entry = _source_literal(root / "main.py", "APP_ENTRY_VERSION")
    engine = _source_literal(root / "core/data_engine_v2.py", "__version__")
    owners = _source_literal(root / "main.py", "_CONTROLLED_CANONICAL_OWNER_MAP")
    if not all(isinstance(value, str) and value for value in (entry, engine)):
        raise ValueError("Invalid source version contract")
    if not isinstance(owners, dict) or not owners:
        raise ValueError("Invalid source canonical owner contract")
    if not all(isinstance(path, str) and isinstance(owner, str) and owner for path, owner in owners.items()):
        raise ValueError("Invalid source canonical owner entries")
    return {
        "entry_version": entry,
        "service_version": entry,
        "engine_version": engine,
    }, owners


def validate_readiness(
    payload: Any,
    *,
    expected_versions: Mapping[str, str],
    expected_owners: Mapping[str, str],
) -> list[str]:
    """Return every violated readiness condition; require typed diagnostics."""
    if not isinstance(payload, Mapping):
        return ["readiness_payload_not_object"]
    reasons = []
    for field in ("ready", "engine_present", "engine_ready", "routes_mounted"):
        if payload.get(field) is not True:
            reasons.append(f"{field}_not_true")
    if type(payload.get("routes_failed_count")) is not int or payload["routes_failed_count"] != 0:
        reasons.append("routes_failed_count_not_zero")
    for field in ("readiness_reasons", "missing_required_keys"):
        value = payload.get(field)
        if not isinstance(value, list) or value:
            reasons.append(f"{field}_not_empty_list")
    mismatches = payload.get("canonical_path_owner_mismatches")
    if not isinstance(mismatches, Mapping) or mismatches:
        reasons.append("canonical_path_owner_mismatches_not_empty_object")
    owners = payload.get("canonical_path_owners")
    if not isinstance(owners, Mapping):
        reasons.append("canonical_path_owners_missing")
    else:
        for path, expected_owner in expected_owners.items():
            actual = owners.get(path)
            if path in _OPTIONAL_CANONICAL_ALIASES and path not in owners:
                continue
            if not isinstance(actual, str) or actual.rsplit(".", 1)[-1] != expected_owner:
                reasons.append(f"canonical_owner_mismatch:{path}")
    for field, expected in expected_versions.items():
        if payload.get(field) != expected:
            reasons.append(f"version_mismatch:{field}")
    return reasons


def probe_readiness(
    backend: str,
    *,
    expected_versions: Mapping[str, str],
    expected_owners: Mapping[str, str],
    token: str = "",
    timeout: float = 10,
) -> tuple[list[str], str]:
    try:
        parsed = urlsplit(backend)
        hostname = parsed.hostname
    except ValueError:
        return ["invalid_backend_url"], ""
    if (parsed.scheme not in {"http", "https"} or not hostname
            or parsed.username or parsed.password or parsed.query or parsed.fragment):
        return ["invalid_backend_url"], ""
    headers = {"Accept": "application/json"}
    if token:
        headers["X-APP-TOKEN"] = token
    request = Request(backend.rstrip("/") + "/readyz", headers=headers)
    try:
        with build_opener(_NoRedirect()).open(request, timeout=timeout) as response:
            if response.status != 200:
                return [f"readiness_http_{response.status}"], ""
            payload = json.loads(response.read(1024 * 1024))
    except HTTPError as exc:
        return [f"readiness_http_{exc.code}"], ""
    except (URLError, TimeoutError, OSError, ValueError):
        return ["readiness_transport_or_json_failure"], ""
    reasons = validate_readiness(
        payload, expected_versions=expected_versions, expected_owners=expected_owners,
    )
    version = payload.get("entry_version", "") if isinstance(payload, Mapping) else ""
    return reasons, version if isinstance(version, str) else ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend", required=True)
    parser.add_argument("--repo-root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--attempts", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args(argv)
    if not 1 <= args.attempts <= 6 or not 0 < args.timeout <= 30:
        parser.error("attempts must be 1..6 and timeout must be 0..30 seconds")
    try:
        versions, owners = source_contract(args.repo_root)
    except (OSError, ValueError, SyntaxError) as exc:
        print(f"::error::Cannot read readiness source contract ({type(exc).__name__}).")
        return 1
    token = (os.environ.get("BACKEND_TOKEN") or os.environ.get("APP_TOKEN") or "").strip()
    for attempt in range(1, args.attempts + 1):
        reasons, version = probe_readiness(
            args.backend, expected_versions=versions, expected_owners=owners,
            token=token, timeout=args.timeout,
        )
        if not reasons:
            print(f"Backend readiness verified: entry={version}, engine={versions['engine_version']}.")
            if args.github_output:
                with args.github_output.open("a", encoding="utf-8") as output:
                    output.write(f"health_status=healthy\nhealth_version={version}\n")
            return 0
        print(f"Readiness attempt {attempt}/{args.attempts} failed: {', '.join(reasons)}")
        if attempt < args.attempts:
            time.sleep(min(2 ** attempt, 16))
    if args.github_output:
        with args.github_output.open("a", encoding="utf-8") as output:
            output.write("health_status=not_ready\n")
    print("::error::Backend readiness rejected; decision publication cannot proceed.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
