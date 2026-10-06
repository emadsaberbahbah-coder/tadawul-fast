"""Request authority and route-path checks independent of URL reconstruction."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def request_path(request: Any) -> str:
    """Use the same ASGI path as routing for authorization and diagnostics."""
    scope = getattr(request, "scope", None)
    if isinstance(scope, Mapping):
        path = scope.get("path")
        if isinstance(path, str):
            return path
    return ""


def invalid_host_authority(scope: Mapping[str, Any]) -> bool:
    """Reject duplicate Host headers and URL delimiters/control characters.

    This permits deployment domains, localhost, ports and bracketed IPv6.
    Missing Host remains compatible with HTTP/1.0 and direct ASGI callers.
    """
    hosts = [
        value for name, value in scope.get("headers", [])
        if name.lower() == b"host"
    ]
    if len(hosts) > 1:
        return True
    if not hosts:
        return False
    host = hosts[0]
    return (
        not host
        or any(char <= 32 or char >= 127 for char in host)
        or any(char in host for char in (b"/", b"\\", b"?", b"#", b"@"))
    )
