"""Authentication for the Site's one-purpose, read-only spreadsheet relay.

This key is deliberately separate from every general backend token.  The
boundary is enforced even when the application's legacy OPEN_MODE is enabled.
"""
from __future__ import annotations

import hmac
import os
import re
from typing import Any

SOURCE_READ_PATH = "/v1/platform/source-read"
SOURCE_READ_HEADER = "x-tfb-source-read-key"
SOURCE_READ_ENV = "TFB_PLATFORM_SOURCE_READ_KEY"
_KEY_PATTERN = re.compile(r"[!-~]{32,256}\Z")


def _scope(request: Any) -> dict:
    value = getattr(request, "scope", None)
    return value if isinstance(value, dict) else {}


def _keys(request: Any) -> list[bytes]:
    result = []
    for pair in _scope(request).get("headers", []):
        if not isinstance(pair, (tuple, list)) or len(pair) != 2:
            continue
        name, value = pair
        if isinstance(name, bytes) and name.lower() == SOURCE_READ_HEADER.encode("ascii"):
            result.append(value)
    return result


def platform_source_boundary_required(request: Any) -> bool:
    """The exact endpoint, or any attempted use of its dedicated credential."""
    return _scope(request).get("path") == SOURCE_READ_PATH or bool(_keys(request))


def platform_source_authorized(request: Any) -> bool:
    """Authorize only the root-mounted POST, without query or header aliases."""
    scope = _scope(request)
    if (scope.get("type") != "http" or scope.get("method") != "POST"
            or scope.get("path") != SOURCE_READ_PATH
            or scope.get("root_path", "") != ""
            or scope.get("query_string", b"")):
        return False
    values = _keys(request)
    configured = os.environ.get(SOURCE_READ_ENV, "")
    if len(values) != 1 or not isinstance(values[0], bytes) or not _KEY_PATTERN.fullmatch(configured):
        return False
    try:
        supplied = values[0].decode("ascii")
    except UnicodeDecodeError:
        return False
    return bool(_KEY_PATTERN.fullmatch(supplied)
                and hmac.compare_digest(values[0], configured.encode("ascii")))
