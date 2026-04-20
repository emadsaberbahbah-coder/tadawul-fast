#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
integrations/setup_credentials.py
================================================================================
Render-safe Google credentials setup for Tadawul Fast Bridge -- v6.1.0
================================================================================

Purpose
-------
Centralize loading and validation of Google service account credentials for:
- Google Sheets
- Google Drive (optional)
- Google APIs client libraries
- gspread (optional)

Design goals
------------
- No network I/O at import time
- Lazy credential creation
- Safe for Render / FastAPI / Gunicorn / Uvicorn
- Supports JSON string, base64 JSON, or file path
- Supports domain-wide delegation if subject/user is provided
- Avoids leaking secrets in logs
- Provides small self-test helpers for diagnostics

v6.1.0 changes (what moved from v6.0.0)
---------------------------------------
- FIX: ``SERVICE_VERSION`` is now declared at the top of the module (as the
  single source of truth) instead of at the bottom. v6.0.0 declared it
  AFTER ``__all__`` and AFTER ``get_setup_credentials_meta``, which
  worked only because ``__all__`` contained the string ``"SERVICE_VERSION"``
  and the meta function hardcoded ``"6.0.0"`` independently.

- FIX: ``get_setup_credentials_meta`` now reads from ``SERVICE_VERSION``
  instead of hardcoding ``"6.0.0"``. Same for the CLI self-test block.
  v6.0.0 required editing two (or three) locations on every version bump.

- FIX: ``_required_missing_fields`` loop variable renamed from ``field``
  to ``field_name`` to stop shadowing ``dataclasses.field`` (a
  persistent linter warning, no runtime impact).

- ROBUSTNESS: ``_safe_b64_json_loads`` strips whitespace and newlines
  before decoding. Base64 values pasted from CI env files sometimes
  arrive with trailing newlines that ``b64decode`` rejects.

- CLEANUP: removed unused imports (``asdict``, ``cast``, ``Union``).

- Bumped ``SERVICE_VERSION`` to ``"6.1.0"``.

Preserved
---------
- All public symbols in ``__all__``.
- Credential loading from env JSON / env base64 / file path, tried in
  that order.
- Credential cache keyed by (scopes_tuple, subject).
- Backward-compat aliases: ``get_credentials``, ``get_sheets_service``,
  ``get_drive_service``.
- Temp file helpers (``write_service_account_temp_file`` +
  ``cleanup_temp_credential_files``) with their own lock.
- CLI self-test output shape.
================================================================================
"""

from __future__ import annotations

import base64
import json
import logging
import os
import tempfile
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
)

# =============================================================================
# Version
# =============================================================================

SERVICE_VERSION = "6.1.0"

# =============================================================================
# Logging Setup
# =============================================================================

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# =============================================================================
# Constants
# =============================================================================

# Default scopes
DEFAULT_SCOPES: Tuple[str, ...] = (
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
)

SHEETS_ONLY_SCOPES: Tuple[str, ...] = (
    "https://www.googleapis.com/auth/spreadsheets",
)

DRIVE_ONLY_SCOPES: Tuple[str, ...] = (
    "https://www.googleapis.com/auth/drive",
)

# Required fields for service account JSON
REQUIRED_SERVICE_ACCOUNT_FIELDS: Tuple[str, ...] = (
    "type",
    "project_id",
    "private_key_id",
    "private_key",
    "client_email",
    "client_id",
    "token_uri",
)

# Environment variable keys
_JSON_ENV_KEYS: Tuple[str, ...] = (
    "GOOGLE_SERVICE_ACCOUNT_JSON",
    "GOOGLE_APPLICATION_CREDENTIALS_JSON",
    "GCP_SERVICE_ACCOUNT_JSON",
    "SERVICE_ACCOUNT_INFO_JSON",
)

_JSON_B64_ENV_KEYS: Tuple[str, ...] = (
    "GOOGLE_SERVICE_ACCOUNT_JSON_B64",
    "GOOGLE_APPLICATION_CREDENTIALS_JSON_B64",
    "GCP_SERVICE_ACCOUNT_JSON_B64",
    "SERVICE_ACCOUNT_INFO_JSON_B64",
)

_FILE_ENV_KEYS: Tuple[str, ...] = (
    "GOOGLE_SERVICE_ACCOUNT_FILE",
    "GOOGLE_APPLICATION_CREDENTIALS",
)

_SUBJECT_ENV_KEYS: Tuple[str, ...] = (
    "GOOGLE_IMPERSONATED_SUBJECT",
    "GOOGLE_SERVICE_ACCOUNT_SUBJECT",
)

_SCOPE_ENV_KEYS: Tuple[str, ...] = (
    "GOOGLE_SHEETS_SCOPES",
    "GOOGLE_DRIVE_SCOPES",
)

# =============================================================================
# Custom Exceptions
# =============================================================================

class CredentialsError(Exception):
    """Base exception for credentials operations."""
    pass


class CredentialsConfigurationError(CredentialsError):
    """Raised when credentials are missing or malformed."""
    pass


class CredentialSourceError(CredentialsError):
    """Raised when credential source cannot be accessed."""
    pass


# =============================================================================
# Enums
# =============================================================================

class CredentialSourceType(str, Enum):
    """Types of credential sources."""
    ENV_JSON = "env_json"
    ENV_JSON_B64 = "env_json_b64"
    FILE = "file"
    MISSING = "missing"


class CredentialValidationStatus(str, Enum):
    """Credential validation status."""
    VALID = "valid"
    INVALID = "invalid"
    MISSING = "missing"
    PARTIAL = "partial"


# =============================================================================
# Data Classes
# =============================================================================

@dataclass(frozen=True)
class CredentialSourceInfo:
    """Information about credential source."""
    found: bool
    source_type: CredentialSourceType
    source_key: Optional[str] = None
    file_path: Optional[str] = None
    client_email_masked: Optional[str] = None
    project_id: Optional[str] = None
    scopes: Tuple[str, ...] = field(default_factory=tuple)
    delegated_subject: Optional[str] = None
    valid_service_account_shape: bool = False
    missing_required_fields: Tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "found": self.found,
            "source_type": self.source_type.value if self.source_type else "unknown",
            "source_key": self.source_key,
            "file_path": self.file_path,
            "client_email_masked": self.client_email_masked,
            "project_id": self.project_id,
            "scopes": list(self.scopes),
            "delegated_subject": self.delegated_subject,
            "valid_service_account_shape": self.valid_service_account_shape,
            "missing_required_fields": list(self.missing_required_fields),
        }


@dataclass(frozen=True)
class ValidationResult:
    """Result of credential validation."""
    status: CredentialValidationStatus
    credential_type: Optional[str] = None
    project_id: Optional[str] = None
    client_email_masked: Optional[str] = None
    scopes: List[str] = field(default_factory=list)
    delegated_subject: Optional[str] = None
    missing_required_fields: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """Check if validation passed."""
        return self.status == CredentialValidationStatus.VALID


# =============================================================================
# Pure Utility Functions
# =============================================================================

def _mask_email(email: Optional[str]) -> Optional[str]:
    """Mask email address for logging."""
    if not email or "@" not in email:
        return email
    local, domain = email.split("@", 1)
    if len(local) <= 2:
        return f"{local[0]}***@{domain}" if local else f"***@{domain}"
    return f"{local[:2]}***@{domain}"


def _normalize_private_key(private_key: str) -> str:
    """Normalize private key by replacing escaped newlines."""
    if not private_key:
        return private_key
    return private_key.replace("\\n", "\n").strip()


def _normalize_service_account_info(info: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize service account info (fix private key)."""
    normalized = dict(info)
    if "private_key" in normalized and isinstance(normalized["private_key"], str):
        normalized["private_key"] = _normalize_private_key(normalized["private_key"])
    return normalized


def _split_scopes(value: str) -> Tuple[str, ...]:
    """Split scope string into tuple."""
    parts = []
    for item in value.replace(";", ",").split(","):
        s = item.strip()
        if s:
            parts.append(s)
    return tuple(dict.fromkeys(parts))


def _env_first(keys: Sequence[str]) -> Tuple[Optional[str], Optional[str]]:
    """Get first non-empty environment variable from list."""
    for key in keys:
        val = os.getenv(key)
        if val and str(val).strip():
            return key, str(val).strip()
    return None, None


def _safe_json_loads(payload: str, source_name: str) -> Dict[str, Any]:
    """Safely load JSON from string."""
    try:
        data = json.loads(payload)
    except Exception as exc:
        raise CredentialsConfigurationError(
            f"Invalid JSON in {source_name}: {exc}"
        ) from exc
    if not isinstance(data, dict):
        raise CredentialsConfigurationError(
            f"Credential payload from {source_name} must decode to a JSON object."
        )
    return data


def _safe_b64_json_loads(payload_b64: str, source_name: str) -> Dict[str, Any]:
    """
    Safely load base64-encoded JSON.

    v6.1.0: whitespace and newlines in the base64 payload are stripped
    before decoding. Base64 secrets pasted from CI environment files or
    copied from shells frequently pick up trailing newlines that
    ``base64.b64decode`` in strict mode refuses.
    """
    # Remove whitespace characters (spaces, tabs, newlines) that commonly
    # appear in env-var-pasted base64 strings.
    sanitized = "".join(payload_b64.split())
    try:
        decoded = base64.b64decode(sanitized).decode("utf-8")
    except Exception as exc:
        raise CredentialsConfigurationError(
            f"Invalid base64 credential payload in {source_name}: {exc}"
        ) from exc
    return _safe_json_loads(decoded, source_name)


def _required_missing_fields(info: Dict[str, Any]) -> Tuple[str, ...]:
    """Check for missing required fields.

    v6.1.0: loop variable renamed from ``field`` to ``field_name`` to stop
    shadowing ``dataclasses.field`` (local-only shadow, but a consistent
    lint warning).
    """
    missing = []
    for field_name in REQUIRED_SERVICE_ACCOUNT_FIELDS:
        value = info.get(field_name)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing.append(field_name)
    return tuple(missing)


def _looks_like_service_account(info: Dict[str, Any]) -> bool:
    """Check if info looks like a service account."""
    return info.get("type") == "service_account"


def _parse_scopes(scopes: Optional[Iterable[str]] = None) -> Tuple[str, ...]:
    """Parse scopes from various sources."""
    if scopes:
        cleaned = []
        for item in scopes:
            if item is None:
                continue
            text = str(item).strip()
            if text:
                cleaned.append(text)
        if cleaned:
            return tuple(dict.fromkeys(cleaned))

    collected: List[str] = []
    for key in _SCOPE_ENV_KEYS:
        raw = os.getenv(key)
        if raw and raw.strip():
            collected.extend(_split_scopes(raw))

    if collected:
        return tuple(dict.fromkeys(collected))

    return DEFAULT_SCOPES


def _resolve_subject(subject: Optional[str] = None) -> Optional[str]:
    """Resolve delegated subject from parameter or environment."""
    if subject and str(subject).strip():
        return str(subject).strip()
    _, value = _env_first(_SUBJECT_ENV_KEYS)
    return value if value else None


# =============================================================================
# Credential Loader
# =============================================================================

class CredentialLoader:
    """Loads service account credentials from various sources."""

    @staticmethod
    def load_service_account_info() -> Tuple[Dict[str, Any], CredentialSourceType, Optional[str]]:
        """
        Load service account info from environment or file.

        Returns:
            Tuple of (info_dict, source_type, source_key_or_path).

        Raises:
            CredentialsConfigurationError: If no credentials found.
        """
        # Try raw JSON in env
        key, value = _env_first(_JSON_ENV_KEYS)
        if key and value:
            info = _safe_json_loads(value, key)
            return _normalize_service_account_info(info), CredentialSourceType.ENV_JSON, key

        # Try base64 JSON in env
        key, value = _env_first(_JSON_B64_ENV_KEYS)
        if key and value:
            info = _safe_b64_json_loads(value, key)
            return _normalize_service_account_info(info), CredentialSourceType.ENV_JSON_B64, key

        # Try file path in env
        key, path = _env_first(_FILE_ENV_KEYS)
        if key and path:
            if not os.path.exists(path):
                raise CredentialsConfigurationError(
                    f"Credential file path from {key} does not exist: {path}"
                )
            try:
                with open(path, "r", encoding="utf-8") as f:
                    info = json.load(f)
            except Exception as exc:
                raise CredentialsConfigurationError(
                    f"Failed to read credential file from {key} ({path}): {exc}"
                ) from exc
            if not isinstance(info, dict):
                raise CredentialsConfigurationError(
                    f"Credential file from {key} must contain a JSON object."
                )
            return _normalize_service_account_info(info), CredentialSourceType.FILE, path

        raise CredentialsConfigurationError(
            "Google service account credentials were not found. "
            "Set one of: GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SERVICE_ACCOUNT_JSON_B64, "
            "GOOGLE_APPLICATION_CREDENTIALS_JSON, GCP_SERVICE_ACCOUNT_JSON, "
            "SERVICE_ACCOUNT_INFO_JSON, GOOGLE_SERVICE_ACCOUNT_FILE, "
            "or GOOGLE_APPLICATION_CREDENTIALS."
        )

    @staticmethod
    def get_service_account_info() -> Dict[str, Any]:
        """
        Get normalized service account info without creating credentials.

        Returns:
            Normalized service account info dict.

        Raises:
            CredentialsConfigurationError: If validation fails.
        """
        info, _source_type, _source_key = CredentialLoader.load_service_account_info()
        missing = _required_missing_fields(info)

        if not _looks_like_service_account(info):
            raise CredentialsConfigurationError(
                "Credential JSON is present but 'type' is not 'service_account'."
            )

        if missing:
            raise CredentialsConfigurationError(
                f"Credential JSON is missing required fields: {', '.join(missing)}"
            )

        return info


# =============================================================================
# Credential Source Info
# =============================================================================

def get_credentials_source_info(
    scopes: Optional[Iterable[str]] = None,
    subject: Optional[str] = None,
) -> CredentialSourceInfo:
    """
    Get diagnostic information about credential source.

    No network I/O.

    Args:
        scopes: OAuth scopes (overrides environment).
        subject: Delegated subject (overrides environment).

    Returns:
        CredentialSourceInfo with details.
    """
    resolved_scopes = _parse_scopes(scopes)
    resolved_subject = _resolve_subject(subject)

    try:
        info, source_type, source_key = CredentialLoader.load_service_account_info()
        missing = _required_missing_fields(info)
        valid_shape = _looks_like_service_account(info) and len(missing) == 0

        is_file = source_type == CredentialSourceType.FILE
        return CredentialSourceInfo(
            found=True,
            source_type=source_type,
            source_key=None if is_file else source_key,
            file_path=source_key if is_file else None,
            client_email_masked=_mask_email(info.get("client_email")),
            project_id=info.get("project_id"),
            scopes=resolved_scopes,
            delegated_subject=resolved_subject,
            valid_service_account_shape=valid_shape,
            missing_required_fields=missing,
        )
    except CredentialsConfigurationError:
        return CredentialSourceInfo(
            found=False,
            source_type=CredentialSourceType.MISSING,
            scopes=resolved_scopes,
            delegated_subject=resolved_subject,
            valid_service_account_shape=False,
            missing_required_fields=REQUIRED_SERVICE_ACCOUNT_FIELDS,
        )


# =============================================================================
# Credential Cache
# =============================================================================

class CredentialCache:
    """Thread-safe cache for Google credentials."""

    def __init__(self) -> None:
        self._cache: Dict[Tuple[Tuple[str, ...], Optional[str]], Any] = {}
        self._lock = threading.RLock()

    def get(
        self,
        scopes: Tuple[str, ...],
        subject: Optional[str],
    ) -> Optional[Any]:
        """Get cached credentials."""
        key = (scopes, subject)
        with self._lock:
            return self._cache.get(key)

    def set(
        self,
        scopes: Tuple[str, ...],
        subject: Optional[str],
        credentials: Any,
    ) -> None:
        """Set cached credentials."""
        key = (scopes, subject)
        with self._lock:
            self._cache[key] = credentials

    def clear(self) -> None:
        """Clear all cached credentials."""
        with self._lock:
            self._cache.clear()


_credential_cache = CredentialCache()


# =============================================================================
# Google Credential Builders
# =============================================================================

def get_google_credentials(
    scopes: Optional[Iterable[str]] = None,
    subject: Optional[str] = None,
    refresh: bool = False,
) -> Any:
    """
    Build and cache Google service account credentials.

    No network I/O is performed here. Token refresh only happens when a client
    library actually sends a request.

    Args:
        scopes: OAuth scopes (overrides environment).
        subject: Delegated subject for domain-wide delegation.
        refresh: Force refresh of cached credentials.

    Returns:
        ``google.oauth2.service_account.Credentials`` object.

    Raises:
        CredentialsConfigurationError: If credentials cannot be created.
    """
    resolved_scopes = _parse_scopes(scopes)
    resolved_subject = _resolve_subject(subject)

    if not refresh:
        cached = _credential_cache.get(resolved_scopes, resolved_subject)
        if cached is not None:
            return cached

    try:
        from google.oauth2 import service_account
    except ImportError as exc:
        raise CredentialsConfigurationError(
            "google-auth is not installed. Please install 'google-auth'."
        ) from exc

    info = CredentialLoader.get_service_account_info()

    try:
        creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=list(resolved_scopes),
        )
        if resolved_subject:
            creds = creds.with_subject(resolved_subject)
    except Exception as exc:
        raise CredentialsConfigurationError(
            f"Failed to create Google credentials: {exc}"
        ) from exc

    _credential_cache.set(resolved_scopes, resolved_subject, creds)
    return creds


def build_sheets_service(
    credentials: Optional[Any] = None,
    scopes: Optional[Iterable[str]] = None,
    subject: Optional[str] = None,
    cache_discovery: bool = False,
) -> Any:
    """
    Build Google Sheets API service object.

    No immediate network I/O; requests happen when methods are executed.

    Args:
        credentials: Optional pre-created credentials.
        scopes: OAuth scopes (ignored if credentials provided).
        subject: Delegated subject (ignored if credentials provided).
        cache_discovery: Whether to cache discovery documents.

    Returns:
        Google Sheets service object.

    Raises:
        CredentialsConfigurationError: If service cannot be built.
    """
    if credentials is None:
        credentials = get_google_credentials(
            scopes=scopes or DEFAULT_SCOPES, subject=subject
        )

    try:
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise CredentialsConfigurationError(
            "google-api-python-client is not installed. "
            "Please install 'google-api-python-client'."
        ) from exc

    try:
        return build(
            "sheets",
            "v4",
            credentials=credentials,
            cache_discovery=cache_discovery,
        )
    except Exception as exc:
        raise CredentialsConfigurationError(
            f"Failed to build Google Sheets service: {exc}"
        ) from exc


def build_drive_service(
    credentials: Optional[Any] = None,
    scopes: Optional[Iterable[str]] = None,
    subject: Optional[str] = None,
    cache_discovery: bool = False,
) -> Any:
    """
    Build Google Drive API service object.

    No immediate network I/O; requests happen when methods are executed.

    Args:
        credentials: Optional pre-created credentials.
        scopes: OAuth scopes (ignored if credentials provided).
        subject: Delegated subject (ignored if credentials provided).
        cache_discovery: Whether to cache discovery documents.

    Returns:
        Google Drive service object.

    Raises:
        CredentialsConfigurationError: If service cannot be built.
    """
    if credentials is None:
        credentials = get_google_credentials(
            scopes=scopes or DEFAULT_SCOPES, subject=subject
        )

    try:
        from googleapiclient.discovery import build
    except ImportError as exc:
        raise CredentialsConfigurationError(
            "google-api-python-client is not installed. "
            "Please install 'google-api-python-client'."
        ) from exc

    try:
        return build(
            "drive",
            "v3",
            credentials=credentials,
            cache_discovery=cache_discovery,
        )
    except Exception as exc:
        raise CredentialsConfigurationError(
            f"Failed to build Google Drive service: {exc}"
        ) from exc


def get_gspread_client(
    credentials: Optional[Any] = None,
    scopes: Optional[Iterable[str]] = None,
    subject: Optional[str] = None,
) -> Any:
    """
    Build gspread client from service account credentials.

    Args:
        credentials: Optional pre-created credentials.
        scopes: OAuth scopes (ignored if credentials provided).
        subject: Delegated subject (ignored if credentials provided).

    Returns:
        gspread client.

    Raises:
        CredentialsConfigurationError: If client cannot be created.
    """
    if credentials is None:
        credentials = get_google_credentials(
            scopes=scopes or DEFAULT_SCOPES, subject=subject
        )

    try:
        import gspread
    except ImportError as exc:
        raise CredentialsConfigurationError(
            "gspread is not installed. Please install 'gspread'."
        ) from exc

    try:
        return gspread.authorize(credentials)
    except Exception as exc:
        raise CredentialsConfigurationError(
            f"Failed to create gspread client: {exc}"
        ) from exc


# =============================================================================
# Temp File Helpers (for libraries that require file path)
# =============================================================================

_temp_files: List[str] = []
_temp_files_lock = threading.RLock()


def write_service_account_temp_file(
    prefix: str = "google-sa-",
    suffix: str = ".json",
) -> str:
    """
    Write normalized service account JSON to a secure temp file.

    Args:
        prefix: File prefix.
        suffix: File suffix.

    Returns:
        Path to temp file.

    Raises:
        CredentialsConfigurationError: If file cannot be created.
    """
    info = CredentialLoader.get_service_account_info()

    try:
        fd, path = tempfile.mkstemp(prefix=prefix, suffix=suffix)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(info, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        raise CredentialsConfigurationError(
            f"Failed to create temp credential file: {exc}"
        ) from exc

    with _temp_files_lock:
        _temp_files.append(path)

    return path


def cleanup_temp_credential_files() -> int:
    """
    Remove temp files created by ``write_service_account_temp_file()``.

    Returns:
        Number of files removed.
    """
    removed = 0
    with _temp_files_lock:
        paths = list(_temp_files)
        _temp_files.clear()

    for path in paths:
        try:
            if path and os.path.exists(path):
                os.remove(path)
                removed += 1
        except Exception as e:
            logger.warning("Could not remove temp credential file %s: %s", path, e)

    return removed


# =============================================================================
# Validation and Diagnostics
# =============================================================================

def validate_google_credentials(
    scopes: Optional[Iterable[str]] = None,
    subject: Optional[str] = None,
) -> ValidationResult:
    """
    Validate credential structure only (no network I/O).

    Args:
        scopes: OAuth scopes (overrides environment).
        subject: Delegated subject (overrides environment).

    Returns:
        ValidationResult with details.
    """
    errors: List[str] = []
    resolved_scopes = _parse_scopes(scopes)
    resolved_subject = _resolve_subject(subject)

    # v6.1.0: load raw info without type-checking so we can distinguish
    # MISSING (no credential found at all) from INVALID (credential is
    # present but shape is wrong, e.g. type="authorized_user").
    try:
        info, _source_type, _source_key = CredentialLoader.load_service_account_info()
    except CredentialsConfigurationError as e:
        return ValidationResult(
            status=CredentialValidationStatus.MISSING,
            scopes=list(resolved_scopes),
            delegated_subject=resolved_subject,
            errors=[str(e)],
        )

    missing = _required_missing_fields(info)
    is_service_account = _looks_like_service_account(info)

    if not is_service_account:
        errors.append("Credential 'type' is not 'service_account'")

    if missing:
        errors.append(f"Missing required fields: {', '.join(missing)}")

    status = (
        CredentialValidationStatus.VALID
        if is_service_account and not missing
        else CredentialValidationStatus.INVALID
    )

    return ValidationResult(
        status=status,
        credential_type=info.get("type"),
        project_id=info.get("project_id"),
        client_email_masked=_mask_email(info.get("client_email")),
        scopes=list(resolved_scopes),
        delegated_subject=resolved_subject,
        missing_required_fields=list(missing),
        errors=errors,
    )


def test_build_clients(
    scopes: Optional[Iterable[str]] = None,
    subject: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Test local client construction (no API calls).

    Args:
        scopes: OAuth scopes (overrides environment).
        subject: Delegated subject (overrides environment).

    Returns:
        Dictionary with test results.
    """
    result: Dict[str, Any] = {
        "credentials_ok": False,
        "sheets_service_ok": False,
        "drive_service_ok": False,
        "gspread_ok": False,
        "errors": [],
    }

    try:
        creds = get_google_credentials(scopes=scopes, subject=subject)
        result["credentials_ok"] = creds is not None
    except Exception as exc:
        result["errors"].append(f"credentials: {exc}")
        return result

    try:
        svc = build_sheets_service(credentials=creds)
        result["sheets_service_ok"] = svc is not None
    except Exception as exc:
        result["errors"].append(f"sheets_service: {exc}")

    try:
        svc = build_drive_service(credentials=creds)
        result["drive_service_ok"] = svc is not None
    except Exception as exc:
        result["errors"].append(f"drive_service: {exc}")

    try:
        gc = get_gspread_client(credentials=creds)
        result["gspread_ok"] = gc is not None
    except Exception as exc:
        result["errors"].append(f"gspread: {exc}")

    return result


def clear_credentials_cache() -> None:
    """Clear cached credentials."""
    _credential_cache.clear()


# =============================================================================
# Backward-Compatible Aliases
# =============================================================================

def get_credentials(
    scopes: Optional[Iterable[str]] = None,
    subject: Optional[str] = None,
    refresh: bool = False,
) -> Any:
    """Alias for ``get_google_credentials``."""
    return get_google_credentials(scopes=scopes, subject=subject, refresh=refresh)


def get_sheets_service(
    credentials: Optional[Any] = None,
    scopes: Optional[Iterable[str]] = None,
    subject: Optional[str] = None,
    cache_discovery: bool = False,
) -> Any:
    """Alias for ``build_sheets_service``."""
    return build_sheets_service(
        credentials=credentials,
        scopes=scopes,
        subject=subject,
        cache_discovery=cache_discovery,
    )


def get_drive_service(
    credentials: Optional[Any] = None,
    scopes: Optional[Iterable[str]] = None,
    subject: Optional[str] = None,
    cache_discovery: bool = False,
) -> Any:
    """Alias for ``build_drive_service``."""
    return build_drive_service(
        credentials=credentials,
        scopes=scopes,
        subject=subject,
        cache_discovery=cache_discovery,
    )


# =============================================================================
# Module Metadata
# =============================================================================

def get_setup_credentials_meta(
    scopes: Optional[Iterable[str]] = None,
    subject: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Get module metadata and credential source info.

    v6.1.0: ``version`` now reads from the ``SERVICE_VERSION`` constant
    instead of being hardcoded.

    Args:
        scopes: OAuth scopes (overrides environment).
        subject: Delegated subject (overrides environment).

    Returns:
        Dictionary with module information.
    """
    info = get_credentials_source_info(scopes=scopes, subject=subject)
    return {
        "module": "integrations.setup_credentials",
        "version": SERVICE_VERSION,
        "status": "ok" if info.found and info.valid_service_account_shape else "warn",
        "details": info.to_dict(),
    }


# =============================================================================
# Module Exports
# =============================================================================

__all__ = [
    # Version
    "SERVICE_VERSION",
    # Exceptions
    "CredentialsError",
    "CredentialsConfigurationError",
    "CredentialSourceError",
    # Enums
    "CredentialSourceType",
    "CredentialValidationStatus",
    # Data Classes
    "CredentialSourceInfo",
    "ValidationResult",
    # Core functions
    "get_google_credentials",
    "build_sheets_service",
    "build_drive_service",
    "get_gspread_client",
    # Source info
    "get_credentials_source_info",
    # Validation
    "validate_google_credentials",
    "test_build_clients",
    "clear_credentials_cache",
    # Temp file helpers
    "write_service_account_temp_file",
    "cleanup_temp_credential_files",
    # Backward compat
    "get_credentials",
    "get_sheets_service",
    "get_drive_service",
    # Metadata
    "get_setup_credentials_meta",
]


# =============================================================================
# CLI Self-Test
# =============================================================================

if __name__ == "__main__":
    try:
        meta = get_setup_credentials_meta()
        validation = validate_google_credentials()
        clients = test_build_clients()

        output = {
            "meta": meta,
            "validation": {
                "status": validation.status.value,
                "credential_type": validation.credential_type,
                "project_id": validation.project_id,
                "client_email_masked": validation.client_email_masked,
                "scopes": validation.scopes,
                "delegated_subject": validation.delegated_subject,
                "missing_required_fields": validation.missing_required_fields,
                "errors": validation.errors,
            },
            "client_build_test": clients,
        }
        print(json.dumps(output, ensure_ascii=False, indent=2, default=str))
    except Exception as exc:
        error_output = {
            "module": "integrations.setup_credentials",
            "version": SERVICE_VERSION,
            "status": "error",
            "error": str(exc),
        }
        print(json.dumps(error_output, ensure_ascii=False, indent=2))
        raise
