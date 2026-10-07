"""Small, dependency-free redaction for diagnostic text and existing log handlers.

This module never loads application configuration or changes business payloads.
Credential environment values are resolved at call time so rotated credentials
are covered without restarting or caching their old values.
"""
from __future__ import annotations

import json
import base64
import logging
import os
import re
from typing import Any, Iterable
from urllib.parse import quote, quote_plus, unquote, urlsplit


REDACTED = "[REDACTED]"

# Deliberately explicit: flags such as ALLOW_QUERY_TOKEN and numeric breaker
# thresholds must never become secret values merely because of their names.
_SCALAR_ENV_NAMES = (
    "APP_TOKEN", "TFB_APP_TOKEN", "BACKEND_TOKEN", "BACKUP_APP_TOKEN",
    "X_APP_TOKEN", "AUTH_TOKEN", "TFB_AUTH_TOKEN", "TOKEN", "TFB_TOKEN",
    "TFB_BEARER_TOKEN", "BEARER_TOKEN", "TFB_API_KEY", "X_API_KEY",
    "API_KEY", "API_TOKEN", "ACCESS_TOKEN", "CLIENT_SECRET",
    "EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_TOKEN", "EODHD_KEY", "TFB_EODHD_API_KEY",
    "FINNHUB_API_KEY", "FINNHUB_TOKEN", "FMP_API_KEY",
    "ALPHA_VANTAGE_API_KEY", "ALPHAVANTAGE_API_KEY", "TWELVEDATA_API_KEY", "TWELVE_DATA_API_KEY",
    "MARKETSTACK_API_KEY", "ARGAAM_API_KEY", "TADAWUL_API_KEY",
    "RENDER_API_KEY", "OPENAI_API_KEY", "GITHUB_TOKEN", "GH_TOKEN",
    "REDIS_PASSWORD", "DATABASE_PASSWORD", "GOOGLE_CLIENT_SECRET",
    "GOOGLE_PRIVATE_KEY", "GOOGLE_SHEETS_PRIVATE_KEY",
    "GOOGLE_SERVICE_ACCOUNT_PRIVATE_KEY",
    "TFB_MAIL_PASSWORD", "AUDIT_SECRET", "DRIFT_AUDIT_SECRET",
)
_LIST_ENV_NAMES = (
    "ALLOWED_TOKENS", "TFB_ALLOWED_TOKENS", "APP_TOKENS", "AUTH_TOKENS",
    "TFB_AUTH_TOKENS", "TFB_APP_TOKENS", "API_TOKENS", "TOKENS",
    "X_APP_TOKENS", "BEARER_TOKENS", "TFB_BEARER_TOKENS",
)
_JSON_ENV_NAMES = (
    "GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS", "GOOGLE_CREDENTIALS_DICT",
    "GOOGLE_SERVICE_ACCOUNT_JSON",
    "GOOGLE_APPLICATION_CREDENTIALS_JSON", "GCP_SERVICE_ACCOUNT_JSON",
)
_BASE64_JSON_ENV_NAMES = ("GOOGLE_SHEETS_CREDENTIALS_B64", "GOOGLE_CREDENTIALS_B64")
_URL_ENV_NAMES = ("DATABASE_URL", "REDIS_URL")

_CREDENTIAL_KEYS = frozenset({
    "key", "apikey", "api_key", "api_token", "token", "secret",
    "access_token", "refresh_token", "id_token", "app_token",
    "backup_app_token", "password", "passwd", "pwd", "credential",
    "credentials", "client_secret", "private_key", "encryption_key",
    "authorization", "proxy_authorization", "bearer", "x_api_key",
    "x_app_token", "x_auth_token", "cookie", "set_cookie",
    "session_token",
} | {name.lower() for name in _SCALAR_ENV_NAMES} | {
    name.lower() for name in (*_LIST_ENV_NAMES, *_JSON_ENV_NAMES, *_BASE64_JSON_ENV_NAMES)
})
_COMPACT_CREDENTIAL_KEYS = frozenset(key.replace("_", "") for key in _CREDENTIAL_KEYS)


def _credential_key(value: str) -> bool:
    normalized = unquote(value).lower().replace("-", "_")
    return normalized.replace("_", "") in _COMPACT_CREDENTIAL_KEYS


def _text(value: Any) -> str:
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode("utf-8", errors="replace")
    try:
        return value if isinstance(value, str) else str(value)
    except Exception:
        return "<unprintable %s>" % type(value).__name__


def _secrets(values: Iterable[str]) -> tuple[str, ...]:
    if isinstance(values, (str, bytes)):
        values = (values,)
    result = set()
    for value in values or ():
        if isinstance(value, (str, bytes)):
            value = _text(value).strip()
            if value:
                result.add(value)
    return tuple(sorted(result, key=lambda value: (-len(value), value)))


def _list_secrets(raw: str) -> tuple[str, ...]:
    try:
        decoded = json.loads(raw)
    except (ValueError, TypeError, RecursionError):
        decoded = None
    if isinstance(decoded, list):
        return _secrets(str(item) for item in decoded if isinstance(item, (str, int, float)))
    if isinstance(decoded, str):
        return _secrets((decoded,))
    return _secrets(part.strip().strip("\"'") for part in re.split(r"[,;|\r\n]+", raw))


def _json_secrets(value: Any) -> list[str]:
    result = []
    if isinstance(value, dict):
        for key, item in value.items():
            if _credential_key(str(key)):
                if isinstance(item, str):
                    result.append(item)
                elif isinstance(item, list):
                    result.extend(item)
                elif isinstance(item, dict):
                    result.extend(_json_secrets(item))
            else:
                result.extend(_json_secrets(item))
    elif isinstance(value, list):
        for item in value:
            result.extend(_json_secrets(item))
    return result


def configured_secret_values() -> tuple[str, ...]:
    """Resolve known credential fields only; never read files or log values."""
    values = [os.environ.get(name, "") for name in _SCALAR_ENV_NAMES]
    for name in _LIST_ENV_NAMES:
        raw = os.environ.get(name, "")
        if raw:
            values.extend(_list_secrets(raw))
    for name in _JSON_ENV_NAMES:
        raw = os.environ.get(name, "")
        if raw:
            values.append(raw)
            try:
                values.extend(_json_secrets(json.loads(raw)))
            except (ValueError, TypeError, RecursionError):
                # The sync writer also accepts base64 JSON in these primary
                # names, rather than requiring a *_B64 alias.
                if name in {"GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS"}:
                    try:
                        decoded = base64.b64decode("".join(raw.split()), validate=True).decode("utf-8")
                        values.extend(_json_secrets(json.loads(decoded)))
                    except (ValueError, UnicodeError, RecursionError):
                        pass
    for name in _BASE64_JSON_ENV_NAMES:
        raw = os.environ.get(name, "")
        if raw:
            values.append(raw)
            try:
                decoded = base64.b64decode("".join(raw.split()), validate=True).decode("utf-8")
                values.extend(_json_secrets(json.loads(decoded)))
            except (ValueError, UnicodeError, RecursionError):
                pass
    for name in _URL_ENV_NAMES:
        raw = os.environ.get(name, "")
        if raw:
            try:
                password = urlsplit(raw).password
                if password:
                    values.append(unquote(password))
            except ValueError:
                pass
    return _secrets(values)


_KEY_PATTERN = "(?:" + "|".join(
    re.escape(key).replace("_", "[_-]?")
    for key in sorted(_CREDENTIAL_KEYS, key=len, reverse=True)
) + ")"
_PEM_RE = re.compile(
    r"-----BEGIN ([A-Z ]*PRIVATE KEY)-----.*?-----END \1-----", re.DOTALL,
)
_USERINFO_RE = re.compile(r"(?P<scheme>\b[a-z][a-z0-9+.-]*://)[^/\s\"'<>\\]+@", re.I)
_QUERY_RE = re.compile(r"(?P<prefix>[?&;])(?P<key>[^?&#;\s=]+)=(?P<value>[^&#\s\"'<>\\]*)")
_AUTH_RE = re.compile(
    r"(?P<prefix>\b(?:proxy[-_]authorization|authorization)\s*[:=]\s*(?:Bearer|Basic|Token)\s+)"
    r"[^\s,;\"'{}\\]+", re.I,
)
_BEARER_RE = re.compile(r"(?P<prefix>\bBearer\s+)[A-Za-z0-9._~+/=-]+", re.I)
_VALUE_PATTERN = r"(?:\[REDACTED\]|\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|[^\s,;&}\[\]]+)"
_ASSIGNMENT_RE = re.compile(
    r"(?<![\w-])(?P<key>" + _KEY_PATTERN + r")(?P<separator>\s*[:=]\s*)"
    r"(?P<value>" + _VALUE_PATTERN + ")", re.I,
)
_QUOTED_ASSIGNMENT_RE = re.compile(
    r"(?P<key_quote>[\"'])(?P<key>" + _KEY_PATTERN + r")(?P=key_quote)"
    r"(?P<separator>\s*:\s*)"
    r"(?P<value>" + _VALUE_PATTERN + ")", re.I,
)


def _replacement(value: str) -> str:
    if value[:1] in ("\"", "'") and value[-1:] == value[:1]:
        return value[0] + REDACTED + value[0]
    return REDACTED


def _redact_assignment(match: re.Match[str]) -> str:
    # A scheme followed by an already-redacted auth value is safe, and retaining
    # the scheme avoids consuming a harmless diagnostic word on a second pass.
    key = match["key"].lower().replace("-", "_")
    if key in {"authorization", "proxy_authorization"} and match["value"].lower() in {"bearer", "basic", "token"}:
        return match[0]
    return match["key"] + match["separator"] + _replacement(match["value"])


def _secret_variants(secret: str) -> set[str]:
    variants = {secret, quote(secret, safe=""), quote_plus(secret, safe="")}
    variants.add("".join("%%%02X" % byte for byte in secret.encode("utf-8")))
    variants.update(json.dumps(secret, ensure_ascii=ascii_only)[1:-1] for ascii_only in (False, True))
    variants.update(re.sub(r"%[0-9A-F]{2}", lambda match: match[0].lower(), value) for value in tuple(variants))
    return variants


def _redact_plain(text: str, secrets: tuple[str, ...]) -> str:
    # Encoded variants are replaced first, before a prefix can hide the rest of
    # a credential. Short credentials match complete tokens, not word fragments.
    variants = {variant for secret in secrets for variant in _secret_variants(secret)}
    for variant in sorted(variants, key=lambda value: (-len(value), value)):
        if len(variant) <= 3:
            text = re.sub(r"(?<![\w])" + re.escape(variant) + r"(?![\w])", REDACTED, text)
        else:
            text = text.replace(variant, REDACTED)
    text = _PEM_RE.sub(REDACTED, text)
    text = _USERINFO_RE.sub(lambda match: match["scheme"] + REDACTED + "@", text)
    text = _QUERY_RE.sub(
        lambda match: match["prefix"] + match["key"] + "=" + REDACTED
        if _credential_key(match["key"]) else match[0], text,
    )
    text = _AUTH_RE.sub(lambda match: match["prefix"] + REDACTED, text)
    text = _BEARER_RE.sub(lambda match: match["prefix"] + REDACTED, text)
    text = _QUOTED_ASSIGNMENT_RE.sub(
        lambda match: match["key_quote"] + match["key"] + match["key_quote"]
        + match["separator"] + _replacement(match["value"]), text,
    )
    return _ASSIGNMENT_RE.sub(_redact_assignment, text)


def _redact_json(value: Any, secrets: tuple[str, ...]) -> Any:
    if isinstance(value, str):
        return _redact_plain(value, secrets)
    if isinstance(value, dict):
        return {key: _redact_credential_value(item) if _credential_key(key) else _redact_json(item, secrets)
                for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_json(item, secrets) for item in value]
    return value


def _redact_credential_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _redact_credential_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_credential_value(item) for item in value]
    return REDACTED if value is not None else None


def redact_text(value: Any, *, secret_values: Iterable[str] = (), limit: int | None = None) -> str:
    """Redact diagnostic text, then apply an optional character limit.

    Known secrets protect bare echoes and percent/JSON-escaped forms. Generic
    credential keys, auth headers, URL userinfo and private-key blocks protect
    values even when the caller cannot identify the credential in advance.
    Full JSON input retains its fields, types, and valid JSON serialization.
    """
    text = _text(value)
    secrets = _secrets((*configured_secret_values(), *_secrets(secret_values)))
    try:
        parsed = json.loads(text)
    except (ValueError, TypeError, RecursionError):
        text = _redact_plain(text, secrets)
    else:
        if parsed is None or isinstance(parsed, (bool, int, float)):
            # A diagnostic consisting solely of a numeric/short credential is
            # still a bare echo. JSON status fields inside objects stay typed.
            cleaned = _redact_plain(text, secrets)
            if cleaned != text:
                text = json.dumps(cleaned)
            cleaned = parsed
        else:
            try:
                cleaned = _redact_json(parsed, secrets)
            except RecursionError:
                text = _redact_plain(text, secrets)
                cleaned = parsed
        if cleaned != parsed:
            text = json.dumps(cleaned, ensure_ascii=False)
    if limit is not None:
        limit = max(0, int(limit))
        if len(text) > limit:
            marker = "...(truncated)"
            text = text[:max(0, limit - len(marker))] + marker[:limit]
    return text


def safe_error_text(exc: Any, *, secret_values: Iterable[str] = (), limit: int | None = 1600) -> str:
    """Retain an exception's class while redacting its message before limiting."""
    text = "%s: %s" % (type(exc).__name__, _text(exc)) if isinstance(exc, BaseException) else _text(exc)
    return redact_text(text, secret_values=secret_values, limit=limit)


class RedactingFormatter(logging.Formatter):
    """Delegate the existing log contract, then scrub its rendered diagnostics."""

    def __init__(self, formatter: logging.Formatter | None = None, *, secret_values: Iterable[str] = ()):
        super().__init__()
        self.delegate = formatter if formatter is not None else logging.Formatter()
        self.secret_values = _secrets(secret_values)

    def format(self, record: logging.LogRecord) -> str:
        return redact_text(self.delegate.format(record), secret_values=self.secret_values)

    def formatException(self, ei: Any) -> str:
        return redact_text(self.delegate.formatException(ei), secret_values=self.secret_values)

    def formatStack(self, stack_info: str) -> str:
        return redact_text(self.delegate.formatStack(stack_info), secret_values=self.secret_values)


def install_redaction_on_handlers(*loggers: logging.Logger | str, secret_values: Iterable[str] = ()) -> int:
    """Idempotently wrap existing handlers; keep handler routing and levels.

    Without arguments, cover root/main and Gunicorn/uvicorn logger families.
    Call again after a server adds handlers; shared handlers are wrapped once.
    """
    if not loggers:
        loggers = ("", "main", "gunicorn", "gunicorn.error", "gunicorn.access",
                   "uvicorn", "uvicorn.error", "uvicorn.access")
    explicit = _secrets(secret_values)
    seen = set()
    wrapped = 0
    for logger in loggers:
        logger = logging.getLogger(logger) if isinstance(logger, str) else logger
        for handler in logger.handlers:
            if id(handler) in seen:
                continue
            seen.add(id(handler))
            current = handler.formatter
            if isinstance(current, RedactingFormatter):
                current.secret_values = _secrets((*current.secret_values, *explicit))
            else:
                handler.setFormatter(RedactingFormatter(current, secret_values=explicit))
                wrapped += 1
    return wrapped
