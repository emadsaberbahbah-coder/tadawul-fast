"""Offline security regressions for the shared diagnostic redactor."""
import io
import base64
import json
import logging
import sys
from urllib.parse import quote, quote_plus

import pytest

from core import secret_redaction as redaction


@pytest.fixture(autouse=True)
def synthetic_environment(monkeypatch):
    # Never consult real runner credential values in pure tests.
    monkeypatch.setattr(redaction.os, "environ", {})


def hidden(output, *values):
    if any(value and value in output for value in values):
        pytest.fail("synthetic credential was retained", pytrace=False)


@pytest.mark.parametrize("template", [
    "https://example.invalid/quote?api_token={}&fmt=json",
    "https://example.invalid/quote?API%5FKEY={}&symbol=TEST",
    "https://example.invalid/quote?apiToken={}&symbol=TEST",
    "HTTP 403 auth_error api-key={}; request_id=req-019",
    "X-APP-TOKEN: {}\nrequest_id=req-019",
    "X-API-KEY={}; status=401",
    "Authorization: Bearer {}\nrequest_id=req-019",
    "Proxy-Authorization: Basic {}\nstatus=401",
    "Bearer {} request_id=req-019",
    "provider returned {{'api_token': '{}', 'status': 403}}",
    'provider returned {{"client_secret": "{}", "status": 403}}',
    'provider returned {{"clientSecret": "{}", "status": 403}}',
])
def test_unknown_credentials_by_context(template):
    secret = "synthetic-context-credential-78621"
    output = redaction.redact_text(template.format(secret))
    hidden(output, secret)
    assert redaction.REDACTED in output
    assert redaction.redact_text(output) == output


@pytest.mark.parametrize("secret", [
    "synthetic-bare-credential-78621", "a/b c+d?=é", 'a"b\\c\nlong',
])
def test_caller_known_bare_percent_and_json_escaped_secrets(secret):
    variants = {secret, quote(secret, safe=""), quote_plus(secret, safe=""),
                json.dumps(secret)[1:-1], json.dumps(secret, ensure_ascii=False)[1:-1]}
    for variant in variants:
        output = redaction.redact_text("upstream echo " + variant + " status=403 request_id=req-019", secret_values=(secret,))
        hidden(output, variant)
        assert "status=403 request_id=req-019" in output


@pytest.mark.parametrize("secret", ["x", "ab", "123"])
def test_short_explicit_credentials_match_whole_tokens(secret):
    output = redaction.redact_text("echo " + secret + " request_id=req-019 status=401 callback taxicab abc123def", secret_values=(secret,))
    assert output == "echo [REDACTED] request_id=req-019 status=401 callback taxicab abc123def"
    hidden(redaction.redact_text(secret, secret_values=(secret,)), secret)


def test_redaction_precedes_truncation():
    secret = "synthetic-crossing-truncation-boundary-78621"
    prefix = "HTTP 403 auth_error "
    output = redaction.redact_text(prefix + secret + " details", secret_values=(secret,), limit=len(prefix) + 8)
    hidden(output, secret, secret[:8])
    assert len(output) == len(prefix) + 8
    assert output.startswith("HTTP 403")
    assert redaction.redact_text(secret, secret_values=(secret,), limit=0) == ""


def test_safe_error_class_status_and_request_id():
    secret = "synthetic-error-credential-78621"
    output = redaction.safe_error_text(ValueError("HTTP 401 auth_error echo " + secret + " request_id=req-019"), secret_values=(secret,))
    hidden(output, secret)
    assert output.startswith("ValueError: HTTP 401 auth_error")
    assert "request_id=req-019" in output


def test_uri_userinfo_and_private_key_blocks():
    password = "synthetic-url-password-78621"
    pem = "-----BEGIN PRIVATE KEY-----\nsynthetic-private-key-body-78621\n-----END PRIVATE KEY-----"
    output = redaction.redact_text("postgres://user:" + password + "@db.invalid:5432/test HTTP 503 " + pem)
    hidden(output, password, "synthetic-private-key-body-78621")
    assert "postgres://[REDACTED]@db.invalid:5432/test HTTP 503" in output
    assert "-----BEGIN PRIVATE KEY-----" not in output


def test_unknown_credentials_in_full_json_and_nested_lists():
    secret = "synthetic-json-credential-78621"
    payload = {"api_tokens": [secret], "password": 123456, "authorization": "Bearer " + secret,
               "items": [{"api-key": secret}], "status_code": 403, "request_id": "req-019",
               "msg": 'provider {"api_token": "' + secret + '"}'}
    output = redaction.redact_text(json.dumps(payload))
    hidden(output, secret)
    parsed = json.loads(output)
    assert parsed["password"] == redaction.REDACTED
    assert parsed["status_code"] == 403
    assert parsed["request_id"] == "req-019"
    assert isinstance(parsed["api_tokens"], list)
    assert payload["password"] == 123456


def test_nonsecret_text_flags_and_json_are_unchanged():
    text = "HTTP 401 auth_error; token_count=2 ALLOW_QUERY_TOKEN=false request_id=req-019 basic diagnosis"
    assert redaction.redact_text(text) == text
    payload = '{"status_code":403,"token_count":2,"request_id":"req-019","price":12.5}'
    assert redaction.redact_text(payload) == payload


@pytest.mark.parametrize("list_value", [
    '["synthetic-list-one-78621", "synthetic-list-two-78621"]',
    'synthetic-list-one-78621, synthetic-list-two-78621',
    'synthetic-list-one-78621;synthetic-list-two-78621',
    'synthetic-list-one-78621|synthetic-list-two-78621',
    'synthetic-list-one-78621\nsynthetic-list-two-78621',
])
def test_configured_list_formats_and_flags(monkeypatch, list_value):
    monkeypatch.setitem(redaction.os.environ, "ALLOWED_TOKENS", list_value)
    monkeypatch.setitem(redaction.os.environ, "ALLOW_QUERY_TOKEN", "false")
    monkeypatch.setitem(redaction.os.environ, "TFB_YC_CB_FAILURE_THRESHOLD", "12345")
    values = redaction.configured_secret_values()
    assert len(values) == 2
    output = redaction.redact_text("echo synthetic-list-one-78621 synthetic-list-two-78621 false 12345")
    hidden(output, "synthetic-list-one-78621", "synthetic-list-two-78621")
    assert output.endswith(" false 12345")


def test_rotation_resolved_each_call(monkeypatch):
    first = "synthetic-old-credential-78621"
    second = "synthetic-new-credential-78621"
    monkeypatch.setitem(redaction.os.environ, "EODHD_API_KEY", first)
    hidden(redaction.redact_text("echo " + first), first)
    monkeypatch.setitem(redaction.os.environ, "EODHD_API_KEY", second)
    hidden(redaction.redact_text("echo " + second), second)
    assert second in redaction.configured_secret_values()
    assert first not in redaction.configured_secret_values()


@pytest.mark.parametrize("name", [
    "X_APP_TOKEN", "AUTH_TOKEN", "TFB_AUTH_TOKEN", "TOKEN", "TFB_TOKEN",
    "TFB_BEARER_TOKEN", "BEARER_TOKEN", "TFB_API_KEY", "X_API_KEY", "EODHD_KEY",
])
def test_actual_scalar_credential_aliases(monkeypatch, name):
    secret = "synthetic-alias-credential-78621"
    monkeypatch.setitem(redaction.os.environ, name, secret)
    assert len(redaction.configured_secret_values()) == 1
    hidden(redaction.redact_text("HTTP 403 bare echo " + secret), secret)


@pytest.mark.parametrize("name", [
    "TFB_APP_TOKENS", "AUTH_TOKENS", "API_TOKENS", "TOKENS", "X_APP_TOKENS",
    "BEARER_TOKENS", "TFB_BEARER_TOKENS", "TFB_AUTH_TOKENS",
])
def test_actual_list_credential_aliases(monkeypatch, name):
    first, second = "synthetic-alias-one-78621", "synthetic-alias-two-78621"
    monkeypatch.setitem(redaction.os.environ, name, json.dumps([first, second]))
    assert len(redaction.configured_secret_values()) == 2
    hidden(redaction.redact_text("bare echo " + first + " " + second), first, second)


def test_fully_percent_encoded_known_credential():
    secret = "synthetic-full-percent-78621"
    encoded = "".join("%%%02X" % byte for byte in secret.encode())
    hidden(redaction.redact_text("bare echo " + encoded, secret_values=(secret,)), encoded)


def test_numeric_credentials_in_supported_json_token_list(monkeypatch):
    monkeypatch.setitem(redaction.os.environ, "ALLOWED_TOKENS", "[123456, 654321]")
    assert len(redaction.configured_secret_values()) == 2
    hidden(redaction.redact_text("echo 123456 654321 status=401"), "123456", "654321")


def test_credential_json_environment_and_url_password(monkeypatch):
    private_key = "synthetic-env-private-key-78621"
    password = "synthetic-env-url-password-78621"
    monkeypatch.setitem(redaction.os.environ, "GOOGLE_SHEETS_CREDENTIALS", json.dumps({
        "project_id": "nonsecret-project", "private_key": private_key,
    }))
    monkeypatch.setitem(redaction.os.environ, "DATABASE_URL", "postgres://user:" + quote(password) + "@db.invalid/db")
    values = redaction.configured_secret_values()
    count = len(values)
    assert count == 3  # Complete serialized credential and its private leaf.
    output = redaction.redact_text("echo " + private_key + " " + password + " nonsecret-project")
    hidden(output, private_key, password)
    assert output.endswith(" nonsecret-project")


@pytest.mark.parametrize("name", [
    "ALPHAVANTAGE_API_KEY", "TFB_EODHD_API_KEY", "TFB_MAIL_PASSWORD",
    "AUDIT_SECRET", "DRIFT_AUDIT_SECRET",
])
def test_actual_additional_scalar_credentials(monkeypatch, name):
    secret = "synthetic-source-alias-78621"
    monkeypatch.setitem(redaction.os.environ, name, secret)
    hidden(redaction.redact_text("bare echo " + secret + " status=403"), secret)


@pytest.mark.parametrize("name", ["GOOGLE_CREDENTIALS", "GOOGLE_CREDENTIALS_DICT"])
def test_actual_google_json_aliases(monkeypatch, name):
    secret = "synthetic-google-json-alias-78621"
    raw = json.dumps({"private_key": secret, "project_id": "nonsecret-project"})
    monkeypatch.setitem(redaction.os.environ, name, raw)
    hidden(redaction.redact_text("bare echo " + secret), secret)
    hidden(redaction.redact_text("opaque serialized echo " + raw), raw, secret)
    assert json.loads(redaction.redact_text(raw))["project_id"] == "nonsecret-project"


@pytest.mark.parametrize("name", ["GOOGLE_SHEETS_CREDENTIALS_B64", "GOOGLE_CREDENTIALS_B64",
                                  "GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS"])
def test_actual_google_base64_aliases_resolve_private_leaf_without_file_reads(monkeypatch, name):
    secret = "synthetic-google-base64-private-78621"
    raw = base64.b64encode(json.dumps({"private_key": secret}).encode()).decode()
    raw = raw[:20] + "\n" + raw[20:]
    monkeypatch.setitem(redaction.os.environ, name, raw)
    monkeypatch.setitem(redaction.os.environ, "GOOGLE_APPLICATION_CREDENTIALS", "/synthetic/credentials.json")
    def forbidden_open(*args, **kwargs):
        pytest.fail("diagnostic redaction attempted to read a credential file", pytrace=False)
    monkeypatch.setattr("builtins.open", forbidden_open)
    hidden(redaction.redact_text("bare private leaf " + secret), secret)
    hidden(redaction.redact_text("opaque encoded echo " + raw), raw)


@pytest.mark.parametrize("raw", ["not-valid-base64!", "e30=", "/w=="])
@pytest.mark.parametrize("name", ["GOOGLE_CREDENTIALS_B64", "GOOGLE_SHEETS_CREDENTIALS", "GOOGLE_CREDENTIALS"])
def test_malformed_or_empty_base64_credentials_never_break_diagnostics(monkeypatch, raw, name):
    monkeypatch.setitem(redaction.os.environ, name, raw)
    output = redaction.redact_text("raw echo " + raw + " HTTP 503")
    hidden(output, raw)
    assert output.endswith(" HTTP 503")


@pytest.mark.parametrize("depth", [650, 1200])
def test_deep_diagnostic_json_cannot_break_log_redaction(depth):
    secret = "synthetic-deep-diagnostic-78621"
    text = "[" * depth + json.dumps({"api_token": secret}) + "]" * depth
    output = redaction.redact_text(text)
    hidden(output, secret)
    assert "[REDACTED]" in output


def test_deep_credential_environment_never_breaks_unrelated_diagnostics(monkeypatch):
    raw = "[" * 1200 + '"invalid-credential-structure"' + "]" * 1200
    monkeypatch.setitem(redaction.os.environ, "GOOGLE_CREDENTIALS", raw)
    assert redaction.redact_text("HTTP 503 request_id=req-019") == "HTTP 503 request_id=req-019"


def test_plain_formatter_preserves_delegate_extras_and_traceback():
    secret = "synthetic-log-credential-78621"
    delegate = logging.Formatter("%(levelname)s %(name)s %(request_id)s %(message)s", datefmt="%H:%M")
    try:
        raise RuntimeError("HTTP 403 echo " + secret)
    except RuntimeError:
        record = logging.LogRecord("application", logging.ERROR, __file__, 1, "provider echo %s", (secret,), sys.exc_info())
    record.request_id = "req-019"
    formatter = redaction.RedactingFormatter(delegate, secret_values=(secret,))
    output = formatter.format(record)
    hidden(output, secret)
    assert output.startswith("ERROR application req-019 provider echo")
    assert "Traceback (most recent call last)" in output
    assert "RuntimeError: HTTP 403" in output
    assert formatter.delegate is delegate
    assert record.args == (secret,)


def test_preinstalled_json_formatter_remains_valid_with_traceback_and_extras():
    secret = 'synthetic-json-log-"credential-78621'

    class ExistingFormatter(logging.Formatter):
        def format(self, record):
            return json.dumps({"custom_contract": "retained", "msg": record.getMessage(),
                               "request_id": record.request_id, "status_code": 401,
                               "exc": self.formatException(record.exc_info)})

    try:
        raise LookupError("HTTP 401 echo " + secret)
    except LookupError:
        record = logging.LogRecord("application", logging.ERROR, __file__, 1, "echo %s", (secret,), sys.exc_info())
    record.request_id = "req-019"
    output = redaction.RedactingFormatter(ExistingFormatter(), secret_values=(secret,)).format(record)
    hidden(output, secret, json.dumps(secret)[1:-1])
    parsed = json.loads(output)
    assert set(parsed) == {"custom_contract", "msg", "request_id", "status_code", "exc"}
    assert parsed["custom_contract"] == "retained"
    assert parsed["request_id"] == "req-019"
    assert parsed["status_code"] == 401
    assert "LookupError: HTTP 401" in parsed["exc"]


def test_handler_wrapping_shared_idempotent_and_new_handlers():
    secret = "synthetic-handler-credential-78621"
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.WARNING)
    delegate = logging.Formatter("%(name)s %(levelname)s %(message)s")
    handler.setFormatter(delegate)
    first, second = logging.Logger("first"), logging.Logger("second")
    first.addHandler(handler)
    second.addHandler(handler)
    assert redaction.install_redaction_on_handlers(first, second, secret_values=(secret,)) == 1
    wrapper = handler.formatter
    assert wrapper.delegate is delegate
    assert handler.level == logging.WARNING
    assert redaction.install_redaction_on_handlers(first, second) == 0
    assert handler.formatter is wrapper
    first.warning("HTTP 403 echo %s", secret)
    hidden(stream.getvalue(), secret)
    assert stream.getvalue().startswith("first WARNING HTTP 403 echo")
    later = logging.StreamHandler(io.StringIO())
    second.addHandler(later)
    assert redaction.install_redaction_on_handlers(second) == 1


def test_wrapped_formatter_tracks_rotation(monkeypatch):
    first = "synthetic-formatter-old-78621"
    second = "synthetic-formatter-new-78621"
    formatter = redaction.RedactingFormatter(logging.Formatter("%(message)s"))
    for secret in (first, second):
        monkeypatch.setitem(redaction.os.environ, "APP_TOKEN", secret)
        record = logging.LogRecord("application", logging.ERROR, __file__, 1, "echo %s", (secret,), None)
        hidden(formatter.format(record), secret)
