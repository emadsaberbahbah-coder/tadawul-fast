#!/usr/bin/env python3
"""
scripts/test_sheets_auth.py
===========================================================
TADAWUL FAST BRIDGE – ENTERPRISE SHEETS AUTH DIAGNOSTIC (v6.3.0)
===========================================================

Design goals (TFB aligned)
- ✅ Hygiene-checker friendly: NO rich UI, NO print(); uses sys.stdout.write only
- ✅ Startup-safe: optional deps are soft (script still runs and reports what’s missing)
- ✅ Credential discovery: env JSON / env base64 / file path (GOOGLE_APPLICATION_CREDENTIALS)
- ✅ Private key repair: fixes "\\n" and wrapping issues safely
- ✅ Auth + access diagnostics:
    - Builds Google Sheets API client
    - Optional Drive API permission audit (if scopes allow)
    - Read / BatchUpdate sanity checks
    - Optional write test (explicit flag)
- ✅ Network diagnostics (DNS/TCP/TLS/latency) best-effort
- ✅ Outputs: console + optional JSON/HTML/JUnit XML artifacts
- ✅ Exit codes:
    0 = all good
    1 = any CRITICAL failure
    2 = any HIGH failure (no critical)
    3 = other failures only

Notes
- This script is intentionally “best effort”. It never hides errors, and it never crashes
  just because an optional library is missing.

Usage examples
- python scripts/test_sheets_auth.py --sheet-id <ID> --sheet-name "Market Leaders" --read-range "A1:C5"
- python scripts/test_sheets_auth.py --sheet-id <ID> --write --cell "A1"
- python scripts/test_sheets_auth.py --network-only
- python scripts/test_sheets_auth.py --sheet-id <ID> --output report.json
- python scripts/test_sheets_auth.py --sheet-id <ID> --output report.html
- python scripts/test_sheets_auth.py --sheet-id <ID> --output report.xml

===========================================================
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import concurrent.futures
import json
import logging
import logging.config
import os
import platform
import re
import socket
import ssl
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        opt = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(v, option=opt, default=str).decode("utf-8")

    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)

    _HAS_ORJSON = True
except Exception:

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        return json.dumps(v, indent=(indent if indent else None), default=str, ensure_ascii=False)

    def json_loads(data: Union[str, bytes]) -> Any:
        if isinstance(data, (bytes, bytearray)):
            data = data.decode("utf-8", errors="replace")
        return json.loads(data)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Optional dependencies (ALL SAFE)
# ---------------------------------------------------------------------------
try:
    import requests  # type: ignore
    REQUESTS_AVAILABLE = True
except Exception:
    requests = None  # type: ignore
    REQUESTS_AVAILABLE = False

try:
    import dns.resolver  # type: ignore
    DNS_AVAILABLE = True
except Exception:
    dns = None  # type: ignore
    DNS_AVAILABLE = False

try:
    from google.oauth2 import service_account  # type: ignore
    from google.auth.transport.requests import Request as GoogleAuthRequest  # type: ignore
    from googleapiclient.discovery import build  # type: ignore
    from googleapiclient.errors import HttpError  # type: ignore
    GOOGLE_API_AVAILABLE = True
except Exception:
    service_account = None  # type: ignore
    GoogleAuthRequest = None  # type: ignore
    build = None  # type: ignore

    class HttpError(Exception):  # type: ignore
        def __init__(self, *args, **kwargs):
            super().__init__(*args)

        @property
        def resp(self):  # pragma: no cover
            class _Resp:
                status = 0
            return _Resp()

    GOOGLE_API_AVAILABLE = False

# ---------------------------------------------------------------------------
# Prometheus (optional) — SAFE dummy
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram  # type: ignore
    PROMETHEUS_AVAILABLE = True
except Exception:
    PROMETHEUS_AVAILABLE = False

    class _DummyMetric:  # type: ignore
        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            return None

        def observe(self, *args, **kwargs):
            return None

    Counter = Histogram = _DummyMetric  # type: ignore

# ---------------------------------------------------------------------------
# OpenTelemetry (optional) — SAFE dummy
# ---------------------------------------------------------------------------
try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore
    OTEL_AVAILABLE = True
except Exception:
    trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    OTEL_AVAILABLE = False

# =============================================================================
# Version & Constants
# =============================================================================
SCRIPT_VERSION = "6.3.0"
SCRIPT_NAME = "SheetsAuthDiagnostic"
MIN_PYTHON = (3, 8)

if sys.version_info < MIN_PYTHON:
    sys.stdout.write(f"❌ Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ required\n")
    raise SystemExit(1)

_IO_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=8, thread_name_prefix="AuthDiagWorker")
_TRACING_ENABLED = (os.getenv("CORE_TRACING_ENABLED", "") or os.getenv("TRACING_ENABLED", "")).strip().lower() in {"1", "true", "yes", "y", "on"}

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/drive.file",
]

ENV_CRED_KEYS = [
    "GOOGLE_SHEETS_CREDENTIALS",
    "GOOGLE_CREDENTIALS",
    "TFB_GOOGLE_CREDENTIALS",
    "SHEETS_CREDENTIALS",
    "GOOGLE_APPLICATION_CREDENTIALS",  # may be JSON or file path (we handle both)
]

ENV_SHEET_ID_KEYS = [
    "DEFAULT_SPREADSHEET_ID",
    "TFB_SPREADSHEET_ID",
    "SPREADSHEET_ID",
    "GOOGLE_SHEETS_ID",
    "SHEET_ID",
]

_RIYADH_TZ = timezone(timedelta(hours=3))

# =============================================================================
# Tracing & Metrics
# =============================================================================
class TraceContext:
    """
    Lightweight tracing context that never breaks if OpenTelemetry is missing.
    """
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._span_cm = None
        self._span = None

    def __enter__(self):
        if OTEL_AVAILABLE and _TRACING_ENABLED and trace is not None:
            try:
                tracer = trace.get_tracer(__name__)
                self._span_cm = tracer.start_as_current_span(self.name)
                self._span = self._span_cm.__enter__()
                if self._span and self.attributes:
                    try:
                        self._span.set_attributes(self.attributes)
                    except Exception:
                        pass
            except Exception:
                self._span_cm = None
                self._span = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._span_cm:
            try:
                if exc_val and OTEL_AVAILABLE and Status is not None and StatusCode is not None and self._span is not None:
                    try:
                        self._span.record_exception(exc_val)
                        self._span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                    except Exception:
                        pass
            finally:
                try:
                    self._span_cm.__exit__(exc_type, exc_val, exc_tb)
                except Exception:
                    pass
        return False


if PROMETHEUS_AVAILABLE:
    diag_tests_total = Counter("sheets_diag_tests_total", "Total diagnostic tests run", ["status", "category"])
    diag_duration = Histogram("sheets_diag_duration_seconds", "Diagnostic test duration", ["category"])
else:
    diag_tests_total = Counter()  # type: ignore
    diag_duration = Histogram()  # type: ignore


# =============================================================================
# Enums & Types
# =============================================================================
class TestCategory(str, Enum):
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    NETWORK = "network"
    PERFORMANCE = "performance"
    COMPLIANCE = "compliance"
    INTEGRATION = "integration"
    SECURITY = "security"


class TestSeverity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class PermissionLevel(str, Enum):
    OWNER = "owner"
    WRITER = "writer"
    READER = "reader"
    NONE = "none"


class CredentialType(str, Enum):
    SERVICE_ACCOUNT = "service_account"
    OAUTH2 = "oauth2"
    API_KEY = "api_key"
    JWT = "jwt"
    NONE = "none"


class AuthMethod(str, Enum):
    SERVICE_ACCOUNT_JSON = "service_account_json"
    SERVICE_ACCOUNT_FILE = "service_account_file"
    OAUTH2_TOKEN = "oauth2_token"
    OAUTH2_CLIENT = "oauth2_client"
    API_KEY = "api_key"
    JWT = "jwt"
    NONE = "none"


class CIProvider(str, Enum):
    GITHUB_ACTIONS = "github_actions"
    GITLAB_CI = "gitlab_ci"
    JENKINS = "jenkins"
    CIRCLECI = "circleci"
    TRAVIS = "travis"
    AZURE_DEVOPS = "azure_devops"
    UNKNOWN = "unknown"


# =============================================================================
# Data Models
# =============================================================================
@dataclass(slots=True)
class TestResult:
    name: str
    category: TestCategory
    severity: TestSeverity
    status: bool
    message: str
    duration_ms: float
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "category": self.category.value,
            "severity": self.severity.value,
            "status": bool(self.status),
            "message": self.message,
            "duration_ms": round(float(self.duration_ms), 2),
            "timestamp": self.timestamp,
            "details": self.details or {},
        }


@dataclass(slots=True)
class DiagnosticReport:
    version: str = SCRIPT_VERSION
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    environment: Dict[str, Any] = field(default_factory=dict)
    results: List[Dict[str, Any]] = field(default_factory=list)
    summary: Dict[str, Any] = field(default_factory=dict)
    recommendations: List[str] = field(default_factory=list)

    def add_result(self, result: TestResult) -> None:
        self.results.append(result.to_dict())

    def generate_summary(self) -> None:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.get("status") is True)
        failed = total - passed

        self.summary = {
            "total": total,
            "passed": passed,
            "failed": failed,
            "success_rate": (passed / total * 100.0) if total > 0 else 0.0,
        }

        recs: List[str] = []
        for r in self.results:
            if r.get("status") is True:
                continue
            sev = str(r.get("severity") or "").lower()
            if sev in {"critical", "high"}:
                recs.append(f"Fix {r.get('name')}: {r.get('message')}")
        self.recommendations = recs

    def to_json_obj(self) -> Dict[str, Any]:
        self.generate_summary()
        return {
            "version": self.version,
            "timestamp": self.timestamp,
            "environment": self.environment,
            "summary": self.summary,
            "recommendations": self.recommendations,
            "results": self.results,
        }

    def to_json(self) -> str:
        return json_dumps(self.to_json_obj(), indent=2)

    def to_html(self) -> str:
        self.generate_summary()
        esc = lambda s: str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Sheets Auth Diagnostic v{esc(self.version)}</title>
  <style>
    body {{ font-family: -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif; margin: 36px; background: #f9fafb; color: #111827; }}
    h1 {{ margin: 0 0 6px 0; }}
    .sub {{ color: #6b7280; margin: 0 0 22px 0; }}
    .card {{ background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; padding: 16px 18px; margin: 14px 0; box-shadow: 0 1px 2px rgba(0,0,0,0.04); }}
    table {{ width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #e5e7eb; border-radius: 12px; overflow: hidden; }}
    th, td {{ text-align: left; padding: 10px 12px; border-bottom: 1px solid #e5e7eb; vertical-align: top; }}
    th {{ background: #f3f4f6; color: #374151; font-weight: 600; }}
    .ok {{ color: #059669; font-weight: 700; }}
    .bad {{ color: #dc2626; font-weight: 700; }}
    .sev-critical {{ background: #fee2e2; }}
    .sev-high {{ background: #ffedd5; }}
    code {{ background: #f3f4f6; padding: 2px 6px; border-radius: 6px; }}
  </style>
</head>
<body>
  <h1>Google Sheets Auth Diagnostic</h1>
  <p class="sub">Version {esc(self.version)} • {esc(self.timestamp)}</p>

  <div class="card">
    <h2>Summary</h2>
    <p><b>Total:</b> {self.summary.get("total",0)} • <span class="ok"><b>Passed:</b> {self.summary.get("passed",0)}</span> • <span class="bad"><b>Failed:</b> {self.summary.get("failed",0)}</span> • <b>Success:</b> {self.summary.get("success_rate",0.0):.1f}%</p>
  </div>

  <div class="card">
    <h2>Environment</h2>
    <table>
      <tr><th>Key</th><th>Value</th></tr>
"""
        for k, v in self.environment.items():
            if isinstance(v, (dict, list)):
                vv = esc(json_dumps(v, indent=2))
            else:
                vv = esc(v)
            html += f"<tr><td>{esc(k)}</td><td><code>{vv}</code></td></tr>\n"

        html += """    </table>
  </div>

  <div class="card">
    <h2>Results</h2>
    <table>
      <tr>
        <th>Status</th><th>Severity</th><th>Category</th><th>Name</th><th>Message</th><th>Duration (ms)</th>
      </tr>
"""
        for r in self.results:
            ok = bool(r.get("status"))
            sev = str(r.get("severity") or "").lower()
            row_cls = ""
            if not ok and sev in {"critical", "high"}:
                row_cls = f' class="sev-{sev}"'
            html += f"""      <tr{row_cls}>
        <td class="{("ok" if ok else "bad")}">{("✅" if ok else "❌")}</td>
        <td>{esc(r.get("severity",""))}</td>
        <td>{esc(r.get("category",""))}</td>
        <td>{esc(r.get("name",""))}</td>
        <td>{esc(r.get("message",""))}</td>
        <td>{esc(r.get("duration_ms",0))}</td>
      </tr>
"""
        html += """    </table>
  </div>
"""

        if self.recommendations:
            html += """  <div class="card">
    <h2>Recommendations</h2>
    <ul>
"""
            for rec in self.recommendations:
                html += f"      <li>{esc(rec)}</li>\n"
            html += """    </ul>
  </div>
"""
        html += "</body></html>"
        return html

    def to_junit_xml(self) -> str:
        # minimal JUnit XML (no external deps)
        self.generate_summary()

        def esc(s: str) -> str:
            return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

        total = int(self.summary.get("total", 0))
        failures = int(self.summary.get("failed", 0))
        ts = esc(self.timestamp)

        out = [f'<testsuite name="{esc(SCRIPT_NAME)}" tests="{total}" failures="{failures}" timestamp="{ts}">']
        for r in self.results:
            name = esc(str(r.get("name") or ""))
            cls = esc(str(r.get("category") or ""))
            tsec = float(r.get("duration_ms") or 0.0) / 1000.0
            out.append(f'  <testcase name="{name}" classname="{cls}" time="{tsec:.4f}">')
            if not r.get("status"):
                msg = esc(str(r.get("message") or "failed"))
                sev = esc(str(r.get("severity") or "failure"))
                out.append(f'    <failure type="{sev}" message="{msg}"></failure>')
            out.append("  </testcase>")
        out.append("</testsuite>")
        return "\n".join(out)


# =============================================================================
# CI/CD Integration
# =============================================================================
class CIDetector:
    @staticmethod
    def detect() -> CIProvider:
        if os.getenv("GITHUB_ACTIONS") == "true":
            return CIProvider.GITHUB_ACTIONS
        if os.getenv("GITLAB_CI") == "true":
            return CIProvider.GITLAB_CI
        if os.getenv("JENKINS_HOME") or os.getenv("JENKINS_URL"):
            return CIProvider.JENKINS
        if os.getenv("CIRCLECI") == "true":
            return CIProvider.CIRCLECI
        if os.getenv("TRAVIS") == "true":
            return CIProvider.TRAVIS
        if os.getenv("TF_BUILD") == "true":
            return CIProvider.AZURE_DEVOPS
        return CIProvider.UNKNOWN

    @staticmethod
    def annotate_error(file_path: str, line: int, column: int, message: str) -> None:
        provider = CIDetector.detect()
        if provider == CIProvider.GITHUB_ACTIONS:
            sys.stdout.write(f"::error file={file_path},line={line},col={column}::{message}\n")
        else:
            sys.stdout.write(f"ERROR: {file_path}:{line}:{column} - {message}\n")

    @staticmethod
    def annotate_warning(file_path: str, line: int, column: int, message: str) -> None:
        provider = CIDetector.detect()
        if provider == CIProvider.GITHUB_ACTIONS:
            sys.stdout.write(f"::warning file={file_path},line={line},col={column}::{message}\n")
        else:
            sys.stdout.write(f"WARNING: {file_path}:{line}:{column} - {message}\n")


# =============================================================================
# Credential Manager
# =============================================================================
class CredentialManager:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.credential_data: Optional[Dict[str, Any]] = None
        self.credential_type: CredentialType = CredentialType.NONE
        self.auth_method: AuthMethod = AuthMethod.NONE
        self.credential_source: Optional[str] = None
        self.credentials: Any = None

    def discover(self) -> Tuple[bool, str]:
        raw, source = self._from_env()
        if raw:
            self.credential_source = source
            ok, msg = self._parse_credentials(raw, source_hint=source or "")
            return ok, msg

        raw, source = self._from_file()
        if raw:
            self.credential_source = source
            ok, msg = self._parse_credentials(raw, source_hint=source or "")
            return ok, msg

        return False, "No credentials found in env or file. Set GOOGLE_SHEETS_CREDENTIALS / GOOGLE_CREDENTIALS or GOOGLE_APPLICATION_CREDENTIALS."

    def _from_env(self) -> Tuple[Optional[str], Optional[str]]:
        for key in ENV_CRED_KEYS:
            v = os.getenv(key)
            if not v or not str(v).strip():
                continue
            return str(v).strip(), f"env:{key}"
        return None, None

    def _from_file(self) -> Tuple[Optional[str], Optional[str]]:
        # GOOGLE_APPLICATION_CREDENTIALS can be a file path OR JSON
        gac = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if gac and gac.strip():
            s = gac.strip()
            if s.startswith("{") and s.endswith("}"):
                return s, "env:GOOGLE_APPLICATION_CREDENTIALS(json)"
            if os.path.exists(s):
                try:
                    return Path(s).read_text(encoding="utf-8"), f"file:{s}"
                except Exception as e:
                    self.logger.debug("Failed reading GOOGLE_APPLICATION_CREDENTIALS file: %s", e)

        common_paths = [
            "credentials.json",
            "service-account.json",
            "config/credentials.json",
            "secrets/credentials.json",
            os.path.expanduser("~/.config/gcloud/application_default_credentials.json"),
        ]
        for p in common_paths:
            if os.path.exists(p):
                try:
                    return Path(p).read_text(encoding="utf-8"), f"file:{p}"
                except Exception:
                    continue
        return None, None

    def _parse_credentials(self, raw: str, *, source_hint: str) -> Tuple[bool, str]:
        s = (raw or "").strip()
        if not s:
            return False, "Empty credentials content"

        # If not JSON, try base64 decode once
        if not s.startswith("{"):
            try:
                dec = base64.b64decode(s).decode("utf-8", errors="replace").strip()
                if dec.startswith("{"):
                    s = dec
            except Exception:
                pass

        try:
            data = json_loads(s)
        except Exception as e:
            return False, f"Invalid JSON credentials: {e}"

        if not isinstance(data, dict):
            return False, "Credentials JSON must be an object/dict"

        # Detect type
        if "client_email" in data and "private_key" in data:
            self.credential_type = CredentialType.SERVICE_ACCOUNT
            self.auth_method = AuthMethod.SERVICE_ACCOUNT_JSON if "env:" in source_hint else AuthMethod.SERVICE_ACCOUNT_FILE
        elif "access_token" in data:
            self.credential_type = CredentialType.OAUTH2
            self.auth_method = AuthMethod.OAUTH2_TOKEN if "refresh_token" in data else AuthMethod.OAUTH2_CLIENT
        elif "api_key" in data:
            self.credential_type = CredentialType.API_KEY
            self.auth_method = AuthMethod.API_KEY
        else:
            self.credential_type = CredentialType.NONE
            self.auth_method = AuthMethod.NONE
            return False, "Unknown credentials format (expected service account JSON or OAuth token)"

        # Validate service account minimum fields + repair private key
        if self.credential_type == CredentialType.SERVICE_ACCOUNT:
            required = ["client_email", "private_key", "project_id"]
            missing = [k for k in required if not str(data.get(k) or "").strip()]
            if missing:
                return False, f"Missing required service account fields: {missing}"

            data["private_key"] = self._repair_private_key(str(data.get("private_key") or ""))
            if "BEGIN PRIVATE KEY" not in data["private_key"]:
                return False, "Private key does not look valid after repair"
            if "@" not in str(data.get("client_email", "")):
                return False, "client_email is invalid"

        self.credential_data = data
        return True, f"Valid {self.credential_type.value} credentials from {source_hint}"

    @staticmethod
    def _repair_private_key(key: str) -> str:
        k = (key or "").strip()
        if not k:
            return k

        # strip surrounding quotes if present
        if len(k) >= 2 and k[0] == k[-1] and k[0] in {"'", '"'}:
            k = k[1:-1].strip()

        # normalize escaped newlines
        k = k.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\r\n", "\n")

        # ensure headers exist
        if "-----BEGIN PRIVATE KEY-----" not in k:
            k = "-----BEGIN PRIVATE KEY-----\n" + k
        if "-----END PRIVATE KEY-----" not in k:
            k = k + "\n-----END PRIVATE KEY-----\n"
        return k

    def build_credentials(self) -> Any:
        if not GOOGLE_API_AVAILABLE:
            raise ImportError("Google API libraries not available (google-auth, google-api-python-client)")

        if self.credential_type == CredentialType.SERVICE_ACCOUNT:
            if service_account is None:
                raise ImportError("google.oauth2.service_account not available")
            self.credentials = service_account.Credentials.from_service_account_info(self.credential_data or {}, scopes=SCOPES)
            return self.credentials

        # OAuth2 flows can be added later if you need them.
        raise ValueError(f"Unsupported credential type for this diagnostic: {self.credential_type.value}")

    def service_account_email(self) -> Optional[str]:
        if isinstance(self.credential_data, dict):
            v = self.credential_data.get("client_email")
            return str(v) if v else None
        return None

    def project_id(self) -> Optional[str]:
        if isinstance(self.credential_data, dict):
            v = self.credential_data.get("project_id")
            return str(v) if v else None
        return None


# =============================================================================
# Network Diagnostics (best-effort)
# =============================================================================
class NetworkDiagnostics:
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def diagnose(self) -> Dict[str, Any]:
        res: Dict[str, Any] = {}
        for name, fn in [
            ("dns", self._dns),
            ("tcp", self._tcp),
            ("tls", self._tls),
            ("latency", self._latency),
        ]:
            try:
                t0 = time.time()
                out = fn()
                out["duration_ms"] = (time.time() - t0) * 1000.0
                res[name] = out
            except Exception as e:
                res[name] = {"ok": False, "error": str(e), "duration_ms": 0.0}
        return res

    def _dns(self) -> Dict[str, Any]:
        hosts = ["sheets.googleapis.com", "www.googleapis.com", "oauth2.googleapis.com"]
        ips: Dict[str, List[str]] = {}
        cnames: Dict[str, str] = {}
        ok_any = False

        for h in hosts:
            try:
                ip_list = socket.gethostbyname_ex(h)[2]
                ips[h] = ip_list
                ok_any = ok_any or bool(ip_list)
            except Exception as e:
                ips[h] = []
                cnames[h] = f"error: {e}"

            if DNS_AVAILABLE:
                try:
                    answers = dns.resolver.resolve(h, "CNAME")  # type: ignore
                    if answers:
                        cnames[h] = str(answers[0].target)
                except Exception:
                    pass

        return {"ok": ok_any, "ips": ips, "cnames": cnames, "dns_available": DNS_AVAILABLE}

    def _tcp(self) -> Dict[str, Any]:
        endpoints = [("sheets.googleapis.com", 443), ("www.googleapis.com", 443), ("oauth2.googleapis.com", 443)]
        ok_any = False
        ok_list: List[str] = []
        failed: List[str] = []
        for host, port in endpoints:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect((host, port))
                s.close()
                ok_any = True
                ok_list.append(f"{host}:{port}")
            except Exception as e:
                failed.append(f"{host}:{port} ({e})")
        return {"ok": ok_any, "ok_endpoints": ok_list, "failed_endpoints": failed}

    def _tls(self) -> Dict[str, Any]:
        try:
            ctx = ssl.create_default_context()
            with socket.create_connection(("sheets.googleapis.com", 443), timeout=5) as sock:
                with ctx.wrap_socket(sock, server_hostname="sheets.googleapis.com") as ssock:
                    cert = ssock.getpeercert()
                    return {
                        "ok": True,
                        "protocol": ssock.version(),
                        "cipher": ssock.cipher(),
                        "cert_subject": cert.get("subject"),
                        "cert_issuer": cert.get("issuer"),
                        "cert_notAfter": cert.get("notAfter"),
                    }
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _latency(self) -> Dict[str, Any]:
        samples: List[float] = []
        for _ in range(5):
            try:
                t0 = time.perf_counter()
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect(("sheets.googleapis.com", 443))
                s.close()
                samples.append((time.perf_counter() - t0) * 1000.0)
            except Exception:
                pass
        if not samples:
            return {"ok": False, "samples_ms": [], "avg_ms": None}
        return {"ok": True, "samples_ms": samples, "avg_ms": sum(samples) / len(samples)}


# =============================================================================
# Google API helpers
# =============================================================================
def _get_sheet_id_from_env_or_args(arg_sheet_id: Optional[str]) -> str:
    if arg_sheet_id and arg_sheet_id.strip():
        return arg_sheet_id.strip()
    for k in ENV_SHEET_ID_KEYS:
        v = os.getenv(k)
        if v and v.strip():
            return v.strip()
    return ""


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _now_riyadh_iso() -> str:
    return datetime.now(_RIYADH_TZ).isoformat()


# =============================================================================
# Main Diagnostic Engine
# =============================================================================
class DiagnosticEngine:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.logger = self._setup_logging()
        self.report = DiagnosticReport()
        self.cred = CredentialManager(self.logger)
        self.net = NetworkDiagnostics(self.logger)

        self.sheets = None
        self.drive = None
        self.sheet_id = _get_sheet_id_from_env_or_args(args.sheet_id)
        self._capture_environment()

    def _setup_logging(self) -> logging.Logger:
        level = logging.DEBUG if self.args.verbose else logging.INFO
        cfg = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {
                    "format": "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                    "datefmt": "%H:%M:%S",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": level,
                    "formatter": "standard",
                    "stream": "ext://sys.stdout",
                }
            },
            "root": {"level": level, "handlers": ["console"]},
        }
        logging.config.dictConfig(cfg)
        return logging.getLogger("SheetsDiag")

    def _capture_environment(self) -> None:
        self.report.environment = {
            "script": SCRIPT_NAME,
            "version": SCRIPT_VERSION,
            "python_version": sys.version.split()[0],
            "python_full": sys.version,
            "platform": platform.platform(),
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "cwd": os.getcwd(),
            "utc_now": _now_utc_iso(),
            "riyadh_now": _now_riyadh_iso(),
            "has_orjson": _HAS_ORJSON,
            "google_api_available": GOOGLE_API_AVAILABLE,
            "requests_available": REQUESTS_AVAILABLE,
            "dns_available": DNS_AVAILABLE,
            "prometheus_available": PROMETHEUS_AVAILABLE,
            "otel_available": OTEL_AVAILABLE,
            "tracing_enabled": _TRACING_ENABLED,
            "sheet_id_provided": bool(self.sheet_id),
        }

    async def _run_in_pool(self, fn: Callable[[], TestResult]) -> TestResult:
        with TraceContext(f"diag_{getattr(fn, '__name__', 'fn')}"):
            t0 = time.time()
            loop = asyncio.get_running_loop()
            result: TestResult = await loop.run_in_executor(_IO_EXECUTOR, fn)
            try:
                cat = result.category.value
                diag_duration.labels(category=cat).observe(max(0.0, time.time() - t0))
                diag_tests_total.labels(status=("success" if result.status else "error"), category=cat).inc()
            except Exception:
                pass
            return result

    async def run(self) -> int:
        started = time.time()

        # Network-only mode
        if self.args.network_only:
            self.report.add_result(self.test_network_connectivity())
            self.report.summary["total_duration_ms"] = (time.time() - started) * 1000.0
            await asyncio.get_running_loop().run_in_executor(_IO_EXECUTOR, self._output_results)
            return 0 if all(r.get("status") for r in self.report.results) else 1

        # Phase 1: network + credential discovery
        net_res, cred_res = await asyncio.gather(
            self._run_in_pool(self.test_network_connectivity),
            self._run_in_pool(self.test_credential_validation),
        )
        self.report.add_result(net_res)
        self.report.add_result(cred_res)

        # Phase 2: build clients + auth
        if cred_res.status:
            auth_res = await self._run_in_pool(self.test_api_authentication)
            self.report.add_result(auth_res)

            # Phase 3: actions if auth ok
            if auth_res.status:
                tasks = [
                    self._run_in_pool(self.test_spreadsheet_access),
                    self._run_in_pool(self.test_read_operations),
                    self._run_in_pool(self.test_batch_operations),
                    self._run_in_pool(self.test_permissions),
                    self._run_in_pool(self.test_rate_limits),
                ]
                if self.args.write:
                    tasks.append(self._run_in_pool(self.test_write_operations))
                results = await asyncio.gather(*tasks)
                for r in results:
                    self.report.add_result(r)

        # Compliance can run anytime
        comp = await self._run_in_pool(self.test_compliance)
        self.report.add_result(comp)

        self.report.summary["total_duration_ms"] = (time.time() - started) * 1000.0
        await asyncio.get_running_loop().run_in_executor(_IO_EXECUTOR, self._output_results)

        # Exit code logic
        critical = any((not r.get("status")) and str(r.get("severity")) == TestSeverity.CRITICAL.value for r in self.report.results)
        high = any((not r.get("status")) and str(r.get("severity")) == TestSeverity.HIGH.value for r in self.report.results)
        any_fail = any((not r.get("status")) for r in self.report.results)

        if critical:
            return 1
        if high:
            return 2
        if any_fail:
            return 3
        return 0

    # -----------------------------
    # Tests (blocking, pool-safe)
    # -----------------------------
    def test_network_connectivity(self) -> TestResult:
        t0 = time.time()
        try:
            details = self.net.diagnose()
            ok = True
            # treat TCP+TLS as most important; DNS can be flaky in some envs
            if not bool(details.get("tcp", {}).get("ok")):
                ok = False
            if not bool(details.get("tls", {}).get("ok")):
                ok = False
            msg = "Network connectivity looks OK" if ok else "Network issues detected (tcp/tls)"
            return TestResult(
                name="Network Connectivity",
                category=TestCategory.NETWORK,
                severity=TestSeverity.CRITICAL,
                status=ok,
                message=msg,
                duration_ms=(time.time() - t0) * 1000.0,
                details=details,
            )
        except Exception as e:
            return TestResult(
                name="Network Connectivity",
                category=TestCategory.NETWORK,
                severity=TestSeverity.CRITICAL,
                status=False,
                message=f"Network test failed: {e}",
                duration_ms=(time.time() - t0) * 1000.0,
                details={},
            )

    def test_credential_validation(self) -> TestResult:
        t0 = time.time()
        ok, msg = self.cred.discover()
        details = {
            "credential_type": self.cred.credential_type.value,
            "auth_method": self.cred.auth_method.value,
            "source": self.cred.credential_source,
            "service_account": self.cred.service_account_email(),
            "project_id": self.cred.project_id(),
        }
        return TestResult(
            name="Credential Validation",
            category=TestCategory.AUTHENTICATION,
            severity=TestSeverity.CRITICAL,
            status=ok,
            message=msg,
            duration_ms=(time.time() - t0) * 1000.0,
            details=details,
        )

    def test_api_authentication(self) -> TestResult:
        t0 = time.time()
        if not GOOGLE_API_AVAILABLE:
            return TestResult(
                name="API Authentication",
                category=TestCategory.AUTHENTICATION,
                severity=TestSeverity.CRITICAL,
                status=False,
                message="Google API libraries not available (google-auth, google-api-python-client).",
                duration_ms=(time.time() - t0) * 1000.0,
                details={},
            )

        try:
            creds = self.cred.build_credentials()

            # Refresh token if needed
            try:
                if hasattr(creds, "expired") and creds.expired and GoogleAuthRequest is not None:
                    creds.refresh(GoogleAuthRequest())
            except Exception:
                pass

            # Build clients
            self.sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
            try:
                self.drive = build("drive", "v3", credentials=creds, cache_discovery=False)
            except Exception:
                self.drive = None

            # Auth proof:
            # - if sheet_id available: try spreadsheets.get (metadata)
            # - else: try drive.about.get (metadata)
            proof_ok = False
            proof_details: Dict[str, Any] = {"sheet_id": self.sheet_id or None}

            if self.sheet_id:
                try:
                    meta = self.sheets.spreadsheets().get(  # type: ignore
                        spreadsheetId=self.sheet_id,
                        fields="spreadsheetId,properties.title,properties.timeZone",
                    ).execute()
                    proof_ok = True
                    proof_details["sheet_title"] = (meta.get("properties") or {}).get("title")
                    proof_details["sheet_timezone"] = (meta.get("properties") or {}).get("timeZone")
                except HttpError as he:
                    # 403/404 are still "authenticated" but not authorized; we report separately later
                    proof_ok = True
                    proof_details["sheets_get_http_status"] = getattr(getattr(he, "resp", None), "status", None)
                    proof_details["sheets_get_error"] = str(he)[:300]
            else:
                if self.drive is not None:
                    try:
                        about = self.drive.about().get(fields="user(emailAddress),storageQuota").execute()  # type: ignore
                        proof_ok = True
                        proof_details["drive_user"] = (about.get("user") or {}).get("emailAddress")
                    except Exception as e:
                        proof_ok = False
                        proof_details["drive_about_error"] = str(e)[:300]
                else:
                    proof_ok = True
                    proof_details["note"] = "No sheet_id provided; Drive client not available; built Sheets client OK"

            details = {
                "auth_method": self.cred.auth_method.value,
                "credential_type": self.cred.credential_type.value,
                "service_account": self.cred.service_account_email(),
                "project_id": self.cred.project_id(),
                "proof": proof_details,
                "scopes": getattr(creds, "scopes", None),
            }

            return TestResult(
                name="API Authentication",
                category=TestCategory.AUTHENTICATION,
                severity=TestSeverity.CRITICAL,
                status=proof_ok,
                message="Successfully built Google API clients" if proof_ok else "Failed to prove auth via API call",
                duration_ms=(time.time() - t0) * 1000.0,
                details=details,
            )
        except Exception as e:
            return TestResult(
                name="API Authentication",
                category=TestCategory.AUTHENTICATION,
                severity=TestSeverity.CRITICAL,
                status=False,
                message=f"Authentication/client build failed: {type(e).__name__}: {e}",
                duration_ms=(time.time() - t0) * 1000.0,
                details={},
            )

    def test_spreadsheet_access(self) -> TestResult:
        t0 = time.time()
        if not self.sheets:
            return TestResult(
                name="Spreadsheet Access",
                category=TestCategory.AUTHORIZATION,
                severity=TestSeverity.HIGH,
                status=False,
                message="Sheets API client not initialized",
                duration_ms=(time.time() - t0) * 1000.0,
                details={},
            )
        if not self.sheet_id:
            return TestResult(
                name="Spreadsheet Access",
                category=TestCategory.AUTHORIZATION,
                severity=TestSeverity.HIGH,
                status=False,
                message="No spreadsheet ID provided (use --sheet-id or DEFAULT_SPREADSHEET_ID env).",
                duration_ms=(time.time() - t0) * 1000.0,
                details={},
            )

        try:
            meta = self.sheets.spreadsheets().get(  # type: ignore
                spreadsheetId=self.sheet_id,
                fields="spreadsheetId,properties.title,properties.timeZone,sheets.properties(title,sheetId,hidden)",
            ).execute()
            title = (meta.get("properties") or {}).get("title")
            tz = (meta.get("properties") or {}).get("timeZone")
            sheets = meta.get("sheets") or []
            details = {
                "title": title,
                "timezone": tz,
                "sheet_count": len(sheets),
                "url": f"https://docs.google.com/spreadsheets/d/{self.sheet_id}",
            }
            return TestResult(
                name="Spreadsheet Access",
                category=TestCategory.AUTHORIZATION,
                severity=TestSeverity.HIGH,
                status=True,
                message=f"Access OK: {title}",
                duration_ms=(time.time() - t0) * 1000.0,
                details=details,
            )
        except HttpError as he:
            code = getattr(getattr(he, "resp", None), "status", None)
            msg = "Permission denied (share sheet with service account)" if code == 403 else "Spreadsheet not found (check ID)" if code == 404 else f"HTTP {code}: {he}"
            return TestResult(
                name="Spreadsheet Access",
                category=TestCategory.AUTHORIZATION,
                severity=TestSeverity.HIGH,
                status=False,
                message=msg,
                duration_ms=(time.time() - t0) * 1000.0,
                details={"http_status": code, "error": str(he)[:500]},
            )
        except Exception as e:
            return TestResult(
                name="Spreadsheet Access",
                category=TestCategory.AUTHORIZATION,
                severity=TestSeverity.HIGH,
                status=False,
                message=f"Spreadsheet access failed: {type(e).__name__}: {e}",
                duration_ms=(time.time() - t0) * 1000.0,
                details={},
            )

    def test_permissions(self) -> TestResult:
        t0 = time.time()
        if not self.drive or not self.sheet_id:
            return TestResult(
                name="Permission Audit",
                category=TestCategory.AUTHORIZATION,
                severity=TestSeverity.MEDIUM,
                status=True,
                message="Permission audit skipped (Drive client unavailable or no sheet_id).",
                duration_ms=(time.time() - t0) * 1000.0,
                details={"skipped": True},
            )

        try:
            perms = self.drive.permissions().list(  # type: ignore
                fileId=self.sheet_id,
                fields="permissions(id,emailAddress,role,type,domain)",
                pageSize=100,
            ).execute()
            plist = perms.get("permissions") or []
            sa = self.cred.service_account_email()
            sa_role = None
            owners: List[str] = []
            for p in plist:
                if p.get("role") == "owner" and p.get("emailAddress"):
                    owners.append(p.get("emailAddress"))
                if sa and p.get("emailAddress") == sa:
                    sa_role = p.get("role")

            ok = True
            if self.args.write:
                ok = (sa_role in {"owner", "writer"})

            details = {
                "service_account": sa,
                "service_account_role": sa_role,
                "owners": owners,
                "permissions_count": len(plist),
                "permissions_sample": plist[:10],
            }
            msg = f"Service account role: {sa_role or 'not_found'}"
            if self.args.write and not ok:
                msg = msg + " (write requires writer/owner)"
            return TestResult(
                name="Permission Audit",
                category=TestCategory.AUTHORIZATION,
                severity=TestSeverity.MEDIUM,
                status=ok,
                message=msg,
                duration_ms=(time.time() - t0) * 1000.0,
                details=details,
            )
        except HttpError as he:
            code = getattr(getattr(he, "resp", None), "status", None)
            return TestResult(
                name="Permission Audit",
                category=TestCategory.AUTHORIZATION,
                severity=TestSeverity.MEDIUM,
                status=False,
                message=f"Drive permissions list failed (HTTP {code}).",
                duration_ms=(time.time() - t0) * 1000.0,
                details={"http_status": code, "error": str(he)[:500]},
            )
        except Exception as e:
            return TestResult(
                name="Permission Audit",
                category=TestCategory.AUTHORIZATION,
                severity=TestSeverity.MEDIUM,
                status=False,
                message=f"Permission audit failed: {type(e).__name__}: {e}",
                duration_ms=(time.time() - t0) * 1000.0,
                details={},
            )

    def test_read_operations(self) -> TestResult:
        t0 = time.time()
        if not self.sheets or not self.sheet_id:
            return TestResult(
                name="Read Operations",
                category=TestCategory.INTEGRATION,
                severity=TestSeverity.MEDIUM,
                status=False,
                message="Sheets client not ready or sheet_id missing.",
                duration_ms=(time.time() - t0) * 1000.0,
                details={},
            )

        rng = f"'{self.args.sheet_name}'!{self.args.read_range}"
        try:
            res = self.sheets.spreadsheets().values().get(  # type: ignore
                spreadsheetId=self.sheet_id,
                range=rng,
            ).execute()
            values = res.get("values") or []
            details = {
                "range": res.get("range"),
                "rows": len(values),
                "cols": (len(values[0]) if values else 0),
                "sample": values[:3],
            }
            return TestResult(
                name="Read Operations",
                category=TestCategory.INTEGRATION,
                severity=TestSeverity.MEDIUM,
                status=True,
                message=f"Read OK: {len(values)} rows from {rng}",
                duration_ms=(time.time() - t0) * 1000.0,
                details=details,
            )
        except HttpError as he:
            code = getattr(getattr(he, "resp", None), "status", None)
            msg = "Permission denied for range read" if code == 403 else f"HTTP {code}: {he}"
            return TestResult(
                name="Read Operations",
                category=TestCategory.INTEGRATION,
                severity=TestSeverity.MEDIUM,
                status=False,
                message=msg,
                duration_ms=(time.time() - t0) * 1000.0,
                details={"range": rng, "http_status": code, "error": str(he)[:500]},
            )
        except Exception as e:
            return TestResult(
                name="Read Operations",
                category=TestCategory.INTEGRATION,
                severity=TestSeverity.MEDIUM,
                status=False,
                message=f"Read failed: {type(e).__name__}: {e}",
                duration_ms=(time.time() - t0) * 1000.0,
                details={"range": rng},
            )

    def test_write_operations(self) -> TestResult:
        t0 = time.time()
        if not self.args.write:
            return TestResult(
                name="Write Operations",
                category=TestCategory.INTEGRATION,
                severity=TestSeverity.LOW,
                status=True,
                message="Write tests skipped (use --write to enable).",
                duration_ms=(time.time() - t0) * 1000.0,
                details={"skipped": True},
            )
        if not self.sheets or not self.sheet_id:
            return TestResult(
                name="Write Operations",
                category=TestCategory.INTEGRATION,
                severity=TestSeverity.MEDIUM,
                status=False,
                message="Sheets client not ready or sheet_id missing.",
                duration_ms=(time.time() - t0) * 1000.0,
                details={},
            )

        cell = f"'{self.args.sheet_name}'!{self.args.cell}"
        payload = [[f"TFB AuthDiag {SCRIPT_VERSION}", _now_utc_iso(), _now_riyadh_iso()]]

        try:
            upd = self.sheets.spreadsheets().values().update(  # type: ignore
                spreadsheetId=self.sheet_id,
                range=cell,
                valueInputOption="RAW",
                body={"values": payload},
            ).execute()
            # verify readback
            got = self.sheets.spreadsheets().values().get(  # type: ignore
                spreadsheetId=self.sheet_id,
                range=cell,
            ).execute()
            ok = (got.get("values") == payload)
            return TestResult(
                name="Write Operations",
                category=TestCategory.INTEGRATION,
                severity=TestSeverity.MEDIUM,
                status=ok,
                message=f"Wrote to {upd.get('updatedRange')} (verified={ok})",
                duration_ms=(time.time() - t0) * 1000.0,
                details={
                    "cell": cell,
                    "updated_range": upd.get("updatedRange"),
                    "updated_cells": upd.get("updatedCells"),
                    "written": payload,
                    "read_back": got.get("values"),
                },
            )
        except HttpError as he:
            code = getattr(getattr(he, "resp", None), "status", None)
            msg = "Write permission denied (need writer/owner)" if code == 403 else f"HTTP {code}: {he}"
            return TestResult(
                name="Write Operations",
                category=TestCategory.INTEGRATION,
                severity=TestSeverity.MEDIUM,
                status=False,
                message=msg,
                duration_ms=(time.time() - t0) * 1000.0,
                details={"cell": cell, "http_status": code, "error": str(he)[:500]},
            )
        except Exception as e:
            return TestResult(
                name="Write Operations",
                category=TestCategory.INTEGRATION,
                severity=TestSeverity.MEDIUM,
                status=False,
                message=f"Write failed: {type(e).__name__}: {e}",
                duration_ms=(time.time() - t0) * 1000.0,
                details={"cell": cell},
            )

    def test_batch_operations(self) -> TestResult:
        t0 = time.time()
        if not self.sheets or not self.sheet_id:
            return TestResult(
                name="Batch Operations",
                category=TestCategory.INTEGRATION,
                severity=TestSeverity.LOW,
                status=False,
                message="Sheets client not ready or sheet_id missing.",
                duration_ms=(time.time() - t0) * 1000.0,
                details={},
            )
        try:
            res = self.sheets.spreadsheets().batchUpdate(  # type: ignore
                spreadsheetId=self.sheet_id,
                body={"requests": []},
            ).execute()
            return TestResult(
                name="Batch Operations",
                category=TestCategory.INTEGRATION,
                severity=TestSeverity.LOW,
                status=True,
                message="BatchUpdate supported",
                duration_ms=(time.time() - t0) * 1000.0,
                details={"replies": len(res.get("replies") or [])},
            )
        except Exception as e:
            return TestResult(
                name="Batch Operations",
                category=TestCategory.INTEGRATION,
                severity=TestSeverity.LOW,
                status=False,
                message=f"BatchUpdate failed: {type(e).__name__}: {e}",
                duration_ms=(time.time() - t0) * 1000.0,
                details={},
            )

    def test_rate_limits(self) -> TestResult:
        t0 = time.time()
        if not self.sheets or not self.sheet_id:
            return TestResult(
                name="Rate Limits",
                category=TestCategory.PERFORMANCE,
                severity=TestSeverity.MEDIUM,
                status=False,
                message="Sheets client not ready or sheet_id missing.",
                duration_ms=(time.time() - t0) * 1000.0,
                details={},
            )

        # Quick burst test (best-effort). We do not try to hit limits; just detect immediate 429.
        ok = True
        hit_429 = False
        errors: List[str] = []
        n_ok = 0

        try:
            for i in range(10):
                try:
                    self.sheets.spreadsheets().get(  # type: ignore
                        spreadsheetId=self.sheet_id,
                        fields="spreadsheetId",
                    ).execute()
                    n_ok += 1
                    time.sleep(0.1)
                except HttpError as he:
                    code = getattr(getattr(he, "resp", None), "status", None)
                    if code == 429:
                        hit_429 = True
                        ok = False
                        errors.append(f"429 after {i+1} requests")
                        break
                    errors.append(f"HTTP {code}: {str(he)[:120]}")
                except Exception as e:
                    errors.append(str(e)[:120])
        except Exception as e:
            ok = False
            errors.append(str(e)[:200])

        msg = f"No 429 observed ({n_ok} OK requests)" if ok else "Rate limiting or errors observed"
        return TestResult(
            name="Rate Limits",
            category=TestCategory.PERFORMANCE,
            severity=TestSeverity.MEDIUM,
            status=ok,
            message=msg,
            duration_ms=(time.time() - t0) * 1000.0,
            details={"ok_requests": n_ok, "hit_429": hit_429, "errors": errors[:10]},
        )

    def test_compliance(self) -> TestResult:
        t0 = time.time()
        checks: List[Dict[str, Any]] = []

        # Simple compliance heuristics: no secrets printing + env location hint
        require_auth = (os.getenv("REQUIRE_AUTH", "true") or "true").strip().lower() in {"1", "true", "yes", "y", "on"}
        token_present = bool((os.getenv("TFB_TOKEN", "") or os.getenv("BACKEND_TOKEN", "") or os.getenv("X_APP_TOKEN", "")).strip())

        checks.append({"check": "require_auth_env", "passed": True, "message": f"REQUIRE_AUTH={require_auth}"})
        if require_auth:
            checks.append({"check": "backend_token_present", "passed": token_present, "message": "Backend token present" if token_present else "No backend token detected"})
        else:
            checks.append({"check": "open_mode_warning", "passed": True, "message": "Auth not required (open mode) - ensure this is intentional"})

        # Credentials sanity
        if self.cred.credential_type == CredentialType.SERVICE_ACCOUNT and isinstance(self.cred.credential_data, dict):
            pk = str(self.cred.credential_data.get("private_key") or "")
            checks.append({"check": "private_key_present", "passed": bool(pk.strip()), "message": "Private key present"})
            # We cannot ensure encryption of SA keys; we only warn if it looks malformed
            checks.append({"check": "private_key_format", "passed": ("BEGIN PRIVATE KEY" in pk and "END PRIVATE KEY" in pk), "message": "Private key appears well-formed"})

        ok = all(bool(c.get("passed")) for c in checks)
        msg = f"Compliance checks: {sum(1 for c in checks if c.get('passed'))}/{len(checks)} passed"
        return TestResult(
            name="Compliance Check",
            category=TestCategory.COMPLIANCE,
            severity=TestSeverity.HIGH,
            status=ok,
            message=msg,
            duration_ms=(time.time() - t0) * 1000.0,
            details={"checks": checks},
        )

    # -----------------------------
    # Output
    # -----------------------------
    def _output_results(self) -> None:
        self.report.generate_summary()

        def out(line: str = "") -> None:
            sys.stdout.write(line + "\n")

        summary = self.report.summary or {}
        if self.args.ci_mode:
            if summary.get("failed", 0) > 0:
                out(f"❌ {summary.get('failed', 0)} tests failed")
            else:
                out(f"✅ All {summary.get('total', 0)} tests passed")
        elif self.args.quiet:
            out(f"Passed: {summary.get('passed', 0)}, Failed: {summary.get('failed', 0)}")
        else:
            out("")
            out("=" * 88)
            out(f"🔍 GOOGLE SHEETS AUTH DIAGNOSTIC REPORT v{SCRIPT_VERSION}")
            out("=" * 88)
            for r in self.report.results:
                status = "✅" if r.get("status") else "❌"
                sev = str(r.get("severity") or "").upper()
                out(f"{status} [{sev:8}] {r.get('name')}: {r.get('message')}")
            out("-" * 88)
            out(f"SUMMARY: {summary.get('passed', 0)}/{summary.get('total', 0)} passed ({summary.get('success_rate', 0.0):.1f}%)")
            if self.report.recommendations:
                out("")
                out("📋 RECOMMENDATIONS:")
                for rec in self.report.recommendations:
                    out(f"  • {rec}")
            out("=" * 88)

        if self.args.output:
            p = Path(self.args.output)
            try:
                p.parent.mkdir(parents=True, exist_ok=True)
            except Exception:
                pass

            try:
                ext = p.suffix.lower()
                if ext == ".json":
                    p.write_text(self.report.to_json(), encoding="utf-8")
                    out(f"📄 JSON report saved to {str(p)}")
                elif ext == ".html":
                    p.write_text(self.report.to_html(), encoding="utf-8")
                    out(f"📄 HTML report saved to {str(p)}")
                elif ext in {".xml", ".junit"}:
                    p.write_text(self.report.to_junit_xml(), encoding="utf-8")
                    out(f"📄 JUnit XML saved to {str(p)}")
                else:
                    # default to json
                    p.write_text(self.report.to_json(), encoding="utf-8")
                    out(f"📄 Report saved (json) to {str(p)}")
            except Exception as e:
                out(f"⚠️ Failed to write output file: {e}")


# =============================================================================
# CLI
# =============================================================================
def create_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=f"Google Sheets Authentication Diagnostic Tool v{SCRIPT_VERSION}",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--sheet-id", default="", help="Spreadsheet ID to test (or use DEFAULT_SPREADSHEET_ID env)")
    p.add_argument("--sheet-name", default="Sheet1", help='Tab name (default: "Sheet1")')
    p.add_argument("--read-range", default="A1:B2", help='Range to read (default: "A1:B2")')
    p.add_argument("--cell", default="A1", help='Cell for write test (default: "A1")')
    p.add_argument("--write", action="store_true", help="Include write test (DANGEROUS: writes to the sheet)")
    p.add_argument("--network-only", action="store_true", help="Only run network diagnostics and exit")
    p.add_argument("--output", default="", help="Output file path (.json / .html / .xml)")
    p.add_argument("--ci-mode", action="store_true", help="CI-friendly output")
    p.add_argument("--quiet", action="store_true", help="Minimal output")
    p.add_argument("--verbose", action="store_true", help="Verbose logging")
    return p


def main() -> int:
    args = create_parser().parse_args()
    engine = DiagnosticEngine(args)
    try:
        return asyncio.run(engine.run())
    except KeyboardInterrupt:
        sys.stdout.write("Interrupted.\n")
        return 130
    finally:
        try:
            _IO_EXECUTOR.shutdown(wait=False)
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
