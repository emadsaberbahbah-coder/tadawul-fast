#!/usr/bin/env python3
"""
scripts/test_api_suite.py
================================================================================
TADAWUL FAST BRIDGE – API TEST SUITE (v6.2.0)
================================================================================
LIVE-ROUTE ALIGNED • PS/RENDER DIAGNOSTIC FRIENDLY • HTTPX SAFE
REQUEST-ID SAFE • PARTIAL/TIMEOUT AWARE • JSON/HTML REPORTING

What v6.2.0 revises
-------------------
- FIX: Replaced several outdated/non-canonical endpoints with the live routes
       actually mounted by the service.
- FIX: Hardened OpenTelemetry TraceContext usage; no invalid attach()/span mix.
- FIX: Added route-aware response validators for sheet-rows/schema endpoints.
- FIX: Handles partial responses such as Insights_Analysis timeout as WARN,
       not blanket FAIL, when the contract itself is returned correctly.
- FIX: Cleaner auth/header handling with X-APP-TOKEN + Bearer support.
- FIX: Safer HTTP retry behavior for transient 429/5xx only.
- ENH: JSON/HTML report output preserved, but test results now include
       contract counts and route-specific notes.

Primary focus
-------------
This suite now tests the canonical public routes you have already validated:
- /
- /health
- /meta
- /readyz
- /livez
- /openapi.json
- /v1/analysis/sheet-rows
- /v1/advanced/sheet-rows
- /v1/advisor/sheet-rows
- /sheet-rows
- /v1/schema/sheet-spec
"""
from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import csv
import io
import logging
import math
import os
import sys
import time
import uuid
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from html import escape as html_escape
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

# ---------------------------------------------------------------------------
# High-performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_dumps(obj: Any, *, indent: int = 0) -> str:
        option = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(obj, option=option).decode("utf-8")

    def json_loads(data: Union[str, bytes]) -> Any:
        return orjson.loads(data)

    _HAS_ORJSON = True
except Exception:
    import json

    def json_dumps(obj: Any, *, indent: int = 0) -> str:
        return json.dumps(obj, indent=indent if indent else None, default=str, ensure_ascii=False)

    def json_loads(data: Union[str, bytes]) -> Any:
        return json.loads(data)

    _HAS_ORJSON = False

# ---------------------------------------------------------------------------
# Optional dependencies
# ---------------------------------------------------------------------------
try:
    import httpx  # type: ignore
    from httpx import AsyncClient, Limits, Timeout
    HTTPX_AVAILABLE = True
except Exception:
    httpx = None  # type: ignore
    AsyncClient = None  # type: ignore
    Limits = None  # type: ignore
    Timeout = None  # type: ignore
    HTTPX_AVAILABLE = False

try:
    from opentelemetry import trace  # type: ignore
    from opentelemetry.trace import Status, StatusCode  # type: ignore
    OPENTELEMETRY_AVAILABLE = True
    tracer = trace.get_tracer(__name__)
except Exception:
    OPENTELEMETRY_AVAILABLE = False
    trace = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    tracer = None

SCRIPT_VERSION = "6.2.0"
_UTC = timezone.utc
_CPU_EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="APITestWorker")
_TRACING_ENABLED = os.getenv("CORE_TRACING_ENABLED", "").strip().lower() in {"1", "true", "yes", "y", "on"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("APITest")


def utc_now() -> datetime:
    return datetime.now(_UTC)


# =============================================================================
# Tracing
# =============================================================================
class TraceContext(AbstractAsyncContextManager):
    def __init__(self, name: str, attributes: Optional[Dict[str, Any]] = None):
        self.name = name
        self.attributes = attributes or {}
        self._cm = None
        self._span = None

    async def __aenter__(self):
        if not (OPENTELEMETRY_AVAILABLE and _TRACING_ENABLED and tracer is not None):
            return self
        try:
            self._cm = tracer.start_as_current_span(self.name)
            self._span = self._cm.__enter__()
            if self._span is not None:
                for key, value in self.attributes.items():
                    try:
                        self._span.set_attribute(str(key), value)
                    except Exception:
                        pass
        except Exception:
            self._cm = None
            self._span = None
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        try:
            if self._span is not None and exc_val is not None:
                try:
                    if hasattr(self._span, "record_exception"):
                        self._span.record_exception(exc_val)
                except Exception:
                    pass
                try:
                    if Status is not None and StatusCode is not None and hasattr(self._span, "set_status"):
                        self._span.set_status(Status(StatusCode.ERROR, str(exc_val)))
                except Exception:
                    pass
        finally:
            if self._cm is not None:
                try:
                    self._cm.__exit__(exc_type, exc_val, exc_tb)
                except Exception:
                    pass
        return False


class FullJitterBackoff:
    """Retry transient 429/408/5xx only."""

    def __init__(self, max_retries: int = 3, base_delay: float = 0.75, max_delay: float = 8.0):
        self.max_retries = max(1, int(max_retries))
        self.base_delay = float(base_delay)
        self.max_delay = float(max_delay)

    async def execute_async(self, func: Callable, *args, **kwargs) -> Any:
        last_exc: Optional[BaseException] = None
        for attempt in range(self.max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception as exc:
                last_exc = exc
                status_code = getattr(exc, "status_code", None)
                if status_code is None:
                    response = getattr(exc, "response", None)
                    status_code = getattr(response, "status_code", None)

                retryable = status_code in {408, 429} or (isinstance(status_code, int) and 500 <= status_code < 600)
                if not retryable or attempt >= self.max_retries - 1:
                    raise
                temp = min(self.max_delay, self.base_delay * (2 ** attempt))
                await asyncio.sleep(temp * __import__("random").random())
        if last_exc:
            raise last_exc


# =============================================================================
# Enums and models
# =============================================================================
class TestSeverity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"


class TestCategory(str, Enum):
    INFRASTRUCTURE = "infrastructure"
    CONTRACT = "contract"
    DATA_VALIDITY = "data_validity"
    AUTH = "auth"
    PERFORMANCE = "performance"


class HTTPMethod(str, Enum):
    GET = "GET"
    POST = "POST"


class TestStatus(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"
    SKIP = "SKIP"
    ERROR = "ERROR"


@dataclass(slots=True)
class TestEndpoint:
    name: str
    path: str
    method: HTTPMethod
    category: TestCategory
    severity: TestSeverity
    expected_status: int = 200
    auth_required: bool = False
    timeout: float = 20.0
    payload: Optional[Dict[str, Any]] = None
    params: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    tags: List[str] = field(default_factory=list)
    validator: Optional[Callable[[int, Any, Dict[str, str]], Tuple[TestStatus, str]]] = None


@dataclass(slots=True)
class TestResult:
    endpoint: str
    name: str
    method: str
    status: TestStatus
    severity: TestSeverity
    category: TestCategory
    response_time: float
    http_status: int = 0
    response_size: int = 0
    message: str = ""
    timestamp: datetime = field(default_factory=utc_now)
    request_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "name": self.name,
            "method": self.method,
            "status": self.status.value,
            "severity": self.severity.value,
            "category": self.category.value,
            "response_time_ms": round(self.response_time * 1000, 2),
            "http_status": self.http_status,
            "timestamp": self.timestamp.isoformat(),
            "message": self.message,
            "response_size": self.response_size,
            "request_id": self.request_id,
        }


@dataclass(slots=True)
class TestSuite:
    name: str
    version: str = SCRIPT_VERSION
    start_time: datetime = field(default_factory=utc_now)
    end_time: Optional[datetime] = None
    results: List[TestResult] = field(default_factory=list)
    environment: str = "unknown"
    target_url: str = ""

    def add_result(self, result: TestResult) -> None:
        self.results.append(result)

    @property
    def duration(self) -> float:
        end = self.end_time or utc_now()
        return (end - self.start_time).total_seconds()

    @property
    def summary(self) -> Dict[str, Any]:
        total = len(self.results)
        by_status: Dict[str, int] = {}
        for item in self.results:
            by_status[item.status.value] = by_status.get(item.status.value, 0) + 1
        times = [r.response_time for r in self.results if r.response_time > 0]
        stats_dict: Dict[str, float] = {}
        if times:
            sorted_times = sorted(times)
            stats_dict = {
                "min": min(sorted_times),
                "max": max(sorted_times),
                "mean": sum(sorted_times) / len(sorted_times),
                "p95": sorted_times[min(len(sorted_times) - 1, int(len(sorted_times) * 0.95))],
            }
        return {
            "total": total,
            "passed": by_status.get("PASS", 0),
            "failed": by_status.get("FAIL", 0),
            "warned": by_status.get("WARN", 0),
            "errors": by_status.get("ERROR", 0),
            "skipped": by_status.get("SKIP", 0),
            "response_times": stats_dict,
            "duration": self.duration,
            "success_rate": ((by_status.get("PASS", 0) + by_status.get("WARN", 0)) / total) if total > 0 else 0.0,
        }

    def to_json(self) -> str:
        return json_dumps(
            {
                "name": self.name,
                "version": self.version,
                "start_time": self.start_time.isoformat(),
                "end_time": self.end_time.isoformat() if self.end_time else None,
                "duration": self.duration,
                "environment": self.environment,
                "target_url": self.target_url,
                "summary": self.summary,
                "results": [r.to_dict() for r in self.results],
            },
            indent=2,
        )


# =============================================================================
# HTTP client
# =============================================================================
class AsyncHTTPClient:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.client: Optional[AsyncClient] = None
        self.backoff = FullJitterBackoff(max_retries=3)

    async def __aenter__(self):
        if not HTTPX_AVAILABLE or AsyncClient is None:
            raise RuntimeError("httpx is required for scripts/test_api_suite.py")
        self.client = AsyncClient(
            base_url=self.base_url,
            timeout=Timeout(30.0),
            limits=Limits(max_keepalive_connections=20, max_connections=100),
            http2=True,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client is not None:
            await self.client.aclose()
            self.client = None
        return False

    async def request(self, method: str, path: str, **kwargs) -> Tuple[int, Any, float, Dict[str, str], str]:
        if self.client is None:
            raise RuntimeError("HTTP client not initialized")

        request_id = str(uuid.uuid4())[:8]
        headers = dict(kwargs.pop("headers", {}) or {})
        headers.setdefault("User-Agent", f"TFB-API-Suite/{SCRIPT_VERSION}")
        headers.setdefault("X-Request-ID", request_id)

        token = str(kwargs.pop("token", "") or "").strip()
        if token:
            headers.setdefault("X-APP-TOKEN", token)
            headers.setdefault("Authorization", f"Bearer {token}")

        start_time = time.perf_counter()

        async def _make_request():
            response = await self.client.request(method, path, headers=headers, **kwargs)
            try:
                data: Any = json_loads(response.content)
            except Exception:
                data = response.text
            return response.status_code, data, dict(response.headers)

        try:
            status_code, data, response_headers = await self.backoff.execute_async(_make_request)
            return status_code, data, time.perf_counter() - start_time, response_headers, request_id
        except httpx.HTTPStatusError as exc:  # pragma: no cover - defensive only
            return exc.response.status_code, exc.response.text, time.perf_counter() - start_time, dict(exc.response.headers), request_id


# =============================================================================
# Validators
# =============================================================================
def _count_safe(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (str, bytes, bytearray)):
        return 1 if value else 0
    try:
        return len(value)  # type: ignore[arg-type]
    except Exception:
        return 1


def _warnings_text(data: Any) -> str:
    if not isinstance(data, dict):
        return ""
    warnings: List[str] = []
    meta = data.get("meta")
    if isinstance(meta, dict):
        meta_warnings = meta.get("warnings")
        if isinstance(meta_warnings, list):
            warnings.extend([str(x) for x in meta_warnings if str(x).strip()])
    raw_warnings = data.get("warnings")
    if isinstance(raw_warnings, list):
        warnings.extend([str(x) for x in raw_warnings if str(x).strip()])
    return " | ".join(warnings)


def _sheet_rows_message(data: Dict[str, Any]) -> str:
    headers = _count_safe(data.get("headers"))
    keys = _count_safe(data.get("keys"))
    rows = max(
        _count_safe(data.get("rows")),
        _count_safe(data.get("row_objects")),
        _count_safe(data.get("data")),
        _count_safe(data.get("items")),
        _count_safe(data.get("quotes")),
    )
    status = str(data.get("status") or "")
    return f"status={status} headers={headers} keys={keys} rows={rows}"


def validate_openapi(status_code: int, data: Any, headers: Dict[str, str]) -> Tuple[TestStatus, str]:
    if status_code != 200:
        return TestStatus.FAIL, f"Expected 200, got {status_code}"
    if not isinstance(data, dict):
        return TestStatus.FAIL, "OpenAPI body is not JSON object"
    paths = data.get("paths")
    if not isinstance(paths, dict) or not paths:
        return TestStatus.FAIL, "OpenAPI has no paths"
    if "/v1/analysis/sheet-rows" not in paths:
        return TestStatus.WARN, "OpenAPI loaded but analysis sheet-rows path missing"
    return TestStatus.PASS, f"paths={len(paths)}"


def validate_basic_200(status_code: int, data: Any, headers: Dict[str, str]) -> Tuple[TestStatus, str]:
    if status_code == 200:
        return TestStatus.PASS, "HTTP 200"
    return TestStatus.FAIL, f"Expected 200, got {status_code}"


def validate_analysis_sheet_rows(status_code: int, data: Any, headers: Dict[str, str]) -> Tuple[TestStatus, str]:
    if status_code != 200:
        return TestStatus.FAIL, f"Expected 200, got {status_code}"
    if not isinstance(data, dict):
        return TestStatus.FAIL, "Body is not JSON object"
    headers_count = _count_safe(data.get("headers"))
    keys_count = _count_safe(data.get("keys"))
    rows_count = max(_count_safe(data.get("rows")), _count_safe(data.get("row_objects")), _count_safe(data.get("data")), _count_safe(data.get("quotes")))
    if headers_count < 50 or keys_count < 50:
        return TestStatus.FAIL, _sheet_rows_message(data)
    if rows_count <= 0:
        return TestStatus.WARN, _sheet_rows_message(data)
    return TestStatus.PASS, _sheet_rows_message(data)


def validate_top10_sheet_rows(status_code: int, data: Any, headers: Dict[str, str]) -> Tuple[TestStatus, str]:
    if status_code != 200:
        return TestStatus.FAIL, f"Expected 200, got {status_code}"
    if not isinstance(data, dict):
        return TestStatus.FAIL, "Body is not JSON object"
    headers_count = _count_safe(data.get("headers"))
    keys_count = _count_safe(data.get("keys"))
    rows_count = max(_count_safe(data.get("rows")), _count_safe(data.get("row_objects")), _count_safe(data.get("data")), _count_safe(data.get("items")))
    if headers_count < 70 or keys_count < 70:
        return TestStatus.FAIL, _sheet_rows_message(data)
    if rows_count <= 0:
        return TestStatus.WARN, _sheet_rows_message(data)
    return TestStatus.PASS, _sheet_rows_message(data)


def validate_insights_partial_ok(status_code: int, data: Any, headers: Dict[str, str]) -> Tuple[TestStatus, str]:
    if status_code != 200:
        return TestStatus.FAIL, f"Expected 200, got {status_code}"
    if not isinstance(data, dict):
        return TestStatus.FAIL, "Body is not JSON object"
    headers_count = _count_safe(data.get("headers"))
    keys_count = _count_safe(data.get("keys"))
    rows_count = max(_count_safe(data.get("rows")), _count_safe(data.get("row_objects")), _count_safe(data.get("data")))
    warnings_text = _warnings_text(data)
    status_text = str(data.get("status") or "")
    if headers_count >= 7 and keys_count >= 7 and rows_count > 0:
        return TestStatus.PASS, f"status={status_text} headers={headers_count} keys={keys_count} rows={rows_count}"
    if headers_count >= 7 and keys_count >= 7 and "timeout" in warnings_text.lower():
        return TestStatus.WARN, f"status={status_text} warnings={warnings_text}"
    if headers_count >= 7 and keys_count >= 7:
        return TestStatus.WARN, f"status={status_text} headers={headers_count} keys={keys_count} rows={rows_count}"
    return TestStatus.FAIL, _sheet_rows_message(data)


def validate_schema_contract(status_code: int, data: Any, headers: Dict[str, str]) -> Tuple[TestStatus, str]:
    if status_code != 200:
        return TestStatus.FAIL, f"Expected 200, got {status_code}"
    if not isinstance(data, dict):
        return TestStatus.FAIL, "Body is not JSON object"
    headers_count = _count_safe(data.get("headers"))
    keys_count = _count_safe(data.get("keys"))
    if max(headers_count, keys_count) < 7:
        return TestStatus.FAIL, f"headers={headers_count} keys={keys_count}"
    return TestStatus.PASS, f"headers={headers_count} keys={keys_count}"


# =============================================================================
# Test runner
# =============================================================================
class TestRunner:
    def __init__(self, base_url: str, token: str = "", strict: bool = False):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.strict = strict
        self.suite = TestSuite(name=f"TFB API Test Suite - {self.base_url}")
        self.client: Optional[AsyncHTTPClient] = None

    def get_endpoints(self) -> List[TestEndpoint]:
        return [
            TestEndpoint("Root", "/", HTTPMethod.GET, TestCategory.INFRASTRUCTURE, TestSeverity.CRITICAL, auth_required=False, tags=["smoke"], validator=validate_basic_200),
            TestEndpoint("Health", "/health", HTTPMethod.GET, TestCategory.INFRASTRUCTURE, TestSeverity.CRITICAL, auth_required=False, tags=["smoke"], validator=validate_basic_200),
            TestEndpoint("Meta", "/meta", HTTPMethod.GET, TestCategory.INFRASTRUCTURE, TestSeverity.HIGH, auth_required=False, tags=["smoke"], validator=validate_basic_200),
            TestEndpoint("Readyz", "/readyz", HTTPMethod.GET, TestCategory.INFRASTRUCTURE, TestSeverity.HIGH, auth_required=False, tags=["smoke"], validator=validate_basic_200),
            TestEndpoint("Livez", "/livez", HTTPMethod.GET, TestCategory.INFRASTRUCTURE, TestSeverity.HIGH, auth_required=False, tags=["smoke"], validator=validate_basic_200),
            TestEndpoint("OpenAPI", "/openapi.json", HTTPMethod.GET, TestCategory.CONTRACT, TestSeverity.HIGH, auth_required=False, tags=["contract", "smoke"], validator=validate_openapi),
            TestEndpoint(
                "Analysis GET Market Leaders",
                "/v1/analysis/sheet-rows",
                HTTPMethod.GET,
                TestCategory.DATA_VALIDITY,
                TestSeverity.CRITICAL,
                auth_required=True,
                params={"page": "Market_Leaders", "limit": 5, "symbols": "2222.SR,1120.SR"},
                tags=["analysis", "market_leaders", "live"],
                validator=validate_analysis_sheet_rows,
            ),
            TestEndpoint(
                "Analysis POST Global Markets",
                "/v1/analysis/sheet-rows",
                HTTPMethod.POST,
                TestCategory.DATA_VALIDITY,
                TestSeverity.CRITICAL,
                auth_required=True,
                payload={"page": "Global_Markets", "limit": 5, "symbols": ["AAPL", "MSFT", "NVDA"], "tickers": ["AAPL", "MSFT", "NVDA"], "direct_symbols": ["AAPL", "MSFT", "NVDA"], "mode": "live"},
                tags=["analysis", "global", "live"],
                validator=validate_analysis_sheet_rows,
            ),
            TestEndpoint(
                "Advanced POST Insights",
                "/v1/advanced/sheet-rows",
                HTTPMethod.POST,
                TestCategory.DATA_VALIDITY,
                TestSeverity.HIGH,
                auth_required=True,
                payload={"page": "Insights_Analysis", "limit": 5, "symbols": ["2222.SR", "AAPL", "GC=F"], "mode": "live"},
                tags=["advanced", "insights", "live"],
                validator=validate_insights_partial_ok,
            ),
            TestEndpoint(
                "Advanced POST Top10",
                "/v1/advanced/sheet-rows",
                HTTPMethod.POST,
                TestCategory.DATA_VALIDITY,
                TestSeverity.HIGH,
                auth_required=True,
                payload={"page": "Top_10_Investments", "limit": 5, "symbols": ["2222.SR", "AAPL", "NVDA"], "mode": "live"},
                tags=["advanced", "top10", "live"],
                validator=validate_top10_sheet_rows,
            ),
            TestEndpoint(
                "Advisor POST Portfolio",
                "/v1/advisor/sheet-rows",
                HTTPMethod.POST,
                TestCategory.DATA_VALIDITY,
                TestSeverity.HIGH,
                auth_required=True,
                payload={"page": "My_Portfolio", "limit": 5, "symbols": ["2222.SR", "AAPL"], "mode": "live"},
                tags=["advisor", "portfolio", "live"],
                validator=validate_analysis_sheet_rows,
            ),
            TestEndpoint(
                "Root Sheet Rows Top10",
                "/sheet-rows",
                HTTPMethod.POST,
                TestCategory.DATA_VALIDITY,
                TestSeverity.HIGH,
                auth_required=True,
                payload={"page": "Top_10_Investments", "limit": 5, "symbols": ["2222.SR", "AAPL", "NVDA"], "mode": "live"},
                tags=["root", "top10", "live"],
                validator=validate_top10_sheet_rows,
            ),
            TestEndpoint(
                "Schema Data Dictionary",
                "/v1/schema/sheet-spec",
                HTTPMethod.GET,
                TestCategory.CONTRACT,
                TestSeverity.HIGH,
                auth_required=True,
                params={"page": "Data_Dictionary"},
                tags=["schema", "dictionary", "contract"],
                validator=validate_schema_contract,
            ),
        ]

    async def run_test(self, endpoint: TestEndpoint) -> TestResult:
        if self.client is None:
            raise RuntimeError("HTTP client not initialized")

        start_time = time.perf_counter()
        try:
            status_code, data, response_time, resp_headers, request_id = await self.client.request(
                endpoint.method.value,
                endpoint.path,
                params=endpoint.params,
                json=endpoint.payload,
                headers=endpoint.headers,
                token=self.token if endpoint.auth_required else "",
            )

            if endpoint.validator is not None:
                status, message = endpoint.validator(status_code, data, resp_headers)
            else:
                status = TestStatus.PASS if status_code == endpoint.expected_status else TestStatus.FAIL
                message = "OK" if status == TestStatus.PASS else f"Expected {endpoint.expected_status}, got {status_code}"

            if self.strict and status == TestStatus.WARN:
                status = TestStatus.FAIL
                message = f"STRICT: {message}"

            return TestResult(
                endpoint=endpoint.path,
                name=endpoint.name,
                method=endpoint.method.value,
                status=status,
                severity=endpoint.severity,
                category=endpoint.category,
                response_time=response_time,
                http_status=status_code,
                response_size=len(str(data)) if data is not None else 0,
                message=message,
                request_id=request_id,
            )
        except Exception as exc:
            return TestResult(
                endpoint=endpoint.path,
                name=endpoint.name,
                method=endpoint.method.value,
                status=TestStatus.ERROR,
                severity=endpoint.severity,
                category=endpoint.category,
                response_time=time.perf_counter() - start_time,
                http_status=0,
                response_size=0,
                message=f"{type(exc).__name__}: {exc}",
                request_id=None,
            )

    async def run_all(self, concurrency: int = 5, tags: Optional[List[str]] = None) -> TestSuite:
        all_endpoints = self.get_endpoints()
        if tags:
            endpoints = [e for e in all_endpoints if any(tag in e.tags for tag in tags)]
        else:
            endpoints = all_endpoints

        sys.stdout.write(f"\nRunning API suite against {self.base_url}\n")
        sys.stdout.write(f"Concurrency: {concurrency} | Endpoints: {len(endpoints)}\n\n")
        logger.info("Running %s tests against %s with concurrency=%s", len(endpoints), self.base_url, concurrency)

        async with AsyncHTTPClient(self.base_url) as client:
            self.client = client
            semaphore = asyncio.Semaphore(max(1, int(concurrency)))

            async def run_with_semaphore(endpoint: TestEndpoint) -> TestResult:
                async with semaphore:
                    async with TraceContext("execute_test", {"endpoint": endpoint.path, "method": endpoint.method.value}):
                        return await self.run_test(endpoint)

            results = await asyncio.gather(*[run_with_semaphore(e) for e in endpoints], return_exceptions=False)
            for result in results:
                self.suite.add_result(result)

        self.suite.end_time = utc_now()
        return self.suite

    def print_summary(self) -> None:
        summary = self.suite.summary
        sys.stdout.write("\n" + "=" * 120 + "\n")
        sys.stdout.write("TFB API TEST SUMMARY\n")
        sys.stdout.write("=" * 120 + "\n")
        sys.stdout.write(f"{'NAME':<28} {'ENDPOINT':<34} {'METHOD':<8} {'STATUS':<8} {'HTTP':<6} {'TIME':<10} DETAILS\n")
        sys.stdout.write("-" * 120 + "\n")

        for result in self.suite.results:
            sys.stdout.write(
                f"{result.name[:27]:<28} {result.endpoint[:33]:<34} {result.method:<8} "
                f"{result.status.value:<8} {result.http_status:<6} {result.response_time*1000:>7.1f}ms  {result.message[:80]}\n"
            )

        sys.stdout.write("=" * 120 + "\n")
        sys.stdout.write(
            f"Total={summary['total']} Passed={summary['passed']} Warned={summary['warned']} Failed={summary['failed']} Errors={summary['errors']}\n"
        )
        sys.stdout.write(
            f"Success Rate={summary['success_rate']*100:.1f}% Duration={summary['duration']:.2f}s "
            f"Avg={summary.get('response_times', {}).get('mean', 0)*1000:.1f}ms "
            f"P95={summary.get('response_times', {}).get('p95', 0)*1000:.1f}ms\n\n"
        )


# =============================================================================
# Report generation
# =============================================================================
class ReportGenerator:
    def __init__(self, suite: TestSuite):
        self.suite = suite

    def to_json(self, filepath: str) -> None:
        with open(filepath, "w", encoding="utf-8") as handle:
            handle.write(self.suite.to_json())
        sys.stdout.write(f"Saved JSON report to {filepath}\n")

    def to_csv(self, filepath: str) -> None:
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=list(self.suite.results[0].to_dict().keys()) if self.suite.results else ["endpoint"])
        writer.writeheader()
        for result in self.suite.results:
            writer.writerow(result.to_dict())
        with open(filepath, "w", encoding="utf-8", newline="") as handle:
            handle.write(output.getvalue())
        sys.stdout.write(f"Saved CSV report to {filepath}\n")

    def to_html(self, filepath: str) -> None:
        summary = self.suite.summary
        rows_html = []
        for result in self.suite.results:
            rows_html.append(
                "<tr>"
                f"<td>{html_escape(result.name)}</td>"
                f"<td>{html_escape(result.endpoint)}</td>"
                f"<td>{html_escape(result.method)}</td>"
                f"<td class='{result.status.value}'>{html_escape(result.status.value)}</td>"
                f"<td>{result.http_status}</td>"
                f"<td>{result.response_time*1000:.1f}ms</td>"
                f"<td>{html_escape(result.message)}</td>"
                "</tr>"
            )

        html = f"""
<html>
<head>
  <title>TFB API Test Report</title>
  <style>
    body {{ font-family: Arial, sans-serif; padding: 32px; background: #f5f7fb; }}
    .container {{ background: white; padding: 24px; border-radius: 10px; box-shadow: 0 4px 18px rgba(0,0,0,.08); }}
    h1 {{ margin-top: 0; }}
    .stats {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin-bottom: 24px; }}
    .box {{ background: #eef3f8; padding: 12px; border-radius: 8px; text-align: center; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ border-bottom: 1px solid #e5e7eb; padding: 10px; text-align: left; font-size: 14px; }}
    th {{ background: #22324a; color: white; }}
    .PASS {{ color: #15803d; font-weight: 700; }}
    .WARN {{ color: #b45309; font-weight: 700; }}
    .FAIL, .ERROR {{ color: #b91c1c; font-weight: 700; }}
  </style>
</head>
<body>
  <div class="container">
    <h1>TFB API Test Report v{SCRIPT_VERSION}</h1>
    <div class="stats">
      <div class="box"><div>Total</div><strong>{summary['total']}</strong></div>
      <div class="box"><div>Passed</div><strong>{summary['passed']}</strong></div>
      <div class="box"><div>Warned</div><strong>{summary['warned']}</strong></div>
      <div class="box"><div>Failed</div><strong>{summary['failed'] + summary['errors']}</strong></div>
      <div class="box"><div>P95</div><strong>{summary.get('response_times', {}).get('p95', 0)*1000:.1f}ms</strong></div>
    </div>
    <table>
      <tr><th>Name</th><th>Endpoint</th><th>Method</th><th>Status</th><th>HTTP</th><th>Time</th><th>Details</th></tr>
      {''.join(rows_html)}
    </table>
  </div>
</body>
</html>
"""
        with open(filepath, "w", encoding="utf-8") as handle:
            handle.write(html)
        sys.stdout.write(f"Saved HTML report to {filepath}\n")


# =============================================================================
# CLI
# =============================================================================
def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=f"TFB API Test Suite v{SCRIPT_VERSION}")
    parser.add_argument("--url", default=os.getenv("TFB_BASE_URL", "http://localhost:8000"), help="Base URL to test")
    parser.add_argument("--token", default=os.getenv("TFB_TOKEN", os.getenv("APP_TOKEN", "")), help="Authentication token")
    parser.add_argument("--strict", action="store_true", help="Treat WARN as FAIL")
    parser.add_argument("--concurrency", type=int, default=6, help="Concurrent request count")
    parser.add_argument("--smoke", action="store_true", help="Run smoke-tagged tests only")
    parser.add_argument("--tags", nargs="*", help="Run tests matching these tags")
    parser.add_argument("--report-json", help="Write JSON report to path")
    parser.add_argument("--report-csv", help="Write CSV report to path")
    parser.add_argument("--report-html", help="Write HTML report to path")
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    return parser


async def async_main() -> int:
    parser = create_parser()
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    runner = TestRunner(args.url, args.token, strict=args.strict)
    runner.suite.environment = os.getenv("APP_ENV", "production")
    runner.suite.target_url = args.url

    try:
        selected_tags: Optional[List[str]] = None
        if args.smoke:
            selected_tags = ["smoke"]
        elif args.tags:
            selected_tags = list(args.tags)

        await runner.run_all(concurrency=args.concurrency, tags=selected_tags)
        runner.print_summary()

        if runner.suite.results:
            reporter = ReportGenerator(runner.suite)
            if args.report_json:
                reporter.to_json(args.report_json)
            if args.report_csv:
                reporter.to_csv(args.report_csv)
            if args.report_html:
                reporter.to_html(args.report_html)

        summary = runner.suite.summary
        if summary["failed"] > 0 or summary["errors"] > 0:
            return 1
        if summary["warned"] > 0 and args.strict:
            return 3
        return 0
    finally:
        _CPU_EXECUTOR.shutdown(wait=False)


def main() -> int:
    if sys.platform == "win32":  # pragma: no cover
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    return asyncio.run(async_main())


if __name__ == "__main__":
    sys.exit(main())
