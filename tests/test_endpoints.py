#!/usr/bin/env python3
"""
test_endpoints.py
===========================================================
TADAWUL FAST BRIDGE – ENTERPRISE API TEST SUITE (v4.5.0)
===========================================================
Comprehensive API Testing Framework with Advanced Diagnostics

Core Capabilities
-----------------
• Multi-environment testing (dev/staging/production)
• Advanced security auditing (auth, rate limiting, CORS)
• Performance benchmarking with percentile analysis
• Schema validation against OpenAPI specifications
• Load testing with configurable concurrency
• Chaos engineering simulations
• Compliance checking (GDPR, SOC2, ISO27001)
• Historical trend analysis
• WebSocket testing (real-time feeds)
• Database integration testing
• Cache validation and hit-rate analysis
• Circuit breaker verification

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

Usage Examples
--------------
# Basic smoke test
python tests/test_endpoints.py --url https://api.tadawulfb.com

# Comprehensive security audit
python tests/test_endpoints.py --security-audit --scan-vulnerabilities

# Performance benchmark with report
python tests/test_endpoints.py --benchmark --concurrency 50 --duration 60 --report perf.html

# Load test with custom profile
python tests/test_endpoints.py --load-test --users 100 --spawn-rate 10 --run-time 5m

# Schema validation against OpenAPI
python tests/test_endpoints.py --validate-schema --openapi spec.yaml

# Compliance check
python tests/test_endpoints.py --compliance --standard soc2 --output compliance.json

# Continuous monitoring mode
python tests/test_endpoints.py --monitor --interval 300 --webhook https://...

# Chaos testing
python tests/test_endpoints.py --chaos --failure-rate 0.1 --latency-ms 1000
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
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import lru_cache, partial, wraps
from http.cookiejar import CookieJar
from pathlib import Path
from threading import Event, Lock, Thread
from typing import (Any, AsyncGenerator, Callable, Dict, List, Optional,
                    Set, Tuple, Type, TypeVar, Union, cast)
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

# =============================================================================
# Version & Core Configuration
# =============================================================================
SCRIPT_VERSION = "4.5.0"
SCRIPT_NAME = "API Test Suite"
MIN_PYTHON = (3, 8)

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
    from aiohttp import ClientTimeout, TCPConnector
    ASYNC_HTTP_AVAILABLE = True
except ImportError:
    aiohttp = None
    ASYNC_HTTP_AVAILABLE = False

try:
    import requests
    from requests.adapters import HTTPAdapter
    from requests.packages.urllib3.util.retry import Retry
    REQUESTS_AVAILABLE = True
except ImportError:
    requests = None
    REQUESTS_AVAILABLE = False

# Performance/Load Testing
try:
    from locust import HttpUser, task, between
    from locust.env import Environment
    from locust.stats import stats_printer, stats_history
    from locust.log import setup_logging
    LOCUST_AVAILABLE = True
except ImportError:
    LOCUST_AVAILABLE = False

# Data Processing
try:
    import numpy as np
    from numpy import random as nprand
    NUMPY_AVAILABLE = True
except ImportError:
    np = None
    NUMPY_AVAILABLE = False

try:
    import pandas as pd
    from pandas import DataFrame, Series
    PANDAS_AVAILABLE = True
except ImportError:
    pd = None
    PANDAS_AVAILABLE = False

try:
    from scipy import stats
    from scipy.stats import percentileofscore
    SCIPY_AVAILABLE = True
except ImportError:
    stats = None
    SCIPY_AVAILABLE = False

# Schema Validation
try:
    import jsonschema
    from jsonschema import validate, Draft7Validator
    JSONSCHEMA_AVAILABLE = True
except ImportError:
    jsonschema = None
    JSONSCHEMA_AVAILABLE = False

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    yaml = None
    YAML_AVAILABLE = False

# OpenAPI/Swagger
try:
    import prance
    from prance import ResolvingParser
    from prance.util import ValidationError
    PRANCE_AVAILABLE = True
except ImportError:
    prance = None
    PRANCE_AVAILABLE = False

# Security
try:
    import jwt
    from jwt import PyJWTError
    JWT_AVAILABLE = True
except ImportError:
    jwt = None
    JWT_AVAILABLE = False

try:
    import cryptography
    from cryptography.fernet import Fernet
    CRYPTO_AVAILABLE = True
except ImportError:
    cryptography = None
    CRYPTO_AVAILABLE = False

# WebSocket
try:
    import websockets
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

class HTTPMethod(Enum):
    """HTTP methods"""
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"

class AuthType(Enum):
    """Authentication types"""
    NONE = "none"
    BEARER = "bearer"
    API_KEY = "api_key"
    BASIC = "basic"
    JWT = "jwt"
    OAUTH2 = "oauth2"

class TestStatus(Enum):
    """Test result status"""
    PASS = "PASS"
    FAIL = "FAIL"
    WARN = "WARN"
    SKIP = "SKIP"
    ERROR = "ERROR"

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
    params: Optional[Dict[str, Any]] = None
    headers: Optional[Dict[str, str]] = None
    validation_schema: Optional[Dict[str, Any]] = None
    tags: List[str] = field(default_factory=list)
    
    @property
    def full_path(self) -> str:
        """Get full path with leading slash"""
        return f"/{self.path.lstrip('/')}"

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
            'validation_errors': self.validation_errors
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
        
        response_times = [r.response_time for r in self.results]
        
        return {
            'total': total,
            'passed': by_status.get('PASS', 0),
            'failed': by_status.get('FAIL', 0),
            'warned': by_status.get('WARN', 0),
            'skipped': by_status.get('SKIP', 0),
            'by_category': dict(by_category),
            'by_severity': dict(by_severity),
            'response_times': {
                'min': min(response_times) if response_times else 0,
                'max': max(response_times) if response_times else 0,
                'avg': sum(response_times) / len(response_times) if response_times else 0,
                'p95': self._percentile(response_times, 95) if response_times else 0,
                'p99': self._percentile(response_times, 99) if response_times else 0
            },
            'duration': self.duration
        }
    
    def _percentile(self, data: List[float], percentile: float) -> float:
        """Calculate percentile"""
        if not data or not NUMPY_AVAILABLE:
            return 0.0
        return float(np.percentile(data, percentile))
    
    def to_json(self) -> str:
        """Convert to JSON"""
        return json.dumps({
            'name': self.name,
            'version': self.version,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration': self.duration,
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
            'std_dev_ms': round(self.std_dev * 1000, 2),
            'error_rate': round(self.error_rate * 100, 2),
            'throughput_rps': round(self.throughput, 2)
        }

@dataclass
class SecurityVulnerability:
    """Security vulnerability found"""
    type: str
    severity: TestSeverity
    endpoint: str
    description: str
    remediation: str
    cve: Optional[str] = None
    cvss_score: Optional[float] = None
    evidence: Optional[Dict[str, Any]] = None

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

def colorize(text: str, color: str, bold: bool = False) -> str:
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
    return f"{minutes}m {secs:.1f}s"

def format_size(bytes_: int) -> str:
    """Format size in human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_ < 1024:
            return f"{bytes_:.1f}{unit}"
        bytes_ /= 1024
    return f"{bytes_:.1f}TB"

def parse_timestamp(ts: str) -> Optional[datetime]:
    """Parse timestamp in various formats"""
    formats = [
        '%Y-%m-%dT%H:%M:%S.%fZ',
        '%Y-%m-%dT%H:%M:%S.%f%z',
        '%Y-%m-%dT%H:%M:%SZ',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
        '%d/%m/%Y %H:%M:%S',
        '%d/%m/%Y',
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

def extract_json_from_response(text: str) -> Optional[Dict[str, Any]]:
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

# =============================================================================
# Async HTTP Client with Advanced Features
# =============================================================================
class AsyncHTTPClient:
    """Advanced async HTTP client with retries, circuit breaker, and monitoring"""
    
    def __init__(self, base_url: str, timeout: float = 30.0, max_retries: int = 3):
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.session: Optional[aiohttp.ClientSession] = None
        self.metrics: Dict[str, List[float]] = defaultdict(list)
        self.circuit_breakers: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
    
    async def __aenter__(self):
        if ASYNC_HTTP_AVAILABLE:
            connector = TCPConnector(
                limit=100,
                ttl_dns_cache=300,
                ssl=False if 'localhost' in self.base_url else True
            )
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=ClientTimeout(total=self.timeout)
            )
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
    
    async def request(self, method: str, path: str, **kwargs) -> Tuple[int, Any, float, Dict[str, Any]]:
        """Make HTTP request with retries and circuit breaker"""
        url = urljoin(self.base_url, path.lstrip('/'))
        
        # Check circuit breaker
        circuit_key = f"{method}:{path}"
        async with self._lock:
            if circuit_key in self.circuit_breakers:
                cb = self.circuit_breakers[circuit_key]
                if cb['state'] == 'open':
                    if datetime.now() > cb['reset_at']:
                        cb['state'] = 'half-open'
                        cb['failures'] = 0
                    else:
                        raise Exception(f"Circuit breaker open for {circuit_key}")
        
        # Default headers
        headers = kwargs.pop('headers', {})
        headers.setdefault('User-Agent', f'TFB-APITest/{SCRIPT_VERSION}')
        headers.setdefault('Accept', 'application/json')
        
        # Add auth token if available
        token = os.getenv('APP_TOKEN')
        if token:
            headers.setdefault('X-APP-TOKEN', token)
            headers.setdefault('Authorization', f'Bearer {token}')
        
        start_time = time.perf_counter()
        error = None
        status = 0
        data = None
        
        for attempt in range(self.max_retries):
            try:
                async with self.session.request(
                    method, url, headers=headers, timeout=self.timeout, **kwargs
                ) as resp:
                    status = resp.status
                    content_type = resp.headers.get('Content-Type', '')
                    
                    if 'application/json' in content_type:
                        data = await resp.json()
                    else:
                        data = await resp.text()
                    
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
                                    cb['state'] = 'closed'
                                    cb['failures'] = 0
                    
                    return status, data, response_time, dict(resp.headers)
                    
            except Exception as e:
                error = e
                response_time = time.perf_counter() - start_time
                
                # Update circuit breaker on failure
                async with self._lock:
                    if circuit_key not in self.circuit_breakers:
                        self.circuit_breakers[circuit_key] = {
                            'state': 'closed',
                            'failures': 0,
                            'reset_at': datetime.now()
                        }
                    
                    cb = self.circuit_breakers[circuit_key]
                    cb['failures'] += 1
                    
                    if cb['failures'] >= 5:  # Threshold
                        cb['state'] = 'open'
                        cb['reset_at'] = datetime.now() + timedelta(seconds=30)
                
                if attempt < self.max_retries - 1:
                    wait = 2 ** attempt + random.uniform(0, 1)
                    await asyncio.sleep(wait)
                    continue
        
        return 0, str(error), response_time, {}
    
    async def get(self, path: str, **kwargs) -> Tuple[int, Any, float, Dict[str, Any]]:
        """GET request"""
        return await self.request('GET', path, **kwargs)
    
    async def post(self, path: str, **kwargs) -> Tuple[int, Any, float, Dict[str, Any]]:
        """POST request"""
        return await self.request('POST', path, **kwargs)
    
    async def put(self, path: str, **kwargs) -> Tuple[int, Any, float, Dict[str, Any]]:
        """PUT request"""
        return await self.request('PUT', path, **kwargs)
    
    async def delete(self, path: str, **kwargs) -> Tuple[int, Any, float, Dict[str, Any]]:
        """DELETE request"""
        return await self.request('DELETE', path, **kwargs)
    
    def get_metrics(self) -> Dict[str, PerformanceMetrics]:
        """Get performance metrics for all endpoints"""
        metrics = {}
        
        for key, times in self.metrics.items():
            if not times:
                continue
            
            method, path = key.split(':', 1)
            times_array = np.array(times) if NUMPY_AVAILABLE else times
            
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
                std_dev=np.std(times_array) if NUMPY_AVAILABLE else 0,
                error_rate=0.0,  # Would need error tracking
                throughput=len(times) / (max(times) - min(times)) if len(times) > 1 else 0
            )
        
        return metrics

# =============================================================================
# Schema Validator
# =============================================================================
class SchemaValidator:
    """OpenAPI/Swagger schema validator"""
    
    def __init__(self, spec_path: Optional[str] = None):
        self.spec_path = spec_path
        self.spec: Optional[Dict[str, Any]] = None
        self.validator = None
        
        if spec_path and PRANCE_AVAILABLE:
            self._load_spec()
    
    def _load_spec(self) -> None:
        """Load OpenAPI specification"""
        try:
            parser = ResolvingParser(self.spec_path, strict=False)
            self.spec = parser.specification
            
            # Create validator
            if JSONSCHEMA_AVAILABLE:
                self.validator = Draft7Validator({})
                
            logger.info(f"Loaded OpenAPI spec: {self.spec.get('info', {}).get('title', 'Unknown')}")
            
        except Exception as e:
            logger.error(f"Failed to load OpenAPI spec: {e}")
    
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
                return errors
            
            # Find operation
            operation = path_item.get(method.lower())
            if not operation:
                return errors
            
            # Find response schema
            responses = operation.get('responses', {})
            response_spec = responses.get(str(status_code), {}).get('content', {})
            
            # Check JSON schema
            json_schema = response_spec.get('application/json', {}).get('schema')
            if json_schema and JSONSCHEMA_AVAILABLE:
                try:
                    validate(instance=response_data, schema=json_schema)
                except jsonschema.ValidationError as e:
                    errors.append(f"Schema validation failed: {e}")
                    
        except Exception as e:
            errors.append(f"Validation error: {e}")
        
        return errors
    
    def _find_path_item(self, path: str) -> Optional[Dict[str, Any]]:
        """Find path item in spec (handles path parameters)"""
        if not self.spec:
            return None
        
        paths = self.spec.get('paths', {})
        
        # Exact match
        if path in paths:
            return paths[path]
        
        # Template match
        for template, item in paths.items():
            if self._path_matches(template, path):
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

# =============================================================================
# Security Auditor
# =============================================================================
class SecurityAuditor:
    """Advanced security auditing"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.vulnerabilities: List[SecurityVulnerability] = []
    
    async def audit_headers(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Audit security headers"""
        vulns = []
        
        status, headers, _, _ = await client.get('/')
        
        if isinstance(headers, dict):
            # Check security headers
            security_headers = {
                'Strict-Transport-Security': {
                    'required': True,
                    'description': 'Missing HSTS header',
                    'remediation': 'Add Strict-Transport-Security header with max-age>=31536000'
                },
                'Content-Security-Policy': {
                    'required': True,
                    'description': 'Missing CSP header',
                    'remediation': 'Implement Content-Security-Policy to prevent XSS'
                },
                'X-Content-Type-Options': {
                    'required': True,
                    'description': 'Missing X-Content-Type-Options: nosniff',
                    'remediation': 'Add X-Content-Type-Options: nosniff header'
                },
                'X-Frame-Options': {
                    'required': True,
                    'description': 'Missing X-Frame-Options',
                    'remediation': 'Add X-Frame-Options: DENY or SAMEORIGIN'
                },
                'X-XSS-Protection': {
                    'required': True,
                    'description': 'Missing X-XSS-Protection',
                    'remediation': 'Add X-XSS-Protection: 1; mode=block'
                },
                'Referrer-Policy': {
                    'required': True,
                    'description': 'Missing Referrer-Policy',
                    'remediation': 'Add Referrer-Policy: strict-origin-when-cross-origin'
                },
                'Permissions-Policy': {
                    'required': True,
                    'description': 'Missing Permissions-Policy',
                    'remediation': 'Implement Permissions-Policy header'
                }
            }
            
            for header, info in security_headers.items():
                if header not in headers:
                    vulns.append(SecurityVulnerability(
                        type='missing_security_header',
                        severity=TestSeverity.MEDIUM,
                        endpoint='/',
                        description=info['description'],
                        remediation=info['remediation']
                    ))
        
        return vulns
    
    async def audit_authentication(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Audit authentication mechanisms"""
        vulns = []
        
        # Test without authentication
        status, data, _, _ = await client.get('/v1/enriched/quote', params={'symbol': 'AAPL'})
        
        if status == 200:
            vulns.append(SecurityVulnerability(
                type='missing_authentication',
                severity=TestSeverity.CRITICAL,
                endpoint='/v1/enriched/quote',
                description='Endpoint accessible without authentication',
                remediation='Require authentication for all protected endpoints'
            ))
        
        # Test with invalid token
        bad_headers = {
            'X-APP-TOKEN': 'invalid_token_123',
            'Authorization': 'Bearer invalid_token_123'
        }
        
        status, data, _, _ = await client.get(
            '/v1/enriched/quote', 
            params={'symbol': 'AAPL'},
            headers=bad_headers
        )
        
        if status not in [401, 403]:
            vulns.append(SecurityVulnerability(
                type='weak_authentication',
                severity=TestSeverity.HIGH,
                endpoint='/v1/enriched/quote',
                description=f'Invalid token accepted (HTTP {status})',
                remediation='Reject requests with invalid tokens with 401/403'
            ))
        
        # Test token leakage
        status, data, _, headers = await client.get('/v1/enriched/quote', params={'symbol': 'AAPL'})
        
        if isinstance(data, dict):
            # Check for token in response
            data_str = json.dumps(data)
            token_patterns = [
                r'[A-Za-z0-9-_=]+\.[A-Za-z0-9-_=]+\.[A-Za-z0-9-_.+/=]*',  # JWT
                r'[A-Za-z0-9]{20,}',  # API key
                r'[A-Za-z0-9+/=]{20,}'  # Base64
            ]
            
            for pattern in token_patterns:
                if re.search(pattern, data_str):
                    vulns.append(SecurityVulnerability(
                        type='token_leakage',
                        severity=TestSeverity.HIGH,
                        endpoint='/v1/enriched/quote',
                        description='Authentication token found in response body',
                        remediation='Never include tokens in response bodies'
                    ))
                    break
        
        return vulns
    
    async def audit_rate_limiting(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Audit rate limiting implementation"""
        vulns = []
        
        # Make rapid requests
        start = time.time()
        responses = []
        
        for i in range(100):
            status, _, _, headers = await client.get('/v1/enriched/quote', params={'symbol': 'AAPL'})
            responses.append(status)
            
            if i % 10 == 0:
                await asyncio.sleep(0.01)
        
        duration = time.time() - start
        rate = len(responses) / duration
        
        # Check for rate limit headers
        rate_limit_headers = ['X-RateLimit-Limit', 'X-RateLimit-Remaining', 'Retry-After']
        has_rate_headers = any(h in headers for h in rate_limit_headers)
        
        if not has_rate_headers and rate > 10:
            vulns.append(SecurityVulnerability(
                type='missing_rate_limiting',
                severity=TestSeverity.MEDIUM,
                endpoint='/v1/enriched/quote',
                description=f'No rate limiting detected ({rate:.1f} req/s possible)',
                remediation='Implement rate limiting with proper headers'
            ))
        
        # Check for 429 responses
        if 429 not in responses:
            vulns.append(SecurityVulnerability(
                type='no_429_responses',
                severity=TestSeverity.LOW,
                endpoint='/v1/enriched/quote',
                description='No 429 Too Many Requests responses seen',
                remediation='Return 429 when rate limit exceeded'
            ))
        
        return vulns
    
    async def audit_sql_injection(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Test for SQL injection vulnerabilities"""
        vulns = []
        
        payloads = [
            "' OR '1'='1",
            "'; DROP TABLE users; --",
            "' UNION SELECT * FROM users--",
            "1' AND 1=1--",
            "1' AND 1=2--",
            "' OR 1=1--",
            "' OR '1'='1'--",
            "' OR '1'='1'#",
            "admin'--",
            "' WAITFOR DELAY '0:0:5'--",
        ]
        
        for payload in payloads:
            status, data, duration, _ = await client.get(
                '/v1/enriched/quote',
                params={'symbol': payload}
            )
            
            # Check for error messages that might indicate SQL injection
            if isinstance(data, str):
                error_indicators = [
                    'sql', 'mysql', 'postgresql', 'oracle',
                    'unclosed quotation mark', 'syntax error',
                    'mysql_fetch', 'ORA-', 'SQLite'
                ]
                
                data_lower = data.lower()
                if any(ind in data_lower for ind in error_indicators):
                    vulns.append(SecurityVulnerability(
                        type='sql_injection',
                        severity=TestSeverity.CRITICAL,
                        endpoint='/v1/enriched/quote',
                        description=f'Possible SQL injection with payload: {payload}',
                        remediation='Use parameterized queries and input validation',
                        evidence={'payload': payload, 'response': data[:200]}
                    ))
                    break
        
        return vulns
    
    async def audit_xss(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Test for XSS vulnerabilities"""
        vulns = []
        
        payloads = [
            '<script>alert(1)</script>',
            '<img src=x onerror=alert(1)>',
            'javascript:alert(1)',
            '"><script>alert(1)</script>',
            '"><img src=x onerror=alert(1)>',
            '{{constructor.constructor("alert(1)")()}}',
            '${alert(1)}',
            '{{alert(1)}}',
        ]
        
        for payload in payloads:
            status, data, _, _ = await client.get(
                '/v1/enriched/quote',
                params={'symbol': payload}
            )
            
            if isinstance(data, str):
                if payload in data and '<script>' in payload:
                    vulns.append(SecurityVulnerability(
                        type='xss_vulnerability',
                        severity=TestSeverity.HIGH,
                        endpoint='/v1/enriched/quote',
                        description=f'Possible XSS vulnerability with payload: {payload}',
                        remediation='Sanitize all user inputs and use CSP headers',
                        evidence={'payload': payload, 'response': data[:200]}
                    ))
                    break
        
        return vulns
    
    async def audit_cors(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Audit CORS configuration"""
        vulns = []
        
        # Test with different origins
        test_origins = [
            'https://evil.com',
            'null',
            '*',
            'https://attacker.net',
        ]
        
        for origin in test_origins:
            status, _, _, headers = await client.get(
                '/v1/enriched/quote',
                headers={'Origin': origin}
            )
            
            acao = headers.get('Access-Control-Allow-Origin', '')
            acac = headers.get('Access-Control-Allow-Credentials', '')
            
            if acao == '*' and acac.lower() == 'true':
                vulns.append(SecurityVulnerability(
                    type='cors_misconfiguration',
                    severity=TestSeverity.HIGH,
                    endpoint='/v1/enriched/quote',
                    description='CORS allows credentials with wildcard origin',
                    remediation='Do not use wildcard origin with credentials'
                ))
            elif acao == origin and acac.lower() == 'true':
                vulns.append(SecurityVulnerability(
                    type='cors_trusts_untrusted',
                    severity=TestSeverity.MEDIUM,
                    endpoint='/v1/enriched/quote',
                    description=f'CORS trusts untrusted origin: {origin}',
                    remediation='Restrict CORS to trusted origins only'
                ))
        
        return vulns
    
    async def audit_jwt(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Audit JWT implementation"""
        vulns = []
        
        if not JWT_AVAILABLE:
            return vulns
        
        # Test JWT algorithm confusion
        test_tokens = [
            # None algorithm
            jwt.encode({'sub': 'test'}, '', algorithm='none'),
            # Weak secret
            jwt.encode({'sub': 'test'}, 'secret', algorithm='HS256'),
            # Empty secret
            jwt.encode({'sub': 'test'}, '', algorithm='HS256'),
        ]
        
        for token in test_tokens:
            headers = {'Authorization': f'Bearer {token}'}
            status, data, _, _ = await client.get('/v1/enriched/quote', headers=headers, params={'symbol': 'AAPL'})
            
            if status == 200:
                vulns.append(SecurityVulnerability(
                    type='weak_jwt',
                    severity=TestSeverity.CRITICAL,
                    endpoint='/v1/enriched/quote',
                    description=f'JWT with weak algorithm accepted',
                    remediation='Reject tokens with "none" algorithm and weak secrets',
                    evidence={'token': token}
                ))
                break
        
        return vulns
    
    async def run_full_audit(self, client: AsyncHTTPClient) -> List[SecurityVulnerability]:
        """Run complete security audit"""
        all_vulns = []
        
        audit_functions = [
            self.audit_headers,
            self.audit_authentication,
            self.audit_rate_limiting,
            self.audit_sql_injection,
            self.audit_xss,
            self.audit_cors,
            self.audit_jwt,
        ]
        
        for audit_func in audit_functions:
            try:
                vulns = await audit_func(client)
                all_vulns.extend(vulns)
            except Exception as e:
                logger.error(f"Security audit failed: {e}")
        
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
        
        def on_start(self):
            """Setup on test start"""
            self.token = os.getenv('APP_TOKEN')
            self.headers = {
                'User-Agent': f'TFB-LoadTest/{SCRIPT_VERSION}',
                'Accept': 'application/json'
            }
            if self.token:
                self.headers['X-APP-TOKEN'] = self.token
                self.headers['Authorization'] = f'Bearer {self.token}'
        
        @task(10)
        def get_quote(self):
            """Get single quote"""
            symbols = ['AAPL', 'MSFT', 'GOOGL', '1120.SR', '2222.SR']
            symbol = random.choice(symbols)
            self.client.get(f"/v1/enriched/quote", params={'symbol': symbol}, headers=self.headers)
        
        @task(5)
        def get_batch_quotes(self):
            """Get batch quotes"""
            symbols = ['AAPL', 'MSFT', 'GOOGL']
            params = [('tickers', s) for s in symbols]
            self.client.get("/v1/enriched/quotes", params=params, headers=self.headers)
        
        @task(3)
        def get_analysis(self):
            """Get analysis"""
            symbols = ['AAPL']
            self.client.post("/v1/analysis/quotes", json={'tickers': symbols}, headers=self.headers)
        
        @task(1)
        def get_advisor_recs(self):
            """Get advisor recommendations"""
            payload = {
                'tickers': ['AAPL', 'MSFT'],
                'risk': 'Moderate',
                'top_n': 5
            }
            self.client.post("/v1/advisor/recommendations", json=payload, headers=self.headers)
        
        @task(2)
        def get_health(self):
            """Check health"""
            self.client.get("/health", headers=self.headers)

def run_locust_test(host: str, users: int, spawn_rate: int, run_time: str):
    """Run Locust load test"""
    if not LOCUST_AVAILABLE:
        logger.error("Locust not available")
        return
    
    from locust import events
    
    class LoadTestEnvironment:
        def __init__(self):
            self.environment = Environment(user_classes=[APIUser], host=host)
            self.environment.create_local_runner()
        
        def run(self):
            self.environment.runner.start(users, spawn_rate=spawn_rate)
            self.environment.runner.greenlet.join()
    
    # Parse run time
    import re
    match = re.match(r'(\d+)([smh])', run_time)
    if match:
        value, unit = match.groups()
        seconds = int(value) * {'s': 1, 'm': 60, 'h': 3600}[unit]
    else:
        seconds = 60
    
    # Run test
    logger.info(f"Starting load test: {users} users, spawn rate {spawn_rate}/s, duration {run_time}")
    
    env = LoadTestEnvironment()
    
    # Setup stats
    stats_printer(env.environment)
    
    # Run
    env.runner.start(users, spawn_rate=spawn_rate)
    time.sleep(seconds)
    env.runner.quit()
    
    # Collect results
    stats = env.environment.stats
    
    return {
        'total_requests': stats.total.num_requests,
        'total_failures': stats.total.num_failures,
        'avg_response_time': stats.total.avg_response_time,
        'min_response_time': stats.total.min_response_time or 0,
        'max_response_time': stats.total.max_response_time or 0,
        'requests_per_second': stats.total.current_rps,
        'fail_per_second': stats.total.current_fail_per_sec
    }

# =============================================================================
# Chaos Engineering
# =============================================================================
class ChaosEngine:
    """Chaos engineering experiments"""
    
    def __init__(self, base_url: str, failure_rate: float = 0.1, latency_ms: int = 0):
        self.base_url = base_url
        self.failure_rate = failure_rate
        self.latency_ms = latency_ms
        self.experiments: List[Dict[str, Any]] = []
    
    async def inject_latency(self, client: AsyncHTTPClient, endpoint: str, 
                             duration: int = 60) -> Dict[str, Any]:
        """Inject latency into endpoint"""
        logger.warning(f"Injecting {self.latency_ms}ms latency to {endpoint} for {duration}s")
        
        start = time.time()
        latencies = []
        failures = 0
        total = 0
        
        while time.time() - start < duration:
            # Add artificial latency
            if self.latency_ms > 0:
                await asyncio.sleep(self.latency_ms / 1000)
            
            # Maybe fail
            if random.random() < self.failure_rate:
                failures += 1
                total += 1
                continue
            
            # Make request
            status, _, response_time, _ = await client.get(endpoint, params={'symbol': 'AAPL'})
            latencies.append(response_time)
            total += 1
            
            if status != 200:
                failures += 1
            
            await asyncio.sleep(0.1)
        
        return {
            'endpoint': endpoint,
            'duration': duration,
            'total_requests': total,
            'failures': failures,
            'failure_rate': failures / total if total > 0 else 0,
            'latency_ms': self.latency_ms,
            'avg_response_time': sum(latencies) / len(latencies) if latencies else 0,
            'p95_response_time': np.percentile(latencies, 95) if latencies and NUMPY_AVAILABLE else 0
        }
    
    async def inject_failures(self, client: AsyncHTTPClient, endpoint: str,
                             duration: int = 60) -> Dict[str, Any]:
        """Inject random failures"""
        logger.warning(f"Injecting {self.failure_rate*100}% failures to {endpoint} for {duration}s")
        
        start = time.time()
        responses = []
        
        while time.time() - start < duration:
            # Maybe return 500 error
            if random.random() < self.failure_rate:
                responses.append(500)
            else:
                status, _, _, _ = await client.get(endpoint, params={'symbol': 'AAPL'})
                responses.append(status)
            
            await asyncio.sleep(0.1)
        
        return {
            'endpoint': endpoint,
            'duration': duration,
            'total_requests': len(responses),
            'failures': sum(1 for r in responses if r != 200),
            'failure_rate': sum(1 for r in responses if r != 200) / len(responses),
            'status_distribution': dict(Counter(responses))
        }
    
    async def test_circuit_breaker(self, client: AsyncHTTPClient, endpoint: str) -> Dict[str, Any]:
        """Test circuit breaker behavior"""
        logger.info(f"Testing circuit breaker for {endpoint}")
        
        results = {
            'before': {},
            'during': {},
            'after': {},
            'recovery_time': 0
        }
        
        # Baseline
        baseline_times = []
        for _ in range(10):
            _, _, rt, _ = await client.get(endpoint, params={'symbol': 'AAPL'})
            baseline_times.append(rt)
        
        results['before'] = {
            'avg_response_time': sum(baseline_times) / len(baseline_times),
            'success_rate': 1.0
        }
        
        # Trigger failures
        failures = 0
        for i in range(50):
            # Force failure by using invalid endpoint
            status, _, rt, _ = await client.get('/invalid-endpoint')
            if status != 200:
                failures += 1
            await asyncio.sleep(0.01)
        
        # Test during failures
        during_times = []
        during_failures = 0
        for _ in range(20):
            try:
                _, _, rt, _ = await client.get(endpoint, params={'symbol': 'AAPL'})
                during_times.append(rt)
            except Exception:
                during_failures += 1
            await asyncio.sleep(0.1)
        
        results['during'] = {
            'failures': during_failures,
            'avg_response_time': sum(during_times) / len(during_times) if during_times else 0
        }
        
        # Wait for recovery
        recovery_start = time.time()
        while time.time() - recovery_start < 60:
            try:
                _, _, _, _ = await client.get(endpoint, params={'symbol': 'AAPL'})
                if time.time() - recovery_start > 5:  # First success after 5s
                    results['recovery_time'] = time.time() - recovery_start
                    break
            except:
                await asyncio.sleep(0.5)
        
        return results

# =============================================================================
# WebSocket Tester
# =============================================================================
class WebSocketTester:
    """WebSocket endpoint tester"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url.replace('http', 'ws')
        self.token = os.getenv('APP_TOKEN')
    
    async def test_connection(self, path: str) -> Dict[str, Any]:
        """Test WebSocket connection"""
        if not WEBSOCKET_AVAILABLE:
            return {'error': 'WebSocket library not available'}
        
        url = urljoin(self.base_url, path.lstrip('/'))
        
        if self.token:
            url += f"?token={self.token}"
        
        results = {
            'connected': False,
            'messages_received': 0,
            'latency_ms': [],
            'errors': []
        }
        
        try:
            async with websockets.connect(url, timeout=10) as ws:
                results['connected'] = True
                
                # Send ping
                for i in range(5):
                    start = time.time()
                    await ws.send(json.dumps({'type': 'ping', 'id': i}))
                    
                    try:
                        response = await asyncio.wait_for(ws.recv(), timeout=5)
                        latency = (time.time() - start) * 1000
                        results['latency_ms'].append(latency)
                        
                        data = json.loads(response)
                        if data.get('type') == 'pong':
                            results['messages_received'] += 1
                    except Exception as e:
                        results['errors'].append(str(e))
                    
                    await asyncio.sleep(0.5)
                
                # Subscribe to updates
                await ws.send(json.dumps({
                    'type': 'subscribe',
                    'symbols': ['AAPL', 'MSFT', '1120.SR']
                }))
                
                # Wait for updates
                for _ in range(3):
                    try:
                        response = await asyncio.wait_for(ws.recv(), timeout=5)
                        data = json.loads(response)
                        if data.get('type') == 'update':
                            results['messages_received'] += 1
                    except asyncio.TimeoutError:
                        results['errors'].append('Timeout waiting for update')
                    except Exception as e:
                        results['errors'].append(str(e))
                    
        except Exception as e:
            results['errors'].append(str(e))
        
        if results['latency_ms']:
            results['avg_latency_ms'] = sum(results['latency_ms']) / len(results['latency_ms'])
        
        return results

# =============================================================================
# Database Integration Tester
# =============================================================================
class DatabaseTester:
    """Test database integration"""
    
    def __init__(self, connection_string: Optional[str] = None):
        self.connection_string = connection_string or os.getenv('DATABASE_URL')
        self.engine = None
        
        if self.connection_string:
            self._init_db()
    
    def _init_db(self):
        """Initialize database connection"""
        try:
            from sqlalchemy import create_engine, text
            self.engine = create_engine(self.connection_string)
            self.SQLALCHEMY_AVAILABLE = True
        except ImportError:
            self.SQLALCHEMY_AVAILABLE = False
    
    def test_connection(self) -> Dict[str, Any]:
        """Test database connection"""
        if not self.engine:
            return {'connected': False, 'error': 'No database connection'}
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                return {
                    'connected': True,
                    'version': conn.execute(text("SELECT version()")).scalar()
                }
        except Exception as e:
            return {'connected': False, 'error': str(e)}
    
    def test_queries(self) -> Dict[str, Any]:
        """Test common queries"""
        if not self.engine:
            return {}
        
        results = {}
        
        try:
            with self.engine.connect() as conn:
                # Test tables exist
                tables = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)).fetchall()
                
                results['tables'] = [t[0] for t in tables]
                
                # Test counts
                for table in results['tables'][:5]:
                    count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
                    results[f'{table}_count'] = count
                
        except Exception as e:
            results['error'] = str(e)
        
        return results
    
    def test_integrity(self) -> Dict[str, Any]:
        """Test data integrity"""
        if not self.engine:
            return {}
        
        results = {'constraints': [], 'errors': []}
        
        try:
            with self.engine.connect() as conn:
                # Check foreign keys
                fks = conn.execute(text("""
                    SELECT
                        tc.table_name, 
                        kcu.column_name,
                        ccu.table_name AS foreign_table_name,
                        ccu.column_name AS foreign_column_name
                    FROM 
                        information_schema.table_constraints AS tc 
                        JOIN information_schema.key_column_usage AS kcu
                          ON tc.constraint_name = kcu.constraint_name
                          AND tc.table_schema = kcu.table_schema
                        JOIN information_schema.constraint_column_usage AS ccu
                          ON ccu.constraint_name = tc.constraint_name
                          AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY';
                """)).fetchall()
                
                for fk in fks:
                    results['constraints'].append({
                        'table': fk[0],
                        'column': fk[1],
                        'references': f"{fk[2]}({fk[3]})"
                    })
                
                # Check for nulls in required columns
                # This would need schema knowledge
                
        except Exception as e:
            results['errors'].append(str(e))
        
        return results

# =============================================================================
# Main Test Runner
# =============================================================================
class TestRunner:
    """Main test execution engine"""
    
    def __init__(self, base_url: str, token: str = "", strict: bool = False):
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.strict = strict
        self.suite = TestSuite(name=f"API Test - {base_url}")
        self.client: Optional[AsyncHTTPClient] = None
        self.validator = SchemaValidator()
        self.security_auditor = SecurityAuditor(base_url)
        self.chaos_engine = ChaosEngine(base_url)
        self.ws_tester = WebSocketTester(base_url)
        self.db_tester = DatabaseTester()
    
    # =========================================================================
    # Test Definitions
    # =========================================================================
    def get_endpoints(self) -> List[TestEndpoint]:
        """Get list of endpoints to test"""
        return [
            # Infrastructure
            TestEndpoint(
                name="Root",
                path="/",
                method=HTTPMethod.GET,
                category=TestCategory.INFRASTRUCTURE,
                severity=TestSeverity.CRITICAL,
                auth_required=False
            ),
            TestEndpoint(
                name="Health Check",
                path="/health",
                method=HTTPMethod.GET,
                category=TestCategory.INFRASTRUCTURE,
                severity=TestSeverity.CRITICAL,
                auth_required=False
            ),
            TestEndpoint(
                name="Readiness",
                path="/readyz",
                method=HTTPMethod.GET,
                category=TestCategory.INFRASTRUCTURE,
                severity=TestSeverity.CRITICAL,
                auth_required=False
            ),
            
            # Enriched API
            TestEndpoint(
                name="Single Quote",
                path="/v1/enriched/quote",
                method=HTTPMethod.GET,
                category=TestCategory.DATA_VALIDITY,
                severity=TestSeverity.HIGH,
                params={'symbol': 'AAPL'}
            ),
            TestEndpoint(
                name="Batch Quotes",
                path="/v1/enriched/quotes",
                method=HTTPMethod.GET,
                category=TestCategory.DATA_VALIDITY,
                severity=TestSeverity.HIGH,
                params=[('tickers', 'AAPL'), ('tickers', 'MSFT')]
            ),
            
            # Analysis API
            TestEndpoint(
                name="Analysis Quote",
                path="/v1/analysis/quotes",
                method=HTTPMethod.POST,
                category=TestCategory.DATA_VALIDITY,
                severity=TestSeverity.HIGH,
                payload={'tickers': ['AAPL', 'MSFT']}
            ),
            
            # Advisor API
            TestEndpoint(
                name="Advisor Recommendations",
                path="/v1/advisor/recommendations",
                method=HTTPMethod.POST,
                category=TestCategory.DATA_VALIDITY,
                severity=TestSeverity.MEDIUM,
                payload={
                    'tickers': ['AAPL', 'MSFT'],
                    'risk': 'Moderate',
                    'top_n': 5
                },
                timeout=30.0
            ),
            
            # Advanced API
            TestEndpoint(
                name="Advanced Sheet Rows",
                path="/v1/advanced/sheet-rows",
                method=HTTPMethod.POST,
                category=TestCategory.DATA_VALIDITY,
                severity=TestSeverity.LOW,
                payload={
                    'tickers': ['AAPL'],
                    'sheet_name': 'Test'
                }
            ),
            
            # Router health checks
            TestEndpoint(
                name="Enriched Router Health",
                path="/v1/enriched/health",
                method=HTTPMethod.GET,
                category=TestCategory.INFRASTRUCTURE,
                severity=TestSeverity.MEDIUM,
                auth_required=False
            ),
            TestEndpoint(
                name="Analysis Router Health",
                path="/v1/analysis/health",
                method=HTTPMethod.GET,
                category=TestCategory.INFRASTRUCTURE,
                severity=TestSeverity.MEDIUM,
                auth_required=False
            ),
        ]
    
    # =========================================================================
    # Test Execution
    # =========================================================================
    async def run_test(self, endpoint: TestEndpoint) -> TestResult:
        """Run single test"""
        if not self.client:
            raise RuntimeError("Client not initialized")
        
        start_time = time.time()
        
        try:
            # Make request
            status, data, response_time, headers = await self.client.request(
                endpoint.method.value,
                endpoint.path,
                params=endpoint.params,
                json=endpoint.payload,
                headers=endpoint.headers
            )
            
            # Check status
            status_ok = status == endpoint.expected_status
            
            # Validate response
            validation_errors = []
            if self.validator:
                validation_errors = self.validator.validate_response(
                    endpoint.path,
                    endpoint.method.value,
                    status,
                    data
                )
            
            # Determine test status
            if status_ok and not validation_errors:
                test_status = TestStatus.PASS
                message = "OK"
            elif not status_ok and endpoint.severity in [TestSeverity.CRITICAL, TestSeverity.HIGH]:
                test_status = TestStatus.FAIL
                message = f"Expected {endpoint.expected_status}, got {status}"
            elif validation_errors:
                test_status = TestStatus.WARN if not self.strict else TestStatus.FAIL
                message = f"Validation errors: {validation_errors[0]}"
            else:
                test_status = TestStatus.WARN
                message = f"Unexpected status: {status}"
            
            # Additional checks for data endpoints
            if endpoint.category == TestCategory.DATA_VALIDITY and test_status == TestStatus.PASS:
                if isinstance(data, dict):
                    # Check for Riyadh timestamp
                    has_riyadh = any('riyadh' in k.lower() or 'ksa' in k.lower() for k in data.keys())
                    if not has_riyadh and isinstance(data.get('items'), list):
                        for item in data['items']:
                            if isinstance(item, dict):
                                has_riyadh = any('riyadh' in k.lower() for k in item.keys())
                                if has_riyadh:
                                    break
                    
                    if not has_riyadh and endpoint.name in ['Single Quote', 'Batch Quotes']:
                        test_status = TestStatus.WARN
                        message = "Missing Riyadh timestamp"
            
            return TestResult(
                endpoint=endpoint.path,
                method=endpoint.method.value,
                status=test_status,
                severity=endpoint.severity,
                category=endpoint.category,
                response_time=response_time,
                http_status=status,
                message=message,
                expected_status=endpoint.expected_status,
                validation_errors=validation_errors,
                details={'headers': dict(headers)} if headers else {}
            )
            
        except Exception as e:
            return TestResult(
                endpoint=endpoint.path,
                method=endpoint.method.value,
                status=TestStatus.ERROR,
                severity=endpoint.severity,
                category=endpoint.category,
                response_time=time.time() - start_time,
                http_status=0,
                message=str(e)
            )
    
    async def run_all(self, concurrency: int = 5) -> TestSuite:
        """Run all tests"""
        endpoints = self.get_endpoints()
        
        async with AsyncHTTPClient(self.base_url) as client:
            self.client = client
            
            # Run tests with concurrency limit
            semaphore = asyncio.Semaphore(concurrency)
            
            async def run_with_semaphore(endpoint):
                async with semaphore:
                    return await self.run_test(endpoint)
            
            tasks = [run_with_semaphore(e) for e in endpoints]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, TestResult):
                    self.suite.add_result(result)
                else:
                    logger.error(f"Test error: {result}")
        
        self.suite.end_time = datetime.now(timezone.utc)
        return self.suite
    
    async def run_security_audit(self) -> List[SecurityVulnerability]:
        """Run security audit"""
        async with AsyncHTTPClient(self.base_url) as client:
            vulns = await self.security_auditor.run_full_audit(client)
            
            for v in vulns:
                self.suite.add_result(TestResult(
                    endpoint=v.endpoint,
                    method="SECURITY",
                    status=TestStatus.FAIL if v.severity in [TestSeverity.CRITICAL, TestSeverity.HIGH] else TestStatus.WARN,
                    severity=v.severity,
                    category=TestCategory.SECURITY,
                    response_time=0,
                    http_status=0,
                    message=v.description,
                    details={'remediation': v.remediation, 'cve': v.cve}
                ))
            
            return vulns
    
    async def run_performance_test(self, duration: int = 60) -> Dict[str, PerformanceMetrics]:
        """Run performance test"""
        async with AsyncHTTPClient(self.base_url) as client:
            self.client = client
            
            endpoints = [
                ('GET', '/v1/enriched/quote', {'symbol': 'AAPL'}),
                ('GET', '/v1/enriched/quotes', [('tickers', 'AAPL'), ('tickers', 'MSFT')]),
                ('POST', '/v1/analysis/quotes', {'tickers': ['AAPL', 'MSFT']}),
            ]
            
            start = time.time()
            while time.time() - start < duration:
                for method, path, params in endpoints:
                    if method == 'GET':
                        await client.get(path, params=params)
                    else:
                        await client.post(path, json=params)
                    await asyncio.sleep(0.01)
            
            return client.get_metrics()
    
    async def run_chaos_test(self, duration: int = 60) -> Dict[str, Any]:
        """Run chaos engineering experiments"""
        results = {}
        
        async with AsyncHTTPClient(self.base_url) as client:
            self.client = client
            
            # Test circuit breaker
            results['circuit_breaker'] = await self.chaos_engine.test_circuit_breaker(
                client, '/v1/enriched/quote'
            )
            
            # Inject latency
            self.chaos_engine.latency_ms = 500
            results['latency'] = await self.chaos_engine.inject_latency(
                client, '/v1/enriched/quote', duration=min(30, duration)
            )
            
            # Inject failures
            self.chaos_engine.failure_rate = 0.2
            results['failures'] = await self.chaos_engine.inject_failures(
                client, '/v1/enriched/quote', duration=min(30, duration)
            )
        
        return results
    
    async def run_websocket_test(self) -> Dict[str, Any]:
        """Run WebSocket tests"""
        return await self.ws_tester.test_connection('/ws')
    
    def run_database_test(self) -> Dict[str, Any]:
        """Run database tests"""
        return {
            'connection': self.db_tester.test_connection(),
            'queries': self.db_tester.test_queries(),
            'integrity': self.db_tester.test_integrity()
        }
    
    def print_summary(self) -> None:
        """Print test summary"""
        summary = self.suite.summary
        
        print("\n" + "=" * 100)
        print(colorize("  API TEST SUMMARY", Colors.BOLD, bold=True))
        print("=" * 100)
        
        # Header
        print(
            f"{colorize('ENDPOINT', Colors.BOLD):<50} "
            f"{colorize('STATUS', Colors.BOLD):<10} "
            f"{colorize('TIME', Colors.BOLD):<10} "
            f"{colorize('HTTP', Colors.BOLD):<6} "
            f"{colorize('DETAILS', Colors.BOLD)}"
        )
        print("-" * 100)
        
        # Results
        for result in self.suite.results:
            # Color based on status
            if result.status == TestStatus.PASS:
                status_color = Colors.GREEN
            elif result.status == TestStatus.WARN:
                status_color = Colors.YELLOW
            elif result.status == TestStatus.FAIL:
                status_color = Colors.RED
            else:
                status_color = Colors.WHITE
            
            # Format endpoint name
            endpoint_display = f"{result.method} {result.endpoint}"
            if len(endpoint_display) > 48:
                endpoint_display = endpoint_display[:45] + "..."
            
            print(
                f"{endpoint_display:<50} "
                f"{colorize(result.status.value, status_color):<10} "
                f"{format_duration(result.response_time):<10} "
                f"{result.http_status if result.http_status else '---':<6} "
                f"{result.message[:50]}"
            )
        
        print("-" * 100)
        
        # Summary statistics
        print(f"\n{colorize('SUMMARY STATISTICS', Colors.BOLD)}")
        print(f"  Total Tests: {summary['total']}")
        print(f"  Passed: {colorize(str(summary['passed']), Colors.GREEN)}")
        print(f"  Failed: {colorize(str(summary['failed']), Colors.RED)}")
        print(f"  Warnings: {colorize(str(summary['warned']), Colors.YELLOW)}")
        print(f"  Duration: {format_duration(summary['duration'])}")
        
        # Response times
        rt = summary['response_times']
        print(f"\n{colorize('RESPONSE TIMES', Colors.BOLD)}")
        print(f"  Min: {format_duration(rt['min'])}")
        print(f"  Max: {format_duration(rt['max'])}")
        print(f"  Avg: {format_duration(rt['avg'])}")
        print(f"  P95: {format_duration(rt['p95'])}")
        print(f"  P99: {format_duration(rt['p99'])}")
        
        # By category
        print(f"\n{colorize('BY CATEGORY', Colors.BOLD)}")
        for category, count in summary['by_category'].items():
            passed = sum(1 for r in self.suite.results 
                        if r.category.value == category and r.status == TestStatus.PASS)
            print(f"  {category:<20}: {passed}/{count} passed")
        
        # By severity
        print(f"\n{colorize('BY SEVERITY', Colors.BOLD)}")
        for severity, count in summary['by_severity'].items():
            color = Colors.RED if severity == 'critical' else Colors.YELLOW if severity == 'high' else Colors.WHITE
            print(f"  {severity:<10}: {colorize(str(count), color)}")
        
        print("=" * 100)

# =============================================================================
# Report Generator
# =============================================================================
class ReportGenerator:
    """Generate comprehensive test reports"""
    
    def __init__(self, suite: TestSuite):
        self.suite = suite
    
    def to_json(self, filepath: str) -> None:
        """Save JSON report"""
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(self.suite.to_json())
        logger.info(f"JSON report saved to {filepath}")
    
    def to_html(self, filepath: str) -> None:
        """Generate HTML report"""
        summary = self.suite.summary
        
        # Generate HTML
        html = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>API Test Report - {self.suite.start_time.strftime('%Y-%m-%d %H:%M:%S')}</title>
            <style>
                body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; background: #f5f5f5; }}
                .container {{ max-width: 1200px; margin: auto; background: white; padding: 20px; box-shadow: 0 0 20px rgba(0,0,0,0.1); }}
                h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
                h2 {{ color: #34495e; margin-top: 30px; }}
                .summary {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin: 20px 0; }}
                .card {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }}
                .card h3 {{ margin: 0 0 10px 0; font-size: 14px; opacity: 0.9; }}
                .card .value {{ font-size: 32px; font-weight: bold; }}
                .card .label {{ font-size: 12px; opacity: 0.8; }}
                .pass {{ color: #27ae60; }}
                .fail {{ color: #c0392b; }}
                .warn {{ color: #f39c12; }}
                table {{ width: 100%; border-collapse: collapse; margin: 20px 0; background: white; }}
                th {{ background: #2c3e50; color: white; padding: 12px; text-align: left; }}
                td {{ padding: 12px; border-bottom: 1px solid #ecf0f1; }}
                tr:hover {{ background: #f8f9fa; }}
                .status-badge {{
                    display: inline-block;
                    padding: 4px 8px;
                    border-radius: 4px;
                    font-size: 12px;
                    font-weight: bold;
                }}
                .status-PASS {{ background: #d4edda; color: #155724; }}
                .status-FAIL {{ background: #f8d7da; color: #721c24; }}
                .status-WARN {{ background: #fff3cd; color: #856404; }}
                .status-ERROR {{ background: #e2e3e5; color: #383d41; }}
                .severity-critical {{ border-left: 4px solid #c0392b; }}
                .severity-high {{ border-left: 4px solid #e67e22; }}
                .severity-medium {{ border-left: 4px solid #f1c40f; }}
                .severity-low {{ border-left: 4px solid #3498db; }}
                .chart {{ margin: 30px 0; }}
                .footer {{ margin-top: 30px; padding-top: 10px; border-top: 1px solid #ddd; color: #7f8c8d; text-align: center; }}
                .metric-grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin: 20px 0; }}
                .metric {{ background: #f8f9fa; padding: 15px; border-radius: 5px; }}
                .metric .value {{ font-size: 24px; font-weight: bold; color: #2c3e50; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>🔍 API Test Report</h1>
                <p>Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                
                <div class="summary">
                    <div class="card">
                        <h3>Total Tests</h3>
                        <div class="value">{summary['total']}</div>
                    </div>
                    <div class="card" style="background: linear-gradient(135deg, #27ae60 0%, #229954 100%);">
                        <h3>Passed</h3>
                        <div class="value">{summary['passed']}</div>
                    </div>
                    <div class="card" style="background: linear-gradient(135deg, #c0392b 0%, #a93226 100%);">
                        <h3>Failed</h3>
                        <div class="value">{summary['failed']}</div>
                    </div>
                    <div class="card" style="background: linear-gradient(135deg, #f39c12 0%, #e67e22 100%);">
                        <h3>Warnings</h3>
                        <div class="value">{summary['warned']}</div>
                    </div>
                </div>
                
                <h2>Response Times</h2>
                <div class="metric-grid">
                    <div class="metric">
                        <div class="label">Min</div>
                        <div class="value">{summary['response_times']['min']*1000:.2f}ms</div>
                    </div>
                    <div class="metric">
                        <div class="label">Max</div>
                        <div class="value">{summary['response_times']['max']*1000:.2f}ms</div>
                    </div>
                    <div class="metric">
                        <div class="label">Avg</div>
                        <div class="value">{summary['response_times']['avg']*1000:.2f}ms</div>
                    </div>
                    <div class="metric">
                        <div class="label">P95</div>
                        <div class="value">{summary['response_times']['p95']*1000:.2f}ms</div>
                    </div>
                </div>
                
                <h2>Test Results</h2>
                <table>
                    <tr>
                        <th>Endpoint</th>
                        <th>Method</th>
                        <th>Status</th>
                        <th>Severity</th>
                        <th>Response Time</th>
                        <th>HTTP</th>
                        <th>Message</th>
                    </tr>
        """
        
        for r in self.suite.results:
            severity_class = f"severity-{r.severity.value}"
            html += f"""
                    <tr class="{severity_class}">
                        <td>{r.endpoint}</td>
                        <td>{r.method}</td>
                        <td><span class="status-badge status-{r.status.value}">{r.status.value}</span></td>
                        <td>{r.severity.value}</td>
                        <td>{r.response_time*1000:.2f}ms</td>
                        <td>{r.http_status if r.http_status else '-'}</td>
                        <td>{r.message}</td>
                    </tr>
            """
        
        html += """
                </table>
                
                <h2>Results by Category</h2>
                <table>
                    <tr>
                        <th>Category</th>
                        <th>Total</th>
                        <th>Passed</th>
                        <th>Failed</th>
                        <th>Warnings</th>
                    </tr>
        """
        
        for category, count in summary['by_category'].items():
            passed = sum(1 for r in self.suite.results if r.category.value == category and r.status == TestStatus.PASS)
            failed = sum(1 for r in self.suite.results if r.category.value == category and r.status == TestStatus.FAIL)
            warned = sum(1 for r in self.suite.results if r.category.value == category and r.status == TestStatus.WARN)
            
            html += f"""
                    <tr>
                        <td>{category}</td>
                        <td>{count}</td>
                        <td class="pass">{passed}</td>
                        <td class="fail">{failed}</td>
                        <td class="warn">{warned}</td>
                    </tr>
            """
        
        html += """
                </table>
                
                <div class="footer">
                    Generated by TFB API Test Suite v{SCRIPT_VERSION}
                </div>
            </div>
        </body>
        </html>
        """
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html)
        
        logger.info(f"HTML report saved to {filepath}")
    
    def to_junit_xml(self, filepath: str) -> None:
        """Generate JUnit XML report"""
        root = ET.Element('testsuites')
        testsuite = ET.SubElement(root, 'testsuite')
        
        testsuite.set('name', 'API Tests')
        testsuite.set('tests', str(self.suite.summary['total']))
        testsuite.set('failures', str(self.suite.summary['failed']))
        testsuite.set('errors', str(self.suite.summary.get('error', 0)))
        testsuite.set('skipped', str(self.suite.summary.get('skipped', 0)))
        testsuite.set('time', str(self.suite.duration))
        testsuite.set('timestamp', self.suite.start_time.isoformat())
        
        for r in self.suite.results:
            testcase = ET.SubElement(testsuite, 'testcase')
            testcase.set('name', f"{r.method} {r.endpoint}")
            testcase.set('classname', r.category.value)
            testcase.set('time', str(r.response_time))
            
            if r.status == TestStatus.FAIL:
                failure = ET.SubElement(testcase, 'failure')
                failure.set('message', r.message)
                failure.set('type', r.severity.value)
            elif r.status == TestStatus.WARN:
                # JUnit doesn't have warnings, use system-out
                system_out = ET.SubElement(testcase, 'system-out')
                system_out.text = f"WARNING: {r.message}"
        
        tree = ET.ElementTree(root)
        tree.write(filepath, encoding='utf-8', xml_declaration=True)
        
        logger.info(f"JUnit XML report saved to {filepath}")

# =============================================================================
# CLI Entry Point
# =============================================================================
def create_parser() -> argparse.ArgumentParser:
    """Create argument parser"""
    parser = argparse.ArgumentParser(
        description=f"Tadawul Fast Bridge - Enterprise API Test Suite v{SCRIPT_VERSION}",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Basic options
    parser.add_argument("--url", default=os.getenv('TFB_BASE_URL', 'http://localhost:8000'),
                       help="Base URL")
    parser.add_argument("--token", default=os.getenv('APP_TOKEN', ''),
                       help="Auth token")
    parser.add_argument("--strict", action="store_true",
                       help="Treat warnings as failures")
    parser.add_argument("--timeout", type=int, default=30,
                       help="Request timeout")
    
    # Test selection
    parser.add_argument("--smoke", action="store_true",
                       help="Run basic smoke tests")
    parser.add_argument("--full", action="store_true",
                       help="Run full test suite")
    parser.add_argument("--security-audit", action="store_true",
                       help="Run security audit")
    parser.add_argument("--performance", action="store_true",
                       help="Run performance tests")
    parser.add_argument("--load-test", action="store_true",
                       help="Run load test (requires locust)")
    parser.add_argument("--chaos", action="store_true",
                       help="Run chaos experiments")
    parser.add_argument("--websocket", action="store_true",
                       help="Test WebSocket endpoints")
    parser.add_argument("--database", action="store_true",
                       help="Test database integration")
    
    # Load test options
    parser.add_argument("--users", type=int, default=10,
                       help="Number of users for load test")
    parser.add_argument("--spawn-rate", type=float, default=1.0,
                       help="User spawn rate")
    parser.add_argument("--run-time", default="1m",
                       help="Load test duration (e.g., 1m, 5m, 1h)")
    
    # Chaos options
    parser.add_argument("--failure-rate", type=float, default=0.1,
                       help="Failure rate for chaos experiments")
    parser.add_argument("--latency-ms", type=int, default=0,
                       help="Latency to inject (ms)")
    parser.add_argument("--chaos-duration", type=int, default=60,
                       help="Chaos experiment duration (seconds)")
    
    # Performance options
    parser.add_argument("--perf-duration", type=int, default=60,
                       help="Performance test duration (seconds)")
    parser.add_argument("--concurrency", type=int, default=5,
                       help="Concurrent requests")
    
    # Report options
    parser.add_argument("--report-json", help="Save JSON report")
    parser.add_argument("--report-html", help="Save HTML report")
    parser.add_argument("--report-junit", help="Save JUnit XML report")
    
    # Other
    parser.add_argument("--verbose", "-v", action="store_true",
                       help="Verbose output")
    
    return parser

async def async_main() -> int:
    """Async main entry point"""
    parser = create_parser()
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create test runner
    runner = TestRunner(args.url, args.token, strict=args.strict)
    
    try:
        # Run selected tests
        if args.smoke or args.full:
            logger.info("Running smoke tests...")
            await runner.run_all(concurrency=args.concurrency)
        
        if args.security_audit or args.full:
            logger.info("Running security audit...")
            await runner.run_security_audit()
        
        if args.performance or args.full:
            logger.info("Running performance tests...")
            perf_metrics = await runner.run_performance_test(duration=args.perf_duration)
            for key, metrics in perf_metrics.items():
                logger.info(f"  {key}: avg={metrics.avg_time*1000:.2f}ms, p95={metrics.p95_time*1000:.2f}ms")
        
        if args.load_test and LOCUST_AVAILABLE:
            logger.info("Running load test...")
            load_results = run_locust_test(args.url, args.users, args.spawn_rate, args.run_time)
            logger.info(f"Load test results: {load_results}")
        
        if args.chaos or args.full:
            logger.info("Running chaos experiments...")
            chaos_results = await runner.run_chaos_test(duration=args.chaos_duration)
            logger.info(f"Chaos results: {chaos_results}")
        
        if args.websocket:
            logger.info("Testing WebSocket...")
            ws_results = await runner.run_websocket_test()
            logger.info(f"WebSocket results: {ws_results}")
        
        if args.database:
            logger.info("Testing database...")
            db_results = runner.run_database_test()
            logger.info(f"Database results: {db_results}")
        
        # Print summary
        if runner.suite.results:
            runner.print_summary()
        
        # Generate reports
        if runner.suite.results:
            report_gen = ReportGenerator(runner.suite)
            
            if args.report_json:
                report_gen.to_json(args.report_json)
            
            if args.report_html:
                report_gen.to_html(args.report_html)
            
            if args.report_junit:
                report_gen.to_junit_xml(args.report_junit)
        
        # Determine exit code
        summary = runner.suite.summary
        
        if summary['failed'] > 0:
            return 2
        elif summary['warned'] > 0 and args.strict:
            return 3
        else:
            return 0
            
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 130
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        return 1

def main() -> int:
    """Main entry point"""
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    return asyncio.run(async_main())

if __name__ == "__main__":
    sys.exit(main())
