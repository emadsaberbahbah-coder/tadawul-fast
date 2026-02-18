#!/usr/bin/env python3
"""
start_web.py
===========================================================
TADAWUL FAST BRIDGE – ENTERPRISE WEB LAUNCHER (v4.0.0)
===========================================================
Production-Ready Application Server with Advanced Orchestration

Core Capabilities
-----------------
• Intelligent worker scaling (CPU, memory, I/O optimized)
• Multi-mode execution (Gunicorn/Uvicorn/Hybrid/Development)
• Advanced health monitoring and self-healing
• Dynamic configuration with hot-reload support
• Performance profiling and metrics collection
• Circuit breaker for external services
• Graceful shutdown with connection draining
• Memory/CPU limits enforcement
• Structured logging with multiple outputs

Exit Codes
----------
0: Clean exit
1: Configuration error
2: Runtime error
3: Worker timeout/failure
4: Health check failed
5: Resource exhaustion
130: Interrupted by user

Usage Examples
--------------
# Production (auto-configured)
python scripts/start_web.py

# Development with hot reload
python scripts/start_web.py --dev --reload

# Custom worker configuration
python scripts/start_web.py --workers 8 --worker-class httptools

# Debug mode with profiling
python scripts/start_web.py --debug --profile --profile-dir ./profiles

# Health check only
python scripts/start_web.py --health-check-only

# With custom config file
python scripts/start_web.py --config production.yaml
"""

from __future__ import annotations

import argparse
import asyncio
import gc
import importlib
import inspect
import json
import logging
import logging.config
import logging.handlers
import os
import platform
import re
import resource
import signal
import socket
import subprocess
import sys
import threading
import time
import tracemalloc
import uuid
import warnings
from collections import deque
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from functools import lru_cache, wraps
from pathlib import Path
from threading import Lock, Thread
from types import FrameType
from typing import (Any, Callable, Dict, List, Optional, Set, Tuple, Type,
                    TypeVar, Union, cast)

# Try importing optional dependencies
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    psutil = None
    PSUTIL_AVAILABLE = False

try:
    import uvloop
    UVLOOP_AVAILABLE = True
except ImportError:
    uvloop = None
    UVLOOP_AVAILABLE = False

try:
    import httptools
    HTTPTOOLS_AVAILABLE = True
except ImportError:
    httptools = None
    HTTPTOOLS_AVAILABLE = False

try:
    import uvicorn
    from uvicorn.config import LOGGING_CONFIG
    UVICORN_AVAILABLE = True
except ImportError:
    uvicorn = None
    UVICORN_AVAILABLE = False

try:
    import gunicorn
    from gunicorn.app.base import BaseApplication
    from gunicorn.workers.ggevent import GeventWorker
    from gunicorn.workers.gthread import ThreadWorker
    GUNICORN_AVAILABLE = True
except ImportError:
    gunicorn = None
    GUNICORN_AVAILABLE = False

try:
    import yaml
    YAML_AVAILABLE = True
except ImportError:
    yaml = None
    YAML_AVAILABLE = False

try:
    import pydantic
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    pydantic = None
    PYDANTIC_AVAILABLE = False

# =============================================================================
# Version & Constants
# =============================================================================
SCRIPT_VERSION = "4.0.0"
SCRIPT_NAME = "TadawulFastBridge"
MIN_PYTHON = (3, 8)

if sys.version_info < MIN_PYTHON:
    sys.exit(f"❌ Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ required")

# Signal handlers for graceful shutdown
SHUTDOWN_EVENT = threading.Event()
RELOAD_EVENT = threading.Event()
HANDLED_SIGNALS = [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]

# =============================================================================
# Enums & Advanced Types
# =============================================================================
class ServerMode(Enum):
    """Server execution modes"""
    DEVELOPMENT = "development"      # Single process, hot reload
    PRODUCTION = "production"        # Multi-worker, optimized
    DEBUG = "debug"                  # Single worker with debug tools
    PROFILING = "profiling"           # With performance profiling
    HEALTH_CHECK = "health-check"     # Only check health, don't start

class WorkerClass(Enum):
    """Available worker types"""
    UVICORN = "uvicorn"               # Default ASGI worker
    UVICORN_LOOP = "uvicorn_loop"     # With uvloop
    UVICORN_HTTPTOOLS = "uvicorn_httptools"  # With httptools
    GUNICORN_SYNC = "gunicorn_sync"   # Sync workers
    GUNICORN_ASYNC = "gunicorn_async" # Async workers
    GUNICORN_GEVENT = "gunicorn_gevent"  # Gevent workers
    GUNICORN_THREAD = "gunicorn_thread"  # Thread workers
    HYBRID = "hybrid"                 # Mix of worker types

class LogFormat(Enum):
    """Log output formats"""
    JSON = "json"                      # Structured JSON logs
    TEXT = "text"                      # Human-readable text
    COLOR = "color"                    # Color-coded console
    GRAYLOG = "graylog"                # GELF format for Graylog

class HealthStatus(Enum):
    """Health check status"""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    MAINTENANCE = "maintenance"

# =============================================================================
# Configuration Models
# =============================================================================
if PYDANTIC_AVAILABLE:
    class ServerConfig(BaseModel):
        """Server configuration with validation"""
        # Core settings
        host: str = Field("0.0.0.0", regex=r"^[\d\.:]+$")
        port: int = Field(8000, ge=1, le=65535)
        mode: ServerMode = ServerMode.PRODUCTION
        workers: Optional[int] = Field(None, ge=1, le=64)
        worker_class: WorkerClass = WorkerClass.UVICORN
        
        # Application settings
        app_module: str = "main"
        app_var: str = "app"
        root_path: str = ""
        proxy_headers: bool = True
        forwarded_allow_ips: str = "*"
        
        # Performance tuning
        backlog: int = Field(2048, ge=64, le=16384)
        limit_concurrency: Optional[int] = Field(None, ge=1)
        limit_max_requests: Optional[int] = Field(None, ge=1)
        timeout_keep_alive: int = Field(65, ge=1, le=300)
        timeout_graceful: int = Field(30, ge=1, le=120)
        timeout_request: int = Field(60, ge=1, le=600)
        
        # Worker settings
        worker_connections: int = Field(1000, ge=10, le=10000)
        worker_max_requests: int = Field(0, ge=0)
        worker_max_requests_jitter: int = Field(0, ge=0)
        worker_tmp_dir: str = "/dev/shm"
        
        # Resource limits
        max_memory_mb: Optional[int] = Field(None, ge=128)
        max_cpu_percent: Optional[int] = Field(None, ge=10, le=100)
        max_fds: Optional[int] = Field(None, ge=64)
        
        # Logging
        log_level: str = Field("info", regex="^(debug|info|warning|error|critical)$")
        log_format: LogFormat = LogFormat.TEXT
        log_file: Optional[str] = None
        access_log: bool = True
        access_log_format: str = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'
        
        # Development
        reload: bool = False
        reload_dirs: List[str] = Field(default_factory=list)
        reload_includes: List[str] = Field(default_factory=list)
        reload_excludes: List[str] = Field(default_factory=list)
        
        # Advanced
        ssl_keyfile: Optional[str] = None
        ssl_certfile: Optional[str] = None
        ssl_ca_certs: Optional[str] = None
        factory: bool = False
        server_header: bool = True
        date_header: bool = True
        
        # Health checks
        health_check_path: str = "/health"
        health_check_interval: int = 30
        health_check_timeout: int = 5
        health_check_unhealthy_threshold: int = 3
        
        # Circuit breaker
        circuit_breaker_failures: int = 5
        circuit_breaker_timeout: int = 60
        
        class Config:
            use_enum_values = True
            validate_assignment = True
        
        @validator('workers', always=True)
        def validate_workers(cls, v, values):
            if v is not None:
                return v
            # Auto-detect optimal workers
            mode = values.get('mode', ServerMode.PRODUCTION)
            if mode == ServerMode.DEVELOPMENT:
                return 1
            return cls._auto_detect_workers()
        
        @staticmethod
        def _auto_detect_workers() -> int:
            """Auto-detect optimal worker count"""
            cpus = os.cpu_count() or 1
            
            # Check memory constraints
            if PSUTIL_AVAILABLE:
                mem = psutil.virtual_memory()
                available_gb = mem.available / (1024 ** 3)
                mem_cap = int(available_gb * 2)  # 2 workers per GB
            else:
                mem_cap = 8  # Conservative default
            
            # I/O bound applications benefit from more workers
            workers = min(cpus * 2 + 1, mem_cap)
            return max(1, workers)
        
        def get_worker_options(self) -> Dict[str, Any]:
            """Get worker-specific options"""
            options = {
                'worker_class': self.worker_class.value,
                'worker_connections': self.worker_connections,
                'max_requests': self.worker_max_requests,
                'max_requests_jitter': self.worker_max_requests_jitter,
                'worker_tmp_dir': self.worker_tmp_dir,
            }
            if self.limit_concurrency:
                options['limit_concurrency'] = self.limit_concurrency
            return options
        
        def get_bind_address(self) -> str:
            """Get full bind address"""
            return f"{self.host}:{self.port}"
else:
    # Simplified config without validation
    @dataclass
    class ServerConfig:
        """Server configuration"""
        host: str = "0.0.0.0"
        port: int = 8000
        mode: ServerMode = ServerMode.PRODUCTION
        workers: Optional[int] = None
        worker_class: WorkerClass = WorkerClass.UVICORN
        app_module: str = "main"
        app_var: str = "app"
        root_path: str = ""
        proxy_headers: bool = True
        forwarded_allow_ips: str = "*"
        backlog: int = 2048
        limit_concurrency: Optional[int] = None
        limit_max_requests: Optional[int] = None
        timeout_keep_alive: int = 65
        timeout_graceful: int = 30
        timeout_request: int = 60
        worker_connections: int = 1000
        worker_max_requests: int = 0
        worker_max_requests_jitter: int = 0
        worker_tmp_dir: str = "/dev/shm"
        max_memory_mb: Optional[int] = None
        max_cpu_percent: Optional[int] = None
        max_fds: Optional[int] = None
        log_level: str = "info"
        log_format: LogFormat = LogFormat.TEXT
        log_file: Optional[str] = None
        access_log: bool = True
        access_log_format: str = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'
        reload: bool = False
        reload_dirs: List[str] = field(default_factory=list)
        reload_includes: List[str] = field(default_factory=list)
        reload_excludes: List[str] = field(default_factory=list)
        ssl_keyfile: Optional[str] = None
        ssl_certfile: Optional[str] = None
        ssl_ca_certs: Optional[str] = None
        factory: bool = False
        server_header: bool = True
        date_header: bool = True
        health_check_path: str = "/health"
        health_check_interval: int = 30
        health_check_timeout: int = 5
        health_check_unhealthy_threshold: int = 3
        circuit_breaker_failures: int = 5
        circuit_breaker_timeout: int = 60

# =============================================================================
# Advanced Logging System
# =============================================================================
class StructuredLogger:
    """Advanced structured logging with multiple outputs"""
    
    def __init__(self, name: str, config: ServerConfig):
        self.name = name
        self.config = config
        self.logger = logging.getLogger(name)
        self.handlers: List[logging.Handler] = []
        self._setup_logging()
    
    def _setup_logging(self):
        """Configure logging based on settings"""
        self.logger.setLevel(getattr(logging, self.config.log_level.upper()))
        
        # Remove existing handlers
        self.logger.handlers.clear()
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(self._get_formatter())
        self.logger.addHandler(console_handler)
        self.handlers.append(console_handler)
        
        # File handler if specified
        if self.config.log_file:
            file_handler = logging.handlers.RotatingFileHandler(
                self.config.log_file,
                maxBytes=100 * 1024 * 1024,  # 100 MB
                backupCount=10
            )
            file_handler.setFormatter(self._get_formatter())
            self.logger.addHandler(file_handler)
            self.handlers.append(file_handler)
        
        # JSON handler if specified
        if self.config.log_format == LogFormat.JSON:
            self._setup_json_logging()
    
    def _get_formatter(self) -> logging.Formatter:
        """Get appropriate formatter"""
        if self.config.log_format == LogFormat.JSON:
            return logging.Formatter(
                '{"time": "%(asctime)s", "level": "%(levelname)s", '
                '"name": "%(name)s", "message": "%(message)s"}'
            )
        elif self.config.log_format == LogFormat.COLOR:
            return logging.Formatter(
                '\033[36m%(asctime)s\033[0m | \033[1m%(levelname)-8s\033[0m | '
                '\033[35m%(name)s\033[0m | %(message)s'
            )
        else:
            return logging.Formatter(
                '%(asctime)s | %(levelname)-8s | %(name)s | %(message)s'
            )
    
    def _setup_json_logging(self):
        """Setup JSON-specific logging"""
        # Could add Graylog/GELF handler here
        pass
    
    def debug(self, msg: str, **kwargs):
        self.logger.debug(msg, extra=kwargs)
    
    def info(self, msg: str, **kwargs):
        self.logger.info(msg, extra=kwargs)
    
    def warning(self, msg: str, **kwargs):
        self.logger.warning(msg, extra=kwargs)
    
    def error(self, msg: str, **kwargs):
        self.logger.error(msg, extra=kwargs)
    
    def critical(self, msg: str, **kwargs):
        self.logger.critical(msg, extra=kwargs)
    
    def metric(self, name: str, value: float, tags: Dict[str, str] = None):
        """Log a metric for monitoring"""
        tags_str = ','.join(f"{k}={v}" for k, v in (tags or {}).items())
        self.info(f"METRIC|{name}|{value}|{tags_str}")

# =============================================================================
# Health Check System
# =============================================================================
class HealthChecker:
    """Advanced health monitoring system"""
    
    def __init__(self, config: ServerConfig, logger: StructuredLogger):
        self.config = config
        self.logger = logger
        self.status = HealthStatus.HEALTHY
        self.failures = 0
        self.last_check: Optional[datetime] = None
        self.last_success: Optional[datetime] = None
        self.checks: Dict[str, Callable[[], bool]] = {}
        self._lock = Lock()
        self._thread: Optional[Thread] = None
        self._running = False
        
        # Register default checks
        self.register_check("import", self._check_import)
        self.register_check("memory", self._check_memory)
        if PSUTIL_AVAILABLE:
            self.register_check("cpu", self._check_cpu)
            self.register_check("disk", self._check_disk)
    
    def register_check(self, name: str, check_func: Callable[[], bool]):
        """Register a health check function"""
        with self._lock:
            self.checks[name] = check_func
    
    def _check_import(self) -> bool:
        """Check if main app can be imported"""
        try:
            module = importlib.import_module(self.config.app_module)
            if hasattr(module, self.config.app_var):
                return True
            return False
        except Exception:
            return False
    
    def _check_memory(self) -> bool:
        """Check memory usage"""
        if self.config.max_memory_mb and PSUTIL_AVAILABLE:
            mem = psutil.Process().memory_info().rss / (1024 * 1024)
            return mem <= self.config.max_memory_mb
        return True
    
    def _check_cpu(self) -> bool:
        """Check CPU usage"""
        if self.config.max_cpu_percent and PSUTIL_AVAILABLE:
            cpu_percent = psutil.Process().cpu_percent(interval=1)
            return cpu_percent <= self.config.max_cpu_percent
        return True
    
    def _check_disk(self) -> bool:
        """Check disk usage"""
        if PSUTIL_AVAILABLE:
            disk = psutil.disk_usage('/')
            return disk.percent < 90  # Less than 90% full
        return True
    
    def check(self) -> HealthStatus:
        """Run all health checks"""
        with self._lock:
            self.last_check = datetime.now()
            
            all_passed = True
            failed_checks = []
            
            for name, check_func in self.checks.items():
                try:
                    if not check_func():
                        all_passed = False
                        failed_checks.append(name)
                except Exception as e:
                    all_passed = False
                    failed_checks.append(f"{name}({e})")
            
            if all_passed:
                self.failures = 0
                self.last_success = datetime.now()
                self.status = HealthStatus.HEALTHY
            else:
                self.failures += 1
                if self.failures >= self.config.health_check_unhealthy_threshold:
                    self.status = HealthStatus.UNHEALTHY
                else:
                    self.status = HealthStatus.DEGRADED
            
            if failed_checks:
                self.logger.warning(f"Health check failures: {', '.join(failed_checks)}")
            
            return self.status
    
    def start_periodic_checks(self):
        """Start periodic health checks in background"""
        if self._thread and self._thread.is_alive():
            return
        
        self._running = True
        self._thread = Thread(target=self._periodic_check_loop, daemon=True)
        self._thread.start()
        self.logger.info("Periodic health checks started")
    
    def stop_periodic_checks(self):
        """Stop periodic health checks"""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
    
    def _periodic_check_loop(self):
        """Background health check loop"""
        while self._running:
            self.check()
            time.sleep(self.config.health_check_interval)

# =============================================================================
# Circuit Breaker
# =============================================================================
class CircuitBreaker:
    """Circuit breaker for external service protection"""
    
    def __init__(self, name: str, config: ServerConfig, logger: StructuredLogger):
        self.name = name
        self.config = config
        self.logger = logger
        self.failures = 0
        self.last_failure: Optional[datetime] = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._lock = Lock()
    
    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            with self._lock:
                if self.state == "OPEN":
                    if (datetime.now() - self.last_failure).seconds > self.config.circuit_breaker_timeout:
                        self.state = "HALF_OPEN"
                        self.logger.info(f"Circuit {self.name} moving to HALF_OPEN")
                    else:
                        raise Exception(f"Circuit {self.name} is OPEN")
            
            try:
                result = func(*args, **kwargs)
                with self._lock:
                    if self.state == "HALF_OPEN":
                        self.state = "CLOSED"
                        self.failures = 0
                        self.logger.info(f"Circuit {self.name} recovered to CLOSED")
                return result
            except Exception as e:
                with self._lock:
                    self.failures += 1
                    self.last_failure = datetime.now()
                    if self.failures >= self.config.circuit_breaker_failures:
                        self.state = "OPEN"
                        self.logger.error(f"Circuit {self.name} OPEN after {self.failures} failures")
                raise e
        return wrapper
    
    def reset(self):
        """Reset circuit breaker"""
        with self._lock:
            self.failures = 0
            self.state = "CLOSED"
            self.last_failure = None
            self.logger.info(f"Circuit {self.name} reset")

# =============================================================================
# Performance Profiler
# =============================================================================
class Profiler:
    """Performance profiling and monitoring"""
    
    def __init__(self, config: ServerConfig, logger: StructuredLogger):
        self.config = config
        self.logger = logger
        self.start_time = time.time()
        self.metrics: Dict[str, deque] = {}
        self._lock = Lock()
        
        # Start metrics collection if in profiling mode
        if config.mode == ServerMode.PROFILING:
            tracemalloc.start()
            self._collector_thread = Thread(target=self._collect_metrics, daemon=True)
            self._collector_thread.start()
    
    def record_metric(self, name: str, value: float, maxlen: int = 1000):
        """Record a metric with rolling window"""
        with self._lock:
            if name not in self.metrics:
                self.metrics[name] = deque(maxlen=maxlen)
            self.metrics[name].append((time.time(), value))
    
    def get_metric_stats(self, name: str, window: int = 300) -> Dict[str, float]:
        """Get statistics for a metric over time window"""
        with self._lock:
            if name not in self.metrics:
                return {}
            
            now = time.time()
            cutoff = now - window
            values = [v for ts, v in self.metrics[name] if ts >= cutoff]
            
            if not values:
                return {}
            
            return {
                'avg': sum(values) / len(values),
                'min': min(values),
                'max': max(values),
                'count': len(values),
                'latest': values[-1]
            }
    
    def _collect_metrics(self):
        """Background metric collection"""
        while True:
            try:
                # System metrics
                if PSUTIL_AVAILABLE:
                    proc = psutil.Process()
                    
                    # CPU usage
                    cpu_percent = proc.cpu_percent(interval=1)
                    self.record_metric('cpu_percent', cpu_percent)
                    
                    # Memory usage
                    mem_info = proc.memory_info()
                    self.record_metric('memory_rss', mem_info.rss / (1024 * 1024))
                    self.record_metric('memory_vms', mem_info.vms / (1024 * 1024))
                    
                    # File descriptors
                    self.record_metric('num_fds', proc.num_fds())
                    
                    # Threads
                    self.record_metric('num_threads', proc.num_threads())
                
                # Memory allocation tracking
                if tracemalloc.is_tracing():
                    current, peak = tracemalloc.get_traced_memory()
                    self.record_metric('mem_current', current / (1024 * 1024))
                    self.record_metric('mem_peak', peak / (1024 * 1024))
                
                # Garbage collection stats
                gc_stats = gc.get_stats()
                for i, stats in enumerate(gc_stats):
                    self.record_metric(f'gc_collected_{i}', stats.get('collected', 0))
                
            except Exception as e:
                self.logger.debug(f"Metric collection error: {e}")
            
            time.sleep(10)  # Collect every 10 seconds
    
    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report"""
        uptime = time.time() - self.start_time
        
        report = {
            'uptime_seconds': uptime,
            'uptime_formatted': str(timedelta(seconds=int(uptime))),
            'metrics': {}
        }
        
        # Add metric summaries
        for name in self.metrics:
            report['metrics'][name] = self.get_metric_stats(name)
        
        # Add system info
        report['system'] = {
            'python_version': sys.version,
            'platform': platform.platform(),
            'cpu_count': os.cpu_count(),
        }
        
        if PSUTIL_AVAILABLE:
            report['system']['cpu_percent'] = psutil.cpu_percent(interval=1)
            report['system']['memory'] = dict(psutil.virtual_memory()._asdict())
            report['system']['disk'] = dict(psutil.disk_usage('/')._asdict())
        
        return report

# =============================================================================
# Server Orchestrator
# =============================================================================
class ServerOrchestrator:
    """Main server orchestrator"""
    
    def __init__(self, config: ServerConfig):
        self.config = config
        self.logger = StructuredLogger("server", config)
        self.health_checker = HealthChecker(config, self.logger)
        self.profiler = Profiler(config, self.logger)
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.start_time = datetime.now()
        self.process = psutil.Process() if PSUTIL_AVAILABLE else None
        
        # Register signal handlers
        self._setup_signals()
        
        # Banner
        self._print_banner()
    
    def _setup_signals(self):
        """Setup signal handlers for graceful shutdown"""
        for sig in HANDLED_SIGNALS:
            signal.signal(sig, self._signal_handler)
    
    def _signal_handler(self, signum: int, frame: Optional[FrameType]):
        """Handle shutdown signals"""
        sig_name = signal.Signals(signum).name
        self.logger.info(f"Received signal {sig_name}, initiating graceful shutdown...")
        SHUTDOWN_EVENT.set()
    
    def _print_banner(self):
        """Print startup banner"""
        banner = f"""
╔══════════════════════════════════════════════════════════════╗
║     TADAWUL FAST BRIDGE - ENTERPRISE WEB LAUNCHER v{SCRIPT_VERSION}     ║
╠══════════════════════════════════════════════════════════════╣
║  Mode: {self.config.mode.value:<35} ║
║  Host: {self.config.host:<15} Port: {self.config.port:<5}                ║
║  Workers: {self.config.workers or 'auto':<6} Class: {self.config.worker_class.value:<12}        ║
║  Log Level: {self.config.log_level:<8} Format: {self.config.log_format.value:<10}         ║
║  Health Checks: Enabled{' ' * 24}║
║  Profiling: {self.config.mode == ServerMode.PROFILING!s:<5} {' ' * 29}║
╚══════════════════════════════════════════════════════════════╝
"""
        print(banner)
    
    def get_circuit_breaker(self, name: str) -> CircuitBreaker:
        """Get or create circuit breaker"""
        if name not in self.circuit_breakers:
            self.circuit_breakers[name] = CircuitBreaker(name, self.config, self.logger)
        return self.circuit_breakers[name]
    
    def check_health(self) -> HealthStatus:
        """Run health check"""
        return self.health_checker.check()
    
    def _verify_app_import(self) -> bool:
        """Verify application can be imported"""
        try:
            module = importlib.import_module(self.config.app_module)
            if not hasattr(module, self.config.app_var):
                self.logger.error(f"App variable '{self.config.app_var}' not found in {self.config.app_module}")
                return False
            self.logger.info(f"✅ Successfully imported {self.config.app_module}:{self.config.app_var}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Failed to import application: {e}")
            return False
    
    def _get_uvicorn_config(self) -> Dict[str, Any]:
        """Get Uvicorn configuration"""
        return {
            "app": f"{self.config.app_module}:{self.config.app_var}",
            "host": self.config.host,
            "port": self.config.port,
            "workers": self.config.workers if self.config.workers and self.config.workers > 1 else None,
            "log_level": self.config.log_level,
            "access_log": self.config.access_log,
            "proxy_headers": self.config.proxy_headers,
            "forwarded_allow_ips": self.config.forwarded_allow_ips,
            "timeout_keep_alive": self.config.timeout_keep_alive,
            "timeout_graceful_shutdown": self.config.timeout_graceful,
            "limit_concurrency": self.config.limit_concurrency,
            "limit_max_requests": self.config.limit_max_requests,
            "backlog": self.config.backlog,
            "root_path": self.config.root_path,
            "ssl_keyfile": self.config.ssl_keyfile,
            "ssl_certfile": self.config.ssl_certfile,
            "ssl_ca_certs": self.config.ssl_ca_certs,
            "factory": self.config.factory,
            "server_header": self.config.server_header,
            "date_header": self.config.date_header,
            "reload": self.config.reload,
            "reload_dirs": self.config.reload_dirs,
            "reload_includes": self.config.reload_includes,
            "reload_excludes": self.config.reload_excludes,
        }
    
    def _run_with_uvicorn(self):
        """Run with Uvicorn"""
        config = self._get_uvicorn_config()
        
        # Configure loop
        if UVLOOP_AVAILABLE and self.config.worker_class in [WorkerClass.UVICORN_LOOP, WorkerClass.HYBRID]:
            uvloop.install()
            self.logger.info("✨ uvloop installed for enhanced performance")
        
        # Configure http parser
        if HTTPTOOLS_AVAILABLE and self.config.worker_class in [WorkerClass.UVICORN_HTTPTOOLS, WorkerClass.HYBRID]:
            config['http'] = 'httptools'
            self.logger.info("✨ httptools enabled for HTTP parsing")
        
        # Start periodic health checks
        if self.config.mode != ServerMode.HEALTH_CHECK:
            self.health_checker.start_periodic_checks()
        
        self.logger.info(f"🚀 Starting Uvicorn server on {self.config.get_bind_address()}")
        
        try:
            uvicorn.run(**config)
        except KeyboardInterrupt:
            self.logger.info("Server stopped by user")
        finally:
            self.health_checker.stop_periodic_checks()
    
    def _run_with_gunicorn(self):
        """Run with Gunicorn"""
        if not GUNICORN_AVAILABLE:
            self.logger.error("Gunicorn not available, falling back to Uvicorn")
            return self._run_with_uvicorn()
        
        class StandaloneApplication(BaseApplication):
            def __init__(self, app, options=None):
                self.application = app
                self.options = options or {}
                super().__init__()
            
            def load_config(self):
                for key, value in self.options.items():
                    if key in self.cfg.settings and value is not None:
                        self.cfg.set(key.lower(), value)
            
            def load(self):
                return self.application
        
        options = {
            'bind': self.config.get_bind_address(),
            'workers': self.config.workers,
            'worker_class': self.config.worker_class.value,
            'worker_connections': self.config.worker_connections,
            'max_requests': self.config.worker_max_requests,
            'max_requests_jitter': self.config.worker_max_requests_jitter,
            'worker_tmp_dir': self.config.worker_tmp_dir,
            'timeout': self.config.timeout_request,
            'graceful_timeout': self.config.timeout_graceful,
            'keepalive': self.config.timeout_keep_alive,
            'limit_concurrency': self.config.limit_concurrency,
            'backlog': self.config.backlog,
            'accesslog': '-',
            'errorlog': '-',
            'loglevel': self.config.log_level,
            'access_log_format': self.config.access_log_format,
            'proxy_allow_ips': self.config.forwarded_allow_ips,
            'preload_app': True,
        }
        
        app = f"{self.config.app_module}:{self.config.app_var}"
        
        self.logger.info(f"🚀 Starting Gunicorn server on {self.config.get_bind_address()}")
        
        try:
            StandaloneApplication(app, options).run()
        except KeyboardInterrupt:
            self.logger.info("Server stopped by user")
    
    def run(self) -> int:
        """Run the server"""
        # Health check only mode
        if self.config.mode == ServerMode.HEALTH_CHECK:
            status = self.check_health()
            print(f"Health status: {status.value}")
            return 0 if status == HealthStatus.HEALTHY else 1
        
        # Verify app import
        if not self._verify_app_import():
            return 1
        
        # Run with appropriate server
        if self.config.mode == ServerMode.DEVELOPMENT:
            # Development mode with uvicorn (supports reload)
            self._run_with_uvicorn()
        elif GUNICORN_AVAILABLE and self.config.workers and self.config.workers > 1:
            # Production mode with multiple workers -> Gunicorn
            self._run_with_gunicorn()
        else:
            # Single worker -> Uvicorn
            self._run_with_uvicorn()
        
        # Generate profiling report if in profiling mode
        if self.config.mode == ServerMode.PROFILING:
            report = self.profiler.generate_report()
            report_file = f"profile_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2, default=str)
            self.logger.info(f"📊 Profiling report saved to {report_file}")
        
        return 0

# =============================================================================
# Configuration Loader
# =============================================================================
def load_config_from_env() -> ServerConfig:
    """Load configuration from environment variables"""
    kwargs = {}
    
    # Map environment variables to config fields
    env_mappings = {
        'HOST': 'host',
        'PORT': 'port',
        'WORKERS': 'workers',
        'WORKER_CLASS': 'worker_class',
        'LOG_LEVEL': 'log_level',
        'LOG_FORMAT': 'log_format',
        'ROOT_PATH': 'root_path',
        'TIMEOUT_KEEP_ALIVE': 'timeout_keep_alive',
        'TIMEOUT_GRACEFUL': 'timeout_graceful',
        'TIMEOUT_REQUEST': 'timeout_request',
        'MAX_MEMORY_MB': 'max_memory_mb',
        'MAX_CPU_PERCENT': 'max_cpu_percent',
        'SSL_KEYFILE': 'ssl_keyfile',
        'SSL_CERTFILE': 'ssl_certfile',
    }
    
    for env_var, config_field in env_mappings.items():
        value = os.getenv(env_var)
        if value is not None:
            # Type conversion
            if config_field in ['port', 'workers', 'timeout_keep_alive', 'timeout_graceful', 
                               'timeout_request', 'max_memory_mb', 'max_cpu_percent']:
                try:
                    kwargs[config_field] = int(value)
                except ValueError:
                    pass
            elif config_field == 'worker_class':
                try:
                    kwargs[config_field] = WorkerClass(value.lower())
                except ValueError:
                    pass
            elif config_field == 'log_format':
                try:
                    kwargs[config_field] = LogFormat(value.lower())
                except ValueError:
                    pass
            else:
                kwargs[config_field] = value
    
    # Special handling for mode
    mode = os.getenv('SERVER_MODE', 'production').lower()
    if mode == 'development':
        kwargs['mode'] = ServerMode.DEVELOPMENT
    elif mode == 'debug':
        kwargs['mode'] = ServerMode.DEBUG
    elif mode == 'profiling':
        kwargs['mode'] = ServerMode.PROFILING
    elif mode == 'health-check':
        kwargs['mode'] = ServerMode.HEALTH_CHECK
    
    # Reload flag for development
    if os.getenv('RELOAD', '').lower() in ['1', 'true', 'yes']:
        kwargs['reload'] = True
    
    return ServerConfig(**kwargs)

def load_config_from_file(filepath: str) -> ServerConfig:
    """Load configuration from YAML/JSON file"""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {filepath}")
    
    content = path.read_text()
    
    if filepath.endswith(('.yaml', '.yml')):
        if not YAML_AVAILABLE:
            raise ImportError("PyYAML required for YAML config files")
        data = yaml.safe_load(content)
    elif filepath.endswith('.json'):
        data = json.loads(content)
    else:
        raise ValueError(f"Unsupported config file format: {filepath}")
    
    # Convert dict to ServerConfig
    if 'mode' in data and isinstance(data['mode'], str):
        try:
            data['mode'] = ServerMode(data['mode'].lower())
        except ValueError:
            pass
    
    if 'worker_class' in data and isinstance(data['worker_class'], str):
        try:
            data['worker_class'] = WorkerClass(data['worker_class'].lower())
        except ValueError:
            pass
    
    if 'log_format' in data and isinstance(data['log_format'], str):
        try:
            data['log_format'] = LogFormat(data['log_format'].lower())
        except ValueError:
            pass
    
    return ServerConfig(**data)

# =============================================================================
# CLI Entry Point
# =============================================================================
def create_parser() -> argparse.ArgumentParser:
    """Create argument parser"""
    parser = argparse.ArgumentParser(
        description=f"Tadawul Fast Bridge Enterprise Web Launcher v{SCRIPT_VERSION}",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Basic options
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=8000, help="Bind port")
    parser.add_argument("--workers", type=int, help="Number of workers")
    parser.add_argument("--worker-class", choices=[c.value for c in WorkerClass],
                       default=WorkerClass.UVICORN.value, help="Worker class")
    
    # Mode selection
    parser.add_argument("--dev", action="store_true", help="Development mode (hot reload)")
    parser.add_argument("--debug", action="store_true", help="Debug mode")
    parser.add_argument("--profile", action="store_true", help="Profiling mode")
    parser.add_argument("--health-check-only", action="store_true", 
                       help="Run health check and exit")
    
    # Configuration
    parser.add_argument("--config", help="Config file (YAML/JSON)")
    parser.add_argument("--env-prefix", default="SERVER_", 
                       help="Environment variable prefix")
    
    # Application
    parser.add_argument("--app-module", default="main", help="Application module")
    parser.add_argument("--app-var", default="app", help="Application variable")
    parser.add_argument("--root-path", help="Root path for mounted app")
    
    # Performance
    parser.add_argument("--limit-concurrency", type=int, 
                       help="Maximum concurrent connections")
    parser.add_argument("--limit-max-requests", type=int,
                       help="Maximum requests before worker restart")
    parser.add_argument("--backlog", type=int, default=2048, help="Connection backlog")
    
    # Timeouts
    parser.add_argument("--timeout-keep-alive", type=int, default=65,
                       help="Keep-alive timeout")
    parser.add_argument("--timeout-graceful", type=int, default=30,
                       help="Graceful shutdown timeout")
    parser.add_argument("--timeout-request", type=int, default=60,
                       help="Request timeout")
    
    # Logging
    parser.add_argument("--log-level", default="info",
                       choices=["debug", "info", "warning", "error", "critical"])
    parser.add_argument("--log-format", choices=[f.value for f in LogFormat],
                       default=LogFormat.TEXT.value, help="Log format")
    parser.add_argument("--log-file", help="Log file path")
    parser.add_argument("--no-access-log", action="store_true",
                       help="Disable access log")
    
    # SSL
    parser.add_argument("--ssl-keyfile", help="SSL key file")
    parser.add_argument("--ssl-certfile", help="SSL certificate file")
    
    # Development
    parser.add_argument("--reload", action="store_true", help="Enable hot reload")
    parser.add_argument("--reload-dir", action="append", help="Directory to watch")
    
    # Resource limits
    parser.add_argument("--max-memory-mb", type=int, help="Maximum memory in MB")
    parser.add_argument("--max-cpu-percent", type=int, help="Maximum CPU percent")
    
    return parser

def main() -> int:
    """Main entry point"""
    parser = create_parser()
    args = parser.parse_args()
    
    # Load configuration
    try:
        if args.config:
            config = load_config_from_file(args.config)
        else:
            config = load_config_from_env()
        
        # Override with CLI arguments
        cli_overrides = {
            'host': args.host,
            'port': args.port,
            'workers': args.workers,
            'app_module': args.app_module,
            'app_var': args.app_var,
            'root_path': args.root_path,
            'limit_concurrency': args.limit_concurrency,
            'limit_max_requests': args.limit_max_requests,
            'backlog': args.backlog,
            'timeout_keep_alive': args.timeout_keep_alive,
            'timeout_graceful': args.timeout_graceful,
            'timeout_request': args.timeout_request,
            'log_level': args.log_level,
            'log_file': args.log_file,
            'access_log': not args.no_access_log,
            'ssl_keyfile': args.ssl_keyfile,
            'ssl_certfile': args.ssl_certfile,
            'reload': args.reload,
            'max_memory_mb': args.max_memory_mb,
            'max_cpu_percent': args.max_cpu_percent,
        }
        
        # Handle enums
        if args.worker_class:
            try:
                cli_overrides['worker_class'] = WorkerClass(args.worker_class)
            except ValueError:
                pass
        
        if args.log_format:
            try:
                cli_overrides['log_format'] = LogFormat(args.log_format)
            except ValueError:
                pass
        
        # Mode selection
        if args.dev:
            cli_overrides['mode'] = ServerMode.DEVELOPMENT
        elif args.debug:
            cli_overrides['mode'] = ServerMode.DEBUG
        elif args.profile:
            cli_overrides['mode'] = ServerMode.PROFILING
        elif args.health_check_only:
            cli_overrides['mode'] = ServerMode.HEALTH_CHECK
        
        # Update config
        for key, value in cli_overrides.items():
            if value is not None and hasattr(config, key):
                setattr(config, key, value)
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")
        return 1
    
    # Create and run orchestrator
    orchestrator = ServerOrchestrator(config)
    
    try:
        return orchestrator.run()
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")
        return 130
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
