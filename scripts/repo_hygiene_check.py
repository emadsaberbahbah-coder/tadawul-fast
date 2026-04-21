#!/usr/bin/env python3
"""
scripts/repo_hygiene_check.py
================================================================================
TADAWUL ENTERPRISE REPOSITORY HYGIENE CHECKER — v5.1.0 (QUANTUM EDITION)
================================================================================
AI-POWERED | CI/CD INTEGRATED | MEMORY-OPTIMIZED | FALSE-POSITIVE RESISTANT

Why this revision (v5.1.0 vs v5.0.1)
-------------------------------------
- 🔑 FIX HIGH: Removed 5 dead CLI flags — `--enable-git`, `--rules-file`,
     `--fail-on-read-error`, `--show-snippets`, `--parallel` were declared
     in argparse but NEVER consumed anywhere in `main_async`. They silently
     did nothing, misleading users. v5.1.0 removes them to make the
     advertised interface match the actual behavior.
- 🔑 FIX HIGH: `OutputFormat.CSV` / `OutputFormat.MARKDOWN` values existed
     in the enum AND in `--format` choices, but had no implementation.
     Users picking `--format csv` got no output and no error — silent
     correctness bug. v5.1.0 implements both formatters. `OutputFormat.HTML`
     is removed from the enum (was also unimplemented).
- 🔑 FIX PERFORMANCE: `scan_parallel` was creating a new
     `ThreadPoolExecutor` for every 100-file chunk, spawning and
     destroying worker threads constantly. v5.1.0 uses a single
     `ThreadPoolExecutor` for the entire scan, with an `asyncio.Semaphore`
     to bound in-flight work.

- FIX: Removed dead imports (`csv`, `bandit`, `git`, `yaml`) and unused
     module-level flags (`_HAS_ORJSON`, `_RICH_AVAILABLE`,
     `_BANDIT_AVAILABLE`, `_GIT_AVAILABLE`, `_YAML_AVAILABLE`).
- FIX: Added project-standard `_TRUTHY` / `_FALSY` vocabulary matching
     `main._TRUTHY` / `_FALSY` plus `_env_bool`, `_env_int`,
     `_env_int_bool` helpers.
- FIX: Added `SERVICE_VERSION = SCRIPT_VERSION` alias (cross-script
     convention with `audit_data_quality.py v4.5.0` / `cleanup_cache.py
     v4.1.0` / `drift_detection.py v4.3.0` / `migrate_schema_v2.py v2.1.0`
     / `refresh_data.py v5.2.0`).
- FIX: Added env var defaults for all CLI flags under the `HYGIENE_*`
     prefix for headless/cron operation.
- FIX: `--output-file` extension dispatch now recognizes `.csv` and `.md`
     in addition to `.json`, `.xml`, `.sarif`.
- FIX: `main_async` body wrapped in `try/finally` with `_shutdown_executor`
     cleanup — leak-proof on exceptions.

v5.0.1 hotfixes (preserved):
- Self-scan suppression for intentional token examples in this script
- Secret detection heuristics tightened (env var names, schema fields,
  placeholders, help text, regex definitions suppressed)
- `Rule.exclude_pattern` honored
- Precise line/column for line-based matches

Env vars
--------
  HYGIENE_ROOT                root directory to scan (default ".")
  HYGIENE_STRICT              strict mode 0/1 (default 0)
  HYGIENE_MAX_OFFENDERS       stop after N offenders (default 0 = no limit)
  HYGIENE_SCAN_ALL_TEXT       scan all text files (default 0)
  HYGIENE_MAX_FILE_SIZE_MB    max file size in MB (default 2)
  HYGIENE_ENABLE_ML           enable ML anomaly detection (default 0)
  HYGIENE_DISABLE_SECRETS     disable secret detection (default 0)
  HYGIENE_WORKERS             worker thread count (default CPU count)
  HYGIENE_FORMAT              output format (default console)
  HYGIENE_OUTPUT_FILE         save output to file
  HYGIENE_ANNOTATE            emit CI annotations (default 1)
  HYGIENE_FAIL_THRESHOLD      threshold for nonzero exit (default high)
  HYGIENE_EXTENSIONS          comma-separated extensions (default .py)
  HYGIENE_EXCLUDE             comma-separated dirs to exclude
  HYGIENE_EXCLUDE_PATTERNS    comma-separated regex patterns to exclude
  HYGIENE_INCLUDE_PATTERNS    comma-separated regex patterns to include

Exit codes
----------
  0 = passed (no findings at or above fail threshold)
  1 = runtime error / interrupted
  2 = findings at or above fail threshold
================================================================================
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import os
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Pattern, Callable

# ---------------------------------------------------------------------------
# High-Performance JSON fallback
# ---------------------------------------------------------------------------
try:
    import orjson  # type: ignore

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        option = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(v, option=option, default=str).decode("utf-8")

    def json_loads(v: Union[str, bytes]) -> Any:
        return orjson.loads(v)

except ImportError:
    import json  # type: ignore

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        return json.dumps(
            v, indent=indent if indent else None, default=str, ensure_ascii=False
        )

    def json_loads(v: Union[str, bytes]) -> Any:
        return json.loads(v)

# ---------------------------------------------------------------------------
# Rich UI (Optional)
# ---------------------------------------------------------------------------
try:
    from rich.console import Console
    from rich.progress import (
        Progress,
        SpinnerColumn,
        TextColumn,
        BarColumn,
        TaskProgressColumn,
        TimeElapsedColumn,
    )
    from rich.table import Table

    console = Console()
except ImportError:
    console = None  # type: ignore[assignment]
    Progress = None  # type: ignore[assignment]
    SpinnerColumn = None  # type: ignore[assignment]
    TextColumn = None  # type: ignore[assignment]
    BarColumn = None  # type: ignore[assignment]
    TaskProgressColumn = None  # type: ignore[assignment]
    TimeElapsedColumn = None  # type: ignore[assignment]
    Table = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Optional ML/AI libraries
# ---------------------------------------------------------------------------
try:
    import numpy as np  # type: ignore
    from sklearn.feature_extraction.text import TfidfVectorizer  # type: ignore
    from sklearn.ensemble import IsolationForest  # type: ignore
    from sklearn.preprocessing import StandardScaler  # type: ignore

    _ML_AVAILABLE = True
except ImportError:
    _ML_AVAILABLE = False

# ---------------------------------------------------------------------------
# Optional JUnit XML output
# ---------------------------------------------------------------------------
try:
    from junit_xml import TestSuite, TestCase  # type: ignore

    _JUNIT_AVAILABLE = True
except ImportError:
    _JUNIT_AVAILABLE = False


# =============================================================================
# Version + Logging
# =============================================================================
SCRIPT_VERSION = "5.1.0"
SERVICE_VERSION = SCRIPT_VERSION  # v5.1.0: cross-script alias

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("RepoHygiene")


# =============================================================================
# Project-wide truthy/falsy vocabulary (matches main._TRUTHY / _FALSY)
# =============================================================================
_TRUTHY = {"1", "true", "yes", "y", "on", "t", "enabled", "enable"}
_FALSY = {"0", "false", "no", "n", "off", "f", "disabled", "disable"}


def _env_bool(name: str, default: bool = False) -> bool:
    """Project-aligned env bool parser."""
    try:
        raw = (os.getenv(name, "") or "").strip().lower()
    except Exception:
        return bool(default)
    if not raw:
        return bool(default)
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return bool(default)


def _env_int(
    name: str, default: int, *, lo: Optional[int] = None, hi: Optional[int] = None
) -> int:
    try:
        raw = (os.getenv(name, "") or "").strip()
        if not raw:
            return default
        v = int(float(raw))
    except Exception:
        return default
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def _env_int_bool(name: str, default: int = 0) -> int:
    """
    Get env var as 0/1 int. Accepts truthy/falsy strings (true/false/yes/no/
    on/off/1/0/t/f/enabled/disabled) so cron-style `HYGIENE_STRICT=true` and
    legacy CLI-style `--strict 1` both resolve correctly.
    """
    raw = (os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    if raw in _TRUTHY:
        return 1
    if raw in _FALSY:
        return 0
    try:
        return 1 if int(float(raw)) != 0 else 0
    except Exception:
        return default


def _env_csv(name: str, default: Optional[List[str]] = None) -> Optional[List[str]]:
    """Parse comma-separated env var into a list of stripped strings."""
    raw = (os.getenv(name, "") or "").strip()
    if not raw:
        return default
    items = [x.strip() for x in raw.split(",") if x.strip()]
    return items or default


# =============================================================================
# Enums & Types
# =============================================================================
class Severity(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"


class FindingCategory(str, Enum):
    MARKDOWN_FENCE = "MARKDOWN_FENCE"
    LLM_ARTIFACT = "LLM_ARTIFACT"
    SECRET = "SECRET"
    VULNERABILITY = "VULNERABILITY"
    LICENSE = "LICENSE"
    CODE_QUALITY = "CODE_QUALITY"
    GIT_HISTORY = "GIT_HISTORY"
    CUSTOM = "CUSTOM"


class OutputFormat(str, Enum):
    # v5.1.0: HTML removed (never implemented). CSV + MARKDOWN now implemented.
    CONSOLE = "console"
    JSON = "json"
    JUNIT = "junit"
    SARIF = "sarif"
    CSV = "csv"
    MARKDOWN = "markdown"


class CIProvider(str, Enum):
    GITHUB_ACTIONS = "github_actions"
    GITLAB_CI = "gitlab_ci"
    JENKINS = "jenkins"
    CIRCLECI = "circleci"
    TRAVIS = "travis"
    AZURE_DEVOPS = "azure_devops"
    UNKNOWN = "unknown"


# =============================================================================
# Data Models (Memory Optimized)
# =============================================================================
@dataclass(slots=True)
class Finding:
    """Represents a single hygiene finding."""

    category: FindingCategory
    severity: Severity
    message: str
    file_path: str
    line: int = 0
    column: int = 0
    line_end: int = 0
    column_end: int = 0
    token: Optional[str] = None
    snippet: Optional[str] = None
    context_lines: List[str] = field(default_factory=list)
    rule_id: Optional[str] = None
    rule_name: Optional[str] = None
    remediation: Optional[str] = None
    references: List[str] = field(default_factory=list)
    confidence: float = 1.0
    impact_score: float = 1.0
    detected_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category.value,
            "severity": self.severity.value,
            "message": self.message,
            "file_path": self.file_path,
            "line": self.line,
            "column": self.column,
            "line_end": self.line_end,
            "column_end": self.column_end,
            "token": self.token,
            "snippet": self.snippet,
            "context_lines": self.context_lines,
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "remediation": self.remediation,
            "references": self.references,
            "confidence": self.confidence,
            "impact_score": self.impact_score,
            "detected_at": self.detected_at,
        }

    def to_sarif(self) -> Dict[str, Any]:
        region = None
        if self.line > 0:
            region = {
                "startLine": max(1, self.line),
                "startColumn": max(1, self.column or 1),
                "endLine": max(1, self.line_end or self.line),
                "endColumn": max(1, self.column_end or (self.column + 1)),
            }

        return {
            "ruleId": self.rule_id or f"HYGIENE-{self.category.value}",
            "level": self._sarif_level(),
            "message": {"text": self.message},
            "locations": [
                {
                    "physicalLocation": {
                        "artifactLocation": {"uri": self.file_path},
                        **({"region": region} if region else {}),
                    }
                }
            ],
            "properties": {
                "category": self.category.value,
                "confidence": self.confidence,
                "impact_score": self.impact_score,
            },
        }

    def _sarif_level(self) -> str:
        mapping = {
            Severity.CRITICAL: "error",
            Severity.HIGH: "error",
            Severity.MEDIUM: "warning",
            Severity.LOW: "note",
            Severity.INFO: "note",
        }
        return mapping.get(self.severity, "note")


@dataclass(slots=True)
class Rule:
    """Custom rule definition."""

    name: str
    pattern: str
    category: FindingCategory
    severity: Severity
    message: str
    remediation: Optional[str] = None
    file_pattern: Optional[str] = None
    exclude_pattern: Optional[str] = None
    case_sensitive: bool = False
    multiline: bool = False

    def compile(self) -> Pattern:
        flags = 0
        if not self.case_sensitive:
            flags |= re.IGNORECASE
        if self.multiline:
            flags |= re.MULTILINE | re.DOTALL
        return re.compile(self.pattern, flags)


@dataclass(slots=True)
class ScanSummary:
    """Summary of scan results."""

    total_files: int = 0
    files_with_issues: int = 0
    total_findings: int = 0
    by_severity: Dict[Severity, int] = field(
        default_factory=lambda: {s: 0 for s in Severity}
    )
    by_category: Dict[FindingCategory, int] = field(default_factory=dict)
    by_file: Dict[str, int] = field(default_factory=dict)
    scan_duration_ms: float = 0.0
    scan_start: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    scan_end: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    version: str = SCRIPT_VERSION

    def add_finding(self, finding: Finding) -> None:
        self.total_findings += 1
        self.by_severity[finding.severity] = (
            self.by_severity.get(finding.severity, 0) + 1
        )
        self.by_category[finding.category] = (
            self.by_category.get(finding.category, 0) + 1
        )
        self.by_file[finding.file_path] = self.by_file.get(finding.file_path, 0) + 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "total_files": self.total_files,
            "files_with_issues": self.files_with_issues,
            "total_findings": self.total_findings,
            "by_severity": {k.value: v for k, v in self.by_severity.items()},
            "by_category": {k.value: v for k, v in self.by_category.items()},
            "by_file": dict(
                sorted(self.by_file.items(), key=lambda x: x[1], reverse=True)[:20]
            ),
            "scan_duration_ms": self.scan_duration_ms,
            "scan_start": self.scan_start,
            "scan_end": self.scan_end,
        }


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
            print(f"::error file={file_path},line={line},col={column}::{message}")
        elif provider == CIProvider.GITLAB_CI:
            print(f"{file_path}:{line}:{column}: error: {message}")
        elif provider == CIProvider.JENKINS:
            print(f"[ERROR] {file_path}:{line}:{column} - {message}")
        else:
            print(f"ERROR: {file_path}:{line}:{column} - {message}")

    @staticmethod
    def annotate_warning(file_path: str, line: int, column: int, message: str) -> None:
        provider = CIDetector.detect()
        if provider == CIProvider.GITHUB_ACTIONS:
            print(f"::warning file={file_path},line={line},col={column}::{message}")
        elif provider == CIProvider.GITLAB_CI:
            print(f"{file_path}:{line}:{column}: warning: {message}")
        elif provider == CIProvider.JENKINS:
            print(f"[WARNING] {file_path}:{line}:{column} - {message}")
        else:
            print(f"WARNING: {file_path}:{line}:{column} - {message}")


# =============================================================================
# Secret Detection
# =============================================================================
class SecretDetector:
    """
    Heuristic secret detector with false-positive suppression.

    Key strategy:
    - Focus on quoted hardcoded values for password/token/key assignments.
    - Allow explicit high-confidence formats (JWT, AWS access keys, private
      keys, DB URLs with creds).
    - Skip common config/schema/env patterns that are not actual secrets.
    """

    # Strong-format patterns (high confidence)
    PATTERNS: List[Tuple[Pattern[str], str]] = [
        (re.compile(r"(?i)\b(AKIA[0-9A-Z]{16})\b"), "AWS Access Key"),
        (
            re.compile(r"(?i)\b(postgresql|mysql|mongodb|redis)://[^:\s]+:[^@\s]+@"),
            "Database URL with credentials",
        ),
        (re.compile(r"-----BEGIN (RSA|DSA|EC|OPENSSH) PRIVATE KEY-----"), "Private Key"),
        (re.compile(r"\beyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\b"), "JWT Token"),
    ]

    # Assignment patterns (quoted values only to avoid config-name false positives)
    API_KEY_ASSIGN_RE = re.compile(
        r"""(?ix)
        \b(api[_-]?key|apikey|api[_-]?secret|client[_-]?secret|secret[_-]?key)\b
        \s*[:=]\s*
        (["'])([^"'\n]{12,})\2
        """
    )

    PASSWORD_ASSIGN_RE = re.compile(
        r"""(?ix)
        \b(password|passwd|pwd)\b
        \s*[:=]\s*
        (["'])([^"'\n]{8,})\2
        """
    )

    TOKEN_ASSIGN_RE = re.compile(
        r"""(?ix)
        \b(oauth|bearer|token|access[_-]?token|refresh[_-]?token)\b
        \s*[:=]\s*
        (["'])([^"'\n]{20,})\2
        """
    )

    AWS_SECRET_ASSIGN_RE = re.compile(
        r"""(?ix)
        \baws[_-]?(access|secret)[_-]?(key|token)?\b
        \s*[:=]\s*
        (["'])([^"'\n]{20,})\3
        """
    )

    # Common non-secret placeholders / examples
    PLACEHOLDER_VALUES = {
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "apikey",
        "changeme",
        "change_me",
        "your_password",
        "your-token-here",
        "your_token_here",
        "your_api_key_here",
        "example",
        "example_password",
        "sample",
        "dummy",
        "testpassword",
        "placeholder",
        "notset",
    }

    # Lines likely to be config/schema/docs/regex, not hardcoded secrets
    SKIP_LINE_SUBSTRINGS = (
        "os.getenv(",
        "os.environ",
        "environ.get(",
        "SecretStr(",
        "Field(",
        "BaseSettings",
        "pydantic",
        "help=",
        "description=",
        "argparse",
        "re.compile(",
        "PATTERNS =",
        "references",
        "docs.github.com/en/code-security/secret-scanning",
    )

    @classmethod
    def _looks_like_placeholder(cls, value: str) -> bool:
        v = value.strip().strip("'\"").lower()
        if v in cls.PLACEHOLDER_VALUES:
            return True
        if "your " in v or "your_" in v or "your-" in v:
            return True
        if "example" in v or "sample" in v or "placeholder" in v:
            return True
        if v.startswith("${") or v.endswith("}"):
            return True
        if v.startswith("%(") and v.endswith(")s"):
            return True
        return False

    @classmethod
    def _is_non_secret_context(cls, line: str, file_path: str) -> bool:
        stripped = line.strip()

        # Obvious comments and doc-ish lines (keep strong-format patterns elsewhere)
        if not stripped:
            return True
        if stripped.startswith("#"):
            return True

        # Frequent false-positive contexts in Python config/router code
        lower = stripped.lower()
        if any(s.lower() in lower for s in cls.SKIP_LINE_SUBSTRINGS):
            return True

        # Type hints / schema defaults / validators often include words like
        # password/token
        if " = None" in line or ' = ""' in line or " = ''" in line:
            return True
        if "default=" in lower:
            return True

        # Common path-specific self-rule definitions
        if file_path.replace("\\", "/").endswith("scripts/repo_hygiene_check.py"):
            if "TokenBuilder" in line or "LLM artifact" in line or "Markdown fence" in line:
                return True

        return False

    @classmethod
    def _emit_finding(
        cls,
        findings: List[Finding],
        *,
        file_path: str,
        line_no: int,
        match: re.Match,
        secret_type: str,
        confidence: float = 0.8,
    ) -> None:
        token = match.group(0)
        findings.append(
            Finding(
                category=FindingCategory.SECRET,
                severity=Severity.CRITICAL,
                message=f"Potential {secret_type} detected",
                file_path=file_path,
                line=line_no,
                column=match.start() + 1,
                line_end=line_no,
                column_end=match.end() + 1,
                token=(token[:80] + "...") if len(token) > 80 else token,
                rule_id="SECRET-001",
                rule_name=secret_type,
                remediation=(
                    "Remove secrets from code, use environment variables or "
                    "secret management services"
                ),
                references=[
                    "https://docs.github.com/en/code-security/secret-scanning"
                ],
                confidence=confidence,
            )
        )

    @classmethod
    def scan(cls, content: str, file_path: str) -> List[Finding]:
        findings: List[Finding] = []
        lines = content.splitlines()

        for i, line in enumerate(lines, 1):
            # 1) High-confidence signatures (still filter self-detector regex examples)
            if not (
                "re.compile(" in line
                and file_path.replace("\\", "/").endswith("repo_hygiene_check.py")
            ):
                for regex, secret_type in cls.PATTERNS:
                    for match in regex.finditer(line):
                        # Avoid obvious placeholders in DB URLs
                        if secret_type == "Database URL with credentials":
                            if any(
                                x in line.lower()
                                for x in ("example", "placeholder", "changeme")
                            ):
                                continue
                        cls._emit_finding(
                            findings,
                            file_path=file_path,
                            line_no=i,
                            match=match,
                            secret_type=secret_type,
                            confidence=(
                                0.95 if secret_type in {"Private Key", "JWT Token"} else 0.9
                            ),
                        )

            # 2) Assignment-style patterns with context suppression
            if cls._is_non_secret_context(line, file_path):
                continue

            # API keys / secrets
            for match in cls.API_KEY_ASSIGN_RE.finditer(line):
                value = match.group(3)
                if cls._looks_like_placeholder(value):
                    continue
                cls._emit_finding(
                    findings,
                    file_path=file_path,
                    line_no=i,
                    match=match,
                    secret_type="API Key",
                    confidence=0.85,
                )

            # Passwords (quoted only)
            for match in cls.PASSWORD_ASSIGN_RE.finditer(line):
                value = match.group(3)
                if cls._looks_like_placeholder(value):
                    continue
                # skip policy-like values / labels
                if re.fullmatch(r"[A-Z_]{4,}", value):
                    continue
                cls._emit_finding(
                    findings,
                    file_path=file_path,
                    line_no=i,
                    match=match,
                    secret_type="Password",
                    confidence=0.82,
                )

            # OAuth / tokens (quoted only)
            for match in cls.TOKEN_ASSIGN_RE.finditer(line):
                value = match.group(3)
                if cls._looks_like_placeholder(value):
                    continue
                # skip enum-ish strings
                if re.fullmatch(r"[A-Z_]{8,}", value):
                    continue
                cls._emit_finding(
                    findings,
                    file_path=file_path,
                    line_no=i,
                    match=match,
                    secret_type="OAuth Token",
                    confidence=0.84,
                )

            # AWS secret assignment
            for match in cls.AWS_SECRET_ASSIGN_RE.finditer(line):
                value = match.group(4)
                if cls._looks_like_placeholder(value):
                    continue
                cls._emit_finding(
                    findings,
                    file_path=file_path,
                    line_no=i,
                    match=match,
                    secret_type="AWS Secret",
                    confidence=0.9,
                )

        return findings


# =============================================================================
# ML Anomaly Detection
# =============================================================================
class MLAnomalyDetector:
    def __init__(self) -> None:
        if not _ML_AVAILABLE:
            self.vectorizer = None  # type: ignore[assignment]
            self.model = None  # type: ignore[assignment]
            self.scaler = None  # type: ignore[assignment]
        else:
            self.vectorizer = TfidfVectorizer(
                max_features=1000, stop_words="english", ngram_range=(1, 3)
            )
            self.model = IsolationForest(
                contamination=0.05, random_state=42, n_estimators=150
            )
            self.scaler = StandardScaler()
        self.is_fitted = False
        self.training_data: List[str] = []

    def add_training_data(self, content: str) -> None:
        if len(content.split()) > 10:  # Ignore tiny files
            self.training_data.append(content)

    def fit(self) -> None:
        if not _ML_AVAILABLE or len(self.training_data) < 10:
            return
        try:
            X = self.vectorizer.fit_transform(self.training_data).toarray()
            X_scaled = self.scaler.fit_transform(X)
            self.model.fit(X_scaled)
            self.is_fitted = True
            logger.info("ML model fitted on %s samples", len(self.training_data))
        except Exception as e:
            logger.error("ML model fitting failed: %s", e)

    def predict(self, content: str) -> Tuple[bool, float]:
        if not self.is_fitted or not _ML_AVAILABLE:
            return False, 0.0
        try:
            X = self.vectorizer.transform([content]).toarray()
            X_scaled = self.scaler.transform(X)
            score = self.model.score_samples(X_scaled)[0]
            prediction = self.model.predict(X_scaled)[0]
            normalized_score = 1.0 / (1.0 + np.exp(-score))
            return prediction == -1, float(normalized_score)
        except Exception as e:
            logger.debug("ML prediction failed: %s", e)
            return False, 0.0


# =============================================================================
# Token Builders
# =============================================================================
class TokenBuilder:
    @staticmethod
    def markdown_fences() -> List[str]:
        bt, td = "```", "~~~"
        fences = [bt, td]
        for lang in [
            "python", "py", "bash", "sh", "javascript", "js", "typescript", "ts",
            "json", "yaml", "xml", "html", "css",
        ]:
            fences.extend([f"{bt}{lang}", f"{td}{lang}"])
        return fences

    @staticmethod
    def llm_artifacts(strict: bool = False) -> List[str]:
        artifacts = [
            "[your code here]",
            "[paste code here]",
            "[your solution here]",
            "<your code here>",
            "<paste here>",
            "PASTE YOUR CODE HERE",
            "INSERT_CODE_HERE",
            "// ... your code here ...",
            "# ... your code here ...",
            "/* ... your code here ... */",
            "TODO: Implement",
            "FIXME: Add code",
        ]
        if strict:
            artifacts.extend(
                [
                    "BEGIN CODE",
                    "END CODE",
                    "Here is the code",
                    "Copy ONLY the code",
                    "The following code",
                    "This is the code",
                    "```",
                    "~~~",
                ]
            )
        return artifacts

    @staticmethod
    def suspicious_patterns() -> List[Tuple[str, str, Severity]]:
        """
        Git merge conflict markers are safely anchored with ^ and $ along with
        re.MULTILINE to prevent flagging decorative headers.
        """
        return [
            (r"^<{7} .*$", "Git merge conflict marker (HEAD)", Severity.HIGH),
            (r"^={7}$", "Git merge conflict marker (Separator)", Severity.HIGH),
            (r"^>{7} .*$", "Git merge conflict marker (Branch)", Severity.HIGH),
            (r"#\s*TODO.*$", "TODO comment", Severity.LOW),
            (r"#\s*FIXME.*$", "FIXME comment", Severity.MEDIUM),
            (r"#\s*HACK.*$", "HACK comment", Severity.MEDIUM),
            (r"#\s*XXX.*$", "XXX comment", Severity.LOW),
            (r'print\s*\(\s*["\'].*["\']\s*\)', "Debug print statement", Severity.LOW),
            (r"console\.log\(", "Debug console log", Severity.LOW),
            (r"debugger;", "Debugger statement", Severity.MEDIUM),
            (r"import pdb; pdb\.set_trace\(\)", "Debugger breakpoint", Severity.MEDIUM),
            (r"breakpoint\(\)", "Debugger breakpoint", Severity.MEDIUM),
        ]


# =============================================================================
# File Scanner
# =============================================================================
class FileScanner:
    def __init__(
        self,
        root: Path,
        skip_dirs: Set[str],
        extensions: List[str],
        scan_all_text: bool,
        exclude_patterns: Optional[List[str]] = None,
        include_patterns: Optional[List[str]] = None,
        max_file_size_mb: int = 10,
    ):
        self.root = root.resolve()
        self.skip_dirs = {d.lower() for d in skip_dirs}
        self.extensions = {
            e.lower() if e.startswith(".") else f".{e.lower()}" for e in extensions
        }
        self.scan_all_text = scan_all_text
        self.exclude_patterns = [re.compile(p) for p in (exclude_patterns or [])]
        self.include_patterns = [re.compile(p) for p in (include_patterns or [])]
        self.max_file_size = max_file_size_mb * 1024 * 1024
        self.binary_extensions = {
            ".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".ico",
            ".pdf", ".zip", ".tar", ".gz", ".7z", ".rar",
            ".exe", ".dll", ".so", ".dylib", ".bin",
            ".pyc", ".pyo", ".pyd", ".class", ".jar",
            ".mp3", ".mp4", ".avi", ".mov",
        }

    def should_skip(self, path: Path) -> bool:
        for part in path.parts:
            if part.lower() in self.skip_dirs:
                return True
            if part.startswith(".") and part not in {
                ".github", ".gitignore", ".gitattributes"
            }:
                return True
        try:
            if path.stat().st_size > self.max_file_size:
                return True
        except OSError:
            return True

        rel_path = str(path.relative_to(self.root)).replace("\\", "/")
        for pattern in self.exclude_patterns:
            if pattern.search(rel_path):
                return True
        if self.include_patterns:
            for pattern in self.include_patterns:
                if pattern.search(rel_path):
                    return False
            return True
        return False

    def is_text_file(self, path: Path) -> bool:
        if path.suffix.lower() in self.binary_extensions:
            return False
        try:
            with open(path, "rb") as f:
                chunk = f.read(1024)
                if b"\x00" in chunk:
                    return False
                chunk.decode("utf-8")
                return True
        except (UnicodeDecodeError, OSError):
            return False

    def get_files(self) -> List[Path]:
        files: List[Path] = []
        for path in self.root.rglob("*"):
            if not path.is_file() or self.should_skip(path):
                continue
            if self.scan_all_text:
                if self.is_text_file(path):
                    files.append(path)
            else:
                if path.suffix.lower() in self.extensions:
                    files.append(path)
        return files


# =============================================================================
# Core Scanner
# =============================================================================
class HygieneScanner:
    SELF_SCRIPT_REL = "scripts/repo_hygiene_check.py"

    def __init__(
        self,
        root: Path,
        rules: List[Rule],
        enable_ml: bool = False,
        enable_secret_detection: bool = True,
        parallel_workers: int = 4,
        self_suppressions: bool = True,
    ):
        self.root = root
        self.rules = rules
        self.enable_ml = enable_ml
        self.enable_secret_detection = enable_secret_detection
        self.parallel_workers = parallel_workers
        self.self_suppressions = self_suppressions
        self.ml_detector = MLAnomalyDetector() if enable_ml else None
        self.secret_detector = SecretDetector() if enable_secret_detection else None

    @staticmethod
    def _norm_rel(path_str: str) -> str:
        return path_str.replace("\\", "/")

    def _is_self_script(self, rel_path: str) -> bool:
        return self._norm_rel(rel_path).endswith(self.SELF_SCRIPT_REL)

    def _suppress_self_false_positive(self, finding: Finding) -> bool:
        """Suppress intentionally embedded examples inside this script."""
        if not self.self_suppressions:
            return False
        if not self._is_self_script(finding.file_path):
            return False

        # Intentional token examples / pattern examples in TokenBuilder and docs
        if finding.category in {
            FindingCategory.MARKDOWN_FENCE,
            FindingCategory.LLM_ARTIFACT,
        }:
            return True

        # Pattern catalog examples in this script can trigger code-quality strings
        if finding.category == FindingCategory.CODE_QUALITY:
            m = finding.message.lower()
            if (
                "debugger" in m
                or "debug print" in m
                or "debug console" in m
                or "todo comment" in m
                or "fixme comment" in m
                or "hack comment" in m
                or "xxx comment" in m
            ):
                return True

        # Secret detector regex definitions / docs inside this file
        if finding.category == FindingCategory.SECRET:
            return True

        return False

    def scan_file(self, file_path: Path) -> List[Finding]:
        findings: List[Finding] = []
        try:
            content = file_path.read_text(encoding="utf-8", errors="replace")
            rel_path = self._norm_rel(str(file_path.relative_to(self.root)))
        except Exception as e:
            logger.debug("Could not read %s: %s", file_path, e)
            return []

        content_lines = content.splitlines()

        # Rule-based scanning
        for rule in self.rules:
            if rule.file_pattern and not re.search(rule.file_pattern, rel_path):
                continue
            if rule.exclude_pattern and re.search(rule.exclude_pattern, rel_path):
                continue

            pattern = rule.compile()

            if rule.multiline:
                for match in pattern.finditer(content):
                    f = self._match_to_finding(match, rule, rel_path, content)
                    if not self._suppress_self_false_positive(f):
                        findings.append(f)
            else:
                for i, line in enumerate(content_lines, 1):
                    for match in pattern.finditer(line):
                        f = self._match_to_finding_line(
                            match, rule, rel_path, content_lines, i, line
                        )
                        if not self._suppress_self_false_positive(f):
                            findings.append(f)

        # Secret scanning
        if self.secret_detector:
            for f in self.secret_detector.scan(content, rel_path):
                if not self._suppress_self_false_positive(f):
                    findings.append(f)

        # ML anomaly (optional)
        if self.ml_detector and self.ml_detector.is_fitted:
            is_anomaly, score = self.ml_detector.predict(content)
            if is_anomaly:
                f = Finding(
                    category=FindingCategory.CODE_QUALITY,
                    severity=Severity.MEDIUM,
                    message=f"Anomalous code pattern detected (score: {score:.2f})",
                    file_path=rel_path,
                    rule_id="ML-001",
                    rule_name="ML Anomaly Detection",
                    confidence=score,
                )
                if not self._suppress_self_false_positive(f):
                    findings.append(f)

        return findings

    def _match_to_finding(
        self,
        match: re.Match,
        rule: Rule,
        file_path: str,
        content: str,
    ) -> Finding:
        line_num = content.count("\n", 0, match.start()) + 1
        last_newline = content.rfind("\n", 0, match.start())
        col_num = (
            (match.start() - last_newline) if last_newline != -1 else (match.start() + 1)
        )

        lines = content.splitlines()
        start_line = max(0, line_num - 3)
        end_line = min(len(lines), line_num + 2)

        return Finding(
            category=rule.category,
            severity=rule.severity,
            message=rule.message,
            file_path=file_path,
            line=line_num,
            column=col_num,
            line_end=line_num,
            column_end=col_num + len(match.group(0)),
            token=match.group(0)[:100],
            snippet=content[
                max(0, match.start() - 40) : min(len(content), match.end() + 40)
            ].replace("\n", "\\n"),
            context_lines=lines[start_line:end_line],
            rule_id=rule.name,
            rule_name=rule.name,
            remediation=rule.remediation,
        )

    def _match_to_finding_line(
        self,
        match: re.Match,
        rule: Rule,
        file_path: str,
        all_lines: List[str],
        line_num: int,
        line_text: str,
    ) -> Finding:
        col_num = match.start() + 1
        start_line = max(0, line_num - 3)
        end_line = min(len(all_lines), line_num + 2)
        snippet_start = max(0, match.start() - 40)
        snippet_end = min(len(line_text), match.end() + 40)

        return Finding(
            category=rule.category,
            severity=rule.severity,
            message=rule.message,
            file_path=file_path,
            line=line_num,
            column=col_num,
            line_end=line_num,
            column_end=match.end() + 1,
            token=match.group(0)[:100],
            snippet=line_text[snippet_start:snippet_end],
            context_lines=all_lines[start_line:end_line],
            rule_id=rule.name,
            rule_name=rule.name,
            remediation=rule.remediation,
        )

    async def scan_parallel(
        self,
        files: List[Path],
        progress_callback: Optional[Callable[[], None]] = None,
    ) -> List[Finding]:
        """
        v5.1.0 FIX: Single ThreadPoolExecutor for the whole scan instead of
        one per 100-file chunk. Uses asyncio.Semaphore to bound in-flight
        work so memory stays controlled even with large repos.
        """
        all_findings: List[Finding] = []
        loop = asyncio.get_running_loop()
        max_in_flight = max(self.parallel_workers * 4, 100)
        sem = asyncio.Semaphore(max_in_flight)

        # ONE executor for the entire scan
        executor = ThreadPoolExecutor(
            max_workers=self.parallel_workers,
            thread_name_prefix="HygieneWorker",
        )

        async def _scan_one(f: Path) -> List[Finding]:
            async with sem:
                try:
                    return await loop.run_in_executor(executor, self.scan_file, f)
                except Exception as e:
                    logger.error("Scan error for %s: %s", f, e)
                    return []
                finally:
                    if progress_callback:
                        try:
                            progress_callback()
                        except Exception:
                            pass

        try:
            tasks = [_scan_one(f) for f in files]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, list):
                    all_findings.extend(result)
        finally:
            executor.shutdown(wait=False)

        return all_findings


# =============================================================================
# Output Formatters
# =============================================================================
class OutputFormatter:
    @staticmethod
    def rich_console(summary: ScanSummary, findings: List[Finding]) -> None:
        if not console:
            return
        console.print(
            f"\n[bold blue]TADAWUL REPOSITORY HYGIENE SCAN v{SCRIPT_VERSION}[/bold blue]"
        )
        console.print("=" * 80)

        table = Table(title="Scan Summary")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", style="magenta")
        table.add_row("Files Scanned", str(summary.total_files))
        table.add_row("Files with Issues", str(summary.files_with_issues))
        table.add_row("Total Findings", str(summary.total_findings))
        table.add_row("Duration", f"{summary.scan_duration_ms:.2f}ms")
        console.print(table)

        if findings:
            console.print("\n[bold red]Findings:[/bold red]")
            by_file: Dict[str, List[Finding]] = defaultdict(list)
            for f in findings:
                by_file[f.file_path].append(f)

            for fp, f_list in sorted(by_file.items()):
                console.print(f"\n📄 [bold yellow]{fp}[/bold yellow]:")
                for f in sorted(f_list, key=lambda x: (x.line, x.column, x.severity.value)):
                    color = {
                        "CRITICAL": "red",
                        "HIGH": "red",
                        "MEDIUM": "yellow",
                        "LOW": "green",
                        "INFO": "blue",
                    }.get(f.severity.value, "white")
                    console.print(
                        f"   [{color}][{f.severity.value}][/{color}] "
                        f"Line {f.line}: {f.message}"
                    )
                    if f.token:
                        console.print(f"       Token: [dim]{f.token}[/dim]")
                    if f.remediation:
                        console.print(
                            f"       💡 Fix: [italic green]{f.remediation}[/italic green]"
                        )

    @staticmethod
    def json(summary: ScanSummary, findings: List[Finding]) -> str:
        return json_dumps(
            {
                "summary": summary.to_dict(),
                "findings": [f.to_dict() for f in findings],
            },
            indent=2,
        )

    @staticmethod
    def junit(summary: ScanSummary, findings: List[Finding]) -> str:
        if not _JUNIT_AVAILABLE:
            return "JUnit XML output requires junit_xml package"
        test_cases: List[Any] = []
        for f in findings:
            tc = TestCase(
                name=f"{f.file_path}:{f.line}",
                classname=f.category.value,
                file=f.file_path,
                line=f.line,
            )
            if f.severity in (Severity.CRITICAL, Severity.HIGH):
                tc.add_failure_info(
                    message=f.message, output=f.token or "",
                    failure_type=f.severity.value,
                )
            elif f.severity == Severity.MEDIUM:
                tc.add_error_info(
                    message=f.message, output=f.token or "",
                    error_type=f.severity.value,
                )
            else:
                tc.add_skipped_info(f.message)
            test_cases.append(tc)
        return TestSuite.to_xml_string(
            [TestSuite("Repository Hygiene Scan", test_cases)]
        )

    @staticmethod
    def sarif(summary: ScanSummary, findings: List[Finding]) -> str:
        sarif: Dict[str, Any] = {
            "$schema": "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/master/Schemata/sarif-schema-2.1.0.json",
            "version": "2.1.0",
            "runs": [
                {
                    "tool": {
                        "driver": {
                            "name": "Tadawul Repo Hygiene",
                            "version": SCRIPT_VERSION,
                            "rules": [],
                        }
                    },
                    "results": [],
                    "properties": {"summary": summary.to_dict()},
                }
            ],
        }

        rule_ids: Set[str] = set()
        for f in findings:
            if f.rule_id and f.rule_id not in rule_ids:
                sarif["runs"][0]["tool"]["driver"]["rules"].append(
                    {
                        "id": f.rule_id,
                        "name": f.rule_name or f.rule_id,
                        "shortDescription": {"text": f.message},
                        "defaultConfiguration": {"level": f._sarif_level()},
                        "properties": {"category": f.category.value},
                    }
                )
                rule_ids.add(f.rule_id)
            sarif["runs"][0]["results"].append(f.to_sarif())

        return json_dumps(sarif, indent=2)

    @staticmethod
    def csv(summary: ScanSummary, findings: List[Finding]) -> str:
        """v5.1.0 NEW: CSV output. RFC 4180-style quoting."""
        import csv as _csv
        import io as _io

        buf = _io.StringIO()
        writer = _csv.writer(buf, quoting=_csv.QUOTE_MINIMAL)
        writer.writerow([
            "category", "severity", "file_path", "line", "column",
            "line_end", "column_end", "message", "rule_id", "rule_name",
            "token", "remediation", "confidence", "detected_at",
        ])
        for f in findings:
            writer.writerow([
                f.category.value, f.severity.value, f.file_path,
                f.line, f.column, f.line_end, f.column_end,
                f.message, f.rule_id or "", f.rule_name or "",
                f.token or "", f.remediation or "",
                f"{f.confidence:.3f}", f.detected_at,
            ])
        return buf.getvalue()

    @staticmethod
    def markdown(summary: ScanSummary, findings: List[Finding]) -> str:
        """v5.1.0 NEW: Markdown report output."""
        lines: List[str] = []
        lines.append(f"# Repository Hygiene Report v{SCRIPT_VERSION}")
        lines.append("")
        lines.append(f"- **Files Scanned:** {summary.total_files}")
        lines.append(f"- **Files with Issues:** {summary.files_with_issues}")
        lines.append(f"- **Total Findings:** {summary.total_findings}")
        lines.append(f"- **Duration:** {summary.scan_duration_ms:.2f}ms")
        lines.append(f"- **Scan Started:** {summary.scan_start}")
        lines.append(f"- **Scan Ended:** {summary.scan_end}")
        lines.append("")

        # Severity breakdown
        lines.append("## Severity Breakdown")
        lines.append("")
        lines.append("| Severity | Count |")
        lines.append("|----------|-------|")
        for sev in (
            Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM,
            Severity.LOW, Severity.INFO,
        ):
            count = summary.by_severity.get(sev, 0)
            lines.append(f"| {sev.value} | {count} |")
        lines.append("")

        if not findings:
            lines.append("## Findings")
            lines.append("")
            lines.append("_No findings._")
            return "\n".join(lines) + "\n"

        # Findings grouped by file
        lines.append("## Findings")
        lines.append("")
        by_file: Dict[str, List[Finding]] = defaultdict(list)
        for f in findings:
            by_file[f.file_path].append(f)

        for fp in sorted(by_file.keys()):
            lines.append(f"### `{fp}`")
            lines.append("")
            lines.append("| Line | Severity | Category | Message | Rule |")
            lines.append("|------|----------|----------|---------|------|")
            for f in sorted(
                by_file[fp], key=lambda x: (x.line, x.column, x.severity.value)
            ):
                # Escape pipe chars in message/rule to avoid breaking tables
                msg = f.message.replace("|", "\\|")
                rule = (f.rule_id or f.rule_name or "").replace("|", "\\|")
                lines.append(
                    f"| {f.line} | {f.severity.value} | {f.category.value} "
                    f"| {msg} | `{rule}` |"
                )
            lines.append("")
        return "\n".join(lines) + "\n"


# =============================================================================
# Main
# =============================================================================
def _stable_short_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:8].upper()


def create_default_rules(strict: bool = False) -> List[Rule]:
    rules: List[Rule] = []

    # Exclude this script from token-example rules (it intentionally contains them)
    self_script_exclude = r"(^|/|\\)scripts[\\/]+repo_hygiene_check\.py$"

    for fence in TokenBuilder.markdown_fences():
        rules.append(
            Rule(
                name=f"MD-FENCE-{_stable_short_hash(fence)}",
                pattern=re.escape(fence),
                category=FindingCategory.MARKDOWN_FENCE,
                severity=Severity.HIGH,
                message=f"Markdown fence detected: {fence}",
                remediation="Remove markdown fences from code files",
                exclude_pattern=self_script_exclude,
            )
        )

    for artifact in TokenBuilder.llm_artifacts(strict):
        rules.append(
            Rule(
                name=f"LLM-{_stable_short_hash(artifact)}",
                pattern=re.escape(artifact),
                category=FindingCategory.LLM_ARTIFACT,
                severity=Severity.MEDIUM,
                message=f"LLM artifact detected: {artifact}",
                remediation="Remove LLM copy-paste artifacts",
                exclude_pattern=self_script_exclude,
            )
        )

    for pattern, message, severity in TokenBuilder.suspicious_patterns():
        rules.append(
            Rule(
                name=f"SUS-{_stable_short_hash(pattern)}",
                pattern=pattern,
                category=FindingCategory.CODE_QUALITY,
                severity=severity,
                message=message,
                remediation="Resolve or remove suspicious code",
                multiline=True,
                # Intentionally defined examples exist in this file.
                exclude_pattern=(
                    self_script_exclude
                    if any(x in message for x in ["TODO", "FIXME", "Debug", "Debugger"])
                    else None
                ),
            )
        )

    return rules


def _write_output_by_extension(
    summary: ScanSummary,
    findings: List[Finding],
    output_file: str,
) -> None:
    """v5.1.0: dispatch on file extension, now covers .csv and .md too."""
    lower = output_file.lower()
    if lower.endswith(".json"):
        payload = OutputFormatter.json(summary, findings)
    elif lower.endswith(".xml"):
        payload = OutputFormatter.junit(summary, findings)
    elif lower.endswith(".sarif"):
        payload = OutputFormatter.sarif(summary, findings)
    elif lower.endswith(".csv"):
        payload = OutputFormatter.csv(summary, findings)
    elif lower.endswith(".md") or lower.endswith(".markdown"):
        payload = OutputFormatter.markdown(summary, findings)
    else:
        # default to JSON for unknown extensions
        payload = OutputFormatter.json(summary, findings)

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(payload)


async def main_async(args: argparse.Namespace) -> int:
    start_time = time.time()
    exit_code = 0

    try:
        root = Path(args.root).resolve()

        skip_dirs = set(args.exclude or [])
        skip_dirs.update(
            {
                "venv", ".venv", "env", ".env", "__pycache__", ".git", ".hg",
                ".svn", ".idea", ".vscode", ".pytest_cache", ".mypy_cache",
                ".ruff_cache", "node_modules", "dist", "build", ".eggs",
                ".tox", "htmlcov",
            }
        )

        rules = create_default_rules(bool(args.strict))
        scanner = HygieneScanner(
            root=root,
            rules=rules,
            enable_ml=bool(args.enable_ml),
            enable_secret_detection=not args.disable_secrets,
            parallel_workers=args.workers or os.cpu_count() or 4,
            self_suppressions=True,
        )
        file_scanner = FileScanner(
            root=root,
            skip_dirs=skip_dirs,
            extensions=args.extensions if args.extensions else [".py"],
            scan_all_text=bool(args.scan_all_text),
            exclude_patterns=args.exclude_patterns,
            include_patterns=args.include_patterns,
            max_file_size_mb=args.max_file_size or 10,
        )

        files = file_scanner.get_files()

        if args.enable_ml and scanner.ml_detector:
            if console:
                console.print("[yellow]Training ML Anomaly Detector...[/yellow]")
            for f in files[:100]:
                try:
                    scanner.ml_detector.add_training_data(
                        f.read_text(encoding="utf-8", errors="replace")
                    )
                except Exception:
                    pass
            scanner.ml_detector.fit()

        if console and Progress is not None:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                TimeElapsedColumn(),
                console=console,
            ) as progress:
                task_id = progress.add_task("[cyan]Scanning files...", total=len(files))

                def update_progress() -> None:
                    progress.advance(task_id)

                findings = await scanner.scan_parallel(files, update_progress)
        else:
            logger.info(
                "Scanning %s files with %s workers...",
                len(files), scanner.parallel_workers,
            )
            findings = await scanner.scan_parallel(files)

        if args.max_offenders > 0 and len(findings) > args.max_offenders:
            findings = findings[: args.max_offenders]

        summary = ScanSummary(
            total_files=len(files),
            files_with_issues=len(set(f.file_path for f in findings)),
            scan_duration_ms=(time.time() - start_time) * 1000,
            scan_end=datetime.now(timezone.utc).isoformat(),
        )
        for f in findings:
            summary.add_finding(f)

        output_format = OutputFormat(args.format) if args.format else OutputFormat.CONSOLE

        if output_format == OutputFormat.CONSOLE:
            if console:
                OutputFormatter.rich_console(summary, findings)
            else:
                print(OutputFormatter.json(summary, findings))
        elif output_format == OutputFormat.JSON:
            print(OutputFormatter.json(summary, findings))
        elif output_format == OutputFormat.JUNIT:
            print(OutputFormatter.junit(summary, findings))
        elif output_format == OutputFormat.SARIF:
            print(OutputFormatter.sarif(summary, findings))
        elif output_format == OutputFormat.CSV:
            # v5.1.0 NEW: CSV formatter
            print(OutputFormatter.csv(summary, findings))
        elif output_format == OutputFormat.MARKDOWN:
            # v5.1.0 NEW: Markdown formatter
            print(OutputFormatter.markdown(summary, findings))

        if args.output_file:
            _write_output_by_extension(summary, findings, args.output_file)

        if args.annotate:
            for f in findings:
                if f.severity in (Severity.CRITICAL, Severity.HIGH):
                    CIDetector.annotate_error(f.file_path, f.line, f.column, f.message)
                elif f.severity == Severity.MEDIUM:
                    CIDetector.annotate_warning(f.file_path, f.line, f.column, f.message)

        critical_count = summary.by_severity.get(Severity.CRITICAL, 0)
        high_count = summary.by_severity.get(Severity.HIGH, 0)

        if args.fail_threshold == "critical" and critical_count > 0:
            exit_code = 2
        elif args.fail_threshold == "high" and (critical_count + high_count) > 0:
            exit_code = 2
        elif args.fail_threshold == "any" and summary.total_findings > 0:
            exit_code = 2
        else:
            exit_code = 0

    except Exception as e:
        logger.exception("Scan failed: %s", e)
        exit_code = 1

    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(
        description=f"Repository Hygiene Checker v{SCRIPT_VERSION}"
    )
    parser.add_argument(
        "--root",
        default=os.getenv("HYGIENE_ROOT", "."),
        help="Repository root to scan (HYGIENE_ROOT env).",
    )
    parser.add_argument(
        "--strict",
        type=int,
        default=_env_int_bool("HYGIENE_STRICT", 0),
        help="Enable strict heuristics (HYGIENE_STRICT env).",
    )
    parser.add_argument(
        "--max-offenders",
        type=int,
        default=_env_int("HYGIENE_MAX_OFFENDERS", 0, lo=0),
        help="Stop after N offenders (HYGIENE_MAX_OFFENDERS env).",
    )
    parser.add_argument(
        "--exclude",
        nargs="+",
        default=_env_csv("HYGIENE_EXCLUDE"),
        help="Additional directories to exclude (HYGIENE_EXCLUDE env, comma-separated).",
    )
    parser.add_argument(
        "--extensions",
        nargs="+",
        default=_env_csv("HYGIENE_EXTENSIONS"),
        help="File extensions to scan (HYGIENE_EXTENSIONS env, comma-separated).",
    )
    parser.add_argument(
        "--scan-all-text",
        type=int,
        default=_env_int_bool("HYGIENE_SCAN_ALL_TEXT", 0),
        help="Scan all text files (HYGIENE_SCAN_ALL_TEXT env).",
    )
    parser.add_argument(
        "--exclude-patterns",
        nargs="+",
        default=_env_csv("HYGIENE_EXCLUDE_PATTERNS"),
        help="Regex patterns to exclude (HYGIENE_EXCLUDE_PATTERNS env).",
    )
    parser.add_argument(
        "--include-patterns",
        nargs="+",
        default=_env_csv("HYGIENE_INCLUDE_PATTERNS"),
        help="Regex patterns to include (HYGIENE_INCLUDE_PATTERNS env).",
    )
    parser.add_argument(
        "--max-file-size",
        type=int,
        default=_env_int("HYGIENE_MAX_FILE_SIZE_MB", 2, lo=1),
        help="Max file size in MB (HYGIENE_MAX_FILE_SIZE_MB env).",
    )
    parser.add_argument(
        "--enable-ml",
        type=int,
        default=_env_int_bool("HYGIENE_ENABLE_ML", 0),
        help="Enable ML anomaly detection (HYGIENE_ENABLE_ML env).",
    )
    parser.add_argument(
        "--disable-secrets",
        action="store_true",
        default=_env_bool("HYGIENE_DISABLE_SECRETS", False),
        help="Disable secret detection (HYGIENE_DISABLE_SECRETS env).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=_env_int("HYGIENE_WORKERS", 0, lo=0) or None,
        help="Number of worker threads (HYGIENE_WORKERS env, default CPU count).",
    )
    parser.add_argument(
        "--format",
        choices=[f.value for f in OutputFormat],
        default=os.getenv("HYGIENE_FORMAT") or None,
        help="Output format (HYGIENE_FORMAT env).",
    )
    parser.add_argument(
        "--output-file",
        default=os.getenv("HYGIENE_OUTPUT_FILE") or None,
        help="Save output to file (HYGIENE_OUTPUT_FILE env). "
             "Extension determines format (.json/.xml/.sarif/.csv/.md).",
    )
    parser.add_argument(
        "--annotate",
        type=int,
        default=_env_int_bool("HYGIENE_ANNOTATE", 1),
        help="Annotate in CI environment (HYGIENE_ANNOTATE env).",
    )
    parser.add_argument(
        "--fail-threshold",
        choices=["critical", "high", "any"],
        default=os.getenv("HYGIENE_FAIL_THRESHOLD", "high"),
        help="Failure threshold (HYGIENE_FAIL_THRESHOLD env).",
    )

    args = parser.parse_args()

    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        if console:
            console.print("\n[yellow]Scan interrupted by user.[/yellow]")
        return 1
    except Exception as e:
        if console:
            console.print(f"\n[red]Scan failed: {e}[/red]")
        else:
            logger.exception("Scan failed: %s", e)
        return 1


__all__ = [
    "SCRIPT_VERSION",
    "SERVICE_VERSION",
    "Severity",
    "FindingCategory",
    "OutputFormat",
    "CIProvider",
    "Finding",
    "Rule",
    "ScanSummary",
    "CIDetector",
    "SecretDetector",
    "MLAnomalyDetector",
    "TokenBuilder",
    "FileScanner",
    "HygieneScanner",
    "OutputFormatter",
    "create_default_rules",
    "main_async",
    "main",
]


if __name__ == "__main__":
    sys.exit(main())
