#!/usr/bin/env python3
"""
scripts/repo_hygiene_check.py
================================================================================
TADAWUL ENTERPRISE REPOSITORY HYGIENE CHECKER — v5.0.1 (QUANTUM EDITION)
================================================================================
AI-POWERED | CI/CD INTEGRATED | MEMORY-OPTIMIZED | FALSE-POSITIVE RESISTANT

v5.0.1 hotfixes:
- ✅ FIX: Self-scan suppression for intentional token examples in this script
       (markdown fences / LLM artifact examples / debugger pattern examples).
- ✅ FIX: Secret detection heuristics tightened to reduce false positives in config/router files
       (e.g., env var names, schema fields, placeholders, help text, regex definitions).
- ✅ FIX: Rule.exclude_pattern is now honored.
- ✅ IMPROVEMENT: More precise line/column handling for line-based matches.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
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
    import orjson

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        option = orjson.OPT_INDENT_2 if indent else 0
        return orjson.dumps(v, option=option).decode("utf-8")

    def json_loads(v: Union[str, bytes]) -> Any:
        return orjson.loads(v)

    _HAS_ORJSON = True
except ImportError:
    import json

    def json_dumps(v: Any, *, indent: int = 0) -> str:
        return json.dumps(v, indent=indent if indent else None)

    def json_loads(v: Union[str, bytes]) -> Any:
        return json.loads(v)

    _HAS_ORJSON = False

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
    from rich.panel import Panel

    _RICH_AVAILABLE = True
    console = Console()
except ImportError:
    _RICH_AVAILABLE = False
    console = None

# Optional ML/AI libraries
try:
    import numpy as np
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.ensemble import IsolationForest
    from sklearn.preprocessing import StandardScaler

    _ML_AVAILABLE = True
except ImportError:
    _ML_AVAILABLE = False

# Optional security scanning
try:
    import bandit
    from bandit.core import manager as bandit_manager

    _BANDIT_AVAILABLE = True
except ImportError:
    _BANDIT_AVAILABLE = False

# Optional Git integration
try:
    import git
    from git import Repo

    _GIT_AVAILABLE = True
except ImportError:
    _GIT_AVAILABLE = False

# Optional output formats
try:
    import junit_xml
    from junit_xml import TestSuite, TestCase

    _JUNIT_AVAILABLE = True
except ImportError:
    _JUNIT_AVAILABLE = False

try:
    import yaml

    _YAML_AVAILABLE = True
except ImportError:
    _YAML_AVAILABLE = False

# Version
SCRIPT_VERSION = "5.0.1"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("RepoHygiene")

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
    CONSOLE = "console"
    JSON = "json"
    JUNIT = "junit"
    SARIF = "sarif"
    CSV = "csv"
    HTML = "html"
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
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

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
    by_severity: Dict[Severity, int] = field(default_factory=lambda: {s: 0 for s in Severity})
    by_category: Dict[FindingCategory, int] = field(default_factory=dict)
    by_file: Dict[str, int] = field(default_factory=dict)
    scan_duration_ms: float = 0.0
    scan_start: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    scan_end: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    version: str = SCRIPT_VERSION

    def add_finding(self, finding: Finding) -> None:
        self.total_findings += 1
        self.by_severity[finding.severity] = self.by_severity.get(finding.severity, 0) + 1
        self.by_category[finding.category] = self.by_category.get(finding.category, 0) + 1
        self.by_file[finding.file_path] = self.by_file.get(finding.file_path, 0) + 1

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "total_files": self.total_files,
            "files_with_issues": self.files_with_issues,
            "total_findings": self.total_findings,
            "by_severity": {k.value: v for k, v in self.by_severity.items()},
            "by_category": {k.value: v for k, v in self.by_category.items()},
            "by_file": dict(sorted(self.by_file.items(), key=lambda x: x[1], reverse=True)[:20]),
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
    - Allow explicit high-confidence formats (JWT, AWS access keys, private keys, DB URLs with creds).
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

        # Type hints / schema defaults / validators often include words like password/token
        if " = None" in line or " = \"\"" in line or " = ''" in line:
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
        match: re.Match[str],
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
                remediation="Remove secrets from code, use environment variables or secret management services",
                references=["https://docs.github.com/en/code-security/secret-scanning"],
                confidence=confidence,
            )
        )

    @classmethod
    def scan(cls, content: str, file_path: str) -> List[Finding]:
        findings: List[Finding] = []
        lines = content.splitlines()

        for i, line in enumerate(lines, 1):
            # 1) High-confidence signatures (still filter self-detector regex examples)
            if not ("re.compile(" in line and file_path.replace("\\", "/").endswith("repo_hygiene_check.py")):
                for regex, secret_type in cls.PATTERNS:
                    for match in regex.finditer(line):
                        # Avoid obvious placeholders in DB URLs (rare but possible)
                        if secret_type == "Database URL with credentials":
                            if any(x in line.lower() for x in ("example", "placeholder", "changeme")):
                                continue
                        cls._emit_finding(
                            findings,
                            file_path=file_path,
                            line_no=i,
                            match=match,
                            secret_type=secret_type,
                            confidence=0.95 if secret_type in {"Private Key", "JWT Token"} else 0.9,
                        )

            # 2) Assignment-style patterns with context suppression
            if cls._is_non_secret_context(line, file_path):
                continue

            # API keys / secrets
            for match in cls.API_KEY_ASSIGN_RE.finditer(line):
                value = match.group(3)
                if cls._looks_like_placeholder(value):
                    continue
                cls._emit_finding(findings, file_path=file_path, line_no=i, match=match, secret_type="API Key", confidence=0.85)

            # Passwords (quoted only)
            for match in cls.PASSWORD_ASSIGN_RE.finditer(line):
                value = match.group(3)
                if cls._looks_like_placeholder(value):
                    continue
                # skip policy-like values / labels
                if re.fullmatch(r"[A-Z_]{4,}", value):
                    continue
                cls._emit_finding(findings, file_path=file_path, line_no=i, match=match, secret_type="Password", confidence=0.82)

            # OAuth / tokens (quoted only)
            for match in cls.TOKEN_ASSIGN_RE.finditer(line):
                value = match.group(3)
                if cls._looks_like_placeholder(value):
                    continue
                # skip enum-ish strings
                if re.fullmatch(r"[A-Z_]{8,}", value):
                    continue
                cls._emit_finding(findings, file_path=file_path, line_no=i, match=match, secret_type="OAuth Token", confidence=0.84)

            # AWS secret assignment
            for match in cls.AWS_SECRET_ASSIGN_RE.finditer(line):
                value = match.group(4)
                if cls._looks_like_placeholder(value):
                    continue
                cls._emit_finding(findings, file_path=file_path, line_no=i, match=match, secret_type="AWS Secret", confidence=0.9)

        return findings


# =============================================================================
# ML Anomaly Detection
# =============================================================================


class MLAnomalyDetector:
    def __init__(self) -> None:
        self.vectorizer = TfidfVectorizer(max_features=1000, stop_words="english", ngram_range=(1, 3))
        self.model = IsolationForest(contamination=0.05, random_state=42, n_estimators=150)
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
            "python",
            "py",
            "bash",
            "sh",
            "javascript",
            "js",
            "typescript",
            "ts",
            "json",
            "yaml",
            "xml",
            "html",
            "css",
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
        V5.0.0 FIX: Safely anchor git merge conflict markers using ^ and $
        along with re.MULTILINE to prevent flagging decorative headers.
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
        self.extensions = {e.lower() if e.startswith(".") else f".{e.lower()}" for e in extensions}
        self.scan_all_text = scan_all_text
        self.exclude_patterns = [re.compile(p) for p in (exclude_patterns or [])]
        self.include_patterns = [re.compile(p) for p in (include_patterns or [])]
        self.max_file_size = max_file_size_mb * 1024 * 1024
        self.binary_extensions = {
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".webp",
            ".bmp",
            ".ico",
            ".pdf",
            ".zip",
            ".tar",
            ".gz",
            ".7z",
            ".rar",
            ".exe",
            ".dll",
            ".so",
            ".dylib",
            ".bin",
            ".pyc",
            ".pyo",
            ".pyd",
            ".class",
            ".jar",
            ".mp3",
            ".mp4",
            ".avi",
            ".mov",
        }

    def should_skip(self, path: Path) -> bool:
        for part in path.parts:
            if part.lower() in self.skip_dirs:
                return True
            if part.startswith(".") and part not in {".github", ".gitignore", ".gitattributes"}:
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
        if finding.category in {FindingCategory.MARKDOWN_FENCE, FindingCategory.LLM_ARTIFACT}:
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
                        f = self._match_to_finding_line(match, rule, rel_path, content_lines, i, line)
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
        match: re.Match[str],
        rule: Rule,
        file_path: str,
        content: str,
    ) -> Finding:
        line_num = content.count("\n", 0, match.start()) + 1
        last_newline = content.rfind("\n", 0, match.start())
        col_num = (match.start() - last_newline) if last_newline != -1 else (match.start() + 1)

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
            snippet=content[max(0, match.start() - 40):min(len(content), match.end() + 40)].replace("\n", "\\n"),
            context_lines=lines[start_line:end_line],
            rule_id=rule.name,
            rule_name=rule.name,
            remediation=rule.remediation,
        )

    def _match_to_finding_line(
        self,
        match: re.Match[str],
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

    async def scan_parallel(self, files: List[Path], progress_callback: Optional[Callable[[], None]] = None) -> List[Finding]:
        all_findings: List[Finding] = []
        loop = asyncio.get_running_loop()

        # Chunking tasks to prevent massive RAM spikes
        chunk_size = 100
        for i in range(0, len(files), chunk_size):
            chunk = files[i : i + chunk_size]
            with ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
                tasks = [loop.run_in_executor(executor, self.scan_file, f) for f in chunk]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for idx, result in enumerate(results):
                    if progress_callback:
                        progress_callback()
                    if isinstance(result, Exception):
                        logger.error("Scan error for %s: %s", chunk[idx], result)
                    elif isinstance(result, list):
                        all_findings.extend(result)
        return all_findings


# =============================================================================
# Output Formatters
# =============================================================================


class OutputFormatter:
    @staticmethod
    def rich_console(summary: ScanSummary, findings: List[Finding]) -> None:
        if not console:
            return
        console.print(f"\n[bold blue]TADAWUL REPOSITORY HYGIENE SCAN v{SCRIPT_VERSION}[/bold blue]")
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
                    console.print(f"   [{color}][{f.severity.value}][/{color}] Line {f.line}: {f.message}")
                    if f.token:
                        console.print(f"       Token: [dim]{f.token}[/dim]")
                    if f.remediation:
                        console.print(f"       💡 Fix: [italic green]{f.remediation}[/italic green]")

    @staticmethod
    def json(summary: ScanSummary, findings: List[Finding]) -> str:
        return json_dumps({"summary": summary.to_dict(), "findings": [f.to_dict() for f in findings]}, indent=2)

    @staticmethod
    def junit(summary: ScanSummary, findings: List[Finding]) -> str:
        if not _JUNIT_AVAILABLE:
            return "JUnit XML output requires junit_xml package"
        test_cases: List[Any] = []
        for f in findings:
            tc = TestCase(name=f"{f.file_path}:{f.line}", classname=f.category.value, file=f.file_path, line=f.line)
            if f.severity in (Severity.CRITICAL, Severity.HIGH):
                tc.add_failure_info(message=f.message, output=f.token or "", failure_type=f.severity.value)
            elif f.severity == Severity.MEDIUM:
                tc.add_error_info(message=f.message, output=f.token or "", error_type=f.severity.value)
            else:
                tc.add_skipped_info(f.message)
            test_cases.append(tc)
        return TestSuite.to_xml_string([TestSuite("Repository Hygiene Scan", test_cases)])

    @staticmethod
    def sarif(summary: ScanSummary, findings: List[Finding]) -> str:
        sarif: Dict[str, Any] = {
            "$schema": "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/master/Schemata/sarif-schema-2.1.0.json",
            "version": "2.1.0",
            "runs": [
                {
                    "tool": {"driver": {"name": "Tadawul Repo Hygiene", "version": SCRIPT_VERSION, "rules": []}},
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
                exclude_pattern=self_script_exclude if any(
                    x in message for x in ["TODO", "FIXME", "Debug", "Debugger"]
                ) else None,
            )
        )

    return rules


async def main_async(args: argparse.Namespace) -> int:
    start_time = time.time()
    root = Path(args.root).resolve()

    skip_dirs = set(args.exclude or [])
    skip_dirs.update(
        {
            "venv",
            ".venv",
            "env",
            ".env",
            "__pycache__",
            ".git",
            ".hg",
            ".svn",
            ".idea",
            ".vscode",
            ".pytest_cache",
            ".mypy_cache",
            ".ruff_cache",
            "node_modules",
            "dist",
            "build",
            ".eggs",
            ".tox",
            "htmlcov",
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
                scanner.ml_detector.add_training_data(f.read_text(encoding="utf-8", errors="replace"))
            except Exception:
                pass
        scanner.ml_detector.fit()

    if console:
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
        logger.info("Scanning %s files with %s workers...", len(files), scanner.parallel_workers)
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
            print(OutputFormatter.json(summary, findings))  # Fallback if rich is missing
    elif output_format == OutputFormat.JSON:
        print(OutputFormatter.json(summary, findings))
    elif output_format == OutputFormat.JUNIT:
        print(OutputFormatter.junit(summary, findings))
    elif output_format == OutputFormat.SARIF:
        print(OutputFormatter.sarif(summary, findings))

    if args.output_file:
        with open(args.output_file, "w", encoding="utf-8") as f:
            if args.output_file.endswith(".json"):
                f.write(OutputFormatter.json(summary, findings))
            elif args.output_file.endswith(".xml"):
                f.write(OutputFormatter.junit(summary, findings))
            elif args.output_file.endswith(".sarif"):
                f.write(OutputFormatter.sarif(summary, findings))
            else:
                f.write(OutputFormatter.json(summary, findings))

    if args.annotate:
        for f in findings:
            if f.severity in (Severity.CRITICAL, Severity.HIGH):
                CIDetector.annotate_error(f.file_path, f.line, f.column, f.message)
            elif f.severity == Severity.MEDIUM:
                CIDetector.annotate_warning(f.file_path, f.line, f.column, f.message)

    critical_count = summary.by_severity.get(Severity.CRITICAL, 0)
    high_count = summary.by_severity.get(Severity.HIGH, 0)

    if args.fail_threshold == "critical" and critical_count > 0:
        return 2
    if args.fail_threshold == "high" and (critical_count + high_count) > 0:
        return 2
    if args.fail_threshold == "any" and summary.total_findings > 0:
        return 2
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=f"Repository Hygiene Checker v{SCRIPT_VERSION}")
    parser.add_argument("--root", default=".", help="Repository root to scan")
    parser.add_argument("--strict", type=int, default=0, help="Enable strict heuristics")
    parser.add_argument("--max-offenders", type=int, default=0, help="Stop after N offenders")
    parser.add_argument("--fail-on-read-error", action="store_true", help="Fail on read errors")
    parser.add_argument("--exclude", nargs="+", help="Additional directories to exclude")
    parser.add_argument("--extensions", nargs="+", help="File extensions to scan")
    parser.add_argument("--scan-all-text", type=int, default=0, help="Scan all text files")
    parser.add_argument("--exclude-patterns", nargs="+", help="Regex patterns to exclude")
    parser.add_argument("--include-patterns", nargs="+", help="Regex patterns to include")
    parser.add_argument("--max-file-size", type=int, default=2, help="Max file size in MB")
    parser.add_argument("--enable-ml", type=int, default=0, help="Enable ML anomaly detection")
    parser.add_argument("--disable-secrets", action="store_true", help="Disable secret detection")
    parser.add_argument("--enable-git", type=int, default=0, help="Enable Git history analysis")
    parser.add_argument("--parallel", type=int, default=1, help="Use parallel scanning")
    parser.add_argument("--workers", type=int, help="Number of worker threads")
    parser.add_argument("--rules-file", help="JSON/YAML file with custom rules")
    parser.add_argument("--format", choices=[f.value for f in OutputFormat], help="Output format")
    parser.add_argument("--output-file", help="Save output to file")
    parser.add_argument("--show-snippets", type=int, default=1, help="Show context snippets")
    parser.add_argument("--annotate", type=int, default=1, help="Annotate in CI environment")
    parser.add_argument(
        "--fail-threshold",
        choices=["critical", "high", "any"],
        default="high",
        help="Failure threshold",
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


if __name__ == "__main__":
    sys.exit(main())
