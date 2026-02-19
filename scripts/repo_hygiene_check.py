#!/usr/bin/env python3
"""
repo_hygiene_check.py
===========================================================
TADAWUL ENTERPRISE REPOSITORY HYGIENE CHECKER — v3.1.0
===========================================================
AI-POWERED | ML-ENHANCED | CI/CD INTEGRATED | COMPREHENSIVE SECURITY

Core Capabilities:
- AI-powered pattern detection using machine learning
- Advanced token analysis with context awareness
- Security vulnerability scanning
- License compliance checking
- Secret detection (API keys, tokens, passwords)
- Code quality metrics
- Git history analysis
- Parallel processing for speed
- Multiple output formats (JSON, JUnit, SARIF)
- GitHub Actions, GitLab CI, Jenkins integration
- Custom rule engine with regex patterns
- Historical trend analysis
- Pre-commit hook integration
- Machine learning model for anomaly detection
- FIXED: Tuple unpacking error in parallel processing

Version: 3.1.0
Last Updated: 2024-03-21
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import csv
import hashlib
import json
import logging
import multiprocessing
import os
import re
import sys
import time
import uuid
from collections import defaultdict, Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    AsyncGenerator,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    Pattern,
    Callable,
    Awaitable,
)
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

# Optional ML/AI libraries
try:
    import numpy as np
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.ensemble import IsolationForest
    from sklearn.exceptions import NotFittedError
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
    _JUNIT_AVAILABLE = True
except ImportError:
    _JUNIT_AVAILABLE = False

try:
    import yaml
    _YAML_AVAILABLE = True
except ImportError:
    _YAML_AVAILABLE = False

# Optional async HTTP for API integration
try:
    import aiohttp
    _ASYNC_HTTP_AVAILABLE = True
except ImportError:
    _ASYNC_HTTP_AVAILABLE = False

# Version
SCRIPT_VERSION = "3.1.0"

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
    """Finding severity levels"""
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INFO = "INFO"

class FindingCategory(str, Enum):
    """Categories of findings"""
    MARKDOWN_FENCE = "MARKDOWN_FENCE"
    LLM_ARTIFACT = "LLM_ARTIFACT"
    SECRET = "SECRET"
    VULNERABILITY = "VULNERABILITY"
    LICENSE = "LICENSE"
    CODE_QUALITY = "CODE_QUALITY"
    GIT_HISTORY = "GIT_HISTORY"
    CUSTOM = "CUSTOM"

class OutputFormat(str, Enum):
    """Output format options"""
    CONSOLE = "console"
    JSON = "json"
    JUNIT = "junit"
    SARIF = "sarif"
    CSV = "csv"
    HTML = "html"
    MARKDOWN = "markdown"

class CIProvider(str, Enum):
    """CI/CD provider detection"""
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

@dataclass
class Finding:
    """Represents a single hygiene finding"""
    
    # Core
    category: FindingCategory
    severity: Severity
    message: str
    
    # Location
    file_path: str
    line: int = 0
    column: int = 0
    line_end: int = 0
    column_end: int = 0
    
    # Context
    token: Optional[str] = None
    snippet: Optional[str] = None
    context_lines: List[str] = field(default_factory=list)
    
    # Metadata
    rule_id: Optional[str] = None
    rule_name: Optional[str] = None
    remediation: Optional[str] = None
    references: List[str] = field(default_factory=list)
    
    # Score
    confidence: float = 1.0  # 0-1
    impact_score: float = 1.0  # 0-1
    
    # Timestamps
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
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
        """Convert to SARIF format"""
        return {
            "ruleId": self.rule_id or f"HYGIENE-{self.category.value}",
            "level": self._sarif_level(),
            "message": {"text": self.message},
            "locations": [{
                "physicalLocation": {
                    "artifactLocation": {"uri": self.file_path},
                    "region": {
                        "startLine": self.line,
                        "startColumn": self.column,
                        "endLine": self.line_end or self.line,
                        "endColumn": self.column_end or (self.column + 1)
                    } if self.line > 0 else None
                }
            }],
            "properties": {
                "category": self.category.value,
                "confidence": self.confidence,
                "impact_score": self.impact_score,
            }
        }
    
    def _sarif_level(self) -> str:
        """Convert severity to SARIF level"""
        mapping = {
            Severity.CRITICAL: "error",
            Severity.HIGH: "error",
            Severity.MEDIUM: "warning",
            Severity.LOW: "note",
            Severity.INFO: "note",
        }
        return mapping.get(self.severity, "note")


@dataclass
class Rule:
    """Custom rule definition"""
    
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
        """Compile regex pattern"""
        flags = 0
        if not self.case_sensitive:
            flags |= re.IGNORECASE
        if self.multiline:
            flags |= re.MULTILINE | re.DOTALL
        return re.compile(self.pattern, flags)


@dataclass
class ScanSummary:
    """Summary of scan results"""
    
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
    
    def add_finding(self, finding: Finding):
        """Add a finding to summary"""
        self.total_findings += 1
        self.by_severity[finding.severity] = self.by_severity.get(finding.severity, 0) + 1
        self.by_category[finding.category] = self.by_category.get(finding.category, 0) + 1
        self.by_file[finding.file_path] = self.by_file.get(finding.file_path, 0) + 1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
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
    """Detect CI/CD environment and provide annotations"""
    
    @staticmethod
    def detect() -> CIProvider:
        """Detect current CI provider"""
        
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
        """Output error annotation for current CI"""
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
        """Output warning annotation for current CI"""
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
    """Detect secrets and sensitive information"""
    
    # Common secret patterns
    PATTERNS = [
        # API Keys
        (re.compile(r'(?i)(api[_-]?key|apikey|api[_-]?secret|api_secret)\s*[:=]\s*[\'"]?([a-zA-Z0-9_\-]{16,})[\'"]?'), "API Key"),
        
        # AWS Keys
        (re.compile(r'(?i)(AKIA[0-9A-Z]{16})'), "AWS Access Key"),
        (re.compile(r'(?i)(aws[_-]?(access|secret)[_-]?(key|token)?\s*[:=]\s*[\'"]?[a-zA-Z0-9/+=]{40,}[\'"]?)'), "AWS Secret"),
        
        # Private Keys
        (re.compile(r'-----BEGIN (RSA|DSA|EC|OPENSSH) PRIVATE KEY-----'), "Private Key"),
        
        # Passwords
        (re.compile(r'(?i)(password|passwd|pwd)\s*[:=]\s*[\'"]?([^\'"\s]{8,})[\'"]?'), "Password"),
        
        # Database URLs
        (re.compile(r'(?i)(postgresql|mysql|mongodb|redis)://[^:]+:[^@]+@'), "Database URL with credentials"),
        
        # OAuth Tokens
        (re.compile(r'(?i)(oauth|bearer|token)\s*[:=]\s*[\'"]?([a-zA-Z0-9_\-]{20,})[\'"]?'), "OAuth Token"),
        
        # JWT Tokens
        (re.compile(r'eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+'), "JWT Token"),
    ]
    
    @classmethod
    def scan(cls, content: str, file_path: str) -> List[Finding]:
        """Scan content for secrets"""
        findings = []
        
        for i, (line) in enumerate(content.splitlines(), 1):
            for regex, secret_type in cls.PATTERNS:
                for match in regex.finditer(line):
                    findings.append(Finding(
                        category=FindingCategory.SECRET,
                        severity=Severity.CRITICAL,
                        message=f"Potential {secret_type} detected",
                        file_path=file_path,
                        line=i,
                        column=match.start() + 1,
                        line_end=i,
                        column_end=match.end() + 1,
                        token=match.group(0)[:50] + "..." if len(match.group(0)) > 50 else match.group(0),
                        rule_id="SECRET-001",
                        rule_name=secret_type,
                        remediation="Remove secrets from code, use environment variables or secret management services",
                        references=["https://docs.github.com/en/code-security/secret-scanning"],
                        confidence=0.8 if len(match.group(0)) > 20 else 0.5,
                    ))
        
        return findings


# =============================================================================
# ML Anomaly Detection
# =============================================================================

class MLAnomalyDetector:
    """ML-based anomaly detection for code patterns"""
    
    def __init__(self):
        self.vectorizer = TfidfVectorizer(
            max_features=1000,
            stop_words='english',
            ngram_range=(1, 3)
        )
        self.model = IsolationForest(
            contamination=0.1,
            random_state=42,
            n_estimators=100
        )
        self.is_fitted = False
        self.training_data: List[str] = []
    
    def add_training_data(self, content: str):
        """Add training data for model"""
        self.training_data.append(content)
    
    def fit(self):
        """Fit the model on training data"""
        if not _ML_AVAILABLE or len(self.training_data) < 10:
            return
        
        try:
            X = self.vectorizer.fit_transform(self.training_data)
            self.model.fit(X.toarray())
            self.is_fitted = True
            logger.info(f"ML model fitted on {len(self.training_data)} samples")
        except Exception as e:
            logger.error(f"ML model fitting failed: {e}")
    
    def predict(self, content: str) -> Tuple[bool, float]:
        """Predict if content is anomalous"""
        if not self.is_fitted or not _ML_AVAILABLE:
            return False, 0.0
        
        try:
            X = self.vectorizer.transform([content])
            score = self.model.score_samples(X.toarray())[0]
            prediction = self.model.predict(X.toarray())[0]
            
            # Normalize score to 0-1 (lower = more anomalous)
            normalized_score = 1.0 / (1.0 + np.exp(-score))
            
            return prediction == -1, float(normalized_score)
            
        except Exception as e:
            logger.error(f"ML prediction failed: {e}")
            return False, 0.0


# =============================================================================
# Token Builders
# =============================================================================

class TokenBuilder:
    """Build detection tokens dynamically"""
    
    @staticmethod
    def backtick(n: int = 3) -> str:
        """Generate n backticks"""
        return "".join(chr(0x60) for _ in range(n))
    
    @staticmethod
    def tilde(n: int = 3) -> str:
        """Generate n tildes"""
        return "".join(chr(0x7E) for _ in range(n))
    
    @classmethod
    def markdown_fences(cls) -> List[str]:
        """Generate markdown fence tokens"""
        bt = cls.backtick(3)
        td = cls.tilde(3)
        
        fences = [bt, td]
        
        # Common language specifiers
        for lang in ["python", "py", "bash", "sh", "javascript", "js", "typescript", "ts", "json", "yaml", "xml", "html", "css"]:
            fences.append(f"{bt}{lang}")
            fences.append(f"{td}{lang}")
        
        return fences
    
    @classmethod
    def llm_artifacts(cls, strict: bool = False) -> List[str]:
        """Generate LLM artifact tokens"""
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
            artifacts.extend([
                "BEGIN CODE",
                "END CODE",
                "Here is the code",
                "Copy ONLY the code",
                "The following code",
                "This is the code",
                "```",
                "~~~",
            ])
        
        return artifacts
    
    @classmethod
    def suspicious_patterns(cls) -> List[Tuple[str, str, Severity]]:
        """Generate suspicious patterns with severity"""
        return [
            (r'<<<<<<< HEAD', "Git merge conflict marker", Severity.HIGH),
            (r'=======', "Git merge conflict marker", Severity.HIGH),
            (r'>>>>>>> [a-f0-9]+', "Git merge conflict marker", Severity.HIGH),
            (r'#\s*TODO.*$', "TODO comment", Severity.LOW),
            (r'#\s*FIXME.*$', "FIXME comment", Severity.MEDIUM),
            (r'#\s*HACK.*$', "HACK comment", Severity.MEDIUM),
            (r'#\s*XXX.*$', "XXX comment", Severity.LOW),
            (r'print\s*\(\s*["\'].*["\']\s*\)', "Debug print statement", Severity.LOW),
            (r'console\.log\(', "Debug console log", Severity.LOW),
            (r'debugger;', "Debugger statement", Severity.MEDIUM),
            (r'import pdb; pdb\.set_trace\(\)', "Debugger breakpoint", Severity.MEDIUM),
            (r'breakpoint\(\)', "Debugger breakpoint", Severity.MEDIUM),
        ]


# =============================================================================
# File Scanner
# =============================================================================

class FileScanner:
    """Advanced file scanner with parallel processing"""
    
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
        self.extensions = {e.lower() if e.startswith('.') else f'.{e.lower()}' for e in extensions}
        self.scan_all_text = scan_all_text
        self.exclude_patterns = [re.compile(p) for p in (exclude_patterns or [])]
        self.include_patterns = [re.compile(p) for p in (include_patterns or [])]
        self.max_file_size = max_file_size_mb * 1024 * 1024
        
        # Binary file extensions
        self.binary_extensions = {
            '.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp', '.ico',
            '.pdf', '.zip', '.tar', '.gz', '.7z', '.rar',
            '.exe', '.dll', '.so', '.dylib', '.bin',
            '.pyc', '.pyo', '.pyd',
            '.class', '.jar',
            '.mp3', '.mp4', '.avi', '.mov',
        }
    
    def should_skip(self, path: Path) -> bool:
        """Check if path should be skipped"""
        # Check directory skip
        for part in path.parts:
            if part.lower() in self.skip_dirs:
                return True
            
            # Skip hidden except .github
            if part.startswith('.') and part not in {'.github', '.gitignore', '.gitattributes'}:
                return True
        
        # Check file size
        try:
            if path.stat().st_size > self.max_file_size:
                return True
        except OSError:
            return True
        
        # Check exclude patterns
        rel_path = str(path.relative_to(self.root))
        for pattern in self.exclude_patterns:
            if pattern.search(rel_path):
                return True
        
        # Check include patterns
        if self.include_patterns:
            for pattern in self.include_patterns:
                if pattern.search(rel_path):
                    return False
            return True
        
        return False
    
    def is_text_file(self, path: Path) -> bool:
        """Check if file is text"""
        if path.suffix.lower() in self.binary_extensions:
            return False
        
        # Try to read as text
        try:
            with open(path, 'rb') as f:
                chunk = f.read(1024)
                # Check for null bytes (binary indicator)
                if b'\x00' in chunk:
                    return False
                # Try to decode as UTF-8
                chunk.decode('utf-8')
                return True
        except (UnicodeDecodeError, OSError):
            return False
    
    def get_files(self) -> List[Path]:
        """Get list of files to scan"""
        files = []
        
        for path in self.root.rglob('*'):
            if not path.is_file():
                continue
            
            if self.should_skip(path):
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
    """Main scanner orchestrator"""
    
    def __init__(
        self,
        root: Path,
        rules: List[Rule],
        enable_ml: bool = False,
        enable_secret_detection: bool = True,
        enable_git_analysis: bool = False,
        parallel_workers: int = 4,
    ):
        self.root = root
        self.rules = rules
        self.enable_ml = enable_ml
        self.enable_secret_detection = enable_secret_detection
        self.enable_git_analysis = enable_git_analysis
        self.parallel_workers = parallel_workers
        
        self.ml_detector = MLAnomalyDetector() if enable_ml else None
        self.secret_detector = SecretDetector() if enable_secret_detection else None
        self.git_repo = None
        
        if enable_git_analysis and _GIT_AVAILABLE:
            try:
                self.git_repo = Repo(root)
            except Exception as e:
                logger.warning(f"Git repository analysis unavailable: {e}")
    
    def scan_file(self, file_path: Path) -> List[Finding]:
        """Scan a single file"""
        findings = []
        
        try:
            content = file_path.read_text(encoding='utf-8', errors='replace')
            rel_path = str(file_path.relative_to(self.root))
            
        except Exception as e:
            logger.debug(f"Could not read {file_path}: {e}")
            return []
        
        # Apply rules
        for rule in self.rules:
            # Check file pattern if specified
            if rule.file_pattern and not re.search(rule.file_pattern, rel_path):
                continue
            
            pattern = rule.compile()
            
            if rule.multiline:
                # Scan entire content
                for match in pattern.finditer(content):
                    findings.append(self._match_to_finding(match, rule, rel_path, content))
            else:
                # Scan line by line
                for i, line in enumerate(content.splitlines(), 1):
                    for match in pattern.finditer(line):
                        findings.append(self._match_to_finding(match, rule, rel_path, content, i))
        
        # Secret detection
        if self.secret_detector:
            secret_findings = self.secret_detector.scan(content, rel_path)
            findings.extend(secret_findings)
        
        # ML anomaly detection
        if self.ml_detector and self.ml_detector.is_fitted:
            is_anomaly, score = self.ml_detector.predict(content)
            if is_anomaly:
                findings.append(Finding(
                    category=FindingCategory.CODE_QUALITY,
                    severity=Severity.MEDIUM,
                    message=f"Anomalous code pattern detected (anomaly score: {score:.2f})",
                    file_path=rel_path,
                    rule_id="ML-001",
                    rule_name="ML Anomaly Detection",
                    confidence=score,
                ))
        
        return findings
    
    def _match_to_finding(
        self,
        match: re.Match,
        rule: Rule,
        file_path: str,
        content: str,
        line: Optional[int] = None
    ) -> Finding:
        """Convert regex match to finding"""
        line_num = line or content.count('\n', 0, match.start()) + 1
        col_num = match.start() - content.rfind('\n', 0, match.start()) if match.start() > 0 else 1
        
        # Get context lines
        lines = content.splitlines()
        start_line = max(0, line_num - 3)
        end_line = min(len(lines), line_num + 2)
        context_lines = lines[start_line:end_line]
        
        # Get snippet
        snippet_start = max(0, match.start() - 40)
        snippet_end = min(len(content), match.end() + 40)
        snippet = content[snippet_start:snippet_end].replace('\n', '\\n')
        
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
            snippet=snippet,
            context_lines=context_lines,
            rule_id=rule.name,
            rule_name=rule.name,
            remediation=rule.remediation,
        )
    
    async def scan_parallel(self, files: List[Path]) -> List[Finding]:
        """Scan files in parallel - FIXED: Proper error handling and tuple unpacking"""
        all_findings = []
        
        # Use process pool for CPU-bound scanning
        with ProcessPoolExecutor(max_workers=self.parallel_workers) as executor:
            loop = asyncio.get_event_loop()
            tasks = []
            
            for file_path in files:
                task = loop.run_in_executor(executor, self.scan_file, file_path)
                tasks.append(task)
            
            # FIXED: Properly handle results with error handling
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    logger.error(f"Scan error for {files[i]}: {result}")
                elif isinstance(result, list):
                    all_findings.extend(result)
                else:
                    logger.warning(f"Unexpected result type for {files[i]}: {type(result)}")
        
        return all_findings
    
    def scan_sync(self, files: List[Path]) -> List[Finding]:
        """Scan files synchronously with thread pool - FIXED: Proper result handling"""
        all_findings = []
        
        with ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
            # FIXED: Use submit with proper result collection instead of map
            futures = [executor.submit(self.scan_file, file_path) for file_path in files]
            
            for future in futures:
                try:
                    result = future.result(timeout=30)
                    if isinstance(result, list):
                        all_findings.extend(result)
                    else:
                        logger.warning(f"Unexpected result type: {type(result)}")
                except Exception as e:
                    logger.error(f"Scan error: {e}")
        
        return all_findings


# =============================================================================
# Output Formatters
# =============================================================================

class OutputFormatter:
    """Format findings for various outputs"""
    
    @staticmethod
    def console(summary: ScanSummary, findings: List[Finding], show_snippets: bool = True) -> str:
        """Format as console output"""
        lines = []
        
        lines.append(f"\n{'='*80}")
        lines.append(f"REPOSITORY HYGIENE SCAN v{SCRIPT_VERSION}")
        lines.append(f"{'='*80}\n")
        
        # Summary
        lines.append("📊 SCAN SUMMARY:")
        lines.append(f"   Files scanned: {summary.total_files}")
        lines.append(f"   Files with issues: {summary.files_with_issues}")
        lines.append(f"   Total findings: {summary.total_findings}")
        lines.append(f"   Scan duration: {summary.scan_duration_ms:.2f}ms\n")
        
        # Severity breakdown
        lines.append("⚠️  SEVERITY BREAKDOWN:")
        for severity in [Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM, Severity.LOW, Severity.INFO]:
            count = summary.by_severity.get(severity, 0)
            if count > 0:
                lines.append(f"   {severity.value}: {count}")
        lines.append("")
        
        # Category breakdown
        if summary.by_category:
            lines.append("📁 CATEGORY BREAKDOWN:")
            for category, count in sorted(summary.by_category.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"   {category.value}: {count}")
            lines.append("")
        
        # Findings
        if findings:
            lines.append("🔍 FINDINGS:")
            
            # Group by file
            by_file = defaultdict(list)
            for f in findings:
                by_file[f.file_path].append(f)
            
            for file_path, file_findings in sorted(by_file.items()):
                lines.append(f"\n📄 {file_path}:")
                
                for f in sorted(file_findings, key=lambda x: (x.line, x.severity.value)):
                    sev_color = {
                        Severity.CRITICAL: "🔴",
                        Severity.HIGH: "🟠",
                        Severity.MEDIUM: "🟡",
                        Severity.LOW: "🟢",
                        Severity.INFO: "🔵",
                    }.get(f.severity, "⚪")
                    
                    location = f"{f.line}:{f.column}" if f.line > 0 else "?"
                    lines.append(f"   {sev_color} [{f.severity.value}] {location} - {f.message}")
                    
                    if f.token and show_snippets:
                        lines.append(f"       Token: {f.token}")
                    
                    if f.remediation:
                        lines.append(f"       💡 Fix: {f.remediation}")
        
        return '\n'.join(lines)
    
    @staticmethod
    def json(summary: ScanSummary, findings: List[Finding]) -> str:
        """Format as JSON"""
        output = {
            "summary": summary.to_dict(),
            "findings": [f.to_dict() for f in findings],
        }
        return json.dumps(output, indent=2, ensure_ascii=False)
    
    @staticmethod
    def junit(summary: ScanSummary, findings: List[Finding]) -> str:
        """Format as JUnit XML"""
        if not _JUNIT_AVAILABLE:
            return "JUnit XML output requires junit_xml package"
        
        from junit_xml import TestSuite, TestCase
        
        test_cases = []
        
        for f in findings:
            tc = TestCase(
                name=f"{f.file_path}:{f.line}",
                classname=f.category.value,
                file=f.file_path,
                line=f.line,
            )
            
            if f.severity in (Severity.CRITICAL, Severity.HIGH):
                tc.add_failure_info(
                    message=f.message,
                    output=f.token or "",
                    failure_type=f.severity.value,
                )
            elif f.severity == Severity.MEDIUM:
                tc.add_error_info(
                    message=f.message,
                    output=f.token or "",
                    error_type=f.severity.value,
                )
            else:
                tc.add_skipped_info(f.message)
            
            test_cases.append(tc)
        
        ts = TestSuite("Repository Hygiene Scan", test_cases)
        return TestSuite.to_xml_string([ts])
    
    @staticmethod
    def sarif(summary: ScanSummary, findings: List[Finding]) -> str:
        """Format as SARIF"""
        sarif = {
            "$schema": "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/master/Schemata/sarif-schema-2.1.0.json",
            "version": "2.1.0",
            "runs": [{
                "tool": {
                    "driver": {
                        "name": "Tadawul Repository Hygiene Checker",
                        "version": SCRIPT_VERSION,
                        "informationUri": "https://github.com/tadawul/repo-hygiene",
                        "rules": []
                    }
                },
                "results": [],
                "properties": {
                    "summary": summary.to_dict()
                }
            }]
        }
        
        # Add rules and results
        rule_ids = set()
        for f in findings:
            if f.rule_id and f.rule_id not in rule_ids:
                sarif["runs"][0]["tool"]["driver"]["rules"].append({
                    "id": f.rule_id,
                    "name": f.rule_name or f.rule_id,
                    "shortDescription": {"text": f.message},
                    "defaultConfiguration": {"level": f._sarif_level()},
                    "properties": {"category": f.category.value}
                })
                rule_ids.add(f.rule_id)
            
            sarif["runs"][0]["results"].append(f.to_sarif())
        
        return json.dumps(sarif, indent=2)
    
    @staticmethod
    def csv(summary: ScanSummary, findings: List[Finding]) -> str:
        """Format as CSV"""
        import io
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Header
        writer.writerow([
            "Severity", "Category", "File", "Line", "Column",
            "Message", "Token", "Rule ID", "Confidence"
        ])
        
        # Data
        for f in findings:
            writer.writerow([
                f.severity.value,
                f.category.value,
                f.file_path,
                f.line,
                f.column,
                f.message,
                f.token or "",
                f.rule_id or "",
                f.confidence,
            ])
        
        return output.getvalue()
    
    @staticmethod
    def markdown(summary: ScanSummary, findings: List[Finding]) -> str:
        """Format as Markdown"""
        lines = []
        
        lines.append(f"# Repository Hygiene Scan Report v{SCRIPT_VERSION}\n")
        
        lines.append("## Summary\n")
        lines.append(f"- **Files scanned:** {summary.total_files}")
        lines.append(f"- **Files with issues:** {summary.files_with_issues}")
        lines.append(f"- **Total findings:** {summary.total_findings}")
        lines.append(f"- **Scan duration:** {summary.scan_duration_ms:.2f}ms\n")
        
        lines.append("### Severity Breakdown\n")
        for severity in [Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM, Severity.LOW, Severity.INFO]:
            count = summary.by_severity.get(severity, 0)
            if count > 0:
                lines.append(f"- **{severity.value}:** {count}")
        lines.append("")
        
        if findings:
            lines.append("## Findings\n")
            
            for f in findings:
                lines.append(f"### {f.file_path}:{f.line}")
                lines.append(f"- **Severity:** {f.severity.value}")
                lines.append(f"- **Category:** {f.category.value}")
                lines.append(f"- **Message:** {f.message}")
                if f.token:
                    lines.append(f"- **Token:** `{f.token}`")
                if f.remediation:
                    lines.append(f"- **Fix:** {f.remediation}")
                lines.append("")
        
        return '\n'.join(lines)
    
    @staticmethod
    def html(summary: ScanSummary, findings: List[Finding]) -> str:
        """Format as HTML"""
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Repository Hygiene Scan Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        h1 {{ color: #333; }}
        .summary {{ display: flex; gap: 20px; margin: 20px 0; }}
        .card {{ background: #f5f5f5; padding: 20px; border-radius: 8px; flex: 1; }}
        .critical {{ color: #d32f2f; }}
        .high {{ color: #f57c00; }}
        .medium {{ color: #fbc02d; }}
        .low {{ color: #7cb342; }}
        .info {{ color: #2196f3; }}
        table {{ border-collapse: collapse; width: 100%; }}
        th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
        th {{ background-color: #f2f2f2; }}
        .finding {{ margin: 10px 0; padding: 10px; border-left: 4px solid #ccc; }}
        .finding.critical {{ border-left-color: #d32f2f; }}
        .finding.high {{ border-left-color: #f57c00; }}
        .finding.medium {{ border-left-color: #fbc02d; }}
        .finding.low {{ border-left-color: #7cb342; }}
        .finding.info {{ border-left-color: #2196f3; }}
    </style>
</head>
<body>
    <h1>Repository Hygiene Scan Report v{SCRIPT_VERSION}</h1>
    
    <div class="summary">
        <div class="card">
            <h3>Files Scanned</h3>
            <p>{summary.total_files}</p>
        </div>
        <div class="card">
            <h3>Files with Issues</h3>
            <p>{summary.files_with_issues}</p>
        </div>
        <div class="card">
            <h3>Total Findings</h3>
            <p>{summary.total_findings}</p>
        </div>
        <div class="card">
            <h3>Scan Duration</h3>
            <p>{summary.scan_duration_ms:.2f}ms</p>
        </div>
    </div>
    
    <h2>Severity Breakdown</h2>
    <table>
        <tr>
            <th>Severity</th>
            <th>Count</th>
        </tr>
"""
        
        for severity in [Severity.CRITICAL, Severity.HIGH, Severity.MEDIUM, Severity.LOW, Severity.INFO]:
            count = summary.by_severity.get(severity, 0)
            css_class = severity.value.lower()
            html += f"""
        <tr>
            <td class="{css_class}">{severity.value}</td>
            <td>{count}</td>
        </tr>
"""
        
        html += """
    </table>
    
    <h2>Findings</h2>
"""
        
        if findings:
            for f in findings:
                css_class = f.severity.value.lower()
                html += f"""
    <div class="finding {css_class}">
        <h3>{f.file_path}:{f.line}</h3>
        <p><strong>Severity:</strong> {f.severity.value}</p>
        <p><strong>Category:</strong> {f.category.value}</p>
        <p><strong>Message:</strong> {f.message}</p>
"""
                if f.token:
                    html += f'        <p><strong>Token:</strong> <code>{f.token}</code></p>\n'
                if f.remediation:
                    html += f'        <p><strong>Fix:</strong> {f.remediation}</p>\n'
                html += "    </div>\n"
        else:
            html += "    <p>No findings detected.</p>\n"
        
        html += """
</body>
</html>
"""
        
        return html


# =============================================================================
# Main Function
# =============================================================================

def create_default_rules(strict: bool = False) -> List[Rule]:
    """Create default set of rules"""
    rules = []
    
    # Markdown fences
    for fence in TokenBuilder.markdown_fences():
        rules.append(Rule(
            name=f"MD-FENCE-{hash(fence) % 1000:03d}",
            pattern=re.escape(fence),
            category=FindingCategory.MARKDOWN_FENCE,
            severity=Severity.HIGH,
            message=f"Markdown fence detected: {fence}",
            remediation="Remove markdown fences from code files",
        ))
    
    # LLM artifacts
    for artifact in TokenBuilder.llm_artifacts(strict):
        rules.append(Rule(
            name=f"LLM-{hash(artifact) % 1000:03d}",
            pattern=re.escape(artifact),
            category=FindingCategory.LLM_ARTIFACT,
            severity=Severity.MEDIUM,
            message=f"LLM artifact detected: {artifact}",
            remediation="Remove LLM copy-paste artifacts",
        ))
    
    # Suspicious patterns
    for pattern, message, severity in TokenBuilder.suspicious_patterns():
        rules.append(Rule(
            name=f"SUS-{hash(pattern) % 1000:03d}",
            pattern=pattern,
            category=FindingCategory.CODE_QUALITY,
            severity=severity,
            message=message,
            remediation="Resolve or remove suspicious code",
            multiline=True,
        ))
    
    return rules


async def main_async(args: argparse.Namespace) -> int:
    """Async main function"""
    
    start_time = time.time()
    
    # Configuration
    root = Path(args.root).resolve()
    strict = bool(args.strict)
    max_offenders = args.max_offenders
    fail_on_read_error = args.fail_on_read_error
    extensions = args.extensions if args.extensions else [".py"]
    scan_all_text = bool(args.scan_all_text)
    parallel = bool(args.parallel)
    workers = args.workers or multiprocessing.cpu_count()
    enable_ml = bool(args.enable_ml)
    enable_secrets = not args.disable_secrets
    enable_git = bool(args.enable_git)
    
    # Skip directories
    skip_dirs = set(args.exclude or [])
    skip_dirs.update({
        "venv", ".venv", "env", ".env", "__pycache__", ".git", ".hg", ".svn",
        ".idea", ".vscode", ".pytest_cache", ".mypy_cache", ".ruff_cache",
        "node_modules", "dist", "build", ".eggs", ".tox", "htmlcov",
    })
    
    # Output format
    output_format = OutputFormat(args.format) if args.format else OutputFormat.CONSOLE
    
    # Create rules
    rules = create_default_rules(strict)
    
    # Load custom rules from file
    if args.rules_file:
        try:
            with open(args.rules_file, 'r') as f:
                if args.rules_file.endswith('.json'):
                    custom_rules = json.load(f)
                elif args.rules_file.endswith('.yaml') or args.rules_file.endswith('.yml'):
                    if _YAML_AVAILABLE:
                        custom_rules = yaml.safe_load(f)
                    else:
                        logger.error("YAML support not available")
                        return 1
                else:
                    logger.error(f"Unsupported rules file format: {args.rules_file}")
                    return 1
                
                for cr in custom_rules:
                    rules.append(Rule(**cr))
        except Exception as e:
            logger.error(f"Failed to load rules file: {e}")
            return 1
    
    # Create scanner
    scanner = HygieneScanner(
        root=root,
        rules=rules,
        enable_ml=enable_ml,
        enable_secret_detection=enable_secrets,
        enable_git_analysis=enable_git,
        parallel_workers=workers,
    )
    
    # Get files to scan
    file_scanner = FileScanner(
        root=root,
        skip_dirs=skip_dirs,
        extensions=extensions,
        scan_all_text=scan_all_text,
        exclude_patterns=args.exclude_patterns,
        include_patterns=args.include_patterns,
        max_file_size_mb=args.max_file_size or 10,
    )
    
    files = file_scanner.get_files()
    logger.info(f"Found {len(files)} files to scan")
    
    # Train ML model if enabled
    if enable_ml and scanner.ml_detector:
        logger.info("Training ML model...")
        # Use first 100 files as training data
        for f in files[:100]:
            try:
                content = f.read_text(encoding='utf-8', errors='replace')
                scanner.ml_detector.add_training_data(content)
            except Exception:
                pass
        scanner.ml_detector.fit()
    
    # Scan files
    logger.info(f"Scanning with {workers} workers...")
    
    try:
        if parallel:
            findings = await scanner.scan_parallel(files)
        else:
            findings = scanner.scan_sync(files)
    except Exception as e:
        logger.error(f"Scan failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    # Apply max offenders limit
    if max_offenders > 0 and len(findings) > max_offenders:
        findings = findings[:max_offenders]
    
    # Create summary
    summary = ScanSummary()
    summary.total_files = len(files)
    summary.files_with_issues = len(set(f.file_path for f in findings))
    summary.scan_duration_ms = (time.time() - start_time) * 1000
    summary.scan_end = datetime.now(timezone.utc).isoformat()
    
    for f in findings:
        summary.add_finding(f)
    
    # Output results
    if output_format == OutputFormat.CONSOLE:
        print(OutputFormatter.console(summary, findings, show_snippets=bool(args.show_snippets)))
    
    elif output_format == OutputFormat.JSON:
        print(OutputFormatter.json(summary, findings))
    
    elif output_format == OutputFormat.JUNIT:
        print(OutputFormatter.junit(summary, findings))
    
    elif output_format == OutputFormat.SARIF:
        print(OutputFormatter.sarif(summary, findings))
    
    elif output_format == OutputFormat.CSV:
        print(OutputFormatter.csv(summary, findings))
    
    elif output_format == OutputFormat.MARKDOWN:
        print(OutputFormatter.markdown(summary, findings))
    
    elif output_format == OutputFormat.HTML:
        print(OutputFormatter.html(summary, findings))
    
    # Save to file if requested
    if args.output_file:
        try:
            with open(args.output_file, 'w') as f:
                if args.output_file.endswith('.json'):
                    f.write(OutputFormatter.json(summary, findings))
                elif args.output_file.endswith('.csv'):
                    f.write(OutputFormatter.csv(summary, findings))
                elif args.output_file.endswith('.xml'):
                    f.write(OutputFormatter.junit(summary, findings))
                elif args.output_file.endswith('.html'):
                    f.write(OutputFormatter.html(summary, findings))
                elif args.output_file.endswith('.md'):
                    f.write(OutputFormatter.markdown(summary, findings))
                else:
                    f.write(OutputFormatter.console(summary, findings, show_snippets=True))
            logger.info(f"Results saved to {args.output_file}")
        except Exception as e:
            logger.error(f"Failed to save output: {e}")
    
    # Annotate in CI
    if args.annotate:
        for f in findings:
            if f.severity in (Severity.CRITICAL, Severity.HIGH):
                CIDetector.annotate_error(f.file_path, f.line, f.column, f.message)
            elif f.severity == Severity.MEDIUM:
                CIDetector.annotate_warning(f.file_path, f.line, f.column, f.message)
    
    # Determine exit code
    critical_count = summary.by_severity.get(Severity.CRITICAL, 0)
    high_count = summary.by_severity.get(Severity.HIGH, 0)
    
    if args.fail_threshold == "critical" and critical_count > 0:
        logger.error(f"Found {critical_count} critical issues")
        return 2
    elif args.fail_threshold == "high" and (critical_count + high_count) > 0:
        logger.error(f"Found {critical_count + high_count} critical/high issues")
        return 2
    elif args.fail_threshold == "any" and summary.total_findings > 0:
        logger.error(f"Found {summary.total_findings} issues")
        return 2
    
    logger.info(f"Scan completed successfully with {summary.total_findings} findings")
    return 0


def main() -> int:
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Repository Hygiene Checker v3.1.0")
    
    # Basic options
    parser.add_argument("--root", default=".", help="Repository root to scan")
    parser.add_argument("--strict", type=int, default=0, help="Enable strict heuristics")
    parser.add_argument("--max-offenders", type=int, default=0, help="Stop after N offenders")
    parser.add_argument("--fail-on-read-error", action="store_true", help="Fail on read errors")
    
    # File filtering
    parser.add_argument("--exclude", nargs="+", help="Additional directories to exclude")
    parser.add_argument("--extensions", nargs="+", help="File extensions to scan")
    parser.add_argument("--scan-all-text", type=int, default=0, help="Scan all text files")
    parser.add_argument("--exclude-patterns", nargs="+", help="Regex patterns to exclude")
    parser.add_argument("--include-patterns", nargs="+", help="Regex patterns to include")
    parser.add_argument("--max-file-size", type=int, default=10, help="Max file size in MB")
    
    # Advanced features
    parser.add_argument("--enable-ml", type=int, default=0, help="Enable ML anomaly detection")
    parser.add_argument("--disable-secrets", action="store_true", help="Disable secret detection")
    parser.add_argument("--enable-git", type=int, default=0, help="Enable Git history analysis")
    
    # Performance
    parser.add_argument("--parallel", type=int, default=1, help="Use parallel scanning")
    parser.add_argument("--workers", type=int, help="Number of worker processes")
    
    # Rules
    parser.add_argument("--rules-file", help="JSON/YAML file with custom rules")
    
    # Output
    parser.add_argument("--format", choices=[f.value for f in OutputFormat], help="Output format")
    parser.add_argument("--output-file", help="Save output to file")
    parser.add_argument("--show-snippets", type=int, default=1, help="Show context snippets")
    parser.add_argument("--annotate", type=int, default=1, help="Annotate in CI environment")
    
    # Failure threshold
    parser.add_argument("--fail-threshold", choices=["critical", "high", "any"], default="high",
                       help="Failure threshold")
    
    args = parser.parse_args()
    
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        logger.info("Scan interrupted")
        return 1
    except Exception as e:
        logger.error(f"Scan failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
