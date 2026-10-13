#!/usr/bin/env bash
# Canonical Render build: install the selected requirements, enforce package
# compatibility, and compile application sources without traversing environments.
set -Eeuo pipefail

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BUILD_VENV="${TFB_BUILD_VENV:-$REPO_ROOT/.venv}"
PYTHON_BOOTSTRAP="${TFB_BUILD_PYTHON:-python}"
if [ ! -x "$BUILD_VENV/bin/python" ]; then
  "$PYTHON_BOOTSTRAP" -m venv "$BUILD_VENV"
fi
BUILD_PYTHON="$BUILD_VENV/bin/python"

REQUIREMENTS_FILE="${TFB_REQUIREMENTS_FILE:-}"
if [ -z "$REQUIREMENTS_FILE" ]; then
  if [ -f requirements.render.txt ]; then
    REQUIREMENTS_FILE=requirements.render.txt
  else
    REQUIREMENTS_FILE=requirements.txt
  fi
fi
if [ ! -f "$REQUIREMENTS_FILE" ]; then
  printf 'ERROR: requirements file not found: %s\n' "$REQUIREMENTS_FILE" >&2
  exit 2
fi

printf 'Installing web requirements from %s\n' "$REQUIREMENTS_FILE"
"$BUILD_PYTHON" -m pip install --upgrade pip
"$BUILD_PYTHON" -m pip install --no-cache-dir -r "$REQUIREMENTS_FILE"
"$BUILD_PYTHON" -m pip check
"$BUILD_PYTHON" -m compileall -q -f \
  -x '(^|[/\\])(\.venv|venv|work|__pycache__|\.git|\.pytest_cache|\.mypy_cache|\.ruff_cache|node_modules)([/\\]|$)' .
printf 'Web build completed\n'
