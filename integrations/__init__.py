#!/usr/bin/env python3
# integrations/__init__.py
"""
integrations — External service integrations (v1.0.0)

    - google_sheets_service       : Google Sheets API service layer
    - google_apps_script_client   : HTTP client for Apps Script endpoints
    - setup_credentials           : credential resolution (env / file / base64)
    - symbols_reader              : schema-safe symbol extraction from
                                    rows, payloads, files, and page
                                    fallback defaults

This __init__ is intentionally empty of runtime imports so boot stays
fast and missing dependencies (e.g. gspread when not installed locally)
never block FastAPI startup. Consumers import modules directly.
"""

from __future__ import annotations

__version__ = "1.0.0"
__all__: list[str] = []
