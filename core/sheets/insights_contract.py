"""Supported Insights report columns, without route or provider imports.

The sync writer accepts exactly the current report or its explicit legacy
predecessor, preserving the caller's column order. Match that boundary in
independent literal-sheet audits; never accept a mixture of the two reports.
"""
from __future__ import annotations

import re
from typing import Sequence


INSIGHTS_PAGE = "Insights_Analysis"
_CONTRACT_KEYS = (
    ("section", "item", "metric", "value", "notes", "source", "sort_order"),
    ("section", "item", "symbol", "metric", "value", "notes", "last_updated_riyadh"),
)


def header_token(value: object) -> str:
    """Match the writer's header normalization, including display/key aliases."""
    return re.sub(r"[^a-z0-9]+", "", str(value if value is not None else "").lower())


def aligned_keys(headers: Sequence[object]) -> list[str] | None:
    """Return canonical keys in physical order for one complete known report."""
    if len(headers) != 7 or any(not isinstance(header, str) or not header.strip() for header in headers):
        return None
    tokens = [header_token(header) for header in headers]
    if len(set(tokens)) != len(tokens):
        return None
    for contract in _CONTRACT_KEYS:
        mapping = {header_token(key): key for key in contract}
        if set(tokens) == set(mapping):
            return [mapping[token] for token in tokens]
    return None


def trim_headers(headers: Sequence[object]) -> list[object]:
    """Drop unpopulated sheet columns while preserving literal cell types."""
    cells = list(headers)
    while cells and (cells[-1] is None or isinstance(cells[-1], str) and not cells[-1].strip()):
        cells.pop()
    return cells


def find_header_row(grid: Sequence[object], scan: int) -> int:
    """Prefer a complete report header over a preamble with shared labels."""
    return next((index for index, row in enumerate(grid[:scan])
                 if isinstance(row, (list, tuple)) and aligned_keys(trim_headers(row)) is not None), -1)
