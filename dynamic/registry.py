from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple, Type

from pydantic import BaseModel

from dynamic.loader import PageConfig, load_page_config
from dynamic.factory import build_row_model


@dataclass(frozen=True)
class RegisteredPage:
    page: PageConfig
    row_model: Type[BaseModel]


# Cache: (page_id, schema_hash) -> RegisteredPage
_CACHE: Dict[Tuple[str, str], RegisteredPage] = {}


def get_registered_page(page_id: str) -> RegisteredPage:
    """
    Loads YAML and returns (PageConfig + dynamic Pydantic row model).
    Cached by schema_hash so a YAML change creates a new model immediately.
    """
    page = load_page_config(page_id)
    key = (page.page_id, page.schema_hash)

    cached = _CACHE.get(key)
    if cached:
        return cached

    model = build_row_model(page)
    reg = RegisteredPage(page=page, row_model=model)
    _CACHE[key] = reg

    # Keep cache small: drop older versions for same page_id
    old_keys = [k for k in _CACHE.keys() if k[0] == page_id and k != key]
    for k in old_keys:
        _CACHE.pop(k, None)

    return reg
