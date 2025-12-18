from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Optional, Tuple, Type

from pydantic import BaseModel, Field, create_model

from dynamic.loader import PageConfig


_TYPE_MAP: dict[str, Any] = {
    "str": str,
    "string": str,
    "int": int,
    "integer": int,
    "float": float,
    "number": float,
    "bool": bool,
    "boolean": bool,
    "decimal": Decimal,
    "date": date,
    "datetime": datetime,
    "any": Any,
}


def _safe_enum_member_name(value: Any) -> str:
    s = str(value).upper()
    out = []
    for ch in s:
        out.append(ch if (ch.isalnum() or ch == "_") else "_")
    name = "".join(out).strip("_")
    return name or "VALUE"


def _make_enum(enum_name: str, values: list[Any]) -> Type[Enum]:
    members: dict[str, Any] = {}
    for v in values:
        base = _safe_enum_member_name(v)
        key = base
        i = 2
        while key in members:
            key = f"{base}_{i}"
            i += 1
        members[key] = v
    return Enum(enum_name, members)  # type: ignore[arg-type]


def build_row_model(page: PageConfig) -> type[BaseModel]:
    """
    Build a Pydantic model dynamically based on YAML columns.
    """
    fields: dict[str, Tuple[Any, Any]] = {}

    for col in page.columns:
        if not isinstance(col, dict):
            continue

        name = str(col.get("name") or "").strip()
        if not name:
            continue

        t_raw = str(col.get("type") or "any").lower().strip()
        required = bool(col.get("required", False))

        label = col.get("label") or name
        description = col.get("description")

        # Enum support
        if isinstance(col.get("enum"), list) and col["enum"]:
            enum_cls = _make_enum(f"Enum_{page.page_id}_{name}", col["enum"])
            py_type = enum_cls
        else:
            py_type = _TYPE_MAP.get(t_raw, Any)

        # Optional if not required
        if not required:
            py_type = Optional[py_type]  # type: ignore[assignment]

        field_kwargs: dict[str, Any] = {"title": str(label)}
        if description:
            field_kwargs["description"] = str(description)

        # Common constraints
        if t_raw in ("str", "string"):
            if "min_length" in col:
                field_kwargs["min_length"] = int(col["min_length"])
            if "max_length" in col:
                field_kwargs["max_length"] = int(col["max_length"])
            if "pattern" in col:
                field_kwargs["pattern"] = str(col["pattern"])

        if t_raw in ("int", "integer", "float", "number", "decimal"):
            if "ge" in col:
                field_kwargs["ge"] = float(col["ge"])
            if "le" in col:
                field_kwargs["le"] = float(col["le"])

        default = Field(..., **field_kwargs) if required else Field(None, **field_kwargs)
        fields[name] = (py_type, default)

    model_name = f"RowModel_{page.page_id}_{page.schema_hash[:8]}"
    return create_model(model_name, __base__=BaseModel, **fields)  # type: ignore[arg-type]
