from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

from config import get_settings
from dynamic.registry import get_registered_page


ALLOWED_REGIONS = {"ksa", "global"}


def _warn(msg: str) -> None:
    print(f"⚠️  {msg}")


def _fail(msg: str) -> None:
    print(f"❌ {msg}")


def _ok(msg: str) -> None:
    print(f"✅ {msg}")


def _validate_columns(raw_columns: List[Dict[str, Any]], page_id: str) -> List[str]:
    errors: List[str] = []
    for i, col in enumerate(raw_columns):
        if not isinstance(col, dict):
            errors.append(f"{page_id}: columns[{i}] is not a dict")
            continue

        name = str(col.get("name") or "").strip()
        ctype = str(col.get("type") or "").strip().lower()
        if not name:
            errors.append(f"{page_id}: columns[{i}] missing 'name'")
        if not ctype:
            errors.append(f"{page_id}: columns[{i}] missing 'type'")

        # Optional checks
        if "enum" in col and not isinstance(col["enum"], list):
            errors.append(f"{page_id}: columns[{i}] enum must be a list")

        if col.get("required") not in (None, True, False):
            errors.append(f"{page_id}: columns[{i}] 'required' must be boolean")

    return errors


def main() -> int:
    settings = get_settings()
    pages_dir = Path(settings.dynamic_pages_dir).resolve()

    print("===============================================")
    print("Dynamic Pages Validator (Phase-1)")
    print("===============================================")
    print(f"DYNAMIC_PAGES_DIR = {pages_dir}")
    print("")

    if not pages_dir.exists():
        _fail(f"Dynamic pages dir not found: {pages_dir}")
        return 2

    yaml_files = sorted(list(pages_dir.glob("*.yaml")) + list(pages_dir.glob("*.yml")))
    if not yaml_files:
        _fail(f"No YAML files found in {pages_dir}")
        return 2

    errors_total: List[str] = []
    checked: List[Tuple[str, str, int]] = []  # (page_id, region, column_count)

    for fp in yaml_files:
        page_id = fp.stem
        try:
            reg = get_registered_page(page_id)
            page = reg.page

            # Region check
            region = (page.region or "").lower().strip()
            if region not in ALLOWED_REGIONS:
                errors_total.append(
                    f"{page_id}: region='{region}' invalid (allowed: {sorted(ALLOWED_REGIONS)})"
                )

            # Columns structure checks (light)
            errors_total.extend(_validate_columns(page.columns, page_id))

            # Model build sanity
            model = reg.row_model
            fields = list(model.model_fields.keys())

            checked.append((page_id, region, len(fields)))
            _ok(f"{page_id}: region={region}, columns={len(fields)}, schema_hash={page.schema_hash[:12]}...")

            if len(fields) == 0:
                _warn(f"{page_id}: model has 0 fields (check YAML columns)")

        except Exception as e:
            errors_total.append(f"{page_id}: failed to load/build model: {e}")

    print("")
    print("-----------------------------------------------")
    print("Summary")
    print("-----------------------------------------------")
    print(f"Pages found: {len(yaml_files)} | Pages checked: {len(checked)}")

    if errors_total:
        print("")
        _fail(f"Validation failed with {len(errors_total)} issue(s):")
        for msg in errors_total:
            print(f"  - {msg}")
        return 1

    print("")
    _ok("All dynamic pages validated successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
