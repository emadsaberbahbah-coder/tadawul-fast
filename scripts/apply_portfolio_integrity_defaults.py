#!/usr/bin/env python3
"""Enable conservative portfolio integrity defaults with exact assertions.

This transformer is intentionally narrow and idempotent.  It changes only
existing kill-switch defaults; operators can still restore the prior behaviour
by setting the corresponding environment variable to 0.
"""
from pathlib import Path

PATH = Path("core/analysis/portfolio_actions.py")

REPLACEMENTS = {
    'str(_env_str("TFB_PF_IDENTITY_GATE", "0"))':
        'str(_env_str("TFB_PF_IDENTITY_GATE", "1"))',
    'str(_env_str("TFB_PF_VF_CONFLICT_GUARD", "0"))':
        'str(_env_str("TFB_PF_VF_CONFLICT_GUARD", "1"))',
    'str(_env_str("TFB_PF_ENGINE_ROI_DISPLAY", "0"))':
        'str(_env_str("TFB_PF_ENGINE_ROI_DISPLAY", "1"))',
    'os.environ.get("TFB_PA_SUKUK_ASSET_CLASS", "0")':
        'os.environ.get("TFB_PA_SUKUK_ASSET_CLASS", "1")',
}


def main() -> None:
    text = PATH.read_text(encoding="utf-8")
    changed = False
    for old, new in REPLACEMENTS.items():
        old_count = text.count(old)
        new_count = text.count(new)
        if old_count == 1:
            text = text.replace(old, new, 1)
            changed = True
        elif old_count == 0 and new_count == 1:
            continue  # already applied
        else:
            raise SystemExit(
                f"unsafe source shape for {old!r}: old={old_count}, new={new_count}"
            )

    # Keep documentation truthful without weakening the assertion-heavy edit.
    doc_replacements = {
        "master switch for the identity (ghost-ticker) BLOCK. DEFAULT OFF":
            "master switch for the identity (ghost-ticker) BLOCK. DEFAULT ON",
        "DEFAULT OFF (opt-in) — set TFB_PF_IDENTITY_GATE=1":
            "DEFAULT ON — set TFB_PF_IDENTITY_GATE=0 to disable; it blocks",
        "DEFAULT OFF (opt-in) — set TFB_PF_VF_CONFLICT_GUARD=1":
            "DEFAULT ON — set TFB_PF_VF_CONFLICT_GUARD=0 to disable; it withholds",
        "engine-forecast display toggle. Default OFF; set":
            "engine-forecast display toggle. Default ON; set",
    }
    for old, new in doc_replacements.items():
        if old in text:
            text = text.replace(old, new, 1)

    if changed:
        PATH.write_text(text, encoding="utf-8")

    final = PATH.read_text(encoding="utf-8")
    for new in REPLACEMENTS.values():
        if final.count(new) != 1:
            raise SystemExit(f"postcondition failed for {new!r}")
    for old in REPLACEMENTS:
        if old in final:
            raise SystemExit(f"legacy unsafe default remains: {old!r}")

    print("portfolio integrity defaults verified")


if __name__ == "__main__":
    main()
