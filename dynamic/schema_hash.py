from __future__ import annotations

import hashlib
from pathlib import Path


def sha256_file(path: Path) -> str:
    """
    Stable schema versioning for YAML configs.
    Any YAML change -> new hash -> new row model name + tracked in history table.
    """
    data = path.read_bytes()
    return hashlib.sha256(data).hexdigest()
