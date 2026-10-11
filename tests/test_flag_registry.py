"""The flag inventory reads code rather than importing environment consumers."""
from pathlib import Path

from scripts.flag_registry import build_registry, render_markdown, scan_python


def test_direct_helper_fallback_and_dynamic_defaults_are_kept_distinct():
    source = '''
import os
def _mode(name, default):
    return os.getenv(name, default)
a = os.getenv("TFB_DIRECT", "off")
b = (os.getenv("TFB_FALLBACK") or "observe").lower()
c = _mode("TFB_HELPER", "enforce")
d = os.environ.get("TRACK_COUNTER", 0)
e = os.getenv("TFB_COMPUTED", calculate())
'''
    flags, unresolved = scan_python(source, "fixture.py")
    assert {item["name"]: item["default"] for item in flags} == {
        "TFB_DIRECT": "'off'", "TFB_FALLBACK": "'observe'",
        "TFB_HELPER": "'enforce'", "TRACK_COUNTER": "0", "TFB_COMPUTED": "computed",
    }
    assert unresolved == [{"source": "fixture.py", "line": 4}]


def test_registry_reconciles_missing_mapping_manifest_and_separate_lanes(tmp_path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    (scripts / "worker.py").write_text('import os\nos.getenv("TFB_WORK", "off")\nos.getenv("TFB_UNMAPPED", "0")\n')
    (scripts / "verify_deployment.py").write_text('FLAGS = [("TFB_WORK", "off", "description", False)]\n')
    core = tmp_path / "core"
    core.mkdir()
    (core / "engine.py").write_text('import os\nos.getenv("TFB_ENGINE", "gate")\n')
    workflows = tmp_path / ".github" / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "worker.yml").write_text("jobs:\n  work:\n    env:\n      TFB_WORK: ${{ vars.TFB_WORK || 'off' }}\n    steps:\n      - run: python scripts/worker.py\n")
    result = build_registry(tmp_path)
    flags = {row["name"]: row for row in result["flags"]}
    assert flags["TFB_WORK"]["manifest"] == {"default": "off", "kill_switch": False}
    assert flags["TFB_WORK"]["lanes"] == ["Actions: worker.yml"]
    assert flags["TFB_ENGINE"]["lanes"] == ["Render/library"]
    assert not flags["TFB_UNMAPPED"]["mappings"]
    assert flags["TFB_ENGINE"]["manifest"] is None
    rendered = render_markdown(result)
    assert "TFB_UNMAPPED" in rendered and "&#124;&#124;" in rendered


def test_scanner_never_imports_the_scanned_module(tmp_path):
    core = tmp_path / "core"
    core.mkdir()
    (core / "unsafe.py").write_text('raise RuntimeError("never import")\nimport os\nos.getenv("TFB_SAFE_SCAN", "off")\n')
    assert build_registry(Path(tmp_path))["flags"][0]["name"] == "TFB_SAFE_SCAN"


def test_import_aliases_and_named_flag_constants_resolve_without_guessing():
    source = '''
import os as _os
from os import getenv as read_environment
_FLAG_ENV: str = "TFB_CONST"
_os.getenv(_FLAG_ENV, "off")
read_environment("TFB_ALIAS", "observe")
'''
    flags, unresolved = scan_python(source, "fixture.py")
    assert {item["name"]: item["default"] for item in flags} == {
        "TFB_CONST": "'off'", "TFB_ALIAS": "'observe'"}
    assert not unresolved
