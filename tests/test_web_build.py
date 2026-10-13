"""Run the production build entrypoint against a small synthetic checkout."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def build_checkout(tmp_path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    script = scripts / "build_web.sh"
    shutil.copy2(ROOT / "scripts" / "build_web.sh", script)
    (tmp_path / "requirements.txt").write_text("synthetic-package==1.0\n")
    (tmp_path / "main.py").write_text("VALUE = 1\n")
    calls = tmp_path / "calls.jsonl"
    bootstrap = tmp_path / "fake_python"
    bootstrap.write_text(
        f"#!{sys.executable}\n"
        "import json, os, pathlib, shutil, subprocess, sys\n"
        'with open(os.environ["TFB_TEST_BUILD_CALLS"], "a") as stream:\n'
        '    stream.write(json.dumps(sys.argv[1:]) + "\\n")\n'
        'if sys.argv[1:3] == ["-m", "venv"]:\n'
        '    dest = pathlib.Path(sys.argv[3]) / "bin" / "python"\n'
        '    dest.parent.mkdir(parents=True, exist_ok=True)\n'
        '    shutil.copy2(__file__, dest)\n'
        'elif sys.argv[1:4] == ["-m", "pip", "check"]:\n'
        '    sys.exit(int(os.environ.get("TFB_TEST_PIP_CHECK_EXIT", "0")))\n'
        'elif sys.argv[1:3] == ["-m", "compileall"]:\n'
        f'    sys.exit(subprocess.call([{sys.executable!r}, *sys.argv[1:]]))\n'
    )
    bootstrap.chmod(0o755)
    env = dict(os.environ)
    for key in ("TFB_BUILD_VENV", "TFB_REQUIREMENTS_FILE", "TFB_TEST_PIP_CHECK_EXIT"):
        env.pop(key, None)
    env.update(TFB_BUILD_PYTHON=str(bootstrap), TFB_TEST_BUILD_CALLS=str(calls))
    return tmp_path, script, env, calls


def run_build(checkout):
    _, script, env, _ = checkout
    return subprocess.run([str(script)], env=env, cwd="/", text=True,
                          capture_output=True, timeout=10)


def build_calls(checkout):
    return [json.loads(line) for line in checkout[3].read_text().splitlines()]


def test_build_creates_environment_and_checks_dependencies(build_checkout):
    result = run_build(build_checkout)
    assert result.returncode == 0, result.stdout + result.stderr
    calls = build_calls(build_checkout)
    assert calls[0][:2] == ["-m", "venv"]
    assert ["-m", "pip", "install", "--no-cache-dir", "-r", "requirements.txt"] in calls
    assert ["-m", "pip", "check"] in calls
    assert calls[-1][:2] == ["-m", "compileall"]


def test_package_conflict_stops_build_before_compile(build_checkout):
    build_checkout[2]["TFB_TEST_PIP_CHECK_EXIT"] = "1"
    result = run_build(build_checkout)
    assert result.returncode == 1
    assert build_calls(build_checkout)[-1] == ["-m", "pip", "check"]
    assert "Web build completed" not in result.stdout


def test_render_requirements_preferred_and_override_preserved(build_checkout):
    root, _, env, _ = build_checkout
    (root / "requirements.render.txt").write_text("synthetic-render-package==1.0\n")
    result = run_build(build_checkout)
    assert result.returncode == 0, result.stdout + result.stderr
    assert ["-m", "pip", "install", "--no-cache-dir", "-r", "requirements.render.txt"] in build_calls(build_checkout)
    env["TFB_REQUIREMENTS_FILE"] = "requirements.txt"
    result = run_build(build_checkout)
    assert result.returncode == 0, result.stdout + result.stderr
    assert build_calls(build_checkout)[-3][-1] == "requirements.txt"


def test_compile_ignores_environment_and_work_scratch(build_checkout):
    root, _, _, _ = build_checkout
    for directory in (".venv", "venv", "work", ".pytest_cache"):
        target = root / directory
        target.mkdir(exist_ok=True)
        (target / "broken.py").write_text("def invalid syntax\n")
    result = run_build(build_checkout)
    assert result.returncode == 0, result.stdout + result.stderr


def test_compile_failure_in_application_source_fails_build(build_checkout):
    root, _, _, _ = build_checkout
    (root / "main.py").write_text("def invalid syntax\n")
    result = run_build(build_checkout)
    assert result.returncode != 0
    assert "Web build completed" not in result.stdout
