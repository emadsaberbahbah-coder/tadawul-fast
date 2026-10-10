"""Offline coverage for the smoke CLI; server requests are explicit CLI work."""
import subprocess
import sys
from pathlib import Path


def test_live_smoke_cli_help_does_not_contact_a_server():
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([sys.executable, str(root / "scripts/smoke_endpoints.py"), "--help"],
                            capture_output=True, text=True, cwd=root, timeout=20)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "--only" in result.stdout and "--skip-sheet-rows" in result.stdout


if __name__ == "__main__":
    raise SystemExit(subprocess.call([sys.executable, str(Path(__file__).resolve().parents[1] / "scripts/smoke_endpoints.py"), *sys.argv[1:]]))
