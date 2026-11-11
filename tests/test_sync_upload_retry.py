"""daily_sync 1.0.14: the sync's log upload is tried twice (run 43086606380 of 11 Nov 2026 went red on one failed
upload after its data was written; the outcome audit then missed the Global_Markets verdict and the release stopped)."""
from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_log_upload_is_tried_twice_and_a_double_failure_still_fails():
    raw = (ROOT / ".github/workflows/daily_sync.yml").read_text(encoding="utf-8")
    assert "# VERSION 1.0.14 (2026-11-12) -- A LOG UPLOAD IS TRIED TWICE" in raw
    steps = yaml.safe_load(raw)["jobs"]["sync-dashboard"]["steps"]
    ups = [s for s in steps if "Upload Logs" in str(s.get("name"))]
    assert [s["name"] for s in ups] == ["📤 Upload Logs", "📤 Upload Logs (second try)"]
    first, second = ups
    assert first["id"] == "upload_logs" and first["continue-on-error"] is True and first["if"] == "always()"
    assert second["if"] == "always() && steps.upload_logs.outcome == 'failure'"
    assert "continue-on-error" not in second                                  # two failures fail the job
    assert second["with"]["overwrite"] is True
    for key in ("name", "path", "retention-days"):
        assert first["with"][key] == second["with"][key], key                  # the audit finds the same artifact
    assert steps.index(second) == steps.index(first) + 1
    assert first["with"]["name"] == "tadawul-sync-logs-${{ github.run_id }}-${{ matrix.group }}"
