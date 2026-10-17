"""G9 (2026-10-17): audit_provider_target_coverage v1.3.0 -- the published seed
baseline ends the CH_BASELINE_EMPTY deadlock (runs #66-#73). Pins the seed
file itself (schema, policy hash, pages, sheet mask), the pure fallback
rules, the live-baseline hand-off through save_last_good, the CLI/main wiring
with a stubbed reader, and the workflow's --seed line."""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from scripts import audit_provider_target_coverage as ptc

ROOT = Path(__file__).resolve().parents[1]
SEED_PATH = ROOT / "config/provider_target_baseline_seed.json"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in list(__import__("os").environ):
        if k.startswith("TFB_PTC_"):
            monkeypatch.delenv(k, raising=False)


def grid(share=50.0, rows=200):
    return [["Symbol", "Forecast Source"]] + [
        [f"S{i}", "provider_target" if i < rows * share / 100 else "phase_ii_synthetic"] for i in range(rows)]


def test_published_seed_file_matches_the_policy_and_the_17_oct_observation():
    seed = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    assert seed["schema"] == ptc.STATE_SCHEMA
    assert seed["config_hash"] == ptc.Config().hash() == ptc.load_config().hash() == "9a644ee2170c"
    assert set(seed["pages"]) == set(ptc.DEFAULT_PAGES)
    assert seed["pages"]["Global_Markets"] == {"share_pct": 51.78, "rows": 6609, "provider": 3422}
    assert seed["pages"]["Market_Leaders"] == {"share_pct": 34.12, "rows": 255, "provider": 87}
    for page in ("Commodities_FX", "Mutual_Funds"):
        assert seed["pages"][page]["share_pct"] == 0.0 and seed["pages"][page]["provider"] == 0
        assert page in ptc.Config().zero_pages
    assert seed["sheet"] == "1QaOS...wsG04" and len(seed["sheet"]) == 13
    assert seed["seed"]["source_run_id"] == 39026626201 and seed["seed"]["published"] == "2026-10-17"
    assert ptc.load_state(str(SEED_PATH)) == (seed, None)


def _seed(cfg):
    return {"schema": ptc.STATE_SCHEMA, "generated_at_utc": "2026-10-17T10:54:23+00:00", "sheet": "***",
            "config_hash": cfg.hash(), "seed": {"published": "2026-10-17", "source_run_id": 39026626201},
            "pages": {"GM": {"share_pct": 50.0, "rows": 200, "provider": 100}}}


def test_apply_seed_rules():
    cfg = ptc.Config(); seed = _seed(cfg)
    live = {"schema": 2, "pages": {"GM": {"share_pct": 55.0, "rows": 200}}}
    assert ptc.apply_seed(None, seed, False, False)[1] is True
    assert ptc.apply_seed({"schema": 2, "pages": {}}, seed, False, False)[1] is True
    assert ptc.apply_seed({"schema": 2}, seed, False, False)[1] is True
    assert ptc.apply_seed(live, seed, False, False) == (live, False)
    assert ptc.apply_seed(None, seed, True, False) == (None, False)      # bootstrap writes explicitly
    assert ptc.apply_seed(None, seed, False, True) == (None, False)      # accept writes explicitly
    assert ptc.apply_seed(None, None, False, False) == (None, False)
    assert ptc.apply_seed(None, {}, False, False) == (None, False)
    eff, seeded = ptc.apply_seed(None, seed, False, False)
    assert seeded and eff["seed"]["applied"] is True and eff["pages"] == seed["pages"]
    assert "applied" not in seed["seed"], "the seed dict itself is not mutated"


def test_seeded_run_passes_warns_and_hands_off_a_live_baseline(tmp_path):
    cfg = ptc.Config(); seed = _seed(cfg)
    state_path = tmp_path / "last_good.json"
    state_path.write_text(json.dumps({"schema": 2, "generated_at_utc": "2026-08-18T07:08:35+00:00",
                                      "sheet": "***", "pages": {}}))     # the real 341-byte deadlock file
    empty, err = ptc.load_state(str(state_path))
    assert err is None and empty["pages"] == {}
    rep = ptc.audit_pages({"GM": grid(52)}, empty, cfg, seed_state=seed)
    assert rep.exit_code == 0 and rep.baseline_source == "seed"
    codes = [f.code for f in rep.findings]
    assert codes == ["PTC_BASELINE_SEEDED"] and rep.findings[0].severity == "WARN"
    assert "source run 39026626201" in rep.findings[0].message
    assert rep.pages["GM"]["verdict"] == "OK" and rep.pages["GM"]["baseline_pct"] == 50.0
    assert getattr(rep, "_next_pages")["GM"]["share_pct"] == 52.0        # healthy upward advance
    assert ptc.save_last_good(str(state_path), rep, "***") == "written"
    live = json.loads(state_path.read_text())
    assert live["pages"]["GM"]["share_pct"] == 52.0
    assert live["generated_at_utc"] == rep.generated_at_utc            # not the 2026-08-18 stamp
    assert live.get("last_verified_at_utc") in (None, rep.generated_at_utc)
    assert "seed" not in live
    # next day: the live reference wins, the seed is silent
    rep2 = ptc.audit_pages({"GM": grid(52)}, live, cfg, seed_state=seed)
    assert rep2.baseline_source == "cache" and rep2.exit_code == 0 and not rep2.findings


def test_seeded_run_still_alerts_on_a_collapse_and_never_persists_it(tmp_path):
    cfg = ptc.Config(); seed = _seed(cfg)
    rep = ptc.audit_pages({"GM": grid(10)}, None, cfg, seed_state=seed)
    assert rep.exit_code == 1 and rep.baseline_source == "seed"
    assert {f.code for f in rep.findings} >= {"PTC_BASELINE_SEEDED"} and any(f.severity == "FAIL" for f in rep.findings)
    assert getattr(rep, "_next_pages")["GM"]["share_pct"] == 50.0
    path = tmp_path / "s.json"
    assert ptc.save_last_good(str(path), rep, "***") == "kept: coverage failure"
    assert not path.exists()


@pytest.mark.parametrize("mutation,code", (
    ({"config_hash": "deadbeef0000"}, "CH_BASELINE_POLICY_MISMATCH"),
    ({"schema": 1}, "CH_BASELINE_MISMATCH"),
    ({"sheet": "OTHER...SHEET"}, "CH_BASELINE_MISMATCH"),
    ({"pages": {}}, "CH_BASELINE_EMPTY"),
))
def test_a_mismatched_seed_is_a_control_failure_not_a_comparison(mutation, code):
    cfg = ptc.Config(); seed = {**_seed(cfg), "sheet": "1QaOS...wsG04", **mutation}
    rep = ptc.audit_pages({"GM": grid(50)}, None, cfg, spreadsheet="1QaOS...wsG04", seed_state=seed)
    assert rep.exit_code == 2 and code in {f.code for f in rep.findings}
    assert rep.pages["GM"]["verdict"] == "NO_BASELINE"


def test_seed_age_is_not_a_staleness_control():
    cfg = ptc.Config(); seed = {**_seed(cfg), "generated_at_utc": "2026-01-01T00:00:00+00:00"}
    rep = ptc.audit_pages({"GM": grid(50)}, None, cfg, seed_state=seed)
    assert rep.exit_code == 0 and [f.code for f in rep.findings] == ["PTC_BASELINE_SEEDED"]
    # ...while a stale LIVE reference still is
    stale = {**_seed(cfg)}; stale.pop("seed")
    rep2 = ptc.audit_pages({"GM": grid(50)}, {**stale, "generated_at_utc": "2026-01-01T00:00:00+00:00"}, cfg)
    assert rep2.exit_code == 2 and "CH_BASELINE_STALE" in {f.code for f in rep2.findings}


def test_main_wires_the_seed_and_reports_unreadable_seeds(tmp_path, monkeypatch):
    cfg = ptc.Config()
    seed_path = tmp_path / "seed.json"; seed_path.write_text(json.dumps({**_seed(cfg), "sheet": "1QaOS...wsG04"}))
    state_path = tmp_path / "state.json"
    out = tmp_path / "out.json"; obs = tmp_path / "obs.json"

    def reader(sid, a1):
        return grid(50) if a1.startswith("GM!") else []
    monkeypatch.setattr(ptc, "resolve_reader", lambda: reader)
    rc = ptc.main(["--sheet-id", "1QaOSNHqKlCaqlnaB45O4RlajWeMNe5cAE-r2HGwsG04", "--pages", "GM",
                   "--state", str(state_path), "--seed", str(seed_path),
                   "--json-out", str(out), "--observation-out", str(obs)])
    report = json.loads(out.read_text())
    assert rc == 0 and report["baseline_source"] == "seed" and report["persist_status"] == "written"
    assert json.loads(state_path.read_text())["pages"]["GM"]["share_pct"] == 50.0
    # second run: live baseline, no seed finding
    rc2 = ptc.main(["--sheet-id", "1QaOSNHqKlCaqlnaB45O4RlajWeMNe5cAE-r2HGwsG04", "--pages", "GM",
                    "--state", str(state_path), "--seed", str(seed_path),
                    "--json-out", str(out), "--observation-out", str(obs)])
    report2 = json.loads(out.read_text())
    assert rc2 == 0 and report2["baseline_source"] == "cache" and report2["findings"] == []
    # unreadable seed with a missing reference: control failure + the seed warning
    rc3 = ptc.main(["--sheet-id", "1QaOSNHqKlCaqlnaB45O4RlajWeMNe5cAE-r2HGwsG04", "--pages", "GM",
                    "--state", str(tmp_path / "none.json"), "--seed", str(tmp_path / "missing.json"),
                    "--json-out", str(out), "--observation-out", str(obs)])
    codes = {f["code"] for f in json.loads(out.read_text())["findings"]}
    assert rc3 == 2 and {"PTC_SEED_UNAVAILABLE", "CH_BASELINE_MISSING"} <= codes
    # no --seed: v1.2.0 behaviour exactly
    rc4 = ptc.main(["--sheet-id", "1QaOSNHqKlCaqlnaB45O4RlajWeMNe5cAE-r2HGwsG04", "--pages", "GM",
                    "--state", str(tmp_path / "none2.json"), "--json-out", str(out), "--observation-out", str(obs)])
    assert rc4 == 2 and json.loads(out.read_text())["baseline_source"] == "none"


def test_workflow_passes_the_seed_and_watches_its_paths():
    wf = yaml.load((ROOT / ".github/workflows/provider_target_coverage.yml").read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    step = [s for s in wf["jobs"]["audit"]["steps"] if s.get("name") == "Audit provider-target coverage"][0]
    assert "--seed config/provider_target_baseline_seed.json" in step["run"]
    for trig in ("push", "pull_request"):
        assert "config/provider_target_baseline_seed.json" in wf["on"][trig]["paths"]
        assert "tests/test_ptc_seed_baseline_g9.py" in wf["on"][trig]["paths"]
    assert "W1B-1 v1.2.0 (2026-10-17) -- PUBLISHED SEED BASELINE" in (ROOT / ".github/workflows/provider_target_coverage.yml").read_text(encoding="utf-8")
