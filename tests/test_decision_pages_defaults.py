"""Decision-pages launch (2026-10-15): the owner-armed defaults.

The owner chose on 2026-10-15 to arm five default-off gates in code:

  opportunity_builder 1.28.2  TFB_OPP_VENUE_CAL_EXT, TFB_OPP_FRESHNESS_ALERT,
                              TFB_OPP_RESEARCH_DIVERSIFY  -> ON when unset
  portfolio_actions 1.17.0    TFB_PF_RESEARCH_VIEW        -> enforce when unset
  data_engine_v2 5.160.0      TFB_ENGINE_SUFFIX_LOCALE_FALLBACK -> ON when unset
  daily_sync.yml 1.0.5        TFB_SYNC_FW2_SCOPE fallback -> enforce (both writers)
  core_repair_readback        declared research view    -> enforce (lockstep)

tests/conftest.py pins the legacy values for the older batteries; every case
here deletes the five names first, so it sees the production defaults.

WORKS  D1-D6   unset reads armed; MWG.VN reads VND; workflow and readback
               declare the same defaults; verifier pins and flag defaults.
NEVER  K1-K4   every explicit off spelling disarms; a misspelt research view
               never arms; TFB_OPP_AUDIT_KEEP stays off; ASCII added lines.
GOLD   G1      against the pre-flip tree (TFB_TEST_LAUNCH_BASE = a directory
               holding `git show 39e708b3:<path>` for the three modules; 8c5b1e54
               before the 2026-10-21 renumbering): the
               same unset environment reads OFF on the base - the golden
               negative for "defaults armed".
               Current base (2026-10-27): `git show ac6888c1:<path>` for
               core/analysis/opportunity_builder.py,
               core/analysis/portfolio_actions.py and core/data_engine_v2.py,
               saved flat (builder 1.28.1, actions 1.16.1, engine 5.159.0).
               The older commits named above were the base before later
               renumberings and now fail G1's version checks.

Run:  python tests/test_decision_pages_defaults.py   (prints PASS k/k)
      pytest -q tests/test_decision_pages_defaults.py
"""
from __future__ import annotations

import importlib
import importlib.util
import os
import re
import sys
import tempfile
import traceback
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
ob = importlib.import_module("core.analysis.opportunity_builder")
pa = importlib.import_module("core.analysis.portfolio_actions")
de = importlib.import_module("core.data_engine_v2")

NAMES = ("TFB_OPP_VENUE_CAL_EXT", "TFB_OPP_FRESHNESS_ALERT",
         "TFB_OPP_RESEARCH_DIVERSIFY", "TFB_PF_RESEARCH_VIEW",
         "TFB_ENGINE_SUFFIX_LOCALE_FALLBACK", "TFB_OPP_AUDIT_KEEP")
OFF_WORDS = ("0", "false", "off", "no", "OFF", " 0 ")
BASE_DIR = os.environ.get("TFB_TEST_LAUNCH_BASE", "")


def _vt(text):
    return tuple(int(x) for x in str(text).split(".")[:3])


def _clean(mp):
    for name in NAMES:
        mp.delenv(name, raising=False)


def _armed(module_ob, module_pa, module_de):
    return {
        "venue_cal_ext": module_ob._env_venue_cal_ext(),
        "freshness_alert": module_ob._env_freshness_alert(),
        "research_diversify": module_ob._env_research_diversify(),
        "audit_keep": module_ob._env_audit_keep(),
        "research_view": module_pa._env_research_view_mode(),
        "suffix_fallback": module_de._suffix_locale_fallback_enabled(),
    }


# ---------------------------------------------------------------- WORKS ----
def case_d1_unset_reads_armed(mp, _tmp):
    _clean(mp)
    assert _armed(ob, pa, de) == {
        "venue_cal_ext": True, "freshness_alert": True,
        "research_diversify": True, "audit_keep": False,
        "research_view": "enforce", "suffix_fallback": True}
    mp.setenv("TFB_OPP_RESEARCH_DIVERSIFY", "")       # blank behaves as unset
    assert ob._env_research_diversify() is True
    mp.setenv("TFB_PF_RESEARCH_VIEW", "   ")          # blank behaves as unset
    assert pa._env_research_view_mode() == "enforce"


def case_d2_engine_labels_vn_by_default(mp, _tmp):
    _clean(mp)
    assert de._infer_currency_from_symbol("MWG.VN") == "VND"
    assert de._infer_exchange_from_symbol("MWG.VN") == "HOSE"
    assert de._infer_currency_from_symbol("AAPL") == "USD"
    assert de._infer_currency_from_symbol("BRK.B") == "USD"


def case_d3_workflow_fw2_defaults_enforce(_mp, _tmp):
    text = (ROOT / ".github" / "workflows" / "daily_sync.yml").read_text(encoding="utf-8")
    hits = re.findall(r"TFB_SYNC_FW2_SCOPE: \$\{\{ vars\.TFB_SYNC_FW2_SCOPE \|\| '(\w+)' \}\}", text)
    assert hits == ["enforce", "enforce"], hits      # sync-dashboard + recovery


def case_d4_readback_declares_enforce(_mp, _tmp):
    wf = (ROOT / ".github" / "workflows" / "core_repair_readback.yml").read_text(encoding="utf-8")
    assert "${{ vars.TFB_PF_RESEARCH_VIEW_EXPECT || 'enforce' }}" in wf
    src = (ROOT / "scripts" / "verify_core_repair_readback.py").read_text(encoding="utf-8")
    assert '"--expect-pf-research-view", default="enforce"' in src
    vr = importlib.import_module("scripts.verify_core_repair_readback")
    assert _vt(vr.SCRIPT_VERSION) >= (1, 2, 2)


def case_d5_versions_and_verifier(_mp, _tmp):
    assert _vt(ob.OPPORTUNITY_BUILDER_VERSION) >= (1, 28, 2)   # merge note 2026-10-27 (#779): the defaults build is 1.28.2 over #778's 1.28.1
    assert _vt(pa.PORTFOLIO_ACTIONS_VERSION) >= (1, 17, 0)
    assert _vt(de.__version__) >= (5, 160, 0)
    text = (ROOT / "scripts" / "verify_deployment.py").read_text(encoding="utf-8")
    for name in ("TFB_OPP_VENUE_CAL_EXT", "TFB_OPP_FRESHNESS_ALERT"):
        assert re.search(r"\('%s', '1'," % name, text), name


def case_d6_research_view_modes(mp, _tmp):
    _clean(mp)
    for value, want in (("observe", "observe"), ("enforce", "enforce"),
                        ("ENFORCE", "enforce"), ("off", "off")):
        mp.setenv("TFB_PF_RESEARCH_VIEW", value)
        assert pa._env_research_view_mode() == want, value


# ---------------------------------------------------------------- NEVER ----
def case_k1_explicit_off_disarms_every_gate(mp, _tmp):
    for word in OFF_WORDS:
        _clean(mp)
        for name in ("TFB_OPP_VENUE_CAL_EXT", "TFB_OPP_FRESHNESS_ALERT",
                     "TFB_OPP_RESEARCH_DIVERSIFY", "TFB_ENGINE_SUFFIX_LOCALE_FALLBACK"):
            mp.setenv(name, word)
        mp.setenv("TFB_PF_RESEARCH_VIEW", "off")
        got = _armed(ob, pa, de)
        assert got == {"venue_cal_ext": False, "freshness_alert": False,
                       "research_diversify": False, "audit_keep": False,
                       "research_view": "off", "suffix_fallback": False}, (word, got)
        assert de._infer_currency_from_symbol("MWG.VN") == "USD"   # v5.154.0 value


def case_k2_misspelt_research_view_never_arms(mp, _tmp):
    _clean(mp)
    for value in ("enforced", "1", "true", "yes", "bogus", "on"):
        mp.setenv("TFB_PF_RESEARCH_VIEW", value)
        assert pa._env_research_view_mode() == "off", value


def case_k3_audit_keep_stays_off(mp, _tmp):
    _clean(mp)
    assert ob._env_audit_keep() is False


def case_k4_ascii_added_lines(_mp, _tmp):
    for rel, start, end in (
            ("core/analysis/opportunity_builder.py", "# v1.28.2 (2026-10-15", 'OPPORTUNITY_BUILDER_VERSION = "'),
            ("core/analysis/portfolio_actions.py", "# v1.17.0 (2026-10-15)", 'PORTFOLIO_ACTIONS_VERSION = "'),
            ("core/data_engine_v2.py", "WHY v5.160.0 (2026-10-15", "WHY v5.159.0 (2026-10-14"),
            (".github/workflows/daily_sync.yml", "# VERSION 1.0.6 (2026-10-15", "# VERSION 1.0.5 (2026-10-14"),
            ("scripts/verify_core_repair_readback.py", "# v1.2.2 (2026-10-15)", "# v1.2.1 (2026-10-13)")):
        text = (ROOT / rel).read_text(encoding="utf-8")
        block = text[text.index(start):text.index(end, text.index(start))]
        assert block and all(ord(ch) < 128 for ch in block), rel


# ----------------------------------------------------------------- GOLD ----
def _load(path, name):
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def case_g1_golden_negative_base_reads_off(mp, _tmp):
    if not BASE_DIR:
        pytest.skip("TFB_TEST_LAUNCH_BASE unset")
    base = Path(BASE_DIR)
    bob = _load(base / "opportunity_builder.py", "ob_launch_base")
    bpa = _load(base / "portfolio_actions.py", "pa_launch_base")
    bde = _load(base / "data_engine_v2.py", "de_launch_base")
    # merge note 2026-10-21: the pre-flip copies are the #778 bundle head (git show
    # 39e708b3:<path>): builder 1.27.1, actions 1.16.0, engine 5.156.0 (renumbered).
    # merge note 2026-10-22: the bundle head is now c5133148 (main #813/#815/#816
    # merged): builder 1.27.3, actions 1.16.1 (#816), engine 5.157.0.
    # second merge note 2026-10-22: the bundle head is now b08d01cc (main #817
    # merged): builder 1.27.3, actions 1.16.1, engine 5.159.0.
    # 2026-10-24: the bundle head is 2462215c (main #821-#829 merged); the same
    # versions, so the assertions below are unchanged.
    # 2026-10-27 (second): the bundle head is ac6888c1 (main #854 merged); the
    # three module blobs equal 0d2f989c's, so the assertions below are unchanged.
    # 2026-10-27: the bundle head is 0d2f989c (main #844, #847-#853 merged):
    # builder 1.28.1 (renumbered over #848's 1.28.0), actions 1.16.1, engine
    # 5.159.0.
    assert bob.OPPORTUNITY_BUILDER_VERSION == "1.28.1"
    assert bpa.PORTFOLIO_ACTIONS_VERSION == "1.16.1"
    assert bde.__version__ == "5.159.0"
    _clean(mp)
    got = _armed(bob, bpa, bde)
    assert got == {"venue_cal_ext": False, "freshness_alert": False,
                   "research_diversify": False, "audit_keep": False,
                   "research_view": "off", "suffix_fallback": False}, got
    assert got != _armed(ob, pa, de)                   # the same env, head vs base


CASES = [value for _name, value in sorted(
    ((name, value) for name, value in globals().items()
     if name.startswith("case_") and callable(value)),
    key=lambda kv: ({"d": 0, "k": 1, "g": 2}[kv[0][5]],
                    int(re.sub(r"\D", "", kv[0].split("_")[1]) or 0), kv[0]))]


@pytest.mark.parametrize("case", CASES, ids=[c.__name__[5:] for c in CASES])
def test_decision_pages_defaults(case, monkeypatch, tmp_path):
    case(monkeypatch, tmp_path)


def test_case_count_is_self_consistent():
    names = [c.__name__ for c in CASES]
    assert len(names) == len(set(names)) == len(CASES)
    assert {n[5] for n in names} == {"d", "k", "g"}


def _main():
    passed = failed = skipped = 0
    for case in CASES:
        with pytest.MonkeyPatch.context() as mp, tempfile.TemporaryDirectory() as tmp:
            try:
                case(mp, Path(tmp))
            except pytest.skip.Exception as exc:
                skipped += 1
                print("SKIP " + case.__name__ + " :: " + str(exc))
                continue
            except Exception:  # noqa: BLE001
                failed += 1
                print("FAIL " + case.__name__)
                traceback.print_exc()
                continue
        passed += 1
        print("ok   " + case.__name__)
    total = passed + failed
    assert total + skipped == len(CASES)
    print("PASS %d/%d%s" % (passed, total, ("  (skipped %d)" % skipped) if skipped else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(_main())
