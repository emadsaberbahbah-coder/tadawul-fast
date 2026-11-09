"""run_shadow_scorer v1.9.4 [SEED-NEW-SEATS] - the REAL module's snapshot rule.

WHY (9 Nov 2026): the board rotated over the weekend to five new names; the
non-trading rows stored no price for them, so Monday could not pair any seat:
DAY_EXCLUDED_INFRA fresh=0/5 new=[EFC.US,MBIN.US,PFS.US,SPFI.US,ZTO.US].
  N1 off (default): the 1.9.3 rule exactly - fresh bar, else carried price,
     else nothing (the new seat stays unpriced)
  N2 on, non-trading day: a new seat stores its latest close as the base and is
     named in `seeded`; a fresh bar and a carried price always win
  N3 on, trading day: nothing is seeded (a stale bar on a session day is not a base)
  N4 on, a new seat with no fetched price stays unpriced (nothing invented)
  N5 main() stores through the helper and names the seeded seats in the note
  N6 gate values; version pin; the embedded selftest battery still passes
"""
import inspect
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from scripts import run_shadow_scorer as s1  # noqa: E402

FRI, SAT, SUN = date(2026, 11, 6), date(2026, 11, 7), date(2026, 11, 8)
SYMS = ["EFC.US", "PFS.US", "ZTO.US", "NEWNOPRICE.US"]
SPOT = {"EFC.US": 11.26, "PFS.US": 22.5, "ZTO.US": 18.0}
ASOF = {"EFC.US": FRI, "PFS.US": FRI, "ZTO.US": FRI}
PREV = {"date": "2026-11-07", "prices": {"ZTO.US": 17.9}}     # ZTO held before the weekend


def _run(monkeypatch, gate, non_trading):
    if gate is None:
        monkeypatch.delenv("TFB_S1_SEED_NEW_SEATS", raising=False)
    else:
        monkeypatch.setenv("TFB_S1_SEED_NEW_SEATS", gate)
    monkeypatch.delenv("TFB_SHADOW_PRICE_HONESTY", raising=False)
    return s1.stored_prices_today(SYMS, SPOT, ASOF, PREV, SAT, non_trading)


def test_n1_off_is_the_old_rule(monkeypatch):
    prices, seeded = _run(monkeypatch, None, True)
    assert prices == {"ZTO.US": 17.9} and seeded == []


def test_n2_on_non_trading_seeds_new_seats(monkeypatch):
    prices, seeded = _run(monkeypatch, "on", True)
    assert prices == {"EFC.US": 11.26, "PFS.US": 22.5, "ZTO.US": 17.9}   # carried ZTO wins over its stale bar
    assert seeded == ["EFC.US", "PFS.US"]
    # a fresh bar always wins over seeding
    asof = dict(ASOF, **{"EFC.US": SUN})
    prices2, seeded2 = s1.stored_prices_today(SYMS, SPOT, asof, PREV, SAT, True)
    assert prices2["EFC.US"] == 11.26 and seeded2 == ["PFS.US"]


def test_n3_on_trading_day_seeds_nothing(monkeypatch):
    prices, seeded = _run(monkeypatch, "on", False)
    assert prices == {"ZTO.US": 17.9} and seeded == []


def test_n4_no_price_is_never_invented(monkeypatch):
    prices, _ = _run(monkeypatch, "on", True)
    assert "NEWNOPRICE.US" not in prices
    zero = dict(SPOT, **{"EFC.US": 0.0})
    p2, s2 = s1.stored_prices_today(SYMS, zero, ASOF, PREV, SAT, True)
    assert "EFC.US" not in p2 and "EFC.US" not in s2


def test_n5_main_stores_through_the_helper():
    src = inspect.getsource(s1.main)
    assert "stored_prices_today(" in src and "seeded=" in src
    assert "prices_today[s] = spot[s]" not in src          # the inline copy is gone


def test_n6_gate_version_and_selftest(monkeypatch):
    for raw, want in (("", False), ("off", False), ("1", True), ("on", True), ("TRUE", True)):
        monkeypatch.setenv("TFB_S1_SEED_NEW_SEATS", raw)
        assert s1._seed_new_seats_enabled() is want
    assert s1.SCRIPT_VERSION == "1.9.4"
    wf = (ROOT / ".github" / "workflows" / "shadow_scorer.yml").read_text(encoding="utf-8")
    assert "TFB_S1_SEED_NEW_SEATS: ${{ vars.TFB_S1_SEED_NEW_SEATS || 'on' }}" in wf
    env = {k: v for k, v in os.environ.items() if not k.startswith("TFB_")}
    r = subprocess.run([sys.executable, str(ROOT / "scripts" / "run_shadow_scorer.py"), "--selftest"],
                       capture_output=True, text=True, cwd=str(ROOT), env=env, timeout=300)
    assert r.returncode == 0, r.stdout[-800:] + r.stderr[-800:]
