"""audit_full_refresh_coverage v1.3.2: name-gap list (diagnostic only).

The Global_Markets 99% names floor failed in 5 of 5 sampled final audits and
nothing said which symbols were nameless. v1.3.2 lists them; every rule,
failure, warning, status and exit code stays as in v1.3.1.
"""
from __future__ import annotations

import asyncio
import json
from dataclasses import asdict
from datetime import datetime, timezone

from scripts import audit_full_refresh_coverage as coverage
from scripts.audit_full_refresh_coverage import Report, Result, Rule, audit_grid

NOW = datetime(2026, 10, 16, 1, 0, 0, tzinfo=timezone.utc)
HEADERS = ["Symbol", "Name", "Current Price", "Last Updated (UTC)", "Position Qty", "Avg Cost", "Data Provider"]
NEW_FIELDS = {"blank_name_count", "blank_name_symbols"}


def grid(n, nameless):
    rows = []
    for i in range(n):
        sym = f"S{i:04d}.US"
        rows.append([sym, "" if sym in nameless else f"Name {i}", 10 + i, NOW.isoformat(), "", "", "eodhd"])
    return [HEADERS, *rows]


def gm_rule():
    return Rule("Global_Markets", 1, 30, 95, 99, 95)


def test_lists_nameless_symbols_sorted_and_keeps_the_name_failure():
    nameless = {"S0007.US", "S0003.US", "S0150.US"}
    r = audit_grid(grid(200, nameless), gm_rule(), HEADERS, NOW)
    assert r.blank_name_count == 3
    assert r.blank_name_symbols == ["S0003.US", "S0007.US", "S0150.US"]
    assert r.name_pct == 98.5 and "name coverage 98.50% below 99.00%" in r.failures
    assert r.status == "FAIL"


def test_list_is_capped_at_200_but_count_is_exact():
    nameless = {f"S{i:04d}.US" for i in range(0, 600, 2)}
    r = audit_grid(grid(600, nameless), gm_rule(), HEADERS, NOW)
    assert r.blank_name_count == 300 and len(r.blank_name_symbols) == 200
    assert r.blank_name_symbols == sorted(nameless)[:200]


def test_verdict_fields_do_not_depend_on_the_new_fields():
    for nameless in (set(), {"S0001.US"}, {f"S{i:04d}.US" for i in range(5)}):
        r = audit_grid(grid(150, nameless), gm_rule(), HEADERS, NOW)
        d = asdict(r)
        assert set(d) - NEW_FIELDS == set(asdict(Result("x"))) - NEW_FIELDS
        # failures/warnings/status are exactly what the name/price/fresh math says
        want_fail = r.name_pct is not None and r.name_pct < 99
        assert any(f.startswith("name coverage") for f in r.failures) == want_fail
        assert r.status == ("FAIL" if r.failures else ("WARN" if r.warnings else "PASS"))


def test_no_name_rule_and_portfolio_rules_never_list():
    nameless = {"S0001.US"}
    no_rule = audit_grid(grid(20, nameless), Rule("Commodities_FX", 1, 30, 95, 0, 95), HEADERS, NOW)
    assert no_rule.blank_name_count == 0 and no_rule.blank_name_symbols == []
    pf = audit_grid(grid(3, {"S0001.US"}), Rule("My_Portfolio", 1, 8, 100, 100, 100, True, True),
                    HEADERS, NOW, active=("S0000.US", "S0001.US", "S0002.US"))
    assert pf.blank_name_count == 0 and pf.blank_name_symbols == []


def test_main_prints_one_notice_per_page_with_a_gap_and_exit_code_unchanged(monkeypatch, capsys):
    gm = audit_grid(grid(100, {"S0002.US", "S0001.US"}), gm_rule(), HEADERS, NOW)
    clean = audit_grid(grid(10, set()), Rule("Market_Leaders", 1, 30, 95, 99, 95), HEADERS, NOW)
    rep = Report(NOW.isoformat(), "x", "s", [], pages=[gm, clean])

    async def fake_run(*_a, **_k):
        return rep
    monkeypatch.setattr(coverage, "run", fake_run)
    code = coverage.main(["--sheet-id", "abc"])
    out = capsys.readouterr().out
    lines = [ln for ln in out.splitlines() if "blank-name symbols" in ln]
    assert lines == ["::notice::Global_Markets: blank-name symbols 2 (name_pct=98.0): S0001.US, S0002.US"]
    assert "{" not in lines[0] and "}" not in lines[0]
    assert code == rep.code == 2
    payload = json.loads(out[: out.index("\n::")])
    assert payload["pages"][0]["blank_name_symbols"] == ["S0001.US", "S0002.US"]
    assert payload["summary"]["exit_code"] == 2
