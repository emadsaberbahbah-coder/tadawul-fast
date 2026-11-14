"""repair_corporate_actions 1.1.0: a reviewed seed of confirmed actions, written into _Corporate_Actions on or
after the effective date, and finished outcomes that span an action repaired (owner 14 Nov 2026: "move to the
next action"; the register was header-only and MBRX's 1-for-20 reverse split left matured outcomes above +1,000%).

All fixtures are invented; no sheet, no network.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from core import corporate_actions as ca  # noqa: E402
from scripts import repair_corporate_actions as rca  # noqa: E402

HDR = ["Record ID", "Key", "Symbol", "Horizon", "Date Recorded (Riyadh)", "Entry Price", "Entry Recommendation",
       "Entry Score", "Risk Bucket", "Confidence", "Origin Tab", "Target Price", "Target ROI %", "Target Date (Riyadh)",
       "Status", "Current Price", "Unrealized ROI %", "Realized ROI %", "Outcome", "Volatility", "Max Drawdown %",
       "Sharpe Ratio", "Sector", "Factor Exposures", "Last Updated (Riyadh)", "Maturity Date", "Notes"]


def rec(rid, sym, recorded, entry, target, status, cur, realized="", outcome="", maturity="", notes="", tdate=None):
    r = [""] * len(HDR)
    tdate = maturity[:10] if tdate is None else tdate                # the target session; by default the same day
    for k, v in (("Record ID", rid), ("Symbol", sym), ("Date Recorded (Riyadh)", recorded), ("Entry Price", entry),
                 ("Target Price", target), ("Status", status), ("Current Price", cur), ("Realized ROI %", realized),
                 ("Outcome", outcome), ("Maturity Date", maturity), ("Notes", notes), ("Target Date (Riyadh)", tdate)):
        r[HDR.index(k)] = str(v)
    return r


SEED = {"actions": [
    {"symbol": "rvs.us", "action_type": "REVERSE_SPLIT", "effective_date": "2026-10-20", "ratio": 20,
     "source": "SEC 8-K 2026-10-16", "notes": "1-for-20"},
    {"symbol": "FUT.TO", "action_type": "SPLIT", "effective_date": "2026-11-24", "ratio": 3, "source": "issuer release"},
    {"symbol": "BAD.US", "action_type": "SPLIT", "effective_date": "2026-10-01", "ratio": 2, "source": "AUTO_DETECT"},
    {"symbol": "NOR.US", "action_type": "SPLIT", "effective_date": "2026-10-01", "source": "issuer"},
]}


# ----------------------------------------------------------------------------- the seed
def test_s1_seed_rows_are_written_on_or_after_their_date_and_once():
    rows, held = rca.seed_rows(SEED, [ca.ACTIONS_HEADER], date(2026, 11, 14), "2026-11-14 12:00:00")
    assert [r[:4] for r in rows] == [["RVS.US", "REVERSE_SPLIT", "2026-10-20", "20"]]
    assert rows[0][6] == "SEC 8-K 2026-10-16" and rows[0][8] == "seed: 1-for-20" and len(rows[0]) == len(ca.ACTIONS_HEADER)
    assert "FUT.TO SPLIT effective 2026-11-24 - written on that day" in held
    assert sum("not a complete confirmed action" in h for h in held) == 2      # AUTO_DETECT source; no ratio
    acts = ca.parse_actions([ca.ACTIONS_HEADER] + rows)
    assert acts[0].confirmed and acts[0].price_factor == pytest.approx(1 / 20)
    again, _ = rca.seed_rows(SEED, [ca.ACTIONS_HEADER] + rows, date(2026, 11, 14), "x")
    assert again == []                                                            # never written twice
    later, held2 = rca.seed_rows(SEED, [ca.ACTIONS_HEADER] + rows, date(2026, 11, 24), "x")
    assert [r[0] for r in later] == ["FUT.TO"] and not any("FUT.TO" in h for h in held2)
    assert rca.seed_rows({}, [], date(2026, 11, 14), "x") == ([], [])


def test_s2_the_shipped_seed_is_well_formed():
    seed = json.loads((ROOT / "config/corporate_actions_confirmed.json").read_text(encoding="utf-8"))
    rows, held = rca.seed_rows(seed, [ca.ACTIONS_HEADER], date(2026, 11, 14), "x")
    assert [(r[0], r[1], r[2], r[3]) for r in rows] == [("MBRX.US", "REVERSE_SPLIT", "2026-10-20", "20")]
    assert held == ["GRT-UN.TO SPLIT effective 2026-11-24 - written on that day"]
    assert all("sec.gov" in r[6] or "Granite" in r[6] for r in rows)


# ----------------------------------------------------------------------------- finished outcomes
IDX = ca.build_adjustment_index(ca.parse_actions([ca.ACTIONS_HEADER,
                                                  ["RVS.US", "REVERSE_SPLIT", "2026-10-20", "20", "", "", "SEC", "", ""]]))


def test_f1_a_matured_outcome_across_the_split_is_recomputed():
    pl = [[""]] * 4 + [HDR,
                       rec("a", "RVS.US", "2026-09-24", "0.40", "0.50", "matured", "6.5", "1525.0", "WIN",
                           "2026-10-24 21:00:00"),                       # across the split: the bad +1,525%
                       rec("b", "RVS.US", "2026-09-01", "0.50", "0.60", "matured", "0.45", "-10.0", "LOSS",
                           "2026-10-01 21:00:00"),                       # wholly before: ROI unchanged
                       rec("c", "RVS.US", "2026-10-21", "7.0", "8.0", "matured", "6.5", "-7.14", "LOSS",
                           "2026-11-04 21:00:00"),                       # wholly after: no repair at all
                       rec("d", "RVS.US", "2026-09-25", "0.42", "0.55", "active", "6.4"),
                       rec("e", "RVS.US", "2026-09-26", "0.44", "0.55", "matured", "6.4", "1354.5", "WIN", ""),
                       rec("g", "RVS.US", "2026-09-23", "0.30", "0.40", "matured", "6.5", "2066.7", "WIN",
                           "2026-10-23 21:00:00")]                       # across the split, a real gain
    plan, _, _ = rca.plan_repairs(pl, IDX)
    by = {p["sheet_row"]: p for p in plan}
    assert sorted(by) == [6, 7, 9, 10, 11]                                 # row 8 is after the split
    assert by[11]["roi_new"] == pytest.approx(8.3333, abs=1e-3) and by[11]["outcome_new"] == "WIN"
    a = by[6]
    assert a["entry_new"] == pytest.approx(8.0) and a["exit_new"] == pytest.approx(6.5)
    assert a["roi_new"] == pytest.approx(-18.75) and a["outcome_new"] == "LOSS" and a["roi_old"] == 1525.0
    b = by[7]
    assert b["entry_new"] == pytest.approx(10.0) and b["exit_new"] == pytest.approx(9.0)
    assert b["roi_new"] == pytest.approx(-10.0) and b["outcome_new"] == "LOSS"
    assert "roi_new" not in by[9]                                          # active: entry only
    assert "roi_new" not in by[10] and "exit basis unknown" in by[10]["held"]   # no exit date: held, untouched


def test_f2_an_exit_priced_before_the_split_but_stamped_after_it_is_held():
    # Codex P1 on #981: entry 100, historical exit 90 at the target session before a 2-for-1, finalized after it.
    # Using the processing date would make it entry 50 / exit 90 = +80%; the basis is ambiguous, so it is held.
    idx = ca.build_adjustment_index(ca.parse_actions([ca.ACTIONS_HEADER,
                                                      ["TWO.US", "SPLIT", "2026-10-20", "2", "", "", "issuer", "", ""]]))
    pl = [HDR,
          rec("x", "TWO.US", "2026-10-01", "100", "110", "matured", "90", "-10.0", "LOSS", "2026-10-25 21:00:00",
              tdate="2026-10-15"),
          rec("y", "TWO.US", "2026-10-01", "100", "110", "matured", "45", "-10.0", "LOSS", "2026-10-25 21:00:00",
              tdate="2026-10-22"),                                           # target after the split: basis known
          rec("z", "TWO.US", "2026-10-01", "100", "110", "stopped", "90", "-10.0", "LOSS", "2026-10-25 21:00:00",
              tdate="")]                                                     # no target session: held
    plan, _, _ = rca.plan_repairs(pl, idx)
    by = {p["sheet_row"]: p for p in plan}
    assert "roi_new" not in by[2] and "ambiguous" in by[2]["held"] and "2026-10-15" in by[2]["held"]
    assert by[3]["entry_new"] == pytest.approx(50.0) and by[3]["roi_new"] == pytest.approx(-10.0)
    assert "held" not in by[3] and "unknown" in by[4]["held"]


class Tab:
    def __init__(self, values):
        self.values = [list(r) for r in values]
        self.appended, self.batches = [], []

    def get_all_values(self):
        return [list(r) for r in self.values]

    def append_rows(self, rows, value_input_option=None):
        self.appended.extend(rows)
        self.values.extend(rows)

    def append_row(self, row, value_input_option=None):
        self.appended.append(row)

    def batch_update(self, updates):
        self.batches.extend(updates)


class Book:
    def __init__(self, tabs):
        self.tabs = tabs

    def worksheet(self, name):
        if name not in self.tabs:
            raise KeyError(name)
        return self.tabs[name]


def _book(pl_rows, register=None):
    return Book({ca.TAB_ACTIONS: Tab(register or [ca.ACTIONS_HEADER]), "Performance_Log": Tab(pl_rows),
                 "_Run_Log": Tab([])})


def test_r1_scan_writes_nothing_and_apply_writes_register_then_repairs(monkeypatch, tmp_path, capsys):
    seed = tmp_path / "seed.json"
    seed.write_text(json.dumps({"actions": [SEED["actions"][0]]}), encoding="utf-8")
    pl = [[""]] * 4 + [HDR, rec("a", "RVS.US", "2026-09-24", "0.40", "0.50", "matured", "6.5", "1525.0", "WIN",
                                "2026-10-24 21:00:00")]
    b = _book(pl)
    monkeypatch.setattr(rca, "_open_sheet", lambda cli: b)
    assert rca.main(["--scan", "--seed", str(seed)]) == 0
    out = capsys.readouterr().out
    assert "seed_rows=1 plan=1 finished=1" in out and "realized 1525.0 -> -18.75" in out
    assert b.tabs[ca.TAB_ACTIONS].appended == [] and b.tabs["Performance_Log"].batches == []
    assert rca.main(["--apply", "--seed", str(seed)]) == 0
    reg = b.tabs[ca.TAB_ACTIONS].appended
    assert [r[:4] for r in reg] == [["RVS.US", "REVERSE_SPLIT", "2026-10-20", "20"]]
    cells = {u["range"]: u["values"][0][0] for u in b.tabs["Performance_Log"].batches}
    col = lambda name: rca._col_letter(HDR.index(name))                                   # noqa: E731
    assert cells[col("Entry Price") + "6"] == pytest.approx(8.0) and cells[col("Target Price") + "6"] == pytest.approx(10.0)
    assert cells[col("Current Price") + "6"] == pytest.approx(6.5)
    assert cells[col("Realized ROI %") + "6"] == pytest.approx(-18.75) and cells[col("Outcome") + "6"] == "LOSS"
    assert cells[col("Notes") + "6"].startswith("ca_adjusted:v1.1.1:f=0.05")
    assert "seed_rows=1" in b.tabs["_Run_Log"].appended[-1][5]
    # the next run: the register holds the row, so the seed adds nothing; the repaired row is tagged and skipped
    b.tabs["Performance_Log"].values[5][HDR.index("Notes")] = cells[col("Notes") + "6"]
    b.tabs[ca.TAB_ACTIONS].appended.clear()
    assert rca.main(["--apply", "--seed", str(seed)]) == 0
    assert b.tabs[ca.TAB_ACTIONS].appended == [] and "seed_rows=0 plan=0" in capsys.readouterr().out


def test_r3_a_missing_seed_is_loud_and_a_held_row_is_never_written(monkeypatch, tmp_path, capsys):
    opened = []
    monkeypatch.setattr(rca, "_open_sheet", lambda cli: opened.append(cli))
    assert rca.main(["--scan", "--seed", str(tmp_path / "absent.json")]) == 2          # not a silent empty seed
    assert "::error::corporate-actions seed not found" in capsys.readouterr().out and opened == []
    seed = tmp_path / "seed.json"
    seed.write_text(json.dumps({"actions": [SEED["actions"][0]]}), encoding="utf-8")
    pl = [HDR, rec("h", "RVS.US", "2026-09-24", "0.40", "0.50", "matured", "0.36", "-10.0", "LOSS",
                   "2026-10-25 21:00:00", tdate="2026-10-15")]                      # priced before, stamped after
    b = _book(pl)
    monkeypatch.setattr(rca, "_open_sheet", lambda cli: b)
    assert rca.main(["--apply", "--seed", str(seed)]) == 0
    out = capsys.readouterr().out
    assert "plan=0 finished=0 held=1" in out and "HELD row     2 RVS.US" in out
    assert b.tabs["Performance_Log"].batches == [] and "held=1" in b.tabs["_Run_Log"].appended[-1][5]


# ----------------------------------------------------------------------------- 1.1.1: the tag is never cut off
LONG = "[OUTCOME-WITNESS v1] " + json.dumps({"k": "v" * 700})               # a 600-770 character receipt, as on MBRX


def test_t1_long_notes_keep_every_character_and_the_tag(monkeypatch, tmp_path, capsys):
    assert rca._tag_notes("", "ca_adjusted:x") == "ca_adjusted:x" and rca._tag_notes("a", "t") == "a | t"
    seed = tmp_path / "seed.json"
    seed.write_text(json.dumps({"actions": [SEED["actions"][0]]}), encoding="utf-8")
    pl = [HDR, rec("a", "RVS.US", "2026-09-24", "0.40", "0.50", "active", "6.5", notes=LONG)]
    b = _book(pl)
    monkeypatch.setattr(rca, "_open_sheet", lambda cli: b)
    assert rca.main(["--apply", "--seed", str(seed), "--retag", str(_retag(tmp_path, []))]) == 0
    cells = {u["range"]: u["values"][0][0] for u in b.tabs["Performance_Log"].batches}
    note = cells[rca._col_letter(HDR.index("Notes")) + "2"]
    assert note == LONG + " | ca_adjusted:v1.1.1:f=0.05" and len(note) > 700     # 1.1.0 cut this to 250
    b.tabs["Performance_Log"].values[1][HDR.index("Notes")] = note
    b.tabs["Performance_Log"].values[1][HDR.index("Entry Price")] = "8.0"
    capsys.readouterr()
    assert rca.main(["--scan", "--seed", str(seed), "--retag", str(_retag(tmp_path, []))]) == 0
    assert "plan=0" in capsys.readouterr().out                                   # never rebased twice


def _retag(tmp_path, rows):
    f = tmp_path / "retag.json"
    f.write_text(json.dumps({"schema": "tfb.corporate-actions-retag.v1", "rows": rows}), encoding="utf-8")
    return f


def test_t2_plan_retags_only_a_row_rebased_exactly_once():
    cut = "[OUTCOME-WITNESS v1] {cut off at 250"
    pl = [HDR,
          rec("m", "RVS.US", "2026-09-24", "8.0", "10", "matured", "6.5", notes=cut),       # once rebased, untagged
          rec("x", "RVS.US", "2026-09-24", "8.0", "10", "active", "6.5", notes=cut),        # active: keeps notes
          rec("t", "RVS.US", "2026-09-24", "8.0", "10", "matured", "6.5", notes="ca_adjusted:v1.1.0:f=0.05"),
          rec("w", "RVS.US", "2026-09-24", "160", "10", "matured", "6.5", notes=cut)]       # twice: held
    spec = [{"record_id": r, "factor": 0.05, "entry_before": 0.4, "original_notes": o}
            for r, o in (("m", "ORIGINAL m"), ("x", None), ("t", "T"), ("w", "W"), ("gone", "G"))]
    plan, hdr_i, cols = rca.plan_repairs(pl, IDX)
    ups, held, ids = rca.plan_retags(pl, hdr_i, cols, {"rows": spec}, "9.9.9")
    assert ids == {"m", "x", "t", "w", "gone"}
    assert {u["record_id"]: u["notes_new"] for u in ups} == {
        "m": "ORIGINAL m | ca_adjusted:v9.9.9:f=0.05:retag", "x": cut + " | ca_adjusted:v9.9.9:f=0.05:retag"}
    assert [u["sheet_row"] for u in ups] == [2, 3]
    assert len(held) == 2 and "row 5 w: Entry Price 160.0 is not 0.4 / 0.05" in held[0] and "gone" in held[1]
    assert {p["record_id"] for p in plan} == {"m", "x", "w"}                      # main drops these from the plan
    assert rca.plan_retags(pl, hdr_i, cols, {}, "x") == ([], [], set())


def test_t3_apply_retags_and_never_rebases_a_listed_row(monkeypatch, tmp_path, capsys):
    seed = tmp_path / "seed.json"
    seed.write_text(json.dumps({"actions": [SEED["actions"][0]]}), encoding="utf-8")
    reg = [ca.ACTIONS_HEADER, ["RVS.US", "REVERSE_SPLIT", "2026-10-20", "20", "", "", "SEC 8-K 2026-10-16", "", ""]]
    pl = [HDR, rec("m", "RVS.US", "2026-09-24", "8.0", "10", "matured", "6.5", "-18.75", "LOSS",
                   "2026-10-24 21:00:00", notes="[OUTCOME-WITNESS v1] {cut")]
    rt = _retag(tmp_path, [{"record_id": "m", "factor": 0.05, "entry_before": 0.4, "original_notes": "FULL"}])
    b = _book(pl, register=reg)
    monkeypatch.setattr(rca, "_open_sheet", lambda cli: b)
    assert rca.main(["--scan", "--seed", str(seed), "--retag", str(rt)]) == 0
    out = capsys.readouterr().out
    assert "plan=0 finished=0 held=0 retag=1 retag_held=0" in out and "RETAG row     2 m" in out
    assert b.tabs["Performance_Log"].batches == []
    assert rca.main(["--apply", "--seed", str(seed), "--retag", str(rt)]) == 0
    cells = {u["range"]: u["values"][0][0] for u in b.tabs["Performance_Log"].batches}
    assert cells == {rca._col_letter(HDR.index("Notes")) + "2": "FULL | ca_adjusted:v1.1.1:f=0.05:retag"}
    assert "retag=1" in b.tabs["_Run_Log"].appended[-1][5]
    b.tabs["Performance_Log"].values[1][HDR.index("Notes")] = cells[rca._col_letter(HDR.index("Notes")) + "2"]
    capsys.readouterr()
    assert rca.main(["--scan", "--seed", str(seed), "--retag", str(rt)]) == 0
    assert "plan=0 finished=0 held=0 retag=0 retag_held=0" in capsys.readouterr().out
    assert rca.main(["--scan", "--seed", str(seed), "--retag", str(tmp_path / "absent.json")]) == 2


def test_t4_the_shipped_retag_file_is_the_five_mbrx_rows():
    import hashlib
    doc = json.loads((ROOT / "config/corporate_actions_retag.json").read_text(encoding="utf-8"))
    assert doc["schema"] == "tfb.corporate-actions-retag.v1" and len(doc["rows"]) == 5
    assert {r["symbol"] for r in doc["rows"]} == {"MBRX.US"} and {r["factor"] for r in doc["rows"]} == {0.05}
    assert sorted(r["sheet_row_on_14_nov"] for r in doc["rows"]) == [16659, 16838, 16839, 17080, 17081]
    for r in doc["rows"]:
        assert r["entry_before"] in (0.5507, 0.6358, 0.6016)
        if r["status_on_14_nov"] == "matured":
            n = r["original_notes"]
            assert hashlib.sha256(n.encode()).hexdigest() == r["original_notes_sha256"] and len(n) > 600
            assert "[OUTCOME-WITNESS v1] {" in n and n.endswith('"tracker_version":"6.49.0"}')
        else:
            assert r["original_notes"] is None


def test_r2_the_workflows_base64_credentials_are_read(monkeypatch, tmp_path):
    import base64
    seen = {}

    class Creds:
        @staticmethod
        def from_service_account_file(path, scopes=None):
            seen["json"] = json.loads(Path(path).read_text(encoding="utf-8"))
            return "creds"

    import types
    fake_gspread = types.SimpleNamespace(authorize=lambda c: types.SimpleNamespace(open_by_key=lambda k: ("book", k)))
    sa = types.ModuleType("google.oauth2.service_account")
    sa.Credentials = Creds
    monkeypatch.setitem(sys.modules, "gspread", fake_gspread)
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", sa)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    monkeypatch.delenv("GOOGLE_SHEETS_CREDENTIALS_B64", raising=False)
    info = {"type": "service_account", "client_email": "x@y.invalid"}
    monkeypatch.setenv("GOOGLE_SHEETS_CREDENTIALS", base64.b64encode(json.dumps(info).encode()).decode())
    assert rca._open_sheet("SHEET123") == ("book", "SHEET123") and seen["json"] == info
    monkeypatch.setenv("GOOGLE_SHEETS_CREDENTIALS", json.dumps(info))
    assert rca._open_sheet("SHEET123") == ("book", "SHEET123")


def test_w1_workflow_and_ci():
    import yaml
    wf = yaml.safe_load((ROOT / ".github/workflows/corporate_actions.yml").read_text(encoding="utf-8"))
    on = wf.get("on") or wf.get(True)
    assert on["schedule"] and "workflow_dispatch" in on
    steps = [s for j in wf["jobs"].values() for s in j["steps"]]
    run = [s for s in steps if "repair_corporate_actions.py" in str(s.get("run", ""))]
    assert run and "--selftest" in str([s.get("run") for s in steps])
    env = run[-1]["env"]
    assert env["TFB_CA_REPAIR"] == "${{ github.event_name == 'workflow_dispatch' && inputs.mode || vars.TFB_CA_REPAIR || 'apply' }}"
    assert "tests/test_repair_corporate_actions_seed.py" in (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
