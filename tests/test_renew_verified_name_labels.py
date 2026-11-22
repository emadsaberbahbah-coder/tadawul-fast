"""Offline suite for scripts/renew_verified_name_labels.py (no network)."""
from __future__ import annotations

import copy
import json
from datetime import UTC, datetime, timedelta

import pytest

from core import verified_name_labels as labels
from scripts import renew_verified_name_labels as renew

NOW = datetime(2026, 10, 20, 0, 0, tzinfo=UTC)
UA = "Example Org ops@example.org"
NASDAQ = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"
SEC = "https://www.sec.gov/files/company_tickers_exchange.json"


def _registry():
    return json.loads(labels.REGISTRY_PATH.read_text(encoding="utf-8"))


def _pick(predicate):
    return copy.deepcopy(next(b for b in _registry()["bindings"] if predicate(b)))


def _directory_binding():
    return _pick(lambda b: any(s["url"] == NASDAQ for s in b["sources"])
                 and b["identifiers"].get("directory_venue") == "Nasdaq")


def _directory_bodies(item, *, status=None, name=None, cik=None):
    ids = item["identifiers"]
    nasdaq = ("Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares\n"
              "AAAA|Other Co Common Stock|G|N|N|100|N|N\n"
              "%s|%s|Q|N|%s|100|N|N\n"
              "File Creation Time: 1019202617:01|||||||\n"
              % (ids["exact_authority_ticker"], name or ids["security_class"],
                 status or ids["directory_financial_status"])).encode()
    sec = json.dumps({"fields": ["cik", "name", "ticker", "exchange"],
                      "data": [[int(cik or ids["CIK"]), item["name"].upper(),
                                ids["exact_authority_ticker"], "Nasdaq"]]}).encode()
    return {NASDAQ: nasdaq, SEC: sec}


def _fetcher(bodies, calls=None):
    def fetch(url):
        if calls is not None:
            calls.append(url)
        if url in bodies:
            body = bodies[url]
            if isinstance(body, Exception):
                raise body
            return 200, body
        return 404, b""
    return fetch


def _one(item):
    return {"schema": "tfb.verified-name-labels/v1", "label_only": True,
            "current_authority_only": True, "bindings": [item]}


def _write_and_load(tmp_path, registry):
    path = tmp_path / "labels.json"
    path.write_text(json.dumps(registry, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return labels._load_registry(path)


# T1 directory binding renews; only review fields change; production loader accepts it
def test_directory_binding_renews_and_passes_production_loader(tmp_path):
    item = _directory_binding()
    new, report = renew.renew(_one(item), fetch=_fetcher(_directory_bodies(item)), now=NOW,
                              sec_user_agent=UA)
    fresh = new["bindings"][0]
    assert report["renewed"] == [item["page"] + "|" + item["symbol"]] and not report["held"]
    for key in ("page", "symbol", "name", "state", "native_metadata", "requires_quarantine"):
        assert fresh[key] == item[key]
    assert fresh["identifiers"]["directory_creation_time"] == "1019202617:01"
    assert {k: v for k, v in fresh["identifiers"].items() if k != "directory_creation_time"} == \
        {k: v for k, v in item["identifiers"].items() if k != "directory_creation_time"}
    reviewed = datetime.fromisoformat(fresh["reviewed_at_utc"])
    assert reviewed == NOW and datetime.fromisoformat(fresh["expires_at_utc"]) - reviewed == timedelta(days=7)
    assert [s["url"] for s in fresh["sources"]] == [s["url"] for s in item["sources"]]
    bindings, _ = _write_and_load(tmp_path, new)
    assert (item["page"], item["symbol"]) in bindings


# T2 any directory or SEC drift holds the binding byte-identical
@pytest.mark.parametrize("change,reason", [
    ({"status": "D"}, "directory_row_changed"),
    ({"name": "Successor Holdings Common Stock"}, "directory_row_changed"),
    ({"cik": "999"}, "sec_ticker_cik_changed"),
])
def test_directory_drift_holds_unchanged(change, reason):
    item = _directory_binding()
    new, report = renew.renew(_one(item), fetch=_fetcher(_directory_bodies(item, **change)),
                              now=NOW, sec_user_agent=UA)
    assert new["bindings"][0] == item
    assert list(report["held"].values()) == [reason]


def test_directory_row_missing_or_duplicated_holds():
    item = _directory_binding()
    bodies = _directory_bodies(item)
    bodies[NASDAQ] = bodies[NASDAQ].replace(item["identifiers"]["exact_authority_ticker"].encode() + b"|",
                                            b"ZZZZ|", 1)
    _, report = renew.renew(_one(item), fetch=_fetcher(bodies), now=NOW, sec_user_agent=UA)
    assert list(report["held"].values()) == ["directory_row_missing_or_ambiguous"]


# T3 sec.gov is never fetched without a declared User-Agent
def test_sec_sources_need_declared_user_agent():
    item = _directory_binding()
    calls = []
    new, report = renew.renew(_one(item), fetch=_fetcher(_directory_bodies(item), calls), now=NOW,
                              sec_user_agent="")
    assert new["bindings"][0] == item
    assert list(report["held"].values()) == ["sec_user_agent_missing"]
    assert not any(renew._is_sec(url) for url in calls)


# T4 a failed or non-2xx fetch holds the binding
@pytest.mark.parametrize("broken", [OSError("reset"), None])
def test_unavailable_source_holds(broken):
    item = _directory_binding()
    bodies = _directory_bodies(item)
    if broken is None:
        del bodies[NASDAQ]
        expected = "http_404"
    else:
        bodies[NASDAQ] = broken
        expected = "fetch_failed:OSError"
    new, report = renew.renew(_one(item), fetch=_fetcher(bodies), now=NOW, sec_user_agent=UA)
    assert new["bindings"][0] == item and list(report["held"].values()) == [expected]


# T5 document bindings need the identifier and the caption in a fresh non-PDF body
def _document_binding():
    return _pick(lambda b: b["symbol"] == "CZAR.US")


def _document_bodies(item, *, text):
    bodies = {s["url"]: b"%PDF-1.7 binary" for s in item["sources"]}
    first_html = next(s["url"] for s in item["sources"] if s["url"].endswith(".htm"))
    bodies[first_html] = text.encode()
    return bodies


def test_document_binding_renews_when_corroborated(tmp_path):
    item = _document_binding()
    text = "<p>Themes ETF Trust &mdash; <b>Themes Natural Monopoly&nbsp;ETF</b> (Ticker: CZAR)</p>"
    new, report = renew.renew(_one(item), fetch=_fetcher(_document_bodies(item, text=text)), now=NOW,
                              sec_user_agent=UA)
    assert not report["held"]
    assert new["bindings"][0]["identifiers"] == item["identifiers"]
    _write_and_load(tmp_path, new)


@pytest.mark.parametrize("text,reason", [
    ("<p>Themes Natural Monopoly ETF</p>", "identifier_not_corroborated"),
    ("<p>CZAR fund notice</p>", "caption_not_corroborated"),
    ("<p>Themes Natural Monopoly ETF, ticker CZARX</p>", "identifier_not_corroborated"),
])
def test_document_binding_holds_without_corroboration(text, reason):
    item = _document_binding()
    new, report = renew.renew(_one(item), fetch=_fetcher(_document_bodies(item, text=text)), now=NOW,
                              sec_user_agent=UA)
    assert new["bindings"][0] == item and list(report["held"].values()) == [reason]


def test_pdf_only_evidence_holds():
    item = _document_binding()
    bodies = {s["url"]: b"%PDF-1.7 Themes Natural Monopoly ETF CZAR" for s in item["sources"]}
    _, report = renew.renew(_one(item), fetch=_fetcher(bodies), now=NOW, sec_user_agent=UA)
    assert list(report["held"].values()) == ["no_text_source"]


# T6 a run where nothing verifies returns the committed registry unchanged
def test_nothing_verifies_returns_registry_unchanged():
    registry = _registry()
    new, report = renew.renew(registry, fetch=_fetcher({}), now=NOW, sec_user_agent=UA)
    assert new == registry and not report["renewed"]
    assert len(report["held"]) == len(registry["bindings"])


# T7 --check reports bindings expiring inside the warning window without network
def test_check_mode_flags_expiring(tmp_path, capsys):
    item = _directory_binding()
    reviewed = datetime.now(UTC) - timedelta(days=6)
    item.update(reviewed_at_utc=reviewed.isoformat(), expires_at_utc=(reviewed + timedelta(days=7)).isoformat())
    path = tmp_path / "labels.json"
    path.write_text(json.dumps(_one(item)), encoding="utf-8")
    report = tmp_path / "report.json"
    assert renew.main(["--registry", str(path), "--check", "--report", str(report)]) == 1
    assert json.loads(report.read_text())["expiring"] == [item["page"] + "|" + item["symbol"]]
    assert renew.main(["--registry", str(path), "--check", "--warn-hours", "1"]) == 0


# --- v1.1.0 --apply-renewal ---------------------------------------------------

def _renewed_copy(item, days=3):
    """The renewal re-stamp of one binding: same caption, newer review."""
    new = copy.deepcopy(item)
    reviewed = datetime.fromisoformat(item["reviewed_at_utc"]) + timedelta(days=days)
    new["reviewed_at_utc"] = reviewed.isoformat()
    new["expires_at_utc"] = (reviewed + timedelta(days=7)).isoformat()
    for source in new["sources"]:
        source["captured_at_utc"] = reviewed.isoformat()
        source["sha256"] = "f" * 64
    if "directory_creation_time" in new["identifiers"]:
        new["identifiers"]["directory_creation_time"] = "Oct 21 2026 07:01"
    new["review_receipt_sha256"] = labels._binding_digest(new)
    return new


def test_apply_renewal_takes_only_matching_newer_captions(tmp_path):
    base = _registry()
    a, b, c = (copy.deepcopy(x) for x in base["bindings"][:3])
    renewed_a = _renewed_copy(a)
    renewed_b = _renewed_copy(b)
    renewed_b["name"] = renewed_b["name"] + " (changed upstream)"   # caption drift
    stale_c = _renewed_copy(c, days=-1)                              # older review
    ghost = _renewed_copy(a)
    ghost["symbol"] = "GHOST.US"                                     # not in base
    renewed = dict(base, bindings=[renewed_a, renewed_b, stale_c, ghost])
    merged, report = renew.apply_renewal(base, renewed)
    keys = [x["page"] + "|" + x["symbol"] for x in merged["bindings"]]
    assert keys == [x["page"] + "|" + x["symbol"] for x in base["bindings"]]   # no add/drop/reorder
    assert merged["bindings"][0] == renewed_a
    assert merged["bindings"][1] == b and merged["bindings"][2] == c
    assert report["applied"] == [a["page"] + "|" + a["symbol"]]
    assert report["kept"][b["page"] + "|" + b["symbol"]] == "caption_changed_since_renewal"
    assert report["kept"][c["page"] + "|" + c["symbol"]] == "not_newer"
    assert "GHOST.US" not in json.dumps(merged)
    assert _write_and_load(tmp_path, merged)


def test_apply_renewal_cli_writes_validated_registry(tmp_path, capsys):
    base = _registry()
    reg = tmp_path / "labels.json"
    reg.write_text(json.dumps(base, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    renewed = dict(base, bindings=[_renewed_copy(base["bindings"][0])])
    art = tmp_path / "renewed.json"
    art.write_text(json.dumps(renewed, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    assert renew.main(["--registry", str(reg), "--apply-renewal", str(art)]) == 0
    assert json.loads(capsys.readouterr().out.strip().splitlines()[-1])["applied"] == 1
    assert labels._load_registry(reg)
    nothing = tmp_path / "nothing.json"
    nothing.write_text(json.dumps(dict(base, bindings=[]), indent=2) + "\n", encoding="utf-8")
    assert renew.main(["--registry", str(reg), "--apply-renewal", str(nothing)]) == 3


# --- v1.2.0 acknowledged held bindings ------------------------------------------

def _expired_registry(tmp_path, symbols=("AAA.US", "BBB.US")):
    reviewed = datetime.now(UTC) - timedelta(days=9)
    items = []
    for symbol in symbols:
        item = _directory_binding()
        item.update(symbol=symbol, reviewed_at_utc=reviewed.isoformat(),
                    expires_at_utc=(reviewed + timedelta(days=7)).isoformat())
        items.append(item)
    path = tmp_path / "labels.json"
    path.write_text(json.dumps(dict(_one(items[0]), bindings=items)), encoding="utf-8")
    return path, [i["page"] + "|" + i["symbol"] for i in items]


def _ack(tmp_path, keys, days=30, name=None):
    review_by = (datetime.now(UTC) + timedelta(days=days)).isoformat()
    rows = [{"page": k.split("|")[0], "symbol": k.split("|")[1], "reason": "fetch_failed:HTTPError",
             "review_by_utc": review_by} for k in keys]
    path = tmp_path / (name or renew.HELD_ACK_NAME)
    path.write_text(json.dumps({"schema": 1, "acknowledged": rows}), encoding="utf-8")
    return path


# T13 an acknowledged held binding no longer fails the check; an unacknowledged one still does
def test_check_passes_only_when_every_due_binding_is_acknowledged(tmp_path):
    path, keys = _expired_registry(tmp_path)
    report = tmp_path / "report.json"
    assert renew.main(["--registry", str(path), "--check"]) == 1           # no list: 1.1.1 behaviour
    _ack(tmp_path, keys[:1])
    assert renew.main(["--registry", str(path), "--check", "--report", str(report)]) == 1
    saved = json.loads(report.read_text())
    assert saved["status"] == "EXPIRED" and saved["open"] == keys[1:] and saved["held_acknowledged"] == keys[:1]
    assert saved["expired"] == sorted(keys)                                  # the raw facts are unchanged
    _ack(tmp_path, keys)
    assert renew.main(["--registry", str(path), "--check", "--report", str(report)]) == 0
    saved = json.loads(report.read_text())
    assert saved["status"] == "HELD_ACKNOWLEDGED" and saved["open"] == [] and saved["held_acknowledged"] == sorted(keys)
    text = renew.render_step_summary(saved)
    assert "HELD, acknowledged until" in text and "do not fail this check" in text


# T14 an acknowledgement lapses on its own date, and one for a renewed binding is reported as unused
def test_acknowledgement_lapses_and_unused_ones_are_listed(tmp_path):
    path, keys = _expired_registry(tmp_path)
    report = tmp_path / "report.json"
    _ack(tmp_path, keys, days=-1)
    assert renew.main(["--registry", str(path), "--check", "--report", str(report)]) == 1
    saved = json.loads(report.read_text())
    assert saved["acknowledgement_lapsed"] == sorted(keys) and saved["held_acknowledged"] == []
    other = _ack(tmp_path, ["Global_Markets|ZZZ.US"], name="other.json")
    assert renew.main(["--registry", str(path), "--check", "--held-ack", str(other), "--report", str(report)]) == 1
    assert json.loads(report.read_text())["acknowledgement_unused"] == ["Global_Markets|ZZZ.US"]


# T15 a malformed list can never turn the check green
@pytest.mark.parametrize("body", [
    "not json", "[]", '{"schema": 2, "acknowledged": []}', '{"schema": 1, "acknowledged": {}}',
    '{"schema": 1, "acknowledged": [{"page": "P", "symbol": "S"}]}',
    '{"schema": 1, "acknowledged": [{"page": "P", "symbol": "S", "reason": "r", "review_by_utc": "soon"}]}',
    '{"schema": 1, "acknowledged": [{"page": "P", "symbol": "S", "reason": "r", "review_by_utc": "2099-01-01T00:00:00+00:00"},'
    ' {"page": "P", "symbol": "S", "reason": "r", "review_by_utc": "2099-01-01T00:00:00+00:00"}]}',
])
def test_malformed_acknowledgement_list_is_fatal(tmp_path, body):
    path, _ = _expired_registry(tmp_path)
    (tmp_path / renew.HELD_ACK_NAME).write_text(body, encoding="utf-8")
    assert renew.main(["--registry", str(path), "--check"]) == 2


# T16 the shipped list is valid and names only bindings that exist in the shipped registry
def test_shipped_acknowledgement_list_matches_the_registry():
    ack = renew.load_held_ack(labels.REGISTRY_PATH.with_name(renew.HELD_ACK_NAME))
    registry = json.loads(labels.REGISTRY_PATH.read_text(encoding="utf-8"))
    known = {b["page"] + "|" + b["symbol"] for b in registry["bindings"]}
    assert ack and set(ack) <= known
    assert all(datetime.fromisoformat(row["review_by_utc"]) <= datetime(2027, 3, 1, tzinfo=UTC) for row in ack.values())


# T17 workflow 1.2.0: the renewal runs on a schedule, never pushes to the default branch,
# and merges by itself only behind the owner's Variable
def test_workflow_schedules_the_renewal_and_gates_the_automatic_merge():
    import yaml
    from pathlib import Path
    text = (Path(renew.__file__).resolve().parents[1] / ".github" / "workflows" / "name_label_renewal.yml").read_text(encoding="utf-8")
    flow = yaml.safe_load(text)
    crons = [row["cron"] for row in flow[True]["schedule"]]
    assert crons == ["15 4 * * *", "40 21 * * 0,2,4"]
    first = flow["jobs"]["name-label-renewal"]
    assert "github.event.schedule == '40 21 * * 0,2,4' && 'renew' || 'check'" in first["env"]["MODE"]
    assert first.get("permissions") is None and flow["permissions"] == {"contents": "read"}
    publish = flow["jobs"]["publish-renewal"]
    assert "github.event.schedule == '40 21 * * 0,2,4'" in publish["if"] and "inputs.publish_branch" in publish["if"]
    step = publish["steps"][-1]
    assert step["env"]["AUTOMERGE"] == "${{ vars.TFB_NAME_RENEWAL_AUTOMERGE || '' }}"
    run = step["run"]
    assert 'if [[ "${AUTOMERGE,,}" == "on" ]]' in run and run.index("gh pr merge") > run.index('"${AUTOMERGE,,}" == "on"')
    assert 'git push -q origin "$BR"' in run and "git push origin main" not in run and "--force" not in run
    assert 'startswith("name-label-renewal/")' in run and "gh pr close" in run
    assert 'if [[ "$CHANGED" != "$REG" ]]' in run
    # the repository forbids Actions-created pull requests: the gated merge then goes through the merges API
    gate = run.index('"${AUTOMERGE,,}" == "on"')
    assert run.index('repos/${GITHUB_REPOSITORY}/merges') > gate and run.index('if [[ "$CHANGED" != "$REG" ]]') > gate
    assert run.index('if [[ "$CHANGED" != "$REG" ]]') < run.index('repos/${GITHUB_REPOSITORY}/merges')
    assert "PR_OPEN=0" in run and 'elif [[ "$PR_OPEN" == "0" ]]' in run
