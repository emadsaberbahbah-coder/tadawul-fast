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
