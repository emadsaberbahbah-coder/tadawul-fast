"""Fresh catalog evidence can propose names without inventing instrument identity."""
from datetime import datetime, timedelta, timezone
import base64
import builtins
import io
import json
import sys
from types import SimpleNamespace
import urllib.error
import urllib.parse

import pytest

from scripts import name_identity_evidence as collector


HEADER = ["Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country"]
EXCHANGES = [{"Code": "US", "CountryISO2": "US"}, {"Code": "AT", "CountryISO2": "GR"}]


def roster(symbol="ADVB.US", name=None, currency="USD", country="USA", page="Global_Markets"):
    return {"pages": {page: [HEADER, [symbol, name, "Equity", "NYSE/NASDAQ", currency, country]]}}


def catalog(rows):
    return {"rows": rows, "receipt": {"source": "https://eodhd.com/api/exchange-symbol-list/US",
            "http_status": 200, "received_at_utc": datetime.now(timezone.utc).isoformat(),
            "sha256": "1" * 64}}


def security(**patch):
    return {"Code": "ADVB", "Name": "Synthetic Advancer Common Stock", "Exchange": "NASDAQ",
            "Currency": "USD", "Country": "USA", "Type": "Common Stock", "Isin": "US0000000000", **patch}


def record(data, active, delisted=None, exchanges=EXCHANGES):
    catalogs = {("US", False): catalog(active)}
    if delisted is not None:
        catalogs[("US", True)] = catalog(delisted)
    return collector.evidence_report(data, exchanges, catalogs)["records"][0]


def test_exact_fresh_active_identity_proposes_name_with_complete_receipt_and_no_writes():
    report = collector.evidence_report(roster(), EXCHANGES, {("US", False): catalog([security()])})
    result = report["records"][0]
    assert result["classification"] == "candidate"
    assert result["candidate_name"] == "Synthetic Advancer Common Stock"
    assert result["evidence"][0]["records"][0]["Isin"] == "US0000000000"
    assert result["evidence"][0]["receipt"]["sha256"] == "1" * 64
    assert report["read_only"] and not report["sheet_writes"] and not report["authorizes_repairs"]


@pytest.mark.parametrize("patch,conflict", [({"Currency": "CAD"}, "currency"),
                                         ({"Exchange": "PINK"}, "declared_exchange"),
                                         ({"Type": "Notes"}, "instrument_type"),
                                         ({"Country": "Canada"}, "country"),
                                         ({"Name": "ADVB"}, "display_name")])
def test_metadata_conflicts_reject_name_only_repair(patch, conflict):
    result = record(roster(), [security(**patch)])
    assert result["classification"] == "metadata_conflict"
    assert conflict in result["conflicts"] and result["candidate_name"] is None


@pytest.mark.parametrize("field", ["Code", "Name", "Country", "Exchange", "Currency", "Type"])
@pytest.mark.parametrize("value", [{"Name": "foreign issuer"}, ["foreign issuer"], True, 123, None,
                                  "ADVB\x00foreign", "X" * 513])
def test_malformed_supplier_identity_never_becomes_a_name_candidate(field, value):
    result = record(roster(), [security(**{field: value})])
    assert result["classification"] == "invalid_provider_metadata"
    assert field in result["conflicts"] and result["candidate_name"] is None
    if result["evidence"]:
        sanitized = result["evidence"][0]["records"][0]
        assert sanitized[field] is None and field in sanitized["invalid_fields"]


@pytest.mark.parametrize("symbol,code", [("123.US", 123), ("TRUE.US", True)])
def test_nonstring_code_cannot_match_by_coercion(symbol, code):
    result = record(roster(symbol), [security(Code=code)])
    assert result["candidate_name"] is None and result["classification"] == "invalid_provider_metadata"


def test_current_code_absence_is_not_an_alias_or_a_delisting_claim():
    result = record(roster("OLD.US"), [security(Code="NEW", Name="Old company renamed")])
    assert result["classification"] == "absent_active_delisted_unavailable"
    assert result["candidate_name"] is None and result["evidence"] == []
    result = record(roster("OLD.US"), [], [])
    assert result["classification"] == "not_listed_in_provider_catalogs"
    assert result["candidate_name"] is None


def test_explicit_delisted_catalog_preserves_evidence_without_proposing_current_name():
    result = record(roster(), [], [security()])
    assert result["classification"] == "provider_delisted"
    assert result["candidate_name"] is None
    assert result["evidence"][0]["catalog_state"] == "delisted"


def test_duplicate_active_identity_is_quarantined():
    result = record(roster(), [security(), security(Name="A different issuer")])
    assert result["classification"] == "ambiguous_identity" and result["candidate_name"] is None


def test_namespace_country_disambiguates_greek_at_from_austrian_at():
    data = roster("JUMBO.AT", country="Greece", currency="EUR")
    result = collector.evidence_report(data, [{"Code": "AT", "CountryISO2": "AT"}], {})["records"][0]
    assert result["classification"] == "unsupported_or_conflicting_namespace"


@pytest.mark.parametrize("receipt_patch", [{"received_at_utc": (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()},
                                         {"received_at_utc": "2026-10-12"}, {"sha256": ""}, {"http_status": 401}])
def test_old_missing_or_invalid_receipt_cannot_certify_a_name(receipt_patch):
    source = catalog([security()])
    source["receipt"].update(receipt_patch)
    result = collector.evidence_report(roster(), EXCHANGES, {("US", False): source})["records"][0]
    assert result["classification"] == "active_catalog_receipt_invalid_or_stale"
    assert result["candidate_name"] is None


def test_bare_us_symbol_requires_explicit_namespace_and_keeps_original_identifier():
    result = record(roster("SPLG", page="Mutual_Funds"), [security(Code="SPLG", Type="ETF")])
    assert result["classification"] == "candidate" and result["symbol"] == "SPLG"
    unknown = collector.missing_name_rows(roster("SPLG", country=""))[0]
    assert unknown["provider_exchange"] is None


def test_transport_sanitizes_error_urls_and_never_returns_token(monkeypatch):
    token = "synthetic-secret-token"
    class Opener:
        def open(self, request, timeout):
            assert token in request.full_url and request.get_method() == "GET"
            raise urllib.error.URLError("secret in request https://eodhd.com/api?api_token=" + token)
    monkeypatch.setattr(collector.urllib.request, "build_opener", lambda *_args: Opener())
    response = collector.fetch_catalog("exchange-symbol-list/US", {"fmt": "json"}, token,
                                       "https://eodhd.com/api")
    assert response["error"] == "provider_transport_or_json_failure"
    assert token not in json.dumps(response) and "api_token" not in json.dumps(response)


def test_fetch_receipt_hashes_exact_response_and_preserves_declared_identity(monkeypatch):
    raw = json.dumps([security()]).encode()
    class Response(io.BytesIO):
        status = 200
        headers = {"Date": "Mon, 12 Oct 2026 00:00:00 GMT"}
    class Opener:
        def open(self, *_args, **_kwargs):
            return Response(raw)
    monkeypatch.setattr(collector.urllib.request, "build_opener", lambda *_args: Opener())
    response = collector.fetch_catalog("exchange-symbol-list/US", {"fmt": "json"}, "synthetic",
                                       "https://eodhd.com/api")
    assert response["rows"] == [security()]
    assert response["receipt"]["record_count"] == 1 and collector._receipt_ok(response)


def test_account_refusal_stops_requests_to_remaining_exchanges(monkeypatch, tmp_path):
    data = roster()
    data["pages"]["Global_Markets"].append(["JUMBO.AT", "", "Equity", "ATHEX", "EUR", "Greece"])
    source, output = tmp_path / "roster.json", tmp_path / "evidence.json"
    source.write_text(json.dumps(data))
    monkeypatch.setenv("EODHD_API_KEY", "synthetic-secret")
    monkeypatch.delenv("EODHD_BASE_URL", raising=False)
    requests = []
    def fetch(path, *_args):
        requests.append(path)
        return catalog(EXCHANGES) if path == "exchanges-list" else {
            "rows": [], "receipt": {}, "error": "provider_http_401"}
    monkeypatch.setattr(collector, "fetch_catalog", fetch)
    assert collector.main(["--roster", str(source), "--output", str(output)]) == 2
    assert len(requests) == 2
    report = json.loads(output.read_text())
    assert all(row["candidate_name"] is None for row in report["records"])
    assert "synthetic-secret" not in output.read_text()


class SheetResponse:
    status_code = 200
    headers = {"Date": "Mon, 12 Oct 2026 00:00:00 GMT"}

    def __init__(self, payload):
        self.raw = json.dumps(payload).encode()
        self.closed = False

    def iter_content(self, chunk_size):
        yield self.raw

    def close(self):
        self.closed = True


class ReadOnlySession:
    def __init__(self, payloads=None):
        self.payloads = payloads or {}
        self.requests = []
        self.responses = []
        self.closed = False

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        range_name = urllib.parse.unquote(url.rsplit("/", 1)[-1])
        page = range_name.split("!")[0].strip("'")
        payload = self.payloads.get(page, {"range": range_name, "majorDimension": "ROWS",
                                          "values": [[page], [], [], [], HEADER,
                                                     ["ADVB.US", "", "Equity", "NYSE/NASDAQ", "USD", "USA"]]})
        response = SheetResponse(payload)
        self.responses.append(response)
        return response

    def close(self):
        self.closed = True

    def post(self, *_args, **_kwargs):
        raise AssertionError("A roster reader cannot write")

    put = patch = delete = post


def test_live_reader_gets_only_bounded_metadata_and_preserves_actual_sheet_row(monkeypatch):
    session = ReadOnlySession()
    original_import = builtins.__import__
    def no_writer_import(name, *args, **kwargs):
        assert not name.startswith(("integrations", "core", "gspread", "googleapiclient"))
        return original_import(name, *args, **kwargs)
    monkeypatch.setattr(builtins, "__import__", no_writer_import)
    snapshot = collector.read_sheet_roster("synthetic-sheet-id", session_factory=lambda: session)
    assert len(session.requests) == 2
    assert [urllib.parse.unquote(url.rsplit("/", 1)[-1]) for url, _ in session.requests] == [
        "'Global_Markets'!A1:F6750", "'Mutual_Funds'!A1:F2700"]
    for url, options in session.requests:
        assert url.startswith("https://sheets.googleapis.com/v4/spreadsheets/")
        assert options["allow_redirects"] is False and options["stream"] is True
        assert options["params"]["fields"] == "range,majorDimension,values"
    assert snapshot["oauth_scopes"] == ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    assert all(target["sheet_row"] == 6 for target in collector.missing_name_rows(snapshot))
    assert snapshot["page_receipts"]["Global_Markets"]["header_row"] == 5
    assert session.closed and all(response.closed for response in session.responses)


@pytest.mark.parametrize("page", ["Global_Markets", "Mutual_Funds"])
def test_filled_final_bound_row_refuses_truncated_roster(page):
    bound = collector.SHEET_BOUNDS[page]
    rows = [HEADER] + [[] for _ in range(bound - 2)] + [["LAST.US", "", "Equity"]]
    payload = {"range": f"{page}!A1:F{bound}", "majorDimension": "ROWS", "values": rows}
    session = ReadOnlySession({page: payload})
    with pytest.raises(collector.ReadOnlyRosterError, match="truncation cannot be excluded"):
        collector.read_sheet_roster("synthetic-sheet-id", session_factory=lambda: session)
    assert session.closed and all(response.closed for response in session.responses)


@pytest.mark.parametrize("patch", [{"range": "Global_Markets!A1:G6750"},
                                  {"majorDimension": "COLUMNS"},
                                  {"values": [["Name", "Symbol", *HEADER[2:]]]},
                                  {"values": [HEADER, HEADER]},
                                  {"values": [HEADER, ["BAD.US", {}, "Equity"]]},
                                  {"values": [HEADER, ["BAD.US"] * 7]},
                                  {"values": [HEADER] + [[]] * 6750}])
def test_malformed_or_unexpected_sheet_layout_refuses_collection(patch):
    payload = {"range": "Global_Markets!A1:F6750", "majorDimension": "ROWS", "values": [HEADER], **patch}
    session = ReadOnlySession({"Global_Markets": payload})
    with pytest.raises(collector.ReadOnlyRosterError):
        collector.read_sheet_roster("synthetic-sheet-id", session_factory=lambda: session)
    assert len(session.requests) == 1 and session.closed


@pytest.mark.parametrize("mode", ["json", "base64"])
def test_google_credentials_always_use_only_readonly_scope(monkeypatch, mode):
    info = {"type": "service_account", "client_email": "synthetic@invalid.example",
            "private_key": "synthetic-private-key", "token_uri": "https://oauth2.googleapis.com/token"}
    monkeypatch.delenv("GOOGLE_SHEETS_CREDENTIALS", raising=False)
    monkeypatch.delenv("GOOGLE_SHEETS_CREDENTIALS_B64", raising=False)
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    raw = json.dumps(info)
    monkeypatch.setenv("GOOGLE_SHEETS_CREDENTIALS" if mode == "json" else "GOOGLE_SHEETS_CREDENTIALS_B64",
                       raw if mode == "json" else base64.b64encode(raw.encode()).decode())
    calls = []
    class Credentials:
        @classmethod
        def from_service_account_info(cls, value, scopes):
            calls.append((value, scopes))
            return "readonly-credentials"
    monkeypatch.setitem(sys.modules, "google.oauth2.service_account", SimpleNamespace(Credentials=Credentials))
    monkeypatch.setitem(sys.modules, "google.auth.transport.requests",
                        SimpleNamespace(AuthorizedSession=lambda credentials: credentials))
    assert collector._readonly_sheets_session() == "readonly-credentials"
    assert calls == [(info, ["https://www.googleapis.com/auth/spreadsheets.readonly"])]


def test_sheet_read_failure_is_sanitized_and_prevents_provider_requests(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("EODHD_API_KEY", "synthetic-secret")
    monkeypatch.delenv("EODHD_BASE_URL", raising=False)
    calls = []
    def refused(_sheet):
        raise collector.ReadOnlyRosterError("Global_Markets: final bounded row is filled")
    monkeypatch.setattr(collector, "read_sheet_roster", refused)
    monkeypatch.setattr(collector, "fetch_catalog", lambda *_args: calls.append("unexpected"))
    with pytest.raises(SystemExit) as exc:
        collector.main(["--sheet-id", "synthetic-sheet-id", "--output", str(tmp_path / "evidence.json")])
    assert exc.value.code == 2 and calls == []
    assert "final bounded row is filled" in capsys.readouterr().err


def test_cli_roster_sources_are_mutually_exclusive(tmp_path):
    with pytest.raises(SystemExit) as exc:
        collector.main(["--sheet-id", "synthetic-sheet-id", "--roster", str(tmp_path / "roster.json"),
                        "--output", str(tmp_path / "evidence.json")])
    assert exc.value.code == 2
