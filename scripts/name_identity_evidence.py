"""Collect read-only instrument-name evidence; never updates a roster or Sheets.

The active EODHD catalog is separate from its delisted catalog. Exact Code,
declared exchange, currency and instrument type are required for a candidate.
No historical label, replacement ticker or inferred instrument alias is used.
"""
from __future__ import annotations

import argparse
import base64
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import urllib.error
import urllib.parse
import urllib.request

VERSION = "1.0.0"
DOC_URL = "https://eodhd.com/financial-apis/exchanges-api-list-of-tickers-and-trading-hours"
EXCHANGE_ROUTES = {
    "US": "US", "L": "LSE", "T": "TSE", "HK": "HK", "PA": "PA",
    "AX": "AU", "AT": "AT", "MX": "MX", "MI": "MI", "CO": "CO",
    "SW": "SW", "IS": "IS", "TO": "TO", "OL": "OL", "NS": "NSE",
    "ST": "ST", "BH": "BH", "DE": "XETRA", "WA": "WAR", "BK": "BK",
}
COUNTRY_ISO2 = {
    "USA": "US", "UNITED STATES": "US", "UNITED STATES OF AMERICA": "US",
    "UK": "GB", "UNITED KINGDOM": "GB", "JAPAN": "JP", "HONG KONG": "HK",
    "FRANCE": "FR", "AUSTRALIA": "AU", "GREECE": "GR", "MEXICO": "MX",
    "ITALY": "IT", "DENMARK": "DK", "SWITZERLAND": "CH", "TURKEY": "TR",
    "CANADA": "CA", "NORWAY": "NO", "INDIA": "IN", "SWEDEN": "SE",
    "BAHRAIN": "BH", "GERMANY": "DE", "POLAND": "PL", "THAILAND": "TH",
}
US_VENUES = {"NYSE", "NASDAQ", "NYSE ARCA", "BATS", "NYSE MKT", "NYSE AMERICAN"}
IDENTITY_FIELDS = ("Code", "Name", "Country", "Exchange", "Currency", "Type", "Isin")
IDENTITY_STRING_LIMITS = {"Code": 64, "Name": 512, "Country": 64, "Exchange": 64,
                          "Currency": 16, "Type": 64}
ROSTER_HEADERS = ("Symbol", "Name", "Asset Class", "Exchange", "Currency", "Country")
SHEET_BOUNDS = {"Global_Markets": 6750, "Mutual_Funds": 2700}
READONLY_SCOPES = ("https://www.googleapis.com/auth/spreadsheets.readonly",)


def _text(value):
    return str(value or "").strip()


class ReadOnlyRosterError(RuntimeError):
    pass


def _readonly_sheets_session():
    """Use the existing credential env process with an explicitly read-only token."""
    try:
        from google.oauth2.service_account import Credentials
        from google.auth.transport.requests import AuthorizedSession
    except ImportError:
        raise ReadOnlyRosterError("Sheets reader requires google-auth and requests") from None
    try:
        raw = _text(os.getenv("GOOGLE_SHEETS_CREDENTIALS"))
        if not raw and _text(os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64")):
            raw = base64.b64decode(os.environ["GOOGLE_SHEETS_CREDENTIALS_B64"], validate=True).decode()
        if not raw and _text(os.getenv("GOOGLE_APPLICATION_CREDENTIALS")):
            raw = Path(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]).read_text()
        if not raw or len(raw) > 65_536:
            raise ValueError("credential unavailable or too large")
        info = json.loads(raw)
        if (not isinstance(info, dict) or info.get("type") != "service_account"
                or not info.get("client_email") or not info.get("private_key")
                or info.get("token_uri") not in ("https://oauth2.googleapis.com/token",
                                                  "https://accounts.google.com/o/oauth2/token")):
            raise ValueError("invalid service account")
        info["private_key"] = info["private_key"].replace("\\n", "\n")
        credentials = Credentials.from_service_account_info(info, scopes=list(READONLY_SCOPES))
        return AuthorizedSession(credentials)
    except Exception:
        raise ReadOnlyRosterError("Google read-only service-account credential unavailable or invalid") from None


def _validate_sheet_values(page, payload, bound):
    if (not isinstance(payload, dict) or payload.get("majorDimension") != "ROWS"
            or _text(payload.get("range")).replace("'", "") != f"{page}!A1:F{bound}"):
        raise ReadOnlyRosterError(page + ": unexpected values range or dimension")
    rows = payload.get("values", [])
    if (not isinstance(rows, list) or len(rows) > bound
            or any(not isinstance(row, list) or len(row) > 6
                   or any(value is not None and not isinstance(value, (str, int, float, bool))
                          for value in row) for row in rows)):
        raise ReadOnlyRosterError(page + ": invalid bounded A:F values")
    if len(rows) == bound and any(_text(value) for value in rows[-1]):
        raise ReadOnlyRosterError(page + ": final bounded row is filled; truncation cannot be excluded")
    expected = [header.lower() for header in ROSTER_HEADERS]
    headers = [index for index, row in enumerate(rows[:10])
               if [_text(value).lower() for value in row] == expected]
    if len(headers) != 1:
        raise ReadOnlyRosterError(page + ": canonical A:F header missing or ambiguous")
    index = headers[0]
    return [list(ROSTER_HEADERS), *rows[index + 1:]], index + 1


def read_sheet_roster(sheet_id, session_factory=None):
    """Read two fixed metadata ranges with no writer/client/settings imports."""
    sheet_id = _text(sheet_id)
    if not re.fullmatch(r"[A-Za-z0-9_-]{10,200}", sheet_id):
        raise ReadOnlyRosterError("A plain valid spreadsheet ID is required")
    session = (session_factory or _readonly_sheets_session)()
    pages, receipts = {}, {}
    try:
        for page, bound in SHEET_BOUNDS.items():
            range_name = f"'{page}'!A1:F{bound}"
            url = ("https://sheets.googleapis.com/v4/spreadsheets/" + sheet_id + "/values/"
                   + urllib.parse.quote(range_name, safe=""))
            response = session.get(url, params={"majorDimension": "ROWS",
                                   "valueRenderOption": "FORMATTED_VALUE",
                                   "fields": "range,majorDimension,values"},
                                   timeout=30, allow_redirects=False, stream=True)
            try:
                if response.status_code != 200:
                    raise ReadOnlyRosterError(page + ": Sheets HTTP " + str(response.status_code))
                chunks, size = [], 0
                for chunk in response.iter_content(chunk_size=65_536):
                    size += len(chunk)
                    if size > 4_000_000:
                        raise ReadOnlyRosterError(page + ": metadata response exceeds bound")
                    chunks.append(chunk)
                raw = b"".join(chunks)
                pages[page], header_row = _validate_sheet_values(page, json.loads(raw), bound)
                receipts[page] = {"requested_range": range_name, "header_row": header_row,
                                  "read_row_bound": bound, "last_bound_row_filled": False,
                                  "received_at_utc": datetime.now(timezone.utc).isoformat(),
                                  "http_date": response.headers.get("Date"),
                                  "sha256": hashlib.sha256(raw).hexdigest()}
            finally:
                try:
                    response.close()
                except Exception:
                    pass
    except ReadOnlyRosterError:
        raise
    except Exception:
        raise ReadOnlyRosterError("Google read-only values GET or JSON failed") from None
    finally:
        try:
            session.close()
        except Exception:
            pass
    return {"source": "google_sheets_values_get", "spreadsheet_id": sheet_id,
            "observed_at": datetime.now(timezone.utc).isoformat(), "pages": pages,
            "page_receipts": receipts, "oauth_scopes": list(READONLY_SCOPES)}


def missing_name_rows(roster):
    targets = []
    for page in ("Global_Markets", "Mutual_Funds"):
        rows = roster.get("pages", {}).get(page, [])
        if not rows:
            continue
        for index, values in enumerate(rows[1:], 1):
            row = dict(zip(rows[0], values))
            symbol, name = _text(row.get("Symbol")).upper(), _text(row.get("Name"))
            if not symbol or (name and name.upper() != symbol):
                continue
            base, suffix = symbol.rsplit(".", 1) if "." in symbol else (symbol, "")
            # A bare US identifier requires the row's explicit US namespace.
            if not suffix and _text(row.get("Country")).upper() in ("USA", "UNITED STATES") \
                    and _text(row.get("Exchange")).upper() in ("NYSE/NASDAQ", "NASDAQ/NYSE"):
                suffix = "US"
            target = {"page": page, "snapshot_data_index": index, "symbol": symbol,
                      "code": base, "provider_exchange": EXCHANGE_ROUTES.get(suffix),
                      "roster_metadata": row}
            header_row = roster.get("page_receipts", {}).get(page, {}).get("header_row")
            if header_row is not None:
                target["sheet_row"] = header_row + index
            targets.append(target)
    return targets


def _namespace_ok(target, exchanges):
    exchange = target["provider_exchange"]
    matches = [row for row in exchanges if _text(row.get("Code")).upper() == exchange]
    if len(matches) != 1:
        return False
    expected = COUNTRY_ISO2.get(_text(target["roster_metadata"].get("Country")).upper())
    observed = _text(matches[0].get("CountryISO2")).upper()
    if not observed:
        observed = COUNTRY_ISO2.get(_text(matches[0].get("Country")).upper())
    return bool(expected and expected == observed)


def _receipt_ok(catalog):
    receipt = (catalog or {}).get("receipt", {})
    try:
        received = datetime.fromisoformat(receipt.get("received_at_utc", ""))
        age = (datetime.now(timezone.utc) - received).total_seconds()
    except (TypeError, ValueError):
        return False
    return (receipt.get("http_status") == 200 and -300 <= age <= 86_400
            and bool(re.fullmatch(r"[a-f0-9]{64}", _text(receipt.get("sha256")))))


def _identity_string(value, limit):
    return (isinstance(value, str) and bool(value.strip())
            and len(value) <= limit and value.isprintable())


def _identity_record(row):
    """Retain bounded identity strings, without embedding malformed supplier objects."""
    result = {field: row.get(field) if _identity_string(row.get(field),
                            IDENTITY_STRING_LIMITS.get(field, 64)) else None for field in IDENTITY_FIELDS}
    invalid = [field for field, limit in IDENTITY_STRING_LIMITS.items()
               if not _identity_string(row.get(field), limit)]
    if invalid:
        result["invalid_fields"] = invalid
    return result


def evaluate_identity(target, exchanges, catalogs):
    result = {**target, "candidate_name": None, "classification": "unresolved", "evidence": []}
    exchange = target["provider_exchange"]
    if not exchange or not _namespace_ok(target, exchanges):
        result["classification"] = "unsupported_or_conflicting_namespace"
        return result
    active = catalogs.get((exchange, False))
    if not active or active.get("error"):
        result["classification"] = "active_catalog_unavailable"
        return result
    if not _receipt_ok(active):
        result["classification"] = "active_catalog_receipt_invalid_or_stale"
        return result
    exact = lambda catalog: [row for row in (catalog or {}).get("rows", [])
                             if _identity_string(row.get("Code"), IDENTITY_STRING_LIMITS["Code"])
                             and row["Code"].strip().upper() == target["code"]]
    found = exact(active)
    delisted = catalogs.get((exchange, True))
    if delisted and (delisted.get("error") or not _receipt_ok(delisted)):
        delisted = None
    retired = exact(delisted)
    for state, catalog, rows in (("active", active, found), ("delisted", delisted, retired)):
        if rows:
            result["evidence"].append({"catalog_state": state,
                                       "receipt": catalog.get("receipt", {}),
                                       "records": [_identity_record(row) for row in rows]})
    if len(found) > 1 or (found and retired):
        result["classification"] = "ambiguous_identity"
        return result
    if not found:
        if any(not _identity_string(row.get("Code"), IDENTITY_STRING_LIMITS["Code"])
               for row in active.get("rows", [])):
            result.update(classification="invalid_provider_metadata", conflicts=["Code"])
        elif retired:
            result["classification"] = "provider_delisted"
        elif not delisted or delisted.get("error"):
            result["classification"] = "absent_active_delisted_unavailable"
        else:
            result["classification"] = "not_listed_in_provider_catalogs"
        return result
    source = found[0]
    malformed = [field for field, limit in IDENTITY_STRING_LIMITS.items()
                 if not _identity_string(source.get(field), limit)]
    if malformed:
        result.update(classification="invalid_provider_metadata", conflicts=malformed)
        return result
    conflicts = []
    venue = _text(source.get("Exchange")).upper()
    if venue not in (US_VENUES if exchange == "US" else {exchange}):
        conflicts.append("declared_exchange")
    expected_currency = _text(target["roster_metadata"].get("Currency")).upper()
    if not expected_currency or _text(source.get("Currency")).upper() != expected_currency:
        conflicts.append("currency")
    expected_country = COUNTRY_ISO2.get(_text(target["roster_metadata"].get("Country")).upper())
    if COUNTRY_ISO2.get(_text(source.get("Country")).upper()) != expected_country:
        conflicts.append("country")
    allowed_types = ({"ETF", "FUND", "MUTUAL FUND"} if target["page"] == "Mutual_Funds"
                     else {"COMMON STOCK", "PREFERRED STOCK", "ADR", "REIT"})
    if _text(source.get("Type")).upper() not in allowed_types:
        conflicts.append("instrument_type")
    name = source["Name"].strip()
    if not name or name.upper() in (target["symbol"], target["code"]):
        conflicts.append("display_name")
    if conflicts:
        result.update(classification="metadata_conflict", conflicts=conflicts)
    else:
        result.update(classification="candidate", candidate_name=name)
    return result


def evidence_report(roster, exchanges, catalogs):
    rows = [evaluate_identity(target, exchanges, catalogs) for target in missing_name_rows(roster)]
    return {"schema": "tfb-name-identity-evidence/v1", "collector_version": VERSION,
            "observed_at_utc": datetime.now(timezone.utc).isoformat(),
            "roster_observed_at": roster.get("observed_at"), "read_only": True,
            "roster_page_receipts": roster.get("page_receipts", {}),
            "sheet_writes": False, "authorizes_repairs": False,
            "documentation": DOC_URL, "counts": dict(Counter(row["classification"] for row in rows)),
            "records": rows,
            "limitations": ["Catalog absence does not prove delisting or authorize retirement.",
                            "Provider delisted classification does not authorize aliases or roster removal.",
                            "A fresh HTTP receipt does not establish the provider's underlying update date.",
                            "Candidates require review; no name or numeric cells are written."]}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise urllib.error.URLError("provider redirect refused")


def fetch_catalog(path, params, token, base, timeout=20):
    """Bounded GET with sanitized errors/receipts; tokens never enter output."""
    receipt = {"source": base + "/" + path, "parameters": params,
               "received_at_utc": datetime.now(timezone.utc).isoformat()}
    url = receipt["source"] + "?" + urllib.parse.urlencode({**params, "api_token": token})
    request = urllib.request.Request(url, headers={"Accept": "application/json",
                                                 "Cache-Control": "no-cache",
                                                 "User-Agent": "TFB-NameIdentityEvidence/" + VERSION})
    try:
        with urllib.request.build_opener(NoRedirect()).open(request, timeout=timeout) as response:
            raw = response.read(2_000_001)
            receipt.update(http_status=response.status, http_date=response.headers.get("Date"),
                           last_modified=response.headers.get("Last-Modified"),
                           cache_age=response.headers.get("Age"))
        if len(raw) > 2_000_000:
            return {"rows": [], "receipt": receipt, "error": "response_exceeds_bound"}
        data = json.loads(raw)
        if not isinstance(data, list) or any(not isinstance(row, dict) for row in data):
            return {"rows": [], "receipt": receipt, "error": "invalid_catalog_shape"}
        receipt.update(sha256=hashlib.sha256(raw).hexdigest(), record_count=len(data))
        return {"rows": data, "receipt": receipt}
    except urllib.error.HTTPError as exc:
        return {"rows": [], "receipt": receipt, "error": "provider_http_" + str(exc.code)}
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        return {"rows": [], "receipt": receipt, "error": "provider_transport_or_json_failure"}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--roster", type=Path, help="Existing read-only roster JSON snapshot")
    source.add_argument("--sheet-id", help="Read current A:F metadata with Sheets read-only OAuth scope")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    token = next((_text(os.getenv(key)) for key in ("EODHD_API_KEY", "EODHD_API_TOKEN", "EODHD_KEY")
                  if _text(os.getenv(key))), "")
    base = _text(os.getenv("EODHD_BASE_URL")) or "https://eodhd.com/api"
    base = base.rstrip("/")
    if not token or base not in ("https://eodhd.com/api", "https://eodhistoricaldata.com/api"):
        parser.exit(2, "Provider credential unavailable or provider base not allowed; no requests made.\n")
    try:
        roster = read_sheet_roster(args.sheet_id) if args.sheet_id else json.loads(args.roster.read_text())
    except ReadOnlyRosterError as exc:
        parser.exit(2, "Read-only roster refused: " + str(exc) + "; no provider requests made.\n")
    except (OSError, ValueError):
        parser.exit(2, "Roster snapshot unavailable or invalid; no provider requests made.\n")
    exchange_response = fetch_catalog("exchanges-list", {}, token, base)
    exchanges = exchange_response["rows"]
    catalogs = {}
    targets = missing_name_rows(roster)
    groups = {}
    for target in targets:
        if _namespace_ok(target, exchanges):
            groups.setdefault(target["provider_exchange"], set()).add(target["code"])
    for exchange, codes in sorted(groups.items()):
        params = {"fmt": "json", "symbols": ",".join(sorted(codes))}
        active = fetch_catalog("exchange-symbol-list/" + exchange, params, token, base)
        catalogs[(exchange, False)] = active
        if active.get("error") in {"provider_http_401", "provider_http_402", "provider_http_403", "provider_http_429"}:
            break  # An account/auth/quota refusal must not fan out across exchanges.
        active_codes = {row["Code"].strip().upper() for row in active["rows"]
                        if _identity_string(row.get("Code"), IDENTITY_STRING_LIMITS["Code"])}
        missing = codes - active_codes
        if missing and not active.get("error"):
            catalogs[(exchange, True)] = fetch_catalog(
                "exchange-symbol-list/" + exchange,
                {**params, "symbols": ",".join(sorted(missing)), "delisted": 1}, token, base)
            if catalogs[(exchange, True)].get("error") in {
                    "provider_http_401", "provider_http_402", "provider_http_403", "provider_http_429"}:
                break
    report = evidence_report(roster, exchanges, catalogs)
    report["exchange_catalog"] = {key: value for key, value in exchange_response.items() if key != "rows"}
    report["catalog_requests"] = [dict(exchange=exchange, delisted=delisted,
                                        **{key: value for key, value in catalog.items() if key != "rows"})
                                  for (exchange, delisted), catalog in catalogs.items()]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"target_rows": len(targets), "counts": report["counts"],
                      "provider_gets": 1 + len(catalogs), "read_only": True}))
    return 2 if exchange_response.get("error") or any(c.get("error") for c in catalogs.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())
