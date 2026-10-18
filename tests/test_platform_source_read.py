"""Offline contracts for the fixed, authenticated Site source-read relay.

Run without optional dependencies:
    python -m unittest discover -s tests -p test_platform_source_read.py -v
FastAPI's HTTP integration cases also run when the normal backend test
dependencies are installed. No credential, Google, provider, or Sheet is used.
"""
from __future__ import annotations

import ast
import asyncio
import copy
import importlib.util
import json
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core import platform_source_auth as auth
from integrations import platform_source_read as relay

KEY = "relay-fixture-only-abcdefghijklmnopqrstuvwxyz123456"


def request(path=auth.SOURCE_READ_PATH, method="POST", key=KEY, **scope_changes):
    scope = {"type": "http", "path": path, "method": method, "root_path": "",
             "query_string": b"", "headers": []}
    if key is not None:
        scope["headers"].append((b"x-tfb-source-read-key", key.encode("ascii")))
    scope.update(scope_changes)
    return types.SimpleNamespace(scope=scope, method=scope["method"])


def metadata():
    # Independent fixed-ID fixture. Grid dimensions deliberately differ from
    # the capture bounds, to test clipping against the current metadata.
    rows = [
        ("_Status", 1752783095, 100, 20),
        ("Top_10_Investments", 1333995836, 120, 30),
        ("Portfolio_Decision", 917293608, 100, 26),
        ("My_Portfolio", 83423516, 1500, 122),
        ("Daily_Review_2026_10_14", 20261014, 28, 8),
        ("_Portfolio_CostBasis", 647241069, 1000, 23),
        ("_Cash_Snapshot", 176574791, 1000, 5),
        ("Market_Leaders", 1952607146, 300, 115),
        ("Global_Markets", 1608764774, 6700, 115),
        ("Commodities_FX", 1986097637, 500, 115),
        ("Mutual_Funds", 1905078810, 2500, 115),
    ]
    return {"spreadsheetId": relay.SPREADSHEET_ID,
            "properties": {"title": "Fixture", "timeZone": "Etc/GMT-3"},
            "sheets": [{"properties": {"title": t, "sheetId": i,
                        "gridProperties": {"rowCount": r, "columnCount": c}}} for t, i, r, c in rows]}


CORE = ["'Top_10_Investments'!A1:AD80", "'Portfolio_Decision'!A1:Z60",
        "'My_Portfolio'!A1:DR64", "'Daily_Review_2026_10_14'!A1:H28"]
PERIMETER = ["'_Status'!A1:M18", "'_Portfolio_CostBasis'!A1:W1000",
             "'Top_10_Investments'!A1:B16", "'Portfolio_Decision'!A1:C5",
             "'_Cash_Snapshot'!A1:E1000"]
INDEX = ["'Market_Leaders'!A1:DK1", "'Market_Leaders'!A2:A300",
         "'Global_Markets'!A1:DK1", "'Global_Markets'!A2:A6700",
         "'Commodities_FX'!A1:DK1", "'Commodities_FX'!A2:A500",
         "'Mutual_Funds'!A1:DK1", "'Mutual_Funds'!A2:A2500"]


def cell_body(ranges=CORE):
    return {"operation": "cells", "ranges": list(ranges), "cellFields": relay.CELL_FIELDS}


def native_cells(ranges=CORE, number=0.07):
    meta = metadata()
    rectangles = relay.authorized_ranges(ranges, meta)
    sheets = []
    for source in meta["sheets"]:
        title = source["properties"]["title"]
        selected = [r for r in rectangles if r.title == title]
        if selected:
            sheets.append({"properties": copy.deepcopy(source["properties"]), "data": [
                {"startRow": r.first_row, "startColumn": 0, "rowData": [{"values": [
                    {"effectiveValue": {"numberValue": number}, "formattedValue": "7%",
                     "effectiveFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0%"}}}
                ]}]} for r in selected]})
    return {"spreadsheetId": relay.SPREADSHEET_ID, "properties": {"timeZone": "Etc/GMT-3"}, "sheets": sheets}


class FakeResponse:
    def __init__(self, data=None, *, raw=None, status=200, headers=None, chunks=None):
        self.raw = raw if raw is not None else json.dumps(data).encode()
        self.status_code, self.headers = status, headers or {}
        self.chunks = chunks
        self.closed, self.iterated = False, False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.closed = True

    def iter_content(self, chunk_size):
        self.iterated = True
        yield from self.chunks if self.chunks is not None else [self.raw]


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls, self.closed = list(responses), [], False

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result

    def close(self):
        self.closed = True


class AuthTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, {auth.SOURCE_READ_ENV: KEY})
        self.env.start()
        self.addCleanup(self.env.stop)

    def test_key_accepts_only_exact_post_with_single_header(self):
        self.assertTrue(auth.platform_source_authorized(request()))
        for method in ("GET", "HEAD", "OPTIONS", "PUT", "DELETE"):
            with self.subTest(method=method):
                self.assertFalse(auth.platform_source_authorized(request(method=method)))
        self.assertFalse(auth.platform_source_authorized(request(headers=[
            (b"x-tfb-source-read-key", KEY.encode()), (b"X-TFB-Source-Read-Key", KEY.encode())])))

    def test_other_paths_query_and_root_path_never_share_credential(self):
        for path in ("/health", "/v1/advanced/sheet-rows", auth.SOURCE_READ_PATH + "/", "/other"):
            with self.subTest(path=path):
                self.assertFalse(auth.platform_source_authorized(request(path=path)))
                self.assertTrue(auth.platform_source_boundary_required(request(path=path)))
        self.assertFalse(auth.platform_source_authorized(request(query_string=b"token=anything")))
        self.assertFalse(auth.platform_source_authorized(request(root_path="/v1")))

    def test_missing_bad_config_bad_key_and_header_aliases_fail_closed(self):
        for key in (None, "x" * 40, " " + KEY, KEY + " "):
            self.assertFalse(auth.platform_source_authorized(request(key=key)))
        for configured in ("", "short", "\n" + KEY, "x" * 257):
            with patch.dict(os.environ, {auth.SOURCE_READ_ENV: configured}):
                self.assertFalse(auth.platform_source_authorized(request()))
        for header in (b"authorization", b"x-app-token", b"x-api-key"):
            self.assertFalse(auth.platform_source_authorized(request(key=None, headers=[(header, KEY.encode())])))

    def test_source_route_is_protected_even_without_header(self):
        self.assertTrue(auth.platform_source_boundary_required(request(key=None)))
        self.assertFalse(auth.platform_source_boundary_required(request(path="/health", key=None)))

    def test_secret_redaction_recognizes_new_environment_and_header(self):
        from core.secret_redaction import redact_text
        self.assertNotIn(KEY, redact_text("bare " + KEY))
        with patch.dict(os.environ, {auth.SOURCE_READ_ENV: ""}):
            self.assertNotIn(KEY, redact_text("X-TFB-Source-Read-Key: " + KEY))


class RequestAndRangeTests(unittest.TestCase):
    def assert_invalid(self, value):
        with self.assertRaises(relay.SourceReadError) as caught:
            relay.decode_request(value if isinstance(value, bytes) else json.dumps(value).encode())
        self.assertEqual(caught.exception.status, 422)

    def test_exact_request_schema(self):
        self.assertEqual(relay.decode_request(b'{"operation":"metadata"}'), {"operation": "metadata"})
        self.assertEqual(relay.decode_request(json.dumps(cell_body()).encode()), cell_body())
        for value in ({"operation": "metadata", "spreadsheetId": "other"},
                      {"operation": "write"}, {**cell_body(), "url": "https://example.invalid"},
                      {**cell_body(), "cellFields": "userEnteredValue"},
                      {**cell_body(), "ranges": []}, {**cell_body(), "ranges": CORE + CORE},
                      [], b'{"operation":"metadata","operation":"metadata"}', b'{"operation":NaN}',
                      b'\xff', b'{}', {**cell_body(), "ranges": ["A1:Z100"]}):
            with self.subTest(value=repr(value)[:100]):
                self.assert_invalid(value)

    def test_body_size_is_limited_before_parsing(self):
        with self.assertRaises(relay.SourceReadError) as error:
            relay.decode_request(b" " * (relay.MAX_BODY_BYTES + 1))
        self.assertEqual(error.exception.status, 413)

    def test_fixed_plans_match_published_site_read_contract(self):
        self.assertEqual(relay.capture_plans(metadata()), {"perimeter": PERIMETER, "core": CORE, "index": INDEX})
        for ranges in (CORE, PERIMETER, INDEX, ["'Global_Markets'!A5:DK5", "'Mutual_Funds'!A200:DK200"]):
            self.assertEqual(len(relay.authorized_ranges(ranges, metadata())), len(ranges))

    def test_fresh_grid_dimensions_change_the_only_authorized_plan(self):
        meta = metadata()
        meta["sheets"][6]["properties"]["gridProperties"]["rowCount"] = 70
        expected = PERIMETER[:-1] + ["'_Cash_Snapshot'!A1:E70"]
        self.assertEqual(relay.capture_plans(meta)["perimeter"], expected)
        relay.authorized_ranges(expected, meta)
        with self.assertRaises(relay.SourceReadError):
            relay.authorized_ranges(PERIMETER, meta)

    def test_unapproved_ranges_and_full_universe_reads_are_rejected(self):
        bad = [CORE[:1], list(reversed(CORE)), ["'Global_Markets'!A1:DK6700"],
               ["'Global_Markets'!A5:DL5"], ["'Global_Markets'!A5:DK6"],
               ["'Global_Markets'!A6701:DK6701"], ["'_Cash_Snapshot'!A67:E67"],
               ["'_Status'!A1:M18"] + ["'Global_Markets'!A5:DK5"],
               ["'Secret_Tab'!A1:DK1"], ["'Global_Markets'!B5:DK5"]]
        for ranges in bad:
            with self.subTest(ranges=ranges), self.assertRaises(relay.SourceReadError):
                relay.authorized_ranges(ranges, metadata())
        self.assert_invalid(cell_body([f"'Global_Markets'!A{r}:DK{r}" for r in range(2, 23)]))

    def test_required_tab_identity_timezone_and_grid_are_checked(self):
        for mutate in (lambda m: m.update(spreadsheetId="other"),
                       lambda m: m["properties"].update(timeZone="UTC"),
                       lambda m: m["sheets"].pop(0),
                       lambda m: m["sheets"][0]["properties"].update(sheetId=42),
                       lambda m: m["sheets"][0]["properties"]["gridProperties"].update(rowCount=0),
                       lambda m: m["sheets"].append(copy.deepcopy(m["sheets"][0]))):
            meta = metadata()
            mutate(meta)
            with self.assertRaises(relay.SourceReadError):
                relay.validated_metadata(meta)

    def test_unrelated_metadata_is_not_exposed(self):
        meta = metadata()
        meta["sheets"].append({"properties": {"title": "Unrelated_Private_Tab", "sheetId": 5}})
        self.assertEqual(relay.validated_metadata(meta), metadata())


class NativeGridTests(unittest.TestCase):
    def test_numbers_formats_strings_and_coordinates_are_preserved(self):
        cells = native_cells()
        value = cells["sheets"][0]["data"][0]["rowData"][0]["values"][0]
        result = relay.validate_cells(cells, metadata(), relay.authorized_ranges(CORE, metadata()))
        self.assertIs(result, cells)
        self.assertEqual(value, {"effectiveValue": {"numberValue": 0.07}, "formattedValue": "7%",
                                "effectiveFormat": {"numberFormat": {"type": "PERCENT", "pattern": "0%"}}})
        for ranges in (PERIMETER, INDEX, ["'Global_Markets'!A99:DK99"]):
            relay.validate_cells(native_cells(ranges), metadata(), relay.authorized_ranges(ranges, metadata()))

    def test_missing_extra_or_changed_grid_is_rejected(self):
        changes = [lambda c: c.update(spreadsheetId="other"),
                   lambda c: c["properties"].update(timeZone="UTC"),
                   lambda c: c["sheets"].pop(),
                   lambda c: c["sheets"][0]["properties"].update(sheetId=1),
                   lambda c: c["sheets"][0]["properties"]["gridProperties"].update(rowCount=121),
                   lambda c: c["sheets"][0]["data"][0].update(startRow=1),
                   lambda c: c["sheets"][0]["data"][0].update(startColumn=1),
                   lambda c: c["sheets"][0]["data"].append(copy.deepcopy(c["sheets"][0]["data"][0])),
                   lambda c: c["sheets"][0]["data"][0]["rowData"][0]["values"][0].update(userEnteredValue={"formulaValue": "=SECRET()"})]
        for change in changes:
            cells = native_cells()
            change(cells)
            with self.assertRaises(relay.SourceReadError):
                relay.validate_cells(cells, metadata(), relay.authorized_ranges(CORE, metadata()))


class TransportTests(unittest.TestCase):
    def test_google_partial_response_mask_has_exact_native_fields(self):
        # Invalid grouping would make Google's spreadsheets.get reject every
        # refresh even when all mocked transport behavior otherwise succeeds.
        expected = (
            "spreadsheetId,properties(timeZone),sheets("
            "properties(sheetId,title,gridProperties(rowCount,columnCount)),"
            "data(startRow,startColumn,rowData(values(formattedValue,effectiveValue,"
            "effectiveFormat(numberFormat)))))"
        )
        self.assertEqual(relay.GRID_FIELDS, expected)
        for mask in (relay.METADATA_FIELDS, relay.GRID_FIELDS):
            depth = 0
            for character in mask:
                depth += (character == "(") - (character == ")")
                self.assertGreaterEqual(depth, 0)
            self.assertEqual(depth, 0)

    def fetch(self, response):
        session = FakeSession([response])
        value = relay._fetch(session, fields=relay.METADATA_FIELDS, ranges=None,
                             deadline=relay.time.monotonic() + 10)
        return value, session

    def test_request_is_fixed_read_only_no_redirects_or_cache(self):
        data, session = self.fetch(FakeResponse(metadata()))
        self.assertEqual(data, metadata())
        url, kwargs = session.calls[0]
        self.assertEqual(url, "https://sheets.googleapis.com/v4/spreadsheets/" + relay.SPREADSHEET_ID)
        self.assertTrue(kwargs["stream"])
        self.assertFalse(kwargs["allow_redirects"])
        self.assertEqual(kwargs["headers"]["Cache-Control"], "no-cache")
        self.assertLessEqual(max(kwargs["timeout"]), 10)

    def test_unsuccessful_responses_are_not_read_or_echoed(self):
        for status in (301, 302, 401, 403, 429, 500):
            response = FakeResponse(raw=b"sensitive provider error", status=status)
            with self.assertRaises(relay.SourceReadError) as caught:
                self.fetch(response)
            self.assertEqual(str(caught.exception), "source_unavailable")
            self.assertFalse(response.iterated)
            self.assertTrue(response.closed)

    def test_response_bytes_are_capped_before_json_parsing(self):
        with patch.object(relay, "MAX_RESPONSE_BYTES", 100):
            for response in (FakeResponse(raw=b"{}", headers={"Content-Length": "101"}),
                             FakeResponse(chunks=[b"x" * 60, b"x" * 60])):
                with self.assertRaises(relay.SourceReadError) as caught:
                    self.fetch(response)
                self.assertEqual(str(caught.exception), "source_response_too_large")
                self.assertTrue(response.closed)

    def test_bad_json_and_deadline_fail_without_payload_echo(self):
        for raw in (b"secret invalid JSON", b'{"x":NaN}', b'{"x":1,"x":2}', b"[]"):
            with self.assertRaises(relay.SourceReadError) as caught:
                self.fetch(FakeResponse(raw=raw))
            self.assertEqual(str(caught.exception), "source_response_invalid")
        with self.assertRaises(relay.SourceReadError) as caught:
            relay._fetch(FakeSession([]), fields="x", ranges=None, deadline=0)
        self.assertEqual(caught.exception.status, 504)


class ReadOperationTests(unittest.TestCase):
    def mock_factory(self, sessions):
        module = types.ModuleType("integrations.google_sheets_service")
        queue = iter(sessions)
        module.create_readonly_sheets_session = lambda: next(queue)
        return patch.dict(sys.modules, {"integrations.google_sheets_service": module})

    def test_success_uses_fresh_metadata_before_cells_and_closes_transport(self):
        session = FakeSession([FakeResponse(metadata()), FakeResponse(native_cells())])
        with self.mock_factory([session]):
            self.assertEqual(relay.read_source(cell_body()), native_cells())
        self.assertTrue(session.closed)
        self.assertEqual(len(session.calls), 2)
        self.assertIn(("includeGridData", "false"), session.calls[0][1]["params"])
        self.assertIn(("includeGridData", "true"), session.calls[1][1]["params"])
        self.assertEqual([v for k, v in session.calls[1][1]["params"] if k == "ranges"], CORE)
        self.assertIn(("fields", relay.GRID_FIELDS), session.calls[1][1]["params"])

    def test_repeated_refresh_does_not_reuse_old_cells(self):
        first = FakeSession([FakeResponse(metadata()), FakeResponse(native_cells(number=0.07))])
        second = FakeSession([FakeResponse(metadata()), FakeResponse(native_cells(number=0.09))])
        with self.mock_factory([first, second]):
            a, b = relay.read_source(cell_body()), relay.read_source(cell_body())
        self.assertNotEqual(a, b)
        self.assertEqual((len(first.calls), len(second.calls)), (2, 2))

    def test_invalid_plan_stops_before_cell_request(self):
        session = FakeSession([FakeResponse(metadata())])
        with self.mock_factory([session]), self.assertRaises(relay.SourceReadError):
            relay.read_source(cell_body(["'Global_Markets'!A1:DK6700"]))
        self.assertEqual(len(session.calls), 1)
        self.assertTrue(session.closed)

    def test_provider_exception_is_sanitized_and_transport_closed(self):
        session = FakeSession([RuntimeError("provider-credential-do-not-expose")])
        with self.mock_factory([session]), self.assertRaises(relay.SourceReadError) as caught:
            relay.read_source({"operation": "metadata"})
        self.assertEqual(str(caught.exception), "source_unavailable")
        self.assertTrue(session.closed)

    def test_concurrency_is_bounded_without_queueing(self):
        self.assertTrue(relay._READ_SLOTS.acquire(False))
        self.assertTrue(relay._READ_SLOTS.acquire(False))
        try:
            with self.mock_factory([]), self.assertRaises(relay.SourceReadError) as caught:
                relay.read_source({"operation": "metadata"})
            self.assertEqual(caught.exception.status, 429)
        finally:
            relay._READ_SLOTS.release()
            relay._READ_SLOTS.release()


class MiddlewareTests(unittest.IsolatedAsyncioTestCase):
    def wall(self, active):
        # Execute the real middleware class without importing the financial
        # application's routers or requiring its production dependencies.
        tree = ast.parse((ROOT / "main.py").read_text())
        node = next(n for n in tree.body if isinstance(n, ast.ClassDef)
                    and n.name == "GlobalAuthEnforcementMiddleware")
        namespace = {
            "BaseHTTPMiddleware": object, "Request": object,
            "platform_source_boundary_required": auth.platform_source_boundary_required,
            "platform_source_authorized": auth.platform_source_authorized,
            "_global_auth_enforcement_active": lambda: active,
            "_global_auth_exempt_request": lambda r: r.scope["path"] == "/health",
            "_auth_ok": lambda r: r.scope.get("general_auth", False),
            "_StrictJSONResponse": lambda **kwargs: kwargs,
            "_request_id_from_request": lambda r: "fixture",
            "_request_scope_path": lambda r: r.scope["path"],
        }
        import datetime
        namespace.update(datetime=datetime.datetime, timezone=datetime.timezone)
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(ROOT / "main.py"), "exec"), namespace)
        return namespace["GlobalAuthEnforcementMiddleware"]()

    async def test_dedicated_boundary_precedes_both_open_and_armed_global_gate(self):
        with patch.dict(os.environ, {auth.SOURCE_READ_ENV: KEY}):
            async def reached(r):
                return {"reached": True}

            for active in (False, True):
                wall = self.wall(active)
                self.assertEqual(await wall.dispatch(request(), reached), {"reached": True})
                for bad in (request(key=None), request(key="x" * 40),
                            request(path="/health"), request(path="/v1/advanced/sheet-rows"),
                            request(method="GET"), request(root_path="/v1"),
                            request(headers=[(b"host", b"x/health?x=")], key=None)):
                    result = await wall.dispatch(bad, reached)
                    self.assertEqual(result["status_code"], 401)
                    self.assertNotIn("reached", result)
                self.assertEqual(await wall.dispatch(request(path="/health", key=None), reached), {"reached": True})

    async def test_other_routes_retain_existing_auth_and_general_key_does_not_open_relay(self):
        with patch.dict(os.environ, {auth.SOURCE_READ_ENV: KEY}):
            async def reached(r):
                return {"reached": True}

            wall = self.wall(True)
            ordinary = request(path="/v1/advanced/sheet-rows", key=None, general_auth=True)
            self.assertEqual(await wall.dispatch(ordinary, reached), {"reached": True})
            result = await wall.dispatch(request(key=None, general_auth=True), reached)
            self.assertEqual(result["status_code"], 401)


class CredentialFactoryTests(unittest.TestCase):
    def test_readonly_factory_uses_existing_manager_and_isolated_session(self):
        tree = ast.parse((ROOT / "integrations/google_sheets_service.py").read_text())
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef)
                    and n.name == "create_readonly_sheets_session")
        credentials, sessions = [], []
        class Credentials:
            @staticmethod
            def from_service_account_info(info, scopes):
                credentials.append((info, scopes))
                return object()
        def session(credential, **kwargs):
            value = object()
            sessions.append((credential, kwargs, value))
            return value
        account = types.ModuleType("google.oauth2.service_account")
        account.Credentials = Credentials
        transport = types.ModuleType("google.auth.transport.requests")
        transport.AuthorizedSession = session
        namespaces = {"Any": object, "_credentials_manager": types.SimpleNamespace(get_credentials=lambda: {"fixture": True})}
        with patch.dict(sys.modules, {"google.oauth2.service_account": account,
                                     "google.auth.transport.requests": transport}):
            exec(compile(ast.Module(body=[node], type_ignores=[]), "factory", "exec"), namespaces)
            a, b = namespaces["create_readonly_sheets_session"](), namespaces["create_readonly_sheets_session"]()
        self.assertIsNot(a, b)
        self.assertEqual(len(credentials), 2)
        self.assertTrue(all(scopes == ["https://www.googleapis.com/auth/spreadsheets.readonly"] for _, scopes in credentials))
        self.assertTrue(all(options == {"max_refresh_attempts": 0, "refresh_timeout": 10} for _, options, _ in sessions))


HAS_HTTP_TEST_DEPS = all(importlib.util.find_spec(name) is not None for name in ("fastapi", "httpx"))


@unittest.skipUnless(HAS_HTTP_TEST_DEPS, "Normal backend FastAPI/httpx test dependencies are not installed locally")
class RouteHTTPTests(unittest.TestCase):
    def setUp(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from routes import platform_source
        self.route = platform_source
        app = FastAPI()
        app.include_router(platform_source.router)
        self.client = TestClient(app)
        self.addCleanup(self.client.close)
        self.env = patch.dict(os.environ, {auth.SOURCE_READ_ENV: KEY})
        self.env.start()
        self.addCleanup(self.env.stop)
        self.headers = {"X-TFB-Source-Read-Key": KEY}

    def test_success_immediately_returns_data_and_no_store(self):
        with patch.object(self.route, "read_source", return_value=metadata()) as read:
            response = self.client.post(auth.SOURCE_READ_PATH, json={"operation": "metadata"}, headers=self.headers)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "success", "data": metadata()})
        self.assertIn("no-store", response.headers["cache-control"])
        read.assert_called_once_with({"operation": "metadata"})

    def test_invalid_requests_never_call_source(self):
        with patch.object(self.route, "read_source") as read:
            self.assertEqual(self.client.post(auth.SOURCE_READ_PATH, json={"operation": "metadata"}).status_code, 401)
            self.assertEqual(self.client.post(auth.SOURCE_READ_PATH, json={"operation": "write"}, headers=self.headers).status_code, 422)
            headers = {**self.headers, "Content-Type": "application/json"}
            self.assertEqual(self.client.post(auth.SOURCE_READ_PATH, content="x" * 5000, headers=headers).status_code, 413)
            self.assertEqual(self.client.post(auth.SOURCE_READ_PATH, content="x", headers=self.headers).status_code, 415)
            read.assert_not_called()

    def test_provider_error_has_stable_sanitized_response(self):
        with patch.object(self.route, "read_source", side_effect=RuntimeError("private-provider-detail")):
            response = self.client.post(auth.SOURCE_READ_PATH, json={"operation": "metadata"}, headers=self.headers)
        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json(), {"status": "error", "error": "source_unavailable"})

    def test_actual_controlled_main_mount_and_auth_wall(self):
        # main caches its settings on first import. Run its real registration
        # and auth checks in a child process so this test's deliberate
        # PRESTART_MOUNT_ROUTES=false cannot disable later suites' routes.
        import subprocess
        script = r'''
import logging, socket, sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path.cwd() / "tests"))
from test_platform_source_read import KEY, metadata
from core import platform_source_auth as auth
from routes import platform_source
from fastapi import FastAPI
from fastapi.testclient import TestClient

def blocked(*args, **kwargs):
    raise OSError("network disabled by platform source regression test")
socket.socket.connect = blocked
socket.socket.connect_ex = blocked
socket.create_connection = blocked
socket.getaddrinfo = blocked
logging.disable(logging.CRITICAL)
import main
app = FastAPI()
snapshot = main._mount_routes_controlled(app)
assert ("platform_source", "routes.platform_source") in main._CONTROLLED_ROUTE_PLAN
assert "routes.platform_source" in snapshot["mounted"]
mounted = [(r.path, r.methods, r.endpoint.__module__) for r in app.routes
           if getattr(r, "path", "").startswith("/v1/platform")]
assert mounted == [(auth.SOURCE_READ_PATH, {"POST"}, "routes.platform_source")]
app.add_middleware(main.GlobalAuthEnforcementMiddleware)
headers = {"X-TFB-Source-Read-Key": KEY}
with TestClient(app) as client, patch.object(main, "_global_auth_enforcement_active", return_value=True), \
        patch.object(platform_source, "read_source", return_value=metadata()) as read:
    assert client.post(auth.SOURCE_READ_PATH, json={"operation": "metadata"}).status_code == 401
    response = client.post(auth.SOURCE_READ_PATH, json={"operation": "metadata"}, headers=headers)
    assert response.status_code == 200
    assert response.json()["data"] == metadata()
    assert client.get(auth.SOURCE_READ_PATH, headers=headers).status_code == 401
    assert client.post(auth.SOURCE_READ_PATH + "/", headers=headers, follow_redirects=False).status_code == 401
    assert client.post("/v1/advanced/sheet-rows", json={},
                       headers={**headers, "Host": "x/health?x="}).status_code == 401
    read.assert_called_once_with({"operation": "metadata"})
'''
        child_env = {**os.environ, "INIT_ENGINE_ON_BOOT": "false", "PRESTART_MOUNT_ROUTES": "false",
                     "ROUTES_STRICT_IMPORT": "false", "DISABLE_NETWORK_AT_IMPORT": "true",
                     "PROVIDERS_OFFLINE_MODE": "true", auth.SOURCE_READ_ENV: KEY}
        result = subprocess.run([sys.executable, "-c", script], cwd=ROOT, env=child_env,
                                text=True, capture_output=True, timeout=45)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


class GoogleClientCompatibilityTests(unittest.TestCase):
    def test_installed_authorized_session_accepts_bounded_streaming_get(self):
        try:
            import io
            import requests
            from google.auth.credentials import AnonymousCredentials
            from google.auth.transport.requests import AuthorizedSession
        except ImportError:
            self.skipTest("Normal backend google-auth/requests dependencies are not installed locally")
        sent = []
        class OfflineAdapter(requests.adapters.BaseAdapter):
            def send(self, prepared, **kwargs):
                sent.append((prepared, kwargs))
                response = requests.Response()
                response.status_code = 200
                response.headers["Content-Type"] = "application/json"
                response.raw = io.BytesIO(json.dumps(metadata()).encode())
                response.request = prepared
                return response
            def close(self):
                pass
        with AuthorizedSession(AnonymousCredentials(), max_refresh_attempts=0, refresh_timeout=10) as session:
            session.mount("https://sheets.googleapis.com/", OfflineAdapter())
            result = relay._fetch(session, fields=relay.METADATA_FIELDS, ranges=None,
                                  deadline=relay.time.monotonic() + 10)
        self.assertEqual(result, metadata())
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][0].method, "GET")
        self.assertTrue(sent[0][0].url.startswith(relay.SHEETS_URL + "?"))
        self.assertTrue(sent[0][1]["stream"])


if __name__ == "__main__":
    unittest.main()
