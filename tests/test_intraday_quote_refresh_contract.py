"""Real intraday main/HTTP/Sheets wire, synthetic rows and no external I/O."""
import copy
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import json
import re
import sys
import urllib.request

import pytest

from core.analysis import opportunity_builder as ob
from core.data_validity import acquisition_tokens, row_acquisition
from core.sheets.schema_registry import get_sheet_headers, get_sheet_keys
from scripts import intraday_quote_refresh as iqr

UTC = timezone.utc
NOW = datetime(2026, 10, 9, 12, tzinfo=UTC)
OLD = NOW - timedelta(minutes=10)
QUOTE = NOW - timedelta(minutes=2)
HEADER = ["Symbol", "Name", "Current Price", "Currency", "Last Updated (UTC)", "Last Updated (Riyadh)",
          "Warnings", "Forecast Price (1M)", "Expected ROI (1M)", "Forecast Price (3M)", "Expected ROI (3M)",
          "Forecast Price (12M)", "Expected ROI (12M)", "Target Price", "Upside/Downside %", "Intrinsic Value", "Upside %",
          "Profit Margin", "Manual Input", "Overall Score", "Recommendation", "Data Provider"]


class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return NOW if tz is None else NOW.astimezone(tz)


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(iqr, "_clock", lambda: NOW)
    monkeypatch.setattr(ob, "datetime", Clock)
    monkeypatch.setattr(ob, "_venue_state", lambda *args: None)
    monkeypatch.setenv("TFB_TICKET_MAX_QUOTE_AGE_MIN", "15")
    monkeypatch.setenv("TFB_IQR_SYMBOL_PAGES", "Top_10_Investments,My_Portfolio,Shadow_Board")
    monkeypatch.setenv("TFB_IQR_TARGET_PAGES", "Market_Leaders,Global_Markets")
    monkeypatch.setattr(urllib.request, "urlopen", lambda *args, **kwargs: pytest.fail("unexpected live provider HTTP"))


def source(symbol="SYNTH.SR", price=110, currency="SAR", **changes):
    row = {"symbol": symbol, "current_price": price, "currency": currency, "data_provider": "EODHD",
           "acquisition_status": "success", "acquisition_provider": "EODHD",
           "acquisition_acquired_at": (NOW-timedelta(seconds=5)).isoformat(),
           "acquisition_quote_asof": QUOTE.isoformat()}
    row.update(changes)
    return row


def target(symbol="SYNTH.SR", **changes):
    row = {"Symbol": symbol, "Name": "Synthetic", "Current Price": 100, "Currency": "SAR",
           "Last Updated (UTC)": OLD.strftime("%Y-%m-%d %H:%M:%S"),
           "Last Updated (Riyadh)": (OLD+timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S"),
           "Warnings": "operator_note:keep; acquisition_status:success; acquisition_provider:EODHD; acquisition_acquired_at:"
                       + OLD.isoformat() + "; acquisition_quote_asof:" + (OLD-timedelta(minutes=1)).isoformat(),
           "Forecast Price (1M)": 110, "Expected ROI (1M)": .1,
           "Forecast Price (3M)": 120, "Expected ROI (3M)": .2,
           "Forecast Price (12M)": 130, "Expected ROI (12M)": .3,
           "Target Price": 140, "Upside/Downside %": .4, "Intrinsic Value": 150, "Upside %": .5,
           "Profit Margin": .9, "Manual Input": "manual untouched", "Overall Score": 85, "Recommendation": "BUY", "Data Provider": "EODHD"}
    row.update(changes)
    return [row.get(name, "") for name in HEADER]


def quotes(rows=None):
    return {row["symbol"]: iqr._source_quote(row, row["symbol"], NOW) for row in (rows or [source()])}


class Response:
    def __init__(self, payload):
        self.payload = payload
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def read(self):
        return json.dumps(self.payload).encode()


def serve(monkeypatch, rows):
    calls = []
    payload = {"rows": [[row.get("symbol"), row.get("current_price")] for row in rows],
               "data": copy.deepcopy(rows), "row_objects": copy.deepcopy(rows)}
    def get(url, **kwargs):
        calls.append(url)
        return Response(payload)
    monkeypatch.setattr(urllib.request, "urlopen", get)
    return calls


class Worksheet:
    def __init__(self, values, hook=None, fail_batch=False):
        self.values = copy.deepcopy(values)
        self.reads, self.writes, self.appends = [], [], []
        self.hook, self.fail_batch = hook, fail_batch
    def get_all_values(self, *, value_render_option):
        assert value_render_option == "UNFORMATTED_VALUE", "formatted reads cannot certify numeric return/proof"
        self.reads.append(value_render_option)
        if self.hook:
            self.hook(len(self.reads), self.values)
        return copy.deepcopy(self.values)
    def batch_update(self, updates, *, value_input_option):
        assert value_input_option == "RAW"
        if self.fail_batch:
            raise RuntimeError("synthetic request rejected")
        self.writes.append(copy.deepcopy(updates))
        for update in updates:
            match = re.fullmatch(r"([A-Z]+)([0-9]+)", update["range"])
            assert match, "never whole-row or range writes"
            col = 0
            for letter in match[1]:
                col = col * 26 + ord(letter) - ord("A") + 1
            row = self.values[int(match[2])-1]
            row.extend([""] * max(0, col-len(row)))
            row[col-1] = update["values"][0][0]
        return {"totalUpdatedCells": len(updates)}
    def append_row(self, row, **kwargs):
        self.appends.append(copy.deepcopy(row))
    def clear(self, *args, **kwargs):
        pytest.fail("market/financial clear forbidden")
    def update(self, *args, **kwargs):
        pytest.fail("whole-row update forbidden")


class Book:
    def __init__(self, market=None):
        self.pages = {"Top_10_Investments": Worksheet([["Symbol", "Name"], ["SYNTH.SR", "Synthetic"]]),
                      "My_Portfolio": Worksheet([["Symbol", "Position Qty"], ["SYNTH.SR", 10]]),
                      "Shadow_Board": Worksheet([["Symbol", "Name"], ["SYNTH.SR", "Synthetic"]]),
                      "Market_Leaders": market or Worksheet([HEADER, target()]),
                      "Global_Markets": Worksheet([HEADER, target()]),
                      "_Run_Log": Worksheet([])}
    def worksheet(self, name):
        return self.pages[name]


def main_wire(monkeypatch, rows=None, book=None, apply=True):
    book = book or Book()
    calls = serve(monkeypatch, rows or [source()])
    monkeypatch.setattr(iqr, "_open_sheet", lambda *args: book)
    monkeypatch.setattr(sys, "argv", ["intraday_quote_refresh", "--apply" if apply else "--scan", "--backend", "https://synthetic.invalid"])
    return iqr.main(), book, calls


def test_actual_main_updates_quote_without_old_roi_or_false_full_model_certification(monkeypatch):
    code, book, http = main_wire(monkeypatch)
    assert code == 0 and len(http) == 2
    for name in ("Market_Leaders", "Global_Markets"):
        sheet = book.pages[name]
        row = dict(zip(HEADER, sheet.values[1]))
        assert row["Current Price"] == 110
        for key in ("Expected ROI (1M)", "Expected ROI (3M)", "Expected ROI (12M)", "Upside/Downside %", "Upside %"):
            assert row[key] == "", "stale return display must be explicitly blanked"
        for key in ("Forecast Price (1M)", "Forecast Price (3M)", "Forecast Price (12M)", "Target Price", "Intrinsic Value",
                    "Profit Margin", "Manual Input", "Overall Score", "Recommendation", "Symbol", "Name", "Currency"):
            assert row[key] == dict(zip(HEADER, target()))[key]
        assert iqr._parse_ts(row["Last Updated (UTC)"], "Last Updated (UTC)") == NOW
        assert iqr._parse_ts(row["Last Updated (Riyadh)"], "Last Updated (Riyadh)") == NOW
        tokens = acquisition_tokens(row["Warnings"])
        assert tokens["acquisition_status"] == "preserved" and tokens["acquisition_quote_asof"] == QUOTE.isoformat()
        assert "intraday_quote_currency:SAR" in row["Warnings"] and "operator_note:keep" in row["Warnings"]
        proof = row_acquisition(row, NOW, 3600)
        assert not proof.successful and proof.reason == "acquisition_preserved"
        assert ob._quote_freshness_assessment({"symbol": "SYNTH.SR", "quote_evidence": {"status": proof.status,
                                                  "reason": proof.reason, "quote_asof": tokens["acquisition_quote_asof"]}})[0] is False
        assert len(sheet.writes) == 1
        allowed = {2, 4, 5, 6, 8, 10, 12, 14, 16}
        assert {int(re.search(r"[0-9]+", patch["range"])[0]) for patch in sheet.writes[0]} == {2}
        assert all(patch["range"] != "A2" for patch in sheet.writes[0])
        assert len(sheet.writes[0]) == len(allowed)
    for name in ("My_Portfolio", "Top_10_Investments", "Shadow_Board"):
        assert not book.pages[name].writes and not book.pages[name].appends
    assert len(book.pages["_Run_Log"].appends) == 1


def test_actual_main_canonical_115_columns_with_warning_only_source_receipt(monkeypatch):
    headers, keys = get_sheet_headers("Market_Leaders"), get_sheet_keys("Market_Leaders")
    assert len(headers) == len(keys) == 115
    normalized = {iqr._key(name): value for name, value in zip(HEADER, target())}
    original = [normalized.get(iqr._key(header), "untouched:%d" % i) for i, header in enumerate(headers)]
    source_row = source()
    incoming = {key: source_row.get(key, "") for key in keys}
    incoming["warnings"] = "; ".join(name + ":" + source_row[name] for name in (
        "acquisition_status", "acquisition_provider", "acquisition_acquired_at", "acquisition_quote_asof"))
    assert not any(key.startswith("acquisition_") for key in incoming)
    book = Book(Worksheet([headers, original]))
    book.pages["Global_Markets"] = Worksheet([get_sheet_headers("Global_Markets"), original])
    code, book, calls = main_wire(monkeypatch, [incoming], book)
    assert code == 0 and len(calls) == 2
    allowed = {"current_price", "last_updated_utc", "last_updated_riyadh", "warnings", "expected_roi_1m",
               "expected_roi_3m", "expected_roi_12m", "upside_downside_pct", "upside_pct"}
    for page in ("Market_Leaders", "Global_Markets"):
        sheet = book.pages[page]
        changed = {keys[i] for i, value in enumerate(sheet.values[1]) if value != original[i]}
        assert changed == allowed and len(sheet.writes) == 1 and len(sheet.writes[0]) == 9
        assert len(sheet.values) == 2 and len(sheet.values[1]) == 115
        assert row_acquisition(dict(zip(headers, sheet.values[1])), NOW, 3600).reason == "acquisition_preserved"


@pytest.mark.parametrize("changes", [
    {"acquisition_status": "failed"}, {"acquisition_status": "preserved"}, {"warnings": "fetch_failed:upstream"},
    {"warnings": "price_unverified_live:history"}, {"warnings": "price_bar_stale:5"}, {"data_provider": "snapshot"},
    {"acquisition_quote_asof": ""}, {"acquisition_quote_asof": "2026-10-09"},
    {"acquisition_quote_asof": "2026-10-09T11:58:00"},
    {"acquisition_quote_asof": (NOW-timedelta(days=7)).isoformat()},
    {"acquisition_quote_asof": (NOW+timedelta(minutes=2)).isoformat()},
    {"currency": "USD"}, {"currency": "GBp"}, {"currency": ""}, {"ticker": "OTHER.SR"},
    {"current_price": True}, {"current_price": 0}, {"current_price": -1},
    {"current_price": float("inf")}, {"current_price": float("nan")},
    {"Provider": "yahoo_chart"}, {"price_bar_ts": (NOW-timedelta(days=7)).isoformat()},
    {"quote_timestamp": "2026-10-09"}, {"regularMarketTime": (NOW-timedelta(days=7)).timestamp()},
])
def test_actual_main_bad_incoming_proof_never_writes_price(changes, monkeypatch):
    code, book, calls = main_wire(monkeypatch, [source(**changes)])
    assert code == 0
    for name in ("Market_Leaders", "Global_Markets"):
        assert book.pages[name].values == [HEADER, target()] and not book.pages[name].writes


def test_source_alias_timestamps_cannot_hide_duplicate_normalized_conflict():
    row = source(price_bar_ts=QUOTE.isoformat())
    row["Price Bar TS"] = OLD.isoformat()
    assert iqr._source_quote(row, "SYNTH.SR", NOW) is None


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("conflict", [source(acquisition_status="preserved"), source(price=120)])
def test_actual_http_same_envelope_conflicting_duplicates_reject_both_orders(conflict, reverse, monkeypatch):
    rows = [source(), conflict]
    if reverse:
        rows.reverse()
    serve(monkeypatch, rows)
    assert iqr.fetch_quotes("https://synthetic.invalid", "Market_Leaders", ["SYNTH.SR"]) == {}


def test_live_source_and_explicit_known_close_use_existing_public_freshness_contract(monkeypatch):
    assert iqr._source_quote(source(), "SYNTH.SR", NOW) is not None
    old = NOW-timedelta(hours=4)
    monkeypatch.setattr(ob, "_venue_state", lambda *args: (False, old))
    assert iqr._source_quote(source(acquisition_quote_asof=(old-timedelta(minutes=1)).isoformat()), "SYNTH.SR", NOW) is not None
    assert iqr._source_quote(source(acquisition_quote_asof=(old-timedelta(minutes=3)).isoformat()), "SYNTH.SR", NOW) is None
    monkeypatch.setattr(ob, "_venue_state", lambda *args: (True, old))
    assert iqr._source_quote(source(acquisition_quote_asof=old.isoformat()), "SYNTH.SR", NOW) is None


def test_bare_price_dictionary_has_no_witness_and_valid_witness_is_immutable():
    assert not iqr.plan_page_updates("Market_Leaders", [HEADER, target()], {"SYNTH.SR": {"price": 110, "last_updated": NOW.isoformat()}})[0]
    evidence = quotes()["SYNTH.SR"]
    with pytest.raises(FrozenInstanceError):
        evidence.price = 999


@pytest.mark.parametrize("field,value", [("acquired_at", "2026-10-09"), ("quote_asof", NOW.replace(tzinfo=None)),
                                         ("retrieved_at", None)])
def test_malformed_evidence_clock_is_unverified_without_crashing(field, value):
    evidence = replace(quotes()["SYNTH.SR"], **{field: value})
    plan, stats = iqr.plan_page_updates("Market_Leaders", [HEADER, target()], {"SYNTH.SR": evidence})
    assert not plan and stats["skipped_unverified"] == 1


@pytest.mark.parametrize("changes", [
    {"Currency": "USD"}, {"Currency": "GBp"},
    {"Last Updated (Riyadh)": OLD.strftime("%Y-%m-%d %H:%M:%S")},
    {"Last Updated (UTC)": "2026-10-09"},
    {"Warnings": "acquisition_status:success; acquisition_quote_asof:" + (NOW-timedelta(seconds=30)).isoformat()},
    {"Warnings": "acquisition_quote_asof:2026-10-09"},
])
def test_target_currency_clocks_and_known_quote_monotonicity(changes):
    assert not iqr.plan_page_updates("Market_Leaders", [HEADER, target(**changes)], quotes())[0]


def test_naive_riyadh_clock_is_not_mistaken_for_utc():
    row = target()
    assert iqr._parse_ts(row[5], HEADER[5]) == iqr._parse_ts(row[4], HEADER[4]) == OLD
    assert iqr.plan_page_updates("Market_Leaders", [HEADER, row], quotes())[0]


def test_canonical_separately_sampled_stamp_aliases_accept_same_second_jitter():
    utc = OLD.replace(microsecond=485300)
    riyadh = OLD.replace(microsecond=485320).astimezone(timezone(timedelta(hours=3)))
    row = target(**{"Last Updated (UTC)": utc.isoformat(), "Last Updated (Riyadh)": riyadh.isoformat()})
    plan, stats = iqr.plan_page_updates("Mutual_Funds", [HEADER, row], quotes())
    assert len(plan) == 1 and not stats["skipped_schema"]
    assert iqr._parse_ts(plan[0]["changes"][4], HEADER[4]) == iqr._parse_ts(plan[0]["changes"][5], HEADER[5]) == NOW


def test_stamp_alias_jitter_uses_maximum_actual_instant_for_one_way_comparison():
    row = target(**{"Last Updated (UTC)": NOW.replace(microsecond=485300).isoformat(),
                    "Last Updated (Riyadh)": NOW.replace(microsecond=485320).isoformat()})
    evidence = replace(quotes()["SYNTH.SR"], retrieved_at=NOW.replace(microsecond=485310))
    plan, stats = iqr.plan_page_updates("Market_Leaders", [HEADER, row], {"SYNTH.SR": evidence})
    assert not plan and stats["skipped_not_newer"] == 1 and not stats["skipped_schema"]


def test_stamp_aliases_in_different_whole_seconds_fail_even_when_less_than_one_second_apart():
    row = target(**{"Last Updated (UTC)": OLD.replace(microsecond=999999).isoformat(),
                    "Last Updated (Riyadh)": (OLD+timedelta(seconds=1)).isoformat()})
    plan, stats = iqr.plan_page_updates("Market_Leaders", [HEADER, row], quotes())
    assert not plan and stats["skipped_schema"] == 1


@pytest.mark.parametrize("reverse", [False, True])
def test_http_conflicting_capitalized_symbol_alias_cannot_be_silently_ignored(reverse, monkeypatch):
    conflict = source(symbol="OTHER.SR", Ticker="SYNTH.SR")
    rows = [source(), conflict]
    serve(monkeypatch, list(reversed(rows)) if reverse else rows)
    assert iqr.fetch_quotes("https://synthetic.invalid", "Market_Leaders", ["SYNTH.SR"]) == {}


@pytest.mark.parametrize("extra", ["Warnings", "Flags", "Expected ROI (12M)", "Acquisition Status", "Current Price", "Ticker"])
def test_ambiguous_provenance_or_owned_schema_cannot_receive_partial_updates(extra):
    values = [HEADER + [extra], target() + ["success"]]
    assert not iqr.plan_page_updates("Market_Leaders", values, quotes())[0]


@pytest.mark.parametrize("kind", ["symbol", "manual", "stamp", "header", "price", "model_price"])
def test_actual_main_race_rechecks_whole_header_row_before_write(kind, monkeypatch):
    def mutate(reads, rows):
        if reads == 2:
            if kind == "header":
                rows[0][18] = "Different Manual Header"
            else:
                col = {"symbol": 0, "manual": 18, "stamp": 4, "price": 2, "model_price": 11}[kind]
                rows[1][col] = "concurrent change"
    sheet = Worksheet([HEADER, target()], hook=mutate)
    code, book, calls = main_wire(monkeypatch, book=Book(sheet))
    assert code == 0 and not sheet.writes


def test_every_batch_rechecks_fingerprints_and_never_splits_a_row(monkeypatch):
    rows = [target("SYNTH%d.SR" % i) for i in range(24)]
    evidence = quotes([source("SYNTH%d.SR" % i) for i in range(24)])
    values = [HEADER] + rows
    plan, _ = iqr.plan_page_updates("Market_Leaders", values, evidence)
    def mutate(reads, grid):
        if reads == 2:
            grid[12][18] = "changed between batches"
    sheet = Worksheet(values, hook=mutate)
    written, abandoned, failed = iqr.apply_page_plan(sheet, "Market_Leaders", plan)
    assert not failed and abandoned == 1 and written == 23*9
    assert len(sheet.writes) == len(sheet.reads) == 3
    row_batches = {}
    for batch_index, batch in enumerate(sheet.writes):
        assert len(batch) <= 100
        for patch in batch:
            row = int(re.search(r"[0-9]+", patch["range"])[0])
            row_batches.setdefault(row, set()).add(batch_index)
    assert all(len(batches) == 1 for batches in row_batches.values()) and 13 not in row_batches


def test_mutated_plan_value_cannot_break_witness_binding():
    values = [HEADER, target()]
    plan, _ = iqr.plan_page_updates("Market_Leaders", values, quotes())
    plan[0]["price_new"] = 999
    plan[0]["changes"][2] = 999
    sheet = Worksheet(values)
    assert iqr.apply_page_plan(sheet, "Market_Leaders", plan) == (0, 1, False)
    assert not sheet.writes


def test_failed_batch_never_counts_planned_cells_as_written(monkeypatch):
    code, book, calls = main_wire(monkeypatch, book=Book(Worksheet([HEADER, target()], fail_batch=True)))
    assert code == 2 and not book.pages["Market_Leaders"].writes
    log = book.pages["_Run_Log"].appends[-1]
    assert log[4] == "PARTIAL" and "cells=9" in log[5]  # only other successful page's cells


def test_scan_and_target_scope_preserve_financial_rows_and_market_membership(monkeypatch):
    code, book, calls = main_wire(monkeypatch, apply=False)
    assert code == 0 and not book.pages["Market_Leaders"].writes and not book.pages["_Run_Log"].appends
    monkeypatch.setenv("TFB_IQR_TARGET_PAGES", "My_Portfolio,Top_10_Investments,Market_Leaders")
    code, book, calls = main_wire(monkeypatch)
    assert code == 0 and not book.pages["My_Portfolio"].writes and not book.pages["Top_10_Investments"].writes
    assert len(book.pages["Market_Leaders"].values) == 2


def test_coherent_existing_return_is_preserved_without_recalculation():
    row = target(**{"Forecast Price (1M)": 121, "Expected ROI (1M)": .1})
    plan, _ = iqr.plan_page_updates("Market_Leaders", [HEADER, row], quotes())
    assert 8 not in plan[0]["changes"]
    assert plan[0]["changes"][2] == 110


# ------------------------------------------------------------ v1.0.4 -------
# 4 Nov 2026: the backend answered 401 to the token-less sheet-rows GETs
# (dry run 41851262140), so no quote was ever read while runs ended green.
import io
import urllib.error

import yaml

TOKEN_NAMES = ("TFB_TOKEN", "X_APP_TOKEN", "APP_TOKEN", "BACKEND_TOKEN")


def _clear_tokens(monkeypatch):
    for name in TOKEN_NAMES:
        monkeypatch.delenv(name, raising=False)


def test_v104_request_carries_the_backend_token_like_the_sync(monkeypatch):
    _clear_tokens(monkeypatch)
    anonymous = iqr._backend_request("https://synthetic.invalid/v1/analysis/sheet-rows?page=x")
    assert anonymous.get_header("X-app-token") is None and anonymous.get_header("Authorization") is None
    monkeypatch.setenv("BACKEND_TOKEN", "synthetic-backend")
    req = iqr._backend_request("https://synthetic.invalid/v1/analysis/sheet-rows?page=x")
    assert req.get_header("X-app-token") == "synthetic-backend"
    assert req.get_header("Authorization") == "Bearer synthetic-backend"
    monkeypatch.setenv("TFB_TOKEN", "synthetic-first")        # run_dashboard_sync order: TFB_TOKEN first
    assert iqr._backend_request("https://x.invalid").get_header("X-app-token") == "synthetic-first"


def test_v104_fetch_sends_the_token_and_reads_quotes(monkeypatch):
    _clear_tokens(monkeypatch)
    monkeypatch.setenv("BACKEND_TOKEN", "synthetic-backend")
    calls = serve(monkeypatch, [source()])
    got = iqr.fetch_quotes("https://synthetic.invalid", "Market_Leaders", ["SYNTH.SR"])
    assert set(got) == {"SYNTH.SR"}
    assert calls[0].get_header("X-app-token") == "synthetic-backend"
    assert "/v1/analysis/sheet-rows?" in calls[0].full_url


def test_v104_http_401_is_named_and_a_run_without_any_quote_exits_3(monkeypatch, capsys):
    _clear_tokens(monkeypatch)
    monkeypatch.setenv("BACKEND_TOKEN", "synthetic-secret-value")
    def refuse(req, **kwargs):
        raise urllib.error.HTTPError(req.full_url, 401, "Unauthorized", {}, io.BytesIO(b""))
    monkeypatch.setattr(urllib.request, "urlopen", refuse)
    quotes, failure, ages = iqr.fetch_quotes_ex("https://synthetic.invalid", "Market_Leaders", ["SYNTH.SR"])
    assert (quotes, failure, ages) == ({}, "HTTPError 401", {})
    book = Book()
    monkeypatch.setattr(iqr, "_open_sheet", lambda *args: book)
    for mode in ("--scan", "--apply"):
        monkeypatch.setattr(sys, "argv", ["intraday_quote_refresh", mode, "--backend", "https://synthetic.invalid"])
        assert iqr.main() == 3, mode
    out = capsys.readouterr().out
    assert "[FETCH-FAIL] Market_Leaders: HTTPError 401" in out and "FETCH FAILED on every target page" in out
    assert "synthetic-secret-value" not in out
    assert book.pages["_Run_Log"].appends[-1][1] == "ERROR" and book.pages["_Run_Log"].appends[-1][4] == "FETCH_FAILED"
    assert not book.pages["Market_Leaders"].writes and not book.pages["Global_Markets"].writes


def test_v104_one_page_failing_is_not_a_run_failure(monkeypatch):
    _clear_tokens(monkeypatch)
    payload = {"row_objects": [source()]}
    def half(req, **kwargs):
        if "page=Global_Markets" in req.full_url:
            raise urllib.error.HTTPError(req.full_url, 503, "Unavailable", {}, io.BytesIO(b""))
        return Response(payload)
    monkeypatch.setattr(urllib.request, "urlopen", half)
    monkeypatch.setattr(iqr, "_open_sheet", lambda *args: Book())
    monkeypatch.setattr(sys, "argv", ["intraday_quote_refresh", "--scan", "--backend", "https://synthetic.invalid"])
    assert iqr.main() == 0


def test_v104_harvest_drops_block_labels_and_keeps_every_supported_identifier():
    # Codex review on #916: bare symbols (core/symbols/normalize.py) and long
    # EODHD suffixes (SAP.XETRA) are supported identifiers and must be kept.
    page = [["Symbol", "Name"], ["CRC.US", "x"], ["MISSING_FX", "label"], ["CASH_FLOOR", "label"],
            ["PRICE_XCHECK", "label"], ["RISK/REWARD", "label"], ["1030.SR", "x"], ["AAPL", "x"], ["QQQ", "x"],
            ["SAP.XETRA", "x"], ["LIVEPOLC-1.MX", "x"], ["GRT-UN.TO", "x"], ["010130.KS", "x"], ["BRK-B.US", "x"],
            ["0883.HK", "x"]]
    assert iqr.harvest_symbols({"Top_10_Investments": page}) == [
        "CRC.US", "1030.SR", "AAPL", "QQQ", "SAP.XETRA", "LIVEPOLC-1.MX", "GRT-UN.TO", "010130.KS", "BRK-B.US",
        "0883.HK"]
    # a one-word label cannot be told from a ticker; it is sent and never matches
    assert iqr.harvest_symbols({"Top_10_Investments": [["Symbol", "Name"], ["TYPE", "label"]]}) == ["TYPE"]


def test_v104_age_report_measures_every_returned_quote_fresh_or_not(monkeypatch):
    rows = [source("AAA.US", acquisition_quote_asof=(NOW - timedelta(minutes=17)).isoformat(),
                   acquisition_acquired_at=(NOW - timedelta(minutes=1)).isoformat(), currency="USD"),
            source("BBB.US", acquisition_quote_asof=(NOW - timedelta(minutes=19)).isoformat(),
                   acquisition_acquired_at=(NOW - timedelta(minutes=1)).isoformat(), currency="USD"),
            source("CCC.US", acquisition_quote_asof=(NOW - timedelta(minutes=4)).isoformat(),
                   acquisition_acquired_at=(NOW - timedelta(minutes=1)).isoformat(), currency="USD"),
            source("SYNTH.SR"),
            source("NOTASKED.US"),
            {"symbol": "DDD.US", "current_price": 5}]                     # no proof: not measured
    rep = iqr.quote_age_report(rows, ["AAA.US", "BBB.US", "CCC.US", "SYNTH.SR", "DDD.US"], NOW)
    assert set(rep) == {"US", "other"}
    us = rep["US"]
    assert us["n"] == 3 and us["within"] == 1 and us["limit"] == 15.0
    assert abs(us["age_median"] - 17.0) < 1e-6 and abs(us["age_max"] - 19.0) < 1e-6
    assert abs(us["delay_median"] - 16.0) < 1e-6                         # acquired 1 min ago, quote 17 min old
    assert rep["other"]["n"] == 1 and rep["other"]["within"] == 1


def test_v104_scan_prints_age_lines(monkeypatch, capsys):
    code, book, calls = main_wire(monkeypatch, apply=False)
    out = capsys.readouterr().out
    assert code == 0 and "[AGE] Market_Leaders   other n=1" in out and "within_15min=1" in out


def test_v104_workflow_maps_the_backend_token_into_the_refresh_step_only():
    w = yaml.load(open(iqr.Path(__file__).resolve().parents[1] / ".github/workflows/intraday_refresh.yml").read(),
                  Loader=yaml.BaseLoader)
    steps = [st for job in w["jobs"].values() for st in job.get("steps", [])]
    holders = [st.get("name") for st in steps if "BACKEND_TOKEN" in (st.get("env") or {})]
    assert holders == ["Refresh decision-symbol quotes"]
    refresh = next(st for st in steps if st.get("name") == "Refresh decision-symbol quotes")
    assert refresh["env"]["BACKEND_TOKEN"] == "${{ secrets.BACKEND_TOKEN }}"
    assert "BACKEND_TOKEN" not in (w.get("env") or {})
    assert all("BACKEND_TOKEN" not in (job.get("env") or {}) for job in w["jobs"].values())


# ------------------------------------------------------------ v1.0.5 -------
# 15 Nov 2026 (run 43724490164): 400 harvested symbols went to each target page in one GET; the backend passed
# its own 25 s limit, the client gave up after 45 s on both pages, and no quote had been read since 8 Nov.
import urllib.parse


class Wide(Book):
    """Holdings H1; board names A1..A5 and the label TYPE; three of them on each target page."""
    def __init__(self):
        super().__init__()
        self.pages["My_Portfolio"] = Worksheet([["Symbol", "Position Qty"], ["H1.SR", 10]])
        self.pages["Top_10_Investments"] = Worksheet([["Symbol", "Name"]] + [["A%d.SR" % i, "n"] for i in range(1, 6)] + [["TYPE", "label"]])
        self.pages["Shadow_Board"] = Worksheet([["Symbol", "Name"], ["A1.SR", "n"]])
        self.pages["Market_Leaders"] = Worksheet([HEADER] + [target(s) for s in ("A2.SR", "H1.SR", "A1.SR", "OTHER.SR")])
        self.pages["Global_Markets"] = Worksheet([HEADER] + [target(s) for s in ("A3.SR", "A4.SR", "A5.SR")])


def _wire(monkeypatch, book, fail=(), mode="--apply"):
    """A backend that answers each request for exactly the symbols it names; `fail` = symbols whose batch answers 503."""
    calls = []
    monkeypatch.delenv("TFB_IQR_SYMBOL_PAGES", raising=False)        # the 1.0.5 default: holdings first
    def get(req, **kwargs):
        q = urllib.parse.parse_qs(urllib.parse.urlparse(req.full_url).query)
        syms = q["symbols"][0].split(",")
        calls.append((q["page"][0], syms, q["limit"][0], kwargs.get("timeout")))
        if set(syms) & set(fail):
            raise urllib.error.HTTPError(req.full_url, 503, "Unavailable", {}, io.BytesIO(b""))
        return Response({"row_objects": [source(s) for s in syms]})
    monkeypatch.setattr(urllib.request, "urlopen", get)
    monkeypatch.setattr(iqr, "_open_sheet", lambda *args: book)
    monkeypatch.setattr(sys, "argv", ["intraday_quote_refresh", mode, "--backend", "https://synthetic.invalid"])
    return calls


def _written(book, page):
    return sorted({book.pages[page].values[int(re.fullmatch(r"[A-Z]+([0-9]+)", u["range"])[1]) - 1][0]
                   for w in book.pages[page].writes for u in w})


def test_v105_a_page_is_asked_only_for_its_own_symbols_in_batches_holdings_first(monkeypatch, capsys):
    monkeypatch.setenv("TFB_IQR_BATCH", "2")
    book = Wide()
    calls = _wire(monkeypatch, book)
    assert iqr.main() == 0
    assert [(c[0], c[1], c[2]) for c in calls] == [
        ("Market_Leaders", ["H1.SR", "A1.SR"], "2"), ("Market_Leaders", ["A2.SR"], "1"),
        ("Global_Markets", ["A3.SR", "A4.SR"], "2"), ("Global_Markets", ["A5.SR"], "1")]
    assert all(c[3] == 45 for c in calls)
    assert not any("TYPE" in c[1] or "OTHER.SR" in c[1] for c in calls)          # a label is not a row; a row nobody decides on is not asked
    assert _written(book, "Market_Leaders") == ["A1.SR", "A2.SR", "H1.SR"] and _written(book, "Global_Markets") == ["A3.SR", "A4.SR", "A5.SR"]
    out = capsys.readouterr().out
    assert "[FETCH] Market_Leaders   wanted=3 batches=2 failed=0 skipped=0 returned=3 usable=3 no_evidence=0 conflicting=0" in out
    assert "[FETCH] Global_Markets   wanted=3 batches=2 failed=0 skipped=0 returned=3 usable=3" in out
    assert "[AGE] Market_Leaders   other n=3" in out and "decision_symbols=7" in out
    assert book.pages["_Run_Log"].appends[-1][4] == "OK"
    assert iqr.SCRIPT_VERSION == "1.0.5"


def test_v105_one_failed_batch_does_not_lose_the_others(monkeypatch, capsys):
    monkeypatch.setenv("TFB_IQR_BATCH", "1")
    book = Wide()
    calls = _wire(monkeypatch, book, fail=("A1.SR",))
    assert iqr.main() == 0
    assert len(calls) == 6 and _written(book, "Market_Leaders") == ["A2.SR", "H1.SR"]
    assert _written(book, "Global_Markets") == ["A3.SR", "A4.SR", "A5.SR"]
    out = capsys.readouterr().out
    assert "[FETCH] Market_Leaders   wanted=3 batches=3 failed=1 skipped=0 returned=2 usable=2" in out
    assert out.count("[FETCH-FAIL] Market_Leaders: HTTPError 503") == 1 and "FETCH FAILED on every target page" not in out
    log = book.pages["_Run_Log"].appends[-1]
    assert log[1] == "INFO" and log[4] == "FETCH_PARTIAL"


def test_v105_a_page_whose_every_batch_failed_is_a_page_failure_and_all_pages_is_exit_3(monkeypatch, capsys):
    monkeypatch.setenv("TFB_IQR_BATCH", "2")
    book = Wide()
    _wire(monkeypatch, book, fail=("H1.SR", "A2.SR"))                # both Market_Leaders batches
    assert iqr.main() == 0 and _written(book, "Market_Leaders") == [] and len(_written(book, "Global_Markets")) == 3
    assert book.pages["_Run_Log"].appends[-1][4] == "FETCH_PARTIAL"
    capsys.readouterr()
    book = Wide()
    _wire(monkeypatch, book, fail=("H1.SR", "A2.SR", "A3.SR", "A5.SR"))
    assert iqr.main() == 3
    out = capsys.readouterr().out
    assert "FETCH FAILED on every target page (Market_Leaders HTTPError 503; Global_Markets HTTPError 503)" in out
    assert out.count("[FETCH-FAIL]") == 4 and book.pages["_Run_Log"].appends[-1][4] == "FETCH_FAILED"


def test_v105_a_failed_batch_on_every_page_is_not_a_run_failure_and_a_skipped_page_is_not_an_answer(monkeypatch, capsys):
    monkeypatch.setenv("TFB_IQR_BATCH", "1")
    book = Wide()
    _wire(monkeypatch, book, fail=("A1.SR", "A3.SR"))                # one batch of each page fails; quotes were read on both
    assert iqr.main() == 0 and book.pages["_Run_Log"].appends[-1][4] == "FETCH_PARTIAL"
    assert _written(book, "Market_Leaders") == ["A2.SR", "H1.SR"] and _written(book, "Global_Markets") == ["A4.SR", "A5.SR"]
    # the only page that was asked failed, and the other was never reached: no quote was read, so the run is red
    monkeypatch.setenv("TFB_IQR_BATCH", "2")
    monkeypatch.setenv("TFB_IQR_BUDGET_SEC", "60")
    ticks = iter([0.0, 1.0, 2.0])
    monkeypatch.setattr(iqr, "_mono", lambda: next(ticks, 999.0))
    book = Wide()
    calls = _wire(monkeypatch, book, fail=("H1.SR", "A2.SR"))
    assert iqr.main() == 3 and {c[0] for c in calls} == {"Market_Leaders"}
    assert book.pages["_Run_Log"].appends[-1][4] == "FETCH_FAILED"
    assert "FETCH FAILED on every target page (Market_Leaders HTTPError 503)" in capsys.readouterr().out


def test_v105_a_run_that_could_send_no_batch_at_all_is_a_failed_run(monkeypatch, capsys):
    """Codex P2, #999: the budget gone before the first batch is not a partial success."""
    monkeypatch.setenv("TFB_IQR_BUDGET_SEC", "60")
    for mode in ("--scan", "--apply"):
        ticks = iter([0.0])                                          # the start; every later reading is past the limit
        monkeypatch.setattr(iqr, "_mono", lambda: next(ticks, 999.0))
        book = Wide()
        calls = _wire(monkeypatch, book, mode=mode)
        assert iqr.main() == 3 and calls == [], mode
        out = capsys.readouterr().out
        assert "NO REQUEST WAS SENT: the 60 s request budget was used up before the first batch" in out
        assert "FETCH FAILED on every target page" not in out and out.count("[BUDGET]") == 2
        if mode == "--apply":
            assert book.pages["_Run_Log"].appends[-1][1] == "ERROR" and book.pages["_Run_Log"].appends[-1][4] == "FETCH_FAILED"
    # no decision symbol on any page is not this case: nothing was wanted, nothing failed
    monkeypatch.setattr(iqr, "_mono", lambda: 999.0)
    quiet = Wide()
    quiet.pages["Market_Leaders"] = Worksheet([HEADER, target("ELSE.SR")])
    quiet.pages["Global_Markets"] = Worksheet([HEADER, target("ELSE.SR")])
    calls = _wire(monkeypatch, quiet, mode="--apply")
    assert iqr.main() == 0 and calls == [] and quiet.pages["_Run_Log"].appends[-1][4] == "OK"


def test_v105_no_request_starts_after_the_budget(monkeypatch, capsys):
    monkeypatch.setenv("TFB_IQR_BATCH", "1")
    monkeypatch.setenv("TFB_IQR_BUDGET_SEC", "60")
    ticks = iter([0.0, 10.0, 59.9, 60.0])                            # the start, then one reading before each batch
    monkeypatch.setattr(iqr, "_mono", lambda: next(ticks, 999.0))
    book = Wide()
    calls = _wire(monkeypatch, book)
    assert iqr.main() == 0
    assert [(c[0], c[1]) for c in calls] == [("Market_Leaders", ["H1.SR"]), ("Market_Leaders", ["A1.SR"])]   # 60.0 is the limit itself
    out = capsys.readouterr().out
    assert "[FETCH] Market_Leaders   wanted=3 batches=2 failed=0 skipped=1" in out
    assert "[BUDGET] Market_Leaders: 1 batch(es) not sent - the 60 s request budget was used up" in out
    assert "[FETCH] Global_Markets   wanted=3 batches=0 failed=0 skipped=3" in out and "[BUDGET] Global_Markets: 3 batch(es)" in out
    assert _written(book, "Market_Leaders") == ["A1.SR", "H1.SR"] and _written(book, "Global_Markets") == []
    assert book.pages["_Run_Log"].appends[-1][4] == "FETCH_PARTIAL" and "FETCH FAILED on every" not in out


def test_v105_a_page_without_a_decision_symbol_is_not_asked_and_a_page_without_a_symbol_column_is_not_filtered(monkeypatch, capsys):
    book = Wide()
    book.pages["Global_Markets"] = Worksheet([HEADER, target("ELSE.SR")])
    calls = _wire(monkeypatch, book, mode="--scan")
    assert iqr.main() == 0
    assert {c[0] for c in calls} == {"Market_Leaders"}
    assert "Global_Markets   rows=1      no decision symbol has a row on this page - nothing asked" in capsys.readouterr().out
    assert iqr.page_symbols([HEADER, target("a1.sr"), target("B2.SR"), [""]]) == {"A1.SR", "B2.SR"}
    assert iqr.page_symbols([]) is None and iqr.page_symbols([["Name", "Price"], ["x", 1]]) is None
    odd = Wide()
    odd.pages["Global_Markets"] = Worksheet([["Name", "Price"], ["x", 1]])       # no symbol column: nothing is known
    calls = _wire(monkeypatch, odd, mode="--scan")
    assert iqr.main() == 0
    assert [c[1] for c in calls if c[0] == "Global_Markets"] == [["H1.SR", "A1.SR", "A2.SR", "A3.SR", "A4.SR", "A5.SR", "TYPE"]]


def test_v105_batch_counts_reasons_and_ages_add_up_exactly(monkeypatch):
    rows = {"AAA.US": [source("AAA.US", currency="USD")],
            "BBB.US": [{"symbol": "BBB.US", "current_price": 5}],                                   # no source witness
            "CCC.US": [source("CCC.US", price=1, currency="USD"), source("CCC.US", price=2, currency="USD")],   # disagree
            "DDD.US": [source("DDD.US", currency="USD", acquisition_quote_asof=(NOW - timedelta(minutes=40)).isoformat())],
            "EEE.SR": [source("EEE.SR")]}
    served = []
    def one(base, page, symbols, timeout=45):
        served.append(list(symbols))
        return [r for s in symbols for r in rows.get(s, [])], None, NOW
    monkeypatch.setattr(iqr, "fetch_rows", one)
    order = ["AAA.US", "BBB.US", "CCC.US", "DDD.US", "EEE.SR", "GONE.US"]
    q, failure, ages, info = iqr.fetch_page_quotes("https://synthetic.invalid", "Global_Markets", order, batch=2)
    assert served == [["AAA.US", "BBB.US"], ["CCC.US", "DDD.US"], ["EEE.SR", "GONE.US"]] and failure is None
    assert info == {"wanted": 6, "batches": 3, "failed": 0, "skipped": 0, "returned": 5, "usable": len(q),
                    "no_evidence": 5 - len(q) - 1, "conflicting": 1}
    assert "AAA.US" in q and "EEE.SR" in q and "BBB.US" not in q and "CCC.US" not in q
    whole = iqr.quote_age_report([r for s in order for r in rows.get(s, [])], order, NOW)
    assert ages == whole and ages["US"]["n"] >= 2 and ages["US"]["age_max"] == 40.0
    one_call = iqr.fetch_page_quotes("https://synthetic.invalid", "Global_Markets", order, batch=200)
    assert one_call[0] == q and one_call[2] == ages and one_call[3]["batches"] == 1
    assert iqr.fetch_page_quotes("https://synthetic.invalid", "Global_Markets", [], batch=2)[3]["batches"] == 0
    assert iqr.quotes_from_rows([], ["AAA.US"], NOW) == ({}, {"returned": 0, "usable": 0, "conflicting": 0, "no_evidence": 0})


def test_v105_knobs_are_clamped_and_the_workflow_maps_them(monkeypatch):
    book = Wide()
    for value, sizes in (("0", [1, 1, 1]), ("999", [3]), ("junk", [3]), ("2", [2, 1])):
        monkeypatch.setenv("TFB_IQR_BATCH", value)
        calls = _wire(monkeypatch, book, mode="--scan")
        assert iqr.main() == 0
        assert [len(c[1]) for c in calls if c[0] == "Market_Leaders"] == sizes, value
    monkeypatch.setenv("TFB_IQR_TIMEOUT_SEC", "7")                   # each request carries the configured timeout
    calls = _wire(monkeypatch, book, mode="--scan")
    assert iqr.main() == 0 and calls and all(c[3] == 7 for c in calls)
    w = yaml.load(open(iqr.Path(__file__).resolve().parents[1] / ".github/workflows/intraday_refresh.yml").read(),
                  Loader=yaml.BaseLoader)
    env = next(st for job in w["jobs"].values() for st in job.get("steps", [])
               if st.get("name") == "Refresh decision-symbol quotes")["env"]
    assert env["TFB_IQR_BATCH"] == "40" and env["TFB_IQR_BUDGET_SEC"] == "300" and env["TFB_IQR_TIMEOUT_SEC"] == "45"
    assert env["TFB_IQR_SYMBOL_PAGES"] == "My_Portfolio,Top_10_Investments,Shadow_Board"
    minutes = int(next(job for job in w["jobs"].values() if "steps" in job)["timeout-minutes"])
    assert int(env["TFB_IQR_BUDGET_SEC"]) + int(env["TFB_IQR_TIMEOUT_SEC"]) < minutes * 60        # the budget fits the job limit
