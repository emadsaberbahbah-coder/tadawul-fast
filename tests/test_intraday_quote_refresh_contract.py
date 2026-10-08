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
