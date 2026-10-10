"""Frozen production-shaped rows prove acquisition truth across all surfaces."""
from datetime import datetime, timezone
from unittest import mock
import asyncio
import copy

import pytest

from core import data_engine_v2 as engine
from core.data_validity import acquisition_census, coverage_validity, row_acquisition
from core.sheets.schema_registry import get_sheet_headers, get_sheet_keys
from scripts import run_dashboard_sync as sync
from scripts.audit_full_refresh_coverage import Rule, audit_grid
from scripts.audit_decision_surface_freshness import audit_surfaces, parse_status_grid

NOW = datetime(2026, 10, 7, 9, tzinfo=timezone.utc)
HEADERS = get_sheet_headers("Commodities_FX")


def quote(symbol="GC=F", **changes):
    row = {"Symbol":symbol,"Name":"Gold futures","Current Price":2600,
           "Data Provider":"yahoo_chart","Last Updated (UTC)":"2026-10-07T08:59:00Z",
           "Last Updated (Riyadh)":"2026-10-07T11:59:00+03:00","Warnings":""}
    row.update(changes)
    return row


def matrix(rows):
    return [[row.get(header, "") for header in HEADERS] for row in rows]


@pytest.mark.parametrize("changes,status",[
    ({"Warnings":"fetch_failed:HTTP 402"},"INVALID"),
    ({"Warnings":"empty_row_no_provider_data"},"INVALID"),
    ({"Warnings":"identity_quarantined:kept_last_good"},"INVALID"),
    ({"Warnings":"persist_sanity_quarantined"},"INVALID"),
    ({"Warnings":"xprovider_price_conflict"},"INVALID"),
    ({"Warnings":"price_unverified_live:snapshot"},"INVALID"),
    ({"Data Provider":"snapshot:yahoo_chart"},"INVALID"),
    ({"Data Provider":"history_or_fallback"},"INVALID"),
    ({"Data Provider":"fallback_error"},"INVALID"),
    ({"Data Provider":"eodhd"},"INVALID"),
    ({"error":"fetch_failed:timeout"},"INVALID"),
    ({"Data Provider":""},"UNKNOWN"),
    ({"Current Price":0},"INVALID"),
    ({"Current Price":float("inf")},"INVALID"),
    ({"Current Price":float("nan")},"INVALID"),
    ({"Last Updated (UTC)":"2026-10-01T08:59:00Z"},"INVALID"),
    ({"Last Updated (UTC)":"2036-10-07T08:59:00Z"},"INVALID"),
    ({"Last Updated (UTC)":"2026-10-07"},"UNKNOWN"),
    ({"Last Updated (UTC)":"garbage"},"UNKNOWN"),
    ({"Last Updated (UTC)":"","Last Updated (Riyadh)":""},"UNKNOWN"),
    ({"Symbol":"COPPER FUTURES"},"INVALID"),
])
def test_recent_publication_never_hides_failure(changes,status):
    assert row_acquisition(quote(**changes),NOW,30*3600).status==status



def projected_fund_quarantine(mode, *, typed=True, failure=""):
    """Exercise the real margin sentry, acquisition producer and 115-key boundary.

    A 10x margin disagreement falls outside the 100x repair band. The quote
    inputs are frozen provider-shaped evidence, not a new live acquisition.
    """
    row = {"symbol":"BHF.US", "name":"Brighthouse Financial", "current_price":50.02,
           "data_provider":"eodhd", "last_updated_utc":"2026-10-07T08:59:00Z",
           "last_updated_riyadh":"2026-10-07T11:59:00+03:00", "pe_ttm":10.0,
           "market_cap":1e9, "revenue_ttm":1e9, "profit_margin":1.0,
           "warnings":"", "recommendation":"HOLD",
           "recommendation_reason":"Await decision inputs"}
    before = copy.deepcopy(row)
    tag = engine._fund_coherence_sentry(row, mode)
    after_sentry = copy.deepcopy(row)
    engine._aq_append_warning(row, tag)
    if failure == "price_missing":
        row["current_price"] = None
    elif failure == "provider_unavailable":
        row["data_provider"] = "fallback_error"
    elif failure == "stale_acquisition":
        row["last_updated_utc"] = "2026-10-01T08:59:00Z"
        row["last_updated_riyadh"] = "2026-10-01T11:59:00+03:00"
    elif failure == "future_acquisition":
        row["last_updated_utc"] = "2036-10-07T08:59:00Z"
        row["last_updated_riyadh"] = "2036-10-07T11:59:00+03:00"
    elif failure and failure != "preserved":
        engine._aq_append_warning(row, failure)
    if typed:
        engine._publish_price_acquisition(row, live_priced=failure != "preserved",
            fallback_source="snapshot" if failure == "preserved" else "",
            acquired_at=row["last_updated_utc"], provider=row["data_provider"],
            quote_asof="2026-10-07T08:58:00Z")
    headers = get_sheet_headers("Global_Markets")
    keys = get_sheet_keys("Global_Markets")
    projected = engine._strict_project_row(keys, row)
    display = engine._strict_project_row_display(headers, keys, projected)
    values = [display[header] for header in headers]
    return before, after_sentry, tag, headers, display, values


@pytest.mark.parametrize("fund_mode", ["observe", "enforce"])
@pytest.mark.parametrize("policy_mode", ["off", "observe", "enforce"])
@pytest.mark.parametrize("typed", [False, True])
def test_margin_sentry_keeps_price_acquisition_and_fundamentals_controls(
        fund_mode, policy_mode, typed, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", policy_mode)
    monkeypatch.setenv("TFB_SYNC_STATUS_TRUTH", "0")
    before, after, tag, headers, display, values = projected_fund_quarantine(fund_mode, typed=typed)
    assert tag == "fund_coherence_quarantined:profit_margin" + (":observe" if fund_mode == "observe" else "")
    expected = dict(before, profit_margin=None) if fund_mode == "enforce" else before
    assert after == expected  # Actual sentry changes only the margin in enforce.
    assert len(headers) == len(values) == 115
    assert tag in display["Warnings"]
    # Source observe mode retains its quantity above; publication requires a
    # value-bound unit receipt and withholds this fixture's unproven margin.
    assert display["Profit Margin"] is None
    if fund_mode == "observe":
        assert "sheet_margin_unknown:profit_margin" in display["Warnings"]
    for key, header in (("current_price", "Current Price"), ("data_provider", "Data Provider"),
                        ("last_updated_utc", "Last Updated (UTC)"),
                        ("last_updated_riyadh", "Last Updated (Riyadh)")):
        assert display[header] == before[key]
    # Acquired price is independent of the existing recommendation/gate verdict.
    assert display["Investability Status"] == "BLOCKED"
    assert display["Final Action"] == "DO_NOT_INVEST"
    assert display["Block Reason"]
    snapshot = copy.deepcopy(display)
    verdict = row_acquisition(display, NOW, 30*3600)
    assert verdict.successful and display == snapshot
    assert (verdict.quote_asof is not None) == typed
    census = acquisition_census(headers, [values], now=NOW, max_age_seconds=30*3600, requested=["BHF.US"])
    assert census.successful == {"BHF.US"} and not census.invalid and not census.unknown
    result = sync.TaskResult(key="GLOBAL", sheet_name="Global_Markets", status="success",
        start_utc=NOW.isoformat(), symbols_requested=1, rows_written=1)
    result._stamp_meta.update(requested=1, pre_persist_rows=1, fresh_lineage_known=True,
        fetched_origin=1, noncurrent_fetched=0, ff_new=0)
    assert sync._record_acquisition_census(result, headers, [values], ["BHF.US"], now=NOW) == {"BHF.US"}
    assert sync._page_fresh_fetch_metrics(result) == (1, 1, 100.0)
    assert sync._uv_page_state(result) == ("OK", 100.0)
    stamp = sync._status_stamp_row("Global_Markets", result, len(headers))
    assert "acquired=1/1" in stamp[3] and "acquisition=COMPLETE" in stamp[3] and "data=COMPLETE" in stamp[3]
    parsed = parse_status_grid([["Page", "Last Updated", "Status", "Message", "Rows", "Columns"], stamp])
    assert parsed["Global_Markets"].acquired_fresh == 1
    assert parsed["Global_Markets"].acquisition == "COMPLETE"
    audit = audit_grid([headers, values], Rule("Global_Markets", 1, 30, 95, 0, 0), headers, NOW)
    assert audit.status == "PASS" and audit.fresh == audit.timestamp_fresh == 1
    assert display == snapshot  # Classification never promotes eligibility or erases the flag.


@pytest.mark.parametrize("fund_mode", ["observe", "enforce"])
@pytest.mark.parametrize("failure", ["fetch_failed:timeout", "identity_quarantined:bad_symbol",
    "persist_sanity_quarantined:v6.36.0", "price_unverified_live:snapshot", "xprovider_price_conflict",
    "preserved", "price_missing", "provider_unavailable", "stale_acquisition", "future_acquisition"])
def test_margin_quarantine_cannot_rescue_actual_price_or_provenance_failure(fund_mode, failure, monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH", "enforce")
    monkeypatch.setenv("TFB_SYNC_STATUS_TRUTH", "0")
    _before, _after, tag, headers, display, values = projected_fund_quarantine(fund_mode, failure=failure)
    assert tag in display["Warnings"]
    assert row_acquisition(display, NOW, 30*3600).status == "INVALID"
    census = acquisition_census(headers, [values], now=NOW, max_age_seconds=30*3600, requested=["BHF.US"])
    assert census.invalid == {"BHF.US"} and not census.successful
    result = sync.TaskResult(key="GLOBAL", sheet_name="Global_Markets", status="success",
        start_utc=NOW.isoformat(), symbols_requested=1, rows_written=1)
    result._stamp_meta.update(requested=1, pre_persist_rows=1, fresh_lineage_known=True,
        fetched_origin=1, noncurrent_fetched=0, ff_new=0)
    assert not sync._record_acquisition_census(result, headers, [values], ["BHF.US"], now=NOW)
    assert sync._page_fresh_fetch_metrics(result) == (0, 1, 0.0)
    stamp = sync._status_stamp_row("Global_Markets", result, len(headers))
    assert "acquired=0/1" in stamp[3] and "acquisition=PARTIAL" in stamp[3] and "data=PARTIAL" in stamp[3]
    audit = audit_grid([headers, values], Rule("Global_Markets", 1, 30, 95, 0, 0), headers, NOW)
    assert audit.status == "FAIL" and audit.fresh == 0


def test_trusted_fallback_and_optional_attempt_failure_count_as_acquired():
    row=quote(Warnings="provider_attempt_failed:eodhd:HTTP404; acquisition_status:success; "
        "acquisition_acquired_at:2026-10-07T08:59:00Z; acquisition_provider:yahoo_chart")
    verdict=row_acquisition(row,NOW,30*3600)
    assert verdict.successful and verdict.quote_asof is None
    # Established legacy retrieval evidence also counts, without fabricating quote time.
    assert row_acquisition(quote(),NOW,30*3600).successful


@pytest.mark.parametrize("reverse",[False,True])
@pytest.mark.parametrize("aliases",[
    [("Warnings","fetch_failed:HTTP402"),("warnings","")],
    [("Data Provider","fallback_error"),("data_provider","yahoo_chart")],
    [("Current Price",0),("current_price",2600)],
    [("Last Updated (UTC)","2026-10-01T08:59:00Z"),("last_updated_utc","2026-10-07T08:59:00Z")],
    [("acquisition_status","failed"),("Acquisition Status","")],
    [("Symbol","GC=F"),("symbol","FOREIGN.US")],
])
def test_alias_order_cannot_erase_invalid_evidence(aliases,reverse):
    row=quote()
    for key,_value in aliases:
        row.pop(key,None)
    row.update(dict(reversed(aliases) if reverse else aliases))
    assert not row_acquisition(row,NOW,30*3600).successful


def test_duplicate_rows_and_headers_count_once_and_invalid_dominates_unknown():
    rows=[quote("GC=F"),quote("GC=F"),quote("SI=F",**{"Data Provider":""}),
          quote("SI=F",Warnings="fetch_failed:timeout"),quote("FOREIGN.US")]
    census=acquisition_census(HEADERS,matrix(rows),now=NOW,max_age_seconds=30*3600,requested=["GC=F","SI=F","GC=F"])
    assert census.successful=={"GC=F"} and census.invalid=={"SI=F"} and not census.unknown
    assert census.requested=={"GC=F","SI=F"} and "FOREIGN.US" not in census.returned
    for headers,values in [(["Symbol","Symbol"],["FOREIGN.US","GC=F"]),
                           (["Symbol","Symbol"],["GC=F","FOREIGN.US"])]:
        headers += ["Current Price","Data Provider","Last Updated (UTC)"]
        values += [2600,"yahoo_chart","2026-10-07T08:59:00Z"]
        result=acquisition_census(headers,[values],now=NOW,max_age_seconds=30*3600,requested=["GC=F"])
        assert not result.successful and result.invalid=={"GC=F"}


@pytest.mark.parametrize("mode",["off","observe","enforce"])
def test_factual_acquisition_matches_status_full_audit_and_decision_audit(mode,monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH",mode)
    monkeypatch.setenv("TFB_SYNC_STATUS_TRUTH","0")
    rows=[quote("GC=F"),quote("SI=F",Warnings="fetch_failed:HTTP422"),
          quote("HG=F",**{"Data Provider":"snapshot:yahoo_chart"})]
    result=sync.TaskResult(key="COMMODITIES_FX",sheet_name="Commodities_FX",status="success",start_utc=NOW.isoformat(),symbols_requested=3,rows_written=3)
    result._stamp_meta.update(requested=3,pre_persist_rows=3,fresh_lineage_known=True,fetched_origin=3,
                              noncurrent_fetched=0,ff_new=1)
    origins=sync._record_acquisition_census(result,HEADERS,matrix(rows),["GC=F","SI=F","HG=F"],now=NOW)
    assert origins=={"GC=F"}
    assert sync._page_fresh_fetch_metrics(result)==(1,3,100/3)
    stamp=sync._status_stamp_row("Commodities_FX",result,len(HEADERS))
    assert "fresh=1" in stamp[3] and "acquired=1/3" in stamp[3] and "timestamp_fresh=3" in stamp[3]
    assert "acquisition=PARTIAL" in stamp[3] and "data=PARTIAL" in stamp[3]
    full=audit_grid([HEADERS]+matrix(rows),Rule("Commodities_FX",3,30,95,0,0),HEADERS,NOW)
    assert full.fresh==1 and full.unique==3 and full.timestamp_fresh==3 and full.status=="FAIL"
    # Configured policy retains its rollout. Factual evidence stays the same.
    state,cov=sync._uv_page_state(result)
    assert state==("STALE_COV" if mode=="enforce" else "OK")
    assert cov==(100/3 if mode=="enforce" else 100.0)
    status=[ ["Page","Last Updated","Status","Message","Rows","Columns"],
             [stamp[0],"2026-10-07T08:59:00Z",stamp[2],stamp[3],3,len(HEADERS)],
             ["My_Portfolio","2026-10-07T08:59:00Z","VALID","acquired=1/1 acquisition=COMPLETE data=COMPLETE",1,122]]
    surface=[["title"],["Status:","Last run 2026-10-07 12:00:00 | status: ok | Commodities_FX 3/3 (full universe)"]]
    decisions=audit_surfaces(status,surface,surface,now_utc=NOW,min_rows={"Commodities_FX":3})
    assert not decisions.executable
    assert "SOURCE_ACQUISITION_INCOMPLETE" in {finding.code for finding in decisions.findings}


def test_restoration_cannot_refresh_original_failure_or_cached_acquisition():
    result=sync.TaskResult(key="CFX",sheet_name="Commodities_FX",status="success",start_utc=NOW.isoformat())
    requested=["GC=F","SI=F","HG=F"]
    incoming=[quote("GC=F"),quote("SI=F",Warnings="fetch_failed:timeout")]
    origins=sync._record_acquisition_census(result,HEADERS,matrix(incoming),requested,now=NOW)
    restored=[quote("GC=F"),quote("SI=F"),quote("HG=F")]
    sync._record_acquisition_census(result,HEADERS,matrix(restored),requested,origins=origins,noncurrent={"SI=F","HG=F"},now=NOW)
    assert sync._page_fresh_fetch_metrics(result)==(1,3,100/3)
    cached=quote(Warnings="acquisition_status:success; acquisition_provider:yahoo_chart; acquisition_acquired_at:2026-10-01T08:59:00Z")
    assert not row_acquisition(cached,NOW,30*3600).successful


def test_warning_list_has_same_truth_as_projected_text_and_conflicting_tokens_fail():
    tokens=["acquisition_status:success","acquisition_provider:yahoo_chart","acquisition_acquired_at:2026-10-07T08:59:00Z"]
    assert row_acquisition(quote(Warnings=tokens),NOW,30*3600).successful
    assert not row_acquisition(quote(Warnings=tokens+["acquisition_status:failed"]),NOW,30*3600).successful


def test_status_duplicate_proofs_or_pages_stay_unknown():
    header=["Page","Last Updated","Status","Message","Rows","Columns"]
    row=["Commodities_FX","2026-10-07T08:59:00Z","SUCCESS","acquired=1/1 acquired=0/1 acquisition=COMPLETE data=COMPLETE",1,115]
    assert parse_status_grid([header,row])["Commodities_FX"].acquired_fresh is None
    row[3]="acquired=1/1 acquisition=COMPLETE data=COMPLETE"
    assert parse_status_grid([header,row,list(row)])["Commodities_FX"].acquisition=="UNKNOWN"


def test_enforce_certification_error_disclosed_without_promoting_observe(monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH","enforce")
    result=sync.TaskResult(key="CFX",sheet_name="Commodities_FX",status="success",start_utc=NOW.isoformat())
    sync._record_acquisition_census(result,HEADERS,matrix([quote()]),["GC=F"],now=NOW)
    with mock.patch.object(sync,"_fetchfail_truth_selftest",return_value="FAIL"):
        assert sync._fetchfail_truth_mode()=="error"
        assert sync._uv_page_state(result)[0]=="POLICY_ERROR"
        stamp=sync._status_stamp_row("Commodities_FX",result,len(HEADERS))
        assert "acquired=1/1" in stamp[3] and "fetchfail_requested=enforce" in stamp[3] and "fetchfail_effective=error" in stamp[3]
    with mock.patch.object(sync,"_fetchfail_truth_selftest",side_effect=RuntimeError("fixture")):
        assert sync._fetchfail_truth_mode()=="error"


def test_exact_floor_is_not_rounded_to_pass():
    assert not coverage_validity(10000,9496,95).valid
    assert coverage_validity(10000,9500,95).valid


@pytest.mark.parametrize("known",[False,True])
def test_enforce_cannot_authorize_unknown_new_run_evidence(known,monkeypatch):
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH","enforce")
    monkeypatch.setenv("TFB_SYNC_STATUS_TRUTH","0")
    result=sync.TaskResult(key="CFX",sheet_name="Commodities_FX",status="success",start_utc=NOW.isoformat(),symbols_requested=1)
    result._stamp_meta.update(requested=1,pre_persist_rows=1,acquisition_known=known,acquired_fresh=None)
    assert sync._uv_page_state(result)==("STALE_COV",None)
    assert sync._status_stamp_row("Commodities_FX",result,len(HEADERS))[2]=="PARTIAL_FRESH"


@pytest.mark.parametrize("persistence",[False,True])
def test_real_runner_captures_acquisition_before_persistence_or_credentials(persistence,monkeypatch):
    rows=matrix([quote("GC=F"),quote("SI=F",Warnings="fetch_failed:HTTP422")])
    class Backend:
        async def post_json(self,_endpoint,_payload):
            return {"headers":HEADERS,"rows_matrix":rows},None,200
    monkeypatch.setenv("TFB_MARKET_SYMBOL_READBACK","0")
    monkeypatch.setenv("TFB_SYNC_SYMBOL_BATCH_SIZE","0")
    monkeypatch.setenv("TFB_SYNC_FETCHFAIL_TRUTH","off")
    with mock.patch.object(sync,"_utc_now",return_value=NOW), mock.patch.object(sync,"_read_symbols",return_value=["GC=F","SI=F"]), \
         mock.patch.object(sync,"_symbol_persistence_enabled",return_value=persistence):
        result=asyncio.run(sync._run_one_task(sync.TaskSpec("COMMODITIES_FX","Commodities_FX","analysis"),"offline","A1",-1,False,False,Backend(),None))
    assert result.status=="partial"
    assert result._stamp_meta["fresh_lineage_known"]
    assert sync._page_fresh_fetch_metrics(result)==(1,2,50.0)


@pytest.mark.parametrize("mechanism",["klg","pv"])
def test_real_runner_published_restoration_agrees_with_row_audit(mechanism,monkeypatch):
    fresh=quote("GC=F", **{"Horizon Days":365,"Invest Period Label":"1Y"})
    prior=quote("SI=F",Warnings="prior_nonacquisition_note; acquisition_status:success; acquisition_provider:yahoo_chart; acquisition_acquired_at:2026-10-07T08:58:00Z")
    failed=quote("SI=F",**{"Current Price":"","Data Provider":"fallback_error","Warnings":"fetch_failed:timeout"})
    class Backend:
        async def post_json(self,_endpoint,_payload):
            incoming=[fresh,quote("HG=F"),quote("CL=F")]
            if mechanism=="klg":
                incoming.append(failed)
            return {"headers":HEADERS,"rows_matrix":matrix(incoming)},None,200
    class Writer:
        published=[]
        def _get_service(self):
            return object()
        def read_values(self,*_args,**_kwargs):
            return [HEADERS]+matrix([prior])
        def write_table(self,_sid,_page,_start,headers,rows):
            self.published=[list(row) for row in rows]
            return len(rows)
        def clear_from(self,*_args,**_kwargs):
            pass
    writer=Writer()
    for name,value in {"TFB_MARKET_SYMBOL_READBACK":"0","TFB_SYNC_SYMBOL_BATCH_SIZE":"0",
                       "TFB_SYNC_PERSISTENCE_HARD":"0","TFB_SYNC_ROW_ID_FIREWALL":"0",
                       "TFB_SYNC_NAME_DEDUP_MODE":"off","TFB_SYNC_OHLC_LAKE":"0",
                       "TFB_SYNC_FALSE_GREEN_SCREEN":"0","TFB_SYNC_STATUS_STAMP":"0",
                       "TFB_SYNC_FETCHFAIL_TRUTH":"off","TFB_SYNC_IDENTITY_TRIPWIRE":"0",
                       "TFB_SYNC_COHERENCE_TRIPWIRE":"0"}.items():
        monkeypatch.setenv(name,value)
    with mock.patch.object(sync,"_utc_now",return_value=NOW), mock.patch.object(sync,"_read_symbols",return_value=["GC=F","SI=F","HG=F","CL=F"]):
        result=asyncio.run(sync._run_one_task(sync.TaskSpec("COMMODITIES_FX","Commodities_FX","analysis"),"offline","A1",-1,False,False,Backend(),writer))
    assert result.status=="success" and writer.published
    assert sync._page_fresh_fetch_metrics(result)==(3,4,75.0)
    assert result._stamp_meta["klg_kept" if mechanism=="klg" else "persist_restored"]==1
    published={row[HEADERS.index("Symbol")]:dict(zip(HEADERS,row)) for row in writer.published}
    assert published["GC=F"]["Warnings"]==fresh["Warnings"]
    assert "acquisition_status:preserved" in published["SI=F"]["Warnings"]
    assert "acquisition_status:success" not in published["SI=F"]["Warnings"]
    assert "prior_nonacquisition_note" in published["SI=F"]["Warnings"]
    for column in ("Current Price","Data Provider","Last Updated (UTC)","Last Updated (Riyadh)"):
        assert published["SI=F"][column]==prior[column]
    full=audit_grid([HEADERS]+writer.published,Rule("Commodities_FX",4,30,95,0,0),HEADERS,NOW)
    assert full.fresh==3 and full.unique==4 and full.timestamp_fresh==4
