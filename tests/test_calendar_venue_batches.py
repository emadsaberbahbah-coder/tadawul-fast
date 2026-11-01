"""calendar_provider v1.3.2: venue batches (US first) and EODHD's exchange
codes for the venues it rejected on 27 Oct 2026 (TFB_CALENDAR_VENUE_BATCHES,
TFB_CALENDAR_SUFFIX_MAP; both default off).

REAL module; the only stub is the provider's HTTP seam (_get_json), which
answers like EODHD did on 27 Oct 2026: HTTP 404 for the WHOLE
/calendar/earnings request when one code in `symbols` is unknown. The
universe is the 327 symbols of Calendar_Events on 27 Oct. No network.

V1  both gates off: requests, results and log lines equal the untouched
    v1.3.1 module (git show e8337a16) on both paths; the replay of the live
    run (calendar_sync run 40498374271) reproduces its WARNING line exactly -
    99 splits, 200 requests, 33 rejected, 42 dropped at the cap, 27 of them
    US names (golden negative)
V2  venue batches: one venue per batch, US first; the same universe -> no
    code dropped, every US code answered, 91 requests, all 35 unknown codes
    named
V3  suffix map: the four translations; answers land on the ORIGINAL symbol
    (also under the untranslated code); every other code left alone
V4  both on (the workflow's setting): 59 requests, 15 codes rejected, the
    late-alphabet US reports present; a pessimistic EODHD (the translated
    codes unknown too): still nothing dropped, the line names them
V5  forward path the same; the per-symbol ex-dividend path untouched
V6  gate spellings, pure helpers, the workflow arms both gates
"""
from __future__ import annotations

import importlib.util
import logging
import subprocess
from datetime import date
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
BASE_REV = "e8337a16"   # main with calendar_provider v1.3.1 (#853)
TODAY = date(2026, 10, 27)
GATES = ("TFB_CALENDAR_SPLIT_404", "TFB_CALENDAR_VENUE_BATCHES", "TFB_CALENDAR_SUFFIX_MAP")

# Calendar_Events column A on 27 Oct 2026 (327 rows, sheet order).
UNIVERSE = """
LADR.US CBAN.US DDI.US AGO.US EFC.US 0883.HK OLP.US ZTO.US GRT-UN.TO AER.US AOMR.US IRS.US GTY.US
GLAD.US FRST.US AX.US HTGC.US WPC.US OPBK.US UMBF.US LIEN.US AVBH.US PNFP.US 5023.SR AED.BR WDP.BR
URW.PA 1972.HK CRC.US 1113.HK TGS.US CBK.US PHP.L SBMO.AS BNP.PA IVR.US ORC.US BHF.US TDY.US MBIN.US
CTO.US OCFC.US EPRT.US FBK.US SPFI.US PDLB.US FSBC.US ACTG.US NEWT.US PGC.US PNC.US CFBK.US NBIX.US
BBOX.L WTFC.US PFS.US COSO.US ATAT.US ITRN.US INDV.US CFG.US FCF.US PB.US SYBT.US NHI.US JMSB.US
CFFN.US CBSH.US RDN.US GOOGL.US NBBK.US INDB.US VICI.US GLPI.US KINS.US KRP.US UNTY.US AVBC.US SAFE.L
RNST.US BYG.L MGY.US SMFG.US HOPE.US NVDA.US 2222.SR FFBC.US 0101.HK VNOM.US DVN.US SMC.US GMG.AX
TDG.US GLNG.US HCI.US VINP.US ADAM.US IBOC.US SOF.BR AMG.US IFS.US TSM.US HTB.US LGND.US AGM.US
GASS.US NWG.US ANIP.US VEL.US DEC.US VTOL.US ECVT.US AVGO.US SAFE.US HNI.US CRAI.US ASURB.MX ELPC.US
GRF.MC FDR.MC BCIC.US YMM.US HHH.US KRN.DE OPRA.US PFLT.US TIMB.US PBH.US GCC.MX BFS.US COL.MC
SCATC.OL RNO.PA HEIA.AS CDRO.US SNY.US CXT.US TAK.US AZN.L 1299.HK IDXX.US CIM.US JOYY.US VEON.US
MCD.US HASI.US SCI.US NEE.US EXPO.US FINW.US AIG.US AEFES.IS SAWAD.BK BMA.BA MRL.MC TLKM.JK ASII.JK
WALMEX.MX ONGC.NS ROVI.MC VIV.US DTE.DE 2020.HK DLX.US AC.MX BBD.US KCHOL.IS HEI.DE 0001.HK 0257.HK
PUK.US SBIN.NS PAX.US SGHC.US HLN.L 0083.HK KOG.OL LSEG.L INDF.JK PRSU.US 0388.HK GBLB.BR BRT.US
FWONA.US MRBK.US NG.L BUD.US LONN.SW AKR.US MNR.US GRNT.US BKTI.US JPFA.JK RSVR.US AMSF.US OUT.US
AZN.US AIT.US SHG.US ARCO.US CHDN.US BDMS.BK GDOT.US BIP.US TEL.OL MSGS.US AROC.US WMS.US AMT.US
RBC.US NRIM.US ICE.US BIPC.US ACIC.US BCE.US GFL.US BCAL.US 1015.KL 5110.SR MTX.US NTPC.NS T82U.SI
GAIN.US FIBK.US 2503.T 5285.KL 0027.HK 3401.T 4503.T ELPE.AT UVV.US ADVANC.BK MCY.US ARR.US DRH.US
ABI.BR APLE.US 6960.T AKSO.OL PEB.US ADM.US CTPNV.AS 9301.T EIX.US 6326.T BMY.US 5201.T REPX.US
1605.T TRNO.US 8002.T RNR.US STNG.US VTS.US MATX.US TUPRS.IS CF.US FTV.US UTDI.DE JAKK.US NN.AS
PRX.AS YUMC.US IG.MI MRK.DE KRG.US ORBIA.MX CCO.US NOS.LS SON.LS CIE.MC 8309.T CHDRAUIB.MX 8053.T
LEG.DE VIB.VN GRI.L TV.US EXE.US PROX.BR CTG.VN AR.US GPOR.US BKV.US 9107.T ALM.MC GNW.US EDP.LS
ACEL.US PAG.L 2269.T TIT.MI PTTEP.BK DAC.US 0019.HK EN.PA ENIC.US REP.MC GNL.US LOCO.US SHEL.US
TTE.PA TX.US TIETO.HE GPRK.US SPNT.US TTE.US CLNX.MC BG.US CRI.US UHR.SW SHEL.L 033780.KS GLE.PA
EBMT.US HTO.AT CCNE.US OMV.VI TITC.AT AES.US HFD.L SGU.US RVSB.US VNM.VN RBI.VI OHI.US A5G.IR
""".split()

# The codes EODHD rejected in run 40498374271 (its WARNING line, in order).
LIVE_REJECTED = ("1015.KL, 1605.T, 2269.T, 2503.T, 3401.T, 4503.T, 5201.T, 5285.KL, 6326.T, 6960.T, "
                 "8002.T, 8053.T, 8309.T, 9107.T, 9301.T, AEFES.IS.US, AZN.L, BBOX.L, BYG.L, "
                 "EDP.LS.US, GRI.L, HFD.L, HLN.L, IG.MI, KCHOL.IS.US, LSEG.L, NG.L, NOS.LS.US, PAG.L, "
                 "PHP.L, SAFE.L, SHEL.L, SON.LS.US")
# + T82U.SI (the one further unknown code that reproduces the live counts)
# + TUPRS.IS.US (the same malformed form as AEFES.IS.US; dropped at the cap live)
UNKNOWN_V131 = frozenset(LIVE_REJECTED.split(", ")) | {"T82U.SI", "TUPRS.IS.US"}
TOKYO = frozenset(c for c in UNKNOWN_V131 if c.endswith(".T"))
UNKNOWN_MAPPED = TOKYO | {"T82U.SI", "IG.MI"}   # EODHD knows LSE / KLSE / IS / LS
LIVE_LINE = ("[calendar_provider vV] last-earnings: 404 batches split 99 time(s), 200 request(s); "
             "EODHD rejected 33 code(s): " + LIVE_REJECTED + "; 42 code(s) dropped at the request cap 200")

# Last reports inside [2026-10-06, 2026-10-26] (synthetic values; FRST is the
# 22 Oct miss). TSM / TDY / WTFC / VICI sit in the range v1.3.1 never reached.
PAST = {
    "FRST.US": [("2026-10-22", {"actual": 0.35, "estimate": 0.45})],
    "OPBK.US": [("2026-10-22", {"actual": 0.52, "estimate": 0.53})],
    "TSM.US": [("2026-10-16", {"actual": 2.92, "estimate": 2.62})],
    "TDY.US": [("2026-10-22", {"actual": 5.57, "estimate": 5.45})],
    "WTFC.US": [("2026-10-20", {"actual": 2.98, "estimate": 2.85})],
    "VICI.US": [("2026-10-08", {"actual": 0.60, "estimate": 0.60})],
    "AZN.LSE": [("2026-10-23", {"actual": 2.10, "estimate": 2.00})],
    "1015.KLSE": [("2026-10-15", {"actual": 0.20, "estimate": 0.25})],
    "EDP.LS": [("2026-10-21", {"actual": 0.10, "estimate": 0.10})],
    "WDP.BR": [("2026-10-14", {"actual": 0.40, "estimate": 0.38})],
}
FWD = {"FRST.US": [("2027-01-28", {})], "WPC.US": [("2026-11-30", {}), ("2026-10-28", {})],
       "AZN.LSE": [("2026-11-06", {})], "OPBK.US": [("2026-12-01", {})]}
LATE_US = ("TSM.US", "TDY.US", "WTFC.US", "VICI.US")


def _load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _base_provider():
    try:
        blob = subprocess.run(["git", "-C", str(ROOT), "show", BASE_REV + ":core/providers/calendar_provider.py"],
                              capture_output=True, timeout=60)
    except Exception:
        return None
    if blob.returncode != 0 or b'__version__ = "1.3.1"' not in blob.stdout:
        return None
    tmp = ROOT / ".pytest_cache_cal_venue_base_provider.py"
    tmp.write_bytes(blob.stdout)
    try:
        return _load(tmp, "calendar_provider_v131_base")
    finally:
        tmp.unlink()


def _http_error(status):
    req = httpx.Request("GET", "https://eodhd.com/api/calendar/earnings")
    return httpx.HTTPStatusError(f"Client error '{status}'", request=req,
                                 response=httpx.Response(status, request=req))


def _fake(calls, reports, unknown, answer_as=None):
    """EODHD-like seam: 404 for the whole request when any code is unknown;
    else the reports of the requested codes inside [from, to]. `answer_as`
    renames a code in the answer (an EODHD that echoes another spelling)."""
    async def fake_get_json(_client, path, params):
        if path.startswith("/div/"):
            calls.append((path,))
            return []
        syms = params["symbols"].split(",")
        calls.append((path, params["from"], params["to"], tuple(syms)))
        if any(s in unknown for s in syms):
            raise _http_error(404)
        out = []
        for s in syms:
            for d, extra in reports.get(s, []):
                if params["from"] <= d <= params["to"]:
                    out.append(dict({"code": (answer_as or {}).get(s, s), "report_date": d}, **extra))
        return {"earnings": out}
    return fake_get_json


def _prep(mod, monkeypatch, calls, reports, unknown, split="on", venue=None, suffix=None, answer_as=None):
    monkeypatch.setattr(mod, "is_enabled", lambda: True)
    monkeypatch.setattr(mod, "_today", lambda: TODAY)
    monkeypatch.setattr(mod, "_get_json", _fake(calls, reports, unknown, answer_as))
    for k in ("TFB_CALENDAR_DAYS_BACK", "TFB_CALENDAR_EARNINGS_CHUNK", "TFB_CALENDAR_SPLIT_MAX_CALLS",
              "TFB_CALENDAR_DAYS_AHEAD", "TFB_CALENDAR_DIV_DAYS_AHEAD", "TFB_CALENDAR_CONCURRENCY"):
        monkeypatch.delenv(k, raising=False)
    for k, v in zip(GATES, (split, venue, suffix)):
        if v is None:
            monkeypatch.delenv(k, raising=False)
        else:
            monkeypatch.setenv(k, v)


def _answered(calls, unknown):
    """Codes that were in a request EODHD answered (or asked alone)."""
    done = set()
    for c in calls:
        if len(c) == 4 and (not any(s in unknown for s in c[3]) or len(c[3]) == 1):
            done.update(c[3])
    return done


def _warnings(caplog, mod):
    return [r.getMessage().replace("v" + mod.__version__, "vV") for r in caplog.records
            if r.levelno == logging.WARNING and "calendar_provider" in r.getMessage()]


@pytest.fixture
def cp():
    import core.providers.calendar_provider as mod
    # 2026-11-01: v1.4.0 (Saudi past reports from Yahoo, gated off by default)
    # stacked on this change; the pin is a floor now
    assert tuple(int(x) for x in mod.__version__.split(".")) >= (1, 3, 2), mod.__version__
    return mod


def test_universe_shape():
    assert len(UNIVERSE) == 327 and len(set(UNIVERSE)) == 327
    assert sum(1 for s in UNIVERSE if s.endswith(".SR")) == 3
    assert sum(1 for s in UNIVERSE if s.endswith(".US")) == 207


# --------------------------------------------------------------------- V1
@pytest.mark.parametrize("venue,suffix", [(None, None), ("off", "off"), ("0", ""), ("junk", "onn")])
def test_v1_gates_off_is_v131_and_the_live_line(cp, monkeypatch, caplog, venue, suffix):
    base = _base_provider()
    if base is None:
        pytest.skip("git show %s unavailable (shallow clone)" % BASE_REV)
    results = {}
    for name, mod in (("new", cp), ("base", base)):
        calls = []
        _prep(mod, monkeypatch, calls, PAST, UNKNOWN_V131, "on", venue, suffix)
        caplog.clear()
        with caplog.at_level(logging.INFO):
            last = mod.fetch_last_earnings_sync(UNIVERSE)
        n_last = len(calls)
        _prep(mod, monkeypatch, calls, FWD, UNKNOWN_V131, "on", venue, suffix)
        with caplog.at_level(logging.INFO):
            fwd = mod._run_sync(mod.fetch_earnings_map(UNIVERSE[:40]))
        logs = [r.getMessage().replace("v" + mod.__version__, "vV") for r in caplog.records
                if "calendar_provider" in r.getMessage()]
        results[name] = (last, fwd, calls, logs, n_last)
    assert results["new"] == results["base"]
    last, fwd, calls, logs, n_last = results["new"]
    assert n_last == 200
    assert LIVE_LINE in logs, logs
    answered = _answered(calls[:n_last], UNKNOWN_V131)
    code_map, _ = cp._split_symbols(UNIVERSE)
    dropped = sorted(c for c in code_map if c not in answered)
    assert len(dropped) == 42 and dropped[0] == "T82U.SI" and dropped[-1] == "ZTO.US"
    us_dropped = [c for c in dropped if c.endswith(".US") and c.count(".") == 1]
    assert len(us_dropped) == 27 and set(LATE_US) <= set(us_dropped)
    assert not set(LATE_US) & set(last), "golden negative: v1.3.1 never reached them"
    assert last["FRST.US"]["eps_surprise_pct"] == -22.2


# --------------------------------------------------------------------- V2
def test_v2_venue_batches_us_first_nothing_dropped(cp, monkeypatch, caplog):
    calls = []
    _prep(cp, monkeypatch, calls, PAST, UNKNOWN_V131, "on", "on", None)
    with caplog.at_level(logging.WARNING):
        last = cp.fetch_last_earnings_sync(UNIVERSE)
    assert len(calls) == 91
    # every request holds one venue; the first five are the 207 US codes
    for c in calls:
        assert len({cp._venue_of(s) for s in c[3]}) == 1, c[3]
    assert [len(c[3]) for c in calls[:5]] == [50, 50, 50, 50, 7]
    assert all(cp._venue_of(s) == "US" for c in calls[:5] for s in c[3])
    code_map, _ = cp._split_symbols(UNIVERSE)
    assert set(code_map) <= _answered(calls, UNKNOWN_V131), "no code dropped"
    for s in LATE_US + ("FRST.US", "OPBK.US"):
        assert s in last, s
    msgs = _warnings(caplog, cp)
    assert len(msgs) == 1 and "404 batches split 29 time(s), 91 request(s); EODHD rejected 35 code(s): " in msgs[0]
    assert "dropped" not in msgs[0] and "sent as" not in msgs[0]
    for c in ("T82U.SI", "TUPRS.IS.US", "AZN.L", "1015.KL", "9301.T"):
        assert c in msgs[0]


# --------------------------------------------------------------------- V3
@pytest.mark.parametrize("code,want", [
    ("AZN.L", "AZN.LSE"), ("1015.KL", "1015.KLSE"), ("AEFES.IS.US", "AEFES.IS"), ("EDP.LS.US", "EDP.LS"),
    ("1605.T", "1605.T"), ("T82U.SI", "T82U.SI"), ("IG.MI", "IG.MI"), ("DTE.XETRA", "DTE.XETRA"),
    ("NTPC.NSE", "NTPC.NSE"), ("BRK.B.US", "BRK.B.US"), ("AAPL.US", "AAPL.US"), ("GRT-UN.TO", "GRT-UN.TO"),
    ("PHP.LSE", "PHP.LSE"), ("X.L.US", "X.L.US"), (".L", ".L"), ("L", "L"), ("A.IS.US.X", "A.IS.US.X")])
def test_v3_calendar_code(cp, code, want):
    assert cp._calendar_code(code) == want


def test_v3_earnings_codes_and_answers_land_on_the_original(cp, monkeypatch, caplog):
    code_map, _ = cp._split_symbols(UNIVERSE)
    monkeypatch.delenv("TFB_CALENDAR_SUFFIX_MAP", raising=False)
    same, alias, summary = cp._earnings_codes(code_map)
    assert same is code_map and alias == {} and summary == ""
    monkeypatch.setenv("TFB_CALENDAR_SUFFIX_MAP", "on")
    mapped, alias, summary = cp._earnings_codes(code_map)
    assert summary == "IS.US->IS x3, KL->KLSE x2, L->LSE x12, LS.US->LS x3"
    assert mapped["AZN.LSE"] == "AZN.L" and mapped["AEFES.IS"] == "AEFES.IS" and mapped["EDP.LS"] == "EDP.LS"
    assert "AZN.L" not in mapped and alias["AZN.L"] == "AZN.L" and len(alias) == 20
    assert len(mapped) == len(code_map)
    assert {k: v for k, v in mapped.items() if k in code_map} == {k: v for k, v in code_map.items()
                                                                if cp._calendar_code(k) == k}
    # suffix map alone (no venue batches): requests carry the EODHD codes
    calls = []
    _prep(cp, monkeypatch, calls, PAST, UNKNOWN_MAPPED, "on", None, "on")
    with caplog.at_level(logging.WARNING):
        last = cp.fetch_last_earnings_sync(UNIVERSE)
    sent = {s for c in calls for s in c[3]}
    assert "AZN.LSE" in sent and "AZN.L" not in sent and "AEFES.IS" in sent and "AEFES.IS.US" not in sent
    assert last["AZN.L"]["last_earnings_date"] == "2026-10-23"
    assert last["1015.KL"]["eps_surprise_pct"] == -20.0 and last["EDP.LS"]["last_earnings_date"] == "2026-10-21"
    # an EODHD that answers under the untranslated spelling still lands
    calls = []
    _prep(cp, monkeypatch, calls, PAST, UNKNOWN_MAPPED, "on", "on", "on", answer_as={"AZN.LSE": "AZN.L"})
    last = cp.fetch_last_earnings_sync(["AZN.L", "FRST.US"])
    assert last["AZN.L"]["last_earnings_date"] == "2026-10-23" and "FRST.US" in last


# --------------------------------------------------------------------- V4
def test_v4_both_on_the_workflow_setting(cp, monkeypatch, caplog):
    calls = []
    _prep(cp, monkeypatch, calls, PAST, UNKNOWN_MAPPED, "on", "on", "on")
    with caplog.at_level(logging.WARNING):
        last = cp.fetch_last_earnings_sync(UNIVERSE)
    assert len(calls) == 59
    assert set(last) == {"FRST.US", "OPBK.US", "TSM.US", "TDY.US", "WTFC.US", "VICI.US", "AZN.L", "1015.KL",
                         "EDP.LS", "WDP.BR"}
    assert last["FRST.US"] == {"last_earnings_date": "2026-10-22", "eps_surprise_pct": -22.2,
                               "eps_actual": 0.35, "eps_estimate": 0.45, "report_timing": None}
    msgs = _warnings(caplog, cp)
    assert msgs == ["[calendar_provider vV] last-earnings: 404 batches split 13 time(s), 59 request(s); "
                    "EODHD rejected 15 code(s): IG.MI, T82U.SI, 1605.T, 2269.T, 2503.T, 3401.T, 4503.T, "
                    "5201.T, 6326.T, 6960.T, 8002.T, 8053.T, 8309.T, 9107.T, 9301.T; sent as EODHD exchange "
                    "codes: IS.US->IS x3, KL->KLSE x2, L->LSE x12, LS.US->LS x3"], msgs
    # pessimistic EODHD: it does not know the translated codes either
    unknown = UNKNOWN_MAPPED | {cp._calendar_code(c) for c in UNKNOWN_V131}
    calls = []
    _prep(cp, monkeypatch, calls, PAST, unknown, "on", "on", "on")
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        last = cp.fetch_last_earnings_sync(UNIVERSE)
    assert len(calls) == 91 and set(LATE_US) <= set(last) and "AZN.L" not in last
    msgs = _warnings(caplog, cp)
    assert len(msgs) == 1 and "EODHD rejected 35 code(s): " in msgs[0] and "dropped" not in msgs[0]
    assert "AZN.LSE" in msgs[0] and "AEFES.IS," in msgs[0] and "1015.KLSE" in msgs[0]


# --------------------------------------------------------------------- V5
def test_v5_forward_path_and_exdiv_untouched(cp, monkeypatch, caplog):
    decision = ["FRST.US", "OPBK.US", "AZN.L", "1605.T", "AEFES.IS", "WPC.US", "2222.SR"]
    calls = []
    _prep(cp, monkeypatch, calls, FWD, UNKNOWN_MAPPED, "on", "on", "on")
    with caplog.at_level(logging.WARNING):
        out = cp._run_sync(cp.fetch_earnings_map(decision))
    # FRST's next report (28 Jan) is past the 45-day window; WPC keeps the earliest
    assert out == {"OPBK.US": "2026-12-01", "WPC.US": "2026-10-28", "AZN.L": "2026-11-06"}
    assert calls[0][3] == ("FRST.US", "OPBK.US", "WPC.US"), "US first"
    assert all("2222" not in ",".join(c[3]) for c in calls), "KSA never sent"
    assert calls[0][1:3] == ("2026-10-27", "2026-12-11")
    msg = _warnings(caplog, cp)[0]
    assert msg.startswith("[calendar_provider vV] earnings: 404 batches split 0 time(s)")
    assert "EODHD rejected 1 code(s): 1605.T" in msg and "L->LSE x1" in msg
    # ex-dividend: the same per-symbol requests with every gate on or off
    paths = {}
    for gates in ((None, None, None), ("on", "on", "on")):
        calls = []
        _prep(cp, monkeypatch, calls, {}, frozenset(), *gates)
        cp._run_sync(cp.fetch_next_exdiv_map(decision))
        paths[gates] = sorted(c[0] for c in calls)
    assert paths[(None, None, None)] == paths[("on", "on", "on")]
    assert "/div/AZN.L" in paths[("on", "on", "on")] and "/div/AEFES.IS.US" in paths[("on", "on", "on")]


# --------------------------------------------------------------------- V6
@pytest.mark.parametrize("raw,on", [("on", True), ("ON", True), (" on ", True), ("1", True), ("true", True),
                                    ("yes", True), ("off", False), ("0", False), ("", False), ("onn", False)])
def test_v6_gate_spellings(cp, monkeypatch, raw, on):
    monkeypatch.setenv("TFB_CALENDAR_VENUE_BATCHES", raw)
    monkeypatch.setenv("TFB_CALENDAR_SUFFIX_MAP", raw)
    assert cp._venue_batches_enabled() is on and cp._suffix_map_enabled() is on


def test_v6_pure_helpers_and_workflow(cp, monkeypatch):
    assert cp._venue_of("AAPL.US") == "US" and cp._venue_of("AZN.LSE") == "LSE"
    assert cp._venue_of("AEFES.IS.US") == "*IS.US" and cp._venue_of("BRK.B.US") == "*B.US"
    codes = sorted(["B.US", "A.LSE", "X.IS.US", "A.US", "C.T", "D.T", "E.US"])
    monkeypatch.delenv("TFB_CALENDAR_VENUE_BATCHES", raising=False)
    assert cp._earnings_batches(codes, 2) == [codes[0:2], codes[2:4], codes[4:6], codes[6:7]]
    monkeypatch.setenv("TFB_CALENDAR_VENUE_BATCHES", "on")
    assert cp._earnings_batches(codes, 2) == [["A.US", "B.US"], ["E.US"], ["A.LSE"], ["C.T", "D.T"],
                                              ["X.IS.US"]]
    assert cp._earnings_batches([], 50) == []
    wf = (ROOT / ".github" / "workflows" / "calendar_sync.yml").read_text(encoding="utf-8")
    env = wf[wf.index("- name: 📅 Run calendar sync"):].split("run: |")[0]
    for k in GATES:
        assert '%s: "on"' % k in env, k
