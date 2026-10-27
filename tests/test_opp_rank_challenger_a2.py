"""tests/test_opp_rank_challenger_a2.py - core/analysis/opportunity_builder.py
v1.28.0 [RANK-CHALLENGER] (program register A2 / loop piece L4).

REAL module, synthetic rows (the decision_evidence_fixtures observed-price and
observed-portfolio packets the other builder suites use). No network.

Env (optional):
  OB_BASE=<v1.27.2 opportunity_builder.py>  golden base when `git show
      53c8544e:core/analysis/opportunity_builder.py` is unavailable (shallow
      clone); without either, the golden legs print SKIP.

P1  surface: version >= 1.28.0; the six pre-registered constants;
    TFB_OPP_RANK_MODE parsing (unset / legacy / shadow / enforce / junk)
P2  GOLDEN: gate unset, "legacy" and an unknown value -> every payload
    (single pass, withheld single pass, research + allocate replay, empty
    pool) and every normalize_candidate dict is byte-identical to the
    untouched v1.27.2 base (version stamps normalised); zero
    [RANK-CHALLENGER] log lines
P3  the base ignores the gate (shadow on the base == legacy on the base), so
    every new key comes from the new code
P4  shadow is selection-neutral: tickets, candidates_rows (order + values),
    near_miss, alerts, kpis and meta equal legacy except the additive
    shadow_rank / meta.rank_challenger
P5  hand-checked order on a 7-row pool: challenger F B G A D E, champion
    A D F G B E, tiers 3/2/1, overlap 5, ticket shadow ranks 4 5 1 3 2
P6  cap rule: with >= 5 below-cap names the two highest-reliability cap
    names sit at ranks 6 and 7; with fewer, they fill behind them
P7  sector-relative strength: sector median when >= 3 values, else the pool
    median; the +/-5 deadband is exclusive
P8  key order inside one reliability band: strength, then revision, then
    the smaller engine ROI, then symbol
P9  floor: missing engine ROI and engine ROI < required_roi_pct sort last;
    the floor follows the criteria value
P10 _rank_revision_dir parsing table
P11 enforce is reserved: behaves as shadow, flags enforce_reserved
P12 allocate replay never ranks: research (shadow) carries the order, the
    allocate payload carries no rank key and funds what legacy funds
P13 a raising challenger never fails a build: legacy selection, an error
    block in meta, no shadow_rank anywhere
P14 exactly one [RANK-CHALLENGER] INFO line per armed build
P15 the gate is read per build (no reload)

Run under pytest in an isolated subprocess.
"""
from pathlib import Path
import subprocess
import sys


def main():
    import contextlib, copy, hashlib, importlib.util, io, json, logging, os, tempfile

    HERE = os.path.dirname(os.path.abspath(__file__))
    ROOT = os.path.abspath(os.path.join(HERE, ".."))
    for p in (ROOT, HERE):
        if p not in sys.path:
            sys.path.insert(0, p)
    BASE_REV = "53c8544e"   # main before v1.28.0: the untouched v1.27.2 builder
    ENV_KEYS = ("TFB_OPP_RANK_MODE",)

    from core.analysis import opportunity_builder as ob
    from decision_evidence_fixtures import (build_with_observed_inputs,
                                            observed_portfolio,
                                            observed_price_fields)

    assert tuple(map(int, ob.OPPORTUNITY_BUILDER_VERSION.split("."))) >= (1, 28, 0), \
        ob.OPPORTUNITY_BUILDER_VERSION
    out = []
    TMP = tempfile.mkdtemp(prefix="tfb_a2_")

    def T(name, cond, detail=""):
        out.append(("PASS " if cond else "FAIL ") + name
                   + (" | " + str(detail)[:300] if detail else ""))
        if not cond:
            raise AssertionError(name + " " + str(detail))

    # Ambient production flags must not leak into the synthetic fixtures.
    for key in list(os.environ):
        if key.startswith("TFB_OPP_") or key.startswith("TFB_T10_"):
            os.environ.pop(key, None)
    os.environ["TFB_OPP_ENABLED"] = "1"
    os.environ["TFB_OPP_FUNDING_PLAN"] = "1"
    os.environ["APP_TOKEN"] = "synthetic-a2-test"
    os.environ.pop("RENDER_GIT_COMMIT", None)

    @contextlib.contextmanager
    def _mode(value):
        saved = {k: os.environ.get(k) for k in ENV_KEYS}
        try:
            os.environ.pop("TFB_OPP_RANK_MODE", None)
            if value is not None:
                os.environ["TFB_OPP_RANK_MODE"] = value
            yield
        finally:
            for k, v in saved.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    class _Cap(logging.Handler):
        def __init__(self):
            super().__init__(level=logging.INFO)
            self.lines = []

        def emit(self, record):
            try:
                self.lines.append(record.getMessage())
            except Exception:
                pass

    def _rank_lines(fn):
        cap = _Cap()
        lg = logging.getLogger("core.analysis.opportunity_builder")
        old = lg.level
        lg.addHandler(cap)
        lg.setLevel(logging.INFO)
        try:
            res = fn()
        finally:
            lg.removeHandler(cap)
            lg.setLevel(old)
        return res, [l for l in cap.lines if "[RANK-CHALLENGER" in l]

    STAMP = {k: v for k, v in observed_price_fields().items()}

    def row(symbol, rel, eng, **kw):
        base = {"symbol": symbol, "name": "Synthetic " + symbol,
                "sector": "Banks", "market": "Tadawul", "currency": "SAR",
                "current_price": 100.0, "intrinsic_value": 130.0,
                "forecast_reliability_score": rel, "data_quality_score": 91.0,
                "risk_bucket": "Moderate", "provider_engine_conflict": "No",
                "volatility_30d": 4.0, "avg_volume_30d": 2500000,
                "expected_roi_12m": eng,
                "recommendation_detailed": "STRONG BUY",
                "investability_status": "INVESTABLE", "block_reason": ""}
        base.update(STAMP)
        base.update(kw)
        return base

    # F1: the hand-checked pool (C.SR fails a gate and only feeds the medians)
    F1 = [
        row("A.SR", 88, 35.0, intrinsic_value=140, momentum_score=60),
        row("B.SR", 86, 20.0, intrinsic_value=125, momentum_score=40),
        row("C.SR", 79, 15.0, sector="Energy", intrinsic_value=118, momentum_score=70),
        row("D.SR", 77, 34.5, sector="Energy", intrinsic_value=140, momentum_score=50),
        row("E.SR", 76, 10.0, sector="Materials", intrinsic_value=120, momentum_score=55),
        row("F.SR", 90, 28.0, sector="Materials", intrinsic_value=135, momentum_score=45),
        row("G.SR", 81, 22.0, sector="Utilities", intrinsic_value=128),
    ]
    CRIT1 = {"max_selected": 5}
    PORT1 = {"cash_available_sar": 500000}

    def build(mod, rows, crit=None, port=None, observed=True):
        rows = copy.deepcopy(rows)
        if observed:
            return build_with_observed_inputs(mod, rows, criteria=copy.deepcopy(crit),
                                              portfolio=copy.deepcopy(port or {}))
        return mod.build_opportunity_payload(rows, criteria=copy.deepcopy(crit),
                                             portfolio=copy.deepcopy(port or {}))

    def canon(payload, vers):
        p = json.loads(json.dumps(payload, sort_keys=True, default=str))
        txt = json.dumps(p, sort_keys=True)
        for v in vers:
            txt = txt.replace('"' + v + '"', '"<V>"')
        p = json.loads(txt)
        meta = p.get("meta") or {}
        meta.pop("generated_at_utc", None)
        bf = meta.get("board_funding") or {}
        for k in ("snapshot", "snapshot_id"):
            bf.pop(k, None)
        return p

    def strip_rank(payload):
        p = json.loads(json.dumps(payload, sort_keys=True, default=str))
        for t in p.get("selected") or []:
            t.pop("shadow_rank", None)
        for c in p.get("candidates_rows") or []:
            c.pop("shadow_rank", None)
        (p.get("meta") or {}).pop("rank_challenger", None)
        (p.get("meta") or {}).pop("generated_at_utc", None)
        return p

    def has_rank_keys(payload):
        return ("rank_challenger" in (payload.get("meta") or {})
                or any("shadow_rank" in t for t in payload.get("selected") or [])
                or any("shadow_rank" in c for c in payload.get("candidates_rows") or []))

    # ------------------------------------------------------------------ P1
    T("P1 version >= 1.28.0", True, ob.OPPORTUNITY_BUILDER_VERSION)
    T("P1 pre-registered constants",
      (ob.RANK_CHALLENGER_VERSION, ob.RANK_CAP_ENGINE_ROI_PCT, ob.RANK_CAP_FIRST_FREE,
       ob.RANK_REL_BAND_PTS, ob.RANK_STRENGTH_DEADBAND, ob.RANK_SECTOR_MIN_PEERS,
       ob.RANK_PUBLISH_TOP_N) == ("1.0", 34.0, 5, 5.0, 5.0, 3, 10))
    parsed = {}
    for v in (None, "", "legacy", "shadow", " SHADOW ", "enforce", "on", "1", "observe", "bogus"):
        with _mode(v):
            parsed[str(v)] = ob._env_rank_mode()
    T("P1 TFB_OPP_RANK_MODE parsing (unknown -> legacy)",
      parsed == {"None": "legacy", "": "legacy", "legacy": "legacy", "shadow": "shadow",
                 " SHADOW ": "shadow", "enforce": "enforce", "on": "legacy", "1": "legacy",
                 "observe": "legacy", "bogus": "legacy"}, parsed)

    # ------------------------------------------------------------------ base
    base_src = os.environ.get("OB_BASE", "")
    if not base_src:
        try:
            blob = subprocess.run(["git", "-C", ROOT, "show",
                                   BASE_REV + ":core/analysis/opportunity_builder.py"],
                                  capture_output=True, timeout=60)
            if blob.returncode == 0 and blob.stdout:
                base_src = os.path.join(TMP, "opportunity_builder_v1_27_2.py")
                with open(base_src, "wb") as fh:
                    fh.write(blob.stdout)
        except Exception:
            base_src = ""
    base = None
    if base_src:
        spec = importlib.util.spec_from_file_location("ob_base_v1_27_2", base_src)
        base = importlib.util.module_from_spec(spec)
        sys.modules["ob_base_v1_27_2"] = base
        spec.loader.exec_module(base)
    VERS = [ob.OPPORTUNITY_BUILDER_VERSION] + ([base.OPPORTUNITY_BUILDER_VERSION] if base else [])

    # Research + allocate helpers (the test_ob_board_funding pattern).
    BF_CRIT = {"max_selected": 2, "max_per_sector": 1, "max_per_market": 10,
               "max_weight_pct": 100.0, "pf_max_sector_pct": 100.0,
               "min_ticket_sar": 5000.0, "trust_gate_enabled": False}
    BF_PORT = observed_portfolio({"cash_available_sar": 10000.0}, {"SAR": 1.0})
    BF_ROWS = [row("R1.SR", 82, 24.0, sector="Energy", momentum_score=52),
               row("R2.SR", 84, 20.0, sector="Banks", momentum_score=61),
               row("R3.SR", 80, 30.0, sector="Materials", momentum_score=48)]

    def research_alloc(mod):
        res = mod.build_opportunity_payload(
            copy.deepcopy(BF_ROWS), criteria={**BF_CRIT, "board_funding_stage": "research"},
            portfolio=copy.deepcopy(BF_PORT), fx_rates={"SAR": 1.0})
        snap = copy.deepcopy(((res.get("meta") or {}).get("board_funding") or {}).get("snapshot"))
        if not snap:
            return res, None
        syms = [t["symbol"] for t in res.get("selected") or []]
        alloc = mod.build_opportunity_payload(
            copy.deepcopy(snap["rows"]),
            criteria={**BF_CRIT, "board_funding_stage": "allocate",
                      "board_funding_symbols": syms, "board_funding_snapshot": snap},
            portfolio=copy.deepcopy(BF_PORT), fx_rates={"SAR": 1.0})
        return res, alloc

    def scenarios(mod):
        res, alloc = research_alloc(mod)
        return {
            "single": build(mod, F1, CRIT1, PORT1),
            "withheld": build(mod, F1, CRIT1, {"cash_available_sar": 50000}, observed=False),
            "research": res,
            "allocate": alloc,
            "empty": build(mod, [], CRIT1, PORT1),
        }

    # ------------------------------------------------------------------ P2/P3
    if base is not None:
        with _mode(None):
            base_sc = scenarios(base)
        T("P2 fixtures exercise every path (withheld / research snapshot / allocate)",
          (base_sc["withheld"]["meta"].get("execution_ready") is False
           and base_sc["allocate"] is not None
           and (base_sc["allocate"]["meta"].get("board_funding") or {}).get("stage") == "allocate"
           and base_sc["single"]["selected"]),
          {k: (v or {}).get("status") for k, v in base_sc.items()})
        for label, value in (("unset", None), ("legacy", "legacy"), ("junk", "bogus")):
            with _mode(value):
                (new_sc, lines) = _rank_lines(lambda: scenarios(ob))
            same = {k: canon(new_sc[k], VERS) == canon(base_sc[k], VERS) for k in base_sc}
            T("P2 GOLDEN gate %s: every payload byte-identical to v1.27.2" % label,
              all(same.values()), same)
            T("P2 gate %s: zero [RANK-CHALLENGER] log lines, zero rank keys" % label,
              not lines and not any(has_rank_keys(v or {}) for v in new_sc.values()), lines)
        fx = {"SAR": 1.0}
        with _mode(None):
            c_new = [ob.normalize_candidate(copy.deepcopy(r), fx, ob.make_criteria(CRIT1)) for r in F1]
            c_old = [base.normalize_candidate(copy.deepcopy(r), fx, base.make_criteria(CRIT1)) for r in F1]
        strip = lambda c: json.loads(json.dumps({k: v for k, v in c.items() if k != "quote_evidence"},
                                                sort_keys=True, default=str))
        T("P2 GOLDEN normalize_candidate legacy == v1.27.2 (same keys, same values)",
          [strip(c) for c in c_new] == [strip(c) for c in c_old]
          and all(set(a) == set(b) for a, b in zip(c_new, c_old)))
        with _mode("shadow"):
            base_shadow = scenarios(base)
        T("P3 the base ignores the gate (shadow on v1.27.2 == legacy on v1.27.2)",
          all(canon(base_shadow[k], VERS) == canon(base_sc[k], VERS) for k in base_sc)
          and not any(has_rank_keys(v or {}) for v in base_shadow.values()))
    else:
        out.append("SKIP P2/P3 golden (git show %s unavailable; set OB_BASE=<v1.27.2 file>)" % BASE_REV)

    # ------------------------------------------------------------------ P4/P5
    with _mode(None):
        L = build(ob, F1, CRIT1, PORT1)
    with _mode("shadow"):
        (S, s_lines) = _rank_lines(lambda: build(ob, F1, CRIT1, PORT1))
    T("P4 shadow is selection-neutral (all surfaces == legacy once the additive keys go)",
      strip_rank(S) == strip_rank(L)
      and [t["symbol"] for t in S["selected"]] == [t["symbol"] for t in L["selected"]]
      and [c["symbol"] for c in S["candidates_rows"]] == [c["symbol"] for c in L["candidates_rows"]])
    rc = S["meta"].get("rank_challenger") or {}
    chal = [r["symbol"] for r in rc.get("challenger") or []]
    champ = [r["symbol"] for r in rc.get("champion") or []]
    T("P5 challenger order F B G A D E / champion A D F G B E",
      chal == ["F.SR", "B.SR", "G.SR", "A.SR", "D.SR", "E.SR"]
      and champ == ["A.SR", "D.SR", "F.SR", "G.SR", "B.SR", "E.SR"], (chal, champ))
    T("P5 tiers 3 below cap / 2 at cap / 1 below floor; overlap_top5 5; pool 6",
      rc.get("tiers") == {"below_cap": 3, "at_cap": 2, "below_floor": 1}
      and rc.get("overlap_top5") == 5 and rc.get("pool") == 6, rc.get("tiers"))
    T("P5 ticket shadow ranks follow the symbols (A4 D5 F1 G3 B2), live order kept",
      [(t["symbol"], t.get("shadow_rank")) for t in S["selected"]]
      == [("A.SR", 4), ("D.SR", 5), ("F.SR", 1), ("G.SR", 3), ("B.SR", 2)],
      [(t["symbol"], t.get("shadow_rank")) for t in S["selected"]])
    inv = {c["symbol"]: c.get("shadow_rank") for c in S["candidates_rows"]}
    T("P5 shadow_rank on every INVEST audit row, none on the rest",
      inv == {"F.SR": 1, "B.SR": 2, "G.SR": 3, "A.SR": 4, "D.SR": 5, "E.SR": 6, "C.SR": None}
      and all(("shadow_rank" in c) == (c["verdict"] == "INVEST") for c in S["candidates_rows"]), inv)
    f_row = (rc.get("challenger") or [{}])[0]
    T("P5 challenger row detail (F.SR: live 3, rel 90, ROI 28, momentum 45, strength -7.5 vs pool, conf High)",
      {k: f_row.get(k) for k in ("shadow_rank", "live_rank", "rel", "engine_roi_pct", "momentum",
                                  "strength", "strength_ref", "cap", "floor_ok", "conf", "revision",
                                  "sector", "market")}
      == {"shadow_rank": 1, "live_rank": 3, "rel": 90.0, "engine_roi_pct": 28.0, "momentum": 45.0,
          "strength": -7.5, "strength_ref": "pool", "cap": False, "floor_ok": True, "conf": "High",
          "revision": 0, "sector": "Materials", "market": "Tadawul"}, f_row)
    T("P5 coverage block (momentum 5/6 invest, 6/7 scanned, no sector median, no revision)",
      rc.get("coverage") == {"invest_with_momentum": 5, "scanned_with_momentum": 6, "scanned": 7,
                             "sectors_with_median": 0, "revision_known": 0}, rc.get("coverage"))
    T("P5 meta carries version / mode / rule / required ROI and no enforce flag in shadow",
      rc.get("version") == "1.0" and rc.get("mode") == "shadow" and "34%" in rc.get("rule", "")
      and rc.get("required_roi_pct") == 12.0 and "enforce_reserved" not in rc
      and rc.get("builder") == ob.OPPORTUNITY_BUILDER_VERSION, {k: rc.get(k) for k in ("version", "mode")})

    # ------------------------------------------------------------------ P6
    def order_of(rows, crit=None, mode="shadow"):
        with _mode(mode):
            p = build(ob, rows, crit or {"max_selected": 10}, {"cash_available_sar": 900000})
        rcx = p["meta"].get("rank_challenger") or {}
        return p, rcx, [r["symbol"] for r in rcx.get("challenger") or []]

    # Reliability bands 14..19 (72, 77, 82, 87, 92, 97); both cap names sit in
    # the top band (99.5 / 98) and would rank 2-3 without the cap rule.
    six = [row("N%d.SR" % i, 72 + 5 * i, 15.0 + i, sector="S%d" % i) for i in range(6)]
    caps = [row("CAPA.SR", 99.5, 35.0, sector="SA"), row("CAPB.SR", 98, 34.0, sector="SB")]
    _p, rc6, o6 = order_of(six + caps)
    T("P6 cap rule: the two top-band cap names are held at ranks 6 and 7",
      o6 == ["N5.SR", "N4.SR", "N3.SR", "N2.SR", "N1.SR", "CAPB.SR", "CAPA.SR", "N0.SR"]
      and (rc6.get("tiers") or {}).get("at_cap") == 2, o6)
    _p, rc6b, o6b = order_of(six[3:] + caps)
    T("P6 cap rule with only 3 below-cap names: cap names fill ranks 4-5 behind them",
      o6b == ["N5.SR", "N4.SR", "N3.SR", "CAPB.SR", "CAPA.SR"], o6b)

    # ------------------------------------------------------------------ P7
    sec = [row("M1.SR", 82, 20.0, sector="Banks", momentum_score=50),
           row("M2.SR", 82, 21.0, sector="Banks", momentum_score=60),
           row("M3.SR", 82, 22.0, sector="Banks", momentum_score=70),
           row("M4.SR", 82, 23.0, sector="Energy", momentum_score=40)]
    _p, rc7, o7 = order_of(sec)
    det = {r["symbol"]: (r["strength"], r["strength_ref"]) for r in rc7.get("challenger") or []}
    T("P7 sector median (Banks 60 from 3 values) vs pool median (Energy: 55 from 4 values)",
      det == {"M1.SR": (-10.0, "sector"), "M2.SR": (0.0, "sector"), "M3.SR": (10.0, "sector"),
              "M4.SR": (-15.0, "pool")}, det)
    T("P7 strength decides inside one reliability band (M3 +1, M2 0, then M1/M4 -1 by smaller ROI)",
      o7 == ["M3.SR", "M2.SR", "M1.SR", "M4.SR"], o7)
    edge = [row("Q1.SR", 82, 20.0, sector="X", momentum_score=55),   # pool median 50 -> +5.0
            row("Q2.SR", 82, 21.0, sector="Y", momentum_score=45),   # -5.0
            row("Q3.SR", 82, 22.0, sector="Z", momentum_score=50.1),  # +0.1
            row("Q4.SR", 82, 23.0, sector="W", momentum_score=49.9)]  # -0.1
    _p, rc7b, o7b = order_of(edge)
    T("P7 the +/-5 deadband is exclusive (exactly +/-5.0 reads neutral)",
      o7b == ["Q1.SR", "Q2.SR", "Q3.SR", "Q4.SR"]
      and [r["strength"] for r in rc7b["challenger"]] == [5.0, -5.0, 0.1, -0.1], (o7b, rc7b.get("challenger")))

    # ------------------------------------------------------------------ P8
    band = [row("K1.SR", 86, 25.0, sector="A1", target_revision_30d=0),
            row("K2.SR", 88, 30.0, sector="A2", target_revision_30d="raised"),
            row("K3.SR", 87, 15.0, sector="A3", target_revision_30d=-2.5),
            row("K4.SR", 85, 16.0, sector="A4"),
            row("K5.SR", 89.9, 16.0, sector="A5"),
            row("K6.SR", 90.0, 33.0, sector="A6")]
    _p, rc8, o8 = order_of(band)
    T("P8 band 18 first; inside band 17: revision (+1 > 0 > -1), then smaller ROI, then symbol",
      o8 == ["K6.SR", "K2.SR", "K4.SR", "K5.SR", "K1.SR", "K3.SR"], o8)
    T("P8 revision_known counts the two non-zero revisions",
      (rc8.get("coverage") or {}).get("revision_known") == 2, rc8.get("coverage"))

    # ------------------------------------------------------------------ P9
    fl = [row("V1.SR", 90, None, sector="B1"), row("V2.SR", 89, 11.9, sector="B2"),
          row("V3.SR", 70, 12.0, sector="B3"), row("V4.SR", 80, 18.0, sector="B4")]
    for r in fl:
        if r["expected_roi_12m"] is None:
            r.pop("expected_roi_12m")
    _p, rc9, o9 = order_of(fl)
    T("P9 floor: missing / below-required engine ROI sort after every floor pass",
      o9[:2] == ["V4.SR", "V3.SR"] and set(o9[2:]) == {"V1.SR", "V2.SR"}
      and rc9.get("tiers") == {"below_cap": 2, "at_cap": 0, "below_floor": 2}, (o9, rc9.get("tiers")))
    _p, rc9b, o9b = order_of(fl, {"max_selected": 10, "required_roi_pct": 15.0})
    T("P9 the floor follows criteria required_roi_pct (15 -> V3 drops below the floor)",
      rc9b.get("required_roi_pct") == 15.0 and o9b[0] == "V4.SR"
      and (rc9b.get("tiers") or {}).get("below_floor") == 3, (o9b, rc9b.get("tiers")))

    # ------------------------------------------------------------------ P10
    table = {None: 0, "": 0, 0: 0, 2.5: 1, -0.1: -1, "3%": 1, "-4.2": -1, "raised": 1,
             "UP": 1, "upgrade": 1, "cut": -1, "Lowered": -1, "down": -1, "flat": 0,
             "n/a": 0, "unknown": 0}
    got = {k: ob._rank_revision_dir(k) for k in table}
    T("P10 revision direction parsing table", got == table, got)

    # ------------------------------------------------------------------ P11
    with _mode("enforce"):
        E = build(ob, F1, CRIT1, PORT1)
    rce = E["meta"].get("rank_challenger") or {}
    T("P11 enforce is reserved: selection == legacy, same order as shadow, flagged",
      strip_rank(E) == strip_rank(L) and rce.get("enforce_reserved") is True
      and rce.get("mode") == "enforce" and [r["symbol"] for r in rce.get("challenger") or []] == chal)

    # ------------------------------------------------------------------ P12
    with _mode("shadow"):
        res_s, alloc_s = research_alloc(ob)
    with _mode(None):
        res_l, alloc_l = research_alloc(ob)
    T("P12 research pass (shadow) carries the order and ticket shadow ranks",
      "rank_challenger" in res_s["meta"] and all("shadow_rank" in t for t in res_s["selected"])
      and res_s["selected"], [t.get("shadow_rank") for t in res_s.get("selected") or []])
    T("P12 allocate replay (shadow) never ranks and funds exactly what legacy funds",
      alloc_s is not None and alloc_l is not None and not has_rank_keys(alloc_s)
      and [(t["symbol"], t["suggested_sar"], t["suggested_shares"]) for t in alloc_s["selected"]]
      == [(t["symbol"], t["suggested_sar"], t["suggested_shares"]) for t in alloc_l["selected"]]
      and alloc_s["selected"], [(t["symbol"], t["suggested_sar"]) for t in (alloc_s or {}).get("selected") or []])

    # ------------------------------------------------------------------ P13
    real = ob._rank_challenger

    def boom(*a, **k):
        raise ValueError("synthetic")
    ob._rank_challenger = boom
    try:
        with _mode("shadow"):
            (X, x_lines) = _rank_lines(lambda: build(ob, F1, CRIT1, PORT1))
    finally:
        ob._rank_challenger = real
    T("P13 a raising challenger never fails the build (legacy selection, error block, no shadow_rank)",
      strip_rank(X) == strip_rank(L)
      and X["meta"].get("rank_challenger") == {"version": "1.0", "mode": "shadow", "error": "ValueError"}
      and not any("shadow_rank" in t for t in X["selected"])
      and not any("shadow_rank" in c for c in X["candidates_rows"])
      and len(x_lines) == 1 and "error=ValueError" in x_lines[0], x_lines)

    # ------------------------------------------------------------------ P14
    T("P14 exactly one INFO line per armed build, with the counts",
      len(s_lines) == 1 and "mode=shadow pool=6 below_cap=3 at_cap=2 below_floor=1 overlap_top5=5 "
      "momentum=5/6 top5=F.SR,B.SR,G.SR,A.SR,D.SR" in s_lines[0], s_lines)

    # ------------------------------------------------------------------ P15
    seq = []
    for v in ("shadow", None, "shadow", "legacy"):
        with _mode(v):
            seq.append("rank_challenger" in build(ob, F1, CRIT1, PORT1)["meta"])
    T("P15 the gate is read per build (no reload)", seq == [True, False, True, False], seq)

    digest = hashlib.sha256("\n".join(out).encode()).hexdigest()[:16]
    print("\n".join(out))
    print("RESULT %d PASS / %d FAIL, digest %s" % (sum(1 for l in out if l.startswith("PASS")),
                                                  sum(1 for l in out if l.startswith("FAIL")), digest))


def test_all():
    result = subprocess.run([sys.executable, str(Path(__file__).resolve())],
                            cwd=Path(__file__).resolve().parents[1],
                            capture_output=True, text=True, timeout=300)
    assert result.returncode == 0, result.stdout + result.stderr


if __name__ == "__main__":
    main()
