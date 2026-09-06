"""tfb_diag_eodhd_fund.py — READ-ONLY diagnostic for the EODHD fundamentals fallback.
Run in the Render shell of tadawul-fast-bridge:
    python tfb_diag_eodhd_fund.py            # default symbol AVGO.US
    python tfb_diag_eodhd_fund.py HNI.US     # any symbol
No sheet writes. Network: one EODHD fundamentals call (10 units) + the normal
provider legs of one enriched quote for the symbol. Prints six numbered layers;
the FIRST layer that reports a stop is the cause.
"""
import asyncio, os, sys, time, traceback

SYM = (sys.argv[1] if len(sys.argv) > 1 else "AVGO.US").strip().upper()
FIELDS = ("market_cap", "pe_ttm", "debt_to_equity", "free_cash_flow_ttm",
          "revenue_ttm", "sector")

def mask(v):
    v = str(v or "")
    return ("<unset>" if not v else v[:4] + "…" + v[-3:] + f" (len {len(v)})")

def line(n, txt):
    print(f"[{n}] {txt}", flush=True)

async def main():
    line(0, f"symbol={SYM}  utc={time.strftime('%Y-%m-%d %H:%M:%SZ', time.gmtime())}")
    # --- 1. environment ---------------------------------------------------
    for k in ("TFB_EODHD_FUNDAMENTALS_FALLBACK", "TFB_EODHD_DAILY_BUDGET",
              "TFB_EODHD_PLAN_RESTRICTED_ISOLATION", "TFB_EODHD_IDENTITY_GUARD",
              "ENGINE_YAHOO_ENRICHMENT_ENABLED", "TFB_ENGINE_FUND_LKG",
              "TFB_ENGINE_FUND_LKG_REDIS", "EODHD_ALLOW_KSA"):
        line(1, f"env {k} = {os.getenv(k, '<unset>')!s}")
    line(1, f"env EODHD_API_KEY = {mask(os.getenv('EODHD_API_KEY'))}")

    # --- 2. provider module + client --------------------------------------
    try:
        from core.providers import eodhd_provider as ep
        line(2, f"eodhd_provider v{getattr(ep, 'PROVIDER_VERSION', '?')} imported; "
                f"module-level get_client={callable(getattr(ep, 'get_client', None))} "
                f"fetch_fundamentals_patch={callable(getattr(ep, 'fetch_fundamentals_patch', None))}")
        client = await ep.get_client()
        line(2, f"client ok: api_key={mask(getattr(client, 'api_key', ''))} daily_budget={getattr(client, 'daily_budget', '?')} "
                f"base_url={getattr(client, 'base_url', '?')}")
    except Exception as exc:
        line(2, f"STOP provider import/client failed: {exc.__class__.__name__}: {exc}")
        traceback.print_exc(); return

    # --- 3. direct provider call (the HTTP truth) --------------------------
    try:
        cache_key = f"f:{ep.normalize_eodhd_symbol(SYM)}"
        line(3, f"plan_restricted_cache_active(fundamentals/…)={ep._plan_restricted_cache_active('fundamentals/' + ep.normalize_eodhd_symbol(SYM))} "
                f"cache_key={cache_key}")
        t0 = time.time()
        patch, err = await client.fetch_fundamentals(SYM)
        got = {k: patch.get(k) for k in FIELDS if isinstance(patch, dict)}
        warns = patch.get("warnings") if isinstance(patch, dict) else None
        line(3, f"client.fetch_fundamentals -> err={err!r} in {time.time()-t0:.1f}s; fields={got}; warnings={warns}")
        if err:
            line(3, "STOP provider-level: the call is refused/failed here (plan, auth, budget, breaker or network)")
    except Exception as exc:
        line(3, f"STOP fetch_fundamentals raised: {exc.__class__.__name__}: {exc}")
        traceback.print_exc()

    # --- 4. engine gate + engine-side fetch -------------------------------
    try:
        import core.data_engine_v2 as eng
        line(4, f"engine v{getattr(eng, '__version__', '?')}; _eodhd_fundamentals_fallback_enabled()={eng._eodhd_fundamentals_fallback_enabled()} "
                f"_yahoo_enrichment_enabled()={eng._yahoo_enrichment_enabled()}")
        engine = await eng.get_engine()
        mod = engine._provider_registry.get("eodhd")
        line(4, f"registry.get('eodhd') -> {getattr(mod, '__name__', mod)}")
        fn = eng._pick_provider_callable(mod, "fetch_fundamentals_patch", "get_fundamentals", "fetch_fundamentals", "fundamentals")
        line(4, f"_pick_provider_callable -> {getattr(fn, '__name__', fn)} (None means get_client path)")
        p2 = await engine._fetch_eodhd_fundamentals_patch(SYM, "Global_Markets")
        line(4, f"engine._fetch_eodhd_fundamentals_patch -> keys={sorted(k for k in (p2 or {}) if k in FIELDS)} "
                f"warnings={(p2 or {}).get('warnings')}")
        if not p2:
            line(4, "STOP engine-level fetch returned {} (registry/callable/exception path)")
    except Exception as exc:
        line(4, f"STOP engine import/fetch raised: {exc.__class__.__name__}: {exc}")
        traceback.print_exc(); return

    # --- 5. engine apply (gap gate + identity guard + missing-field filter) --
    try:
        row = {"symbol": SYM, "requested_symbol": SYM, "current_price": 1.0,
               "debt_to_equity": None, "free_cash_flow_ttm": None, "warnings": ""}
        out = await engine._apply_eodhd_fundamentals_fallback(dict(row), SYM, "Global_Markets")
        filled = {k: out.get(k) for k in FIELDS}
        line(5, f"engine._apply_eodhd_fundamentals_fallback on a blank row -> filled={filled} warnings={out.get('warnings')!r}")
        if "eodhd_fundamentals_fallback_applied" not in str(out.get("warnings", "")):
            line(5, "STOP apply-level: fetch returned data (see [4]) but nothing was merged/tagged — identity guard or field filter")
    except Exception as exc:
        line(5, f"STOP apply raised: {exc.__class__.__name__}: {exc}")
        traceback.print_exc()

    # --- 6. end-to-end enriched quote (what the sheet gets) ----------------
    try:
        q = await engine.get_enriched_quote(SYM, "Global_Markets")
        d = q if isinstance(q, dict) else getattr(q, "model_dump", getattr(q, "dict", lambda: {}))()
        line(6, f"get_enriched_quote -> fields={ {k: d.get(k) for k in FIELDS} }")
        w = str(d.get("warnings", ""))
        tags = [t for t in ("yahoo_enrichment_applied", "eodhd_fundamentals_fallback_applied",
                            "fundamentals_lkg", "identity_patch_refused", "low_data_trust") if t in w]
        line(6, f"warnings tags present: {tags}")
    except Exception as exc:
        line(6, f"end-to-end raised: {exc.__class__.__name__}: {exc}")
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())
