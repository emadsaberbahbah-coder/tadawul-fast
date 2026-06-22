# READ-ONLY backtest grid sweep. Prints actual signal vocabulary + a
# factor x threshold x horizon table. Writes NOTHING. For Render shell.
import asyncio, math
from collections import Counter
import track_performance as T

UNIVERSE = ["2222.SR","1120.SR","1180.SR","2010.SR","7010.SR","1211.SR",
            "4013.SR","1010.SR","2350.SR","4002.SR",
            "AAPL.US","MSFT.US","NVDA.US","JPM.US","INTC.US","T.US"]
DAYS = 730
HORIZONS = [5, 10, 20, 60]
MIN_SAMPLE = 15
MIN_WIN = 12
MIN_EFF = 50.0
MIN_T = 2.0

def _yf_history(sym, period="2y"):
    """Direct yfinance pull with an explicit 2y depth. yfinance understands
    Tadawul '.SR' symbols; this bypasses the provider shim's 1mo default that
    capped KSA history at ~22 bars. '.US' symbols fail here (yfinance wants bare
    tickers) but EODHD already covers US, so that's fine."""
    try:
        import yfinance as yf
    except Exception:
        return []
    try:
        df = yf.Ticker(sym).history(period=period, interval="1d", auto_adjust=False)
    except Exception:
        return []
    if df is None or getattr(df, "empty", True):
        return []
    rows = []
    for idx, r in df.iterrows():
        try:
            rows.append({
                "date": idx.strftime("%Y-%m-%d"),
                "open": float(r["Open"]), "high": float(r["High"]),
                "low": float(r["Low"]), "close": float(r["Close"]),
                "volume": float(r.get("Volume", 0) or 0),
            })
        except Exception:
            continue
    return rows


async def _fetch_history(symbols, days):
    """US -> EODHD (deep, reliable). KSA -> yfinance direct @ period=2y (the
    only deep source for Tadawul). Takes whichever returns more bars."""
    out = {}
    eod = None
    for p in ("core.providers.eodhd_provider", "eodhd_provider"):
        try:
            eod = __import__(p, fromlist=["fetch_history"]); break
        except Exception:
            continue
    loop = asyncio.get_event_loop()
    sem = asyncio.Semaphore(6)

    async def one(s):
        async with sem:
            bars = []
            # try EODHD first (covers US with full depth; 404s on .SR)
            if eod is not None and hasattr(eod, "fetch_history"):
                try:
                    b = await eod.fetch_history(s, days=days)
                    if isinstance(b, list):
                        bars = b
                except Exception:
                    bars = []
            # if shallow/missing (i.e. KSA), pull 2y direct from yfinance
            if len(bars) < 60:
                yb = await loop.run_in_executor(None, _yf_history, s)
                if yb and len(yb) >= len(bars):
                    bars = yb
            if bars:
                out[s] = sorted(bars, key=lambda r: str((r or {}).get("date") or ""))

    await asyncio.gather(*[one(s) for s in symbols])
    return out

def _close(b):
    for k in ("adjusted_close","adjclose","close","c"):
        v = b.get(k) if isinstance(b, dict) else None
        if v not in (None, ""):
            try:
                f = float(v)
                if f > 0: return f
            except Exception:
                pass
    return None

async def main(hist=None):
    if hist is None:
        hist = await _fetch_history(UNIVERSE, DAYS)
    print("history: %d symbols | bars:" % len(hist),
          {s: len(b) for s, b in sorted(hist.items())})
    try:
        from core.data_engine_v2 import analyze_flow, analyze_candle_structure
    except Exception:
        from data_engine_v2 import analyze_flow, analyze_candle_structure

    from collections import Counter
    # per-symbol ordered records so we can build NON-OVERLAPPING samples
    per_sym = {}   # sym -> [(flow_val, candle_val, {h: roi}), ...] in day order
    ftal, ctal = Counter(), Counter()
    minh = min(HORIZONS)
    for s, bars in hist.items():
        closes = [_close(b) for b in bars]
        n = len(bars); seq = []
        for d in range(MIN_WIN, n - minh):
            fv = str((analyze_flow(bars[:d+1]) or {}).get("flow") or "").upper()
            cv = str((analyze_candle_structure(bars[:d+1]) or {}).get("candle_structure") or "").upper()
            ftal[fv] += 1; ctal[cv] += 1
            c0 = closes[d]; rois = {}
            for h in HORIZONS:
                if d + h < n and c0 and closes[d+h]:
                    rois[h] = (closes[d+h] - c0) / c0 * 100.0
            seq.append((fv, cv, rois))
        per_sym[s] = seq

    print("\nFLOW vocabulary:", dict(ftal.most_common()))
    print("CANDLE vocabulary:", dict(ctal.most_common()))

    N_TESTS = 32
    BONF_T = 2.94   # ~one-sided 0.05/32 family-wise; the honest bar for 32 tests

    def evaluate(idx_field, trig, bullish, h):
        sig_o, base_o = [], []   # overlapping (all days) -> unbiased eff/hit
        sig_i, base_i = [], []   # non-overlapping -> honest t
        for seq in per_sym.values():
            # overlapping
            for rec in seq:
                if h in rec[2]:
                    (sig_o if rec[idx_field] == trig else base_o).append(rec[2][h])
            # non-overlapping: advance cursor by h whenever a usable day is consumed
            i = 0; L = len(seq)
            while i < L:
                rec = seq[i]
                if h in rec[2]:
                    (sig_i if rec[idx_field] == trig else base_i).append(rec[2][h])
                    i += h
                else:
                    i += 1
        n_o = len(sig_o)
        if n_o == 0:
            return ("thin", 0, 0, 0.0, 0.0, 0.0)
        ms = sum(sig_o)/n_o
        mb = (sum(base_o)/len(base_o)) if base_o else 0.0
        eff = (ms - mb) * 100.0
        hits = sum(1 for x in sig_o if (x > 0 if bullish else x < 0))
        hr = hits/n_o*100.0
        n_i = len(sig_i)
        t_i = T._welch_t(sig_i, base_i) if n_i >= 2 else 0.0
        return ("ok", n_o, n_i, hr, eff, t_i)

    GRID = [
        ("flow", 0, "ACCUMULATION", True), ("flow", 0, "STRONG_ACCUMULATION", True),
        ("flow", 0, "DISTRIBUTION", False), ("flow", 0, "STRONG_DISTRIBUTION", False),
        ("candle", 1, "BULLISH", True), ("candle", 1, "STRONG_BULLISH", True),
        ("candle", 1, "BEARISH", False), ("candle", 1, "STRONG_BEARISH", False),
    ]
    print("\n(non-overlapping t = honest; ACCEPT needs eff>=50bps in-dir, |t_indep|>=2.0; * = survives 32-test Bonferroni |t|>=2.94)")
    print("\n%-7s %-20s %3s %6s %6s %5s %8s %8s" %
          ("factor","trigger","H","n_all","n_indep","hit%","eff_bps","t_indep"))
    print("-" * 74)
    survivors = []
    for fac, idxf, trig, bull in GRID:
        for h in HORIZONS:
            st, n_o, n_i, hr, eff, t_i = evaluate(idxf, trig, bull, h)
            if st == "thin":
                print("%-7s %-20s %3d %6s %6s %5s %8s %8s" % (fac, trig, h, "-","-","-","-","-"))
                continue
            eff_ok = (eff >= MIN_EFF) if bull else (eff <= -MIN_EFF)
            t_ok = (t_i >= 2.0) if bull else (t_i <= -2.0)
            t_bonf = (t_i >= BONF_T) if bull else (t_i <= -BONF_T)
            thin_i = n_i < MIN_SAMPLE
            if thin_i:
                mark = "  (indep thin)"
            elif eff_ok and t_ok and t_bonf:
                mark = "  <== ACCEPT *"; survivors.append((fac,trig,h,eff,t_i,"robust"))
            elif eff_ok and t_ok:
                mark = "  <== accept (sub-Bonferroni)"; survivors.append((fac,trig,h,eff,t_i,"marginal"))
            else:
                mark = ""
            td = (">99" if t_i > 99 else ("<-99" if t_i < -99 else "%8.2f" % t_i))
            print("%-7s %-20s %3d %6d %6d %5.0f %8.0f %8s%s" %
                  (fac, trig, h, n_o, n_i, hr, eff, td, mark))

    print("\nSURVIVORS:")
    if not survivors:
        print("  NONE pass eff>=50bps + |t_indep|>=2.0 -> no robust forward edge.")
    else:
        for fac,trig,h,eff,t_i,grade in survivors:
            print("  %-7s %-20s H=%-3d eff=%+.0fbps t_indep=%+.2f  [%s]" % (fac,trig,h,eff,t_i,grade))


if __name__ == "__main__":
    asyncio.run(main())
