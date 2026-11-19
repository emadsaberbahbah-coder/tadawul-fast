#!/usr/bin/env python3
"""One-off research job (owner, 20 Nov 2026 01:35 Riyadh): the US sector leaders' last-3-year return.
Reads research/sector_leaders.json (up to 10 US companies per sector, chosen by market cap from the TFB market pages),
fetches daily bars from the Yahoo chart API, and writes, per company: price return and total return (dividends
reinvested, Yahoo adjusted close) from the last close on or before the start date to the latest close, the
annualised total return, the largest fall from a high inside the window, and the same for the sector fund and SPY.
Prints a markdown summary; writes a CSV. Public data only; no secret, no workbook, no BigQuery."""
import csv, datetime as dt, json, sys, time
import requests

HOSTS = ("query1.finance.yahoo.com", "query2.finance.yahoo.com")
UA = {"User-Agent": "Mozilla/5.0 (tfb-research)", "Accept": "application/json"}


def bars(sym):
    ysym = sym.replace(".", "-")
    for attempt in range(3):
        for host in HOSTS:
            try:
                r = requests.get("https://%s/v8/finance/chart/%s" % (host, ysym),
                                 params={"range": "5y", "interval": "1d", "events": "div,split"}, headers=UA, timeout=30)
                if r.status_code != 200:
                    continue
                res = r.json()["chart"]["result"][0]
                ts = res.get("timestamp") or []
                q = res["indicators"]["quote"][0]
                adj = (res["indicators"].get("adjclose") or [{}])[0].get("adjclose") or [None] * len(ts)
                out = []
                for t, c, a in zip(ts, q.get("close") or [], adj):
                    if c is None or a is None:
                        continue
                    out.append((dt.datetime.utcfromtimestamp(t).date(), float(c), float(a)))
                if out:
                    return out
            except Exception as e:  # noqa: BLE001
                err = e
        time.sleep(3 * (attempt + 1))
    return []


def measure(series, start):
    before = [b for b in series if b[0] <= start]
    if not before or series[0][0] > start - dt.timedelta(days=7):
        return None
    s, e = before[-1], series[-1]
    window = [b for b in series if b[0] >= s[0]]
    peak, mdd = -1.0, 0.0
    for _, _, a in window:
        peak = max(peak, a)
        mdd = min(mdd, a / peak - 1.0)
    yrs = (e[0] - s[0]).days / 365.25
    tr = e[2] / s[2] - 1.0
    return {"start_day": s[0].isoformat(), "end_day": e[0].isoformat(), "start_close": round(s[1], 4), "end_close": round(e[1], 4),
            "price_ret_pct": round((e[1] / s[1] - 1.0) * 100, 1), "total_ret_pct": round(tr * 100, 1),
            "cagr_pct": round(((1.0 + tr) ** (1.0 / yrs) - 1.0) * 100, 1) if tr > -1 else None,
            "max_drawdown_pct": round(mdd * 100, 1), "years": round(yrs, 2)}


def main(src, dst):
    cfg = json.load(open(src))
    start = dt.date.fromisoformat(cfg["start"])
    rows, cache = [], {}

    def get(sym):
        if sym not in cache:
            cache[sym] = measure(bars(sym), start)
            time.sleep(0.4)
        return cache[sym]
    bench = get(cfg["benchmark"])
    for sec in cfg["sectors"]:
        etf = get(sec["etf"])
        for c in sec["companies"]:
            m = get(c["symbol"])
            row = {"sector": sec["sector"], "symbol": c["symbol"], "name": c["name"], "mcap_b": c["mcap_b"], "etf": sec["etf"]}
            row.update(m or {"note": "no 3-year history"})
            row["etf_total_ret_pct"] = etf["total_ret_pct"] if etf else None
            row["spy_total_ret_pct"] = bench["total_ret_pct"] if bench else None
            rows.append(row)
    keys = ["sector", "symbol", "name", "mcap_b", "start_day", "end_day", "start_close", "end_close", "price_ret_pct",
            "total_ret_pct", "cagr_pct", "max_drawdown_pct", "years", "etf", "etf_total_ret_pct", "spy_total_ret_pct", "note"]
    with open(dst, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)
    json.dump({"benchmark": bench, "etfs": {s["etf"]: cache.get(s["etf"]) for s in cfg["sectors"]}}, open(dst + ".bench.json", "w"), indent=1)
    print("## US sector leaders: 3-year return (start %s)\n" % cfg["start"])
    print("SPY total return: %s%%\n" % (bench or {}).get("total_ret_pct"))
    print("| Sector | Symbol | Price % | Total % | CAGR % | Max DD % | Window |\n|---|---|---|---|---|---|---|")
    for r in rows:
        print("| %s | %s | %s | %s | %s | %s | %s to %s |" % (r["sector"], r["symbol"], r.get("price_ret_pct"), r.get("total_ret_pct"),
              r.get("cagr_pct"), r.get("max_drawdown_pct"), r.get("start_day"), r.get("end_day")))
    missing = [r["symbol"] for r in rows if r.get("note")]
    print("\nMissing: %s" % (", ".join(missing) or "none"))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
