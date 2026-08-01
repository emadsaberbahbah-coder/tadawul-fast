#!/usr/bin/env python3
"""Read-only, full-row audit for the GitHub automatic refresh pipeline."""
from __future__ import annotations
import argparse, asyncio, importlib, inspect, json, math, os, sys
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone

# =============================================================================
# v1.1.0 (2026-08-01) — TZ-AWARE parse_dt (3-hour UTC/Riyadh conflation fixed).
# parse_dt stripped tzinfo blindly: "...Z" lost its zone and the UTC wall time
# was then compared against Riyadh-naive now (audit_decision_surface_freshness
# builds now_riyadh at +03:00 and every _age_hours call is naive-Riyadh), so a
# 22:03:19Z stamp read as 22:03 Riyadh — 3 hours OLDER than reality, enough to
# flip stale/fresh at the 4-8h thresholds in live use. Offset-suffixed ISO
# ("+03:00"/"+00:00") took the fromisoformat tail, which normalised to
# UTC-naive — the SAME +3h error by the other route. Aware datetime instances
# were blind-stripped identically. FIX (flag TFB_AUDIT_TZ_AWARE, default ON):
# any tz-AWARE input — Z, ±offset ISO, aware datetime — converts to Riyadh
# (+03:00, no DST) BEFORE the naive strip; naive strings keep the documented
# Riyadh-local convention unchanged; Excel serials and m/d/Y unchanged.
# Requires Python 3.11+ fromisoformat (runtime.txt pins 3.11). OFF-path
# preserves the legacy body byte-equivalently (dual-run selftested against
# the untouched baseline). This file previously carried NO version constant
# and NO verify_deployment pin — both added here (implicit prior = 1.0.0).
# audit_decision_surface_freshness imports parse_dt from here and is fixed
# transitively; it stays unversioned/unpinned until its own run_id build
# (CG-4) touches it. Zero functions removed.
# =============================================================================
SCRIPT_VERSION = "1.1.0"
_RIYADH_TZ = timezone(timedelta(hours=3))


def _audit_tz_aware_enabled():
    """v1.1.0 kill-switch (default ON). 0/false/off/no -> legacy parse."""
    return (os.getenv("TFB_AUDIT_TZ_AWARE") or "1").strip().lower() \
        not in {"0", "false", "off", "no"}

from pathlib import Path
from typing import Any, Callable, Optional, Sequence

VERSION = "1.0.0"
END_COL, DEFAULT_MAX_ROWS = "EZ", 20000
SYMBOL = ("Symbol", "Ticker")
NAME = ("Name", "Company Name", "Instrument Name")
PRICE = ("Current Price", "Price", "Last Price")
STAMP = ("Last Updated (Riyadh)", "Last Updated (UTC)", "Last Updated")
QTY = ("Position Qty", "Quantity", "Shares")
COST = ("Avg Cost", "Average Cost", "Buy Price")

for p in (Path(__file__).resolve().parent, Path(__file__).resolve().parent.parent):
    if str(p) not in sys.path: sys.path.insert(0, str(p))

@dataclass(frozen=True)
class Rule:
    page: str; min_rows: int; max_age_h: Optional[float]; min_fresh: float
    min_name: float; min_price: float; symbols: bool = True; portfolio: bool = False

@dataclass
class Result:
    page: str; status: str = "PENDING"; header_row: int = 0
    expected_cols: int = 0; actual_cols: int = 0; rows: int = 0; unique: int = 0
    min_rows: int = 0; fresh: int = 0; stale: int = 0; bad_stamps: int = 0
    fresh_pct: Optional[float] = None; name_pct: Optional[float] = None
    price_pct: Optional[float] = None; newest_age_h: Optional[float] = None
    oldest_age_h: Optional[float] = None; duplicates: list[str] = field(default_factory=list)
    blank_symbols: int = 0; missing_portfolio: list[str] = field(default_factory=list)
    extra_portfolio: list[str] = field(default_factory=list)
    missing_qty: list[str] = field(default_factory=list); missing_cost: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list); warnings: list[str] = field(default_factory=list)
    def finish(self):
        self.status = "FAIL" if self.failures else ("WARN" if self.warnings else "PASS"); return self

@dataclass
class Report:
    generated_at_utc: str; sheet: str; schema_version: str; active_holdings: list[str]
    pages: list[Result] = field(default_factory=list); fatal: str = ""
    @property
    def code(self): return 3 if self.fatal else (2 if any(p.failures for p in self.pages) else (1 if any(p.warnings for p in self.pages) else 0))
    def payload(self):
        return {"script_version": VERSION, "generated_at_utc": self.generated_at_utc,
                "sheet": self.sheet, "schema_version": self.schema_version,
                "summary": {"pages": len(self.pages), "failures": sum(bool(p.failures) for p in self.pages),
                            "warnings": sum(bool(p.warnings) for p in self.pages), "exit_code": self.code, "fatal": self.fatal},
                "active_holdings": self.active_holdings, "pages": [asdict(p) for p in self.pages]}

def s(v):
    try: return "" if v is None else str(v).strip()
    except Exception: return ""

def f(v):
    if v is None or isinstance(v, bool): return None
    try:
        x = float(v) if isinstance(v, (int,float)) else float(s(v).replace(",","").replace("%",""))
        return None if math.isnan(x) or math.isinf(x) else x
    except Exception: return None

def env_i(n,d):
    try: return int(float(os.getenv(n, "") or d))
    except Exception: return d

def env_f(n,d):
    try: return float(os.getenv(n, "") or d)
    except Exception: return d

def pct(a,b): return None if b <= 0 else round(100*a/b, 4)
def trim(row):
    out=[s(x) for x in row]
    while out and not out[-1]: out.pop()
    return out

def idx(headers, names):
    m={s(h).casefold():i for i,h in enumerate(headers)}
    return next((m[n.casefold()] for n in names if n.casefold() in m), -1)
def has(row): return any(s(x) for x in row)

def parse_dt(v):
    tz_on = _audit_tz_aware_enabled()
    if isinstance(v, datetime):
        # v1.1.0: an AWARE instance converts to Riyadh before the strip;
        # a naive instance keeps the legacy pass-through.
        if tz_on and v.tzinfo is not None:
            return v.astimezone(_RIYADH_TZ).replace(tzinfo=None)
        return v.replace(tzinfo=None)
    if isinstance(v,(int,float)) and not isinstance(v,bool) and 20000 < float(v) < 80000:
        return datetime(1899,12,30)+timedelta(days=float(v))
    if tz_on:
        # v1.1.0: aware-first. "Z" becomes an explicit +00:00 (never
        # silently dropped); any offset-carrying ISO converts to Riyadh
        # then strips. Naive strings fall through to the legacy formats
        # under the documented Riyadh-local convention.
        t = s(v).replace("T", " ")
        if t.endswith("Z"):
            t = t[:-1] + "+00:00"
        try:
            d = datetime.fromisoformat(t)
            if d.tzinfo is not None:
                return d.astimezone(_RIYADH_TZ).replace(tzinfo=None)
        except Exception:
            pass
        t2 = s(v).replace("T", " ").replace("Z", "")
        for fmt in ("%Y-%m-%d %H:%M:%S.%f","%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M","%Y-%m-%d","%m/%d/%Y %H:%M:%S","%m/%d/%Y"):
            try: return datetime.strptime(t2,fmt)
            except Exception: pass
        try:
            d = datetime.fromisoformat(t2)
            return d.astimezone(_RIYADH_TZ).replace(tzinfo=None) if d.tzinfo else d
        except Exception: return None
    # ---- legacy path (TFB_AUDIT_TZ_AWARE=0): v-prior body, verbatim ----
    t=s(v).replace("T"," ").replace("Z","")
    for fmt in ("%Y-%m-%d %H:%M:%S.%f","%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M","%Y-%m-%d","%m/%d/%Y %H:%M:%S","%m/%d/%Y"):
        try: return datetime.strptime(t,fmt)
        except Exception: pass
    try:
        d=datetime.fromisoformat(t); return d.astimezone(timezone.utc).replace(tzinfo=None) if d.tzinfo else d
    except Exception: return None

def resolve_reader():
    for mod in ("integrations.google_sheets_service","core.integrations.google_sheets_service","google_sheets_service","core.google_sheets_service"):
        try:
            fn=getattr(importlib.import_module(mod),"read_range",None)
            if callable(fn): return fn
        except Exception: pass
    return None

def resolve_registry():
    for mod in ("core.sheets.schema_registry","schema_registry","core.schema_registry","sheets.schema_registry"):
        try:
            m=importlib.import_module(mod)
            if callable(getattr(m,"get_sheet_headers",None)): return m
        except Exception: pass
    return None

async def read(reader, sid, page, limit):
    loop=asyncio.get_running_loop(); val=await loop.run_in_executor(None,lambda:reader(sid,f"{page}!A1:{END_COL}{limit+50}"))
    if inspect.isawaitable(val): val=await val
    if not isinstance(val,list): raise TypeError("read_range did not return a list")
    return [list(r) if isinstance(r,(list,tuple)) else [r] for r in val]

def header_row(grid, expected):
    exp={s(x).casefold() for x in expected if s(x)}; best=(-1,0)
    for i,row in enumerate(grid[:45]):
        score=len({s(x).casefold() for x in row if s(x)} & exp)
        if score>best[1]: best=(i,score)
    return best[0] if best[1] >= (3 if len(exp)>=3 else 1) else -1

def ledger_symbols(grid):
    hr=-1; headers=[]
    for i,row in enumerate(grid[:45]):
        h=trim(row); low={x.casefold() for x in h}
        if "symbol" in low and ("status" in low or "shares" in low or "quantity" in low): hr,headers=i,h; break
    if hr<0: return [], ["_Portfolio_CostBasis header not found"]
    si,sti,qi=idx(headers,("Symbol",)),idx(headers,("Status",)),idx(headers,("Shares","Quantity","Position Qty"))
    active=[]
    for row in grid[hr+1:]:
        if not has(row): continue
        sym=s(row[si] if si<len(row) else "").upper(); status=s(row[sti] if 0<=sti<len(row) else "Active").casefold(); q=f(row[qi] if 0<=qi<len(row) else 1)
        if sym and status not in {"inactive","closed","sold"} and (q is None or q>0): active.append(sym)
    return sorted(set(active)), []

def rules():
    def R(page,count,price=95,name=99):
        key=page.upper(); return Rule(page,env_i(f"TFB_EXPECTED_MIN_ROWS_{key}",count),env_f(f"TFB_REFRESH_MAX_AGE_H_{key}",30),env_f(f"TFB_REFRESH_MIN_FRESH_PCT_{key}",95),name,price)
    return [R("Market_Leaders",1025),R("Global_Markets",6512),R("Commodities_FX",453,95,95),R("Mutual_Funds",4496,90),
            Rule("My_Portfolio",1,env_f("TFB_REFRESH_MAX_AGE_H_MY_PORTFOLIO",8),100,100,100,True,True),
            Rule("Insights_Analysis",1,None,0,0,0,False),Rule("Data_Dictionary",1,None,0,0,0,False)]

def audit_grid(grid, rule, expected, now, active=()):
    hr=header_row(grid,expected); r=Result(rule.page,min_rows=rule.min_rows,expected_cols=len(expected),header_row=hr+1 if hr>=0 else 0)
    if hr<0: r.failures.append("header row not found"); return r.finish()
    headers=trim(grid[hr]); r.actual_cols=len(headers)
    if headers != [s(x) for x in expected]: r.failures.append(f"header mismatch: expected {len(expected)}, found {len(headers)}")
    rows=[list(x) for x in grid[hr+1:] if has(x)]; r.rows=len(rows)
    if r.rows<rule.min_rows: r.failures.append(f"row count {r.rows} below minimum {rule.min_rows}")
    if not rule.symbols: return r.finish()
    si,ni,pi,ti,qi,ci=idx(headers,SYMBOL),idx(headers,NAME),idx(headers,PRICE),idx(headers,STAMP),idx(headers,QTY),idx(headers,COST)
    if si<0: r.failures.append("Symbol column missing"); return r.finish()
    symbols=[]; names=prices=fresh=stale=bad=0; ages=[]; qmap={}; cmap={}; riyadh=ti>=0 and "riyadh" in headers[ti].casefold(); now0=now.astimezone(timezone.utc).replace(tzinfo=None)+(timedelta(hours=3) if riyadh else timedelta())
    for row in rows:
        sym=s(row[si] if si<len(row) else "").upper()
        if not sym: r.blank_symbols+=1; continue
        symbols.append(sym); names+=int(ni>=0 and ni<len(row) and bool(s(row[ni]))); prices+=int(pi>=0 and pi<len(row) and (f(row[pi]) or 0)>0)
        if qi>=0: qmap[sym]=f(row[qi] if qi<len(row) else None)
        if ci>=0: cmap[sym]=f(row[ci] if ci<len(row) else None)
        if rule.max_age_h is not None:
            d=parse_dt(row[ti] if ti>=0 and ti<len(row) else None)
            if d is None: bad+=1; stale+=1
            else:
                age=max(0,(now0-d).total_seconds()/3600); ages.append(age); fresh+=int(age<=rule.max_age_h); stale+=int(age>rule.max_age_h)
    c=Counter(symbols); r.unique=len(c); r.duplicates=sorted(k for k,v in c.items() if v>1)
    if r.blank_symbols: r.failures.append(f"{r.blank_symbols} blank-symbol row(s)")
    if r.duplicates: r.failures.append("duplicate symbols: "+", ".join(r.duplicates[:20]))
    r.name_pct=pct(names,len(symbols)) if ni>=0 else None; r.price_pct=pct(prices,len(symbols)) if pi>=0 else None
    if ni<0 and rule.min_name: r.failures.append("Name column missing")
    elif r.name_pct is not None and r.name_pct<rule.min_name: r.failures.append(f"name coverage {r.name_pct:.2f}% below {rule.min_name:.2f}%")
    if pi<0 and rule.min_price: r.failures.append("Current Price column missing")
    elif r.price_pct is not None and r.price_pct<rule.min_price: r.failures.append(f"price coverage {r.price_pct:.2f}% below {rule.min_price:.2f}%")
    if rule.max_age_h is not None:
        r.fresh,r.stale,r.bad_stamps=fresh,stale,bad; r.fresh_pct=pct(fresh,len(symbols)); r.newest_age_h=min(ages) if ages else None; r.oldest_age_h=max(ages) if ages else None
        if ti<0: r.failures.append("Last Updated column missing")
        elif r.fresh_pct is None or r.fresh_pct<rule.min_fresh: r.failures.append(f"fresh coverage {r.fresh_pct} below {rule.min_fresh}% within {rule.max_age_h}h")
        if bad: r.warnings.append(f"{bad} blank/unparseable timestamp(s)")
    if rule.portfolio:
        a,p=set(active),set(symbols); r.missing_portfolio=sorted(a-p); r.extra_portfolio=sorted(p-a); r.min_rows=len(a) or 1
        if r.missing_portfolio: r.failures.append("active ledger symbols missing: "+", ".join(r.missing_portfolio))
        if r.extra_portfolio: r.warnings.append("portfolio symbols not active in ledger: "+", ".join(r.extra_portfolio[:20]))
        r.missing_qty=sorted(x for x in a if (qmap.get(x) or 0)<=0); r.missing_cost=sorted(x for x in a if (cmap.get(x) or 0)<=0)
        if qi<0: r.failures.append("Position Qty column missing")
        elif r.missing_qty: r.failures.append("missing/non-positive quantity: "+", ".join(r.missing_qty))
        if ci<0: r.failures.append("Avg Cost column missing")
        elif r.missing_cost: r.failures.append("missing/non-positive cost: "+", ".join(r.missing_cost))
    return r.finish()

async def run(sid, limit=DEFAULT_MAX_ROWS, reader=None, registry=None, now=None):
    reader,registry,now=reader or resolve_reader(),registry or resolve_registry(),now or datetime.now(timezone.utc); rep=Report(now.isoformat(),sid[:5]+"..."+sid[-5:] if len(sid)>10 else "***",s(getattr(registry,"SCHEMA_VERSION",getattr(registry,"__version__","unknown"))) if registry else "unknown",[])
    if not sid: rep.fatal="spreadsheet ID missing"; return rep
    if not reader: rep.fatal="read_range unavailable"; return rep
    if not registry: rep.fatal="schema_registry unavailable"; return rep
    try: rep.active_holdings,warn=ledger_symbols(await read(reader,sid,"_Portfolio_CostBasis",limit))
    except Exception as e: warn=[f"ledger unreadable: {e}"]
    for rule in rules():
        try:
            out=audit_grid(await read(reader,sid,rule.page,limit),rule,list(registry.get_sheet_headers(rule.page)),now,rep.active_holdings)
            if rule.portfolio and warn: out.warnings+=warn; out.failures+=(["active ledger universe not proven"] if not rep.active_holdings else []); out.finish()
            rep.pages.append(out)
        except Exception as e: rep.pages.append(Result(rule.page,status="FAIL",min_rows=rule.min_rows,failures=[f"audit failed: {e}"]))
    return rep

def main(argv=None):
    ap=argparse.ArgumentParser(description=__doc__); ap.add_argument("--sheet-id",default=os.getenv("DEFAULT_SPREADSHEET_ID", "")); ap.add_argument("--max-rows",type=int,default=env_i("TFB_REFRESH_AUDIT_MAX_ROWS",DEFAULT_MAX_ROWS)); ap.add_argument("--json-out",default=""); a=ap.parse_args(argv)
    rep=asyncio.run(run(a.sheet_id,max(100,a.max_rows))); text=json.dumps(rep.payload(),ensure_ascii=False,indent=2); print(text)
    for p in rep.pages: print(f"::{'error' if p.failures else ('warning' if p.warnings else 'notice')}::{p.page}: {p.status}, rows={p.rows}, unique={p.unique}, fresh_pct={p.fresh_pct}, failures={'; '.join(p.failures[:3]) or 'none'}")
    if rep.fatal: print(f"::error::FULL_REFRESH_AUDIT_FATAL: {rep.fatal}")
    if a.json_out: Path(a.json_out).write_text(text+"\n",encoding="utf-8")
    return rep.code

if __name__ == "__main__": raise SystemExit(main())
