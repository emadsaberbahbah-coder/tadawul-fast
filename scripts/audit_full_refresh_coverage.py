#!/usr/bin/env python3
"""Read-only, full-row audit for the GitHub automatic refresh pipeline.

VERSION 1.3.2 (2026-10-16) — NAME-GAP LIST (diagnostic only)
WHY: the Global_Markets 99% names floor failed in 5 of 5 sampled final
audits (names 97.00-98.11%) and was the ONLY coverage failure in 2 of them
(10-14 00:48Z, 10-15 11:06Z). The floor allows at most 66 blank names in
6,609 rows; HEAL-FIRST reports 123-129 blank-name symbols per pass. Nothing
said WHICH symbols, so the owner could not label them (NAME-LABELS registry)
or retire them (P0-4/D3 list), and the gap never closed. WHAT: each page
result gains blank_name_count and blank_name_symbols (sorted, first 200)
for pages with a name rule; portfolio rules are excluded (no holdings data
is ever listed). Both entry points print one ::notice line per page with a
gap (this main() and finalize_decision_publication.main(), the final-job
entry point, via name_gap_notice). Rules,
floors, failures, warnings, status and exit codes are UNCHANGED (pinned by
a test that compares them with the field-free result). Functions added: 0;
removed: 0 (one pure helper added:
name_gap_notice). Rollback: revert.

VERSION 1.3.1 — exact scoped membership requires an independent complete A:A
read crossbound to the quality scan. Sparse tails beyond the audit bound fail
closed instead of certifying a partial window as the full approved membership.

VERSION 1.3.0 — standalone live audits require the approved current universe
policy. Market Leaders and Mutual Funds use its exact physical-symbol
membership, with explicit receipts for superseded historical/configured row
floors. Other coverage, acquisition and portfolio gates remain mandatory.

VERSION 1.2.2 — callers may supply explicit audited rules. The final publication
guard uses this to prevent environment settings weakening its acceptance floor.
Default standalone rules and exit-code behavior are unchanged.

VERSION 1.2.1 — fundamentals-only margin quarantine does not invalidate a
successful price acquisition. Its flag and fundamentals controls remain intact.

VERSION 1.2.0 — successful acquisitions and timestamp freshness are separate.
Valid price/provider, bounded retrieval time and unusable provenance are
checked per unique identity. Generated timestamps cannot green failed rows.
Physical row floors remain unchanged. Quote-asof is never inferred from the
retrieval stamp; legacy valid rows retain the established acquisition basis.

VERSION 1.1.0 (2026-09-08) — TIMEZONE TRUTH IN parse_dt (P-104)
WHY v1.1.0: parse_dt emitted THREE inconsistent bases: naive strings/serials
passed through as Riyadh wall time; "+HH:MM" ISO stamps were converted to
naive UTC (every downstream naive-Riyadh subtraction inflated their age by
exactly +3h — reproduced against the live _Status stamp); and "Z"-suffixed
stamps had the suffix blanket-stripped, so a UTC instant was silently read
as Riyadh (-3h). audit_grid additionally switched its `now0` basis on the
STAMP column header, which only ever accidentally matched. FIX: (1) new PURE
parse_dt_precision(v) -> (naive-Riyadh datetime | None, precision in
{"datetime","date","none"}) — aware inputs convert via astimezone(RIYADH);
"Z" is normalized to "+00:00", never stripped; naive inputs remain Riyadh
wall time; integer sheet serials and date-only formats report precision
"date" instead of fabricating midnight-as-instant. (2) parse_dt(v) is now a
thin wrapper returning only the datetime — same signature, uniform basis.
(3) audit_grid's now0 is unconditionally Riyadh-naive to match. Rules,
floors, coverage math, exit codes: UNTOUCHED.
"""
from __future__ import annotations
import argparse, asyncio, importlib, inspect, json, math, os, sys
from collections import Counter
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

VERSION = "1.3.2"
END_COL, DEFAULT_MAX_ROWS = "EZ", 20000
SYMBOL = ("Symbol", "Ticker")
NAME = ("Name", "Company Name", "Instrument Name")
PRICE = ("Current Price", "Price", "Last Price")
STAMP = ("Last Updated (Riyadh)", "Last Updated (UTC)", "Last Updated")
QTY = ("Position Qty", "Quantity", "Shares")
COST = ("Avg Cost", "Average Cost", "Buy Price")

for p in (Path(__file__).resolve().parent, Path(__file__).resolve().parent.parent):
    if str(p) not in sys.path: sys.path.insert(0, str(p))

from core.data_validity import (  # noqa: E402
    acquisition_census, coverage_validity, retrieval_timestamp, row_acquisition,
    timestamp_freshness,
)
from scripts.critical_symbol_identity import canonicalize_symbol  # noqa: E402
from scripts.approved_universe_scope import (  # noqa: E402
    ScopePolicy, approved_floors, load_scope, validate_symbols,
)

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
    timestamp_fresh: int = 0; timestamp_fresh_pct: Optional[float] = None
    unknown_acquisitions: int = 0; invalid_acquisitions: int = 0
    acquisition_reasons: dict[str, int] = field(default_factory=dict)
    quote_asof_known: int = 0
    price_pct: Optional[float] = None; newest_age_h: Optional[float] = None
    oldest_age_h: Optional[float] = None; duplicates: list[str] = field(default_factory=list)
    blank_symbols: int = 0; missing_portfolio: list[str] = field(default_factory=list)
    extra_portfolio: list[str] = field(default_factory=list)
    missing_qty: list[str] = field(default_factory=list); missing_cost: list[str] = field(default_factory=list)
    failures: list[str] = field(default_factory=list); warnings: list[str] = field(default_factory=list)
    approved_scope: dict = field(default_factory=dict)
    blank_name_count: int = 0
    blank_name_symbols: list[str] = field(default_factory=list)
    def finish(self):
        self.status = "FAIL" if self.failures else ("WARN" if self.warnings else "PASS"); return self

@dataclass
class Report:
    generated_at_utc: str; sheet: str; schema_version: str; active_holdings: list[str]
    pages: list[Result] = field(default_factory=list); fatal: str = ""
    approved_scope: dict = field(default_factory=dict)
    @property
    def code(self): return 3 if self.fatal else (2 if any(p.failures for p in self.pages) else (1 if any(p.warnings for p in self.pages) else 0))
    def payload(self):
        return {"script_version": VERSION, "generated_at_utc": self.generated_at_utc,
                "sheet": self.sheet, "schema_version": self.schema_version,
                "summary": {"pages": len(self.pages), "failures": sum(bool(p.failures) for p in self.pages),
                            "warnings": sum(bool(p.warnings) for p in self.pages), "exit_code": self.code, "fatal": self.fatal},
                "active_holdings": self.active_holdings, "approved_scope": self.approved_scope,
                "pages": [asdict(p) for p in self.pages]}

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

RIYADH_TZ = timezone(timedelta(hours=3))          # v1.1.0 (P-104)
_DATE_ONLY_FMTS = {"%Y-%m-%d", "%m/%d/%Y"}

def parse_dt_precision(v):
    """v1.1.0 PURE (P-104): (naive-RIYADH datetime | None, precision).

    precision: "datetime" | "date" | "none". One output basis for every
    input basis: aware -> astimezone(RIYADH) then strip tzinfo; "Z" is
    normalized to "+00:00" (never stripped); naive strings and sheet
    serials are Riyadh wall time as stored. An integer serial or a
    date-only format is a DATE, reported as such — midnight is never
    invented as an instant."""
    if isinstance(v, datetime):
        d = v.astimezone(RIYADH_TZ).replace(tzinfo=None) if v.tzinfo else v
        return d, "datetime"
    if isinstance(v,(int,float)) and not isinstance(v,bool) and 20000 < float(v) < 80000:
        x=float(v)
        return datetime(1899,12,30)+timedelta(days=x), ("date" if x==int(x) else "datetime")
    t=s(v).replace("T"," ")
    if not t: return None, "none"
    if t.endswith("Z"): t=t[:-1]+"+00:00"
    for fmt in ("%Y-%m-%d %H:%M:%S.%f","%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M","%Y-%m-%d","%m/%d/%Y %H:%M:%S","%m/%d/%Y"):
        try: return datetime.strptime(t,fmt), ("date" if fmt in _DATE_ONLY_FMTS else "datetime")
        except Exception: pass
    try:
        d=datetime.fromisoformat(t)
        if d.tzinfo: d=d.astimezone(RIYADH_TZ).replace(tzinfo=None)
        return d, ("datetime" if ":" in t else "date")
    except Exception: return None, "none"

def parse_dt(v):
    return parse_dt_precision(v)[0]

def resolve_reader():
    # Presentation formatting may hide a real intraday timestamp in _Status.
    # Only audit readers change; the normal production Sheets reader is untouched.
    from scripts.workflow_audit_support import resolve_audit_reader
    return resolve_audit_reader()


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

async def read_symbol_column(reader, sid, page):
    """A complete A:A read proves that no sparse tail symbol escaped the window."""
    loop=asyncio.get_running_loop()
    val=await loop.run_in_executor(None,lambda:reader(sid,f"{page}!A:A"))
    if inspect.isawaitable(val): val=await val
    if not isinstance(val,list): raise TypeError("complete Symbol column did not return a list")
    return val

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

def _strict_setting(name, default, *, minimum=None, maximum=None):
    value=env_f(name,default)
    if not math.isfinite(value) or value<=0: value=default
    if minimum is not None: value=max(minimum,value)
    if maximum is not None: value=min(maximum,value)
    return value

def rules(*, approved_scope: ScopePolicy | None=None):
    scope=approved_scope if approved_scope is not None else load_scope()
    current=approved_floors(policy=scope)
    def R(page,count,price=95,name=99):
        key=page.upper()
        floor=current[page] if page in current else math.ceil(_strict_setting(f"TFB_EXPECTED_MIN_ROWS_{key}",count,minimum=count))
        return Rule(page,floor,_strict_setting(f"TFB_REFRESH_MAX_AGE_H_{key}",30,maximum=30),_strict_setting(f"TFB_REFRESH_MIN_FRESH_PCT_{key}",95,minimum=95,maximum=100),name,price)
    return [R("Market_Leaders",current["Market_Leaders"]),R("Global_Markets",6512),R("Commodities_FX",453,95,95),R("Mutual_Funds",current["Mutual_Funds"],90,99),
            Rule("My_Portfolio",1,_strict_setting("TFB_REFRESH_MAX_AGE_H_MY_PORTFOLIO",8,maximum=8),100,100,100,True,True),
            Rule("Insights_Analysis",1,None,0,0,0,False),Rule("Data_Dictionary",1,None,0,0,0,False)]

def audit_grid(grid, rule, expected, now, active=(), *, approved_scope: ScopePolicy | None=None, source_column=None):
    if approved_scope is not None and rule.page in approved_scope.pages:
        rule=replace(rule,min_rows=approved_scope.pages[rule.page].approved_count)
    hr=header_row(grid,expected); r=Result(rule.page,min_rows=rule.min_rows,expected_cols=len(expected),header_row=hr+1 if hr>=0 else 0)
    if hr<0: r.failures.append("header row not found"); return r.finish()
    headers=trim(grid[hr]); r.actual_cols=len(headers)
    if headers != [s(x) for x in expected]: r.failures.append(f"header mismatch: expected {len(expected)}, found {len(headers)}")
    rows=[list(x) for x in grid[hr+1:] if has(x)]; r.rows=len(rows)
    if r.rows<rule.min_rows: r.failures.append(f"row count {r.rows} below minimum {rule.min_rows}")
    if not rule.symbols: return r.finish()
    si,ni,pi,ti,qi,ci=idx(headers,SYMBOL),idx(headers,NAME),idx(headers,PRICE),idx(headers,STAMP),idx(headers,QTY),idx(headers,COST)
    if si<0: r.failures.append("Symbol column missing"); return r.finish()
    if approved_scope is not None and rule.page in approved_scope.pages:
        wide_validation=validate_symbols(rule.page,[row[si] if si<len(row) else None for row in rows],policy=approved_scope)
        wide_receipt=wide_validation.to_dict()
        r.approved_scope=wide_receipt.copy()
        proof={"range":f"{rule.page}!A:A","bound":DEFAULT_MAX_ROWS+50,"valid":False,"crossbound":False,
               "returned_rows":len(source_column) if isinstance(source_column,list) else None,
               "header_row":hr+1,"symbol_column":1}
        try:
            if si!=0 or sum(s(h).casefold()=="symbol" for h in headers)!=1:
                raise ValueError("approved universe requires exactly one physical Symbol column in A")
            if not isinstance(source_column,list):
                raise ValueError("complete Symbol column was not supplied")
            if len(source_column)>DEFAULT_MAX_ROWS+50 or len(grid)>DEFAULT_MAX_ROWS+50:
                raise ValueError("complete Symbol column exceeds the 20,050-row audit bound")
            if any(not isinstance(row,(list,tuple)) or len(row)>1 for row in source_column):
                raise ValueError("complete Symbol column has malformed or extra-column rows")
            column_headers=[i for i,row in enumerate(source_column[:45]) if row and s(row[0]).casefold()=="symbol"]
            if column_headers!=[hr]:
                raise ValueError("complete and bounded Symbol headers are ambiguous or do not match")
            column_symbols=[row[0] for row in source_column[hr+1:] if row and s(row[0])]
            full_validation=validate_symbols(rule.page,column_symbols,policy=approved_scope)
            r.approved_scope=full_validation.to_dict()
            proof["observed_symbol_set_sha256"]=r.approved_scope["observed_symbol_set_sha256"]
            proof["actual_membership_sha256"]=r.approved_scope["actual_membership_sha256"]
            proof["bounded_symbol_set_sha256"]=wide_receipt["observed_symbol_set_sha256"]
            proof["bounded_membership_sha256"]=wide_receipt["actual_membership_sha256"]
            if not full_validation.valid or not wide_validation.valid:
                raise ValueError("complete and bounded memberships must both match the approved scope")
            extent=max(len(grid),len(source_column))
            for position in range(hr+1,extent):
                wide=s(grid[position][0] if position<len(grid) and grid[position] else None).upper()
                full=s(source_column[position][0] if position<len(source_column) and source_column[position] else None).upper()
                if wide!=full:
                    raise ValueError("complete and bounded Symbol columns changed between reads")
            proof.update(valid=True,crossbound=True)
        except Exception as exc:
            proof["error"]=f"{type(exc).__name__}: {exc}"
            r.failures.append("complete approved universe membership unproven: "+proof["error"])
        r.approved_scope["complete_column_proof"]=proof
        r.approved_scope["valid"]=r.approved_scope["valid"] is True and proof["valid"] is True
        setting="TFB_EXPECTED_MIN_ROWS_"+rule.page.upper()
        r.approved_scope["superseded_configured_floor"]={"environment":setting,"value":os.getenv(setting),
            "legacy_effective_min_rows":env_i(setting,approved_scope.pages[rule.page].supersedes_min_rows)}
        r.approved_scope["applied_min_rows"]=rule.min_rows
        if not wide_validation.valid:
            r.failures.append("approved universe membership failed: "
                f"missing={wide_receipt['missing_count']}, unexpected={wide_receipt['unexpected_count']}, "
                f"duplicates={wide_receipt['duplicate_count']}, invalid={wide_receipt['bad_symbol_count']}")
    symbols=[]; names=prices=0; ages=[]; qmap={}; cmap={}
    timestamp_symbols=set(); bad_symbols=set(); quote_symbols=set(); blank_names=set()
    for row in rows:
        sym=canonicalize_symbol(s(row[si] if si<len(row) else "").upper())
        if not sym: r.blank_symbols+=1; continue
        symbols.append(sym); names+=int(ni>=0 and ni<len(row) and bool(s(row[ni]))); prices+=int(pi>=0 and pi<len(row) and (f(row[pi]) or 0)>0)
        if ni>=0 and not (ni<len(row) and s(row[ni])): blank_names.add(sym)
        if qi>=0: qmap[sym]=f(row[qi] if qi<len(row) else None)
        if ci>=0: cmap[sym]=f(row[ci] if ci<len(row) else None)
        if rule.max_age_h is not None:
            mapped={name: row[i] if i<len(row) else None for i,name in enumerate(headers)}
            stamp,precision=retrieval_timestamp(mapped)
            timely,age,_reason=timestamp_freshness(stamp,now,max_age_seconds=rule.max_age_h*3600,precision=precision)
            if timely: timestamp_symbols.add(sym)
            if stamp is None or precision!="datetime": bad_symbols.add(sym)
            if age is not None: ages.append(age/3600)
            if row_acquisition(mapped,now,rule.max_age_h*3600).quote_asof is not None: quote_symbols.add(sym)
    c=Counter(symbols); r.unique=len(c); r.duplicates=sorted(k for k,v in c.items() if v>1)
    if r.blank_symbols: r.failures.append(f"{r.blank_symbols} blank-symbol row(s)")
    if r.duplicates: r.failures.append("duplicate symbols: "+", ".join(r.duplicates[:20]))
    r.name_pct=pct(names,len(symbols)) if ni>=0 else None; r.price_pct=pct(prices,len(symbols)) if pi>=0 else None
    if ni>=0 and rule.min_name and not rule.portfolio:  # v1.3.2 diagnostic only
        r.blank_name_count=len(blank_names); r.blank_name_symbols=sorted(blank_names)[:200]
    if ni<0 and rule.min_name: r.failures.append("Name column missing")
    elif r.name_pct is not None and r.name_pct<rule.min_name: r.failures.append(f"name coverage {r.name_pct:.2f}% below {rule.min_name:.2f}%")
    if pi<0 and rule.min_price: r.failures.append("Current Price column missing")
    elif r.price_pct is not None and r.price_pct<rule.min_price: r.failures.append(f"price coverage {r.price_pct:.2f}% below {rule.min_price:.2f}%")
    if rule.max_age_h is not None:
        census=acquisition_census(headers,rows,now=now,max_age_seconds=rule.max_age_h*3600,symbol_key=canonicalize_symbol)
        r.fresh=len(census.successful); r.stale=r.unique-r.fresh; r.bad_stamps=len(bad_symbols)
        r.fresh_pct=pct(r.fresh,r.unique); r.timestamp_fresh=len(census.timestamp_fresh); r.timestamp_fresh_pct=pct(r.timestamp_fresh,r.unique)
        r.unknown_acquisitions=len(census.unknown); r.invalid_acquisitions=len(census.invalid); r.acquisition_reasons=census.reason_counts
        r.quote_asof_known=len(census.quote_asof_known)
        r.newest_age_h=min(ages) if ages else None; r.oldest_age_h=max(ages) if ages else None
        if ti<0: r.failures.append("Last Updated column missing")
        if not coverage_validity(r.unique,r.fresh,rule.min_fresh).valid: r.failures.append(f"successful acquisition coverage {r.fresh_pct} below {rule.min_fresh}% within {rule.max_age_h}h")
        if r.bad_stamps: r.warnings.append(f"{r.bad_stamps} blank/unparseable/date-only timestamp(s)")
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

async def run(sid, limit=DEFAULT_MAX_ROWS, reader=None, registry=None, now=None, *, policy_rules=None, approved_scope: ScopePolicy | None=None):
    now=now or datetime.now(timezone.utc)
    rep=Report(now.isoformat(),sid[:5]+"..."+sid[-5:] if len(sid)>10 else "***","unknown",[])
    try:
        scope=approved_scope if approved_scope is not None else (load_scope() if policy_rules is None else None)
        selected=rules(approved_scope=scope) if policy_rules is None else policy_rules
        if scope is not None: rep.approved_scope=scope.receipt()
    except Exception as e:
        rep.fatal=f"approved universe policy unavailable: {e}"; return rep
    reader,registry=reader or resolve_reader(),registry or resolve_registry()
    rep.schema_version=s(getattr(registry,"SCHEMA_VERSION",getattr(registry,"__version__","unknown"))) if registry else "unknown"
    if not sid: rep.fatal="spreadsheet ID missing"; return rep
    if not reader: rep.fatal="read_range unavailable"; return rep
    if not registry: rep.fatal="schema_registry unavailable"; return rep
    try: rep.active_holdings,warn=ledger_symbols(await read(reader,sid,"_Portfolio_CostBasis",limit))
    except Exception as e: warn=[f"ledger unreadable: {e}"]
    for rule in selected:
        try:
            grid=await read(reader,sid,rule.page,limit)
            column=await read_symbol_column(reader,sid,rule.page) if scope is not None and rule.page in scope.pages else None
            out=audit_grid(grid,rule,list(registry.get_sheet_headers(rule.page)),now,rep.active_holdings,approved_scope=scope,source_column=column)
            if rule.portfolio and warn: out.warnings+=warn; out.failures+=(["active ledger universe not proven"] if not rep.active_holdings else []); out.finish()
            rep.pages.append(out)
        except Exception as e: rep.pages.append(Result(rule.page,status="FAIL",min_rows=rule.min_rows,failures=[f"audit failed: {e}"]))
    return rep

def name_gap_notice(page: dict) -> str:
    """v1.3.2 PURE: the one name-gap ::notice line for a page result dict
    (asdict(Result) or the same dict inside a JSON payload). No braces."""
    syms=[str(x) for x in (page.get("blank_name_symbols") or [])]; n=int(page.get("blank_name_count") or 0)
    return (f"::notice::{page.get('page')}: blank-name symbols {n} (name_pct={page.get('name_pct')}): "
            f"{', '.join(syms[:60])}{' ...' if n>60 else ''}")

def main(argv=None):
    ap=argparse.ArgumentParser(description=__doc__); ap.add_argument("--sheet-id",default=os.getenv("DEFAULT_SPREADSHEET_ID", "")); ap.add_argument("--max-rows",type=int,default=env_i("TFB_REFRESH_AUDIT_MAX_ROWS",DEFAULT_MAX_ROWS)); ap.add_argument("--json-out",default=""); a=ap.parse_args(argv)
    rep=asyncio.run(run(a.sheet_id,max(100,a.max_rows))); text=json.dumps(rep.payload(),ensure_ascii=False,indent=2); print(text)
    for p in rep.pages: print(f"::{'error' if p.failures else ('warning' if p.warnings else 'notice')}::{p.page}: {p.status}, rows={p.rows}, unique={p.unique}, fresh_pct={p.fresh_pct}, failures={'; '.join(p.failures[:3]) or 'none'}")
    for p in rep.pages:
        if p.blank_name_count: print(name_gap_notice(asdict(p)))
    if rep.fatal: print(f"::error::FULL_REFRESH_AUDIT_FATAL: {rep.fatal}")
    if a.json_out: Path(a.json_out).write_text(text+"\n",encoding="utf-8")
    return rep.code

if __name__ == "__main__": raise SystemExit(main())
