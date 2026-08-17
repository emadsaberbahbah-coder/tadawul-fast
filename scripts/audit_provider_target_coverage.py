#!/usr/bin/env python3
"""Read-only audit of forecast provenance coverage across the market pages.

WHY (W1B-1, evidence 2026-08-16 / 2026-08-17)
---------------------------------------------
The engine derives ``forecast_source`` from scratch on every fetch: a row
whose analyst-target leg fails is silently re-stamped ``phase_ii_synthetic``
and reads as a complete, confident row.  On 2026-08-17 the analyst-target
feed suffered a throttling outage that outlasted the price feed's recovery
by 2-3 hours; Global_Markets provider-target share fell 27.5% -> 10.7%
(count 1,823 -> 711).  Six portfolio holdings flipped verdict at
byte-identical prices, in the mirror direction of the 2026-08-16 event.
Nothing in the system announced either event.

v1.1.0 (external audit 2026-08-17, ChatGPT P0-1..P0-4 all confirmed by
reproduction and ACCEPTED)
--------------------------------------------------------------------------
1. LAST-GOOD BASELINE LIFECYCLE.  The reference baseline is the last
   ACCEPTED HEALTHY observation, never merely the previous run.  It advances
   automatically only UPWARD (share >= prior) on a fully healthy run;
   a lower structural baseline requires explicit operator acceptance
   (``--accept-baseline`` / TFB_PTC_ACCEPT_BASELINE=1, one-shot).  A
   COLLAPSE therefore stays red every day until recovery or acceptance,
   and gradual downward drift accumulates against the true reference.
2. CONTROL-HEALTH FAILS CLOSED (exit 2).  A required page that cannot be
   read, has no measurement contract, or truncates at the read cap; a
   scheduled run whose expected baseline is missing, corrupt, stale,
   schema-incompatible, or for a different sheet; invalid critical config
   -- all are CONTROL findings.  A monitor that cannot measure the page it
   protects is never green.
3. EXPLICIT BOOTSTRAP.  First deployment runs once with ``--bootstrap``
   (or the workflow input) after a verified healthy sync; a scheduled run
   with no baseline is a control-health failure, not a silent re-baseline.
4. PER-PAGE POLICY.  Structural-zero pages (default Commodities_FX,
   Mutual_Funds -- no analyst coverage exists for FX pairs or funds under
   the current model) are exempt from any positive floor by policy, not by
   comment.  Row-count collapse is detected independently of share so a
   page that loses most of its rows cannot pass on an unchanged ratio.
5. SOURCE CLASSIFICATION.  provider_target / known synthetic
   (phase_ii_synthetic, fallback) / missing / UNKNOWN are counted
   separately; a material unknown share warns instead of hiding inside
   "synthetic".

CONTRACT
--------
Read-only.  No provider call, no Google Sheet write, no recommendation
surface: this control cannot change a ticket.  It ALERTS; prevention of
provenance-driven verdict flips is the R-6 keep-last-good layer
(target_keeper.py), by design.

Exit codes: 0 healthy / bootstrap / structural-zero; 1 coverage failure
(collapse, floor breach, row collapse); 2 control-health failure.

State files:
  provider_target_last_good.json    reference baseline (eligibility-gated)
  provider_target_observation.json  latest measured facts (always written)
  provider_target_coverage.json     full report (always written)

Env (invalid explicit values are control-health failures, not defaults):
  TFB_PTC_DROP_PCT            relative drop vs last-good that FAILs (20)
  TFB_PTC_ROW_DROP_PCT        relative row-count drop that FAILs (30)
  TFB_PTC_FLOOR_PCT           absolute share floor, 0 = disabled (0)
  TFB_PTC_MIN_ROWS            pages smaller than this are not judged (50)
  TFB_PTC_BASELINE_MAX_AGE_H  scheduled baseline max age hours (72)
  TFB_PTC_ZERO_PAGES          structural-zero page list
  TFB_PTC_PAGES               page list override
  TFB_PTC_BOOTSTRAP / TFB_PTC_ACCEPT_BASELINE / TFB_PTC_FREEZE  one-shots
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import inspect
import json
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional, Sequence

for _path in (Path(__file__).resolve().parent, Path(__file__).resolve().parent.parent):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from scripts.audit_full_refresh_coverage import resolve_reader, s  # noqa: E402

VERSION = "1.1.0"
STATE_SCHEMA = 2
DEFAULT_PAGES = ("Market_Leaders", "Global_Markets", "Commodities_FX", "Mutual_Funds")
DEFAULT_ZERO_PAGES = ("Commodities_FX", "Mutual_Funds")
PROVIDER_TOKEN = "provider_target"
KNOWN_SYNTHETIC = {"phase_ii_synthetic", "fallback"}
SOURCE_ALIASES = {"forecastsource", "forecast source", "forecast_source", "forecastbasis"}
SYMBOL_ALIASES = {"symbol", "ticker", "code", "instrument"}
END_COL = "DZ"
MAX_ROWS = 12000
UNKNOWN_WARN_PCT = 1.0  # unknown-source share above this warns


# ----------------------------------------------------------------------------
# Config (explicit invalid values are CONTROL failures — never silent defaults)
# ----------------------------------------------------------------------------
@dataclass
class Config:
    drop_pct: float = 20.0
    row_drop_pct: float = 30.0
    floor_pct: float = 0.0
    min_rows: int = 50
    baseline_max_age_h: float = 72.0
    zero_pages: tuple = DEFAULT_ZERO_PAGES
    errors: list = field(default_factory=list)

    def hash(self) -> str:
        blob = json.dumps({
            "drop": self.drop_pct, "row_drop": self.row_drop_pct,
            "floor": self.floor_pct, "min_rows": self.min_rows,
            "zero": sorted(self.zero_pages)}, sort_keys=True)
        return hashlib.sha256(blob.encode()).hexdigest()[:12]


def _read_num(cfg: Config, name: str, default: float, lo: float, hi: float,
              as_int: bool = False):
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return int(default) if as_int else default
    try:
        v = float(raw)
    except Exception:
        cfg.errors.append(f"{name}={raw!r} is not numeric")
        return int(default) if as_int else default
    if not (lo <= v <= hi):
        cfg.errors.append(f"{name}={v} outside [{lo}, {hi}]")
        return int(default) if as_int else default
    return int(v) if as_int else v


def load_config() -> Config:
    cfg = Config()
    cfg.drop_pct = _read_num(cfg, "TFB_PTC_DROP_PCT", 20.0, 0.1, 100.0)
    cfg.row_drop_pct = _read_num(cfg, "TFB_PTC_ROW_DROP_PCT", 30.0, 0.1, 100.0)
    cfg.floor_pct = _read_num(cfg, "TFB_PTC_FLOOR_PCT", 0.0, 0.0, 99.9)
    cfg.min_rows = _read_num(cfg, "TFB_PTC_MIN_ROWS", 50, 1, 100000, as_int=True)
    cfg.baseline_max_age_h = _read_num(cfg, "TFB_PTC_BASELINE_MAX_AGE_H", 72.0, 1.0, 8760.0)
    zp = (os.getenv("TFB_PTC_ZERO_PAGES") or "").strip()
    cfg.zero_pages = tuple(x.strip() for x in zp.split(",") if x.strip()) or DEFAULT_ZERO_PAGES
    return cfg


def _flag(*names: str) -> bool:
    return any((os.getenv(n) or "").strip().lower() in ("1", "true", "yes", "on")
               for n in names)


def _norm(v: Any) -> str:
    return s(v).strip().casefold()


# ----------------------------------------------------------------------------
# Measurement
# ----------------------------------------------------------------------------
@dataclass
class PageCoverage:
    page: str
    rows: int = 0
    provider: int = 0
    synthetic: int = 0
    missing: int = 0
    unknown: int = 0
    unknown_tokens: list = field(default_factory=list)
    truncated: bool = False
    read_error: str = ""
    share_pct: Optional[float] = None
    baseline_pct: Optional[float] = None
    baseline_rows: Optional[int] = None
    drop_pct: Optional[float] = None
    row_drop_pct: Optional[float] = None
    verdict: str = "UNKNOWN"
    detail: str = ""


@dataclass
class Finding:
    severity: str  # CONTROL | FAIL | WARN
    code: str
    page: str
    message: str


@dataclass
class CoverageReport:
    generated_at_utc: str
    spreadsheet: str
    version: str = VERSION
    config: dict = field(default_factory=dict)
    mode: str = "scheduled"  # scheduled | bootstrap | accept
    pages: dict[str, dict[str, Any]] = field(default_factory=dict)
    findings: list[Finding] = field(default_factory=list)
    baseline_at_utc: Optional[str] = None
    baseline_age_h: Optional[float] = None
    baseline_updated: bool = False
    baseline_update_detail: str = ""
    persist_status: str = "pending"
    fatal: Optional[str] = None

    @property
    def exit_code(self) -> int:
        if self.fatal:
            return 2
        if any(f.severity == "CONTROL" for f in self.findings):
            return 2
        if any(f.severity == "FAIL" for f in self.findings):
            return 1
        return 0

    def payload(self) -> dict[str, Any]:
        out = asdict(self)
        out["exit_code"] = self.exit_code
        return out


def _find_col(header: Sequence[Any], aliases: set) -> int:
    for i, h in enumerate(header or []):
        if _norm(h) in aliases:
            return i
    return -1


def _header_index(grid: Sequence[Sequence[Any]]) -> int:
    for i, row in enumerate(grid[:45]):
        if _find_col(row, SYMBOL_ALIASES) >= 0 and _find_col(row, SOURCE_ALIASES) >= 0:
            return i
    return -1


def measure_page(page: str, grid: Sequence[Sequence[Any]],
                 read_error: str = "") -> PageCoverage:
    """Count forecast provenance on one page grid. Pure; never raises."""
    cov = PageCoverage(page=page, read_error=read_error)
    if read_error:
        cov.verdict = "READ_FAIL"
        cov.detail = read_error
        return cov
    if not grid:
        cov.verdict = "NO_DATA"
        cov.detail = "empty grid"
        return cov
    if len(grid) >= MAX_ROWS:
        cov.truncated = True
    hi = _header_index(grid)
    if hi < 0:
        cov.verdict = "NO_CONTRACT"
        cov.detail = "no header row with Symbol + Forecast Source"
        return cov
    header = grid[hi]
    src_i = _find_col(header, SOURCE_ALIASES)
    sym_i = _find_col(header, SYMBOL_ALIASES)
    for row in grid[hi + 1:]:
        if not isinstance(row, (list, tuple)):
            continue
        if sym_i >= len(row) or not s(row[sym_i]):
            continue
        cov.rows += 1
        val = _norm(row[src_i]) if src_i < len(row) else ""
        if val == PROVIDER_TOKEN:
            cov.provider += 1
        elif val in KNOWN_SYNTHETIC:
            cov.synthetic += 1
        elif not val:
            cov.missing += 1
        else:
            cov.unknown += 1
            if val not in cov.unknown_tokens and len(cov.unknown_tokens) < 8:
                cov.unknown_tokens.append(val)
    if cov.rows:
        cov.share_pct = round(100.0 * cov.provider / cov.rows, 2)
    return cov


# ----------------------------------------------------------------------------
# Judgment
# ----------------------------------------------------------------------------
def judge(cov: PageCoverage, prior: Optional[dict], cfg: Config,
          bootstrap: bool) -> list[Finding]:
    """Compare one page against the last-good baseline entry. Pure."""
    out: list[Finding] = []
    structural_zero = cov.page in cfg.zero_pages

    # -- control health of the measurement itself -----------------------------
    if cov.verdict == "READ_FAIL":
        out.append(Finding("CONTROL", "CH_READ_FAIL", cov.page,
                           f"{cov.page}: required page unreadable ({cov.read_error}) — "
                           f"a monitor that cannot read the page it protects is never green"))
        return out
    if cov.verdict in ("NO_DATA", "NO_CONTRACT"):
        out.append(Finding("CONTROL", "CH_NO_CONTRACT", cov.page,
                           f"{cov.page}: {cov.detail} — measurement contract broken"))
        return out
    if cov.truncated:
        out.append(Finding("CONTROL", "CH_TRUNCATED", cov.page,
                           f"{cov.page}: read hit the {MAX_ROWS}-row cap; measured share "
                           f"may be biased — raise the cap before judging"))
        return out
    if cov.share_pct is None:
        out.append(Finding("CONTROL", "CH_NO_ROWS", cov.page,
                           f"{cov.page}: zero measurable rows on a required page"))
        return out

    prior_share = prior.get("share_pct") if isinstance(prior, dict) else None
    prior_rows = prior.get("rows") if isinstance(prior, dict) else None
    try:
        prior_share = float(prior_share) if prior_share is not None else None
    except Exception:
        prior_share = None
    try:
        prior_rows = int(prior_rows) if prior_rows is not None else None
    except Exception:
        prior_rows = None
    cov.baseline_pct = prior_share
    cov.baseline_rows = prior_rows

    if cov.rows < cfg.min_rows:
        cov.verdict = "TOO_SMALL"
        cov.detail = f"{cov.rows} rows < min {cfg.min_rows}; reported, not judged"
        return out

    # -- unknown provenance vocabulary ---------------------------------------
    if cov.rows and 100.0 * cov.unknown / cov.rows > UNKNOWN_WARN_PCT:
        out.append(Finding("WARN", "PTC_UNKNOWN_SOURCE", cov.page,
                           f"{cov.page}: {cov.unknown} rows carry unknown forecast_source "
                           f"tokens {cov.unknown_tokens} — vocabulary drift, classify before trusting"))

    # -- row-count collapse (independent of share) ---------------------------
    if prior_rows and prior_rows >= cfg.min_rows:
        rdrop = round(100.0 * (prior_rows - cov.rows) / prior_rows, 2)
        cov.row_drop_pct = rdrop
        if rdrop >= cfg.row_drop_pct:
            cov.verdict = "ROW_COLLAPSE"
            out.append(Finding("FAIL", "PTC_ROW_COLLAPSE", cov.page,
                               f"{cov.page}: row count {prior_rows} -> {cov.rows} "
                               f"({rdrop:.1f}% drop >= {cfg.row_drop_pct:.1f}%) — page shrank "
                               f"materially even if share held"))
            return out

    # -- structural-zero pages: policy exemption from floor AND drop ---------
    if structural_zero:
        cov.verdict = "STRUCTURAL_ZERO"
        cov.detail = "page declared structural-zero by policy; floor and drop tests not applicable"
        if cov.share_pct > 0 and (prior_share is None or prior_share == 0):
            out.append(Finding("WARN", "PTC_ZERO_PAGE_NONZERO", cov.page,
                               f"{cov.page}: declared structural-zero but measured "
                               f"{cov.share_pct:.1f}% provider share — revisit the policy"))
        return out

    # -- absolute floor -------------------------------------------------------
    if cfg.floor_pct > 0.0 and cov.share_pct < cfg.floor_pct:
        cov.verdict = "BELOW_FLOOR"
        out.append(Finding("FAIL", "PTC_BELOW_FLOOR", cov.page,
                           f"{cov.page}: provider-target share {cov.share_pct:.1f}% below "
                           f"floor {cfg.floor_pct:.1f}%"))
        return out

    # -- baseline comparison ---------------------------------------------------
    if prior_share is None:
        cov.verdict = "BOOTSTRAP" if bootstrap else "NO_BASELINE"
        cov.detail = ("bootstrap run; baseline will be recorded" if bootstrap
                      else "no baseline entry for this page")
        if not bootstrap:
            out.append(Finding("CONTROL", "CH_BASELINE_MISSING", cov.page,
                               f"{cov.page}: scheduled run with no baseline entry — "
                               f"bootstrap explicitly after a verified healthy sync"))
        return out
    if prior_share <= 0.0:
        cov.verdict = "STRUCTURAL_ZERO"
        cov.detail = "baseline 0% — drop test not applicable"
        return out

    drop = round(100.0 * (prior_share - cov.share_pct) / prior_share, 2)
    cov.drop_pct = drop
    if drop >= cfg.drop_pct:
        cov.verdict = "COLLAPSE"
        out.append(Finding("FAIL", "PTC_SHARE_COLLAPSE", cov.page,
                           f"{cov.page}: provider-target share {prior_share:.1f}% -> "
                           f"{cov.share_pct:.1f}% ({drop:.1f}% relative drop >= "
                           f"{cfg.drop_pct:.1f}% vs last accepted healthy baseline). Verdicts "
                           f"on this page may be forecast-provenance artifacts, not signals — "
                           f"treat SELL/REDUCE moves as unverified until the target leg recovers "
                           f"or the operator accepts a new baseline."))
    elif drop >= cfg.drop_pct / 2.0:
        cov.verdict = "DEGRADED"
        out.append(Finding("WARN", "PTC_SHARE_DEGRADED", cov.page,
                           f"{cov.page}: provider-target share {prior_share:.1f}% -> "
                           f"{cov.share_pct:.1f}% ({drop:.1f}% relative drop; baseline held)"))
    else:
        cov.verdict = "OK"
    return out


# ----------------------------------------------------------------------------
# Baseline lifecycle
# ----------------------------------------------------------------------------
def validate_baseline(state: Optional[dict], sheet_masked: str, cfg: Config,
                      bootstrap: bool, now: datetime) -> tuple[dict, list[Finding], Optional[float]]:
    """Return (pages_map, findings, age_h). Missing/corrupt/stale/mismatched
    baselines are CONTROL failures on scheduled runs; bootstrap tolerates
    absence (and only absence)."""
    findings: list[Finding] = []
    if state is None:
        if bootstrap:
            return {}, findings, None
        findings.append(Finding("CONTROL", "CH_BASELINE_MISSING", "*",
                                "expected baseline is missing (first run, cache eviction, or "
                                "corrupt file) — scheduled runs must not silently re-baseline; "
                                "run once with --bootstrap after a verified healthy sync"))
        return {}, findings, None
    if state.get("schema") != STATE_SCHEMA:
        findings.append(Finding("CONTROL", "CH_BASELINE_MISMATCH", "*",
                                f"baseline schema {state.get('schema')!r} != {STATE_SCHEMA} — "
                                f"re-bootstrap explicitly"))
        return {}, findings, None
    if state.get("sheet") and sheet_masked != "***" and state.get("sheet") != sheet_masked:
        findings.append(Finding("CONTROL", "CH_BASELINE_MISMATCH", "*",
                                f"baseline belongs to sheet {state.get('sheet')} not "
                                f"{sheet_masked} — refusing to compare"))
        return {}, findings, None
    age_h = None
    ts = state.get("generated_at_utc")
    try:
        prev = datetime.fromisoformat(str(ts))
        if prev.tzinfo is None:
            prev = prev.replace(tzinfo=timezone.utc)
        age_h = round((now - prev).total_seconds() / 3600.0, 1)
    except Exception:
        findings.append(Finding("CONTROL", "CH_BASELINE_MISMATCH", "*",
                                f"baseline timestamp unreadable: {ts!r}"))
        return {}, findings, age_h
    if age_h is not None and age_h > cfg.baseline_max_age_h and not bootstrap:
        findings.append(Finding("CONTROL", "CH_BASELINE_STALE", "*",
                                f"baseline is {age_h:.1f}h old > max {cfg.baseline_max_age_h:.0f}h — "
                                f"the reference is not the prior healthy run; re-bootstrap or "
                                f"investigate the gap"))
        return {}, findings, age_h
    pages = state.get("pages")
    return (pages if isinstance(pages, dict) else {}), findings, age_h


def next_last_good(report: CoverageReport, prior_pages: dict, accept: bool) -> tuple[dict, str]:
    """Eligibility-gated baseline advance. Pure.

    Rules (external audit 2026-08-17, accepted): never advance on CONTROL or
    FAIL; never ratchet DOWN automatically (WARN/DEGRADED and even sub-WARN
    dips keep the old reference); advance UP or equal automatically on a
    fully healthy run; ``accept`` overrides downward for every measurable
    page (explicit operator decision, one-shot)."""
    if report.exit_code == 2:
        return prior_pages, "kept: control-health failure"
    new = dict(prior_pages)
    changed = []
    for page, cov in report.pages.items():
        share = cov.get("share_pct")
        if share is None:
            continue
        entry = {"share_pct": share, "rows": cov.get("rows"),
                 "provider": cov.get("provider")}
        prior = prior_pages.get(page) or {}
        prior_share = prior.get("share_pct")
        if accept:
            new[page] = entry
            changed.append(f"{page}(accepted {share}%)")
            continue
        if report.exit_code == 1:
            continue  # never promote any page from a failing run
        if cov.get("verdict") in ("OK", "STRUCTURAL_ZERO", "BOOTSTRAP", "TOO_SMALL"):
            if prior_share is None or float(share) >= float(prior_share):
                new[page] = entry
                if prior_share is None or float(share) > float(prior_share):
                    changed.append(f"{page}({prior_share}->{share}%)")
    if accept:
        return new, "operator acceptance: " + (", ".join(changed) or "no measurable pages")
    if report.exit_code == 1:
        return prior_pages, "kept: coverage failure — reference is the last healthy run"
    return new, (("advanced: " + ", ".join(changed)) if changed else "unchanged")


# ----------------------------------------------------------------------------
# Aggregation
# ----------------------------------------------------------------------------
def audit_pages(grids: dict[str, Any], baseline_state: Optional[dict],
                cfg: Config, spreadsheet: str = "***", bootstrap: bool = False,
                accept: bool = False,
                read_errors: Optional[dict] = None) -> CoverageReport:
    """Pure core: grids in, report (with next-baseline decision) out."""
    now = datetime.now(timezone.utc)
    rep = CoverageReport(generated_at_utc=now.isoformat(), spreadsheet=spreadsheet,
                         config={"drop_pct": cfg.drop_pct, "row_drop_pct": cfg.row_drop_pct,
                                 "floor_pct": cfg.floor_pct, "min_rows": cfg.min_rows,
                                 "baseline_max_age_h": cfg.baseline_max_age_h,
                                 "zero_pages": list(cfg.zero_pages),
                                 "config_hash": cfg.hash()},
                         mode="accept" if accept else ("bootstrap" if bootstrap else "scheduled"))
    if cfg.errors:
        for e in cfg.errors:
            rep.findings.append(Finding("CONTROL", "CH_CONFIG_INVALID", "*",
                                        f"invalid critical config: {e}"))
    prior_pages, base_findings, age_h = validate_baseline(
        baseline_state, spreadsheet, cfg, bootstrap, now)
    rep.findings.extend(base_findings)
    rep.baseline_at_utc = (baseline_state or {}).get("generated_at_utc")
    rep.baseline_age_h = age_h
    read_errors = read_errors or {}
    for page, grid in grids.items():
        cov = measure_page(page, grid, read_error=read_errors.get(page, ""))
        rep.findings.extend(judge(cov, prior_pages.get(page), cfg, bootstrap))
        rep.pages[page] = asdict(cov)
    new_pages, detail = next_last_good(rep, prior_pages, accept)
    rep.baseline_updated = new_pages != prior_pages
    rep.baseline_update_detail = detail if isinstance(detail, str) else "unchanged"
    rep._next_pages = new_pages  # type: ignore[attr-defined]
    return rep


# ----------------------------------------------------------------------------
# I/O
# ----------------------------------------------------------------------------
async def _read(reader: Callable[..., Any], sid: str, page: str) -> list:
    loop = asyncio.get_running_loop()
    a1 = f"{page}!A1:{END_COL}{MAX_ROWS}"
    val = await loop.run_in_executor(None, lambda: reader(sid, a1))
    if inspect.isawaitable(val):
        val = await val
    if not isinstance(val, list):
        raise TypeError(f"read_range for {a1} did not return a list")
    return [list(r) if isinstance(r, (list, tuple)) else [r] for r in val]


async def run_live(spreadsheet_id: str, pages: Sequence[str], baseline_state: Optional[dict],
                   cfg: Config, bootstrap: bool, accept: bool, reader=None) -> CoverageReport:
    masked = (spreadsheet_id[:5] + "..." + spreadsheet_id[-5:]
              if len(spreadsheet_id) > 10 else "***")
    if not spreadsheet_id:
        rep = CoverageReport(datetime.now(timezone.utc).isoformat(), "***")
        rep.fatal = "spreadsheet ID missing"
        return rep
    reader = reader or resolve_reader()
    if not reader:
        rep = CoverageReport(datetime.now(timezone.utc).isoformat(), masked)
        rep.fatal = "read_range unavailable"
        return rep
    grids: dict[str, list] = {}
    errors: dict[str, str] = {}
    for page in pages:
        try:
            grids[page] = await _read(reader, spreadsheet_id, page)
        except Exception as exc:
            grids[page] = []
            errors[page] = f"{type(exc).__name__}: {exc}"
    return audit_pages(grids, baseline_state, cfg, spreadsheet=masked,
                       bootstrap=bootstrap, accept=accept, read_errors=errors)


def load_state(path: str) -> tuple[Optional[dict], Optional[str]]:
    """Return (state, error). Distinguishes ABSENT (None, None) from
    CORRUPT (None, message) — a corrupt file is a control event, not a
    quiet bootstrap."""
    if not path:
        return None, None
    p = Path(path)
    if not p.is_file():
        return None, None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        return None, f"baseline file unreadable: {type(exc).__name__}: {exc}"
    if not isinstance(data, dict):
        return None, "baseline file is not a JSON object"
    return data, None


def save_last_good(path: str, rep: CoverageReport, sheet_masked: str) -> str:
    """Write the eligibility-gated reference. Returns persist status."""
    if not path:
        return "disabled"
    if rep.fatal:
        return "kept: fatal"
    if _flag("TFB_PTC_FREEZE"):
        return "kept: frozen by TFB_PTC_FREEZE"
    pages = getattr(rep, "_next_pages", None)
    if not isinstance(pages, dict):
        return "kept: no eligible update"
    if not rep.baseline_updated and Path(path).is_file():
        return "unchanged"
    blob = {"schema": STATE_SCHEMA, "generated_at_utc": rep.generated_at_utc,
            "version": VERSION, "sheet": sheet_masked,
            "config_hash": rep.config.get("config_hash"), "pages": pages}
    tmp = Path(path + ".tmp")
    try:
        tmp.write_text(json.dumps(blob, ensure_ascii=False, indent=2) + "\n",
                       encoding="utf-8")
        tmp.replace(Path(path))
        return "written"
    except Exception as exc:
        return f"WRITE_FAILED: {type(exc).__name__}: {exc}"


def _write_json(path: str, payload: dict) -> str:
    if not path:
        return "disabled"
    try:
        tmp = Path(path + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
                       encoding="utf-8")
        tmp.replace(Path(path))
        return "written"
    except Exception as exc:
        return f"WRITE_FAILED: {type(exc).__name__}: {exc}"


# ----------------------------------------------------------------------------
# Self-test — the external audit's acceptance suite as executable checks
# ----------------------------------------------------------------------------
def selftest() -> int:
    cfg = Config()
    P = lambda share, n=200: [["Symbol", "Forecast Source"]] + [
        [f"S{i}", "provider_target" if i < int(n * share / 100) else "phase_ii_synthetic"]
        for i in range(n)]
    base = {"schema": STATE_SCHEMA, "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "sheet": "***", "pages": {"GM": {"share_pct": 50.0, "rows": 200}}}
    checks = []

    r = audit_pages({"GM": []}, base, cfg)
    checks.append(("read/empty page => exit 2, baseline kept",
                   r.exit_code == 2 and getattr(r, "_next_pages") == base["pages"]))
    r = audit_pages({"GM": [["junk"], ["a"]]}, base, cfg)
    checks.append(("missing contract => exit 2", r.exit_code == 2))
    r = audit_pages({"GM": P(10)}, base, cfg)
    checks.append(("50->10 collapse => exit 1, baseline kept at 50",
                   r.exit_code == 1 and getattr(r, "_next_pages")["GM"]["share_pct"] == 50.0))
    r2 = audit_pages({"GM": P(10)}, {**base, "pages": getattr(r, "_next_pages")}, cfg)
    checks.append(("same outage next run STILL fails", r2.exit_code == 1))
    rA = audit_pages({"GM": P(42.5)}, base, cfg)
    kept = getattr(rA, "_next_pages")
    checks.append(("50->42.5 WARN does not advance baseline",
                   rA.exit_code == 0 and kept["GM"]["share_pct"] == 50.0))
    rB = audit_pages({"GM": P(36.0)}, {**base, "pages": kept}, cfg)
    checks.append(("then 36.0 FAILS cumulatively vs 50", rB.exit_code == 1))
    r = audit_pages({"GM": P(40)}, None, cfg)
    checks.append(("scheduled + missing baseline => exit 2", r.exit_code == 2))
    r = audit_pages({"GM": P(40)}, None, cfg, bootstrap=True)
    checks.append(("bootstrap + missing baseline => exit 0, baseline written",
                   r.exit_code == 0 and getattr(r, "_next_pages").get("GM", {}).get("share_pct") == 40.0))
    stale = {**base, "generated_at_utc": "2020-01-01T00:00:00+00:00"}
    r = audit_pages({"GM": P(50)}, stale, cfg)
    checks.append(("stale baseline => exit 2", r.exit_code == 2))
    r = audit_pages({"GM": P(50)}, {**base, "schema": 1}, cfg)
    checks.append(("schema mismatch => exit 2", r.exit_code == 2))
    zcfg = Config(); zcfg.floor_pct = 5.0; zcfg.zero_pages = ("CFX",)
    r = audit_pages({"CFX": P(0)}, {**base, "pages": {"CFX": {"share_pct": 0.0, "rows": 200}}}, zcfg)
    checks.append(("structural-zero exempt from floor", r.exit_code == 0
                   and r.pages["CFX"]["verdict"] == "STRUCTURAL_ZERO"))
    r = audit_pages({"GM": P(50, 100)}, {**base, "pages": {"GM": {"share_pct": 50.0, "rows": 200}}}, cfg)
    checks.append(("row collapse 200->100 fails even at same share",
                   r.exit_code == 1 and r.pages["GM"]["verdict"] == "ROW_COLLAPSE"))
    grid = [["Symbol", "Forecast Source"]] + [[f"S{i}", "mystery_token"] for i in range(200)]
    r = audit_pages({"GM": grid}, base, cfg)
    checks.append(("unknown tokens reported as unknown, not synthetic",
                   r.pages["GM"]["unknown"] == 200 and r.pages["GM"]["synthetic"] == 0))
    bad = Config(); bad.errors.append("TFB_PTC_DROP_PCT=-5 outside [0.1, 100.0]")
    r = audit_pages({"GM": P(50)}, base, bad)
    checks.append(("invalid config => exit 2", r.exit_code == 2))
    r = audit_pages({"GM": P(55)}, base, cfg)
    checks.append(("healthy upward run advances baseline 50->55",
                   r.exit_code == 0 and getattr(r, "_next_pages")["GM"]["share_pct"] == 55.0))
    r = audit_pages({"GM": P(30)}, base, cfg, accept=True)
    checks.append(("operator acceptance writes lower baseline explicitly",
                   getattr(r, "_next_pages")["GM"]["share_pct"] == 30.0))

    ok = True
    for name, passed in checks:
        print(f"  {'PASS' if passed else 'FAIL'}  {name}")
        ok = ok and passed
    print(f"[PTC v{VERSION}] selftest {'PASS' if ok else 'FAIL'} "
          f"{sum(1 for _, p in checks if p)}/{len(checks)}")
    return 0 if ok else 2


# ----------------------------------------------------------------------------
# CLI
# ----------------------------------------------------------------------------
def create_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sheet-id", default=os.getenv("DEFAULT_SPREADSHEET_ID", ""))
    p.add_argument("--pages", default=os.getenv("TFB_PTC_PAGES", ""))
    p.add_argument("--state", default="provider_target_last_good.json")
    p.add_argument("--json-out", default="provider_target_coverage.json")
    p.add_argument("--observation-out", default="provider_target_observation.json")
    p.add_argument("--bootstrap", action="store_true",
                   help="explicit first-deployment baseline creation")
    p.add_argument("--accept-baseline", action="store_true",
                   help="operator acceptance of a lower structural baseline (one-shot)")
    p.add_argument("--selftest", action="store_true")
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = create_parser().parse_args(argv)
    if args.selftest:
        return selftest()
    cfg = load_config()
    bootstrap = args.bootstrap or _flag("TFB_PTC_BOOTSTRAP")
    accept = args.accept_baseline or _flag("TFB_PTC_ACCEPT_BASELINE")
    pages = tuple(x.strip() for x in args.pages.split(",") if x.strip()) or DEFAULT_PAGES
    state, state_err = load_state(args.state)
    rep = asyncio.run(run_live(args.sheet_id, pages, state, cfg, bootstrap, accept))
    if state_err and not bootstrap:
        rep.findings.append(Finding("CONTROL", "CH_BASELINE_CORRUPT", "*",
                                    f"{state_err} — refusing to silently re-baseline; "
                                    f"restore or re-bootstrap explicitly"))
    masked = rep.spreadsheet
    rep.persist_status = save_last_good(args.state, rep, masked)
    if rep.persist_status.startswith("WRITE_FAILED"):
        rep.findings.append(Finding("WARN", "PTC_PERSIST_FAILED", "*",
                                    f"last-good baseline not persisted ({rep.persist_status}) — "
                                    f"next run may see a stale or missing reference"))
    rendered = json.dumps(rep.payload(), ensure_ascii=False, indent=2, default=str)
    print(rendered)
    for cov in rep.pages.values():
        share = cov.get("share_pct")
        basep = cov.get("baseline_pct")
        print(f"[PTC v{VERSION}] {cov.get('page'):<16} rows={cov.get('rows'):>6} "
              f"provider={cov.get('provider'):>5} "
              f"share={'n/a' if share is None else format(share, '.1f') + '%':>7} "
              f"last_good={'n/a' if basep is None else format(basep, '.1f') + '%':>7} "
              f"verdict={cov.get('verdict')}")
    print(f"[PTC v{VERSION}] mode={rep.mode} baseline_updated={rep.baseline_updated} "
          f"({rep.baseline_update_detail}) persist={rep.persist_status}")
    for f in rep.findings:
        ann = "error" if f.severity in ("FAIL", "CONTROL") else "warning"
        print(f"::{ann}::{f.page} [{f.code}] {f.message}")
    if rep.fatal:
        print(f"::error::{rep.fatal}")
    obs_status = _write_json(args.observation_out, rep.payload())
    out_status = _write_json(args.json_out, rep.payload())
    if "FAILED" in obs_status or "FAILED" in out_status:
        print(f"::warning::evidence write degraded (obs={obs_status}, report={out_status})")
    return rep.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
