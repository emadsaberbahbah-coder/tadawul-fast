"""
scripts/repair_corporate_actions.py — Performance_Log CA sweep & repair
========================================================================
VERSION 1.2.0  (2026-11-25)  — THE OUTCOME'S OWN RECEIPT NAMES ITS EXIT SESSION
WHY (full audit of 25 Nov 2026): S-1 criterion 5 has read "CA UNREPAIRED" since
Granite REIT's 3-for-1 split of 24 Nov, on ONE row: the 1W outcome of
GRT-UN.TO (row 30795) matured with Target Date 21 Nov and the Maturity stamp
24 Nov, so the split falls between the two and 1.1.0 holds the row ("exit basis
ambiguous, review by hand"). The shadow scorer's check counts a held row as an
unrepaired one, so the gate stays FAIL until a person edits the row. The row
already says which session its exit price came from: the tracker writes an
[OUTCOME-WITNESS v1] receipt into Notes with "quote_session" (here 2026-11-23,
the close before the split) and "price" (88.92 = the row's Current Price).
WHAT: _finished_repair reads that receipt when the two ends disagree. The exit
price is put on today's basis by the factor of the actions effective after the
witnessed session, only when the receipt's price equals the row's Current
Price (the receipt describes this exit price, not an older one) and the
session lies between the two ends. Anything else is held exactly as in 1.1.1.
The plan item records "exit_basis": "witness <date>". On the GRT row the entry
87.49 and the exit 88.92 are both pre-split prices, so the ROI stays +1.63%
and both prices move to the new basis (29.16 / 29.64).
Rollback: repository variable TFB_CA_WITNESS_BASIS=off (1.1.1 behaviour).
Functions added: 2 (_witness_session, _witness_on).

VERSION 1.1.1  (2026-11-14)  — THE REPAIR TAG IS NEVER CUT OFF
WHY: the first apply (run 43638975586) rebased 12 MBRX rows, but five of them
already held long Notes (the outcome-price receipt, 600-769 characters) and
the tag was appended and the whole cell cut to 250 characters: the receipt
lost its tail and the `ca_adjusted` tag itself was cut away, so the next run
would have rebased those rows a SECOND time (-35.7% -> -96.8%). The daily job
was disabled the same evening, before any second run.
WHAT:
  * _tag_notes keeps the whole of the existing Notes and appends the tag;
    nothing is truncated (a Sheets cell holds 50,000 characters).
  * --retag FILE (default config/corporate_actions_retag.json): rows already
    rebased once whose tag was lost. Each is retagged once, and only while its
    Entry Price is still exactly the original divided by the factor; a
    matured row gets its original Notes back (from the archived snapshot,
    SHA-256 checked); an active row keeps its current Notes. Those rows are
    never in the rebase plan. A row whose entry no longer matches is HELD.
  * plan rows carry their Record ID.

VERSION 1.1.0  (2026-11-14)  — A REVIEWED SEED, AND FINISHED OUTCOMES REPAIRED
WHY (owner 14 Nov: "move to the next action"; reviews of 13-14 Nov): the
_Corporate_Actions register was header-only, so nothing was ever repaired:
MBRX's 1-for-20 reverse split of 20 Oct left seven matured outcomes above
+1,000% in Performance_Log, and the S-1 corporate-action check had nothing
to stand on (shadow_scorer 1.9.6 now reads an empty register as pending).
WHAT:
  * --seed FILE (default config/corporate_actions_confirmed.json): actions
    confirmed from the issuer's filing and merged through review are written
    into _Corporate_Actions with their filing as Source, ON OR AFTER their
    effective date only - never earlier, because the repair rebases prices
    dated before the effective date and today's quotes are still on the old
    basis until then. A row already in the register (same symbol, type and
    date) is not written again.
  * plan_repairs now also repairs a FINISHED outcome (matured / stopped) that
    spans the action: the exit price (Current Price) is put on the same
    basis as the rebased entry - divided by the factors of actions effective
    after its Maturity Date - and Realized ROI % and Outcome are recomputed
    from the two. An outcome wholly before or wholly after the action keeps
    its ROI (only its prices move onto today's basis).
  * the credentials read accepts the base64 form the workflows pass in
    GOOGLE_SHEETS_CREDENTIALS.
  * --scan prints the seed rows it would write and the repairs, and writes
    nothing; --apply writes both, then the plan.
Workflow: .github/workflows/corporate_actions.yml (daily, and on dispatch).
Rollback: repository variable TFB_CA_REPAIR=scan (the workflow then only
prints), or remove a row from the seed (rows already written stay; edit the
register's Source to AUTO_DETECT to stop it applying).

VERSION 1.0.0  (2026-07-18)  — NEW SCRIPT (Wave A0, deliverable #6b)

WHY: see core/corporate_actions.py v1.0.0 (the pure library this tool drives).
The first 1M cohort matures 2026-07-28; anchors must be split-safe before then.

GOVERNANCE (hard rules):
  * DRY-RUN by default. Nothing is written without --apply.
  * Only CONFIRMED actions (Source != AUTO_DETECT) are ever applied.
  * Detection writes PROPOSAL rows (Source=AUTO_DETECT) to `_Corporate_Actions`
    for the operator to confirm by editing Source — never self-confirming.
  * Idempotent: records already tagged `ca_adjusted` in Notes are skipped.

USAGE:
  --selftest                       offline fixtures, no network
  --seed FILE                      reviewed confirmed actions (1.1.0)
  --retag FILE                     rows rebased once whose tag was lost (1.1.1)
  --scan            (default)     dry-run: print plan + proposals
  --apply                         execute the confirmed plan + write proposals
  --sheet-id ID                   override spreadsheet id

ENV: TARGET_SHEET_ID | TRACK_SHEET_ID | DEFAULT_SPREADSHEET_ID ;
     GOOGLE_APPLICATION_CREDENTIALS | GOOGLE_SHEETS_CREDENTIALS(_B64)
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Allow both repo-root and scripts/ execution
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core import corporate_actions as ca  # noqa: E402

SCRIPT_VERSION = "1.2.0"
_RIYADH = timezone(timedelta(hours=3))
PL_TAB = "Performance_Log"


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# --------------------------------------------------------------------------- #
# pure planning core (unit-tested offline)                                     #
# --------------------------------------------------------------------------- #
def locate_header(values: Sequence[Sequence[Any]]) -> Optional[int]:
    for i, row in enumerate(values[:10]):
        if row and str(row[0]).strip() == "Record ID":
            return i
    return None


def _col(hdr: List[str], name: str) -> Optional[int]:
    try:
        return hdr.index(name)
    except ValueError:
        return None


def plan_repairs(values: Sequence[Sequence[Any]],
                 index: Dict[str, List[Tuple[date, float]]]
                 ) -> Tuple[List[Dict[str, Any]], Optional[int], Dict[str, int]]:
    """-> (plan rows, header_row_index, column map). Plan entries carry the
    sheet row number (1-based) plus old/new Entry & Target prices and factor."""
    hdr_i = locate_header(values)
    if hdr_i is None:
        return [], None, {}
    hdr = [str(h).strip() for h in values[hdr_i]]
    cols = {k: _col(hdr, k) for k in
            ("Record ID", "Symbol", "Date Recorded (Riyadh)", "Entry Price",
             "Target Price", "Status", "Current Price", "Notes",
             "Realized ROI %", "Outcome", "Maturity Date", "Target Date (Riyadh)")}
    plan: List[Dict[str, Any]] = []
    for r_i in range(hdr_i + 1, len(values)):
        row = values[r_i]
        sym = str(row[cols["Symbol"]]).strip().upper() \
            if cols["Symbol"] is not None and len(row) > cols["Symbol"] else ""
        if not sym:
            continue
        notes = str(row[cols["Notes"]]) if cols["Notes"] is not None \
            and len(row) > cols["Notes"] else ""
        if ca.APPLIED_TAG in notes:
            continue
        d = ca._as_date(row[cols["Date Recorded (Riyadh)"]]
                        if cols["Date Recorded (Riyadh)"] is not None
                        and len(row) > cols["Date Recorded (Riyadh)"] else None)
        f = ca.cumulative_factor(sym, d, index)
        if f == 1.0:
            continue
        ep = ca._as_float(row[cols["Entry Price"]]) \
            if cols["Entry Price"] is not None else None
        tp = ca._as_float(row[cols["Target Price"]]) \
            if cols["Target Price"] is not None else None
        if ep is None:
            continue
        item = {
            "sheet_row": r_i + 1, "symbol": sym, "date": str(d or ""),
            "record_id": str(_cell(row, cols.get("Record ID")) or "").strip(),      # 1.1.1
            "factor": f,
            "entry_old": ep, "entry_new": round(ep / f, 6),
            "target_old": tp,
            "target_new": (round(tp / f, 6) if tp is not None else None),
            "notes_old": notes,
        }
        item.update(_finished_repair(row, cols, sym, ep / f, index))      # 1.1.0
        plan.append(item)
    return plan, hdr_i, cols


_FINISHED = ("matured", "stopped")


def _cell(row: Sequence[Any], c: Optional[int]) -> Any:
    return row[c] if c is not None and len(row) > c else None


def _finished_repair(row: Sequence[Any], cols: Dict[str, Optional[int]], sym: str, entry_new: float,
                     index: Dict[str, List[Tuple[date, float]]]) -> Dict[str, Any]:
    """1.1.0: a finished outcome (matured / stopped) with an exit price: the exit price on today's basis and the
    Realized ROI % and Outcome recomputed from it and the rebased entry. {} when the row is not finished, has no
    exit price, or nothing changes; {"held": reason} when the exit price's basis cannot be established.

    The exit quote session is not the processing time: track_performance prices a late-finalized outcome at the
    TARGET session's historical close but stamps Maturity Date with the run time. So the exit session lies
    between Target Date (Riyadh) and Maturity Date. The basis is known only when no action falls between the two
    (the same factor at both ends); otherwise the whole row is held for review, never rewritten on a guess."""
    status = str(_cell(row, cols.get("Status")) or "").strip().lower()
    cur = ca._as_float(_cell(row, cols.get("Current Price")))
    if status not in _FINISHED or cur is None or not entry_new:
        return {}
    target_on = ca._as_date(_cell(row, cols.get("Target Date (Riyadh)")))
    matured_on = ca._as_date(_cell(row, cols.get("Maturity Date")))
    if target_on is None or matured_on is None:
        return {"held": "finished outcome without Target Date and Maturity Date: exit basis unknown"}
    lo, hi = min(target_on, matured_on), max(target_on, matured_on)
    f_lo, f_hi = ca.cumulative_factor(sym, lo, index), ca.cumulative_factor(sym, hi, index)
    basis = None
    if abs(f_lo - f_hi) > 1e-12:
        q = _witness_session(str(_cell(row, cols.get("Notes")) or ""), cur) if _witness_on() else None   # 1.2.0
        if q is None or not (lo <= q <= hi):
            return {"held": f"an action falls between the target session {lo} and the maturity stamp {hi}: "
                            "exit basis ambiguous, review by hand"}
        f_hi, basis = ca.cumulative_factor(sym, q, index), "witness " + q.isoformat()
    cur_new = cur / f_hi
    roi_new = round((cur_new / entry_new - 1.0) * 100.0, 4)
    roi_old = ca._as_float(_cell(row, cols.get("Realized ROI %")))
    out: Dict[str, Any] = {"exit_old": cur, "exit_new": round(cur_new, 6), "roi_old": roi_old, "roi_new": roi_new,
                           "outcome_new": "WIN" if roi_new > 0 else ("LOSS" if roi_new < 0 else "BREAKEVEN")}
    if basis:
        out["exit_basis"] = basis                                         # 1.2.0
    return out


_WITNESS_TAG = "[OUTCOME-WITNESS v1]"


def _witness_on() -> bool:
    """1.2.0: TFB_CA_WITNESS_BASIS = off / 0 / false / no restores the 1.1.1 hold."""
    return os.environ.get("TFB_CA_WITNESS_BASIS", "").strip().lower() not in ("off", "0", "false", "no")


def _witness_session(notes: str, exit_price: Optional[float]) -> Optional[date]:
    """1.2.0: the quote session named by the row's [OUTCOME-WITNESS v1] receipt, when that receipt's price is the
    row's exit price (relative difference under 1e-6); else None. The receipt is the tracker's JSON object that
    follows the tag. PURE."""
    i = notes.find(_WITNESS_TAG)
    if i < 0 or not exit_price:
        return None
    try:
        obj, _end = json.JSONDecoder().raw_decode(notes[i + len(_WITNESS_TAG):].lstrip())
    except (ValueError, TypeError):
        return None
    if not isinstance(obj, dict):
        return None
    price = ca._as_float(obj.get("price"))
    if price is None or abs(price - exit_price) > 1e-6 * max(abs(exit_price), 1.0):
        return None
    return ca._as_date(obj.get("quote_session"))


def _tag_notes(notes_old: str, tag: str) -> str:
    """1.1.1: the existing Notes, whole, then the tag. Never truncated: cutting the cell is how 1.1.0 lost the tag
    (and the tail of the outcome receipt) on five MBRX rows. PURE."""
    notes_old = str(notes_old or "")
    return f"{notes_old} | {tag}" if notes_old.strip() else tag


def plan_retags(values: Sequence[Sequence[Any]], hdr_i: Optional[int], cols: Dict[str, Optional[int]],
                retag: Dict[str, Any], version: str = SCRIPT_VERSION
                ) -> Tuple[List[Dict[str, Any]], List[str], set]:
    """1.1.1: (Notes writes, held notes, the record IDs the retag file covers). A listed row that already carries
    the tag needs nothing. One without it is retagged only while its Entry Price equals entry_before / factor (it
    was rebased exactly once); otherwise it is held. PURE."""
    want = {str(r.get("record_id") or "").strip(): r for r in (retag or {}).get("rows") or []
            if str(r.get("record_id") or "").strip()}
    ups: List[Dict[str, Any]] = []
    held: List[str] = []
    if hdr_i is None or not want or cols.get("Record ID") is None:
        return ups, held, set(want)
    seen = set()
    for r_i in range(hdr_i + 1, len(values)):
        row = values[r_i]
        rid = str(_cell(row, cols.get("Record ID")) or "").strip()
        spec = want.get(rid)
        if spec is None:
            continue
        seen.add(rid)
        notes = str(_cell(row, cols.get("Notes")) or "")
        if ca.APPLIED_TAG in notes:
            continue
        factor = ca._as_float(spec.get("factor"))
        before = ca._as_float(spec.get("entry_before"))
        now = ca._as_float(_cell(row, cols.get("Entry Price")))
        if not factor or not before or now is None or abs(now - before / factor) > 1e-6 * max(1.0, abs(now)):
            held.append(f"row {r_i + 1} {rid}: Entry Price {now} is not {before} / {factor} - not retagged")
            continue
        base = spec.get("original_notes")
        tag = f"{ca.APPLIED_TAG}:v{version}:f={factor:g}:retag"
        ups.append({"sheet_row": r_i + 1, "record_id": rid,
                    "notes_new": _tag_notes(base if base else notes, tag)})
    for rid in sorted(set(want) - seen):
        held.append(f"{rid}: not found in Performance_Log")
    return ups, held, set(want)


def seed_rows(seed: Dict[str, Any], existing: Sequence[Sequence[Any]], today: date,
              stamp: str) -> Tuple[List[List[str]], List[str]]:
    """1.1.0: (_Corporate_Actions rows to write, notes on the seed rows held back). A seed action is written once
    its effective date has come (today >= effective) and when the register holds no row with the same symbol,
    type and effective date. PURE."""
    have = set()
    for r in list(existing or [])[1:]:
        if r and str(r[0]).strip():
            have.add((str(r[0]).strip().upper(), str(r[1] if len(r) > 1 else "").strip().upper(),
                      str(ca._as_date(r[2] if len(r) > 2 else None) or "")))
    rows: List[List[str]] = []
    held: List[str] = []
    for a in (seed or {}).get("actions") or []:
        sym = str(a.get("symbol") or "").strip().upper()
        typ = str(a.get("action_type") or "").strip().upper()
        eff = ca._as_date(a.get("effective_date"))
        src = str(a.get("source") or "").strip()
        ratio = ca._as_float(a.get("ratio"))
        if not sym or typ not in ("SPLIT", "REVERSE_SPLIT", "SYMBOL_CHANGE") or eff is None or not src \
                or src.upper() == ca.SOURCE_AUTO or (typ != "SYMBOL_CHANGE" and not ratio):
            held.append("%s: not a complete confirmed action - skipped" % (sym or "?"))
            continue
        if (sym, typ, str(eff)) in have:
            continue
        if eff > today:
            held.append("%s %s effective %s - written on that day" % (sym, typ, eff))
            continue
        rows.append([sym, typ, str(eff), ("" if ratio is None else ("%g" % ratio)),
                     str(a.get("old_symbol") or ""), str(a.get("new_symbol") or ""), src[:240], stamp,
                     ("seed: " + str(a.get("notes") or ""))[:240]])
        have.add((sym, typ, str(eff)))
    return rows, held


def detect_proposals(values: Sequence[Sequence[Any]],
                     index: Dict[str, List[Tuple[date, float]]],
                     existing_actions: Sequence[ca.Action]
                     ) -> List[List[str]]:
    """Active records whose entry/current ratio matches a canonical split and
    which have NO logged action -> proposal rows for `_Corporate_Actions`."""
    hdr_i = locate_header(values)
    if hdr_i is None:
        return []
    hdr = [str(h).strip() for h in values[hdr_i]]
    cols = {k: _col(hdr, k) for k in
            ("Symbol", "Date Recorded (Riyadh)", "Entry Price",
             "Current Price", "Status", "Notes")}
    logged = {a.symbol for a in existing_actions}
    seen: Dict[str, List[str]] = {}
    for r_i in range(hdr_i + 1, len(values)):
        row = values[r_i]
        def g(key: str) -> Any:
            c = cols.get(key)
            return row[c] if c is not None and len(row) > c else None
        sym = str(g("Symbol") or "").strip().upper()
        if not sym or sym in logged or sym in seen:
            continue
        if ca.APPLIED_TAG in str(g("Notes") or ""):
            continue  # terminal: already repaired — never re-proposed
        if str(g("Status") or "").strip().lower() != "active":
            continue
        d = ca._as_date(g("Date Recorded (Riyadh)"))
        ep_adj = ca.adjust_price(sym, ca._as_float(g("Entry Price")), d, index)
        hit = ca.detect_split_candidate(ep_adj, ca._as_float(g("Current Price")))
        if hit:
            atype, k = hit
            seen[sym] = [sym, atype, str(date.today()),
                         str(int(k) if float(k).is_integer() else k),
                         "", "", ca.SOURCE_AUTO, _now_utc(),
                         f"auto-detected from Performance_Log row {r_i + 1}; "
                         f"CONFIRM by editing Source before any repair"]
    return list(seen.values())


# --------------------------------------------------------------------------- #
# sheets I/O                                                                   #
# --------------------------------------------------------------------------- #
def _sheet_id(cli: Optional[str]) -> str:
    for v in (cli, os.getenv("TARGET_SHEET_ID"), os.getenv("TRACK_SHEET_ID"),
              os.getenv("DEFAULT_SPREADSHEET_ID")):
        if v and str(v).strip():
            return str(v).strip()
    raise SystemExit("::error::Spreadsheet ID missing.")


def _open_sheet(cli: Optional[str]):
    import gspread
    from google.oauth2.service_account import Credentials
    path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    raw, b64 = os.getenv("GOOGLE_SHEETS_CREDENTIALS"), os.getenv("GOOGLE_SHEETS_CREDENTIALS_B64")
    if not path and (raw or b64):
        data = raw or base64.b64decode(b64).decode("utf-8")
        if not data.lstrip().startswith("{"):                       # 1.1.0: the workflows may pass base64 here
            data = base64.b64decode(data).decode("utf-8")
        f = tempfile.NamedTemporaryFile("w", suffix=".json", delete=False)
        f.write(data); f.close(); path = f.name
    if not path:
        raise SystemExit("::error::Google credentials missing.")
    creds = Credentials.from_service_account_file(
        path, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds).open_by_key(_sheet_id(cli))


def _col_letter(idx0: int) -> str:
    s, n = "", idx0 + 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--scan", action="store_true")
    ap.add_argument("--sheet-id")
    ap.add_argument("--seed", default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                                   "config", "corporate_actions_confirmed.json"))
    ap.add_argument("--retag", default=os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                                                    "config", "corporate_actions_retag.json"))
    args = ap.parse_args(argv)
    if args.selftest:
        return _selftest()
    retag: Dict[str, Any] = {}
    if args.retag:                                                    # 1.1.1: rows whose tag 1.1.0 cut off
        if not os.path.isfile(args.retag):
            print(f"::error::corporate-actions retag file not found: {args.retag}")
            return 2
        with open(args.retag, encoding="utf-8") as fh:
            retag = json.load(fh)
    seed: Dict[str, Any] = {}
    if args.seed:                                                     # 1.1.0: the reviewed seed
        if not os.path.isfile(args.seed):                             # a configured seed that is missing is loud
            print(f"::error::corporate-actions seed not found: {args.seed}")
            return 2
        with open(args.seed, encoding="utf-8") as fh:
            seed = json.load(fh)

    sh = _open_sheet(args.sheet_id)

    # actions tab (create with header if missing)
    try:
        ws_a = sh.worksheet(ca.TAB_ACTIONS)
    except Exception:
        ws_a = sh.add_worksheet(title=ca.TAB_ACTIONS, rows=200,
                                cols=len(ca.ACTIONS_HEADER))
        ws_a.update(values=[ca.ACTIONS_HEADER], range_name="A1")
    register = ws_a.get_all_values()
    today = datetime.now(timezone.utc).astimezone(_RIYADH).date()
    new_rows, held = seed_rows(seed, register, today, _now_utc())
    for h in held:
        print("  seed held: " + h)
    for r in new_rows:
        print(f"  seed row: {r[0]} {r[1]} {r[2]} ratio={r[3]} source={r[6][:60]}")
    actions = ca.parse_actions(list(register) + new_rows)
    index = ca.build_adjustment_index(actions, confirmed_only=True)

    ws_pl = sh.worksheet(PL_TAB)
    values = ws_pl.get_all_values()
    plan, hdr_i, cols = plan_repairs(values, index)
    held_rows = [p for p in plan if "held" in p]                      # 1.1.0: basis unknown -> untouched
    retags, retag_held, retag_ids = plan_retags(values, hdr_i, cols, retag)
    plan = [p for p in plan if "held" not in p and p.get("record_id") not in retag_ids]   # 1.1.1
    proposals = detect_proposals(values, index, actions)

    print(f"[CA-REPAIR v{SCRIPT_VERSION}] confirmed_actions="
          f"{sum(1 for a in actions if a.confirmed)} seed_rows={len(new_rows)} plan={len(plan)} "
          f"finished={sum(1 for p in plan if 'roi_new' in p)} held={len(held_rows)} "
          f"retag={len(retags)} retag_held={len(retag_held)} "
          f"proposals={len(proposals)} mode={'APPLY' if args.apply else 'DRY-RUN'}")
    for p in plan[:25]:
        print(f"  row {p['sheet_row']:>5} {p['symbol']:<10} f={p['factor']:g} "
              f"entry {p['entry_old']} -> {p['entry_new']}"
              + (f" | exit {p['exit_old']} -> {p['exit_new']} | realized {p['roi_old']} -> {p['roi_new']}"
                 if "roi_new" in p else ""))
    for t in retags:
        print(f"  RETAG row {t['sheet_row']:>5} {t['record_id']}")
    for h in retag_held:
        print("  RETAG HELD " + h)
    for p in held_rows[:25]:
        print(f"  HELD row {p['sheet_row']:>5} {p['symbol']:<10} {p['held']}")
    for pr in proposals[:15]:
        print(f"  PROPOSAL {pr[0]:<10} {pr[1]} ratio={pr[3]}")

    if args.apply:
        if new_rows:                                                  # 1.1.0: the register first
            ws_a.append_rows(new_rows, value_input_option="RAW")
        updates = []
        for p in plan:
            r = p["sheet_row"]
            updates.append({"range": f"{_col_letter(cols['Entry Price'])}{r}",
                            "values": [[p["entry_new"]]]})
            if p["target_new"] is not None and cols.get("Target Price") is not None:
                updates.append({"range": f"{_col_letter(cols['Target Price'])}{r}",
                                "values": [[p["target_new"]]]})
            if "roi_new" in p:                                         # 1.1.0: a finished outcome
                for col, val in (("Current Price", p["exit_new"]), ("Realized ROI %", p["roi_new"]),
                                 ("Outcome", p["outcome_new"])):
                    if cols.get(col) is not None:
                        updates.append({"range": f"{_col_letter(cols[col])}{r}", "values": [[val]]})
            if cols.get("Notes") is not None:                         # 1.1.1: never truncated
                updates.append({"range": f"{_col_letter(cols['Notes'])}{r}",
                                "values": [[_tag_notes(p["notes_old"],
                                                       f"{ca.APPLIED_TAG}:v{SCRIPT_VERSION}:f={p['factor']:g}")]]})
        if cols.get("Notes") is not None:
            for t in retags:                                          # 1.1.1: the tag 1.1.0 cut off
                updates.append({"range": f"{_col_letter(cols['Notes'])}{t['sheet_row']}",
                                "values": [[t["notes_new"]]]})
        for i in range(0, len(updates), 50):
            ws_pl.batch_update(updates[i:i + 50])
        if proposals:
            ws_a.append_rows(proposals, value_input_option="RAW")
        try:
            sh.worksheet("_Run_Log").append_row(
                [_now_utc(), "INFO", "ca_repair", PL_TAB, "OK",
                 f"[CA-REPAIR v{SCRIPT_VERSION}] applied={len(plan)} held={len(held_rows)} retag={len(retags)} "
                 f"seed_rows={len(new_rows)} proposals={len(proposals)}", "", "", "",
                 json.dumps({"version": SCRIPT_VERSION})],
                value_input_option="RAW")
        except Exception:
            pass
        print(f"[CA-REPAIR v{SCRIPT_VERSION}] APPLIED cells="
              f"{len(updates)} proposal_rows={len(proposals)}")
    elif proposals or new_rows or plan or retags:
        print("  (dry-run: nothing written; re-run with --apply)")
    return 0


# --------------------------------------------------------------------------- #
# SELFTEST                                                                     #
# --------------------------------------------------------------------------- #
def _selftest() -> int:
    checks: List[Tuple[str, bool]] = []
    pl = [
        ["", ""], ["", ""], ["", ""], ["", ""],
        ["Record ID", "Key", "Symbol", "Horizon", "Date Recorded (Riyadh)",
         "Entry Price", "Entry Recommendation", "Entry Score", "Risk Bucket",
         "Confidence", "Origin Tab", "Target Price", "Target ROI %",
         "Target Date (Riyadh)", "Status", "Current Price", "Unrealized ROI %",
         "Realized ROI %", "Outcome", "Volatility", "Max Drawdown %",
         "Sharpe Ratio", "Sector", "Factor Exposures",
         "Last Updated (Riyadh)", "Maturity Date", "Notes"],
        ["id1", "k", "ABCD.US", "1M", "2026-07-01", "100", "", "", "", "", "",
         "110", "", "", "active", "26.1", "", "", "", "", "", "", "", "", "",
         "", ""],
        ["id2", "k", "ABCD.US", "1M", "2026-07-16", "26", "", "", "", "", "",
         "28", "", "", "active", "26.1", "", "", "", "", "", "", "", "", "",
         "", ""],
        ["id3", "k", "SPLT.US", "1M", "2026-06-30", "200", "", "", "", "", "",
         "220", "", "", "active", "49.6", "", "", "", "", "", "", "", "", "",
         "", ""],
        ["id4", "k", "DONE.US", "1M", "2026-06-30", "80", "", "", "", "", "",
         "", "", "", "active", "20.1", "", "", "", "", "", "", "", "", "",
         "", "ca_adjusted:v1.0.0:f=4"],
    ]
    acts = ca.parse_actions([
        ca.ACTIONS_HEADER,
        ["ABCD.US", "SPLIT", "2026-07-10", "4", "", "", "CONFIRMED", "", ""],
    ])
    idx = ca.build_adjustment_index(acts)

    plan, hdr_i, cols = plan_repairs(pl, idx)
    checks.append(("header located at row 5", hdr_i == 4))
    checks.append(("plan: only pre-split ABCD row",
                   len(plan) == 1 and plan[0]["symbol"] == "ABCD.US"
                   and plan[0]["sheet_row"] == 6))
    checks.append(("plan math entry 100->25 target 110->27.5",
                   plan[0]["entry_new"] == 25.0 and plan[0]["target_new"] == 27.5))

    props = detect_proposals(pl, idx, acts)
    syms = [p[0] for p in props]
    checks.append(("proposal for unlogged SPLT.US 4:1",
                   syms == ["SPLT.US"] and props[0][1] == "SPLIT"
                   and props[0][3] == "4"))
    checks.append(("logged symbol not re-proposed", "ABCD.US" not in syms))
    checks.append(("proposal source is AUTO_DETECT",
                   props[0][6] == ca.SOURCE_AUTO))
    checks.append(("already-tagged DONE.US untouched everywhere",
                   all(p["symbol"] != "DONE.US" for p in plan)
                   and "DONE.US" not in syms))
    checks.append(("adjusted anchor kills false detection",
                   ca.detect_split_candidate(
                       ca.adjust_price("ABCD.US", 100, date(2026, 7, 1), idx),
                       26.1) is None))
    checks.append(("col letter math", _col_letter(0) == "A"
                   and _col_letter(26) == "AA"))

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[repair_corporate_actions v{SCRIPT_VERSION}] SELFTEST "
          f"{passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    sys.exit(main())
