"""Renew reviewed Name captions by re-capturing and re-checking their public sources.

`data/verified_name_labels.json` bindings expire seven days after review
(core/verified_name_labels.py). An expired binding stops filling a blank Name,
so the coverage audit's Name floor fails a week after each review (19 Oct:
Mutual_Funds 97.98% < 99%). This tool performs the "another current authority
review" the registry requires, mechanically and conservatively:

* every source URL of a binding is fetched again (HTTPS only, no redirects
  off-host, bounded size); any fetch failure holds that binding;
* the caption must still be corroborated by the fresh bodies:
  - Nasdaq-directory bindings (identifier `directory_financial_status`): the
    exact ticker row must still carry the same Security Name, venue and
    financial status, and the SEC ticker file must still map the exact ticker
    to the same CIK; `directory_creation_time` is re-read from the file;
  - every other binding: the exact exchange code (or a recorded series, class,
    CUSIP or CIK identifier) and the normalised caption must each appear in at
    least one fresh non-PDF body;
* only `reviewed_at_utc`, `expires_at_utc`, the source receipts,
  `directory_creation_time` and the review digest change. Name, state,
  metadata fence and quarantine flag are never edited, and a held binding is
  kept byte-identical (it simply stays expired).

sec.gov requires a declared User-Agent with a contact. It is read from
`TFB_SEC_USER_AGENT` (or --sec-user-agent) and never defaulted; without it,
bindings that cite sec.gov are held with reason `sec_user_agent_missing`.

The output is validated with the production loader before it is written.
Exit codes: 0 every binding renewed (or --check clean), 1 some held or
expiring soon, 2 fatal.

v1.1.0 (2026-10-21) --apply-renewal RENEWED: merge a renewed registry (the
artifact of a renew run) into --registry binding by binding. A binding takes
the renewed copy only when every field other than the renewal fields
(reviewed_at_utc, expires_at_utc, sources, review_receipt_sha256 and
identifiers.directory_creation_time) is identical and the renewed review is
newer; anything else keeps the --registry copy. So a renewal generated from
another revision can never revert or add a binding: it can only re-stamp the
same captions. Exit 0 when at least one binding was applied, 3 when none.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core import verified_name_labels as labels  # noqa: E402

VERSION = "1.1.0"
REVIEW_WINDOW = timedelta(days=7)
MAX_BODY = 40_000_000
DIRECTORY_FILES = {
    "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt": "nasdaq",
    "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt": "other",
}
SEC_TICKERS = "https://www.sec.gov/files/company_tickers_exchange.json"
_ID_KEYS = ("series_id", "class_id", "cusip")


class _SameHostRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        old, new = urlsplit(req.full_url), urlsplit(newurl)
        if new.scheme != "https" or new.hostname != old.hostname:
            return None
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _is_sec(url):
    host = urlsplit(url).hostname or ""
    return host == "sec.gov" or host.endswith(".sec.gov")


def http_fetch(url, *, sec_user_agent, timeout=45):
    """Return (status, body bytes). Raises on transport failure."""
    agent = sec_user_agent if _is_sec(url) else "tfb-renew-verified-name-labels/" + VERSION
    request = urllib.request.Request(url, headers={"User-Agent": agent, "Accept": "*/*"})
    opener = urllib.request.build_opener(_SameHostRedirect)
    with opener.open(request, timeout=timeout) as response:
        body = response.read(MAX_BODY + 1)
        if len(body) > MAX_BODY:
            raise ValueError("body exceeds size bound")
        return response.status, body


def _norm(text):
    text = html.unescape(re.sub(r"<[^>]+>", " ", text))
    text = text.casefold().replace("&", " and ")
    return " ".join(re.sub(r"[^\w]+", " ", text).split())


def _is_pdf(body):
    return body[:5] == b"%PDF-"


def _directory_rows(body):
    lines = body.decode("utf-8", "replace").splitlines()
    rows, created = {}, ""
    for line in lines[1:]:
        if line.startswith("File Creation Time:"):
            created = line.split(":", 1)[1].split("|", 1)[0].strip()
            continue
        cells = line.split("|")
        if cells and cells[0]:
            rows.setdefault(cells[0], []).append(cells)
    return rows, created


def _check_directory(item, bodies):
    ids = item["identifiers"]
    ticker = ids.get("exact_authority_ticker", "")
    found = []
    for url, kind in DIRECTORY_FILES.items():
        if url not in bodies:
            continue
        rows, created = _directory_rows(bodies[url])
        for cells in rows.get(ticker, []):
            if kind == "nasdaq" and len(cells) > 4:
                found.append((cells[1], "Nasdaq", cells[4], created))
            elif kind == "other" and len(cells) > 2:
                found.append((cells[1], cells[2], "not supplied", created))
    if len(found) != 1:
        return "directory_row_missing_or_ambiguous", None
    name, venue, status, created = found[0]
    if (name != ids.get("security_class") or venue != ids.get("directory_venue")
            or status != ids.get("directory_financial_status")):
        return "directory_row_changed", None
    if not created:
        return "directory_creation_time_missing", None
    if SEC_TICKERS not in bodies:
        return "sec_ticker_file_missing", None
    try:
        sec = json.loads(bodies[SEC_TICKERS])
        fields = sec["fields"]
        ci, ti = fields.index("cik"), fields.index("ticker")
        ciks = {int(row[ci]) for row in sec["data"] if row[ti] == ticker}
    except (ValueError, KeyError, TypeError):
        return "sec_ticker_file_unreadable", None
    if ciks != {int(ids.get("CIK", "0") or 0)}:
        return "sec_ticker_cik_changed", None
    return None, created


def _check_document(item, bodies):
    texts = [_norm(body.decode("utf-8", "replace")) for body in bodies.values() if not _is_pdf(body)]
    if not texts:
        return "no_text_source"
    ids = item["identifiers"]
    tokens = [ids.get("exact_exchange_code") or ids.get("exact_authority_ticker")
              or item["symbol"].rsplit(".", 1)[0]]
    tokens += [ids[k] for k in _ID_KEYS if ids.get(k)]
    if ids.get("CIK"):
        tokens.append(str(int(ids["CIK"])) if ids["CIK"].isdigit() else ids["CIK"])
    tokens = [_norm(t) for t in tokens if t and _norm(t)]
    if not any(re.search(r"(?<!\w)" + re.escape(t) + r"(?!\w)", text) for t in tokens for text in texts):
        return "identifier_not_corroborated"
    name = _norm(item["name"])
    if not any(name in text for text in texts):
        return "caption_not_corroborated"
    return None


def renew(registry, *, fetch, now, sec_user_agent=""):
    """Return (new_registry, report). `fetch(url) -> (status, bytes)`."""
    cache, report = {}, {"renewed": [], "held": {}}

    def get(url):
        if url not in cache:
            if _is_sec(url) and not sec_user_agent:
                cache[url] = ("sec_user_agent_missing", None, None)
            else:
                try:
                    status, body = fetch(url)
                    stamp = datetime.now(UTC) if now is None else now
                    cache[url] = (None if 200 <= status < 300 else "http_%d" % status, body, stamp)
                except (OSError, ValueError, urllib.error.URLError) as exc:
                    cache[url] = ("fetch_failed:" + type(exc).__name__, None, None)
        return cache[url]

    out = []
    for item in registry["bindings"]:
        key = item["page"] + "|" + item["symbol"]
        bodies, receipts, reason = {}, [], None
        for source in item["sources"]:
            error, body, stamp = get(source["url"])
            if error:
                reason = error
                break
            bodies[source["url"]] = body
            receipts.append({"url": source["url"], "sha256": hashlib.sha256(body).hexdigest(),
                             "captured_at_utc": stamp.isoformat()})
        created = None
        if reason is None:
            if "directory_financial_status" in item["identifiers"]:
                reason, created = _check_directory(item, bodies)
            else:
                reason = _check_document(item, bodies)
        if reason:
            report["held"][key] = reason
            out.append(item)
            continue
        reviewed = max(datetime.fromisoformat(r["captured_at_utc"]) for r in receipts)
        reviewed = max(reviewed, now or reviewed)
        fresh = dict(item, sources=receipts, reviewed_at_utc=reviewed.isoformat(),
                     expires_at_utc=(reviewed + REVIEW_WINDOW).isoformat())
        if created is not None:
            fresh["identifiers"] = dict(item["identifiers"], directory_creation_time=created)
        fresh["review_receipt_sha256"] = labels._binding_digest(fresh)
        out.append(fresh)
        report["renewed"].append(key)
    return dict(registry, bindings=out), report


_RENEWAL_FIELDS = ("reviewed_at_utc", "expires_at_utc", "sources", "review_receipt_sha256")


def _caption_core(item):
    """Everything a renewal may NOT change (v1.1.0)."""
    core = {k: v for k, v in item.items() if k not in _RENEWAL_FIELDS}
    ids = dict(core.get("identifiers") or {})
    ids.pop("directory_creation_time", None)
    core["identifiers"] = ids
    return json.dumps(core, sort_keys=True, ensure_ascii=False)


def apply_renewal(base, renewed):
    """v1.1.0: return (merged registry, report). Bindings come from `base`;
    one takes its renewed copy only when its caption core is identical and
    the renewed review is strictly newer."""
    fresh = {}
    for item in renewed.get("bindings") or []:
        fresh.setdefault((item.get("page"), item.get("symbol")), item)
    out, report = [], {"applied": [], "kept": {}}
    for item in base["bindings"]:
        key = (item["page"], item["symbol"])
        label = item["page"] + "|" + item["symbol"]
        new = fresh.get(key)
        if new is None:
            out.append(item)
            report["kept"][label] = "not_in_renewal"
            continue
        if _caption_core(new) != _caption_core(item):
            out.append(item)
            report["kept"][label] = "caption_changed_since_renewal"
            continue
        try:
            newer = (datetime.fromisoformat(new["reviewed_at_utc"])
                     > datetime.fromisoformat(item["reviewed_at_utc"]))
        except (KeyError, TypeError, ValueError):
            newer = False
        if not newer:
            out.append(item)
            report["kept"][label] = "not_newer"
            continue
        out.append(new)
        report["applied"].append(label)
    return dict(base, bindings=out), report


def expiring(registry, *, now, within):
    return sorted(b["page"] + "|" + b["symbol"] for b in registry["bindings"]
                  if datetime.fromisoformat(b["expires_at_utc"]) <= now + within)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--registry", default=str(labels.REGISTRY_PATH))
    parser.add_argument("--out", help="write the renewed registry here (default: dry run)")
    parser.add_argument("--report", help="write the JSON report here")
    parser.add_argument("--sec-user-agent", default=os.getenv("TFB_SEC_USER_AGENT", ""))
    parser.add_argument("--check", action="store_true",
                        help="only report bindings expiring within --warn-hours; no network")
    parser.add_argument("--warn-hours", type=float, default=48.0)
    parser.add_argument("--apply-renewal", metavar="RENEWED",
                        help="v1.1.0: merge a renewed registry into --registry (see header); "
                             "writes --out (default: --registry)")
    args = parser.parse_args(argv)

    path = Path(args.registry)
    registry = json.loads(path.read_text(encoding="utf-8"))
    now = datetime.now(UTC)
    if args.apply_renewal:
        renewed = json.loads(Path(args.apply_renewal).read_text(encoding="utf-8"))
        merged, report = apply_renewal(registry, renewed)
        out = Path(args.out or args.registry)
        tmp = out.with_suffix(out.suffix + ".tmp")
        tmp.write_text(json.dumps(merged, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        try:
            labels._load_registry(tmp)
        except ValueError as exc:
            tmp.unlink()
            print("::error::merged registry fails the production loader: %s" % exc)
            return 2
        tmp.replace(out)
        summary = {"version": VERSION, "applied": len(report["applied"]),
                   "kept": len(report["kept"]), "kept_reasons": {}}
        for reason in report["kept"].values():
            summary["kept_reasons"][reason] = summary["kept_reasons"].get(reason, 0) + 1
        print(json.dumps(summary))
        if args.report:
            Path(args.report).write_text(json.dumps(dict(summary, **report), indent=1) + "\n",
                                         encoding="utf-8")
        return 0 if report["applied"] else 3
    soon = expiring(registry, now=now, within=timedelta(hours=args.warn_hours))
    if args.check:
        report = {"version": VERSION, "checked_at_utc": now.isoformat(),
                  "bindings": len(registry["bindings"]), "expiring_within_h": args.warn_hours,
                  "expiring": soon}
        print(json.dumps({k: (len(v) if k == "expiring" else v) for k, v in report.items()}))
        if args.report:
            Path(args.report).write_text(json.dumps(report, indent=1) + "\n", encoding="utf-8")
        return 1 if soon else 0

    started = time.monotonic()
    new, report = renew(registry, now=None, sec_user_agent=args.sec_user_agent.strip(),
                        fetch=lambda url: http_fetch(url, sec_user_agent=args.sec_user_agent.strip()))
    raw = json.dumps(new, indent=2, ensure_ascii=False) + "\n"
    if args.out:
        out = Path(args.out)
        tmp = out.with_suffix(out.suffix + ".tmp")
        tmp.write_text(raw, encoding="utf-8")
        try:
            labels._load_registry(tmp)
        except ValueError as exc:
            tmp.unlink()
            print("::error::renewed registry fails the production loader: %s" % exc)
            return 2
        tmp.replace(out)
    summary = {"version": VERSION, "renewed": len(report["renewed"]), "held": len(report["held"]),
               "held_reasons": {}, "seconds": round(time.monotonic() - started, 1),
               "written": bool(args.out)}
    for reason in report["held"].values():
        summary["held_reasons"][reason] = summary["held_reasons"].get(reason, 0) + 1
    print(json.dumps(summary))
    for key, reason in sorted(report["held"].items()):
        print("HELD %s %s" % (key, reason))
    if args.report:
        Path(args.report).write_text(json.dumps(dict(summary, **report), indent=1) + "\n",
                                     encoding="utf-8")
    return 1 if report["held"] else 0


if __name__ == "__main__":
    sys.exit(main())
