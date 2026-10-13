"""Read-only deployed board-contract check; never accesses Sheets or a broker."""
from __future__ import annotations

import argparse
import ast
import copy
import hashlib
import json
import math
import os
import re
import urllib.error
import urllib.request
from datetime import UTC, datetime
from decimal import Decimal, localcontext
from pathlib import Path

BACKEND = "https://tadawul-fast-bridge.onrender.com"
SCRIPT_VERSION = "1.2.1"
# v1.2.1 (2026-10-13) - [P0-6 ENFORCE MUST PUBLISH ITS LEVELS]
# WHY: review of v1.2.0 (PR #764): a declared enforce accepted a blank
# (None) research amount, so a deployed regression that blanked every
# newly exposed value still passed the readback. The probe row supplies a
# positive quantity, buy price, current price, SAR FX and intrinsic value;
# portfolio_actions 1.16.0 under enforce serves MV 1000, cost 900, P&L 100,
# P&L % 11.1, stop 92, TP1 115, TP2 130 and the same four holdings KPIs.
# WHAT: under a declared enforce, every _RESEARCH_ROW_KEYS amount on the
# probe row and every _RESEARCH_KPI_KEYS total must be present and a valid
# research amount (finite; positive except P&L). off and observe are
# unchanged, and every money / execution / weight / alert check is kept.
# v1.2.0 (2026-10-13) - [P0-6 RESEARCH VIEW LOCKSTEP]
# WHY: portfolio_actions 1.16.0 adds TFB_PF_RESEARCH_VIEW (default off).
# Under enforce an unreconciled holding stays BLOCK with zero money but
# shows market value, cost, P&L and stop / TP levels, and the holdings
# KPIs can be filled. The unchanged probe below rejects that with
# "uncertified portfolio amount was published: holdings_value_sar", so
# arming enforce would turn the next manual readback red.
# WHAT: new --expect-pf-research-view off|observe|enforce (default off).
#   off     -> require_blocked_portfolio is byte-identical to v1.1.1 and
#              the artifact gains no key (an armed enforce is still caught).
#   observe -> the same strict checks, plus the served page must declare
#              observe and carry the observe tag on the probe row.
#   enforce -> row market value / cost / P&L / P&L % / stop / TP1 / TP2
#              and the four holdings KPIs may be numbers ONLY when the page
#              declares enforce and the row is BLOCK, carries the
#              "[custody-unverified]" tag, ends "; not executable" and names
#              no ADD verdict. Every money, execution, weight, sector, cash
#              KPI and alert assertion is kept unchanged.
#   A payload's own meta.research_view.mode is never enough: the workflow
#   must declare the mode, so an accidental arming still fails.
PF_RESEARCH_VIEW_MODES = ("off", "observe", "enforce")
PF_RESEARCH_TAG = "[custody-unverified]"
OPPORTUNITY_PATH = "/sheet-rows/opportunity-candidates"
PORTFOLIO_ACTIONS_PATH = "/sheet-rows/portfolio-actions"
PORTFOLIO_PROBE_SYMBOL = "SYNTH.SR"
TINY_QUOTE_PATH = "/v1/analysis/sheet-rows"
TINY_SYMBOLS = ("SHIB-USD", "DOGE-USD", "XRP-USD")
TINY_GATES = {"price_precision": "on", "roi12_softcap": "observe",
              "tuple_noise_release": "observe", "settle_unresolved": "observe",
              "scoring_settle": "observe", "env_combo": "ok"}


class ReadbackError(RuntimeError):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *_args, **_kwargs):
        raise ReadbackError("backend redirect refused")


def source_version(relative_path, name):
    root = Path(__file__).resolve().parents[1]
    for node in ast.parse((root / relative_path).read_text()).body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == name
            for target in node.targets
        ) and isinstance(node.value, ast.Constant):
            return str(node.value.value)
    raise ReadbackError("expected source version unavailable")


def transport(token):
    opener = urllib.request.build_opener(NoRedirect())
    receipts = []

    def record(path, status, raw):
        receipts.append({"path": path, "http_status": status,
                         "captured_at_utc": datetime.now(UTC).isoformat(),
                         "response_sha256": hashlib.sha256(raw).hexdigest(),
                         "response_bytes": len(raw)})

    def request(path, body=None, authenticated=False):
        headers = {"Accept": "application/json"}
        if authenticated:
            headers["X-APP-TOKEN"] = token
        data = None if body is None else json.dumps(body, allow_nan=False).encode()
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(BACKEND + path, data=data, headers=headers)
        try:
            with opener.open(req, timeout=45) as response:
                raw = response.read(4_000_001)
                record(path, response.status, raw)
            if len(raw) > 4_000_000:
                raise ReadbackError("backend response exceeds bound")
            result = json.loads(raw)
            if not isinstance(result, dict):
                raise ReadbackError("backend returned an invalid envelope")
            return result
        except urllib.error.HTTPError as exc:
            record(path, exc.code, exc.read(4_000_001))
            raise ReadbackError("backend HTTP " + str(exc.code)) from None
        except (urllib.error.URLError, TimeoutError, ValueError):
            raise ReadbackError("backend request or JSON failed") from None

    request.receipts = receipts
    return request


def _safe_quote_text(value, limit=4096):
    if not isinstance(value, str) or any(ord(c) < 32 and c not in "\t\n\r" for c in value):
        return None
    value = re.sub(r"https?://\S+", "[URL]", value[:limit])
    return re.sub(r"(?i)(token|api_key|authorization|secret)\s*[:=]\s*(?:Bearer\s+)?[^\s;]+", r"\1=[REDACTED]", value)


def _quote_number(value):
    if value is None or value == "":
        return None, "missing_or_null"
    if type(value) not in (int, float):
        return None, "invalid_numeric_type"
    if not math.isfinite(value):
        return None, "nonfinite"
    return value, None


def _quote_health(health, expected_commit, expected_engine):
    if not isinstance(health, dict):
        raise ReadbackError("tiny quote health envelope invalid")
    deploy = health.get("deploy") or {}
    gates = health.get("engine_gates") or {}
    summary = {"served_commit": deploy.get("render_git_commit"),
               "engine_version": health.get("engine_version"),
               "ready": health.get("ready"), "engine_ready": health.get("engine_ready"),
               "gates": {key: gates.get(key) for key in TINY_GATES},
               "worker_instance": _safe_quote_text(deploy.get("render_instance_id"), 160),
               "worker_boot_utc": _safe_quote_text(deploy.get("worker_boot_utc"), 64)}
    if summary["served_commit"] != expected_commit:
        raise ReadbackError("tiny quote served SHA differs from expected backend commit")
    if (summary["ready"] is not True or summary["engine_ready"] is not True
            or summary["engine_version"] != expected_engine
            or health.get("startup_warnings") != []):
        raise ReadbackError("tiny quote health is not the expected ready engine")
    if summary["gates"] != TINY_GATES:
        raise ReadbackError("tiny quote effective modes differ from required rollout modes")
    return summary


def _quote_receipt(request):
    receipts = getattr(request, "receipts", [])
    if not isinstance(receipts, list):
        return None
    for value in reversed(receipts):
        if isinstance(value, dict) and value.get("path") == TINY_QUOTE_PATH:
            return {key: value.get(key) for key in (
                "path", "http_status", "captured_at_utc", "response_sha256", "response_bytes")}
    return None


def _quote_error(exc):
    """Retain diagnostic codes without serializing arbitrary transport text."""
    message = str(exc)
    known = {
        "tiny quote health envelope invalid", "tiny quote served SHA differs from expected backend commit",
        "tiny quote health is not the expected ready engine",
        "tiny quote effective modes differ from required rollout modes",
        "actual quote response byte receipt missing or invalid", "actual quote response envelope/version failed",
        "actual quote row count or shape incomplete", "actual quote identities duplicate, cross-symbol or incomplete",
        "backend redirect refused", "backend response exceeds bound", "backend returned an invalid envelope",
        "backend request or JSON failed",
    }
    if message in known:
        return message
    match = re.search(r"\bHTTP\s*(\d{3})\b", message)
    return "backend HTTP " + match.group(1) if match else "tiny quote request failed"


def _quote_acquisition(row, now, request_started_at):
    warnings = row.get("warnings")
    if not isinstance(warnings, str):
        return {}, ["acquisition_receipt_missing"]
    tokens = {}
    for token in warnings.split(";"):
        name, sep, value = token.strip().partition(":")
        if name not in {"acquisition_status", "acquisition_provider",
                        "acquisition_acquired_at", "acquisition_quote_asof"}:
            continue
        if not sep or name in tokens:
            return {}, ["acquisition_receipt_ambiguous"]
        tokens[name] = value.strip()
    reasons = []
    provider = row.get("data_provider")
    if (tokens.get("acquisition_status") != "success" or not isinstance(provider, str)
            or not provider or tokens.get("acquisition_provider", "").lower() != provider.lower()):
        reasons.append("acquisition_unverified")
    times = {}
    for name in ("acquisition_acquired_at", "acquisition_quote_asof"):
        raw = tokens.get(name, "")
        try:
            stamp = datetime.fromisoformat(raw)
            if stamp.tzinfo is None or not re.search(r"T\d{2}:\d{2}:\d{2}", raw):
                raise ValueError("imprecise timestamp")
            stamp = stamp.astimezone(UTC)
            age = (now - stamp).total_seconds()
            if not -60 <= age <= 900:
                reasons.append(name + "_not_current")
            times[name] = stamp
        except (TypeError, ValueError, OverflowError):
            reasons.append(name + "_missing_or_invalid")
    if (len(times) == 2 and (times["acquisition_quote_asof"] - times["acquisition_acquired_at"]).total_seconds() > 60):
        reasons.append("quote_after_acquisition")
    if ("acquisition_acquired_at" in times
            and times["acquisition_acquired_at"] < request_started_at):
        reasons.append("acquisition_predates_quote_request")
    return {key: _safe_quote_text(value, 128) for key, value in tokens.items()}, reasons


def _quote_tuple(price, forecast, roi):
    with localcontext() as context:
        context.prec = 128
        cp, fp, rr = (Decimal(str(value)) for value in (price, forecast, roi))
        digits = max(4, 6 - fp.adjusted()) if fp < 10 else 4
        half_quantum = Decimal("0.5").scaleb(-digits)
        tolerance = Decimal("0.0000005") + half_quantum / cp
        derived = fp / cp - 1
        error = abs(rr - derived)
        return {"derived_roi_fraction": str(derived), "absolute_error": str(error),
                "rounding_tolerance": str(tolerance), "price_round_digits": digits,
                "status": "verified" if error <= tolerance else "failed",
                "reasons": [] if error <= tolerance else ["forecast_roi_fraction_mismatch"]}


def verify_tiny_quotes(request, expected_commit, expected_engine, expected_route,
                       workflow_commit=None):
    """Capture actual deployed quotes; no Sheet, broker, provider SDK or orders.

    Null/erased values remain unavailable. A returned target is an observed
    model output, not a supplier target or actionable investment certificate.
    A recent cached acquisition predating this request cannot certify the
    current price boundary; its numbers are retained as withheld observations.
    """
    result = {"ok": False, "status": "withheld", "requested_symbols": list(TINY_SYMBOLS),
              "expected_backend_commit": expected_commit, "workflow_source_sha": workflow_commit,
              "health_before": None, "health_after": None, "response_receipt": None,
              "request_started_at_utc": None,
              "rows": [], "reasons": [], "read_only": True,
              "scope": "actual API quote/forecast precision witness only; no funding, custody or native installation attested"}
    if (not re.fullmatch(r"[0-9a-f]{40}", str(expected_commit)) or
            not re.fullmatch(r"[0-9a-f]{40}", str(workflow_commit))):
        result.update(status="failed", reasons=["full_commit_sha_required"])
        return result
    if workflow_commit != expected_commit:
        result.update(status="failed", reasons=["workflow_backend_sha_mismatch"])
        return result
    quote_requested = False
    final_health_attempted = False
    try:
        result["health_before"] = _quote_health(request("/health"), expected_commit, expected_engine)
        body = {"page": "Commodities_FX", "symbols": list(TINY_SYMBOLS), "tickers": list(TINY_SYMBOLS),
                "limit": 3, "offset": 0, "include_meta": True, "include_matrix": False,
                "request_id": "tiny-quote-readback"}
        quote_requested = True
        request_started_at = datetime.now(UTC)
        result["request_started_at_utc"] = request_started_at.isoformat()
        payload = request(TINY_QUOTE_PATH, body, True)
        result["response_receipt"] = _quote_receipt(request)
        receipt = result["response_receipt"] or {}
        if (receipt.get("http_status") != 200 or not re.fullmatch(r"[0-9a-f]{64}", str(receipt.get("response_sha256")))
                or type(receipt.get("response_bytes")) is not int or not 0 < receipt["response_bytes"] <= 4_000_000):
            raise ReadbackError("actual quote response byte receipt missing or invalid")
        if (not isinstance(payload, dict) or payload.get("page") != "Commodities_FX"
                or payload.get("status") not in {"success", "ok"} or payload.get("error")
                or payload.get("version") != expected_route):
            raise ReadbackError("actual quote response envelope/version failed")
        result["route_version"] = expected_route
        result["request_id"] = _safe_quote_text(payload.get("request_id"), 128)
        rows = payload.get("row_objects")
        if (not isinstance(rows, list) or len(rows) != 3 or type(payload.get("count")) is not int
                or payload["count"] != 3 or any(not isinstance(row, dict) for row in rows)):
            raise ReadbackError("actual quote row count or shape incomplete")
        symbols = [row.get("symbol") for row in rows]
        if (any(not isinstance(symbol, str) for symbol in symbols) or len(set(symbols)) != 3
                or set(symbols) != set(TINY_SYMBOLS)
                or any(row.get("requested_symbol", row["symbol"]) != row["symbol"] for row in rows)):
            raise ReadbackError("actual quote identities duplicate, cross-symbol or incomplete")
        now = datetime.now(UTC)
        for row in rows:
            out = {"symbol": row["symbol"], "data_provider": _safe_quote_text(row.get("data_provider"), 128),
                   "forecast_source": _safe_quote_text(row.get("forecast_source"), 128),
                   "warnings": _safe_quote_text(row.get("warnings")), "tuples": {}, "reasons": []}
            for key in ("investability_status", "final_action", "block_reason", "scoring_errors"):
                out[key] = _safe_quote_text(row.get(key))
            numeric = ["current_price"] + [field + horizon for horizon in ("1m", "3m", "12m")
                                         for field in ("forecast_price_", "expected_roi_")]
            for key in numeric:
                out[key], issue = _quote_number(row.get(key))
                if issue:
                    out["reasons"].append(key + ":" + issue)
            cp = out["current_price"]
            if cp is not None and (cp <= 0 or cp >= 10 or (row["symbol"] == "SHIB-USD" and cp >= 0.0001)):
                out["reasons"].append("current_price:outside_tiny_positive_scope")
            out["acquisition_receipt"], acquisition_reasons = _quote_acquisition(row, now, request_started_at)
            out["reasons"].extend(acquisition_reasons)
            if not out["forecast_source"]:
                out["reasons"].append("forecast_source_missing")
            for horizon in ("1m", "3m", "12m"):
                fp, roi = out["forecast_price_" + horizon], out["expected_roi_" + horizon]
                check = {"status": "withheld", "reasons": ["tuple_missing_or_nonpositive"], "roi_unit": "fraction"}
                if cp is not None and cp > 0 and fp is not None and fp > 0 and roi is not None:
                    check = {**_quote_tuple(cp, fp, roi), "roi_unit": "fraction"}
                    if acquisition_reasons and check["status"] == "verified":
                        check.update(status="withheld", arithmetic_status="verified",
                                     reasons=list(acquisition_reasons))
                check.update(forecast_price=fp, expected_roi_fraction=roi)
                out["tuples"][horizon] = check
            invalid = any(reason.endswith((":invalid_numeric_type", ":nonfinite")) for reason in out["reasons"])
            statuses = [value["status"] for value in out["tuples"].values()]
            out["status"] = ("failed" if invalid or "failed" in statuses else
                             "withheld" if out["reasons"] or "withheld" in statuses else "verified")
            result["rows"].append(out)
        final_health_attempted = True
        result["health_after"] = _quote_health(request("/health"), expected_commit, expected_engine)
        statuses = [row["status"] for row in result["rows"]]
        result["status"] = "failed" if "failed" in statuses else "withheld" if "withheld" in statuses else "verified"
        result["ok"] = result["status"] == "verified"
    except ReadbackError as exc:
        result["status"] = "failed"
        result["reasons"].append(_quote_error(exc))
        result["response_receipt"] = _quote_receipt(request)
    except Exception as exc:  # noqa: BLE001 - preserve sanitized failure evidence for malformed responses
        result["status"] = "failed"
        result["reasons"].append("quote readback failed: " + type(exc).__name__)
        result["response_receipt"] = _quote_receipt(request)
    finally:
        if quote_requested and not final_health_attempted:
            try:
                result["health_after"] = _quote_health(request("/health"), expected_commit, expected_engine)
            except ReadbackError as exc:
                result["status"] = "failed"
                result["ok"] = False
                result["reasons"].append(_quote_error(exc))
            except Exception as exc:  # noqa: BLE001 - final health failures must leave an artifact
                result["status"] = "failed"
                result["ok"] = False
                result["reasons"].append("final quote health failed: " + type(exc).__name__)
    return result


def require_zero_money(payload):
    if payload.get("selected") != []:
        raise ReadbackError("blocked probe produced selected tickets")
    kpis = payload.get("kpis")
    if not isinstance(kpis, dict):
        raise ReadbackError("board KPI envelope missing")
    if not {"selected_count", "expected_gain_12m_sar", "deployable_sar",
            "capital_unallocated_sar"}.issubset(kpis):
        raise ReadbackError("required board monetary KPIs missing")
    for key in ("selected_count", "fundable_now", "fundable_by_rotation",
                "capital_call", "capital_call_topn_sar", "expected_gain_12m_sar",
                "total_suggested_sar", "budget_used_sar", "deployable_sar",
                "deployable_current_sar", "deployable_proforma_sar",
                "capital_unallocated_sar"):
        value = kpis.get(key, 0)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value != 0:
            raise ReadbackError("nonzero or invalid blocked-probe KPI: " + key)
    alerts = payload.get("alerts")
    if not isinstance(alerts, list) or any(not isinstance(a, dict) for a in alerts):
        raise ReadbackError("board alert envelope missing or invalid")
    forbidden = {"capital_call", "rotation_proposal", "unfunded_candidates"}
    if any(alert.get("type") in forbidden for alert in alerts):
        raise ReadbackError("blocked probe produced a funding alert")


def require_uncertified(payload, label):
    meta = payload.get("meta")
    if not isinstance(meta, dict) or meta.get("execution_ready") is not False:
        raise ReadbackError(label + " did not explicitly withhold execution")
    certification = meta.get("input_certification")
    if not isinstance(certification, dict) or certification.get("funding_eligible") is not False:
        raise ReadbackError(label + " did not explicitly withhold certified funding")


_RESEARCH_KPI_KEYS = ("holdings_value_sar", "cost_basis_sar", "pnl_sar", "pnl_pct")
_RESEARCH_ROW_KEYS = ("market_value_sar", "cost_sar", "pnl_sar", "pnl_pct", "stop_sar", "tp1_sar", "tp2_sar")
_RESEARCH_SIGNED_KEYS = ("pnl_sar", "pnl_pct")


def _research_amount(key, value):
    """v1.2.0: a research-view display amount is blank or a finite number;
    every amount except P&L must also be positive."""
    if value is None:
        return True
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return False
    return key in _RESEARCH_SIGNED_KEYS or value > 0


def _require_research_view(payload, action, mode):
    """v1.2.0: the served page must carry the mode the workflow declared."""
    view = payload["meta"].get("research_view")
    detail = action.get("detail") if isinstance(action.get("detail"), dict) else {}
    row_view = detail.get("research_view")
    if not isinstance(view, dict) or view.get("mode") != mode or view.get("status") != "ok" or \
            not isinstance(row_view, dict) or row_view.get("mode") != mode:
        raise ReadbackError("declared portfolio research view mode not served: " + mode)
    reason = action.get("action_reason")
    if not isinstance(reason, str) or not reason.startswith("RECONCILIATION_REQUIRED: "):
        raise ReadbackError("portfolio research view row lost its reconciliation prefix")
    if row_view.get("verdict") == "ADD" or "research verdict ADD" in reason:
        raise ReadbackError("portfolio research view named an ADD verdict")
    if mode == "observe":
        if " | [research-observe] " + PF_RESEARCH_TAG + " " not in reason:
            raise ReadbackError("observe portfolio research view tag missing")
        return
    if " | " + PF_RESEARCH_TAG + " " not in reason or "[research-observe]" in reason or \
            not reason.endswith("; not executable"):
        raise ReadbackError("enforce portfolio research view tag missing")
    # v1.2.1: the probe has every input, so a declared enforce must publish
    # every research amount; a blank one is a regression, not a pass.
    for key in _RESEARCH_ROW_KEYS:
        if action.get(key) is None or not _research_amount(key, action[key]):
            raise ReadbackError("enforce portfolio research view did not publish: " + key)
    for key in _RESEARCH_KPI_KEYS:
        if payload["kpis"].get(key) is None or not _research_amount(key, payload["kpis"][key]):
            raise ReadbackError("enforce portfolio research view did not publish KPI: " + key)
    holdings = payload["kpis"].get("holdings_value_sar")
    value = action.get("market_value_sar")
    if holdings is not None and (value is None or abs(holdings - value) > 1):
        raise ReadbackError("portfolio research view holdings total disagrees with its row")


def require_blocked_portfolio(payload, expected_version, expected_research_view="off"):
    # v1.2.0: off keeps every v1.1.1 check, order and message; only a
    # declared enforce may show research display amounts (see header).
    if expected_research_view not in PF_RESEARCH_VIEW_MODES:
        raise ReadbackError("unsupported expected portfolio research view mode")
    research = expected_research_view == "enforce"
    if payload.get("version") != expected_version or payload.get("status") != "ok":
        raise ReadbackError("portfolio actions version or status differs from source")
    require_uncertified(payload, "portfolio probe")
    meta = payload["meta"]
    versions, route = meta.get("versions"), meta.get("route")
    if not isinstance(versions, dict) or not isinstance(route, dict) or \
            versions.get("portfolio_actions") != expected_version or \
            route.get("portfolio_actions_version") != expected_version:
        raise ReadbackError("portfolio actions runtime version attestation missing")
    kpis = payload.get("kpis")
    if not isinstance(kpis, dict):
        raise ReadbackError("portfolio KPI envelope missing")
    for key in ("deployable_sar", "adds_funded_sar", "proceeds_pending_sar", "capital_unallocated_sar"):
        value = kpis.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value != 0:
            raise ReadbackError("nonzero or invalid withheld portfolio KPI: " + key)
    for key in ("portfolio_value_sar", "holdings_value_sar", "cash_sar", "cash_pct",
                "cost_basis_sar", "pnl_sar", "pnl_pct"):
        if research and key in _RESEARCH_KPI_KEYS and key in kpis and _research_amount(key, kpis[key]):
            continue
        if key not in kpis or kpis[key] is not None:
            raise ReadbackError("uncertified portfolio amount was published: " + key)
    actions = payload.get("actions")
    if not isinstance(actions, list) or len(actions) != 1 or not isinstance(actions[0], dict):
        raise ReadbackError("portfolio probe did not return exactly one protective row")
    action = actions[0]
    if action.get("symbol") != PORTFOLIO_PROBE_SYMBOL or action.get("action") != "BLOCK":
        raise ReadbackError("unreconciled synthetic holding was not blocked")
    for key in ("suggested_delta_sar", "suggested_delta_shares", "proceeds_sar"):
        value = action.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value != 0:
            raise ReadbackError("unreconciled holding produced execution money: " + key)
    for key in ("market_value_sar", "cost_sar", "pnl_sar", "pnl_pct", "weight_pct",
                "post_trade_weight_pct", "stop_sar", "tp1_sar", "tp2_sar", "funds_from"):
        if research and key in _RESEARCH_ROW_KEYS and key in action and _research_amount(key, action[key]):
            continue
        if key not in action or action[key] is not None:
            raise ReadbackError("unreconciled holding produced an execution level: " + key)
    detail = action.get("detail")
    if not isinstance(detail, dict) or detail.get("execution_ready") is not False or \
            detail.get("position_evidence_matched") is not False or \
            "sector_weight_pct" not in detail or detail["sector_weight_pct"] is not None:
        raise ReadbackError("portfolio row execution or sector weight was not withheld")
    if payload.get("sector_summary") != []:
        raise ReadbackError("unreconciled portfolio published sector weights")
    alerts = payload.get("alerts")
    if not isinstance(alerts, list) or len(alerts) != 1 or not isinstance(alerts[0], dict) or \
            alerts[0].get("type") != "portfolio_inputs_unverified":
        raise ReadbackError("portfolio protective alert missing or replaced with funding advice")
    if expected_research_view != "off":
        _require_research_view(payload, action, expected_research_view)


def verify(request, expected_commit, expected_engine, expected_builder, expected_contract=1,
           expected_portfolio_actions=None, expected_pf_research_view="off"):
    if expected_pf_research_view not in PF_RESEARCH_VIEW_MODES:
        raise ReadbackError("unsupported expected portfolio research view mode")
    if expected_portfolio_actions is None:
        expected_portfolio_actions = source_version("core/analysis/portfolio_actions.py", "PORTFOLIO_ACTIONS_VERSION")
    health = request("/health")
    deployed = (health.get("deploy") or {}).get("render_git_commit")
    if deployed != expected_commit:
        raise ReadbackError("deployed commit differs from workflow commit")
    if not health.get("ready") or health.get("engine_version") != expected_engine:
        raise ReadbackError("deployed engine is not the expected ready version")
    body = {
        "rows": [{"symbol": "AAPL.US", "name": "Blocked synthetic rollout probe",
                  "current_price": 100.0, "market": "NASDAQ/NYSE", "currency": "USD",
                  "investability_status": "BLOCKED",
                  "block_reason": "read-only rollout probe; never allocate"}],
        "criteria": {"Board Funding Stage": "research"},
        "portfolio": {"cash_available_sar": 0.0},
        "fx_rates": {"SAR": 1.0, "USD": 3.75},
    }
    research = request(OPPORTUNITY_PATH, body, True)
    require_zero_money(research)
    if research.get("version") != expected_builder or research.get("status") not in ("ok", "no_candidates"):
        raise ReadbackError("deployed builder version differs from source")
    board = (research.get("meta") or {}).get("board_funding") or {}
    snapshot = board.get("snapshot")
    if board.get("snapshot_available") is not True or not isinstance(snapshot, dict):
        raise ReadbackError("authenticated signed research snapshot unavailable")
    if snapshot.get("rows") != [] or not snapshot.get("snapshot_id"):
        raise ReadbackError("blocked probe did not produce an empty signed snapshot")
    if board.get("contract_version") != expected_contract or \
            board.get("stage") != "research" or \
            board.get("snapshot_id") != snapshot.get("snapshot_id") or \
            snapshot.get("contract_version") != expected_contract or \
            snapshot.get("builder_version") != expected_builder:
        raise ReadbackError("research snapshot has an unexpected contract")
    replay = copy.deepcopy(body)
    replay["rows"] = []
    replay["criteria"].update({"Board Funding Stage": "allocate",
                              "Board Funding Symbols": [],
                              "Board Funding Snapshot": snapshot})
    allocated = request(OPPORTUNITY_PATH, replay, True)
    require_zero_money(allocated)
    require_uncertified(allocated, "allocation replay")
    allocated_board = (allocated.get("meta") or {}).get("board_funding") or {}
    if allocated.get("version") != expected_builder or \
            allocated.get("status") not in ("ok", "no_candidates") or \
            allocated_board.get("contract_version") != expected_contract or \
            allocated_board.get("snapshot_id") != snapshot["snapshot_id"] or \
            allocated_board.get("eligible_symbols") != [] or \
            allocated_board.get("stage") != "allocate" or \
            allocated_board.get("snapshot_available") is not True:
        raise ReadbackError("signed allocation replay rejected")
    for section, key, value in (("portfolio", "cash_available_sar", 1.0),
                               ("fx_rates", "USD", 4.0)):
        altered = copy.deepcopy(replay)
        altered[section][key] = value
        rejected = request(OPPORTUNITY_PATH, altered, True)
        require_zero_money(rejected)
        if rejected.get("status") != "board_funding_mismatch":
            raise ReadbackError("changed " + section + " basis was accepted")
    stamp = datetime.now(UTC).isoformat()
    portfolio_probe = {
        "rows": [{"Symbol": PORTFOLIO_PROBE_SYMBOL, "Name": "Unreconciled synthetic readback probe",
                  "Currency": "SAR", "Position Qty": 10, "Buy Price": 90, "Current Price": 100,
                  "Intrinsic Value": 130, "Sector": "Energy", "Market": "Tadawul",
                  "Forecast Reliability Score": 90, "Data Quality Score": 90,
                  "Recommendation": "BUY", "Investability Status": "INVESTABLE", "Risk Bucket": "Moderate",
                  "Data Provider": "EODHD", "Last Updated": stamp,
                  "Warnings": "acquisition_status:success; acquisition_provider:EODHD; acquisition_acquired_at:"
                              + stamp + "; acquisition_quote_asof:" + stamp}],
        "controls": {"Cash Available (SAR)": 10_000, "target_cash_pct": 10,
                     "max_position_pct": 20, "max_sector_pct": 30, "trust_gate_enabled": False},
        "fx_rates": {"SAR": 1},
    }
    # These are fabricated probe inputs, with no account/custody capture and
    # no reconciliation packet. The real route must withhold all execution.
    protective = request(PORTFOLIO_ACTIONS_PATH, portfolio_probe, True)
    require_blocked_portfolio(protective, expected_portfolio_actions, expected_pf_research_view)
    final_health = request("/health")
    if (final_health.get("deploy") or {}).get("render_git_commit") != expected_commit:
        raise ReadbackError("deployment changed during readback")
    margin_mode = (final_health.get("engine_gates") or {}).get("margin_publish")
    margin_mode = margin_mode if isinstance(margin_mode, str) and margin_mode in {"off", "observe", "enforce"} else None
    return {"ok": True, "checked_at_utc": datetime.now(UTC).isoformat(),
            "commit": deployed, "engine_version": expected_engine,
            "builder_version": expected_builder, "signed_research": True,
            "empty_allocation": True, "cash_and_fx_changes_rejected": True,
            "uncertified_replay_withheld": True,
            "portfolio_actions_version": expected_portfolio_actions,
            "unreconciled_portfolio_blocked": True,
            "margin_publish": margin_mode,
            # v1.2.0: recorded only when declared, so off stays byte-identical.
            **({"pf_research_view": expected_pf_research_view}
               if expected_pf_research_view != "off" else {}),
            "scope": "blocked synthetic board and unreconciled holding API probes; Apps Script and live coverage not attested"}


def _redact_credential(value, token):
    if isinstance(value, str):
        return value.replace(token, "[REDACTED]") if token else value
    if isinstance(value, dict):
        return {key: _redact_credential(item, token) for key, item in value.items()}
    if isinstance(value, list):
        return [_redact_credential(item, token) for item in value]
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--workflow-source-sha", default=os.getenv("GITHUB_SHA"))
    parser.add_argument("--tiny-quote-readback", action="store_true", default=False,
                        help="Capture actual three-symbol crypto forecast/ROI precision evidence; no Sheet writes")
    parser.add_argument("--output", default="artifacts/core-repair-readback.json")
    # v1.2.0: the portfolio research view mode this run expects (see header).
    parser.add_argument("--expect-pf-research-view", default="off",
                        help="Declared TFB_PF_RESEARCH_VIEW mode: off (default), observe or enforce")
    args = parser.parse_args()
    research_view = str(args.expect_pf_research_view or "").strip().lower()
    token = os.getenv("BACKEND_TOKEN", "").strip()
    request = None
    try:
        if not token:
            raise ReadbackError("BACKEND_TOKEN is not configured")
        if not re.fullmatch(r"[0-9a-f]{40}", args.expected_commit):
            raise ReadbackError("expected backend commit must be a full 40-character hexadecimal SHA")
        if not re.fullmatch(r"[0-9a-f]{40}", str(args.workflow_source_sha)):
            raise ReadbackError("workflow source SHA must be a full 40-character hexadecimal SHA")
        if args.workflow_source_sha != args.expected_commit:
            raise ReadbackError("workflow source SHA differs from expected backend commit")
        if research_view not in PF_RESEARCH_VIEW_MODES:
            raise ReadbackError("unsupported expected portfolio research view mode")
        request = transport(token)
        # v1.2.0: off passes exactly the v1.1.1 arguments.
        research_kwargs = {} if research_view == "off" else {"expected_pf_research_view": research_view}
        result = verify(request, args.expected_commit,
                        source_version("core/data_engine_v2.py", "__version__"),
                        source_version("core/analysis/opportunity_builder.py", "OPPORTUNITY_BUILDER_VERSION"),
                        int(source_version("core/analysis/opportunity_builder.py", "_BOARD_FUNDING_VERSION")),
                        source_version("core/analysis/portfolio_actions.py", "PORTFOLIO_ACTIONS_VERSION"),
                        **research_kwargs)
    except ReadbackError as exc:
        result = {"ok": False, "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 - never serialize arbitrary backend exception text
        # Never serialize an unexpected backend envelope or credential into
        # the artifact. A malformed response still leaves a useful failure.
        result = {"ok": False, "error": "readback failed: " + type(exc).__name__}
    result["expected_backend_commit"] = args.expected_commit
    result["workflow_source_sha"] = args.workflow_source_sha
    if research_view != "off":
        result["expected_pf_research_view"] = (
            research_view if research_view in PF_RESEARCH_VIEW_MODES else "unsupported")
    result["readback_version"] = SCRIPT_VERSION
    result["helper_source_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    run_id = os.getenv("GITHUB_RUN_ID", "")
    result["workflow_run_id"] = run_id if re.fullmatch(r"\d+", run_id) else None
    if args.tiny_quote_readback:
        if request is None:
            result["tiny_quote_readback"] = {"ok": False, "status": "failed", "rows": [],
                                             "reasons": ["authenticated_transport_unavailable"]}
        else:
            try:
                result["tiny_quote_readback"] = verify_tiny_quotes(
                    request, args.expected_commit, source_version("core/data_engine_v2.py", "__version__"),
                    source_version("routes/analysis_sheet_rows.py", "ANALYSIS_SHEET_ROWS_VERSION"), args.workflow_source_sha)
            except Exception as exc:  # noqa: BLE001 - source extraction failures must leave an artifact
                result["tiny_quote_readback"] = {"ok": False, "status": "failed", "rows": [],
                    "reasons": ["quote source verification failed: " + type(exc).__name__]}
        result["ok"] = result["ok"] and result["tiny_quote_readback"]["ok"]
    result["http_receipts"] = getattr(request, "receipts", [])
    # Only explicit public-symbol fields and byte receipts are serialized;
    # additionally remove any accidental exact credential echo from strings.
    serialized = json.dumps(_redact_credential(result, token), indent=2, allow_nan=False)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(serialized + "\n")
    print(serialized)
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
