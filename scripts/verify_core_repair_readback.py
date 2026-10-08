"""Read-only deployed board-contract check; never accesses Sheets or a broker."""
from __future__ import annotations

import argparse
import ast
import copy
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import urllib.error
import urllib.request

BACKEND = "https://tadawul-fast-bridge.onrender.com"
OPPORTUNITY_PATH = "/sheet-rows/opportunity-candidates"
PORTFOLIO_ACTIONS_PATH = "/sheet-rows/portfolio-actions"
PORTFOLIO_PROBE_SYMBOL = "SYNTH.SR"


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
            if len(raw) > 4_000_000:
                raise ReadbackError("backend response exceeds bound")
            result = json.loads(raw)
            if not isinstance(result, dict):
                raise ReadbackError("backend returned an invalid envelope")
            return result
        except urllib.error.HTTPError as exc:
            raise ReadbackError("backend HTTP " + str(exc.code)) from None
        except (urllib.error.URLError, TimeoutError, ValueError):
            raise ReadbackError("backend request or JSON failed") from None

    return request


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


def require_blocked_portfolio(payload, expected_version):
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


def verify(request, expected_commit, expected_engine, expected_builder, expected_contract=1,
           expected_portfolio_actions=None):
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
    stamp = datetime.now(timezone.utc).isoformat()
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
    require_blocked_portfolio(protective, expected_portfolio_actions)
    final_health = request("/health")
    if (final_health.get("deploy") or {}).get("render_git_commit") != expected_commit:
        raise ReadbackError("deployment changed during readback")
    margin_mode = (final_health.get("engine_gates") or {}).get("margin_publish")
    margin_mode = margin_mode if isinstance(margin_mode, str) and margin_mode in {"off", "observe", "enforce"} else None
    return {"ok": True, "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "commit": deployed, "engine_version": expected_engine,
            "builder_version": expected_builder, "signed_research": True,
            "empty_allocation": True, "cash_and_fx_changes_rejected": True,
            "uncertified_replay_withheld": True,
            "portfolio_actions_version": expected_portfolio_actions,
            "unreconciled_portfolio_blocked": True,
            "margin_publish": margin_mode,
            "scope": "blocked synthetic board and unreconciled holding API probes; Apps Script and live coverage not attested"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-commit", required=True)
    parser.add_argument("--output", default="artifacts/core-repair-readback.json")
    args = parser.parse_args()
    token = os.getenv("BACKEND_TOKEN", "").strip()
    if not token:
        raise SystemExit("BACKEND_TOKEN is not configured")
    try:
        result = verify(transport(token), args.expected_commit,
                        source_version("core/data_engine_v2.py", "__version__"),
                        source_version("core/analysis/opportunity_builder.py", "OPPORTUNITY_BUILDER_VERSION"),
                        int(source_version("core/analysis/opportunity_builder.py", "_BOARD_FUNDING_VERSION")),
                        source_version("core/analysis/portfolio_actions.py", "PORTFOLIO_ACTIONS_VERSION"))
    except ReadbackError as exc:
        result = {"ok": False, "error": str(exc)}
    except Exception as exc:
        # Never serialize an unexpected backend envelope or credential into
        # the artifact. A malformed response still leaves a useful failure.
        result = {"ok": False, "error": "readback failed: " + type(exc).__name__}
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
