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


def verify(request, expected_commit, expected_engine, expected_builder, expected_contract=1):
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
    final_health = request("/health")
    if (final_health.get("deploy") or {}).get("render_git_commit") != expected_commit:
        raise ReadbackError("deployment changed during readback")
    return {"ok": True, "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "commit": deployed, "engine_version": expected_engine,
            "builder_version": expected_builder, "signed_research": True,
            "empty_allocation": True, "cash_and_fx_changes_rejected": True,
            "margin_publish": (final_health.get("engine_gates") or {}).get("margin_publish"),
            "scope": "blocked synthetic API probe; Apps Script and live coverage not attested"}


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
                        int(source_version("core/analysis/opportunity_builder.py", "_BOARD_FUNDING_VERSION")))
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
