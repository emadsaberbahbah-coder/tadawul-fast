"""run_dashboard_sync v6.71.0 BATCH-CONC: bounded concurrent market-page batches.

Production evidence (GitHub Actions runs 38711216198, 38774823513, 38821487562):
Global_Markets (6,609 symbols) is fetched as a strictly serial chain of 268
POST /v1/analysis/sheet-rows calls (25 symbols, median ~10.0 s each): 50.9-55.1
min per pass plus a 48.6-51.4 min recovery replay, ~92% of every daily_sync run.
Every call returned HTTP 200; EODHD peaked at 32% of its daily quota.

TFB_SYNC_BATCH_CONCURRENCY = k (default 1 = the sequential loop). These tests
prove, with a fake backend whose per-batch latencies make completion order
differ from launch order:
  * k unset == k = 1 (same rows, same warnings, no BATCH-CONC line);
  * k > 1 builds the same page as k = 1 (rows, order, dedupe, identity bleed,
    failed batches, retry pass, last_err, TIME-BUDGET line), plus exactly one
    BATCH-CONC line;
  * the semaphore bound holds (max in flight == k, never more);
  * a budget that runs out after N launches stops launching at the same batch
    the sequential loop stops at;
  * a raised batch propagates the same exception as k = 1.
"""
import asyncio
from datetime import datetime, timezone
from unittest import mock

import pytest

from core.sheets.schema_registry import get_sheet_headers
from scripts import run_dashboard_sync as sync

NOW = datetime(2026, 10, 16, 0, 30, tzinfo=timezone.utc)
HEADERS = get_sheet_headers("Global_Markets")
SYM_I = HEADERS.index("Symbol")
PX_I = HEADERS.index("Current Price")
EP = "/v1/analysis/sheet-rows"
SYMBOLS = [f"S{i:02d}.US" for i in range(11)]          # batch size 2 -> 6 batches
# Per-batch latency (s), indexed by batch: later batches finish first.
JITTER = [0.004, 0.030, 0.006, 0.022, 0.002, 0.012, 0.018, 0.001, 0.009, 0.015]


def quote(symbol, price, **changes):
    row = {"Symbol": symbol, "Name": symbol.split(".")[0] + " Corp", "Current Price": price,
           "Data Provider": "eodhd", "Last Updated (UTC)": "2026-10-16T00:20:00Z",
           "Last Updated (Riyadh)": "2026-10-16T03:20:00+03:00", "Warnings": ""}
    row.update(changes)
    return row


def matrix(rows):
    return [[row.get(h, "") for h in HEADERS] for row in rows]


class FakeBackend:
    """Async backend: per-batch latency, scripted failures, call/flight log.

    `fail` maps a request_id suffix ("b3", "r3") or a batch number to a failure
    kind: "http" (non-200 error tuple), "raise" (exception), "nodict",
    "noheaders". `extra` maps a batch number to rows appended to its answer
    (identity-bleed / duplicate probes). `endpoints` limits which endpoints
    answer (others return HTTP 404)."""

    def __init__(self, jitter=JITTER, fail=None, extra=None, endpoints=None):
        self.jitter = list(jitter)
        self.fail = dict(fail or {})
        self.extra = dict(extra or {})
        self.endpoints = endpoints
        self.calls = []          # (endpoint, request suffix) in START order
        self.completed = []      # request suffix in COMPLETION order
        self.in_flight = 0
        self.max_in_flight = 0

    async def post_json(self, endpoint, payload):
        suffix = str(payload["request_id"]).rsplit("-", 1)[-1]
        num = int(suffix[1:])
        self.calls.append((endpoint, suffix))
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await asyncio.sleep(self.jitter[(num - 1) % len(self.jitter)])
            if self.endpoints is not None and endpoint not in self.endpoints:
                return None, "HTTP 404: not found", 404
            kind = self.fail.get(suffix)
            if kind == "http":
                return None, f"HTTP 502: bad gateway {suffix}", 502
            if kind == "raise":
                raise RuntimeError(f"transport exploded on {suffix}")
            if kind == "nodict":
                return ["not", "a", "dict"], None, 200
            if kind == "noheaders":
                return {"headers": [], "rows_matrix": []}, None, 200
            rows = [quote(s, 100.0 + num + SYMBOLS.index(s) / 100.0 if s in SYMBOLS else 1.0)
                    for s in payload["symbols"]]
            rows.extend(self.extra.get(num, []))
            return {"headers": HEADERS, "rows_matrix": matrix(rows)}, None, 200
        finally:
            self.in_flight -= 1
            self.completed.append(suffix)


def _set_k(monkeypatch, k):
    if k is None:
        monkeypatch.delenv("TFB_SYNC_BATCH_CONCURRENCY", raising=False)
    else:
        monkeypatch.setenv("TFB_SYNC_BATCH_CONCURRENCY", str(k))


def _fetch(monkeypatch, k, backend, symbols=SYMBOLS, env=None, budget=None):
    """Call the real _fetch_market_rows_batched once; return everything a
    caller can observe."""
    base = {"TFB_SYNC_SYMBOL_BATCH_SIZE": "2", "TFB_SYNC_BATCH_IDENTITY": "1",
            "TFB_SYNC_BATCH_RETRY": "1", "TFB_SYNC_TIME_BUDGET_SEC": "0",
            "TFB_SYNC_BATCH_DELAY_MS": "0", "TFB_SYNC_SAFE_GATEWAYS": "1"}
    base.update(env or {})
    for name, value in base.items():
        monkeypatch.setenv(name, value)
    _set_k(monkeypatch, k)
    task = sync.TaskSpec("GLOBAL_MARKETS", "Global_Markets", "analysis")
    res = sync.TaskResult(key=task.key, sheet_name=task.sheet_name, status="pending",
                          start_utc=NOW.isoformat())
    with mock.patch.object(sync, "_time_budget_exceeded", budget or (lambda: False)):
        out = asyncio.run(sync._fetch_market_rows_batched(
            backend, task, list(symbols), {"sheet_name": "Global_Markets"}, "analysis", res))
    return out, res


def _strip_conc(warnings):
    return [w for w in warnings if not w.startswith("[BATCH-CONC")]


def _conc_lines(warnings):
    return [w for w in warnings if w.startswith("[BATCH-CONC")]


def _field(line, name):
    return line.split(f" {name}=", 1)[1].split(" ", 1)[0]


# --------------------------------------------------------------------------- env


@pytest.mark.parametrize("raw,want", [
    (None, 1), ("", 1), ("x", 1), ("0", 1), ("1", 1), ("2", 2), ("9", 4),
    ("-3", 1), (" 3 ", 3), ("2.5", 1), ("4", 4),
])
def test_env_parsing_and_clamp(monkeypatch, raw, want):
    _set_k(monkeypatch, raw)
    assert sync._sync_batch_concurrency() == want


def test_line_format_has_no_braces_and_nearest_rank_percentiles():
    line = sync._batch_conc_line("Global_Markets", 2, 268, 268, 1612.345,
                                 [10.0, 9.0, 12.0, 30.0, 11.0], 0)
    assert "{" not in line and "}" not in line
    assert line == ("[BATCH-CONC v6.71.0] page=Global_Markets k=2 batches=268 launched=268 "
                    "wall_s=1612.35 p50_s=11.00 p90_s=30.00 failed=0")
    assert sync._batch_conc_pct([], 0.5) == 0.0
    assert sync._batch_conc_pct([4.0], 0.9) == 4.0


# ------------------------------------------------------------- direct fetch path


def test_k_unset_equals_k1_and_emits_no_line(monkeypatch):
    out_u, res_u = _fetch(monkeypatch, None, FakeBackend(fail={"b3": "http"}))
    b1 = FakeBackend(fail={"b3": "http"})
    out_1, res_1 = _fetch(monkeypatch, 1, b1)
    assert out_u == out_1
    assert res_u.warnings == res_1.warnings
    assert not _conc_lines(res_1.warnings)
    assert b1.max_in_flight == 1
    # the failed batch went to the serial retry pass and was recovered
    assert ("r3" in [c[1] for c in b1.calls]
            and any("BATCH-RETRY" in w and "recovered 1/1" in w for w in res_1.warnings))


@pytest.mark.parametrize("k", [2, 3, 4])
def test_concurrent_page_equals_sequential_page(monkeypatch, k):
    seq = FakeBackend()
    out_1, res_1 = _fetch(monkeypatch, 1, seq)
    conc = FakeBackend()
    out_k, res_k = _fetch(monkeypatch, k, conc)
    _headers, rows, used, last_err = out_k
    assert out_k == out_1
    assert [r[SYM_I] for r in rows] == SYMBOLS and used == EP and last_err is None
    assert _strip_conc(res_k.warnings) == res_1.warnings
    # completion order really differed from launch order
    launched = [c[1] for c in conc.calls]
    assert conc.completed != launched, (conc.completed, launched)
    # the bound: never more than k in flight, and k reached
    assert conc.max_in_flight == k
    assert seq.max_in_flight == 1
    lines = _conc_lines(res_k.warnings)
    assert len(lines) == 1, lines
    assert f"page=Global_Markets k={k} batches=6 launched=6 " in lines[0], lines
    assert _field(lines[0], "failed") == "0"
    assert "{" not in lines[0] and "}" not in lines[0]


def test_semaphore_bound_on_a_long_page(monkeypatch):
    symbols = [f"T{i:03d}.US" for i in range(40)]           # 20 batches
    for k in (2, 3):
        backend = FakeBackend()
        (_h, rows, _u, _e), res = _fetch(monkeypatch, k, backend, symbols=symbols)
        assert backend.max_in_flight == k
        assert [r[SYM_I] for r in rows] == symbols
        assert _field(_conc_lines(res.warnings)[0], "launched") == "20"


def test_first_occurrence_and_bleed_follow_batch_order_not_completion_order(monkeypatch):
    # S02.US is requested in batch 2 and again in batch 4 (duplicate request);
    # both run in the concurrent phase and batch 4 answers FIRST with another
    # price. First occurrence in BATCH order (batch 2's row) must win, as at
    # k = 1. Batch 2 also repeats S02.US inside its own answer (duplicate) and
    # batch 5 answers S03.US, which it did not request (identity bleed).
    symbols = ["S00.US", "S01.US", "S02.US", "S03.US", "S04.US", "S05.US", "S02.US", "S06.US",
               "S07.US", "S08.US"]
    jitter = [0.002, 0.030, 0.020, 0.001, 0.002]
    extra = {5: [quote("S03.US", 999.0)], 2: [quote("S02.US", 555.0)]}
    out_1, res_1 = _fetch(monkeypatch, 1, FakeBackend(jitter=jitter, extra=extra), symbols=symbols)
    conc = FakeBackend(jitter=jitter, extra=extra)
    out_3, res_3 = _fetch(monkeypatch, 3, conc, symbols=symbols)
    assert conc.completed.index("b4") < conc.completed.index("b2")
    assert out_3 == out_1
    by_sym = {r[SYM_I]: r[PX_I] for r in out_3[1]}
    assert by_sym["S02.US"] == 102.02                     # batch 2, not batch 4 (104.02)
    assert by_sym["S03.US"] == 102.03                     # not the bled 999.0
    assert _strip_conc(res_3.warnings) == res_1.warnings
    assert any("dropped 1 cross-batch row(s), 2 duplicate-symbol row(s)" in w for w in res_1.warnings)


def test_positional_legacy_path_keeps_batch_order(monkeypatch):
    env = {"TFB_SYNC_BATCH_IDENTITY": "0"}
    out_1, res_1 = _fetch(monkeypatch, 1, FakeBackend(), env=env)
    out_3, res_3 = _fetch(monkeypatch, 3, FakeBackend(), env=env)
    assert out_3 == out_1
    assert [r[SYM_I] for r in out_3[1]] == SYMBOLS
    assert _strip_conc(res_3.warnings) == res_1.warnings


@pytest.mark.parametrize("kind", ["http", "nodict", "noheaders"])
def test_failed_batch_handled_like_sequential(monkeypatch, kind):
    # b3 fails in the first pass and in the retry pass; b5 fails only first.
    fail = {"b3": kind, "r3": kind, "b5": kind}
    seq = FakeBackend(fail=fail)
    out_1, res_1 = _fetch(monkeypatch, 1, seq)
    conc = FakeBackend(fail=fail)
    out_3, res_3 = _fetch(monkeypatch, 3, conc)
    assert out_3 == out_1
    assert out_1[3] is not None and out_1[3].startswith(EP + " -> ")      # last_err kept
    assert _strip_conc(res_3.warnings) == res_1.warnings
    def retry(b):
        return [c for c in b.calls if c[1].startswith("r")]
    assert retry(conc) == retry(seq) == [(EP, "r3"), (EP, "r5")]
    assert any("recovered 1/2" in w for w in res_1.warnings)
    missing = {"S04.US", "S05.US"}
    assert {r[SYM_I] for r in out_3[1]} == set(SYMBOLS) - missing
    assert _field(_conc_lines(res_3.warnings)[0], "failed") == "2"


def test_failed_batches_without_retry_are_identical(monkeypatch):
    fail = {"b2": "http", "b6": "http"}
    out_1, res_1 = _fetch(monkeypatch, 1, FakeBackend(fail=fail), env={"TFB_SYNC_BATCH_RETRY": "0"})
    conc = FakeBackend(fail=fail)
    out_3, res_3 = _fetch(monkeypatch, 3, conc, env={"TFB_SYNC_BATCH_RETRY": "0"})
    assert out_3 == out_1 and _strip_conc(res_3.warnings) == res_1.warnings
    assert "HTTP 502: bad gateway b6" in out_3[3]                         # last in BATCH order
    assert not [c for c in conc.calls if c[1].startswith("r")]


def test_raised_batch_propagates_the_same_exception(monkeypatch):
    fail = {"b3": "raise"}
    with pytest.raises(RuntimeError, match="transport exploded on b3"):
        _fetch(monkeypatch, 1, FakeBackend(fail=fail))
    conc = FakeBackend(fail=fail)
    with pytest.raises(RuntimeError, match="transport exploded on b3"):
        _fetch(monkeypatch, 3, conc)
    assert conc.max_in_flight <= 3


def test_endpoint_resolution_stays_serial(monkeypatch):
    # Unsafe gateways: 8 candidates, only the 3rd answers; batch 1 also fails
    # outright on all of them, so batch 2 resolves the endpoint. Everything up
    # to the resolving batch runs serially, then k streams use that endpoint.
    good = "/v1/ai/sheet-rows"
    env = {"TFB_SYNC_SAFE_GATEWAYS": "0"}
    fail = {"b1": "http"}
    seq = FakeBackend(fail=fail, endpoints={good})
    out_1, res_1 = _fetch(monkeypatch, 1, seq, env=env)
    conc = FakeBackend(fail=fail, endpoints={good})
    out_3, res_3 = _fetch(monkeypatch, 3, conc, env=env)
    assert out_3 == out_1 and out_3[2] == good
    assert _strip_conc(res_3.warnings) == res_1.warnings
    assert sorted(conc.calls) == sorted(seq.calls)
    first_pass = [c for c in conc.calls if c[1].startswith("b")]
    n_serial = sum(1 for c in first_pass if c[1] in ("b1", "b2"))
    assert first_pass[:n_serial] == [c for c in seq.calls if c[1] in ("b1", "b2")]
    assert {c[0] for c in first_pass[n_serial:]} == {good}


def _budget_after(n_checks):
    """Fake _time_budget_exceeded: the first n_checks calls answer False. Call
    1 is the pre-fetch page check, call 1+i guards batch i (both modes)."""
    seen = {"n": 0}

    def exceeded():
        seen["n"] += 1
        return seen["n"] > n_checks
    return exceeded


@pytest.mark.parametrize("launches", [1, 2, 4])
def test_budget_exhausted_after_n_launches(monkeypatch, launches):
    fail = {"b2": "http"}
    seq = FakeBackend(fail=fail)
    out_1, res_1 = _fetch(monkeypatch, 1, seq, budget=_budget_after(launches))
    conc = FakeBackend(fail=fail)
    out_3, res_3 = _fetch(monkeypatch, 3, conc, budget=_budget_after(launches))
    assert out_3 == out_1
    assert _strip_conc(res_3.warnings) == res_1.warnings
    assert sorted(conc.calls) == sorted(seq.calls)
    assert [c[1] for c in sorted(conc.calls)] == [f"b{i + 1}" for i in range(launches)]
    ok = launches - (1 if launches >= 2 else 0)
    assert any(f"exhausted after {ok}/6 batch(es)" in w for w in res_1.warnings), res_1.warnings
    if launches >= 2:      # the failed batch is not retried once the budget is gone
        assert any("BATCH-RETRY" in w and "budget ended" in w for w in res_1.warnings)
    assert _field(_conc_lines(res_3.warnings)[0], "launched") == str(launches)


def test_per_stream_delay_keeps_results(monkeypatch):
    env = {"TFB_SYNC_BATCH_DELAY_MS": "3"}
    out_1, res_1 = _fetch(monkeypatch, 1, FakeBackend(), env=env)
    conc = FakeBackend()
    out_2, res_2 = _fetch(monkeypatch, 2, conc, env=env)
    assert out_2 == out_1 and _strip_conc(res_2.warnings) == res_1.warnings
    assert conc.max_in_flight <= 2


# --------------------------------------------------------------- real runner


PRIOR = [quote(s, 50.0, **{"Last Updated (UTC)": "2026-10-15T00:20:00Z"}) for s in SYMBOLS]


def _run_page(monkeypatch, k, fail=None, budget=None):
    backend = FakeBackend(fail=fail)

    class Writer:
        def __init__(self):
            self.published = []

        def _get_service(self):
            return object()

        def read_values(self, *_a, **_k):
            return [HEADERS] + matrix(PRIOR)

        def write_table(self, _sid, _page, _start, headers, rows):
            self.published = [list(r) for r in rows]
            return len(rows)

        def clear_from(self, *_a, **_k):
            pass

    env = {"TFB_MARKET_SYMBOL_READBACK": "0", "TFB_SYNC_SYMBOL_BATCH_SIZE": "2",
           "TFB_SYNC_BATCH_DELAY_MS": "0", "TFB_SYNC_TIME_BUDGET_SEC": "0",
           "TFB_SYNC_BATCH_RETRY": "1", "TFB_SYNC_BATCH_IDENTITY": "1",
           "TFB_SYNC_PERSISTENCE_HARD": "0", "TFB_SYNC_ROW_ID_FIREWALL": "1",
           "TFB_SYNC_NAME_DEDUP_MODE": "off", "TFB_SYNC_OHLC_LAKE": "0",
           "TFB_SYNC_FALSE_GREEN_SCREEN": "0", "TFB_SYNC_STATUS_STAMP": "0",
           "TFB_SYNC_FETCHFAIL_TRUTH": "off", "TFB_SYNC_IDENTITY_TRIPWIRE": "0",
           "TFB_SYNC_COHERENCE_TRIPWIRE": "0", "TFB_SYNC_IDFW_RUNLOG": "0",
           "TFB_SYNC_EMPTY_RETRY": "0"}
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    _set_k(monkeypatch, k)
    writer = Writer()
    with mock.patch.object(sync, "_utc_now", return_value=NOW), \
            mock.patch.object(sync, "_read_symbols", return_value=list(SYMBOLS)), \
            mock.patch.object(sync, "_time_budget_exceeded", budget or (lambda: False)):
        result = asyncio.run(sync._run_one_task(sync.TaskSpec("GLOBAL_MARKETS", "Global_Markets", "analysis"),
                                                "offline", "A1", -1, False, False, backend, writer))
    return result, writer.published, backend


def _comparable(result):
    return (result.status, result.error, _strip_conc(result.warnings))


def test_real_runner_k_unset_equals_k1(monkeypatch):
    r_u, p_u, b_u = _run_page(monkeypatch, None, fail={"b3": "http", "r3": "http"})
    r_1, p_1, b_1 = _run_page(monkeypatch, 1, fail={"b3": "http", "r3": "http"})
    assert p_u and p_u == p_1
    assert r_u.warnings == r_1.warnings and not _conc_lines(r_1.warnings)
    assert (r_u.status, r_u.error) == (r_1.status, r_1.error)
    assert b_u.calls == b_1.calls and b_1.max_in_flight == 1


@pytest.mark.parametrize("fail", [None, {"b3": "http", "r3": "http", "b5": "http"}])
def test_real_runner_k3_publishes_the_k1_page(monkeypatch, fail):
    r_1, p_1, _b1 = _run_page(monkeypatch, 1, fail=fail)
    r_3, p_3, b_3 = _run_page(monkeypatch, 3, fail=fail)
    assert p_1 and p_3 == p_1
    assert _comparable(r_3) == _comparable(r_1)
    assert sorted(b_3.calls) == sorted(_b1.calls)
    assert b_3.max_in_flight == 3 and b_3.completed != [c[1] for c in b_3.calls]
    lines = _conc_lines(r_3.warnings)
    assert len(lines) == 1 and "page=Global_Markets k=3 batches=6 launched=6 " in lines[0], lines
    assert _field(lines[0], "failed") == ("0" if fail is None else "2")


def test_real_runner_raised_batch_fails_the_page_alike(monkeypatch):
    r_1, p_1, _ = _run_page(monkeypatch, 1, fail={"b4": "raise"})
    r_3, p_3, _ = _run_page(monkeypatch, 3, fail={"b4": "raise"})
    assert r_1.status == "failed" and "transport exploded on b4" in (r_1.error or "")
    assert (r_3.status, r_3.error) == (r_1.status, r_1.error)
    assert p_3 == p_1
    lines = _conc_lines(r_3.warnings)
    assert len(lines) == 1 and _field(lines[0], "failed") == "1", lines


def test_real_runner_budget_after_three_launches(monkeypatch):
    r_1, p_1, b_1 = _run_page(monkeypatch, 1, budget=_budget_after(3))
    r_3, p_3, b_3 = _run_page(monkeypatch, 3, budget=_budget_after(3))
    assert sorted(b_3.calls) == sorted(b_1.calls) == [(EP, "b1"), (EP, "b2"), (EP, "b3")]
    assert p_3 == p_1
    assert _comparable(r_3) == _comparable(r_1)
    assert any("exhausted after 3/6 batch(es)" in w for w in r_1.warnings)
