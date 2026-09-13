"""P-130 regression battery — routes/investment_advisor.py v2.18.0.

TypeError retry discrimination at the twin sites (_call_candidate,
_auth_passed): only SIGNATURE-mismatch TypeErrors may walk the kwargs
variants; a TypeError from inside the called function's body must ride
the existing "raised" path. Kill switch TFB_ADV_TYPEERROR_LEGACY=1
restores legacy retry-all at both sites.

Run:  python tests/test_adv_typeerror_retry_p130.py   (or pytest)
No network. Deterministic. Exits non-zero on failure.
"""
import asyncio
import importlib.util
import os
import sys

# --- locate the module: repo layout first, file fallback second ---------------
def _load():
    try:
        from routes import investment_advisor as ia  # type: ignore
        return ia
    except Exception:
        here = os.path.dirname(os.path.abspath(__file__))
        for cand in (
            os.path.join(here, "..", "routes", "investment_advisor.py"),
            os.path.join(here, "investment_advisor.py"),
        ):
            cand = os.path.abspath(cand)
            if os.path.exists(cand):
                spec = importlib.util.spec_from_file_location("ia_p130", cand)
                m = importlib.util.module_from_spec(spec)
                sys.modules["ia_p130"] = m
                spec.loader.exec_module(m)
                return m
    raise RuntimeError("routes/investment_advisor.py not found")

IA = _load()

class _URL:
    path = "/v1/advanced/sheet-rows"

class _Req:
    url = _URL()
    headers = {}

def _cc(fn, kill="0"):
    os.environ["TFB_ADV_TYPEERROR_LEGACY"] = kill
    try:
        IA._call_candidate._last_call_summary = []
    except Exception:
        pass
    try:
        r, s, o = asyncio.run(IA._call_candidate(
            fn, body={"b": 1}, request=_Req(), page="P",
            limit=5, offset=0, schema_only=False))
        return {"kind": "returned", "result": r, "summary": s, "label": o}
    except BaseException as e:
        return {"kind": "raised", "exc": type(e).__name__,
                "summary": list(getattr(IA._call_candidate, "_last_call_summary", []) or [])}
    finally:
        os.environ["TFB_ADV_TYPEERROR_LEGACY"] = "0"

def _auth(auth_fn, kill="0"):
    os.environ["TFB_ADV_TYPEERROR_LEGACY"] = kill
    calls = {"n": 0}
    def counted(**kw):
        calls["n"] += 1
        return auth_fn(**kw)
    old = getattr(IA, "auth_ok", None)
    IA.auth_ok = counted
    try:
        ok = IA._auth_passed(request=_Req(), token_query="tok",
                             x_app_token=None, authorization=None)
        return bool(ok), calls["n"]
    finally:
        IA.auth_ok = old
        os.environ["TFB_ADV_TYPEERROR_LEGACY"] = "0"

# --- case functions -----------------------------------------------------------
def fn_sig_walk(*, request, body):
    return {"got": [type(request).__name__, body]}

def fn_body_te(**kw):
    raise TypeError("'NoneType' object is not subscriptable")

_MIX = {"n": 0}
def fn_mixed(**kw):
    _MIX["n"] += 1
    if _MIX["n"] <= 2:
        raise TypeError("got an unexpected keyword argument 'request'")
    raise TypeError("'NoneType' object is not subscriptable")

def fn_valueerr(**kw):
    raise ValueError("boom")

def auth_sig_only(**kw):
    extra = set(kw) - {"token"}
    if extra:
        raise TypeError("got an unexpected keyword argument '%s'" % sorted(extra)[0])
    return True

def auth_body_te(**kw):
    raise TypeError("'NoneType' object is not subscriptable")

def main():
    assert not IA._is_public_path(_URL.path)

    # T1 — signature walk still succeeds; typeerror attempts flagged retryable
    r = _cc(fn_sig_walk)
    assert r["kind"] == "returned" and r["label"] == "success", r
    assert all(rec.get("signature_retryable") is True
               for rec in r["summary"] if rec["outcome"] == "typeerror"), r

    # T2 — real body TypeError: raise after 1 attempt, flag False
    r = _cc(fn_body_te)
    assert r["kind"] == "raised" and r["exc"] == "TypeError", r
    assert len(r["summary"]) == 1 and r["summary"][0]["signature_retryable"] is False, r

    # T3 — kill switch restores legacy retry-all (9 attempts, typed_mismatch)
    r = _cc(fn_body_te, kill="1")
    assert r["kind"] == "returned" and r["label"] == "all_signatures_typed_mismatch", r
    assert len(r["summary"]) == 9, r

    # T4 — mixed: two signature misses then a body bug -> raise at attempt 3
    _MIX["n"] = 0
    r = _cc(fn_mixed)
    assert r["kind"] == "raised" and len(r["summary"]) == 3, r
    assert [x.get("signature_retryable") for x in r["summary"]] == [True, True, False], r

    # T5 — non-TypeError branch untouched
    r = _cc(fn_valueerr)
    assert r["kind"] == "raised" and r["exc"] == "ValueError" and len(r["summary"]) == 1, r

    # T6 — _auth_passed: signature walk unchanged; body bug terminal at call 1
    ok, n = _auth(auth_sig_only)
    assert ok is True and n == 6, (ok, n)
    ok, n = _auth(auth_body_te)
    assert ok is False and n == 1, (ok, n)
    ok, n = _auth(auth_body_te, kill="1")
    assert ok is False and n == 7, (ok, n)

    print("P-130 battery: T1-T6 PASS | module version", IA.INVESTMENT_ADVISOR_VERSION)

if __name__ == "__main__":
    main()
