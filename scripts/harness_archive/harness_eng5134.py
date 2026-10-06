import sys, os, importlib.util
from unittest.mock import MagicMock
sys.path.insert(0,"/home/claude/repo/tadawul-fast-main")
def load(path,name):
    spec=importlib.util.spec_from_file_location(name,path); m=importlib.util.module_from_spec(spec); sys.modules[name]=m
    try:
        spec.loader.exec_module(m)
    except ModuleNotFoundError as e:
        missing=str(e).split("'")[1]; mm=MagicMock(); mm.__name__=missing; sys.modules[missing]=mm
        return load(path,name)
    return m
for k in ("TFB_TARGET_MEAN_PUBLISH",): os.environ.pop(k,None)
M=load("/home/claude/eng5134.py","eng5134"); assert M.__version__=="5.134.0"
def run(payload, sym):
    out=M._canonicalize_provider_row(dict(payload), requested_symbol=sym, normalized_symbol=sym, provider="eodhd")
    M._apply_analyst_trend_block(out); return out
# T1 - EODHD-shaped row + Yahoo-style consensus target (snake case, the live fallback shape): Target Price must now publish
base={"symbol":"2503.T","name":"Kirin Holdings","current_price":3053.0,"provider_rating":"BUY","target_mean_price":4100.0,"currency":"JPY","exchange":"Tokyo"}
o=run(base,"2503.T")
assert o.get("forecast_source")=="provider_target", o.get("forecast_source")
assert M._as_float(o.get("target_price"))==4100.0, ("target_price",o.get("target_price"))
assert abs(M._as_float(o.get("upside_downside_pct"))-(4100.0/3053.0-1.0))<1e-6, o.get("upside_downside_pct")
assert o.get("analyst_rating")=="BUY"
print("T1 PASS EODHD row + snake-case target -> forecast_source=provider_target, Target Price 4100.0, Upside %.4f"%M._as_float(o.get("upside_downside_pct")))
# T2 - camelCase provider alias (targetMeanPrice) also publishes
o2=run({"symbol":"AAPL","name":"Apple","current_price":200.0,"provider_rating":"STRONG_BUY","targetMeanPrice":250.0},"AAPL")
assert o2.get("forecast_source")=="provider_target" and M._as_float(o2.get("target_price"))==250.0, (o2.get("forecast_source"),o2.get("target_price"))
print("T2 PASS camelCase targetMeanPrice -> Target Price 250.0")
# T3 - implausible target (band) is dropped by the existing gate: NO target published, no provider_target tag
o3=run({"symbol":"UHS","name":"Universal Health","current_price":146.0,"provider_rating":"BUY","targetMeanPrice":417.0},"UHS")
assert o3.get("forecast_source")!="provider_target" and M._as_float(o3.get("target_price")) is None, (o3.get("forecast_source"),o3.get("target_price"))
print("T3 PASS implausible 417 vs 146 stays dropped: no Target Price, no provider_target (gate unchanged)")
# T4 - no target at all: fill-only, nothing invented
o4=run({"symbol":"XYZ","name":"Xyz","current_price":10.0,"provider_rating":"HOLD"},"XYZ")
assert M._as_float(o4.get("target_price")) is None and o4.get("forecast_source")!="provider_target"
print("T4 PASS no target -> no Target Price, no tag")
# T5 - kill-switch restores v5.133.0 behaviour (tag yes, Target Price blank)
os.environ["TFB_TARGET_MEAN_PUBLISH"]="0"; o5=run(base,"2503.T"); os.environ.pop("TFB_TARGET_MEAN_PUBLISH")
assert o5.get("forecast_source")=="provider_target" and M._as_float(o5.get("target_price")) is None, (o5.get("forecast_source"),o5.get("target_price"))
print("T5 PASS kill-switch: provider_target tag kept, Target Price blank (v5.133.0 behaviour reproduced)")
# T6 - control: the live defect reproduced on v5.133.0 (same input -> blank Target Price)
for k in list(sys.modules):
    if k.startswith("eng5134"): del sys.modules[k]
M0=load("/home/claude/eng5133.py","eng5133"); assert M0.__version__=="5.133.0"
o0=M0._canonicalize_provider_row(dict(base),requested_symbol="2503.T",normalized_symbol="2503.T",provider="eodhd"); M0._apply_analyst_trend_block(o0)
assert o0.get("forecast_source")=="provider_target" and M0._as_float(o0.get("target_price")) is None, (o0.get("forecast_source"),o0.get("target_price"))
print("T6 PASS control: v5.133.0 reproduces the live defect (provider_target tagged, Target Price blank) on the identical input")
print("PASS 6/6 — real engine module data_engine_v2 v5.134.0")
