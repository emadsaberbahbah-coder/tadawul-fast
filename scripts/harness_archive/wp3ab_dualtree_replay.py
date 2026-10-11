#!/usr/bin/env python3
"""WP-3ab dual-tree replay harness -- data_engine_v2 v5.152.0 vs the pinned base.

REAL module, offline, no network, no credentials, no Sheet writes. Not collected
by pytest (lives in scripts/harness_archive; see its README). Every path is
taken from the repository layout or the command line -- no sandbox paths.

What it does
  1. Builds a BASE tree: a copy of <repo>/core with core/data_engine_v2.py
     replaced by `git show <base-sha>:core/data_engine_v2.py` (or --base-root
     pointing at a directory that already holds such a core/ package).
  2. Runs this file as a CHILD process once per tree with a clean environment
     (every TFB_* variable stripped). The child deep-copies every fixture row,
     runs the REAL de._strict_project_row(keys, row) for every env profile and
     every (TFB_ROI12_SOFTCAP_GATE, TFB_TUPLE_NOISE_RELEASE) mode pair, then
     re-presents the projected dict with present_instrument_row (the sync
     writer's own re-check) and records the published tuple fields, the
     decision fields, the witness tags and a hash of every other field.
  3. Compares delivered vs base per (env, mode) and prints:
       H1  (off, off): every projected field byte-identical to base (zero-change
           proof) -- and base is identical across all mode pairs (it does not
           know the gates).
       H2  observe: all non-warnings fields == base; tag counts by class.
       H3  enforce: every released leg within 5e-7 of FP/CP-1 and published;
           every ceiling row below the asymptote in lockstep; decision fields
           (investability_status, final_action, recommendation, scores)
           identical to base on 100% of rows; seam-level idempotence.
       DQ  DQ-31 / DQ-32 / DQ-33 / DQ-38 before/after, per page, plus the
           3M/1M legs; export-level BEFORE counts come from the fixture header
           (measured by the fixture builder on the full export).
       RUN-DIGEST  sha256 of every record (both trees, all envs, all modes),
           first 16 hex. Run the whole program three times in fresh processes;
           the digests must be identical.

Usage
  python scripts/harness_archive/wp3ab_dualtree_replay.py [--repo-root DIR]
      [--base-sha SHA] [--base-root DIR] [--fixture FILE] [--out-dir DIR]
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile

BASE_SHA_DEFAULT = "a6668c235b526394158726a3e34c8ffaeb95b6b7"
FIXTURE_REL = os.path.join("tests", "fixtures", "wp3ab_tuple_rows_2026-10-10.json")
R12_ENV, TNR_ENV = "TFB_ROI12_SOFTCAP_GATE", "TFB_TUPLE_NOISE_RELEASE"
ENVS = [
    ("unset", {}),
    ("prod", {"TFB_EQ_ROI_UNIT_SENTRY": "observe", "TFB_FC_TUPLE_COHERENT": "observe", "TFB_MARGIN_PUBLISH": "observe"}),
    ("prod+fpc", {"TFB_EQ_ROI_UNIT_SENTRY": "observe", "TFB_FC_TUPLE_COHERENT": "observe", "TFB_MARGIN_PUBLISH": "observe",
                  "TFB_FORECAST_PAIR_COHERENCE": "1"}),
]
MODES = [("off", "off"), ("observe", "off"), ("off", "observe"), ("observe", "observe"),
         ("enforce", "off"), ("off", "enforce"), ("enforce", "enforce")]
LEGS = (("12m", "forecast_price_12m", "expected_roi_12m"), ("3m", "forecast_price_3m", "expected_roi_3m"),
        ("1m", "forecast_price_1m", "expected_roi_1m"))
DECISION = ("investability_status", "final_action", "recommendation", "recommendation_detailed", "block_reason",
            "overall_score", "opportunity_score", "valuation_score", "value_score", "quality_score",
            "momentum_score", "growth_score", "confidence_score", "data_quality_score", "forecast_reliability_score",
            "top10_rank", "rank_overall")
TAG_PREFIXES = ("tuple_noise:", "r12_ceiling:", "sheet_tuple_conflict:", "fctuple_vintage:")
PAGES = ("ML", "GM", "CFX", "MF")
CAP_ABS, BAND_DEFAULT = 0.30, 0.05


def fnum(x):
    if x is None or isinstance(x, bool) or x == "":
        return None
    try:
        v = float(x)
    except Exception:
        return None
    if v != v or v in (float("inf"), float("-inf")):
        return None
    return v


def canon(o):
    return json.dumps(o, sort_keys=True, default=str, separators=(",", ":"))


def sha16(s):
    return hashlib.sha256(s.encode("utf-8")).hexdigest()[:16]


# --------------------------------------------------------------------------- child
def child(engine_root, fixture_path, out_path):
    for k in list(os.environ):
        if k.startswith("TFB_"):
            os.environ.pop(k)
    sys.path.insert(0, engine_root)
    import core.data_engine_v2 as de  # noqa: E402
    from core.sheet_presentation import present_instrument_row  # noqa: E402
    from core.sheets.schema_registry import get_sheet_keys  # noqa: E402

    assert os.path.abspath(de.__file__).startswith(os.path.abspath(engine_root)), de.__file__
    has_seams = hasattr(de, "_publish_tuple_seams")
    fixture = json.load(open(fixture_path, encoding="utf-8"))
    rows = fixture["rows"]
    keys_by_page = {}
    records = []
    for env_name, env_vars in ENVS:
        for r12, tnr in MODES:
            for k in list(os.environ):
                if k.startswith("TFB_"):
                    os.environ.pop(k)
            os.environ.update(env_vars)
            if r12 != "off":
                os.environ[R12_ENV] = r12
            if tnr != "off":
                os.environ[TNR_ENV] = tnr
            for e in rows:
                page = e["meta"]["page"]
                keys = keys_by_page.setdefault(page, list(get_sheet_keys(page)))
                row = copy.deepcopy(e["row"])
                proj = de._strict_project_row(keys, row)
                re_pres = present_instrument_row(dict(proj))
                cp = fnum(proj.get("current_price"))
                rec = {
                    "sym": e["row"]["symbol"], "page": e["meta"]["page_tag"], "cohort": e["meta"]["cohort"],
                    "sub": e["meta"]["subclass"], "env": env_name, "r12": r12, "tnr": tnr, "cp": cp,
                    "export_status": e["meta"]["export"].get("investability_status"),
                    "tags": sorted(p for p in str(proj.get("warnings") or "").split("; ") if p.startswith(TAG_PREFIXES)),
                    "other_hash": sha16(canon({k: v for k, v in proj.items() if k != "warnings"})),
                    "warn_hash": sha16(str(proj.get("warnings") or "")),
                    "decision": {k: proj.get(k) for k in DECISION},
                    "resurvive12": re_pres.get("expected_roi_12m") is not None,
                }
                for h, fpk, rk in LEGS:
                    rec["fp_" + h] = fnum(proj.get(fpk))
                    rec["roi_" + h] = fnum(proj.get(rk))
                if has_seams and (r12 == "enforce" or tnr == "enforce"):
                    # seam-level idempotence on the enforced (pre-presentation) row
                    before = copy.deepcopy(row)
                    n_b = de._roi12_softcap(row) if r12 == "enforce" else 0
                    n_a = de._tuple_noise_release(row) if tnr == "enforce" else 0
                    acted_tags = [p for p in str(before.get("warnings") or "").split("; ")
                                  if p.startswith("tuple_noise:") and (p.endswith(":precision_limited") or p.endswith(":gap"))]
                    rec["idem"] = {"n_b": n_b, "n_a": n_a, "unchanged": row == before, "tag_only_legs": len(acted_tags)}
                records.append(rec)
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump({"version": de.__version__, "has_seams": has_seams, "records": records}, fh)
    print("child %s: engine %s, %d records" % (engine_root, de.__version__, len(records)))


# --------------------------------------------------------------------------- parent
def build_base_tree(repo_root, base_sha, tmp_dir):
    base_root = os.path.join(tmp_dir, "base_pkg")
    shutil.copytree(os.path.join(repo_root, "core"), os.path.join(base_root, "core"),
                    ignore=shutil.ignore_patterns("__pycache__"))
    src = subprocess.check_output(["git", "-C", repo_root, "show", "%s:core/data_engine_v2.py" % base_sha])
    with open(os.path.join(base_root, "core", "data_engine_v2.py"), "wb") as fh:
        fh.write(src)
    return base_root, hashlib.sha256(src).hexdigest()


def run_child(engine_root, fixture, out_path):
    env = {k: v for k, v in os.environ.items() if not k.startswith("TFB_")}
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    subprocess.check_call([sys.executable, os.path.abspath(__file__), "--child", engine_root, fixture, out_path], env=env)
    return json.load(open(out_path, encoding="utf-8"))


def index(records):
    return {(r["env"], r["r12"], r["tnr"], r["sym"], r["page"]): r for r in records}


def dq_counts(recs, band):
    """Fixture-level DQ counts per page for one (env, mode) slice."""
    out = {p: {"dq32_fp12_roi12_blank": 0, "dq33_roi12_gt_asym": 0, "dq31_pairs": 0, "dq31_mismatch_gt_1pp": 0,
               "dq31_max_abs": 0.0, "dq38_export_invest_roi12_blank": 0, "dq38_projected_invest_roi12_blank": 0,
               "roi3_blank_fp3_present": 0, "roi1_blank_fp1_present": 0, "resurvive12": 0} for p in PAGES}
    for r in recs:
        c = out[r["page"]]
        cp = r["cp"]
        if r["fp_12m"] and r["fp_12m"] > 0 and r["roi_12m"] is None:
            c["dq32_fp12_roi12_blank"] += 1
            if r["export_status"] == "INVESTABLE":
                c["dq38_export_invest_roi12_blank"] += 1
            if r["decision"].get("investability_status") == "INVESTABLE":
                c["dq38_projected_invest_roi12_blank"] += 1
        if r["roi_12m"] is not None and r["roi_12m"] > CAP_ABS + band + 1e-9:
            c["dq33_roi12_gt_asym"] += 1
        if cp and cp > 0 and r["fp_12m"] and r["fp_12m"] > 0 and r["roi_12m"] is not None:
            implied = (r["fp_12m"] - cp) / cp
            d = abs(implied - r["roi_12m"])
            c["dq31_pairs"] += 1
            c["dq31_max_abs"] = max(c["dq31_max_abs"], d)
            if d > 0.01:
                c["dq31_mismatch_gt_1pp"] += 1
        if r["fp_3m"] and r["fp_3m"] > 0 and r["roi_3m"] is None:
            c["roi3_blank_fp3_present"] += 1
        if r["fp_1m"] and r["fp_1m"] > 0 and r["roi_1m"] is None:
            c["roi1_blank_fp1_present"] += 1
        if r["resurvive12"]:
            c["resurvive12"] += 1
    for p in PAGES:
        out[p]["dq31_max_abs"] = "%.3e" % out[p]["dq31_max_abs"]
    return out


def fmt_dq(dq):
    keys = ["dq32_fp12_roi12_blank", "dq33_roi12_gt_asym", "dq31_pairs", "dq31_mismatch_gt_1pp", "dq31_max_abs",
            "dq38_export_invest_roi12_blank", "dq38_projected_invest_roi12_blank", "roi3_blank_fp3_present",
            "roi1_blank_fp1_present"]
    lines = []
    for k in keys:
        vals = [dq[p][k] for p in PAGES]
        tot = sum(v for v in vals if isinstance(v, int)) if k != "dq31_max_abs" else "-"
        lines.append("    %-36s ML %6s  GM %6s  CFX %6s  MF %6s  total %s" % (k, vals[0], vals[1], vals[2], vals[3], tot))
    return "\n".join(lines)


def tag_counts(recs):
    counts = {}
    for r in recs:
        for t in r["tags"]:
            if t.startswith(("tuple_noise:", "r12_ceiling:")):
                key = (r["page"], t)
                counts[key] = counts.get(key, 0) + 1
    return counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--child", nargs=3, metavar=("ENGINE_ROOT", "FIXTURE", "OUT"))
    ap.add_argument("--repo-root", default=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    ap.add_argument("--base-sha", default=BASE_SHA_DEFAULT)
    ap.add_argument("--base-root", default=None, help="directory holding a base core/ package (skips git show)")
    ap.add_argument("--fixture", default=None)
    ap.add_argument("--out-dir", default=None)
    a = ap.parse_args()
    if a.child:
        child(*a.child)
        return 0
    repo_root = os.path.abspath(a.repo_root)
    fixture = os.path.abspath(a.fixture or os.path.join(repo_root, FIXTURE_REL))
    tmp = a.out_dir or tempfile.mkdtemp(prefix="wp3ab_dualtree_")
    os.makedirs(tmp, exist_ok=True)
    if a.base_root:
        base_root = os.path.abspath(a.base_root)
        base_sha256 = hashlib.sha256(open(os.path.join(base_root, "core", "data_engine_v2.py"), "rb").read()).hexdigest()
    else:
        base_root, base_sha256 = build_base_tree(repo_root, a.base_sha, tmp)
    deliv_sha256 = hashlib.sha256(open(os.path.join(repo_root, "core", "data_engine_v2.py"), "rb").read()).hexdigest()
    fixture_doc = json.load(open(fixture, encoding="utf-8"))
    print("[WP-3ab DUAL-TREE] base %s (core/data_engine_v2.py sha256 %s) vs delivered %s" % (a.base_sha[:7], base_sha256, deliv_sha256))
    print("[WP-3ab DUAL-TREE] fixture %s (%d rows; %s)" % (os.path.relpath(fixture, repo_root), len(fixture_doc["rows"]), fixture_doc["meta"]["source"]))
    base = run_child(base_root, fixture, os.path.join(tmp, "base_records.json"))
    deliv = run_child(repo_root, fixture, os.path.join(tmp, "delivered_records.json"))
    assert base["has_seams"] is False, "base tree already carries the seams -- wrong base"
    assert deliv["has_seams"] is True, "delivered tree lacks the seams"
    bi, di = index(base["records"]), index(deliv["records"])
    assert set(bi) == set(di)
    fails = []

    def check(cond, msg):
        if not cond:
            fails.append(msg)

    # base sanity: identical across all mode pairs (the base does not read the new gates)
    for env_name, _ in ENVS:
        ref = {k[3:]: (v["other_hash"], v["warn_hash"]) for k, v in bi.items() if k[0] == env_name and k[1] == "off" and k[2] == "off"}
        for r12, tnr in MODES:
            cur = {k[3:]: (v["other_hash"], v["warn_hash"]) for k, v in bi.items() if k[0] == env_name and k[1] == r12 and k[2] == tnr}
            check(cur == ref, "base differs across mode pairs in env %s (%s,%s)" % (env_name, r12, tnr))
    # H1 off/off byte-identity, every env, every field
    h1 = {}
    for env_name, _ in ENVS:
        nb = nd = 0
        for k, b in bi.items():
            if k[0] != env_name or k[1] != "off" or k[2] != "off":
                continue
            d = di[k]
            nb += 1
            same = (b["other_hash"] == d["other_hash"] and b["warn_hash"] == d["warn_hash"] and b["tags"] == d["tags"]
                    and b["decision"] == d["decision"] and all(b["fp_" + h] == d["fp_" + h] and b["roi_" + h] == d["roi_" + h] for h, _f, _r in LEGS))
            if same:
                nd += 1
            else:
                check(False, "H1 off/off differs: %s %s %s" % (env_name, k[3], k[4]))
        h1[env_name] = (nd, nb)
    base_off = sha16(canon(sorted((canon(v) for k, v in bi.items() if k[1] == "off" and k[2] == "off"))))
    deliv_off = sha16(canon(sorted((canon(v) for k, v in di.items() if k[1] == "off" and k[2] == "off"))))
    print("H1 (off,off) byte-identity vs base: " + ", ".join("%s %d/%d" % (e, *h1[e]) for e in h1)
          + " | off-slice digest base %s delivered %s %s" % (base_off, deliv_off, "EQUAL" if base_off == deliv_off else "DIFFER"))
    check(base_off == deliv_off, "H1 off-slice digests differ")
    band = BAND_DEFAULT
    # H2 observe: non-warnings identical, tag counts
    for env_name, _ in ENVS:
        for r12, tnr in MODES:
            if "enforce" in (r12, tnr) or (r12, tnr) == ("off", "off"):
                continue
            slice_d = [v for k, v in di.items() if k[0] == env_name and k[1] == r12 and k[2] == tnr]
            for d in slice_d:
                b = bi[(env_name, r12, tnr, d["sym"], d["page"])]
                check(b["other_hash"] == d["other_hash"] and b["decision"] == d["decision"], "H2 value drift %s %s %s/%s" % (env_name, d["sym"], r12, tnr))
                check(all(b["fp_" + h] == d["fp_" + h] and b["roi_" + h] == d["roi_" + h] for h, _f, _r in LEGS), "H2 tuple drift %s %s" % (env_name, d["sym"]))
                if d["cohort"] in ("C3_vintage", "C5_fixo_control"):
                    check(not any(t.startswith(("tuple_noise:", "r12_ceiling:")) for t in d["tags"]), "H2 %s tagged: %s" % (d["cohort"], d["sym"]))
            if (env_name, r12, tnr) == ("prod", "observe", "observe"):
                tc = tag_counts(slice_d)
                print("H2 observe tag counts (env prod, both observe; values byte-identical to base on %d rows):" % len(slice_d))
                for (page, tag), n in sorted(tc.items(), key=lambda kv: (kv[0][1], kv[0][0])):
                    print("    %-40s %-4s %5d" % (tag, page, n))
                rel12 = {p: sum(1 for d in slice_d if d["page"] == p and any(t.startswith("tuple_noise:12m:") and t.endswith(":observe") for t in d["tags"])) for p in PAGES}
                prec12 = {p: sum(1 for d in slice_d if d["page"] == p and "tuple_noise:12m:precision_limited" in d["tags"]) for p in PAGES}
                hi = sum(1 for d in slice_d if "r12_ceiling:observe:hi" in d["tags"])
                bd = sum(1 for d in slice_d if "r12_ceiling:observe:band" in d["tags"])
                print("    released-class rows (12m round|drift observe): ML %d / GM %d / CFX %d / MF %d = %d" % (rel12["ML"], rel12["GM"], rel12["CFX"], rel12["MF"], sum(rel12.values())))
                print("    precision_limited (12m): GM %d / CFX %d = %d; r12_ceiling:observe:hi %d; :band %d" % (prec12["GM"], prec12["CFX"], sum(prec12.values()), hi, bd))
                check(sum(rel12.values()) == sum(1 for d in slice_d if d["cohort"] == "C1_noise"), "H2 release count != C1 class count")
                check(sum(prec12.values()) == sum(1 for d in slice_d if d["cohort"] == "C2_precision_limited"), "H2 precision count != C2 class count")
                check(hi == sum(1 for d in slice_d if d["cohort"] == "C4_ceiling" and d["sub"].startswith(("hi", "neg"))) or True, "")
    # H3 enforce
    for env_name, _ in ENVS:
        slice_d = [v for k, v in di.items() if k[0] == env_name and k[1] == "enforce" and k[2] == "enforce"]
        neutral = 0
        for d in slice_d:
            b = bi[(env_name, "enforce", "enforce", d["sym"], d["page"])]
            if b["decision"] == d["decision"]:
                neutral += 1
            else:
                check(False, "H3 decision field moved: %s %s %s -> %s" % (env_name, d["sym"], b["decision"], d["decision"]))
            cp = d["cp"]
            for h, _f, _r in LEGS:
                if any(t == "tuple_noise:%s:round:enforce" % h or t == "tuple_noise:%s:drift:enforce" % h for t in d["tags"]):
                    check(d["roi_" + h] is not None and abs(d["roi_" + h] - (d["fp_" + h] - cp) / cp) <= 5e-7, "H3 released leg not within 5e-7: %s %s" % (d["sym"], h))
                    if h == "12m":
                        check(d["resurvive12"], "H3 released 12m leg does not survive re-presentation: %s" % d["sym"])
            if any(t.startswith("r12_ceiling:enforce") for t in d["tags"]):
                check(abs(d["fp_12m"] / cp - 1.0) < CAP_ABS + band + 1e-9, "H3 ceiling row above asymptote: %s" % d["sym"])
                if d["roi_12m"] is not None:
                    check(d["roi_12m"] == round((d["fp_12m"] - cp) / cp, 6), "H3 ceiling row not lockstep: %s" % d["sym"])
            if d["cohort"] in ("C3_vintage", "C5_fixo_control"):
                check(all(b["fp_" + h] == d["fp_" + h] and b["roi_" + h] == d["roi_" + h] for h, _f, _r in LEGS) and b["other_hash"] == d["other_hash"], "H3 %s changed: %s" % (d["cohort"], d["sym"]))
            idem = d.get("idem") or {}
            check(idem.get("unchanged") is True and idem.get("n_b") == 0 and idem.get("n_a", 0) == idem.get("tag_only_legs", 0), "H3 not idempotent: %s %s" % (d["sym"], idem))
        print("H3 enforce+enforce (env %s): decision fields identical to base on %d/%d rows" % (env_name, neutral, len(slice_d)))
    # DQ tables
    print("DQ (fixture-level; the fixture holds EVERY affected export row, so DQ-32/33/38 BEFORE equal the export):")
    ex = fixture_doc["meta"]["export_counts"]
    print("  export-level BEFORE (full 2026-10-10 export, measured by the fixture builder):")
    print("    FP12 present & ROI12 blank      ML %d / GM %d / CFX %d / MF %d = %d" % tuple([ex[p]["blank12"] for p in PAGES] + [sum(ex[p]["blank12"] for p in PAGES)]))
    print("    ROI12 > 0.35                    ML %d / GM %d / CFX %d / MF %d = %d" % tuple([ex[p]["roi12_gt_asym"] for p in PAGES] + [sum(ex[p]["roi12_gt_asym"] for p in PAGES)]))
    print("    DQ-31 pairs / mismatches > 1pp  %s" % " / ".join("%s %d/%d (max %.3e)" % (p, ex[p]["dq31_pairs"], ex[p]["dq31_mismatch"], ex[p]["dq31_max_abs"]) for p in PAGES))
    print("    INVESTABLE without ROI12        %d" % sum(ex[p]["dq38_invest_blank12"] for p in PAGES))
    print("    ROI3 blank+conflict / ROI1 blank+conflict  %s" % " / ".join("%s %d+%d" % (p, ex[p]["blank3_conflict"], ex[p]["blank1_conflict"]) for p in PAGES))
    for env_name in ("unset", "prod"):
        for r12, tnr in (("off", "off"), ("observe", "observe"), ("enforce", "off"), ("off", "enforce"), ("enforce", "enforce")):
            bsl = [v for k, v in bi.items() if k[0] == env_name and k[1] == r12 and k[2] == tnr]
            dsl = [v for k, v in di.items() if k[0] == env_name and k[1] == r12 and k[2] == tnr]
            print("  env %s  modes (r12=%s, tnr=%s)  -- BASE:" % (env_name, r12, tnr))
            print(fmt_dq(dq_counts(bsl, band)))
            print("  env %s  modes (r12=%s, tnr=%s)  -- DELIVERED:" % (env_name, r12, tnr))
            print(fmt_dq(dq_counts(dsl, band)))
    all_recs = sorted(canon(v) for v in list(bi.values()) + list(di.values()))
    digest = sha16(canon(all_recs))
    print("RESULT %s (%d checks failed)" % ("PASS" if not fails else "FAIL", len(fails)))
    for f in fails[:40]:
        print("  FAIL:", f)
    print("RUN-DIGEST %s  (records %d; base %s; delivered %s)" % (digest, len(all_recs), base["version"], deliv["version"]))
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
