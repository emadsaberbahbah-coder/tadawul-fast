"""
core/validation.py — TFB Gen-2 Statistical Validation Harness
==============================================================
VERSION 1.0.0  (2026-07-19)  — NEW MODULE (Wave B foundation, deliverable #19)

WHY (Master Plan v2.1 §8, and the standing constraint that "every event->sector
and theme->rotation linkage requires a registered hypothesis + backtest before
influencing recommendations"): that sentence is unenforceable without a gate
that can actually reject. This module IS the gate. Nothing — not a candle
pattern, not a news->sector link, not a weight change from the calibrator —
influences a recommendation until it survives every check here.

BUILT BEFORE THE DATA ARRIVES, DELIBERATELY. These are pure statistics: they
need synthetic fixtures to verify, not matured cohorts. Building now means the
first cohort (2026-07-28) meets a finished gate rather than an empty promise,
and — more importantly — the acceptance thresholds are fixed BEFORE anyone has
seen the results they will judge. A gate written after seeing the data is not
a gate.

WHAT IT ENFORCES:
  1. PURGED WALK-FORWARD + EMBARGO. Overlapping forward-return labels leak:
     a 1M label on day t overlaps every label from t-29..t+29. Naive k-fold
     therefore trains on the future. Splits purge overlapping observations
     and apply an embargo after each test block (Lopez de Prado's method).
  2. EFFECTIVE SAMPLE SIZE. 200 overlapping 30-day returns are NOT 200
     independent observations; the induced MA(h-1) structure inflates every
     t-stat. ESS is computed from the actual overlap autocorrelation, and the
     minimum-sample gate runs on ESS, never on the raw count.
  3. DEFLATED SHARPE RATIO. Testing many variants guarantees a good-looking
     winner by chance. DSR (Bailey & Lopez de Prado) discounts the observed
     Sharpe by the number of trials, plus skew and kurtosis.
  4. PROBABILITY OF BACKTEST OVERFITTING via CSCV: how often the in-sample
     best underperforms the median out-of-sample. PBO > 0.5 means the
     selection procedure is worse than a coin flip.

PURITY: stdlib only (math/itertools). No I/O, no env, no network — so it is
testable in isolation and identical wherever it runs. Consumers pass numbers
in and receive a verdict with reasons.

GOVERNANCE: verdicts are advisory to a HUMAN. A PASS authorizes a registered
hypothesis to be considered; it never auto-arms a weight. Thresholds live in
DEFAULT_GATES and every one is printed with the verdict.
"""

from __future__ import annotations

import itertools
import math
from typing import Any, Dict, List, Optional, Sequence, Tuple

__version__ = "1.0.0"
VALIDATION_VERSION = __version__

EULER_GAMMA = 0.5772156649015329

# Acceptance thresholds — fixed in advance, printed with every verdict.
DEFAULT_GATES: Dict[str, float] = {
    "min_effective_sample": 30.0,   # ESS, not raw n
    "min_deflated_sharpe": 0.95,    # DSR is a probability; 0.95 = 95% confident
    "max_pbo": 0.50,                # above this, selection is worse than chance
    "min_folds": 3.0,
}


# --------------------------------------------------------------------------- #
# normal distribution helpers (stdlib only)                                    #
# --------------------------------------------------------------------------- #
def norm_cdf(x: float) -> float:
    return 0.5 * (1.0 + math.erf(float(x) / math.sqrt(2.0)))


def norm_ppf(p: float) -> float:
    """Inverse normal CDF (Acklam's rational approximation, |error| < 1.15e-9)."""
    p = min(max(float(p), 1e-12), 1.0 - 1e-12)
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    plow, phigh = 0.02425, 1 - 0.02425
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
               ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
                ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    q, r = p - 0.5, (p - 0.5) * (p - 0.5)
    return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / \
           (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)


# --------------------------------------------------------------------------- #
# moments                                                                      #
# --------------------------------------------------------------------------- #
def _mean(xs: Sequence[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _std(xs: Sequence[float], ddof: int = 1) -> float:
    n = len(xs)
    if n <= ddof:
        return 0.0
    m = _mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - ddof))


def moments(xs: Sequence[float]) -> Dict[str, float]:
    """mean, std, skewness, excess kurtosis (population form)."""
    n = len(xs)
    if n < 2:
        return {"n": n, "mean": _mean(xs), "std": 0.0, "skew": 0.0, "kurt": 0.0}
    m, s = _mean(xs), _std(xs)
    if s == 0.0:
        return {"n": n, "mean": m, "std": 0.0, "skew": 0.0, "kurt": 0.0}
    skew = sum(((x - m) / s) ** 3 for x in xs) / n
    kurt = sum(((x - m) / s) ** 4 for x in xs) / n
    return {"n": n, "mean": m, "std": s, "skew": skew, "kurt": kurt}


def sharpe_ratio(returns: Sequence[float], rf: float = 0.0) -> Optional[float]:
    """Per-observation Sharpe (NOT annualized — annualization is the caller's
    decision and must never be hidden inside a validator)."""
    if len(returns) < 2:
        return None
    s = _std(returns)
    if s == 0.0:
        return None
    return (_mean(returns) - rf) / s


# --------------------------------------------------------------------------- #
# 1. purged walk-forward with embargo                                          #
# --------------------------------------------------------------------------- #
def purged_walk_forward_splits(n: int, n_splits: int = 5, horizon: int = 30,
                               embargo_pct: float = 0.01
                               ) -> List[Tuple[List[int], List[int]]]:
    """Forward-chaining splits. Training indices that OVERLAP the test block's
    label window are purged; an embargo of `embargo_pct * n` observations
    after the test block is also removed. -> [(train_idx, test_idx), ...]

    Train is strictly past-only (walk-forward): a model is never fitted on
    observations that postdate its test block."""
    n = int(n)
    if n <= 0 or n_splits < 2:
        return []
    fold = max(1, n // (n_splits + 1))
    embargo = int(math.ceil(max(0.0, embargo_pct) * n))
    h = max(0, int(horizon) - 1)
    out: List[Tuple[List[int], List[int]]] = []
    for k in range(1, n_splits + 1):
        test_start = k * fold
        test_end = min(n, test_start + fold)
        if test_start >= n or test_end <= test_start:
            break
        test_idx = list(range(test_start, test_end))
        # purge: drop training points whose label window reaches the test block
        purge_from = max(0, test_start - h)
        train_idx = [i for i in range(0, purge_from)]
        # embargo is enforced by walk-forward (nothing after test is used),
        # but is recorded so the caller can widen it for anytime-CV variants.
        if train_idx:
            out.append((train_idx, test_idx))
    return out


def split_diagnostics(splits: Sequence[Tuple[List[int], List[int]]],
                      horizon: int = 30) -> Dict[str, Any]:
    """Verify no train index can leak into a test label window."""
    leaks = 0
    for train, test in splits or []:
        if not train or not test:
            continue
        if max(train) >= min(test) - (int(horizon) - 1):
            leaks += 1
    return {"folds": len(splits or []), "leaky_folds": leaks,
            "clean": leaks == 0}


# --------------------------------------------------------------------------- #
# 2. effective sample size under overlapping windows                           #
# --------------------------------------------------------------------------- #
def effective_sample_size(n: int, horizon: int = 1) -> float:
    """Overlapping h-period forward returns induce an MA(h-1) structure with
    rho_k = (h-k)/h. ESS = n / (1 + 2*sum_k rho_k*(1-k/n)).
    h=1 (non-overlapping) returns n exactly."""
    n = int(n)
    h = max(1, int(horizon))
    if n <= 1:
        return float(max(0, n))
    if h == 1:
        return float(n)
    infl = 1.0 + 2.0 * sum(((h - k) / h) * (1.0 - k / n)
                           for k in range(1, min(h, n)))
    return max(1.0, n / infl) if infl > 0 else float(n)


def cluster_adjusted_ess(n: int, horizon: int = 1,
                         avg_cross_correlation: float = 0.0,
                         n_names: int = 1) -> float:
    """Cross-sectional clustering shrinks ESS further: k correlated names on
    the same date are not k observations. Uses the standard design effect
    1 + (k-1)*rho."""
    base = effective_sample_size(n, horizon)
    k = max(1, int(n_names))
    rho = min(max(float(avg_cross_correlation), 0.0), 1.0)
    design_effect = 1.0 + (k - 1) * rho
    return max(1.0, base / design_effect) if design_effect > 0 else base


# --------------------------------------------------------------------------- #
# 3. deflated Sharpe ratio                                                     #
# --------------------------------------------------------------------------- #
def expected_max_sharpe(n_trials: int, sharpe_variance: float = 1.0) -> float:
    """Expected maximum Sharpe from N independent trials of zero true skill —
    the benchmark an observed Sharpe must beat."""
    n = max(2, int(n_trials))
    sd = math.sqrt(max(1e-12, float(sharpe_variance)))
    return sd * ((1.0 - EULER_GAMMA) * norm_ppf(1.0 - 1.0 / n)
                 + EULER_GAMMA * norm_ppf(1.0 - 1.0 / (n * math.e)))


def deflated_sharpe_ratio(observed_sharpe: float, n_obs: int,
                          n_trials: int = 1, skew: float = 0.0,
                          kurt: float = 3.0,
                          benchmark_sharpe: Optional[float] = None
                          ) -> Optional[float]:
    """Probability the observed Sharpe exceeds what multiple testing alone
    would produce. Returns None when inputs cannot support the statistic."""
    n = int(n_obs)
    if n < 3:
        return None
    sr = float(observed_sharpe)
    # V[SR] is the variance of the Sharpe ESTIMATOR, ~ (1 + sr^2/2)/n under
    # normality — NOT 1.0. Assuming unit variance made the multiple-testing
    # benchmark scale-free and absurdly high (sr0=1.19 for 5 trials), which
    # would have rejected every genuine edge. Caught by the harness.
    var_sr = (1.0 + 0.5 * sr * sr) / float(n)
    sr0 = (expected_max_sharpe(n_trials, var_sr) if benchmark_sharpe is None
           else float(benchmark_sharpe))
    denom_sq = 1.0 - float(skew) * sr + ((float(kurt) - 1.0) / 4.0) * sr * sr
    if denom_sq <= 0:
        return None
    z = (sr - sr0) * math.sqrt(n - 1) / math.sqrt(denom_sq)
    return norm_cdf(z)


# --------------------------------------------------------------------------- #
# 4. probability of backtest overfitting (CSCV)                                #
# --------------------------------------------------------------------------- #
def probability_of_backtest_overfitting(
        performance: Sequence[Sequence[float]], n_partitions: int = 8
) -> Optional[Dict[str, Any]]:
    """CSCV. `performance` is T x N: rows = time slices, cols = strategy
    variants. Splits rows into S partitions, forms every balanced train/test
    combination, and measures how often the IN-SAMPLE best falls below the
    OUT-OF-SAMPLE median. PBO > 0.5 => selection is worse than chance."""
    T = len(performance or [])
    if T < 4:
        return None
    N = len(performance[0])
    if N < 2 or any(len(r) != N for r in performance):
        return None
    S = max(2, min(int(n_partitions), T))
    if S % 2:
        S -= 1
    if S < 2:
        return None
    size = T // S
    if size < 1:
        return None
    parts = [list(range(i * size, (i + 1) * size)) for i in range(S)]
    half = S // 2
    logits: List[float] = []
    below = 0
    total = 0
    for combo in itertools.combinations(range(S), half):
        tr = [i for p in combo for i in parts[p]]
        te = [i for p in range(S) if p not in combo for i in parts[p]]
        if not tr or not te:
            continue
        is_mean = [sum(performance[i][j] for i in tr) / len(tr) for j in range(N)]
        oos_mean = [sum(performance[i][j] for i in te) / len(te) for j in range(N)]
        best = max(range(N), key=lambda j: is_mean[j])
        order = sorted(range(N), key=lambda j: oos_mean[j])
        rank = order.index(best) + 1                 # 1 = worst
        w = rank / (N + 1.0)
        total += 1
        if w <= 0.5:
            below += 1
        w = min(max(w, 1e-9), 1 - 1e-9)
        logits.append(math.log(w / (1 - w)))
    if not total:
        return None
    return {"pbo": below / total, "n_combinations": total,
            "median_logit": (sorted(logits)[len(logits) // 2] if logits else None),
            "n_variants": N, "n_partitions": S}


# --------------------------------------------------------------------------- #
# composite gate                                                               #
# --------------------------------------------------------------------------- #
def validate_hypothesis(returns: Sequence[float],
                        horizon: int = 30,
                        n_trials: int = 1,
                        n_names: int = 1,
                        avg_cross_correlation: float = 0.0,
                        performance_matrix: Optional[Sequence[Sequence[float]]] = None,
                        gates: Optional[Dict[str, float]] = None,
                        n_splits: int = 5) -> Dict[str, Any]:
    """The graduation gate. PASS only when every check clears; any missing
    input yields INSUFFICIENT (never a silent pass)."""
    g = dict(DEFAULT_GATES)
    g.update(gates or {})
    rs = [float(r) for r in (returns or [])]
    reasons: List[str] = []
    checks: List[Dict[str, Any]] = []

    mom = moments(rs)
    sr = sharpe_ratio(rs)
    ess = cluster_adjusted_ess(len(rs), horizon, avg_cross_correlation, n_names)
    checks.append({"name": "effective sample size",
                   "value": round(ess, 2), "gate": g["min_effective_sample"],
                   "status": "PASS" if ess >= g["min_effective_sample"] else "FAIL"})
    if ess < g["min_effective_sample"]:
        reasons.append(f"ESS {ess:.1f} < {g['min_effective_sample']:.0f} "
                       f"(raw n={len(rs)}, horizon={horizon})")

    splits = purged_walk_forward_splits(len(rs), n_splits, horizon)
    diag = split_diagnostics(splits, horizon)
    checks.append({"name": "purged walk-forward folds",
                   "value": diag["folds"], "gate": g["min_folds"],
                   "status": "PASS" if (diag["folds"] >= g["min_folds"]
                                        and diag["clean"]) else "FAIL"})
    if diag["folds"] < g["min_folds"]:
        reasons.append(f"only {diag['folds']} clean folds available")
    if not diag["clean"]:
        reasons.append(f"{diag['leaky_folds']} fold(s) leak across the label window")

    dsr = (deflated_sharpe_ratio(sr, int(ess), n_trials, mom["skew"],
                                 mom["kurt"]) if sr is not None else None)
    checks.append({"name": "deflated Sharpe (prob.)",
                   "value": (round(dsr, 4) if dsr is not None else None),
                   "gate": g["min_deflated_sharpe"],
                   "status": ("PASS" if (dsr is not None
                                         and dsr >= g["min_deflated_sharpe"])
                              else "INSUFFICIENT" if dsr is None else "FAIL")})
    if dsr is None:
        reasons.append("Sharpe/DSR not computable from the sample")
    elif dsr < g["min_deflated_sharpe"]:
        reasons.append(f"DSR {dsr:.3f} < {g['min_deflated_sharpe']} "
                       f"after deflating for {n_trials} trial(s)")

    pbo = (probability_of_backtest_overfitting(performance_matrix)
           if performance_matrix else None)
    checks.append({"name": "probability of backtest overfitting",
                   "value": (round(pbo["pbo"], 4) if pbo else None),
                   "gate": g["max_pbo"],
                   "status": ("PASS" if (pbo and pbo["pbo"] <= g["max_pbo"])
                              else "INSUFFICIENT" if not pbo else "FAIL")})
    if pbo and pbo["pbo"] > g["max_pbo"]:
        reasons.append(f"PBO {pbo['pbo']:.2f} > {g['max_pbo']} — the selection "
                       f"procedure is worse than a coin flip")

    statuses = [c["status"] for c in checks]
    if "FAIL" in statuses:
        verdict = "REJECT"
    elif "INSUFFICIENT" in statuses:
        verdict = "INSUFFICIENT"
    else:
        verdict = "PASS"
    return {"version": __version__, "verdict": verdict, "checks": checks,
            "reasons": reasons, "gates": g,
            "sample": {"n": len(rs), "ess": round(ess, 2),
                       "sharpe": (round(sr, 4) if sr is not None else None),
                       "skew": round(mom["skew"], 4),
                       "kurt": round(mom["kurt"], 4)},
            "governance": ("advisory to a human — a PASS authorizes a "
                           "registered hypothesis to be CONSIDERED; it never "
                           "auto-arms a weight")}


# --------------------------------------------------------------------------- #
# SELFTEST                                                                     #
# --------------------------------------------------------------------------- #
def _lcg(seed: int = 12345):
    x = seed
    while True:
        x = (1103515245 * x + 12345) % (1 << 31)
        yield x / (1 << 31)


def _normals(n: int, mu: float = 0.0, sd: float = 1.0, seed: int = 7):
    g = _lcg(seed)
    out = []
    for _ in range(n):
        u1 = max(1e-12, next(g))
        u2 = next(g)
        out.append(mu + sd * math.sqrt(-2 * math.log(u1)) * math.cos(2 * math.pi * u2))
    return out


def _selftest() -> int:
    checks: List[Tuple[str, bool]] = []

    checks.append(("norm_cdf/ppf round-trip",
                   abs(norm_cdf(norm_ppf(0.975)) - 0.975) < 1e-6
                   and abs(norm_ppf(0.5)) < 1e-9))
    checks.append(("norm_cdf known values",
                   abs(norm_cdf(0) - 0.5) < 1e-12
                   and abs(norm_cdf(1.96) - 0.975) < 1e-3))

    checks.append(("ESS: non-overlapping equals n",
                   effective_sample_size(200, 1) == 200.0))
    e30 = effective_sample_size(200, 30)
    # ~n/h is the statistically correct answer (200 daily obs of 30d
    # overlapping returns hold ~7 independent windows). My first bound
    # (10,30) was the wrong expectation, not a code defect.
    checks.append(("ESS: 200 overlapping 30d returns collapse to ~n/h",
                   5.0 < e30 < 12.0 and abs(e30 - 200 / 30) < 1.0))
    checks.append(("ESS: shrinks monotonically with horizon",
                   effective_sample_size(200, 7) > effective_sample_size(200, 14)
                   > effective_sample_size(200, 30)))
    checks.append(("ESS: cross-correlated names shrink it further",
                   cluster_adjusted_ess(200, 30, 0.7, 8) < e30))
    checks.append(("ESS: zero correlation leaves it unchanged",
                   abs(cluster_adjusted_ess(200, 30, 0.0, 8) - e30) < 1e-9))

    sp = purged_walk_forward_splits(300, n_splits=5, horizon=30)
    checks.append(("walk-forward produces folds", len(sp) >= 3))
    checks.append(("train is strictly before test in every fold",
                   all(max(tr) < min(te) for tr, te in sp)))
    checks.append(("purge gap >= horizon-1 in every fold",
                   all(min(te) - max(tr) >= 29 for tr, te in sp)))
    checks.append(("split diagnostics confirm no leakage",
                   split_diagnostics(sp, 30)["clean"] is True))
    leaky = [([0, 1, 2, 3, 4], [5, 6, 7])]
    checks.append(("diagnostics DETECT a leaky split",
                   split_diagnostics(leaky, 30)["clean"] is False))

    checks.append(("expected max Sharpe rises with trials",
                   expected_max_sharpe(100) > expected_max_sharpe(10) > 0))
    d1 = deflated_sharpe_ratio(0.30, 100, n_trials=1)
    d100 = deflated_sharpe_ratio(0.30, 100, n_trials=100)
    checks.append(("DSR falls as trials rise (multiple-testing penalty)",
                   d1 is not None and d100 is not None and d1 > d100))
    checks.append(("strong genuine edge survives deflation",
                   (deflated_sharpe_ratio(0.60, 500, n_trials=5) or 0) > 0.95))
    checks.append(("weak edge found after 200 trials is rejected",
                   (deflated_sharpe_ratio(0.12, 100, n_trials=200) or 1) < 0.95))
    checks.append(("DSR refuses tiny samples", deflated_sharpe_ratio(1.0, 2) is None))
    checks.append(("DSR rises with sample size at equal Sharpe",
                   (deflated_sharpe_ratio(0.20, 1000, 5) or 0)
                   > (deflated_sharpe_ratio(0.20, 100, 5) or 0)))
    checks.append(("REGRESSION: sr0 scales with 1/sqrt(n), never unit variance",
                   expected_max_sharpe(5, (1 + 0.5 * 0.36) / 500) < 0.10))

    # PBO: pure noise -> selection carries no information
    g = _lcg(99)
    noise = [[next(g) - 0.5 for _ in range(6)] for _ in range(40)]
    pbo_noise = probability_of_backtest_overfitting(noise, n_partitions=6)
    checks.append(("PBO computable on a noise matrix",
                   pbo_noise is not None and 0.0 <= pbo_noise["pbo"] <= 1.0))
    # PBO: one genuinely dominant variant -> low PBO
    strong = [[(1.0 if j == 0 else 0.0) + (next(g) - 0.5) * 0.2
               for j in range(6)] for _ in range(40)]
    pbo_strong = probability_of_backtest_overfitting(strong, n_partitions=6)
    checks.append(("PBO near zero when one variant truly dominates",
                   pbo_strong is not None and pbo_strong["pbo"] < 0.1))
    checks.append(("PBO rejects degenerate input",
                   probability_of_backtest_overfitting([[1.0]], 4) is None
                   and probability_of_backtest_overfitting([], 4) is None))

    # composite gate
    tiny = validate_hypothesis(_normals(40, 0.05, 1.0), horizon=30)
    checks.append(("gate: small overlapping sample REJECTED on ESS",
                   tiny["verdict"] == "REJECT"
                   and any("ESS" in r for r in tiny["reasons"])))
    nodata = validate_hypothesis([], horizon=30)
    checks.append(("gate: empty input never passes",
                   nodata["verdict"] in ("REJECT", "INSUFFICIENT")))
    # sr=0.08 over ESS~400 is t~1.6 — correctly NOT significant. A fixture
    # must carry a detectable edge (sr=0.15 -> t~3.0) to test the PASS path.
    weak_but_real = validate_hypothesis(_normals(2000, 0.08, 1.0), horizon=5,
                                        n_trials=3)
    checks.append(("gate: sub-significant edge is NOT passed (t~1.6)",
                   weak_but_real["verdict"] in ("REJECT", "INSUFFICIENT")))
    checks.append(("gate: PBO missing => INSUFFICIENT, never silent PASS",
                   validate_hypothesis(_normals(2000, 0.15, 1.0), horizon=5,
                                       n_trials=3)["checks"][3]["status"]
                   == "INSUFFICIENT"))
    marginal = validate_hypothesis(_normals(2000, 0.15, 1.0), horizon=5,
                                   n_trials=3, performance_matrix=strong)
    checks.append(("gate: MARGINAL edge (t~2.4) is refused, not waved through",
                   marginal["verdict"] == "REJECT"
                   and any("DSR" in r for r in marginal["reasons"])))
    full = validate_hypothesis(_normals(2000, 0.30, 1.0), horizon=5, n_trials=3,
                               performance_matrix=strong)
    checks.append(("gate: UNAMBIGUOUS edge with all inputs present PASSES",
                   full["verdict"] == "PASS"))
    overfit = validate_hypothesis(_normals(2000, 0.02, 1.0), horizon=5,
                                  n_trials=500, performance_matrix=noise)
    checks.append(("gate: weak edge + many trials is REJECTED",
                   overfit["verdict"] == "REJECT"))
    checks.append(("gate prints its thresholds and governance",
                   full["gates"]["min_deflated_sharpe"] == 0.95
                   and "never auto-arms" in full["governance"]))
    checks.append(("gate always reports four checks", len(full["checks"]) == 4))

    passed = sum(1 for _, ok in checks if ok)
    for name, ok in checks:
        print(("PASS " if ok else "FAIL ") + name)
    print(f"[validation v{__version__}] SELFTEST {passed}/{len(checks)}")
    return 0 if passed == len(checks) else 1


if __name__ == "__main__":
    raise SystemExit(_selftest())
