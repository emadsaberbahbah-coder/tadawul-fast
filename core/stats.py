# -*- coding: utf-8 -*-
"""
core.stats — TFB centralized statistical primitives (one source of truth)
=========================================================================
Version: 1.0.0

WHY THIS MODULE EXISTS
----------------------
Before this file, every module that needed a statistic invented its own:
`track_performance.py` ran Monte-Carlo win-rate on `random`, `scoring.py`
mapped factors with `math`, `news_intelligence.py` rolled its own sentiment
arithmetic, and `scipy` was imported in some places but commented out of
`requirements.txt`. That guarantees drift — the same cross-file failure mode
the data-quality audit already forbids ("one source of truth for whether a row
is trustworthy"), now applied to math. When the Calibrator and the (b)/(c)
event-study backtests arrive, they would each re-implement confidence intervals
and significance tests AGAIN, inconsistently.

This module is the single vetted home for every statistical primitive the
strategy needs. Every downstream consumer (the backtest harness, the Calibrator,
the scoring layer, the data-quality gate, the news/event layer) calls THESE
functions, so the statistics are consistent, testable, and auditable.

DESIGN PRINCIPLES
-----------------
1. numpy-required, scipy-OPTIONAL. numpy is already a core dependency. scipy is
   used only to sharpen p-values when present; pure-numpy fallbacks (Acklam
   inverse-normal, erfc normal CDF, Numerical-Recipes incomplete beta for the
   Student-t tail) make every function fully functional WITHOUT scipy. This is
   the same try/except pattern `track_performance.py` already uses, and it means
   this file deploys unchanged in both the lean web image (no scipy) and the
   analytics crons (scipy enabled). No new hard dependency is introduced.
2. Pure functions. No logging, no TFB-internal imports, no global state, no I/O.
   This keeps the module free of circular imports and trivially unit-testable.
3. JSON-serializable outputs. Results are plain dicts / floats / lists, because
   they are logged to Google Sheets (Performance_Log, Signal_History, etc.).
4. Deterministic resampling. The bootstrap uses a fixed default seed so the same
   data always yields the same interval — required for auditability.
5. Honest edge handling. Degenerate inputs (empty, n<2, zero variance) return
   NaN, never a crash and never a fabricated number.

NUMPY COMPATIBILITY
-------------------
Written against the production pin numpy>=1.26.4,<2.0.0. Only stable core APIs
are used (median, percentile, digitize, argsort, cumsum, nan-aware reductions,
default_rng); no aliases removed in numpy 2.x are referenced, so it also runs on
numpy 2.x environments.
"""

from __future__ import annotations

import math
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

try:
    import numpy as np  # type: ignore

    NUMPY_AVAILABLE = True
except Exception:  # pragma: no cover - numpy is a core dependency
    np = None  # type: ignore
    NUMPY_AVAILABLE = False

try:
    from scipy import stats as _scipy_stats  # type: ignore

    SCIPY_AVAILABLE = True
except Exception:
    _scipy_stats = None  # type: ignore
    SCIPY_AVAILABLE = False

__version__ = "1.0.0"

__all__ = [
    # distribution helpers
    "normal_cdf",
    "normal_ppf",
    "t_sf_two_sided",
    # hygiene
    "winsorize",
    "cross_sectional_z",
    "mad",
    "modified_zscore",
    "is_mad_outlier",
    "mad_outliers",
    # resampling
    "block_bootstrap_ci",
    # multiple testing
    "benjamini_hochberg",
    # power / testability (the B0 gate)
    "required_n_two_prop",
    "min_detectable_effect",
    "effective_sample_size",
    "testability_report",
    # signal strength
    "information_coefficient",
    # calibration
    "wilson_interval",
    "brier_score",
    "reliability_curve",
    "expected_calibration_error",
    "spiegelhalter_z",
    "isotonic_fit",
    "isotonic_apply",
    # event study
    "car_event_study",
]

_NAN = float("nan")
_SQRT2 = math.sqrt(2.0)
_SQRT_2PI = math.sqrt(2.0 * math.pi)


# =============================================================================
# SECTION 1 — Distribution helpers (scipy-optional, pure-numpy fallbacks)
# =============================================================================

def normal_cdf(x: float) -> float:
    """Standard normal CDF Phi(x). Uses erfc for tail stability."""
    try:
        return 0.5 * math.erfc(-float(x) / _SQRT2)
    except Exception:
        return _NAN


def normal_ppf(p: float) -> float:
    """
    Inverse standard normal CDF (quantile function).

    Acklam's rational approximation followed by one Halley refinement step;
    accurate to ~1e-12. Used wherever a z-multiplier is needed (Wilson
    intervals, power calculations, calibration bands).
    """
    p = float(p)
    if not (0.0 < p < 1.0):
        if p == 0.0:
            return float("-inf")
        if p == 1.0:
            return float("inf")
        return _NAN

    a = (-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02,
         1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00)
    b = (-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02,
         6.680131188771972e01, -1.328068155288572e01)
    c = (-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00,
         -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00)
    d = (7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00,
         3.754408661907416e00)

    plow, phigh = 0.02425, 1.0 - 0.02425
    if p < plow:
        q = math.sqrt(-2.0 * math.log(p))
        x = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
    elif p <= phigh:
        q = p - 0.5
        r = q * q
        x = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / \
            (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    else:
        q = math.sqrt(-2.0 * math.log(1.0 - p))
        x = -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)

    # one Halley step for full double precision
    e = normal_cdf(x) - p
    u = e * _SQRT_2PI * math.exp(x * x / 2.0)
    x = x - u / (1.0 + x * u / 2.0)
    return x


def _betacf(a: float, b: float, x: float) -> float:
    """Continued fraction for the incomplete beta function (Lentz's method)."""
    MAXIT = 300
    EPS = 3.0e-16
    FPMIN = 1.0e-300
    qab = a + b
    qap = a + 1.0
    qam = a - 1.0
    c = 1.0
    d = 1.0 - qab * x / qap
    if abs(d) < FPMIN:
        d = FPMIN
    d = 1.0 / d
    h = d
    for m in range(1, MAXIT + 1):
        m2 = 2 * m
        aa = m * (b - m) * x / ((qam + m2) * (a + m2))
        d = 1.0 + aa * d
        if abs(d) < FPMIN:
            d = FPMIN
        c = 1.0 + aa / c
        if abs(c) < FPMIN:
            c = FPMIN
        d = 1.0 / d
        h *= d * c
        aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2))
        d = 1.0 + aa * d
        if abs(d) < FPMIN:
            d = FPMIN
        c = 1.0 + aa / c
        if abs(c) < FPMIN:
            c = FPMIN
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < EPS:
            break
    return h


def _betai(a: float, b: float, x: float) -> float:
    """Regularized incomplete beta function I_x(a, b)."""
    if x <= 0.0:
        return 0.0
    if x >= 1.0:
        return 1.0
    lbeta = math.lgamma(a + b) - math.lgamma(a) - math.lgamma(b)
    bt = math.exp(lbeta + a * math.log(x) + b * math.log(1.0 - x))
    if x < (a + 1.0) / (a + b + 2.0):
        return bt * _betacf(a, b, x) / a
    return 1.0 - bt * _betacf(b, a, 1.0 - x) / b


def t_sf_two_sided(t: float, df: float) -> float:
    """
    Two-sided Student-t tail probability P(|T| >= |t|) with `df` degrees of
    freedom. Uses scipy when available, otherwise the incomplete-beta identity.
    """
    try:
        t = abs(float(t))
        df = float(df)
        if df <= 0 or not math.isfinite(t):
            return _NAN
        if SCIPY_AVAILABLE:
            return float(2.0 * _scipy_stats.t.sf(t, df))
        x = df / (df + t * t)
        return float(_betai(df / 2.0, 0.5, x))
    except Exception:
        return _NAN


# =============================================================================
# Internal array coercion helpers
# =============================================================================

def _arr(x: Sequence[Any]) -> "np.ndarray":
    return np.asarray(x, dtype=float).ravel()


def _coerce1d(x: Sequence[Any], drop_nan: bool = False) -> "np.ndarray":
    a = _arr(x)
    if drop_nan:
        a = a[np.isfinite(a)]
    return a


def _coerce_pair(x: Sequence[Any], y: Sequence[Any]) -> Tuple["np.ndarray", "np.ndarray"]:
    a = _arr(x)
    b = _arr(y)
    m = min(a.size, b.size)
    a, b = a[:m], b[:m]
    mask = np.isfinite(a) & np.isfinite(b)
    return a[mask], b[mask]


def _coerce_probs_outcomes(probs: Sequence[Any], outcomes: Sequence[Any]
                           ) -> Tuple["np.ndarray", "np.ndarray"]:
    p = _arr(probs)
    o = _arr(outcomes)
    m = min(p.size, o.size)
    p, o = p[:m], o[:m]
    mask = np.isfinite(p) & np.isfinite(o)
    p = np.clip(p[mask], 0.0, 1.0)
    # outcomes treated as binary: anything >= 0.5 counts as a hit
    o = (o[mask] >= 0.5).astype(float)
    return p, o


# =============================================================================
# SECTION 2 — Descriptive hygiene (used by scoring.py)
# =============================================================================

def winsorize(x: Sequence[float], lower: float = 0.01, upper: float = 0.99
              ) -> "np.ndarray":
    """
    Clip values to the [lower, upper] empirical percentiles, computed on finite
    values only. Defends factor inputs against single-symbol outliers before
    standardisation. NaNs are clipped along with everything else (NaN stays NaN).
    """
    a = _arr(x)
    finite = a[np.isfinite(a)]
    if finite.size == 0:
        return a.copy()
    lo = float(np.percentile(finite, lower * 100.0))
    hi = float(np.percentile(finite, upper * 100.0))
    return np.clip(a, lo, hi)


def cross_sectional_z(x: Sequence[float], robust: bool = False) -> "np.ndarray":
    """
    Standardise a factor across the symbol universe to mean 0 / sd 1 so factor
    weights mean what they say. `robust=True` uses median / (1.4826 * MAD),
    which resists outliers. Zero-variance input returns all zeros (no signal).
    """
    a = _arr(x)
    finite = a[np.isfinite(a)]
    if finite.size == 0:
        return a.copy()
    if robust:
        med = float(np.median(finite))
        md = float(np.median(np.abs(finite - med))) * 1.4826
        if md == 0.0:
            return np.zeros_like(a)
        return (a - med) / md
    if finite.size < 2:
        return np.zeros_like(a)
    mu = float(finite.mean())
    sd = float(finite.std(ddof=1))
    if sd == 0.0:
        return np.zeros_like(a)
    return (a - mu) / sd


def mad(x: Sequence[float]) -> float:
    """Median absolute deviation (raw, not scaled)."""
    a = _coerce1d(x, drop_nan=True)
    if a.size == 0:
        return _NAN
    med = float(np.median(a))
    return float(np.median(np.abs(a - med)))


def modified_zscore(history: Sequence[float], value: float) -> float:
    """
    Iglewicz-Hoaglin modified z-score of `value` against a `history`
    distribution: 0.6745 * (value - median) / MAD. Robust to outliers in the
    history itself. Falls back to mean-absolute-deviation scaling if MAD == 0.
    """
    a = _coerce1d(history, drop_nan=True)
    if a.size == 0:
        return _NAN
    med = float(np.median(a))
    md = float(np.median(np.abs(a - med)))
    value = float(value)
    if md == 0.0:
        mean_ad = float(np.mean(np.abs(a - med)))
        if mean_ad == 0.0:
            return 0.0 if value == med else float("inf")
        return 0.7979 * (value - med) / mean_ad
    return 0.6745 * (value - med) / md


def is_mad_outlier(history: Sequence[float], value: float, k: float = 3.5) -> bool:
    """
    True if `value` is more than `k` modified-z (MAD) units from the history
    median. This is the principled cost-basis plausibility check: a $250 cost
    basis against a ~$3.50 price history is a >40-MAD outlier and fails trivially.
    k=3.5 is the Iglewicz-Hoaglin recommendation.
    """
    z = modified_zscore(history, value)
    if not math.isfinite(z):
        # inf => extreme outlier vs a constant history; NaN => no history
        return math.isinf(z)
    return abs(z) > float(k)


def mad_outliers(x: Sequence[float], k: float = 3.5) -> List[bool]:
    """Vectorised `is_mad_outlier`: flag each point against the whole sample."""
    a = _arr(x)
    finite = a[np.isfinite(a)]
    if finite.size == 0:
        return [False] * a.size
    med = float(np.median(finite))
    md = float(np.median(np.abs(finite - med)))
    out: List[bool] = []
    for v in a:
        if not math.isfinite(v):
            out.append(False)
            continue
        if md == 0.0:
            out.append(v != med)
        else:
            out.append(abs(0.6745 * (v - med) / md) > float(k))
    return out


# =============================================================================
# SECTION 3 — Bootstrap / resampling (used by the backtest harness, Calibrator)
# =============================================================================

def block_bootstrap_ci(
    data: Sequence[float],
    statistic: Optional[Callable[["np.ndarray"], float]] = None,
    n_boot: int = 2000,
    block_size: Optional[int] = None,
    ci: float = 0.95,
    seed: int = 12345,
) -> Dict[str, Any]:
    """
    Moving-block bootstrap confidence interval for a statistic of a TIME-ORDERED
    series. Unlike an IID resample, blocks of consecutive observations preserve
    short-range autocorrelation, so the interval is honest for overlapping
    market data (hit-rates, returns). This is the correct upgrade over a plain
    Monte-Carlo `simulate_win_rate`.

    Args:
        data: 1-D time-ordered sequence (e.g. per-period outcomes or returns).
        statistic: array -> float; defaults to the mean.
        n_boot: number of bootstrap resamples.
        block_size: block length; defaults to round(n ** (1/3)).
        ci: confidence level (0.95 -> 95% percentile interval).
        seed: fixed for reproducibility (override only deliberately).

    Returns dict: point, ci_low, ci_high, n, n_boot, block_size, ci.
    """
    x = _coerce1d(data, drop_nan=True)
    n = int(x.size)
    if statistic is None:
        statistic = lambda a: float(np.mean(a))
    if n == 0:
        return {"point": _NAN, "ci_low": _NAN, "ci_high": _NAN,
                "n": 0, "n_boot": 0, "block_size": 0, "ci": ci}
    point = float(statistic(x))
    if n < 2:
        return {"point": point, "ci_low": _NAN, "ci_high": _NAN,
                "n": n, "n_boot": 0, "block_size": min(1, n), "ci": ci}

    if block_size is None:
        block_size = max(1, int(round(n ** (1.0 / 3.0))))
    block_size = int(min(block_size, n))
    rng = np.random.default_rng(seed)
    n_blocks = int(math.ceil(n / block_size))
    max_start = n - block_size
    offsets = np.arange(block_size)
    boot = np.empty(int(n_boot), dtype=float)
    for i in range(int(n_boot)):
        starts = rng.integers(0, max_start + 1, size=n_blocks)
        idx = (starts[:, None] + offsets[None, :]).ravel()[:n]
        boot[i] = statistic(x[idx])
    alpha = 1.0 - ci
    lo = float(np.percentile(boot, 100.0 * alpha / 2.0))
    hi = float(np.percentile(boot, 100.0 * (1.0 - alpha / 2.0)))
    return {"point": point, "ci_low": lo, "ci_high": hi,
            "n": n, "n_boot": int(n_boot), "block_size": block_size, "ci": ci}


# =============================================================================
# SECTION 4 — Multiple-testing correction (used by the hypothesis backtest gate)
# =============================================================================

def benjamini_hochberg(pvalues: Sequence[float], alpha: float = 0.05) -> Dict[str, Any]:
    """
    Benjamini-Hochberg false-discovery-rate control across many simultaneously
    tested hypotheses (every registered event->sector / theme->rotation claim).
    More powerful than a Bonferroni floor while still bounding the expected
    proportion of false discoveries at `alpha`.

    Returns dict: reject (list[bool], input order), pvalues_adjusted
    (list[float], input order), n_significant, n_tests, alpha.
    """
    p = _arr(pvalues)
    m = int(p.size)
    if m == 0:
        return {"reject": [], "pvalues_adjusted": [], "n_significant": 0,
                "n_tests": 0, "alpha": alpha}
    order = np.argsort(p, kind="mergesort")
    ranked = p[order]
    adj_sorted = np.empty(m, dtype=float)
    prev = 1.0
    for i in range(m - 1, -1, -1):
        val = ranked[i] * m / (i + 1)
        prev = min(prev, val)
        adj_sorted[i] = min(prev, 1.0)
    reject_sorted = adj_sorted <= float(alpha)
    adjusted = np.empty(m, dtype=float)
    adjusted[order] = adj_sorted
    reject = np.empty(m, dtype=bool)
    reject[order] = reject_sorted
    return {"reject": reject.tolist(), "pvalues_adjusted": adjusted.tolist(),
            "n_significant": int(reject.sum()), "n_tests": m, "alpha": float(alpha)}


# =============================================================================
# SECTION 5 — Power / testability  ===  THE B0 GATE
# =============================================================================
# Operationalises the master-plan B0 constraint ("a rare trigger at a long
# horizon is untestable at 16 symbols") as math, so the harness can REFUSE an
# underpowered hypothesis and return UNTESTABLE_BY_DESIGN instead of silently
# returning INSUFFICIENT_DATA that looks like "no edge".

def _power_two_prop(p0: float, p1: float, n: float,
                    alpha: float = 0.05, two_sided: bool = True) -> float:
    """Power of a two-proportion z-test, equal n per group."""
    if n <= 0:
        return 0.0
    pbar = (p0 + p1) / 2.0
    se0 = math.sqrt(max(2.0 * pbar * (1.0 - pbar) / n, 0.0))
    se1 = math.sqrt(max(p0 * (1.0 - p0) / n + p1 * (1.0 - p1) / n, 0.0))
    if se1 <= 0.0:
        return _NAN
    za = normal_ppf(1.0 - alpha / 2.0) if two_sided else normal_ppf(1.0 - alpha)
    delta = abs(p1 - p0)
    power = normal_cdf((delta - za * se0) / se1)
    if two_sided:
        power += normal_cdf((-delta - za * se0) / se1)
    return min(max(power, 0.0), 1.0)


def required_n_two_prop(p0: float, p1: float, alpha: float = 0.05,
                        power: float = 0.8, two_sided: bool = True) -> float:
    """
    Sample size per group needed to detect a difference between proportions p0
    and p1 at the given alpha and power. Returns +inf if p0 == p1.
    """
    p0, p1 = float(p0), float(p1)
    if p1 == p0:
        return float("inf")
    za = normal_ppf(1.0 - alpha / 2.0) if two_sided else normal_ppf(1.0 - alpha)
    zb = normal_ppf(power)
    pbar = (p0 + p1) / 2.0
    num = (za * math.sqrt(2.0 * pbar * (1.0 - pbar))
           + zb * math.sqrt(p0 * (1.0 - p0) + p1 * (1.0 - p1))) ** 2
    return math.ceil(num / (p1 - p0) ** 2)


def min_detectable_effect(p0: float, n: float, alpha: float = 0.05,
                          power: float = 0.8, two_sided: bool = True,
                          direction: str = "increase") -> float:
    """
    Smallest absolute effect |p1 - p0| detectable with `n` observations per
    group at the given power. Found by bisection on the power curve (monotone in
    effect size). Returns NaN if even the maximal effect cannot reach the target
    power at this n — itself a signal of untestability.
    """
    p0 = float(p0)
    n = float(n)
    if n <= 0:
        return _NAN
    if direction == "increase":
        a, b = p0 + 1e-9, 1.0 - 1e-9
    else:
        a, b = 1e-9, p0 - 1e-9
    if a >= b:
        return _NAN
    if _power_two_prop(p0, b, n, alpha, two_sided) < power:
        return _NAN  # undetectable at any plausible effect for this n
    lo, hi = a, b
    for _ in range(100):
        mid = (lo + hi) / 2.0
        if _power_two_prop(p0, mid, n, alpha, two_sided) < power:
            lo = mid
        else:
            hi = mid
    return abs(hi - p0)


def effective_sample_size(total_windows: float, trigger_rate: float) -> float:
    """
    Expected number of windows in which a trigger with base frequency
    `trigger_rate` actually fires. The crux of B0: 384 windows * a 3% trigger
    rate is only ~11.5 usable observations, which is why a rare long-horizon
    signal is untestable regardless of edge.
    """
    return float(total_windows) * float(trigger_rate)


def testability_report(total_windows: float, trigger_rate: float, base_rate: float,
                       plausible_effect: float, alpha: float = 0.05,
                       power: float = 0.8, two_sided: bool = True) -> Dict[str, Any]:
    """
    Pre-registration gate for a hypothesis. Combines the effective sample size
    with the minimum detectable effect to deliver a verdict the harness can act
    on BEFORE spending a backtest.

    Verdict is one of:
        "TESTABLE"             - a plausibly-sized effect could be detected
        "UNTESTABLE_BY_DESIGN" - the design can never see the effect; redesign
                                 (shorter horizon -> more windows, wider symbol
                                 universe, or a commoner trigger)

    Returns dict with the verdict plus every number behind it, so the decision
    is fully auditable: effective_n, min_detectable_effect, plausible_effect,
    required_effective_n (to detect the plausible effect), and a recommendation.
    """
    n_eff = effective_sample_size(total_windows, trigger_rate)
    mde = min_detectable_effect(base_rate, n_eff, alpha, power, two_sided)
    required_eff_n = required_n_two_prop(
        base_rate, min(base_rate + plausible_effect, 0.999999),
        alpha, power, two_sided)
    testable = math.isfinite(mde) and (mde <= plausible_effect)
    verdict = "TESTABLE" if testable else "UNTESTABLE_BY_DESIGN"
    if testable:
        rec = ("Effect of the plausible size is detectable with the available "
               "effective sample.")
    else:
        rec = ("Underpowered: redesign for testability (shorter horizon for more "
               "independent windows, wider symbol universe, or a more common "
               "trigger). Do NOT read an INSUFFICIENT result here as 'no edge'.")
    return {
        "verdict": verdict,
        "testable": bool(testable),
        "effective_n": float(n_eff),
        "min_detectable_effect": (None if not math.isfinite(mde) else float(mde)),
        "plausible_effect": float(plausible_effect),
        "required_effective_n": (None if not math.isfinite(required_eff_n)
                                 else float(required_eff_n)),
        "total_windows": float(total_windows),
        "trigger_rate": float(trigger_rate),
        "base_rate": float(base_rate),
        "alpha": float(alpha),
        "power": float(power),
        "recommendation": rec,
    }


# =============================================================================
# SECTION 6 — Signal strength (used by the harness and scoring)
# =============================================================================

def _rankdata(a: "np.ndarray") -> "np.ndarray":
    """Average-tie ranks (1-indexed), matching scipy.stats.rankdata defaults."""
    order = np.argsort(a, kind="mergesort")
    ranks = np.empty(a.size, dtype=float)
    ranks[order] = np.arange(1, a.size + 1, dtype=float)
    srt = a[order]
    i = 0
    n = a.size
    while i < n:
        j = i
        while j + 1 < n and srt[j + 1] == srt[i]:
            j += 1
        if j > i:
            avg = (i + 1 + j + 1) / 2.0
            for k in range(i, j + 1):
                ranks[order[k]] = avg
        i = j + 1
    return ranks


def _pearson(a: "np.ndarray", b: "np.ndarray") -> float:
    a = a - a.mean()
    b = b - b.mean()
    denom = math.sqrt(float((a * a).sum()) * float((b * b).sum()))
    if denom == 0.0:
        return _NAN
    return float((a * b).sum() / denom)


def information_coefficient(scores: Sequence[float], forward_returns: Sequence[float],
                            method: str = "spearman") -> Dict[str, Any]:
    """
    Information Coefficient: correlation between today's score and the realised
    forward return, across the whole cross-section. The standard quant measure of
    "does the score carry signal", and — unlike hit-rate — it uses EVERY symbol
    every day, not just the ones that triggered a STRONG label. That makes it far
    more data-efficient and directly mitigates the B0 "too few windows" problem.

    method: "spearman" (rank IC, robust, default) or "pearson".

    Returns dict: ic, t_stat, p_value, n, method.
    """
    s, r = _coerce_pair(scores, forward_returns)
    n = int(s.size)
    if n < 3:
        return {"ic": _NAN, "t_stat": _NAN, "p_value": _NAN, "n": n, "method": method}
    if method == "spearman":
        a, b = _rankdata(s), _rankdata(r)
    else:
        a, b = s, r
    ic = _pearson(a, b)
    if not math.isfinite(ic) or abs(ic) >= 1.0:
        return {"ic": (None if not math.isfinite(ic) else float(ic)),
                "t_stat": _NAN, "p_value": (_NAN if not math.isfinite(ic) else 0.0),
                "n": n, "method": method}
    t = ic * math.sqrt((n - 2) / (1.0 - ic * ic))
    p = t_sf_two_sided(t, n - 2)
    return {"ic": float(ic), "t_stat": float(t), "p_value": float(p),
            "n": n, "method": method}


# =============================================================================
# SECTION 7 — Calibration (THE CALIBRATOR'S TOOLKIT)
# =============================================================================
# Everything the ~Jul-18 Calibrator needs to turn "forecast readiness 75.6%"
# into a MEASURED number: claimed reliability vs realised outcomes.

def wilson_interval(successes: float, n: float, confidence: float = 0.95
                    ) -> Tuple[float, float]:
    """
    Wilson score interval for a binomial proportion. Correct for the small,
    sparse bins a fresh calibration curve will have (the normal approximation
    breaks there). Returns (low, high); (NaN, NaN) when n == 0.
    """
    n = float(n)
    if n <= 0:
        return (_NAN, _NAN)
    z = normal_ppf((1.0 + confidence) / 2.0)
    phat = float(successes) / n
    denom = 1.0 + z * z / n
    center = (phat + z * z / (2.0 * n)) / denom
    half = (z * math.sqrt(phat * (1.0 - phat) / n + z * z / (4.0 * n * n))) / denom
    return (max(0.0, center - half), min(1.0, center + half))


def brier_score(probs: Sequence[float], outcomes: Sequence[float], n_bins: int = 10
                ) -> Dict[str, Any]:
    """
    Brier score and its Murphy decomposition. The single scalar quality of a
    probabilistic forecast, split into the parts that say WHY it is good or bad:

        brier        = mean((p - o)^2)              (lower is better)
        reliability  = miscalibration               (lower is better)
        resolution   = ability to separate outcomes (higher is better)
        uncertainty  = irreducible base-rate variance
        skill_score  = 1 - brier/uncertainty        (Brier Skill Score vs the
                       base-rate forecast; >0 means better than always
                       predicting the base rate)

    Satisfies brier ~= reliability - resolution + uncertainty.
    """
    p, o = _coerce_probs_outcomes(probs, outcomes)
    n = int(p.size)
    if n == 0:
        return {"brier": _NAN, "reliability": _NAN, "resolution": _NAN,
                "uncertainty": _NAN, "skill_score": _NAN, "n": 0}
    bs = float(np.mean((p - o) ** 2))
    obar = float(np.mean(o))
    uncertainty = obar * (1.0 - obar)
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    idx = np.digitize(p, edges[1:-1])
    rel = 0.0
    res = 0.0
    for k in range(n_bins):
        mask = idx == k
        nk = int(mask.sum())
        if nk == 0:
            continue
        fk = float(p[mask].mean())
        ok = float(o[mask].mean())
        rel += nk * (fk - ok) ** 2
        res += nk * (ok - obar) ** 2
    rel /= n
    res /= n
    skill = (1.0 - bs / uncertainty) if uncertainty > 0 else _NAN
    return {"brier": bs, "reliability": rel, "resolution": res,
            "uncertainty": uncertainty, "skill_score": skill, "n": n}


def reliability_curve(probs: Sequence[float], outcomes: Sequence[float],
                      n_bins: int = 10, strategy: str = "uniform",
                      confidence: float = 0.95) -> List[Dict[str, Any]]:
    """
    Binned claimed-probability vs realised-frequency, with a Wilson confidence
    interval per bin. This is the reliability diagram: when the tool said "70%",
    did it happen ~70% of the time? `strategy` is "uniform" (equal-width bins) or
    "quantile" (equal-count bins). Returns one dict per non-empty bin.
    """
    p, o = _coerce_probs_outcomes(probs, outcomes)
    if p.size == 0:
        return []
    if strategy == "quantile":
        qs = np.linspace(0.0, 1.0, n_bins + 1)
        edges = np.unique(np.quantile(p, qs))
        if edges.size < 2:
            edges = np.array([0.0, 1.0])
    else:
        edges = np.linspace(0.0, 1.0, n_bins + 1)
    interior = edges[1:-1]
    idx = np.digitize(p, interior)
    out: List[Dict[str, Any]] = []
    nb = edges.size - 1
    for k in range(nb):
        mask = idx == k
        count = int(mask.sum())
        if count == 0:
            continue
        succ = float(o[mask].sum())
        lo, hi = wilson_interval(succ, count, confidence)
        out.append({
            "bin_low": float(edges[k]),
            "bin_high": float(edges[k + 1]),
            "mean_predicted": float(p[mask].mean()),
            "observed_freq": succ / count,
            "count": count,
            "ci_low": lo,
            "ci_high": hi,
        })
    return out


def expected_calibration_error(probs: Sequence[float], outcomes: Sequence[float],
                               n_bins: int = 10) -> Dict[str, Any]:
    """
    Expected Calibration Error (count-weighted mean gap between predicted and
    observed) and Maximum Calibration Error (the worst bin). One number each to
    track calibration over time.
    """
    p, o = _coerce_probs_outcomes(probs, outcomes)
    n = int(p.size)
    if n == 0:
        return {"ece": _NAN, "mce": _NAN, "n": 0, "n_bins": n_bins}
    edges = np.linspace(0.0, 1.0, n_bins + 1)
    idx = np.digitize(p, edges[1:-1])
    ece = 0.0
    mce = 0.0
    for k in range(n_bins):
        mask = idx == k
        nk = int(mask.sum())
        if nk == 0:
            continue
        gap = abs(float(p[mask].mean()) - float(o[mask].mean()))
        ece += (nk / n) * gap
        mce = max(mce, gap)
    return {"ece": float(ece), "mce": float(mce), "n": n, "n_bins": n_bins}


def spiegelhalter_z(probs: Sequence[float], outcomes: Sequence[float]) -> Dict[str, Any]:
    """
    Spiegelhalter's z-test for calibration: is the miscalibration statistically
    real or just small-sample noise? z is asymptotically standard normal under
    perfect calibration. Returns dict: z, p_value (two-sided), n.
    """
    p, o = _coerce_probs_outcomes(probs, outcomes)
    n = int(p.size)
    if n == 0:
        return {"z": _NAN, "p_value": _NAN, "n": 0}
    num = float(np.sum((o - p) * (1.0 - 2.0 * p)))
    den = math.sqrt(float(np.sum((1.0 - 2.0 * p) ** 2 * p * (1.0 - p))))
    if den == 0.0:
        return {"z": _NAN, "p_value": _NAN, "n": n}
    z = num / den
    pval = 2.0 * (1.0 - normal_cdf(abs(z)))
    return {"z": float(z), "p_value": float(pval), "n": n}


def _pava(values: List[float], weights: List[float]) -> List[float]:
    """Pool-Adjacent-Violators: nearest non-decreasing fit (weighted)."""
    blocks: List[List[float]] = []  # [value, weight, size]
    for v, w in zip(values, weights):
        blocks.append([float(v), float(w), 1.0])
        while len(blocks) >= 2 and blocks[-2][0] > blocks[-1][0]:
            v2, w2, s2 = blocks.pop()
            v1, w1, s1 = blocks.pop()
            nv = (v1 * w1 + v2 * w2) / (w1 + w2)
            blocks.append([nv, w1 + w2, s1 + s2])
    out: List[float] = []
    for v, _w, s in blocks:
        out.extend([v] * int(s))
    return out


def isotonic_fit(x: Sequence[float], y: Sequence[float]) -> Dict[str, Any]:
    """
    Fit a monotonic (non-decreasing) calibration map from raw scores/probabilities
    `x` to realised outcomes `y` via isotonic regression (PAVA). Once enough
    outcomes exist, this RECALIBRATES the score->probability map so a claimed
    "75% reliable" actually realises ~75% — turning honesty about miscalibration
    into actual calibration.

    Returns a serializable fit: {x_knots, y_knots, n} for logging, consumable by
    `isotonic_apply`.
    """
    a, b = _coerce_pair(x, y)
    if a.size == 0:
        return {"x_knots": [], "y_knots": [], "n": 0}
    order = np.argsort(a, kind="mergesort")
    xs = a[order]
    ys = b[order]
    fitted = _pava(list(ys), [1.0] * ys.size)
    return {"x_knots": [float(v) for v in xs],
            "y_knots": [float(v) for v in fitted],
            "n": int(a.size)}


def isotonic_apply(fit: Dict[str, Any], x_new: Sequence[float]) -> List[float]:
    """
    Apply a fitted isotonic map to new scores via monotone interpolation,
    clamped to the training range. Returns calibrated values.
    """
    xk = np.asarray(fit.get("x_knots", []), dtype=float)
    yk = np.asarray(fit.get("y_knots", []), dtype=float)
    xn = _arr(x_new)
    if xk.size == 0:
        return [_NAN] * xn.size
    if xk.size == 1:
        return [float(yk[0])] * xn.size
    return [float(v) for v in np.interp(xn, xk, yk)]


# =============================================================================
# SECTION 8 — Event study (THE (b) NEWS->SECTOR PRIMITIVE)
# =============================================================================

def car_event_study(asset_returns: Sequence[Sequence[float]],
                    market_returns: Sequence[Sequence[float]]) -> Dict[str, Any]:
    """
    Market-adjusted event study. For each event, abnormal return AR = asset
    return - market return over the event window; the Cumulative Abnormal Return
    (CAR) is the window sum. Across events, the mean CAR is tested with a
    cross-sectional t-test. This is the rigorous, assumption-light way to answer
    "did this event move this sector" — the statistical backbone for the (b)
    news->sector hypotheses, to be wired in behind the hypothesis registry once
    News_Archive has 1-3 months of data.

    Args:
        asset_returns:  2-D [n_events, window_len] (or a single 1-D window).
        market_returns: same shape, aligned by event and window position.

    Returns dict: mean_car, t_stat, p_value, n_events, car_per_event, aar
    (average abnormal return per window day), caar (cumulative AAR).
    """
    A = np.asarray(asset_returns, dtype=float)
    M = np.asarray(market_returns, dtype=float)
    if A.ndim == 1:
        A = A[None, :]
    if M.ndim == 1:
        M = M[None, :]
    AR = A - M
    car_per_event = np.nansum(AR, axis=1)
    n = int(car_per_event.size)
    if n == 0:
        return {"mean_car": _NAN, "t_stat": _NAN, "p_value": _NAN,
                "n_events": 0, "car_per_event": [], "aar": [], "caar": []}
    mean_car = float(np.nanmean(car_per_event))
    if n > 1:
        sd = float(np.nanstd(car_per_event, ddof=1))
        se = sd / math.sqrt(n) if sd > 0 else _NAN
        t = mean_car / se if (se and math.isfinite(se) and se > 0) else _NAN
        p = t_sf_two_sided(t, n - 1) if math.isfinite(t) else _NAN
    else:
        t = _NAN
        p = _NAN
    aar = np.nanmean(AR, axis=0)
    caar = np.nancumsum(aar)
    return {"mean_car": mean_car, "t_stat": (None if not math.isfinite(t) else float(t)),
            "p_value": (None if not math.isfinite(p) else float(p)),
            "n_events": n, "car_per_event": [float(v) for v in car_per_event],
            "aar": [float(v) for v in aar], "caar": [float(v) for v in caar]}
