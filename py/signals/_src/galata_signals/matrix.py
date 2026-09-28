"""Figures derived from a covariance matrix: beta, absorption, surprise, turbulence.

Pure functions over numpy arrays, so each is checked on a matrix made by hand.
A figure that cannot be computed raises `Undefined`, and the calculator writes
it absent with the reason.

- **Beta** to a reference: Σ_iB / Σ_BB, Engle's (2016, *JFEc* 14(4), Eq. 3)
  dynamic conditional beta with one regressor — the ratio of the conditional
  covariance to the conditional variance (Bollerslev, Engle and Wooldridge
  1988) — and the idiosyncratic share 1 − ρ².
- **Absorption ratio** (Kritzman, Li, Page and Rigobon 2011, *JPM* 37(4),
  Eq. 1): the variance of the first n eigenportfolios over the total, n ≈ N/5,
  so 1 at N = 6. On the covariance, the paper's; on the correlation, the
  co-movement of a universe whose volatilities differ by a factor of ten.
- **Surprise** (the conditional form): the last bar's returns against the Σ
  forecast before it. d = yᵀΣ⁻¹y is χ²_N under the model with Gaussian
  returns, so its percentile is the model's check. Split by Kinlaw and
  Turkington (2013, *JAM* 14): magnitude (the correlation-blind mean of
  squared z) and correlation surprise, (d/N) / magnitude.
- **Turbulence** (the historical form; Kritzman and Li 2010, *FAJ* 66(5),
  Eq. 2): d against the sample's own mean and covariance, ranked among the
  sample's own values — relative by their own account, and in-sample.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

#: Under this, a magnitude surprise is zero moves, and a ratio over it is not a figure.
MAGNITUDE_FLOOR = 1e-12


class Undefined(ValueError):
    """A figure the inputs do not define, with the reason."""


def _sym(sigma) -> np.ndarray:
    s = np.asarray(sigma, dtype=float)
    return (s + s.T) / 2


def _chol(sigma: np.ndarray) -> np.ndarray:
    try:
        return np.linalg.cholesky(sigma)
    except np.linalg.LinAlgError:
        raise Undefined("the covariance matrix is not positive definite") from None


def mahalanobis(y, sigma) -> float:
    """yᵀΣ⁻¹y through the Cholesky factor, never an explicit inverse."""
    z = np.linalg.solve(_chol(_sym(sigma)), np.asarray(y, dtype=float))
    return float(z @ z)


def beta(sigma, tickers: list[str], reference: str = "BTC") -> dict[str, tuple[float, float]]:
    """`{ticker: (beta, idiosyncratic_share)}` for every ticker but the reference."""
    if reference not in tickers:
        raise Undefined(f"{reference} is not among the instruments")
    s = _sym(sigma)
    b = tickers.index(reference)
    if s[b, b] <= 0:
        raise Undefined(f"{reference}'s variance is not positive")
    out = {}
    for i, t in enumerate(tickers):
        if i == b:
            continue
        rho2 = s[i, b] ** 2 / (s[i, i] * s[b, b])
        out[t] = (float(s[i, b] / s[b, b]), float(1 - rho2))
    return out


def absorption(sigma, n: int = 1) -> tuple[float, float]:
    """`(covariance_ar, correlation_ar)`: the first `n` eigenvalues' share of the total, of Σ and of R."""
    s = _sym(sigma)
    d = np.sqrt(np.diag(s))
    if np.any(d <= 0):
        raise Undefined("a variance is not positive")
    r = s / np.outer(d, d)
    np.fill_diagonal(r, 1.0)
    cov = np.sort(np.linalg.eigvalsh(s))[::-1]
    cor = np.sort(np.linalg.eigvalsh(r))[::-1]
    return float(cov[:n].sum() / np.trace(s)), float(cor[:n].sum() / len(d))


def surprise(y, sigma) -> dict[str, float | None]:
    """The last bar against the forecast before it: mahalanobis, chi2_percentile, magnitude, correlation surprise."""
    y = np.asarray(y, dtype=float)
    s = _sym(sigma)
    n = len(y)
    d = mahalanobis(y, s)
    magnitude = float(np.mean(y**2 / np.diag(s)))
    return {
        "mahalanobis": d,
        "chi2_percentile": float(stats.chi2.cdf(d, n)),
        "magnitude_surprise": magnitude,
        "correlation_surprise": (d / n) / magnitude if magnitude > MAGNITUDE_FLOOR else None,
    }


def turbulence(returns) -> tuple[float, float]:
    """`(turbulence, percentile)` of the last row of a (T × N) sample, against the sample's own mean and covariance."""
    x = np.asarray(returns, dtype=float)
    if x.shape[0] <= x.shape[1]:
        raise Undefined(f"{x.shape[0]} joint returns cannot estimate a {x.shape[1]}×{x.shape[1]} covariance")
    mu = x.mean(axis=0)
    s = np.cov(x, rowvar=False)
    z = np.linalg.solve(_chol(_sym(s)), (x - mu).T)
    d = (z * z).sum(axis=0)
    return float(d[-1]), float((d <= d[-1]).mean())
