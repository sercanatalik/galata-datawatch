"""The derived figures on matrices made by hand."""

from __future__ import annotations

import numpy as np
import pytest

from galata_signals import matrix


def a_beta_against_itself_is_not_written():
    s = np.array([[4.0, 2.0, 0.0], [2.0, 9.0, 0.0], [0.0, 0.0, 1.0]])
    b = matrix.beta(s, ["BTC", "ETH", "GOLD"])
    assert set(b) == {"ETH", "GOLD"}
    assert b["ETH"] == pytest.approx((0.5, 1 - 4 / 36))
    assert b["GOLD"] == pytest.approx((0.0, 1.0))


def every_market_moving_as_one_absorbs_everything():
    s = np.ones((6, 6)) * 2.0
    cov_ar, cor_ar = matrix.absorption(s)
    assert cov_ar == pytest.approx(1.0) and cor_ar == pytest.approx(1.0)


def an_independent_universe_absorbs_one_nth():
    cov_ar, cor_ar = matrix.absorption(np.eye(6) * 3.0)
    assert cor_ar == pytest.approx(1 / 6) and cov_ar == pytest.approx(1 / 6)


def the_covariance_ar_follows_the_most_volatile():
    # Uncorrelated, one instrument ten times as volatile: the covariance form reads
    # it as absorption, the correlation form does not.
    cov_ar, cor_ar = matrix.absorption(np.diag([100.0, 1, 1, 1, 1, 1]))
    assert cov_ar == pytest.approx(100 / 105) and cor_ar == pytest.approx(1 / 6)


def a_surprise_is_judged_against_the_forecast_before_it():
    s = np.diag([4.0, 9.0, 1.0])
    found = matrix.surprise([2.0, -3.0, 1.0], s)
    assert found["mahalanobis"] == pytest.approx(3.0)
    assert found["magnitude_surprise"] == pytest.approx(1.0)
    assert found["correlation_surprise"] == pytest.approx(1.0)
    assert 0 < found["chi2_percentile"] < 1


def a_move_against_the_correlation_is_the_correlation_surprise():
    s = np.array([[1.0, 0.5], [0.5, 1.0]])
    assert matrix.surprise([1.0, -1.0], s)["correlation_surprise"] == pytest.approx(2.0)
    assert matrix.surprise([1.0, 1.0], s)["correlation_surprise"] == pytest.approx(2 / 3)


def no_move_has_no_correlation_surprise():
    assert matrix.surprise([0.0, 0.0], np.eye(2))["correlation_surprise"] is None


def a_matrix_that_is_not_positive_definite_is_absent():
    with pytest.raises(matrix.Undefined, match="not positive definite"):
        matrix.surprise([1.0, 1.0], np.array([[1.0, 2.0], [2.0, 1.0]]))


def the_turbulence_of_an_ordinary_bar_is_ordinary():
    rng = np.random.default_rng(3)
    x = rng.normal(size=(2000, 4))
    calm = np.vstack([x, np.zeros(4) + x.mean(axis=0)])
    wild = np.vstack([x, np.array([6.0, -6.0, 6.0, -6.0])])
    assert matrix.turbulence(calm)[1] < 0.01
    assert matrix.turbulence(wild)[1] == pytest.approx(1.0)
