"""
Walk-forward parameter selection (decision 41, research/walk-forward-2026-10-19.md).

ParameterOptimizer.run_walk_forward picks each calendar year's parameters on
the data before it and trades them in one continuous account. These tests pin
what makes that figure honest: picks never see their validate year, folds
partition the evaluated span, a switch costs what a real switch costs, and on
paths with no edge the out-of-sample figure finds none while the in-sample
one does. The default call (no walk-forward) must not move at all.
"""

import numpy as np
import pandas as pd
import pytest

from backtesting.parameter_optimizer import (
    ParameterOptimizer,
    walk_forward_folds,
    window_metrics,
)

MA_GRID = [{"short_window": s, "long_window": w} for s in (10, 20, 30) for w in (50, 100, 200)]


def _frame(seed: int, start: str, bars: int, drift: float = 0.0003, vol: float = 0.012):
    index = pd.bdate_range(start, periods=bars, tz="UTC")
    rng = np.random.default_rng(seed)
    close = 100 * np.exp(np.cumsum(rng.normal(drift, vol, bars)))
    open_ = np.concatenate([[100.0], close[:-1]]) * (1 + rng.normal(0, 0.002, bars))
    return pd.DataFrame(
        {
            "Open": open_,
            "High": np.maximum(open_, close) * 1.003,
            "Low": np.minimum(open_, close) * 0.997,
            "Close": close,
            "Volume": 1e6,
        },
        index=index,
    )


def _optimizer(frame, grid=MA_GRID, accrual=None, **kwargs):
    if accrual is None:
        accrual = np.zeros(len(frame))
    optimizer = ParameterOptimizer(
        frame, "ma_crossover", grid, cash_accrual=accrual, lot_size=1.0, seed=1, **kwargs
    )
    optimizer.run_optimization()
    return optimizer


# ---------------------------------------------------------------------------
# 4. The default call is unchanged
# ---------------------------------------------------------------------------

# Captured from ParameterOptimizer at e231ce0, BEFORE walk-forward was added:
# (short, long, Final Value, Sharpe Ratio, Trade Count), ranked best-first.
GOLDEN_AT_E231CE0 = [
    (20, 100, 122668.74038654706, 0.24660671900036357, 9.0),
    (30, 100, 120322.38720964287, 0.20220341042819284, 9.0),
    (10, 100, 117618.8981574896, 0.14498249298423543, 11.0),
    (10, 50, 111277.03348025505, 0.013648153680116893, 21.0),
    (20, 200, 106994.20585654142, -0.0871806055489492, 7.0),
    (30, 200, 106263.89415502071, -0.10441213830269579, 9.0),
    (30, 50, 105271.50541998676, -0.1140696882921665, 19.0),
    (10, 200, 101285.64468983932, -0.2295661986922767, 9.0),
    (20, 50, 92266.20770821656, -0.3963516300661307, 19.0),
]


def test_the_in_sample_search_is_bit_identical_to_before_walk_forward():
    frame = _frame(7, "2019-01-01", 252 * 4)
    accrual = np.full(len(frame), 0.0001)
    accrual[0] = 0.0
    optimizer = ParameterOptimizer(
        frame, "ma_crossover", MA_GRID, cash_accrual=accrual, lot_size=1.0, seed=1
    )
    optimizer.run_optimization()

    ranked = [
        (r["short_window"], r["long_window"], r["Final Value"], r["Sharpe Ratio"], r["Trade Count"])
        for r in optimizer.get_ranked_records()
    ]
    assert ranked == GOLDEN_AT_E231CE0
    assert optimizer.get_best_parameters() == {"short_window": 20, "long_window": 100}


# ---------------------------------------------------------------------------
# 3. Fold structure
# ---------------------------------------------------------------------------


def test_folds_are_contiguous_and_each_train_window_ends_before_its_validate_window():
    frame = _frame(1, "2010-01-04", 252 * 8)
    report = _optimizer(frame).run_walk_forward()

    folds = report["folds"]
    assert len(folds) >= 4
    for fold in folds:
        assert fold["train_start"] == frame.index[0].isoformat()
        assert pd.Timestamp(fold["train_end"]) < pd.Timestamp(fold["validate_start"])
    for previous, current in zip(folds, folds[1:], strict=False):
        # The next validate window starts on the bar after the previous one
        # ends: no gap and no overlap.
        after = frame.index.get_loc(pd.Timestamp(previous["validate_end"])) + 1
        assert pd.Timestamp(current["validate_start"]) == frame.index[after]
        assert current["train_end"] == previous["validate_end"]
    assert report["evaluated_from"] == folds[0]["validate_start"]
    assert report["evaluated_to"] == frame.index[-1].isoformat()


def test_three_whole_years_precede_the_first_validate_year():
    # Bars from 2010-01-04 (the first session of 2010) validate from 2013,
    # as decision 41 measured; a series that starts a week later does not
    # have three whole years before 2013.
    first = walk_forward_folds(pd.bdate_range("2010-01-04", "2016-12-30"), 3)[0]
    assert pd.bdate_range("2010-01-04", "2016-12-30")[first[0] + 1].year == 2013
    later = walk_forward_folds(pd.bdate_range("2010-01-11", "2016-12-30"), 3)[0]
    assert pd.bdate_range("2010-01-11", "2016-12-30")[later[0] + 1].year == 2014


def test_a_window_without_a_fold_returns_none_with_the_reason():
    frame = _frame(1, "2023-01-02", 400)
    optimizer = _optimizer(frame)
    assert optimizer.run_walk_forward() is None
    assert "3 whole calendar year" in optimizer.walk_forward_unavailable
    assert optimizer.walk_forward_portfolio is None


def test_min_train_years_below_one_is_refused():
    with pytest.raises(ValueError):
        walk_forward_folds(pd.bdate_range("2010-01-04", "2016-12-30"), 0)


def test_walk_forward_needs_the_search_first():
    frame = _frame(1, "2010-01-04", 252 * 5)
    optimizer = ParameterOptimizer(
        frame, "ma_crossover", MA_GRID, cash_accrual=np.zeros(len(frame)), lot_size=1.0
    )
    with pytest.raises(RuntimeError):
        optimizer.run_walk_forward()


# ---------------------------------------------------------------------------
# 2. No leakage
# ---------------------------------------------------------------------------


def test_perturbing_a_validate_year_never_changes_the_picks_made_before_it():
    frame = _frame(3, "2010-01-04", 252 * 8)
    picks = [f["params"] for f in _optimizer(frame).run_walk_forward()["folds"]]

    perturbed = frame.copy()
    target = perturbed.index.year == 2015
    shock = np.random.default_rng(99).normal(1.0, 0.05, target.sum())
    for column in ("Open", "High", "Low", "Close"):
        perturbed.loc[target, column] = perturbed.loc[target, column] * shock
    report = _optimizer(perturbed).run_walk_forward()
    years = [pd.Timestamp(f["validate_start"]).year for f in report["folds"]]
    after = [f["params"] for f in report["folds"]]

    # Folds up to and including 2015 trained only on data before 2015. Later
    # folds train on the perturbed year and may legitimately differ.
    upto = years.index(2015) + 1
    assert after[:upto] == picks[:upto]
    # And the perturbation is not vacuous: it reaches 2015's out-of-sample
    # figure, so the picks above held although the data they did not see moved.
    assert report["folds"][upto - 1]["oos_metrics"]["Final Value"] != pytest.approx(
        _optimizer(frame).run_walk_forward()["folds"][upto - 1]["oos_metrics"]["Final Value"]
    )


# ---------------------------------------------------------------------------
# 1. On paths with no edge, out-of-sample finds none
# ---------------------------------------------------------------------------


def _leaky_oos(optimizer, report):
    """The OOS Sharpe if each fold's pick were made ON its validate year."""
    index = optimizer.price_data.index
    values = []
    for boundary, last in walk_forward_folds(index, 3):
        scores = [
            window_metrics(t, p, index, optimizer.cash_accrual, boundary, last, 1e5)["Sharpe Ratio"]
            for t, p in optimizer._runs
        ]
        values.append(np.nanmax(scores))
    return float(np.nanmean(values))


def test_on_zero_edge_paths_the_out_of_sample_figure_finds_nothing():
    # Paths that compound to nothing (log-drift 0), so a rule that is in the
    # market part of the time has no expected excess either way. Costs are the
    # optimizer's (slippage and the $1 fee); the comparison is against the grid
    # median, which pays them too.
    oos, median, train, leaky = [], [], [], []
    for seed in range(30):
        frame = _frame(seed, "2010-01-04", 252 * 8, drift=0.0)
        optimizer = _optimizer(frame, transaction_cost=0.0)
        report = optimizer.run_walk_forward()
        base, last = report["span_base"], len(frame) - 1
        index = frame.index
        span = [
            window_metrics(t, p, index, optimizer.cash_accrual, base, last, 1e5)["Sharpe Ratio"]
            for t, p in optimizer._runs
        ]
        oos.append(report["oos_metrics"]["Sharpe Ratio"])
        median.append(np.nanmedian(span))
        train.append(np.nanmean([f["train_metric"] for f in report["folds"]]))
        leaky.append(_leaky_oos(optimizer, report))

    oos, median = np.array(oos), np.array(median)
    se = np.std(oos - median, ddof=1) / np.sqrt(len(oos))
    # In sample, the search always finds something...
    assert np.mean(train) > 0.15
    # ...out of sample, the pick does no better than a combination at random.
    assert abs(np.mean(oos - median)) < 3 * se + 1e-12
    assert abs(np.mean(oos)) < 3 * np.std(oos, ddof=1) / np.sqrt(len(oos))
    # The same measurement catches a selector that sees its validate year: if
    # picks leaked, the figure above would read like this, not like noise.
    assert np.mean(leaky) > np.mean(oos) + 0.5


# ---------------------------------------------------------------------------
# 5. A switch at a fold boundary trades at the next open and pays for it
# ---------------------------------------------------------------------------


def test_a_switch_into_a_held_position_buys_at_the_first_open_and_pays_costs():
    frame = _frame(5, "2020-01-02", 252 * 2)
    optimizer = ParameterOptimizer(
        frame, "buy_and_hold", [{}], cash_accrual=np.zeros(len(frame)), lot_size=1.0, seed=1
    )
    optimizer.run_optimization()
    report = optimizer.run_walk_forward(min_train_years=1)
    account = optimizer.walk_forward_portfolio

    first = frame.index.get_loc(pd.Timestamp(report["evaluated_from"]))
    assert frame.index[first].year == 2021
    # Flat through the train year, although buy and hold's own run held all of it.
    assert (account["position"].iloc[:first] == 0).all()
    open_ = frame["Open"].iloc[first]
    # All the cash, in whole shares: floor(cash / open), or one fewer when
    # slippage and costs make that fill dearer than the cash (the resize).
    quantity = account["position"].iloc[first]
    assert quantity in (np.floor(100_000 / open_), np.floor(100_000 / open_) - 1)
    assert account["cash"].iloc[first] >= 0

    # Paid: between the open plus costs, and the open at the worst slippage
    # (0.05%) plus costs. 10 bps of the fill's value and the $1 fee.
    paid = 100_000 - account["cash"].iloc[first]
    assert quantity * open_ * 1.001 + 1 <= paid
    assert paid <= quantity * open_ * 1.0005 * 1.001 + 1 + 1e-6
    assert report["folds"][0]["oos_metrics"]["Trade Count"] == 1
