# Strategy evaluation, 2026-10-14

First evaluation since backtests hold positions (decision 22, `14f5d71`).
Every single-asset strategy in the registry, on every served equity and ETF
with three or more years of bars since 2020 and current data: **515 names**
(503 equities, 12 ETFs; 501 start on 2020-01-02). Through the live API, so
with everything a user gets: 10 bps per fill plus adverse slippage, idle cash
earning `^IRX`, Sharpe against that cash account, whole-share lots, seed 1.
Scripts: `scratchpad/eval_run.py`, `eval_analyse.py` (not committed; raw
results `eval_results.json`). 0 of 4,120 part-A runs failed.

**Read every figure with this caveat.** 515 names over one 2020-2026 path
are not 515 independent trials: they share one market regime (a crash, a
rate shock, a long rally). Agreement across names overstates confidence.
Every name is also a current index member, a survivor; prices before 2020
are not stored (`DEFAULT_START_DATE`).

## A. Defaults, 2020-01 to 2026-10, against buy and hold on the same name

| strategy | median Sharpe | buy and hold | beats B&H Sharpe | median CAGR vs B&H | median max drawdown (B&H -50.8%) | shallower drawdown | median time in market |
|---|---|---|---|---|---|---|---|
| rsi | 0.17 | 0.19 | 48% | -1.9 pp | -42.8% | 97% | 39% |
| ma_crossover | 0.05 | 0.19 | 31% | -5.4 pp | -41.3% | 78% | 58% |
| mean_reversion | 0.03 | 0.19 | 28% | -5.4 pp | -40.1% | 99% | 30% |
| trend_following | 0.02 | 0.19 | 23% | -6.4 pp | -42.1% | 77% | 56% |
| atr_breakout | -0.05 | 0.19 | 20% | -7.5 pp | -30.4% | 95% | 29% |
| ml_random_forest | -0.37 | 0.19 | 3% | -13.4 pp | -48.2% | 60% | 40% |
| push_response | -0.39 | 0.19 | 3% | -14.2 pp | -54.4% | 43% | 51% |

- **None beats buy and hold at its defaults.** rsi comes closest (better
  Sharpe on 48% of names, 1.9 pp a year less return). The others give up
  5-8 pp a year.
- **What they buy is a shallower drawdown, by being out of the market.**
  mean_reversion's is shallower than buy and hold's on 99% of names, at 30%
  time in the market; it is cash, not skill, and cash earned ^IRX here.
- **push_response and the random forest lose to buy and hold on 97% of
  names**, with drawdowns as deep or deeper while invested half the time.
  Both trade their own day-to-day predictions (SPY: 649 and 426 fills).
- **ETFs (12):** ma_crossover beats buy and hold on 9 of 12, median Sharpe
  0.24 against 0.27; every other strategy on 3 or fewer.

## B. Fitted on 2020-2022, tested on 2023-01 to 2026-10

The two strategies with a default grid, fitted by Sharpe per name on the
first three years, then run on the rest with the fitted parameters:

| strategy | in-sample fitted Sharpe | out-of-sample fitted | out-of-sample default | out-of-sample B&H | fitted beats default | fitted beats B&H |
|---|---|---|---|---|---|---|
| mean_reversion | 0.72 | 0.19 | 0.08 | 0.19 | 57% | 47% |
| ma_crossover | 0.48 | -0.01 | -0.05 | 0.19 | 53% | 19% |

- **Fits decay by three quarters or more out of sample** (0.72 -> 0.19,
  0.48 -> -0.01).
- **Fitting helps mean_reversion a little** (0.19 against 0.08 at the
  default, better on 57% of names), which brings it level with buy and hold
  out of sample, not ahead. The most-chosen fit is the widest band
  (threshold 2.5, window 50: 113 names), i.e. trade rarely.
- **For ma_crossover fitting is a coin flip** (53%) and loses to buy and
  hold on 81% of names in 2023-2026.

## Decision 25 (approved and built 2026-10-14)

25. **What to do with the result.** Recommend **keep every strategy, and
    say what was measured where a user picks one**: a one-line evidence
    note in each single-asset strategy's registry caveat ("2020-2026, 515
    names, defaults: better Sharpe than buy and hold on N%, median drawdown
    X% against -50.8%"), shown by the strategy selector as caveats already
    are. push_response and ml_random_forest would say "loses to buy and hold
    on 97% of names". No strategy is removed: they are templates, and a
    template's value is that it runs correctly, which since decision 22 they
    do. About 30 min agent time. Alternative: record it here only and change
    nothing a user sees.

    **Built:** `measured()` in `alpha_models/registry.py` puts each single-asset
    strategy's finding in its caveat (buy and hold has none), which the strategy
    selector, backtest, compare and simulate responses already show.
