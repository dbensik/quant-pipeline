# Walk-forward against in-sample grid search (decision 41, 2026-10-19)

`ParameterOptimizer` (`backtesting/parameter_optimizer.py`) picks the best
parameters on a window and reports their metrics on that same window. The
Optimize page shows that figure, and so does `/api/v1/compare` with
`optimize` on: it tunes each strategy on the window it then ranks them on.
Neither reports what the chosen parameters do on data they were not picked
on. This study measures the difference between the two.

## Method

- **Anchored walk-forward, annual folds.** For validate year Y, train on
  [first bar, 31 Dec Y-1], pick the combination with the best train Sharpe,
  and trade it through Y. The out-of-sample (OOS) figure is **one continuous
  account**: in year Y its signal is the chosen combination's, and on the
  last bar of Y-1 an entry or exit is injected so the account holds what the
  new choice holds (it fills at Y's first open and pays costs). Switching
  between choices therefore costs what it would really cost. That cost turned
  out negligible: slicing each choice's own full run instead changes fold
  Sharpe by a median 0.00-0.01.
- **Train Sharpes come from one full run per combination.** Every strategy
  passes the look-ahead contract, so the train-window figure of a full run
  equals a run cut off at the train end. One run per combination, not one
  per fold.
- **Settings match the API.** 10 bps + adverse slippage, idle cash earns
  `^IRX`, Sharpe against cash, lots, seed 1, next-open fills. The harness
  matches `/api/v1/backtest` exactly: SPY ma_crossover 10/100 2016-2025 ended
  at $327,792.65, Sharpe 0.8685, 27 trades, both ways.
- **Names:** decision 28's 428 (served, bars from 2010-01-04, minus DHR,
  EXPE, IRM, TMUS). Data runs 2010-01-04 to 2026-10-16. Validate years run
  2013 to 2026, 14 folds, so the first train window is three years. 2026 is
  a partial fold.
- **Grids:** the registry `default_grid` for ma_crossover (9) and
  mean_reversion (20), which are what Optimize and Compare use. I chose the
  other four for this study: a wide MA grid (short 5-50 step 5 x long 50-300
  step 25, 109 valid; it tests whether a bigger search overfits more), rsi
  (window 7/14/21/28 x buy 20-35 x sell 65-80, 64), atr_breakout (window
  10-50 x multiplier 1-3, 25) and trend_following (window 20-250 step 10, 24).
- **Compared on the same 2013-2026 OOS span:** the walk-forward account,
  the registry defaults, buy and hold, and the **hindsight** figure: the
  best combination's Sharpe on 2013-2026 itself, which is what the
  optimizer reports today. Every run starts at the data start (2010, or
  2016 for the SPY headline), so a hindsight figure comes from a run
  already warmed up when the span begins, not one that starts cold.
- **Zero-edge control:** 200 simulated price paths (random walks with no
  edge built in) on SPY's calendar, about 19% volatility a year, run through
  the same pipeline twice. *Free* has no drift, no costs and no interest on
  cash. *Live* drifts at the cash rate and uses the live costs and ^IRX
  accrual, so the strategies have no edge before costs.
- Scripts (session scratchpad, nothing in the repo edited): `dump41.py`,
  `wf41.py`, `noise41.py`, `noise41b.py` (the log-drift-0 rerun), `analyse41.py`. 0 errors on 2,568 name-grid
  studies; 1 min 38 s on 13 processes.

## SPY: the default grid, 2016-2025 (validate 2019-2025)

| ma_crossover, default grid | Sharpe | CAGR |
|---|---|---|
| hindsight best on 2019-2025 (10/100), what the optimizer would report for that span | 0.81 | |
| mean train Sharpe of each year's pick | 1.01 | |
| **walk-forward, out of sample** | **0.32** | 7.5% |
| defaults (40/100) | 0.44 | 9.5% |
| buy and hold | 0.71 | 17.2% |

| fold | pick | train Sharpe | OOS Sharpe | default | B&H |
|---|---|---|---|---|---|
| 2019 | 20/50 | 1.20 | 0.92 | 1.34 | 2.28 |
| 2020 | 20/200 | 1.19 | -0.39 | -0.38 | 0.53 |
| 2021 | 20/50 | 1.08 | 1.73 | 2.21 | 2.21 |
| 2022 | 10/100 | 1.22 | -2.16 | -1.43 | -0.82 |
| 2023 | 10/100 | 0.76 | 0.87 | 1.09 | 1.52 |
| 2024 | 10/100 | 0.77 | 1.47 | 1.47 | 1.47 |
| 2025 | 10/100 | 0.85 | 1.02 | 0.55 | 0.66 |

The 10/100 that the grid search crowns was picked only from 2022 on, and
2022 was its worst year. On SPY the same pattern holds for every grid:

| SPY 2016-2025 | hindsight | walk-forward | defaults | B&H |
|---|---|---|---|---|
| ma_crossover (9) | 0.81 | 0.32 | 0.44 | 0.71 |
| mean_reversion (20) | 0.28 | 0.00 | 0.08 | 0.71 |
| ma_wide (109) | 0.87 | 0.52 | 0.44 | 0.71 |
| rsi (64) | 1.08 | 0.27 | 0.42 | 0.71 |
| atr_breakout (25) | 0.85 | 0.67 | 0.26 | 0.71 |
| trend_following (24) | 0.81 | 0.53 | 0.54 | 0.71 |

In hindsight, rsi and atr_breakout "beat" buy and hold on SPY. Out of
sample, nothing does.

## 428 names, validate 2013-2026 (medians across names)

| grid (combos) | hindsight | grid median | train (IS) | **walk-forward OOS** | defaults | B&H | IS - OOS per fold |
|---|---|---|---|---|---|---|---|
| ma_crossover (9) | 0.35 | 0.19 | 0.52 | **0.19** | 0.17 | 0.39 | 0.31 |
| mean_reversion (20) | 0.40 | 0.17 | 0.62 | **0.13** | 0.16 | 0.39 | 0.24 |
| ma_wide (109) | 0.44 | 0.23 | 0.65 | **0.18** | 0.17 | 0.39 | 0.41 |
| rsi (64) | 0.65 | 0.27 | 0.89 | **0.30** | 0.27 | 0.39 | 0.40 |
| atr_breakout (25) | 0.31 | 0.02 | 0.45 | **0.02** | 0.04 | 0.39 | 0.47 |
| trend_following (24) | 0.34 | 0.17 | 0.51 | **0.18** | 0.07 | 0.39 | 0.32 |

| grid | better Sharpe than B&H: hindsight | walk-forward | defaults | WF better than defaults | WF CAGR vs B&H | distinct picks in 14 folds | hindsight winner ever picked |
|---|---|---|---|---|---|---|---|
| ma_crossover | 39% | 14% | 13% | 50% | -7.1 pp | 3 | 79% |
| mean_reversion | 51% | 10% | 12% | 45% | -8.4 pp | 4 | 62% |
| ma_wide | 66% | 15% | 13% | 49% | -6.7 pp | 5 | 48% |
| rsi | **98%** | 30% | 29% | 49% | -4.6 pp | 4 | 61% |
| atr_breakout | 37% | 7% | 8% | 48% | -10.6 pp | 4 | 70% |
| trend_following | 37% | 12% | 7% | 62% | -7.6 pp | 4 | 54% |

- **In-sample optimization makes almost every strategy look like it beats
  buy and hold, and out of sample almost none do.** A tuned rsi "beats" it on
  98% of names in hindsight. Walk-forward, it beats it on 30%, which is
  what its untuned defaults already did (29%). This hindsight figure is
  what the Optimize page and Compare with `optimize` on report today.
- **Tuning adds nothing out of sample.** The walk-forward pick scores about
  what the grid's median combination does (0.02-0.30 against 0.02-0.27),
  not what its best does. It beats the defaults on about
  half the names, and the median difference is 0.00 to -0.02. The one
  exception is trend_following (+0.07; it wins on 62% of names), because its
  default window of 50 sits in the weak part of its range. That is a fact
  about the default, not evidence that tuning works.
- **In-sample overstates out-of-sample Sharpe by 0.24-0.47 a fold**, and the
  bigger the search, the bigger the gap. ma_crossover's 9 combinations gap
  0.31; the same strategy's 109 gap 0.41, and its walk-forward result is no
  better (0.18 against 0.19).
- **Picks do not persist.** A name has a median of 3-5 distinct picks
  across its 14 folds, and the full-window winner is never picked at all on
  21-52% of names.
- **ETFs** (AGG, EFA, GLD, IEF, IWM, QQQ, SPY, TLT, VNQ): no walk-forward
  rule's median Sharpe reaches buy and hold's (0.27). ma_crossover's walk-forward beats it on 4 of
  9 ETFs, its defaults on 5.

## Zero-edge control

| grid | hindsight median / 95th pct (free) | train Sharpe (free) | walk-forward OOS mean, free (s.e.) | defaults, free | walk-forward OOS mean, live | IS - OOS per fold, free |
|---|---|---|---|---|---|---|
| ma_crossover (9) | 0.08 / 0.53 | 0.16 | -0.040 (0.018) | -0.060 | -0.082 | 0.28 |
| mean_reversion (20) | 0.30 / 0.70 | 0.44 | -0.027 (0.020) | -0.026 | -0.096 | 0.30 |
| ma_wide (109) | 0.19 / 0.57 | 0.28 | -0.065 (0.019) | -0.060 | -0.094 | 0.41 |
| rsi (64) | 0.41 / 0.76 | 0.53 | -0.017 (0.019) | -0.043 | -0.058 | 0.42 |
| atr_breakout (25) | 0.23 / 0.61 | 0.32 | -0.043 (0.019) | -0.048 | -0.150 | 0.44 |
| trend_following (24) | 0.12 / 0.53 | 0.19 | -0.059 (0.017) | -0.070 | -0.184 | 0.32 |

- **On paths with no edge, the grid search still finds one in-sample**
  (median hindsight Sharpe 0.08-0.41, up to 0.76 at the 95th percentile),
  and walk-forward removes it. The walk-forward pick does no better than the
  defaults on the same paths. The in-sample-minus-OOS gap is 0.28-0.44, the
  same size as on real names (0.24-0.47).
- The free OOS means are slightly negative (-0.02 to -0.07), and the
  defaults' are too. That is not selection bias. A no-drift path still loses
  about σ²/2 a year in compound growth, so every long-or-cash rule scores a
  little below 0 on the Sharpe the backtester uses (compound growth over
  volatility). **Rerun at log-drift 0** (paths that compound to nothing; same
  200 seeds, cost-free): the walk-forward OOS mean is -0.013 to +0.032 and
  within 2 s.e. of 0 for every grid, and within 2 s.e. of the grid median
  (WF minus median -0.025 to +0.022, s.e. 0.008-0.015). The train Sharpe of
  the picks is still 0.22-0.58. Acceptance test 1 is written against this
  version.
- **Most real winners do not stand out from noise.** The best combination's
  lead over the grid median ("selection premium") is no bigger on real names
  than on zero-edge paths. It exceeds the zero-edge 95th percentile (live
  costs) on 0-7% of names. This is the deflated-Sharpe question answered
  empirically: by this test, almost none of the hindsight winners are
  distinguishable from what a search over noise finds.

## What this changes

- No conclusion of decisions 25, 28 or 31 moves. Those used defaults, and
  defaults are as good as tuned parameters out of sample.
- The registry caveats for mean_reversion and ma_crossover already say
  "parameters fitted on 2010-2019 lost to buy and hold on 62% / 72% of names
  in 2020-2026". That was one split. This is 14, and it agrees.
- What is wrong today is the **number the Optimize page shows, and Compare's
  ranking with `optimize` on.** Both are hindsight. In the two default grids
  they overstate the gain over buy and hold by a factor of 3-5: 39% and 51%
  of names "beat" it, against 14% and 10% out of sample.

## Decision

41. **Recommend B.**

    - **A: walk-forward in `ParameterOptimizer`, off by default.** A
      `walk_forward` option: anchored, annual folds by default, a minimum
      train window, and the continuous out-of-sample account with
      injections at fold boundaries described above. The result carries
      per-fold picks, train and OOS Sharpe, the stitched OOS Sharpe and
      CAGR, and the IS-minus-OOS gap. The existing call is unchanged and
      bit-identical. API field on `/optimize/strategy` and its websocket.
      About 2.5-3.5 h of my time.
    - **B (recommended): A, plus show it by default.** The Optimize page
      shows the walk-forward OOS Sharpe and the gap beside the in-sample
      ranking, labelled as such, and runs walk-forward whenever the window
      allows at least one fold. Compare with `optimize` on reports each
      strategy's walk-forward OOS metrics, not the hindsight-tuned ones.
      About 4.5-6 h of my time, assuming the Optimize page's table takes
      two new columns and a fold table without a redesign.
    - **C: caveat only.** Text on the Optimize page and Compare saying the
      figures are in-sample, with this study's numbers. About 30 min.

    **Acceptance tests (new `tests/unit/test_walk_forward.py`):**
    1. On seeded zero-log-drift simulated paths, cost-free, the walk-forward
       OOS Sharpe averaged over the seeds is within 3 standard errors of 0
       and of the grid median (measured above: within 2), while the mean
       train Sharpe of the picks is above 0.15. The test must FAIL if
       selection is allowed to see the validate window.
    2. Perturbing validate-window prices does not change any pick made
       from earlier windows.
    3. Validate windows are contiguous and non-overlapping, and each train
       window ends strictly before its validate window starts.
    4. With `walk_forward` off, results are bit-identical to `HEAD` on the
       existing optimizer tests and on 12 real runs. The `/optimize` and
       `/compare` API tests pass unchanged.
    5. On a series where the switch at a fold boundary changes the held
       position, the OOS account trades at the next open and pays the
       cost. A hand-computed example.

    **Approved 2026-10-19 (B) and built.** `ParameterOptimizer.run_walk_forward`,
    `walk_forward` and `min_train_years` on `/optimize/strategy` and its websocket,
    Compare's tuning, and both pages. The in-sample search is bit-identical on the
    12 real runs (SPY, AAPL, JPM, QQQ x ma_crossover, mean_reversion,
    trend_following) and on a pinned fixture. Through the API, SPY 2016-2025
    default grid reproduces this study exactly (OOS 0.318, hindsight 10/100 at
    0.814, the same seven picks), and Compare on SPY 2010-2026 gives
    ma_crossover 0.54, mean_reversion 0.18 and buy and hold 0.76, as above.
    Acceptance test 1 runs with the optimizer's own fill costs (slippage and
    the $1 fee, transaction_cost 0) and compares the pick against the grid
    median on 30 paths of 8 years. Each test was checked against a mutant on a
    copy of the repo: a validate-window leak, a missing boundary switch, an
    off-by-one fold rule and a carried position counted as a fill each fail.
    One deviation from test 4: Compare's tuned tests now request
    `min_train_years: 1` and a 2025 end, because a 2024-only window is
    refused with tuning on.

    **Not proposed:** a closed-form deflated Sharpe or White's reality
    check. The zero-edge control above answers the same question for these
    grids. Add either one only if a strategy ever survives walk-forward.
