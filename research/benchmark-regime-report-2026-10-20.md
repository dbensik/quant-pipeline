# A benchmark and regime report for every backtest (decision 53, 2026-10-20)

Vault task: "Add benchmark and regime report to backtest results" (brief
task 5). A backtest today reports return, Sharpe and drawdown, and the
Allocation page puts the equal-weighted basket beside it. Nothing answers two
questions:

- **Exposure:** is the result more than exposure to the market? A strategy
  can only be said to have an edge if what it earns beyond its market
  exposure is distinguishable from zero.
- **Regimes:** does it behave differently in calm and in stressed markets?

## What exists

- **`StatisticalAnalyzer.run_ols_regression`:** the Statistics page's
  "OLS Regression (Alpha / Beta)". It regresses raw returns, not returns in
  excess of cash, and gives no standard error for alpha.
- **`cash_account_sharpe`:** the Sharpe every backtest uses. There is no
  rolling version.
- **Benchmarks:** `/backtest/compare` takes an optional `benchmark_symbol`
  (buy and hold). `/backtest` takes none. `/backtest/portfolio` compares only
  with its own basket.
- **Volatility index:** none. ^VIX is not stored, so a regime has to come
  from prices.

## Measured: eight runs against SPY, 2011-2026

These are decision 29's runs (2010-01-04 to 2026-10-14, at defaults, seed 1,
10 bps, interest on cash) plus three single-asset rules on SPY.
- **Returns:** daily, in excess of the ^IRX cash accrual, regressed on SPY's
  excess return.
- **t-statistics:** Newey-West (5 lags). Plain OLS t-statistics differ by
  at most 0.04.
- **Window:** statistics on 2011-01 to 2026-10.

| run | beta | alpha a year | t | R² |
|---|---|---|---|---|
| faber5 equal weight | 0.59 | -2.0% | -1.49 | 0.79 |
| faber5 trend | 0.30 | -0.1% | -0.04 | 0.41 |
| faber5 momentum | 0.55 | +1.3% | 0.58 | 0.53 |
| SPY/TLT switching | 0.53 | -1.3% | -0.38 | 0.30 |
| 60/40 SPY/AGG | 0.60 | +0.1% | 0.22 | 0.97 |
| SPY rsi | 0.62 | +0.3% | 0.13 | 0.61 |
| SPY trend_following | 0.39 | -1.8% | -0.82 | 0.39 |
| SPY ma_crossover | 0.58 | -0.0% | -0.02 | 0.57 |

- **No run has an alpha distinguishable from zero** (every |t| < 1.5).
- **Trend on Faber's five behaves like 30% SPY and 70% cash**: beta 0.30,
  alpha -0.1% a year (t -0.04). Its shallower drawdown is what that
  exposure would give. It is not evidence of skill.
- **Momentum's +1.3% a year is the largest alpha**, and is well inside the
  noise (t 0.58).
- A report showing these numbers would have said in one line what decisions
  25, 28 and 29 took a study each to find.

### Regimes

A day's regime comes from SPY's volatility over the previous 21 sessions,
annualized, as known at the close BEFORE that day: no look-ahead. Two ways to
cut it were tried.

**Fixed cut-offs** (under 15%, 15-25%, over 25%) give 68% calm days, 23%
normal and 9% stressed. 9% is about 1.4 years in 16.

| Sharpe | calm | normal | stressed |
|---|---|---|---|
| SPY | 0.91 | 0.88 | 0.65 |
| faber5 equal weight | 0.41 | 1.05 | 0.24 |
| faber5 trend | 0.44 | 0.74 | 0.19 |
| faber5 momentum | 0.56 | 1.01 | 0.50 |
| SPY/TLT switching | 0.52 | 0.52 | -0.31 |
| SPY ma_crossover | 0.72 | 1.01 | -0.64 |

**Thirds of SPY's own history before each day:** a day is "high" if the
trailing volatility exceeds two thirds of all earlier days'. Each third is
about 5 years.

| Sharpe | low third | middle | high third |
|---|---|---|---|
| SPY | 0.17 | 1.71 | 0.64 |
| faber5 equal weight | -0.48 | 1.41 | 0.55 |
| faber5 trend | -0.51 | 1.34 | 0.56 |
| faber5 momentum | -0.49 | 1.59 | 0.81 |
| SPY/TLT switching | 0.49 | 0.89 | -0.09 |
| SPY ma_crossover | 0.05 | 1.52 | 0.24 |

**A Sharpe on a slice of days is noisy.** Its standard error is about
1 / √years.
- **Fixed cut-offs:** about 0.85 on the 1.4 stressed years.
- **Thirds:** about 0.45 on each.

So no difference in either table is evidence. For example, trend's high-third
Sharpe of 0.56 against the basket's 0.55 says nothing either way. The split
is only useful if the error is printed beside every figure. Without it, a
reader would take the stressed column at face value.

**Thirds are better than fixed cut-offs:**
- every bucket is about the same size, so no bucket is 1.4 years;
- the same rule works for crypto, whose ordinary volatility (50-80%) would
  sit in "stressed" under SPY's cut-offs every day;
- each day is ranked only against days before it, so a 2011 day is never
  judged by 2020.

## The design

- **`analysis/benchmark_report.py`** (pure): from a strategy's equity
  curve, the benchmark's closes and the cash accrual it gives:
  - **Exposure:** beta; annualized alpha with its Newey-West t and 95%
    interval; R²; days used.
  - **Rolling Sharpe:** `cash_account_sharpe` over a trailing 252-bar
    window, for the strategy and the benchmark, as two curves.
  - **Regimes:** low, middle and high thirds. For each, the share of days
    and the strategy's and benchmark's annualized excess return and Sharpe,
    each Sharpe with its standard error. Days with under 252 earlier days
    of benchmark history are "unranked", counted but not split.
  - **Settings:** `config/settings.py` holds `BENCHMARK_REGIME_WINDOW` (21),
    `BENCHMARK_REGIME_MIN_HISTORY` (252), `BENCHMARK_ROLLING_SHARPE_WINDOW`
    (252) and `BENCHMARK_NEWEY_WEST_LAGS` (5).
- **The benchmark:**
  - By default, **SPY for equities and ETFs and BTC-USD for crypto**
    (`BENCHMARK_BY_ASSET_CLASS`). A basket mixing the two takes SPY.
  - `benchmark_symbol` in the request overrides it.
  - Its bars are read from the store like any backtest's. Its ranking
    history starts 252 bars before the run, so the first year is ranked.
  - **Calendars:** a crypto strategy against SPY has its weekend returns
    compounded into the next SPY bar, so both series share one calendar.
- **Surfaces:**
  - `/backtest`, `/backtest/compare` (each row) and `/backtest/portfolio`
    each return `benchmark_report`.
  - The Dashboard, Compare and Allocation pages show one section:
    - the exposure line ("beta 0.30, alpha -0.1% a year, t -0.04: not
      distinguishable from zero");
    - the rolling Sharpe chart;
    - the regime table, with ± errors.
  - Compare's existing `benchmark_symbol` becomes the report's benchmark,
    so one request has one benchmark.
- **Not in scope:** the paper tracking card (it compares with its own
  backtest), the optimizer grid (one report per combination would cost
  what the search costs), and the Statistics page's OLS test (left as it
  is).

### Tests (each checked failing against a mutant)

- SPY against itself gives beta 1, alpha 0 and R² 1.
- A curve built as 0.3 × SPY's excess return plus cash gives beta 0.3,
  alpha 0.
- Regime thirds partition the ranked days exactly once. A day's regime is
  unchanged by changing any later price (no look-ahead).
- Rolling Sharpe at one date equals `cash_account_sharpe` on that window,
  computed by hand.
- The API returns the report for each of the three endpoints. Crypto
  against SPY has the weekend compounding.
- The eight runs above, through the API, reproduce this doc's table.

## Decision

53. **Recommend A.**

    - **A: the whole report.** Exposure with its t-statistic, rolling
      Sharpe, and regime thirds with errors, on the three endpoints and three
      pages. About 6-8 h of my time. The estimate assumes the
      existing chart component takes the rolling Sharpe curves as it stands
      (it took the tracking curves without change).
    - **B: exposure and rolling Sharpe, no regimes.** About 4-5 h. The
      regime split, at about ±0.45 per bucket, cannot tell any rule tested
      here from its basket. B leaves out the one view most open to being
      over-read. Against that, it is also what the brief asks for, and with
      the errors printed it shows that the split proves nothing.
    - **C: nothing.** The measurement above goes into the strategy caveats
      as a sentence each: "beta 0.30 to SPY, alpha indistinguishable from
      zero". About 1 h.

**Approved: A, 2026-10-20.**
