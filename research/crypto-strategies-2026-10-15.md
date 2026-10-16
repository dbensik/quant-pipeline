# Strategies on crypto, 2018-2026 (decision 35, 2026-10-15)

Decisions 25, 28 and 31 measured the single-asset strategies on 428
equities and ETFs: none beats buy and hold at its defaults. No crypto was
measured, because until decision 33 crypto history began in 2020. This is
the first crypto measurement.

## Method

- **Coins.** The 14 with bars from 2018-01-01 or earlier (BTC, LTC, ETH, XRP,
  BCH, DOGE, ETC, ADA, BNB, KCS, LINK, TRX, XLM, XMR) over 2018-2026, split
  2018-2021 / 2022-2026; and all 26 non-stable, non-wrapped coins with bars
  from 2020 over 2020-2026. Stablecoins (USDT, USDC, DAI) and wrapped tokens
  (WBTC, WETH) left out; FIL starts 2020-10 (decision 34).
- **Engine.** The shipped `Backtester` in process: next-open fills, 10 bps +
  adverse slippage, idle cash earns ^IRX, lots of 1e-8, seed 1. Checked
  equal to POST `/api/v1/backtest` to the last digit (BTC-USD 2018-2026,
  buy and hold and rsi).
- **Read with three caveats.** (1) Every coin is a 2026 survivor; the many
  2017-2018 coins that went to zero are not here. (2) Crypto moves as one
  market: 14 coins are not 14 trials — 2018-2026 is two bear markets and
  two bull markets. (3) Sharpes below are at 365 bars a year (restated
  after decision 38; measured at sqrt(252), every value was 1.2035x higher).
  The counts — which beat buy and hold — do not depend on the scaling.
- Scripts: `load35.py`, `run35.py`, `analyse35.py`, `robust35.py`,
  `alloc35.py` (session scratchpad, not committed).

## Results — coins where the strategy's Sharpe beat buy and hold's

| strategy | 2018-26 (14) | 2018-21 | 2022-26 | 2020-26 (26) | median CAGR vs B&H, 2018-26 | median max DD (B&H -94.7%) |
|---|---|---|---|---|---|---|
| trend_following | **13** | 14 | 9 | 17 | +23.4 pp | -79.9% |
| atr_breakout | **12** | 11 | 11 | 16 | +24.5 pp | -62.0% |
| ma_crossover | 9 | 9 | 8 | 8 | +6.7 pp | -83.1% |
| rsi | 8 | 7 | 11 | 12 | -2.4 pp | -77.3% |
| ml_random_forest | 1 | 3 | 4 | 0 | -11.3 pp | -86.7% |
| mean_reversion | 0 | 0 | 8 | 1 | -22.1 pp | -92.4% |
| push_response | 0 | 2 | 1 | 0 | -24.7 pp | -95.7% |

Buy and hold on the median coin: Sharpe 0.04 (0.036), 6.4% a year, worst drawdown
-94.7% (2018-2026).

- **The opposite of equities.** Trend following and ATR breakout beat buy
  and hold on 13 and 12 of 14 coins; on 428 equities they did on 6% and 7%.
- **The edge is crash avoidance, year by year:**

  | year | B&H median | trend beat B&H | atr beat B&H |
  |---|---|---|---|
  | 2018 | -83.0% | 14/14 | 14/14 |
  | 2019 | -3.2% | 11/14 | 10/14 |
  | 2020 | +178.1% | 6/14 | 3/14 |
  | 2021 | +229.5% | 6/14 | 0/14 |
  | 2022 | -61.7% | 12/14 | 13/14 |
  | 2023 | +81.1% | 2/14 | 3/14 |
  | 2024 | +56.7% | 3/14 | 7/14 |
  | 2025 | -11.3% | 9/14 | 9/14 |
  | 2026 | -20.7% | 11/14 | 9/14 |

  They win every falling year and lose most rising ones. Crypto fell 60-83%
  twice in the window; stocks never fell that far for that long after 2010,
  which is why the same rules lost there. The result is a statement about
  this window's crashes as much as about the rules: in a window without a
  deep, slow crash, expect buy and hold to win.
- **Costs.** At 25 bps a fill, trend still beats buy and hold on 12/14 and
  ATR on 12/14; at 50 bps, 9/14 and 11/14. Trend trades about 23 times a
  year.
- **Fitting does not help** (fit 2018-2021, test 2022-2026): mean_reversion
  fitted beats its default on 7/14 coins, ma_crossover on 9/14.
- **BTC itself:** buy and hold Sharpe 0.31, worst drawdown -81.5%; trend
  1.07 and -57.1%; MA crossover 0.62 and -59.7%.

## Allocation rules on fixed crypto baskets, 2018-2026

Baskets fixed by a rule, not picked: BTC/ETH; and the five largest coins by
market capitalisation on 2018-01-01 (BTC, ETH, XRP, BCH, ADA).

| run | Sharpe | CAGR | max DD | 2018 | 2022 | 2021 |
|---|---|---|---|---|---|---|
| BTC/ETH equal weight | 0.28 | 23.0% | -87.4% | -75.3% | -66.1% | +204.8% |
| BTC/ETH trend | 0.58 | 32.0% | -59.4% | +2.0% | -23.5% | +200.0% |
| BTC/ETH momentum (top 1) | 0.49 | 38.1% | -77.3% | -27.3% | -68.4% | +213.4% |
| top-5 equal weight | 0.14 | 14.1% | -92.3% | -85.6% | -70.1% | +344.9% |
| top-5 trend | 0.23 | 14.6% | -60.7% | -2.8% | -20.3% | +184.0% |
| top-5 momentum (top 2) | 0.08 | 8.7% | -87.2% | -46.6% | -78.7% | +219.8% |
| top-5 momentum (top 1) | -0.40 | -32.6% | -99.1% | -21.3% | -78.2% | +3.4% |

- **Trend on a basket does what it did on ETFs, more strongly:** it was flat
  in 2018 and lost 20-24% in 2022 while the baskets lost 66-70%.
- **Single-coin momentum is ruinous on the top 5:** it buys whichever coin
  ran hardest. Checked trade by trade, not a fault: it bought XRP on
  2020-12-01 at 0.66, the SEC sued Ripple that month, and it sold at 0.22.

## Decisions

36. **Recommend: add the crypto finding to the caveats of trend_following
    and atr_breakout**, beside the equity one, e.g. trend: "On 14 crypto
    coins 2018-2026 it beat buy and hold on 13, by sitting out the 2018 and
    2022 crashes; it lost in most rising years." And to asset_class_trend:
    "on BTC/ETH 2018-2026 it cut the worst drawdown from -87% to -59%."
    About 20-30 min agent time, then restart 8001. Alternative: record it
    here only.

37. **Proposed: annualize by the asset's own calendar.** `PerformanceAnalyzer`
    annualizes volatility (and so Sharpe and Sortino) with sqrt(252) for
    every asset. Crypto has 365 bars a year, so its volatility is understated
    by about 17% and every crypto Sharpe the app shows is about 20% high; a
    basket mixing crypto and equities has bars on weekends for one and not
    the other. Measure the effect across crypto and mixed baskets and
    propose a rule (bars per year from the series itself, or per asset
    class). About 1 h agent time to measure; read-only.

    **36 approved and built 2026-10-15:** `measured_crypto()` beside
    `measured()` in `alpha_models/registry.py`; trend_following and
    atr_breakout carry the crypto line, asset_class_trend the BTC/ETH one.
    They quote counts, returns and drawdowns, not Sharpe (see 37).

## Decision 37 — measured 2026-10-15

Every place the code annualizes with 252: `PerformanceAnalyzer`
(volatility, the Sharpe's excess volatility, Sortino), the portfolio
weights optimizer (volatility and Sharpe of each weighting) and
`StatisticalAnalyzer` (alpha x 252). Options realised vol has its own
setting; the screeners compare daily figures. Measured in memory with the
rule "365 when the series has weekend bars, else 252":

- **Equities and ETFs: no change.** 0 weekend bars in 2,073,293 (crypto:
  50,424 of 176,495, 2 days in 7). Every equity figure stays bit-identical.
- **Crypto: every Sharpe and Sortino is sqrt(365/252) = 1.2035x too high,
  every volatility 17% too low** — exactly that factor on all 56 runs
  checked (14 coins x 4 strategies). Because it is one constant per series,
  no strategy-versus-buy-and-hold verdict changes (0 of 42 flipped): the
  findings of decision 35 stand. BTC buy and hold 2018-2026: 0.367 shown,
  0.305 correct; trend 1.291 -> 1.072.
- **A basket holding crypto runs on a calendar with weekends** (the union
  of its symbols' dates). SPY + BTC equal weight 2017-2026: 3,423 bars, 978
  on weekends; Sharpe shown 1.062, correct 0.883; volatility 30.7% -> 37.0%.
- **The weights optimizer's choice does not move** (one constant per basket),
  only the volatility and Sharpe it reports for a basket with crypto in it.
- **Alpha:** a coin regressed on SPY overlaps on weekdays only, so 252 is
  right there; a coin on another coin should be 365.

38. **Recommend: annualize by the series' own calendar** — one function,
    `periods_per_year(index)`: 365 when at least 10% of the bars fall on a
    weekend, else 252 (a share, not "any", so one stray weekend bar cannot
    flip an equity series). Used by the three sites above, on the series
    each actually computes from (an equity series, a crypto series, a
    basket's union calendar, a regression's overlapping dates). Tests: a
    crypto series annualizes at 365 and an equity one at 252, built to fail
    on the current code; the 15 real equity runs stay bit-identical. Every
    crypto figure the app shows moves by the constant; no caveat quotes a
    crypto Sharpe. About 45-60 min agent time, then restart 8001.
    Alternative: by asset class — breaks for a mixed basket, which has no
    single class.

    **Approved and built 2026-10-15.** `periods_per_year()` in
    `analysis/performance_analyzer.py` (thresholds in `config/settings.py`),
    used by `PerformanceAnalyzer`, `PortfolioOptimizer`, the OLS alpha and,
    found while building, `PortfolioBacktester`'s volatility targeting (which
    sized a crypto sleeve on sqrt(252) volatility). `tests/unit/
    test_annualization.py`: the 4 calendar-day tests fail on the old code,
    the 7 others guard 252. Real runs: 12 equity backtests identical in every
    metric; 6 crypto runs' Sharpe exactly 1.2035x lower and volatility 1.2035x
    higher, final values unchanged. The decision 35 Sharpes above were
    restated at 365 the same day (exact: unrounded values / 1.2035, and the
    basket table rerun at 365).

