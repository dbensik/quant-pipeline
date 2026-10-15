# Fills at the signal bar's close, or the next bar (decision 30, 2026-10-15)

Both backtesters fill an order at the close of the bar whose close produced
the signal (`backtesting/backtester.py`, `backtesting/portfolio_backtester.py`).
A real order placed on a close fills at the next session's open at the
earliest. Decision 29 found that a one-bar delay cut the allocation rules'
Sharpe by 0.03-0.09; this measures every default strategy three ways.

## Method

- **Three conventions, in patched copies of the two backtesters** (session
  scratchpad, nothing in the repo edited): `close` — as shipped; `next_close`
  — act on the previous bar's signal at this bar's close; `next_open` — act
  on it at this bar's open. Positions are valued at the close in all three;
  sizing uses the fill price.
- **`close` reproduces the shipped code.** Single-asset: 72 of 72 runs equal
  the API to the last digit on SPY/AAPL/JPM; over the full run, every
  `close` result equals decision 28's API run except MU and PNC (48 runs),
  whose 2026-10-14 dividends were recorded by the 2026-10-15 06:00 job and
  rescale their total-return history — expected. Allocation: every `close`
  run equals decision 29's final value exactly.
- **Single-asset:** the 428 names of decision 28, the eight registry
  strategies at their defaults, windows 2010-2026, 2010-2019, 2020-2026 run
  separately (as decision 28 did), seed 1, costs and interest on cash —
  30,816 runs, 0 errors. Every served bar has an open (0 NULL).
- **Allocation:** decision 29's default runs and benchmarks.

## Single-asset strategies — share of names with a better Sharpe than buy and hold

| strategy | 2010-26: close / next close / next open | 2010-19 | 2020-26 | median Sharpe change (close -> next open) | names worse | trades a name |
|---|---|---|---|---|---|---|
| rsi | 28% / 27% / 26% | 25 / 24 / 23 | 50 / 48 / 49 | -0.008 | 68% | 31 |
| mean_reversion | 14% / 13% / 11% | 19 / 20 / 18 | 28 / 28 / 25 | -0.006 | 54% | 198 |
| ma_crossover | 13% / 12% / 12% | 16 / 15 / 14 | 29 / 28 / 25 | 0.000 | 50% | 46 |
| trend_following | 7% / 5% / 6% | 7 / 5 / 7 | 21 / 22 / 20 | +0.007 | 44% | 298 |
| atr_breakout | 6% / 7% / 7% | 8 / 6 / 8 | 19 / 21 / 20 | +0.015 | 39% | 148 |
| push_response | 0% / 0% / 0% | 0 / 0 / 0 | 3 / 3 / 4 | -0.003 | 52% | 1,642 |
| ml_random_forest | 0% / 0% / 0% | 0 / 0 / 0 | 3 / 4 / 4 | 0.000 | 50% | 1,394 |

Buy and hold's median Sharpe is unchanged (0.40 / 0.40 / 0.40 over
2010-2026).

- **For the single-asset strategies the convention barely matters.** Shares
  move 0-4 points, median Sharpe at most 0.03, median return gap at most
  0.9 pp, in any window. No
  conclusion of decisions 25 or 28 changes: none beats buy and hold, rsi
  comes closest (26% at next open).
- **The direction is not uniform.** rsi and mean_reversion lose a little
  (worse on 68% / 54% of names); trend_following and atr_breakout gain a
  little (why was not measured). Even push_response and the random forest, 1,400-1,600 trades a
  name, barely move: their edge was already nil.

## Allocation rules (decision 29), Sharpe 2011-2026

| default run | close | next close | next open |
|---|---|---|---|
| faber5 equal weight | 0.45 | 0.46 | 0.46 |
| faber5 trend | 0.49 | 0.40 | 0.45 |
| faber5 momentum | 0.64 | 0.61 | 0.61 |
| faber7 equal weight | 0.52 | 0.53 | 0.52 |
| faber7 trend | 0.54 | 0.47 | 0.51 |
| faber7 momentum | 0.51 | 0.45 | 0.49 |
| SPY/TLT switching | 0.34 | 0.26 | 0.27 |
| SPY/IEF switching | 0.35 | 0.30 | 0.29 |
| SPY/AGG switching | 0.35 | 0.33 | 0.31 |
| 60/40 SPY/AGG | 0.73 | 0.74 | 0.74 |
| 50/50 SPY/TLT | 0.68 | 0.69 | 0.69 |

- **The rules that switch whole sleeves at month-end lose the most** (trend
  -0.03 to -0.04, switching -0.04 to -0.07 at next open), — each switch
  moves the whole sleeve on one day, so one day's price change on it
  weighs heavily (a likely reason, not measured). Static rebalancing does not move.
- **At next open, trend is level with its basket, not ahead** (0.45 against
  0.46; 0.51 against 0.52). Its drawdown protection is what remains.

## Decision

31. **Recommend: fill at the next bar's open in both backtesters**, the
    convention a real order meets. The evidence says it changes no
    conclusion for the single-asset strategies and trims the allocation
    rules by 0.02-0.07; what it buys is that no figure quoted to a user
    rests on trading at a price that was only known once the market had
    closed. Build: a signal on bar t fills at bar t+1's open (a signal on
    the last bar does not fill), sizing at that open, valuation at the
    close; the same in `PortfolioBacktester`; tests built so they fail on
    same-close fills; every surface that runs a backtest (backtest, compare,
    optimize, simulate, portfolio, their websockets) inherits it; then
    restate decisions 28 and 29's caveats from these next-open figures
    (already measured — no re-run needed). It is a deliberate change of
    meaning, measured here before and after, not a refactor. About 2-3 h
    agent time, then restart 8001.

    Alternative: keep same-close fills and say so in each caveat
    ("fills at the signal bar's close; at the next open the share is N%").

    **Approved 2026-10-15 and built.** `Backtester` and
    `PortfolioBacktester` act at bar t+1's open on bar t's signals; the
    basket sizes on equity valued at that open, and volatility targeting
    reads closes before the bar. `tests/unit/test_next_open_fills.py`: 7
    tests, 6 failing on the same-close code (the seventh, marking at the
    close, holds under both). The shipped `Backtester` equals the measured
    `next_open` variant on all 8,988 runs compared (seven strategies, the
    random forest left out for time). The shipped `PortfolioBacktester`
    differs from the variant only by sizing on open equity: Sharpes within
    0.01. Every caveat restated from next-open figures; the two fit figures
    re-run with the shipped optimizer (mean_reversion fitted lost to buy and
    hold on 62% of names in 2020-2026, ma_crossover 72%).

    **One finding changed.** At next-open fills the 2011-2019 fit no longer
    picks trend's 200-day window: it picks 100 days on faber5 and 50 on
    faber7, which then score 0.32 and 0.42 in 2020-2026 against the
    default's 0.57 and 0.60. Decision 29's "trend's fit held up" was a
    same-close result; at the next open, fitting trend hurt it. Momentum's
    fit (126 days, top 3) still beat its default out of sample (0.64 / 0.81
    against 0.55 / 0.56).
