# Multi-asset allocation strategies, 2010-2026 (decision 29, 2026-10-15)

Decisions 25 and 28 measured only the single-asset strategies. This is the
first measurement of the three allocation rules in the registry:

- `asset_class_trend` — hold each sleeve while it is above its 200-day
  average, else cash; monthly (Faber's 10-month rule).
- `momentum_allocation` — hold the `top_n` (2) sleeves by 126-day return;
  monthly.
- `paired_switching` — hold whichever of two assets had the better 63-day
  return; quarterly.

`pairs_trading`, `cointegrated_mean_reversion`, `basket_trading` were left
out: their pairs or weights would have to be chosen on the data they are
tested on (and `basket_trading` cannot be built from the API).

**Every figure below is at same-close fills.** Decision 31 moved both
backtesters to next-open fills the same day; the restated figures, and one
finding that does not survive (trend's fit), are in
`research/fill-timing-2026-10-15.md`.

## Method

- **Baskets fixed in advance, not picked on this data.** `faber5` = SPY,
  EFA, IEF, VNQ, DBC (Faber, 2007: US and foreign equity, Treasuries,
  REITs, commodities); `faber7` = faber5 + GLD + TLT. Pairs: SPY with TLT,
  IEF, AGG. ETFs do not delist, so survivorship is not the issue it is for
  equities. All have bars from 2010-01-04.
- **Benchmarks:** the same basket equal-weighted and rebalanced monthly
  (`index_rebalancing`); 60/40 SPY/AGG and 50/50 SPY/TLT, monthly; SPY
  bought and held.
- **Runs:** in process through the endpoint's own helpers (`_build_signals`,
  `PortfolioBacktester`, `resolve_risk_free`, lots) — checked equal to POST
  `/api/v1/backtest/portfolio` to the last digit on one run. 10 bps per
  fill, adverse slippage (seed 1), idle cash earns `^IRX`, Sharpe against
  that cash account. Sized purely by weights (`max_trade_risk_pct` and
  `max_portfolio_drawdown_pct` = 1.0; the defaults are measured separately
  below). Momentum bought 1/`top_n` per sleeve, pairs 1.0 per leg, trend
  1/N. These are weights AT ENTRY: a sleeve held from one month to the next
  is not re-weighted (a 1 signal buys only when flat), so weights drift.
  The holdings check below verifies the SET held, not the weights.
- **One run per configuration over 2010-2026, statistics on slices**
  (2011-2026, 2011-2019, 2020-2026), so no window starts with a strategy
  still warming up — a run started in 2020 would have sat in cash through
  the March crash and flattered itself.
- 46 configurations: each strategy's default grid on each basket or pair,
  plus benchmarks. Scripts `run29.py`, `check29.py`, `analyse29.py`,
  `lag29.py` (session scratchpad, not committed).

## Each rule does what it says

Holdings rebuilt from each default run's trade log and compared, on every
rebalance date, with the rule recomputed from prices: trend 192 of 192
month-ends on both baskets, momentum 195 of 195, paired switching 66 of 66
quarter-ends on each pair — no difference. The check is not vacuous:
pointed at the wrong parameters (trend 100 days, momentum 63 days) it
reports 107 and 123 differing month-ends.

## Results, defaults, 2011-01 to 2026-10

| run | CAGR | vol | Sharpe | max DD | 2020 crash | 2022 | invested |
|---|---|---|---|---|---|---|---|
| faber5 equal weight | 6.8% | 11.3% | 0.45 | -26.2% | -26.2% | -11.4% | 100% |
| faber5 trend | 5.5% | 7.8% | 0.49 | -14.0% | -5.8% | -4.6% | 72% |
| faber5 momentum | 9.8% | 12.6% | 0.64 | -21.7% | -21.2% | -12.1% | 100% |
| faber7 equal weight | 6.3% | 9.0% | 0.52 | -19.5% | -17.7% | -12.4% | 100% |
| faber7 trend | 5.3% | 6.8% | 0.54 | -7.7% | -2.4% | -4.0% | 70% |
| faber7 momentum | 8.5% | 13.2% | 0.51 | -20.2% | -2.8% | -12.1% | 99% |
| SPY/TLT switching | 7.2% | 16.2% | 0.34 | -35.7% | -33.7% | -23.6% | 100% |
| SPY/IEF switching | 6.6% | 14.2% | 0.35 | -36.2% | -33.7% | -19.7% | 100% |
| SPY/AGG switching | 6.5% | 13.8% | 0.35 | -35.1% | -33.7% | -23.2% | 100% |
| 60/40 SPY/AGG | 9.3% | 10.3% | 0.73 | -21.6% | -21.6% | -15.9% | 100% |
| 50/50 SPY/TLT | 8.2% | 9.6% | 0.68 | -28.9% | -11.5% | -23.8% | 100% |
| SPY buy and hold | 14.1% | 17.0% | 0.72 | -33.7% | -33.7% | -18.6% | 100% |

"2020 crash" is 2020-02-19 to 2020-03-23.

By decade (Sharpe, 2011-2019 / 2020-2026): faber5 equal weight 0.50 /
0.42, trend 0.42 / 0.56, momentum 0.71 / 0.57; faber7 equal weight 0.62 /
0.43, trend 0.51 / 0.57, momentum 0.44 / 0.59; switching SPY/TLT 0.79 /
-0.09 against its own 50/50's 1.41 / 0.17; 60/40 1.05 / 0.49.

- **Trend following is the one rule in the registry that measurably limits
  drawdowns while keeping its basket's Sharpe.** It halved the basket's
  worst drawdown (-14.0% against -26.2%; -7.7% against -19.5%), lost 5.8%
  and 2.4% in the 2020 crash against 26.2% and 17.7%, and 4.6% / 4.0% in
  2022, when stocks and bonds fell together. It costs about 1-1.3 pp a year
  of return, and its Sharpe is level with its basket (0.49 against 0.45,
  0.54 against 0.52) — lower in 2011-2019, higher in 2020-2026. Unlike the
  single-asset strategies' shallower drawdowns, which were time in cash
  across the board, this is cash at the right time: 72% invested on
  average; on faber5, 85% invested on 2020-02-19 and 24% at the 03-23 low
  (the February month-end sold most sleeves), and in 2022 98% in January,
  67% in April, 29% in July, all cash from October.
- **Momentum's result depends on the basket.** On faber5 it beat its basket
  (0.64 against 0.45, +3 pp a year); on faber7 it did not (0.51 against
  0.52). It offers no drawdown protection (2022: -12.1%), and across its
  grid the 2020-2026 Sharpe runs from 0.32 to 0.86 — the parameters matter
  more than the rule.
- **Paired switching is the worst rule here.** It loses to a static 50/50
  of the same pair in both decades (2011-2019 0.79 against 1.41; 2020-2026
  -0.09 against 0.17), took the full 2020 crash (-33.7%, same as SPY), and
  holds SPY or TLT in 2022 for -23.6%.
- **None beats 60/40 or SPY on Sharpe over 2011-2026** (0.73, 0.72). That is
  a different portfolio, not a fair test of the rules: US large caps were
  the decade's best asset, and every Faber sleeve but SPY lagged them.
  Against the basket each rule actually trades, trend matches and protects,
  momentum depends on the basket, switching loses.

### Fitted on 2011-2019, tested on 2020-2026

| | best in-sample | in-sample Sharpe | 2020-26 fitted | 2020-26 default | grid range 2020-26 |
|---|---|---|---|---|---|
| faber5 trend | window 200 (the default) | 0.42 | 0.56 | 0.56 | 0.29-0.56 |
| faber7 trend | window 200 (the default) | 0.51 | 0.57 | 0.57 | 0.33-0.57 |
| faber5 momentum | 126 days, top 3 | 0.80 | 0.64 | 0.57 | 0.32-0.78 |
| faber7 momentum | 126 days, top 3 | 0.69 | 0.82 | 0.59 | 0.36-0.86 |
| SPY/TLT switching | 63 days (the default) | 0.79 | -0.09 | -0.09 | -0.38-0.38 |

Unlike the single-asset fits (58-92% of the Sharpe lost out of sample),
the allocation rules held up: trend's in-sample choice was the classic
200 days, and its out-of-sample Sharpe was higher than in-sample. Paired
switching collapsed (0.79 -> -0.09).

## What a user gets at the API's defaults

Multi-asset strategies are reachable only through
`POST /api/v1/backtest/portfolio` — no dashboard page calls it, and Compare
is single-asset. Two defaults there leave a portfolio mostly in cash:

- `max_trade_risk_pct = 0.02` caps every buy at 2% of equity: trend on
  faber5 is 7.6% invested on average, for 2.0% a year (the endpoint's own
  field description already says so).
- Equal weights for momentum give each held sleeve 1/5, not 1/2: 42%
  invested, 5.0% a year against 9.8%.

## Fills at the signal bar's close

Both backtesters (`backtesting/backtester.py` and
`backtesting/portfolio_backtester.py`) fill an order at the close of the
bar whose close produced the signal. Delaying every signal one bar:

| default run, 2011-2026 | Sharpe same close -> next bar | CAGR | max DD |
|---|---|---|---|
| faber5 trend | 0.49 -> 0.40 | 5.5% -> 4.8% | -14.0% -> -16.4% |
| faber7 trend | 0.54 -> 0.47 | 5.3% -> 4.8% | -7.7% -> -8.6% |
| faber5 momentum | 0.64 -> 0.61 | 9.8% -> 9.5% | -21.7% -> -19.6% |
| faber7 momentum | 0.51 -> 0.45 | 8.5% -> 7.7% | -20.2% -> -22.5% |
| SPY/TLT switching | 0.34 -> 0.26 | 7.2% -> 5.8% | -35.7% -> -35.6% |

With a one-bar delay, faber5 trend falls below its basket's Sharpe (0.40
against 0.45); the drawdown protection survives (-16.4% against -26.2%).
These rules trade monthly; the single-asset strategies trade on daily
signals and should be more exposed. Not measured here — decision 30.

## Decisions

29. **Recommend: say what was measured where a user picks one of these
    three**, as decisions 25 and 28 did for the single-asset strategies —
    an evidence line in each caveat (through a second helper beside
    `measured()`, whose "428 names, 2010-2026" prefix is single-asset) (trend: "on the Faber 5 and 7
    baskets, 2011-2026, halved the basket's worst drawdown (-14% against
    -26%) at about 1 pp a year, Sharpe level with the basket"; momentum:
    "beat its basket on one of two baskets, no drawdown protection, and
    its sizing default leaves it 40% invested — pass weights of 1/top_n";
    switching: "lost to a static 50/50 of the same pair in both decades").
    No strategy removed, no default changed. About 30-45 min agent time,
    then restart 8001. Alternative: record it here only.

    **Approved 2026-10-15 and built:** `measured_allocation()` in
    `alpha_models/registry.py` puts the finding in each of the three
    caveats (paired switching keeps its sizing note first). The figures are
    at same-close fills; decision 30 measured the next-open ones
    (`research/fill-timing-2026-10-15.md`).

30. **Approved 2026-10-15 and measured** — `research/fill-timing-2026-10-15.md`.
    As proposed: **fills at the next bar.** Re-run the
    428-name single-asset evaluation and these allocation defaults with
    each order filled at the next bar (its open where stored, else its
    close), and report how far every figure moves. Read-only, about 2-3 h
    agent time; whether to change the convention would be a separate
    decision on that evidence.

32. **Approved 2026-10-15 and built: make the allocation rules usable.**
    (a) `POST /backtest/portfolio` defaults `max_trade_risk_pct` and
    `max_portfolio_drawdown_pct` to 1.0 (off), and a request without
    `weights` takes the strategy's own `held_weight` from the registry. A
    default run of trend or momentum on faber5 now equals the sized-by-weights
    run to the cent. (b) An Allocation page (`/allocation`): Faber 5 / Faber 7 /
    SPY-TLT presets or any basket, the strategy and its parameters, its caveat,
    and the result beside the same basket equal-weighted, with both curves.

