# A fill shows a day late in allocation backtests (decision 52, 2026-10-20)

Found while building decision 51 (`research/paper-tracking-2026-10-20.md`).

## What happens

`PortfolioBacktester.run`'s event path, used by every allocation rule
(basket_trading, index_rebalancing, asset_class_trend, momentum_allocation,
paired_switching), handles each bar in this order:

1. It marks the positions it carried in at the close and writes the bar's
   `total`.
2. It executes the orders signalled on the bar before, at this bar's open.
3. It updates `cash` and the positions, but not `total`.

So on a fill day, `total` is the old holdings marked at the close. The fill,
its costs and the new holdings' move from open to close first appear in the
next bar's `total`. Nothing is lost: cash and positions are right, so the
next bar is right and the final equity is exact. The error sits in one day's
value per fill day, and that one day's return is wrong.

Neither the single-asset `Backtester` (it records state after fills) nor the
target path (`_run_targets`, decision 48: it marks after fills) has this
fault. Pairs trading is the control below.

## Measured

The 27 real runs the regression pin uses (6 baskets x the allocation rules,
2016-2025, plus vol targeting, a risk-capped run and pairs trading).
"Corrected" is `cash + positions x close` on every bar, which is what
decision 51's tracking already uses.

- **Final equity and total return:** identical in every run.
- **Max drawdown:** identical in every run.
- **Sharpe:** moves -0.002 to +0.009, mean |0.0015|. By rule:
  - asset_class_trend -0.002 to +0.009 (the largest, Faber 5 with
    volatility targeting: 0.638 to 0.648);
  - momentum_allocation +0.001 to +0.006;
  - basket_trading and index_rebalancing -0.001 to +0.002;
  - paired_switching 0.000 to +0.002.
- **Volatility:** -0.2 to +0.04 pp.
- **pairs_trading:** exactly 0, as expected.
- Fill days are 3% of bars on average.

The rules that trade large blocks (trend, momentum, switching) move most,
because their fill days carry the largest one-day misstatement: up to
$76k on a basket holding BTC.

Two registry caveats quote allocation Sharpes to two decimals:
- momentum_allocation, Faber 5: 0.61 against 0.46;
- the trend caveat's drawdown and return figures, which should not move,
  since drawdown and return did not.

momentum_allocation's 0.61 can move by 0.01.

## The fix

- After the bar's fills, recompute the bar's holdings value and `total` at
  the close. One statement at the end of the bar loop, the same arithmetic
  the target path uses.
- Trades, fills, positions, cash and final equity stay bit-identical:
  checked on the 27 pinned runs, where only `total`, `holdings_value` and
  `returns` on fill days may change.
- A test fails against the old code: a bar's `total` after a fill equals
  cash plus the new holdings at the close.
- Decision 51's tracking then reads `total` directly, and its workaround goes.

## Decision

52. **Recommend A.**

    - **A: fix it, and re-measure the figures the caveats quote.** Re-run the
      decision 29/32 basket study (`research/multi-asset-strategies-2026-10-15.md`,
      2011-2026) for the cited Sharpes, and update any that change. About
      2-3 h of my time; the estimate assumes the study's runs are reproducible
      from the doc's stated baskets and windows through the API.
    - **B: fix it, without re-measuring.** About 1 h. It leaves a caveat that
      may be 0.01 off with nothing saying so: the rule is to re-measure
      before editing a figure, and a change to the backtester makes them
      stale.
    - **C: leave it,** noted in CLAUDE.md. Nothing a user acts on changes
      (returns and drawdowns are exact; Sharpes move in the third decimal),
      but every allocation-rule equity curve on the Allocation page keeps a
      one-day step on each fill day.

**Approved: A, 2026-10-20.**

## Built (A, 2026-10-20)

- **The fix:** after a bar's fills, `PortfolioBacktester.run` revalues that
  bar's `holdings_value` and `total` at the close, from the cash and
  positions the fills left. Only bars with orders are revalued. A bar whose
  orders all failed revalues to the same figure.
- **The 27 pinned runs, HEAD against the fix:**
  - trade logs, cash and every position column are identical;
  - `total` changes on exactly the fill bars and on no other;
  - final equity, total return and max drawdown are identical;
  - each run's new Sharpe equals the "corrected" figure measured above, to
    the last digit;
  - pairs_trading did not change.
- **Test:** `test_a_basket_fill_shows_in_the_equity_of_its_own_bar` in
  `tests/unit/test_next_open_fills.py` covers a buy and a sale. It fails on
  the old code.
- **Tracking (decision 51)** now reads the engine's `total` directly, and its
  workaround is gone. `test_without_slippage_the_backtest_is_the_paper_record`
  fails without the engine fix, so it pins this too.

### The caveats, re-measured

Decision 29's method: one run per configuration, 2010-01-04 to 2026-10-14,
at its defaults through the API. Settings: seed 1, 10 bps, interest on cash,
risk caps off. Sharpe is measured on slices. On HEAD the method reproduces
every quoted figure. Before against after:

| run | 2011-26 | 2011-19 | 2020-26 |
|---|---|---|---|
| faber5 equal weight | 0.457 / 0.457 | 0.499 / 0.499 | 0.422 / 0.422 |
| faber5 trend | 0.446 / 0.449 | 0.343 / 0.345 | 0.574 / 0.580 |
| faber5 momentum | **0.613 / 0.617** | 0.672 / 0.673 | 0.546 / 0.564 |
| faber7 equal weight | 0.522 / 0.522 | 0.630 / 0.630 | 0.435 / 0.435 |
| faber7 trend | **0.514 / 0.516** | 0.435 / 0.437 | 0.596 / 0.598 |
| faber7 momentum | 0.491 / 0.492 | 0.429 / 0.429 | 0.556 / 0.559 |
| SPY/TLT switching | 0.265 / 0.265 | 0.732 / 0.734 | -0.180 / -0.180 |
| SPY/TLT 50/50 | 0.686 / 0.686 | 1.413 / 1.413 | 0.174 / 0.174 |
| momentum grid, 2020-26 (both baskets) | | | 0.221-0.879 / 0.222-0.880 |

- **Two quoted figures moved at two decimals:**
  - momentum on Faber five: 0.61 to **0.62** against 0.46;
  - trend on Faber seven: 0.51 to **0.52** against 0.52, so it is now
    level with its basket on both baskets.
- **Both caveats are restated in the registry.** Every other figure they
  quote is unchanged: the switching decades, the momentum grid range, and
  all drawdowns (max drawdown did not move in any run).
- **Not changed here:** the trend caveat quotes the Faber seven basket's
  drawdown as -20%. The method gives -19.5% both before and after this fix,
  so it is not a lag effect. It is left for Danny.
