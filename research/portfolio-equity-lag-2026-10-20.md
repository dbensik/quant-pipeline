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
