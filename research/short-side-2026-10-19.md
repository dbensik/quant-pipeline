# Short positions in the single-asset backtester (decision 47, 2026-10-19)

Vault task "Add short-side support to the backtester". `Backtester` reads
-1 as "exit long" (decision 22); the task asked for real short positions
with borrow costs, on the grounds that strategies which mean "short" are
evaluated long-only. The agent brief asked for the convention to be proposed
before coding, and for -1 not to change meaning for existing strategies.
This measures first whether shorts would help at all.

## Method

- **A prototype engine in the session scratchpad; nothing in the repo is
  edited.** It copies `Backtester`'s loop: next-open fills, adverse
  slippage, 10 bps + $1, idle cash earning ^IRX, lots, seed 1. On top of
  that it trades a target in {-1, 0, +1}.
- **It reproduces the shipped `Backtester` to the cent with shorts off:**
  SPY 2016-2025 ma_crossover 10/100 $327,792.65, 27 trades, Sharpe
  0.868509; mean_reversion, rsi and atr_breakout also exact.
- **Hand checks:** a short over a 100 -> 50 slide gains about $50k, and
  over 100 -> 150 loses about $50k. 5% borrow costs $3,608 against $3,605
  computed by hand.
- **Accounting choices:**
  - A flip from long to short is two fills, each paying costs.
  - Short size is the equity at the next open: 1x notional, never long and
    short together.
  - Short proceeds are collateral and earn nothing. Sensitivity row
    `LS1p`: proceeds earn ^IRX.
  - Borrow accrues on |position| x the previous close per calendar day.
    It is swept at 0%, 1% and 5%; LS1 (1%) is the headline.
  - The run stops if equity reaches 0 ("ruin").
- **Which strategies have a short mirror.** For ma_crossover,
  trend_following and atr_breakout, the exit state is a bearish regime, so
  -1 reverses to short. For rsi, selling at overbought reverses to short.
  For push_response, -1 is its "price will fall" prediction (what the task
  text meant). mean_reversion's -1 fires AT the mean, so reversing there
  would short fair value; it gets its own mirror (long below -threshold,
  short above +threshold, flat when z crosses 0), which reproduces its
  long-only events exactly. ml_random_forest is excluded.
- **Assets:** decision 28's 428 equities and ETFs (windows 2010-26, 2010-19,
  2020-26) and decision 35's 14 coins (2018-26, 2018-21, 2022-26).
  Defaults, 7,956 runs, 0 errors, 4 min on 13 processes.
- Scripts `ls47.py` and `dump47c.py` (session scratchpad).

## Results (LS = long/short at 1% borrow; medians across names)

### Equities and ETFs, 2010-2026

| strategy | Sharpe: long-only / LS / B&H | CAGR: long-only / LS / B&H | max DD: LS (B&H -53%) | LS beats long-only | LS beats B&H | ruined |
|---|---|---|---|---|---|---|
| ma_crossover | 0.21 / -0.17 / 0.39 | 6.1% / -3.2% / 12.9% | -79% | 0.2% | 1.9% | 6 |
| trend_following | 0.09 / -0.37 | 3.2% / -7.8% | -85% | 0% | 0% | 1 |
| atr_breakout | 0.05 / -0.53 | 2.1% / -12.3% | -93% | 0% | 0.2% | 1 |
| rsi | 0.29 / -0.28 | 7.5% / -13.0% | -98% | 0.7% | 3.0% | **209** |
| mean_reversion (mirror) | 0.20 / -0.19 | 5.0% / -3.1% | -72% | 0% | 0.9% | 16 |
| push_response | -0.32 / -1.08 | -5.0% / -27.4% | -99.6% | 0% | 0% | 2 |

In 2010-2019 and 2020-2026 separately the picture is the same: LS beats
long-only on 0-2% of names in 2010-19 and 1-11% in 2020-26. rsi shorts
ruined 143 and 109 names.

### Crypto (14 coins)

| strategy | LS beats long-only: 2018-26 / 2018-21 / 2022-26 | LS beats B&H, 2018-26 | Sharpe 2018-26: long-only / LS | ruined, 2018-26 |
|---|---|---|---|---|
| trend_following | 14% / **50%** / 21% | 64% (long-only 93%) | 0.35 / 0.05 | 0 |
| ma_crossover | 0% / 7% / 21% | 21% | 0.10 / -0.17 | 6 |
| atr_breakout | 0% / 0% / 21% | 29% | 0.61 / -0.12 | 1 |
| rsi | 0% / 0% / 7% | 0% | 0.14 / -0.96 | **14 of 14** |
| mean_reversion (mirror) | 14% / 29% / 7% | 0% | -0.28 / -1.18 | 11 |
| push_response | 0% / 0% / 14% | 0% | -0.40 / -1.16 | 14 |

### Sensitivity

- **Borrow rate barely matters next to the effect.** From 0% to 5% the
  median Sharpe moves by 0.01-0.4 (rsi on equities -0.23 -> -0.65); from
  long-only to LS at 0% it already falls by 0.3-1.1.
- **Paying interest on short proceeds** (`LS1p`) adds 0.01-0.05.

## Reading

- **Shorting loses on every strategy, on both asset classes, in every
  window.** On equities it was expected: an upward-drifting asset charges a
  short its drift. A long/short trend rule is short about a third of the
  time and pays for it (ma_crossover -3.2% a year against +6.1%
  long-only).
- **Crypto's bear markets did not rescue it either.** In 2022-2026, which
  includes the 2022 crash, LS beat long-only on at most 21% of coins. The
  rules flip short after a fall and get squeezed on the rebounds. Long-only
  trend following already captures the protection (decision 35) by
  sitting in cash.
- **The one exception is trend following on crypto in 2018-2021,** better
  on 7 of 14 coins. It did not persist into 2022-2026 (3 of 14); this is the
  decision 41 lesson.
- **Contrarian shorts are dangerous, not just weak.** rsi's mirror sells
  short at "overbought" and holds it to "oversold": it ruined every coin
  and half the equities. Any short support would need a maintenance-margin
  rule, not just ruin.
- **The task's premise, that strategies are understated by being evaluated
  long-only, is not borne out.** Each would be worse with its shorts.
- **More hypotheses with no measured edge is the overfitting risk decision
  41 measured.** The task says shorts would "roughly double the hypothesis
  space".
- **The size of the loss is biased against shorts; its direction is not.**
  The 428 names are survivors with an upward drift. The names a short wins
  on most, those that went to zero, are among the delisted tickers Yahoo no
  longer serves. A trend flip still pays the drift of what it shorts.
- **This measures these strategies' exits, not short selling.** Whether the
  platform should hold shorts is a separate question, for strategies whose
  edge is a short leg by design (pairs, long/short factors). It is decision
  48.

## Decision

47. **Recommend B.**

    - **A: build it.** Opt-in short support in `Backtester` and
      `PortfolioBacktester`:
      - a strategy declares a short mirror (`supports_short` in the registry,
        with its own rule where -1 is not a regime flip, as for
        mean_reversion);
      - `allow_short` on a backtest request, with a configurable borrow rate;
      - 1x notional, a maintenance-margin rule in config, and the run
        stopped at ruin;
      - shorts off is bit-identical to today.

      Tests: the brief's four (shorts off byte-identical, a falling series
      profits net of borrow, borrow matches a hand computation, cash never
      breaks the margin rule) plus a forced margin call. About 5-7 h of my
      time. On this evidence it would ship a switch that makes every
      strategy worse.
    - **B (recommended): don't build; record the finding.** Close the vault
      task with this measurement. Add one sentence to the caveats of
      trend_following, ma_crossover, atr_breakout, rsi and push_response:
      with its -1 read as a short, on how many names and coins it beat
      long-only (decision 47). Revisit only for a strategy whose edge is
      market-neutral by design (decision 48). About 30 min.
    - **C: build only what has evidence, a long/short trend rule for
      crypto.** Not recommended: it won on 7 of 14 coins in one window and
      3 of 14 in the next.

## Approved: B, re-scoped (2026-10-19)

Danny's goal is to backtest and paper trade short positions. This decision
answers only whether the six single-asset strategies' exits should become
shorts: they should not, and `Backtester` stays long-only. Short support is
decision 48 (`research/short-engine-2026-10-19.md`), built in
`PortfolioBacktester`, where every market-neutral strategy already runs.
A second opinion (Fable, reading the code) found three things this write-up
had wrong or missed, each checked:

- **`pairs_trading` does not run one leg at a time; it buys the leg that
  should be short.** Entering a short spread emits -1 for one leg, which
  `PortfolioBacktester` ignores at flat (it sells only a holding above 0),
  and +1 for the other, which it buys. When the spread trade closes, the
  first leg gets +1 and is BOUGHT. With 0.0 written where it means "no
  change" (the decision 22 bug), it holds only while |z| is beyond the
  threshold: on a synthetic pair most trades lasted one day. Its registry
  caveat now says so.
- **Portfolio backtests took shorts through a negative weight.** The router
  checked weight names only; a weight of -0.3 reached the rebalance branch,
  which SOLD from flat and spent the proceeds as cash. Traced: weights
  {A: 1.3, B: -0.3} on $100k left B at -616 shares and $128k of cash, with
  no collateral, borrow or margin, and the risk manager checks buys only.
  A negative weight is now 422.
- **Paper portfolios accept a short with the same treatment:** a sale larger
  than the holding leaves a negative quantity, and its proceeds count as
  cash. That is decision 48's paper half.

Built: the 422 and its test (which returned 200 before the fix); the five
caveats, through `measured_short()` in `alpha_models/registry.py`, with
counts read from the run (better Sharpe than long-only at 1% borrow:
ma_crossover 1 of 428 names, rsi 3 with 209 ruined, the rest 0; on coins
2018-2026 trend following 2 of 14, the rest 0); the `pairs_trading` caveat.
