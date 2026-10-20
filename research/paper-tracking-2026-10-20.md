# A paper portfolio against its own backtest (decision 51, 2026-10-20)

Decision 50 attached strategies to paper portfolios. "Paper - Asset Class
Trend" (Faber 5: SPY, EFA, IEF, VNQ, DBC; window 200, monthly) was attached
on 2026-10-20. It holds cash until its first monthly decision, at the
2026-10-30 close, which fills at the 2026-11-02 open. This decision is about
how anyone would know, months from now, whether that record still says what
its backtest says, and why not where it does not.

## What can make them differ

Tests pin the trader to the engine (identical fills on fixtures with no
corporate actions, no rates and no slippage). Real data adds five sources of
difference. Each has a known sign, and only the last is a fault:

1. **Dividends.** Paper takes dividends as cash; the backtest reinvests
   them through adjusted prices.
2. **Sizing.** Paper sizes on the raw open; the backtest sizes on an open
   lowered by later dividends. In the decision 50 check, VNQ was 203 shares
   against 205.
3. **Slippage.** Paper fills at the open exactly. The backtest the
   Allocation page runs draws adverse slippage averaging 0.025% a fill.
4. **Interest.** Paper compounds ^IRX nightly. The backtest accrues the rate
   known at each bar's start, over the calendar days to the next bar. These
   are close, not identical.
5. **Restated history.** The trader acted on the bars as they were each
   morning; a backtest run later reads them as they are now. If Yahoo
   restates a bar, or a newly fetched corporate action re-adjusts the past
   (an unlisted spinoff, a corrected dividend), the backtest's signals for
   days already traded can change. The paper record then no longer follows
   the strategy it names. Today nothing would notice: `check_fresh_returns`
   catches a restatement in the prices, not in what a strategy did with
   them.

Measured in decision 50 (three scratch portfolios, 2026-07-01 to
2026-10-19, 76 sessions), against a slippage-free backtest:

| strategy | paper equity | backtest | gap |
|---|---|---|---|
| asset_class_trend, 5 ETFs | $98,563.50 | $98,554.83 | +$8.67 |
| momentum_allocation, 4 ETFs | $99,065.58 | $99,064.36 | +$1.22 |
| pairs_trading GLD/SLV | $101,499.36 | $101,487.70 | +$11.66 |

The gaps are small and expected. Nothing in them is item 5, which is the one
that matters.

## The design

### The comparison, on demand

`GET /portfolios/{name}/tracking`, for a portfolio with a strategy, returns
two daily equity curves from the attach date.

- **Paper:** `derive_state`'s day loop extended to emit equity at each raw
  close. One pass, not one derivation per day; it costs about what one state
  read costs today (0.5 s for the Default Portfolio).
- **Backtest:** `PortfolioBacktester` on the same strategy, parameters and
  basket, with signals masked before the attach date so it starts flat on
  the same day. It runs at the Allocation page's defaults (seed 42 slippage,
  the stored `transaction_cost`, ^IRX accrual): the backtest someone would
  actually look at.

The response also lists each side's fills, and the gap split where it can be
measured exactly:
- dividends received as cash;
- the quantity difference on each matched fill, valued at its price;
- the slippage the backtest paid (fill price against the open);
- what is left over, unexplained.

### The check that matters: restated decisions

Each recorded fill names its signal day (in `notes`, since decision 50).
The check re-derives, from today's bars, the strategy's signals for every
signal day already traded, then compares:

- **the same sessions, tickers and sides:** fine, whatever the quantities;
- **anything else:** a RESTATED flag naming the session, what was traded and
  what today's history says.

The record is never rewritten (decision 49). The flag says the past has
moved under it, and you decide whether the portfolio still means anything.

- It costs one signal computation per portfolio, about 0.5 s for pairs, not
  one per session.
- It runs in the 06:00 job, inside `run_paper_strategies.py` after each
  portfolio's run, with exit 1 on a flag.

### The page

On the Strategy card:
- the two curves on one chart (the existing `ComparisonChart`);
- the gap and its parts as a short table;
- a RESTATED alert beside the drift alert.

## Tests (each checked failing against the code it guards)

- On the parity fixtures the two curves are identical, and the unexplained
  part is 0.
- A dividend on a holding: the gap equals the cash dividend less the
  reinvested value, by hand.
- Seeded slippage: the slippage part equals the sum of fill-price minus
  open, times quantity.
- A bar restated after a session was traded raises RESTATED. Restating a bar
  after the last traded session does not.
- The paper curve's last point equals `derive_state`'s equity at the same
  close.

## Decision

51. **Recommend A.**

    - **A: the tracking endpoint, the RESTATED check in the 06:00 job, and
      the page chart.** About 5-7 h of my time. The estimate assumes
      `ComparisonChart` takes two curves as it stands, and that the gap's
      parts need no change to `PortfolioBacktester`: slippage is read from
      its trade log against the opens.
    - **B: the RESTATED check alone,** in the job, with no curves or page.
      About 2-3 h. It covers the one fault that silently invalidates a
      record. The cost of the expected differences is never shown, so a
      growing gap has no explanation beside it.
    - **C: nothing now.** Revisit once "Paper - Asset Class Trend" has a few
      months of fills. It costs nothing today. The check works backwards, so
      a restatement that lands before then is still found once it is built,
      but the portfolio keeps trading on the moved history until it is.

**Approved: A, 2026-10-20.**

## Built (A, 2026-10-20)

- **`derive_state(..., curve=)`:** the day loop emits equity at each raw
  close: the state's own cash, in one pass. A day on which a holding has no
  close is left out, never valued at cost.
- **The RESTATED check compares DECISIONS, not fills.** This is a change from
  the design above. Each session now records, in
  `metadata.strategy.decisions`, what its signal day asked for: each
  non-zero event or non-NaN target, by key. The check re-derives those from
  today's bars (one signal computation) for every signal day already traded,
  and flags any difference.
  - Comparing fills would also have flagged expected sizing differences: a
    rebalance can buy in one and sell in the other over a two-share gap.
  - It would also have missed a changed decision that traded nothing in
    either.
  - Signal days not yet traded are not checked.
- **`api/paper_tracking.track`:**
  - **Paper:** the curve from the attach date.
  - **Backtest:** `PortfolioBacktester` on the same signals, masked before
    the attach date. It uses seed 42, the handler's 0.05% slippage bound, the
    stored cost, ^IRX accrual and the portfolio's short terms.
  - **The gap at the last common close,** split into the slippage the
    backtest paid (fill against open, times quantity), the cost difference,
    and the rest.
  - **The dividend part could not be split exactly.** The backtest also
    earns dividends, through adjusted prices, so dividends sit inside the
    rest, and the response labels the rest that way.
- **A finding, not changed here:** on the event path, `PortfolioBacktester`'s
  `total` for a bar is computed BEFORE that bar's open fills. A fill day's
  equity is the previous position marked at the close, and the fill first
  shows at the next close.
  - Tracking values the backtest from the engine's own cash and positions at
  each close instead. Read raw, that column put a one-bar step into the gap
  on every fill day.
  - The target path does not have this.
  - Every allocation-rule metric reads that column. The effect is one bar
    per fill, so small for monthly rules, but it has not been measured. That
    would be its own decision.
- **REST:** `GET /portfolios/{name}/tracking`: 404, 409 without a strategy,
  422 when ^IRX misses the window.
- **The 06:00 job:** `run_paper_strategies.py` prints a TRACKING line per
  portfolio, and a RESTATED flag (exit 1). A missing rate skips the
  comparison, never the fills.
- **The page:** the two curves on the Strategy card, the gap table, and a
  RESTATED alert.

### Tests

- **API:** 6.
  - With slippage 0 the two curves are the same curve and the gap is 0.
  - The gap splits exactly into its parts.
  - A halved AAPL bar on a traded decision day is flagged (with the later
    decision whose average holds it).
  - A restated newest bar is not flagged, and neither are new bars' decisions
    not yet traded.
- **Accounting:** 1 (the curve). **Page:** 3.
- **9 mutants** (7 Python, 2 page), each caught.

### Real-data check

Two scratch portfolios, attached as of 2026-07-01 and traded through
2026-10-19 (77 closes), then deleted:

| strategy | gap | slippage | costs | other | slippage-free gap | restated |
|---|---|---|---|---|---|---|
| asset_class_trend, weekly, 5 ETFs | +$27.01 | $24.92 | $0.00 | +$2.09 | +$1.91 | 0 |
| pairs_trading GLD/SLV | +$28.60 | $28.42 | -$0.01 | +$0.20 | +$0.05 | 0 |

- **The gap is slippage.** Paper is ahead by what the backtest's slippage
  cost; without it they differ by $0.05-1.91.
- **The decision 50 table's $1-12 gaps** compared the engine's
  `total`, with the one-bar lag above.
- **Run time:** a comparison takes 1-2 s.

The change to the RESTATED check (decisions, not fills) approved by Danny, 2026-10-20.
