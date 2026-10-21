# An option strategy payoff page (decision 54, 2026-10-20)

Vault task: "Build interactive option strategy visualizer" (brief task 6),
the payoff half only. Build multi-leg positions and see their profit at
expiry, with breakevens and the most each can make and lose. The task's
purpose is teaching: payoff shape is the fastest way to learn how a
structure behaves (CFA III LM 1). The Greeks, the curve before expiry and
the return-distribution overlay are separate vault tasks that wait for
this one.

## What exists

- **Option chains:** captured once a day since 2026-09-15 for AAPL, IWM,
  NVDA, QQQ, SPY and TQQQ, 25 complete days each, in
  `data/option_chains/<TICKER>/<day>.parquet`. Each row has the strike, right,
  expiry, bid and ask, and the spot the capture read. SPY 2026-10-20 has
  5,680 contracts over 18 expiries. The quotes are near the close: they are
  read at about 15:45 New York time.
- **Options page:** it shows smiles, the term structure and a one-option
  pricer. It has no multi-leg tool. The surface it reads keeps only
  out-of-the-money quotes, so an in-the-money put's quote is not in it. A
  premium for any leg has to come from the raw capture.
- **Pricers:** Black-Scholes, a tree and Monte Carlo already exist in
  `pricing/`. The task says not to write another pricer, and the payoff at
  expiry needs none.

## The arithmetic

A position's profit at expiry, at underlying price S, is the sum over its
legs of side x quantity x multiplier x (the leg's value at S minus its
premium):

- call: max(S - K, 0)
- put: max(K - S, 0)
- stock: S

The side is +1 for a buy and -1 for a sale. The multiplier is 100 shares a
contract and 1 for stock.

The profit is piecewise linear, and it bends only at the strikes. That
makes everything exact without sampling:

- **Breakevens:** within each segment between strikes, the profit is
  linear, so its zero is solved directly.
- **Maximum gain and loss:** the extremes are at S = 0, at a strike, or at
  infinity. Beyond the highest strike the slope settles it: a positive
  slope is an unlimited gain, a negative one an unlimited loss. The left
  end stops at S = 0, so a short put's loss is bounded, at K minus the
  premium a share.

The chart draws the same function between 0.5x and 1.5x the reference
price, with the strikes inside that range.

## The one question: calendars

The brief lists calendars, and a calendar's legs expire on different
dates. At the near expiry, the far option is still alive, and what it is
worth then depends on volatility and the time it has left. That is a
price, not a payoff. There is no way to draw a calendar "at expiry"
without a pricer and a volatility input, and those are exactly what the
payoff half excludes.

The value of the far leg at the near expiry is one point on the curve
before expiry, which is what the Greeks task builds.

## Options

**A. Every same-expiry structure, with premiums from the captured quotes
(recommended).** About 4-5 h of my time, assuming the capture format holds
for all six tickers.
- **Page:** a new "Payoffs" page.
- **Arithmetic:** a pure, tested `payoff.ts`, so the chart and the figures
  update as each leg is edited, with no request.
- **Templates:** long and short call and put, bull and bear call and put
  spreads, long and short straddle and strangle, call and put butterfly,
  iron condor, covered call, collar. Each starts at the strike nearest the
  reference price and adjusts from there.
- **Legs:** a template fills in its legs, and any leg can then be edited,
  added or removed.
- **Premiums:** they come from one capture, through a new `GET
  /options/quotes` (ticker, day, expiry: bid and ask for every strike and
  both rights, plus the capture's spot). A buy pays the ask and a sale
  receives the bid by default, which is what the position would have cost.
  Mid is a switch. Any premium can be typed over. The capture's spot is
  the stock leg's price and the chart's reference line.
- **Without a capture:** the page also works with typed premiums for any
  symbol.
- **Calendars:** left out until the Greeks task brings the curve before
  expiry. The page says why.

**B. A plus calendars.** About 6-8 h.
- The far leg is valued at the near expiry with `pricing/`'s
  Black-Scholes, through a new endpoint.
- Its volatility is the far expiry's implied volatility at that strike,
  from the surface (computed and cached for the last seven days only, so
  an older capture costs 10-35 s).
- This puts a pricer and a volatility into the half the task kept free of
  them. That volatility is also a modelling choice that should be made
  once, in the Greeks task, not here first.

**C. Typed premiums only, no backend.** About 3-4 h. This is the brief taken
literally. But every premium is then made up. With six tickers captured
daily, the real cost of a structure is one request away, and a payoff built
on invented premiums teaches the wrong breakevens.

## Tests

- **Values:** the profit of each named structure matches a hand-computed
  value at three spot prices (below, between and above its strikes).
- **Extremes:** breakevens, maximum gain and maximum loss are correct for
  each structure, with "unlimited" exactly where the slope says so. A
  covered call's loss is bounded, and a short call's is not.
- **Symmetry:** a long straddle's profit is symmetric about its strike.
- **Quotes endpoint:** bid and ask straight from the parquet. 404 for a
  missing capture or expiry, and a partial capture is not served, as for
  the surface.
- **Mutants:** each test is checked against a mutant of the code it covers.

**Approved: A, 2026-10-20.**

## Built (option A, 2026-10-20)

- **Endpoint:** `GET /api/v1/options/quotes` (`api/routers/options.py`):
  - one expiry's bid and ask per strike, for both rights, straight from
    the parquet;
  - the capture's spot is the median of that expiry's rows;
  - the default expiry is the one nearest `OPTION_QUOTES_TARGET_DAYS` (30);
  - a partial capture is never served.

  The surface's latest-capture lookup is now shared with it
  (`latest_capture`).
- **Arithmetic:** `frontend/src/components/options/payoff.ts` gives profit,
  breakevens, extremes and the chart curve, solved exactly from S = 0, the
  strikes and the slope beyond them.
- **Templates:** `payoffTemplates.ts` holds the 17 templates, with strikes
  2.5% apart at the listed strike nearest each target. A premium is the ask
  to buy, the bid to sell, or mid. A buy needs an ask above zero; a sale
  may take a zero bid.
- **Page:** `pages/PayoffPage.tsx` plus `charts/PayoffChart.tsx`, at
  `/payoffs` ("Payoffs" in the nav).
- **Tests:**

  | where | tests |
  |---|---|
  | `payoff.test.ts` | 34: 13 structures at three prices, plus their breakevens and extremes |
  | `payoffTemplates.test.ts` | 9 |
  | `PayoffPage.test.tsx` | 8 |
  | API | 3 |

  Totals: Python 1789 passed (1 skipped), frontend 393 passed; eslint and
  the build are clean.
- **Mutants:** 14 of 14 caught. Two survived at first, and the tests were
  strengthened until they were caught:
  - a breakeven exactly at a strike;
  - a later partial capture.
- **Chart window, changed on review:** the chart runs from 15% below the
  lowest strike, breakeven or spot to 15% above the highest
  (`payoffWindow`). The first window, 0.5x to 1.5x spot, squeezed a SPY
  straddle into a narrow V. The top margin was also widened so the spot
  label is no longer cut off.
- **Real capture:** SPY 2026-10-20 defaults to 2026-11-20, which has 221
  strikes, with spot 768.56.
