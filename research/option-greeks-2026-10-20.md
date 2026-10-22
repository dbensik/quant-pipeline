# Greeks and the curve before expiry on the payoff page (decision 55, 2026-10-20)

Vault task: "Add Greeks visualizer to the option strategy tool". This is the
half of the option tool that follows the payoff page (decision 54). It adds
three things:

- **Aggregate Greeks** across the legs (delta, gamma, theta, vega, rho),
  drawn as curves against the underlying's price.
- **The curve before expiry:** a time slider shows the marked-to-market
  profit relaxing into the payoff at expiry.
- **Calendars:** the structures decision 54 left out.

Every value at a time before expiry is a model price. So the one question
here is which volatility prices each leg. Everything else follows from
code that already exists.

## What exists

- **Pricer:** `pricing/black_scholes.py` has a vectorised price and
  analytic Greeks (delta, gamma, vega, theta, rho), already checked
  against the tree and Monte Carlo (`pricing/CLAUDE.md`). The vault task
  says to import Greeks from one module and not recompute them, so the
  page asks the API and does not price in the browser.
- **Volatility per strike:** each cached surface (`data/surfaces`) carries
  every expiry's smile: our implied volatility per strike, from
  out-of-the-money mids, with the early-exercise premium removed. It also
  carries the expiry's parity forward and the rate. The surface is cached
  for the last 7 days, and a cold one costs 10-35 s.
- **SVI:** no SVI fit exists yet. That is decision 56 (brief task 8),
  written next.

## Measured: how much the smile matters

These are the smiles of the 2026-11-20 expiry (32 days out), from the
2026-10-19 capture:

| | 10% below the forward | at the forward | 5% above |
|---|---|---|---|
| SPY | 25.0% | 13.5% | 10.9% |
| AAPL | 31.8% | 26.8% | 24.8% |
| TQQQ | 70.3% | 61.0% | 57.8% |

At one flat volatility (SPY's at-the-money 13.5%), a SPY put 10% out of the
money is worth $0.03, against $1.75 at its own volatility. A call 5% out
is worth $1.90, against $0.85. The flat volatility gets the put wrong by
a factor of 50 and the call by a factor of 2.

A curve drawn at one volatility would not even start where the market
does: at today's spot, its value would not match the premiums just paid.

## How each leg is priced

- **Model:** Black-76 on each expiry's parity forward, with the capture's
  ^IRX rate. The forward moves with spot as F = S x F0/S0, as the Options
  page's pricer does.
- **Time:** to a leg's own expiry. A leg that has expired pays its
  intrinsic value, so a calendar's near leg turns into a payoff while its
  far leg still has a price.
- **Volatility:** the leg's own implied volatility, read off its expiry's
  smile at its strike. A strike between quoted points is interpolated in
  log-moneyness; a strike past the last quote takes the last value, and
  the page says so.
- **Check:** at the capture's spot and time, the position's model value
  equals the mids it was built from, give or take the early-exercise
  premium. Profit there is minus the cost of crossing the spread. This is
  pinned by a test.

## Options

**A. Each leg at its own market volatility, held at its strike, with a
switch to hold it at its moneyness (recommended).** About 7-9 h of my time,
assuming the cached surfaces cover the capture the page uses (true for
the last 7 days).
- **Sticky strike (the default):** each leg keeps the volatility of its
  own strike as spot moves.
- **Sticky moneyness (the switch):** each leg reads the smile at its new
  log-moneyness, K over the moved forward. As spot falls, a fixed strike's
  K/F rises, so on a downward skew its volatility FALLS. (Corrected
  2026-10-21: this line had said a falling put's volatility rises, which
  is backwards; a long put's delta is less negative under this rule.)
- **Why the switch matters:** it is the lesson the vault task names, "but
  I was delta-neutral". The same position shows a different delta under
  the two rules, and the page draws both.
- **Smile source:** the market smile, interpolated linearly. If decision
  56 builds the SVI fit, the switch reads the fitted smile instead, a
  one-line change.

**B. One volatility for every leg, set by a slider (default: at the
money).** About 5-6 h. This teaches the geometry of Greeks: gamma's sign
flipping across a butterfly's body, and theta mirroring gamma. But as
measured above, it misprices every wing, and a skewed structure (a collar,
a put spread, a strangle) starts away from its own premiums.

**C. Wait for decision 56 and use only the SVI fit.** This is A, delayed
until 56 is built. Under sticky strike the smile gives one number per leg,
and every listed strike is already quoted, so SVI adds little there. It
matters only for sticky moneyness, and a linear smile serves that until 56
lands.

**Approved: A, 2026-10-21.**

## What A builds

- **Endpoint:** `POST /api/v1/options/position-curve`.
  - Input: the capture, the legs (right, strike, expiry, side, quantity,
    premium), a date between the capture and the last expiry, and the
    volatility rule.
  - Output: on a grid of spot prices, the position's profit and its five
    aggregate Greeks, plus each leg's volatility and where it came from.
  - All of it from `pricing.black_scholes`.
- **Page:**
  - A time slider over the Payoffs chart, from the capture's date to the
    last expiry, drawing the curve before expiry over the payoff at
    expiry.
  - A Greek chart with a selector for which Greek.
  - The volatility rule switch.
  - Calendar templates (call and put), with a far-expiry choice for a
    leg.
- **Shared code:** profit at expiry stays in `payoff.ts`, and the curve
  before expiry comes from the API. At the last expiry, the two must agree
  exactly.

## Tests

- **Greeks against finite differences** for every template, as the vault
  task asks. A sign error on a short leg's gamma would otherwise look
  plausible.
- **Start and end:** at the capture date and spot, profit is minus the
  cost of crossing the spread, give or take the early-exercise premium.
  At the last expiry, the curve equals `payoff.ts`.
- **Sticky strike against sticky moneyness:** the same profit and
  volatility at the capture's spot, and a different delta away from it,
  with the sign the skew implies.
- **A calendar** at the near expiry: the near leg pays its intrinsic value
  and the far leg is priced.
- **Mutants:** every test is checked against a mutant.

## Caveats the page will state

- **American options as European:** the legs are American, priced here as
  European at volatilities with the early-exercise premium removed. Out of
  the money that premium is cents. In the money it is not: on the 32-day
  SPY smile of 2026-10-19 it is $0.22 a share at the money, $1.98 for a put
  5% in the money and $2.99 at 10% (AAPL $0.47 at 10%, TQQQ $0.04).
  (Corrected 2026-10-21: this line had said at most $0.25, which holds out
  of the money only.)
- **Older captures are slow the first time:** the Payoffs page offers all
  25 capture days, but surfaces are cached for the last 7 only. On an
  older day, the first curve before expiry waits 10-35 s while its surface
  is computed. After that it is cached.
- **A model, not a forecast:** the curve before expiry assumes the
  volatility rule chosen. Real volatility will move.
