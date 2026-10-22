# An SVI fit and arbitrage checks for the option surfaces (decision 56, 2026-10-20)

Vault task: "Fold evsg into the pipeline as vol_surface" (brief task 8).

**There is no evsg code to fold in.** The vault note of 2026-10-19 says so:
v0.1.0 was described, but nothing exists on this Mac or on GitHub. The task
is therefore to build what evsg was meant to add, inside the pipeline:
- an SVI fit per expiry;
- butterfly and calendar no-arbitrage checks;
- a report that explains each fitted parameter.

Most of the rest of evsg's spec already exists in `pricing/`:
- **Forwards:** put-call parity, with discrete dividends.
- **Implied volatility:** inverted with the early-exercise premium removed.
- **Data:** the archived daily chains, not yfinance.
- **Pricers:** one set, with no second Black-Scholes to merge
  (`ml_models/option_pricing.py` is gone).

## Measured on the cached surfaces

The fits use every expiry with 8 or more smile points and at least 2 days
to run. That is 1,938 slices from 144 surfaces (AAPL, IWM, NVDA, QQQ, SPY
and TQQQ, 2026-09-15 to 2026-10-19). Each fit is raw SVI by least squares
on total variance, from nine starting points. The scripts (`svi_fit.py`,
`market.py`, `ssvi.py`, `starts.py`) were run from the session scratchpad,
not the repo.

**Fit quality** (root-mean-square error against our implied volatility):

| | slices | median | 90th pct |
|---|---|---|---|
| all | 1,938 | 0.49 vol pts | 1.21 |
| SPY | 395 | 0.33 | 0.60 |
| IWM | 359 | 0.27 | 0.73 |
| QQQ | 383 | 0.43 | 0.94 |
| AAPL | 271 | 0.71 | 1.29 |
| NVDA | 268 | 0.77 | 1.26 |
| TQQQ | 262 | 0.97 | 2.97 |

**Within the spread:** on the 2026-10-19 captures, 40% of 8,267 quotes fit
inside half their bid-ask spread in volatility terms. That ranges from 25%
on SPY, whose spreads are a cent wide, to 90% on TQQQ. A five-parameter
curve passes through most of a smile but not through every quote.

**Arbitrage:**
- **Butterfly:** in 227 of 1,938 fitted slices (12%), the density turns
  negative somewhere on the quoted range plus 0.1 of log-moneyness either
  side. 150 of those 227 are expiries under 14 days.
- **Calendar:** in 421 of 1,794 adjacent pairs of fits, total variance
  falls with time somewhere on their shared range. The quotes do it
  themselves in 371 pairs (21%), comparing interpolated market total
  variance. Only 170 pairs fall in both counts, so they are largely
  different pairs:

  | fits cross | quotes cross | pairs |
  |---|---|---|
  | yes | yes | 170 |
  | yes | no | 251 |
  | no | yes | 201 |
  | no | no | 1,172 |

  **The fit makes calendar arbitrage the quotes do not have in 251 pairs
  (14%), and smooths away the quotes' crossing in 201.** A report that
  only showed the fit's flags would mislead, so every flag has to say
  which side shows it.

**SSVI**, one arbitrage-free surface for all expiries (power-law φ, three
parameters), fits far worse on the same six latest captures. Its error was
2.6-6.3 vol points, against 0.4-2.1 for SVI per slice. Fitted freely, it
broke its own no-arbitrage condition on 3 of the 6. Constraining it would
fit worse still.

**Cost:** nine starting points took 0.24 s a slice. One start took 0.03 s,
and its error was no worse on any of the 81 latest slices. Three starts
(about 0.1 s a slice, 1-2 s a surface) keep a margin.

## Options

**A. Raw SVI per expiry on the surface's own volatilities, with the
arbitrage reported, not repaired (recommended).** About 6-8 h of my time.
- **Code:** `pricing/svi.py` for the fit, the analytic w', w'' and the
  density function g(k). `pricing/arbitrage.py` for the butterfly check
  (g < 0 on the quoted range) and the calendar check (total variance
  falling between adjacent expiries on their shared range).
- **Attribution:** each flag says whether the quotes show the same
  violation, so a reader can tell the market's arbitrage from the fit's.
  The checks use the market's own interpolated total variance for that.
- **Endpoint:** `GET /api/v1/options/svi` (ticker, day). It reads the
  cached surface and adds no re-pricing. Each expiry gets its five
  parameters, error, share within the spread and flags, and the response
  carries a plain-English meaning for each parameter.
- **Caching:** results are kept in process by the surface's key. The SVI
  code stays out of the surface's cache fingerprint, so none of the 144
  cached surfaces is recomputed.
- **Options page:** the fitted curve over each smile, a table of
  parameters and fit quality, and the flags.
- **Folder name:** this lives in `pricing/`, the project's one home for
  option arithmetic, not in a new `vol_surface/` package.

**B. A, plus fits forced to be free of arbitrage** (a penalty on negative
density and on crossing the previous expiry). About 4-6 h more. This would
remove the 251 crossings the fit makes on its own. But where the quotes
themselves cross, which is 21% of pairs, the fit must then leave the
quotes. That is the right curve to PRICE from, but a worse picture of
the market. It is worth doing only if decision 55's sticky-moneyness switch
needs an arbitrage-free smile, and it does not: it reads one smile at a
time.

**C. SSVI.** Rejected on the measurement above.

**D. Close the task without building.** The Greeks (decision 55) work from
the linear market smile. The vault task is then closed as "covered by
`pricing/`", and the SVI parameters and the arbitrage report are never
built. This costs nothing.

**Approved: A, 2026-10-21.**

## Tests

- **Recovery:** a smile generated from known SVI parameters is fitted back
  to them, to 1e-6.
- **Analytic derivatives:** w', w'' and g(k) match finite differences.
- **Butterfly check:** it flags a slice built to have negative density at
  a known k, and passes Gatheral's arbitrage-free example.
- **Calendar check:** it flags two crossing slices at the right k range,
  and says whether the quotes cross too.
- **Endpoint:** on the SPY fixture, one fit per expiry with 8 or more
  points; a missing capture is 404.
- **Mutants:** every test is checked against a mutant.
