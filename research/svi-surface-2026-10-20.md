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

## Built (A), 2026-10-22

- **Code:** `pricing/svi.py` (fit, w', w'', g), `pricing/arbitrage.py`
  (checks), `core/svi_report.py` (one surface's report, kept in process
  by the surface's key), `GET /api/v1/options/svi`, and an SVI card plus
  dashed fits over the smile on the Options page. Settings are `SVI_*` in
  `config/settings.py`. None of it is in the surface's fingerprint, so no
  cached surface was recomputed.
- **The fit:** least squares on total variance from 3 starts. The bottom of
  the smile, a + b σ √(1 − ρ²), is a parameter bounded at 0, so total
  variance is never negative. The fit quality reproduces the measurement
  above: on the 1,033 October slices the median error is 0.49 vol points
  and the 90th percentile 1.19. SPY's median is 0.28 and TQQQ's 1.06. A
  fit takes 0.08 s, and a surface 0.5-2.6 s; the request then costs about
  1-2 s.
- **Re-measured with the built code** on the same 1,938 slices, 1,794 pairs:

  | | none | both | fit only | quotes only |
  |---|---|---|---|---|
  | butterfly | 1,406 | 63 | 141 | 328 |
  | calendar | 1,010 | 311 | 113 | 360 |

  **Fit butterflies:** 204 of 1,938 slices, against 227 measured above;
  the starting points differ.

  **Calendar, fit side:** 424 crossings, against 421 above.

  **Calendar, quote side:** 671 crossings, against 371 above. So the
  bolded finding above (the fit adds 251 crossings, hides 201) becomes 113
  and 360 with the built code. The conclusion, that every flag must say
  whose it is, holds more strongly. The scratch
  scripts behind 371 are gone, and no variant tried reproduces it:
  - interpolating at 401 points gives 632;
  - the union of both expiries' strikes gives 715;
  - the near expiry's strikes alone give 380.

  The quote side is interpolated mid total variance, as approved. Linear
  interpolation between sparse wing quotes crosses easily, so the quote
  side is the noisier of the two.
- **Quote side of the butterfly: a DEVIATION from A as approved, for
  Danny to confirm or reverse.** The approved text checks the quotes with
  the market's interpolated total variance. Built instead: three adjacent
  strikes of one right, bought for less than nothing at bid and ask. This
  is model-free and needs no forward. The approved method and one other
  were tried and rejected:
  - At mid vols, the approved method, 1,917 of 1,938 slices failed.
  - Mixing puts and calls across the money, by parity on our forward, gave
    credits up to $1.53 on SPY 2026-10-19. The largest was on 2026-12-18,
    the expiry whose forward sits off raw parity.

  The 391 slices with quote-side butterflies are mostly SPY, QQQ and IWM:
  one-cent credits on $1 strikes.
- **Found: 1,106 of 1,938 fits (57%) put the smile's bottom m past the
  quotes.** An equity smile often falls across its whole quoted range. When
  it does, b, ρ and m trade off against each other to draw the same curve;
  SPY 2026-12-18 fits ρ 0.97 and b 5.3. The fit is still good, but the
  parameters are not a reading of the market. Each fit therefore carries
  `bottom_quoted`, and the page greys out and marks (†) any parameters it
  should not be read from. 470 fits also have a wing steeper than Lee's
  bound of 2 beyond the quotes (`wings_within_lee`).
- **Tests:**
  - backend: `tests/unit/test_svi.py`, `tests/unit/test_svi_report.py`,
    and `/svi` in `tests/api/test_options_router.py`;
  - frontend: `sviRows.test.ts` and `OptionsPage.test.tsx`;
  - mutants, run on a copy: 14 backend and 4 frontend, all killed. Two
    backend mutants survived at first; the share-within-spread test and
    the best-start test were then tightened.

**Open, not built:**
1. If you want to read the parameters, the fit could constrain m to the
   quoted range. That fit was not measured.
2. Option B (arbitrage-free fits) is still unbuilt.
3. BUILT 2026-10-22 (below): the quote side of the calendar is tested at executable prices,
   as the butterfly's is, instead of at interpolated mids.

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

## Calendar at executable prices (measured and built 2026-10-22, open pick 3)

Question: of the quote-side calendar flags, which survive the bid-ask
spread? Measured on every cached surface: 150 captures, 6 tickers,
2026-09-15 to 2026-10-20, 1,863 adjacent pairs (more than the 1,794 above;
the archive has grown). Today's quote side flags 692 of them: total variance
from interpolated MID vols, falling from the nearer expiry to the farther.

Two executable versions, run on the same pairs:

- **Same strike, same right (model-free).** Flag a pair when the farther
  expiry's ASK is below the nearer one's BID at a strike both quote. A
  longer American option is worth at least a shorter one at the same
  strike, dividends or not, so this is a trade: buy far, sell near, for a
  credit. It needs no forward, like the quote-side butterfly.
- **Spread-aware variance.** Total variance at the farther expiry's ask vol
  below the nearer one's bid vol, at the same k. Each vol bound is mid IV
  plus or minus half the spread over vega, the linearisation
  `within_spread_share` already uses. It still depends on the forward.

| quote side | pairs flagged | of today's 692 |
|---|---|---|
| mid total variance (today) | 692 | 692 |
| spread-aware variance | 44 | 44 |
| same strike, far ask < near bid | 25 | 24 |

**About 94-96% of today's quote-side calendar flags are the spread.** SPY
2026-10-20 has 9 mid flags and 0 under either executable test.

The 25 same-strike flags are 3 cases:
- **IWM 2026-12-31 against 2027-01-15, 23 captures.** Calls at 375 and 380
  are bid 0.11 and 0.20 on the quarterly 12-31, every capture from
  2026-09-17. A bid unchanged across two weeks on a far-OTM quarterly reads
  as stale, not as a credit anyone could take. This is not checked against
  a trade print.
- **QQQ 09-24 against 09-25** (capture 09-21): the 746 and 749 calls,
  credits of $0.20 and $0.05.
- **TQQQ 09-23 against 09-25** (capture 09-21): the 79 call, $0.01.

If the quote side became the same-strike test, the 1,863 pairs would read
none 1,423, fit only 415, both 23, quotes only 2, against 1,053 / 118 / 320
/ 372 today. Almost every calendar flag would then belong to the fit.

**Recommendation, for Danny:** make the same-strike executable test the
calendar's quote side, the same rule as the butterfly's. Keep the mid
crossing beside it, renamed so it does not read as an arbitrage.

### Built (2026-10-22, on Danny's "go on 2")

- `pricing/arbitrage.quoted_calendars`: strikes both expiries quote on the
  same right where the far ask is below the near bid. That is the
  calendar's quote side and its verdict.
- The mid crossing is kept as `mid_crossing` and is never in the verdict.
  On the Options page it is a greyed "Mids cross" column, titled as not an
  arbitrage.
- `GET /options/svi` carries each credit as strike, right, near bid, far
  ask and credit, so the page names the trade instead of a range in k.
- Re-run with the built code on the same 1,863 pairs: none 1,423, fit only
  415, both 23, quotes only 2, mid crossings 692. That is exactly the table
  above. SPY 2026-10-20 reads none 8, fit only 7.
- **Tests:** 7 calendar tests in `test_svi.py` and 1 report test. These
  mutants were run on a copy, and each was caught:
  - treating ask = bid as a credit;
  - ignoring the right;
  - letting the mid crossing into the verdict.
