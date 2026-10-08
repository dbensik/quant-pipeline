# Option pricing and volatility — plan (2026-10-08, approved; nothing built yet)

Tier 2 of the three-tier "financial models" assessment of 2026-10-07. Tier 1
(Monte Carlo over the backtester) shipped the same day:
`monte-carlo-plan-2026-10-07.md`. Tier 3 (DCF / three-statement) is still
blocked on a point-in-time fundamentals source and is not touched here.

Nothing is built yet. Approved 2026-10-08 with the recommended answer to every
decision (the decisions themselves, with reasoning, are at the end):

| # | decision | approved |
|---|---|---|
| 1 | risk-free rate | ingest `^IRX` daily, read as of the chain date (Yahoo serves it: 23 rows in the last month, 4.04% on 2026-10-08) |
| 2 | TQQQ | register as an ETF and backfill; confirmed absent from `assets` on 2026-10-08 while the other five capture tickers are present |
| 3 | implied vol | recompute from mids; keep Yahoo's as `vendor_iv`, never plotted |
| 4 | dividends | continuous yield from trailing 12 months in `corporate_actions`; discrete treatment flagged on straddling expiries, later |
| 5 | where it lives | a new Options page with its own nav entry |
| 6 | `ml_models/option_pricing.py` | delete |
| 7 | scope | no Heston/SABR, no barrier/Asian, no American Monte Carlo, no option strategies in the backtester |
| 8 | archive second copy | still open, not this plan's work |

The `^IRX` series is shared with tier 3 (`dcf-plan-2026-10-08.md`), which
reads it as the risk-free leg of the discount rate; it is built once, here, in
phase 0. The sequence across tiers is in `financial-models-roadmap-2026-10-08.md`.

## The gap, measured

- **The only pricer is a fake.** `ml_models/option_pricing.py` is a
  16-line `BarrierOptionStub` whose `price()` returns `max(0, 100 - strike)
  + 10 * sigma` with a comment saying "replace with actual pricing logic or
  hook into the C++ engine later". Nothing imports it.
- **There is no realised-volatility estimator.** Four strategies compute a
  rolling `std` inline for their own signal; `analysis/` has none, and nothing
  reads OHLC for a range-based estimate.
- **There is no risk-free rate.** `optimize.py` and `data_enricher.py` carry a
  hardcoded `0.02` default. No rate series is ingested.
- **But the chain archive is real and clean.** `data/option_chains/` holds 6
  tickers (SPY, QQQ, IWM, TQQQ, AAPL, NVDA) × 17 trading days (2026-09-15 →
  2026-10-07), 304,236 rows, 15 MB, no partial files, 24 columns including
  `spot`, `captured_at_utc` and `market_state`. Captured twice a weekday
  (15:45 and 17:00 ET) by the launchd agent since 2026-09-14. It has ONE copy.

Quality of the vendor fields, whole archive, measured 2026-10-08:

| field | finding | consequence |
|---|---|---|
| `impliedVolatility` (Yahoo's) | 0.7% exactly 0; 3.5% above 150%; deep-in-the-money calls (K < 0.8 S) have a **median IV of 75%** on 31,609 rows | Yahoo's IV is unusable off the money-near region. Recompute IV from mids; keep the vendor column only for comparison |
| `bid` | 11.6% of rows are zero; 77% non-zero inside 7 DTE, 93–97% beyond | a zero-bid mid is meaningless — flag and exclude from the surface |
| crossed (`ask < bid`) | 14 rows of 304k | drop with a flag |
| ATM 30-day SPY call, 2026-10-07 | 777 strike, 12.76 / 12.86, vendor IV 14.1%, spot 777.24 | the near-the-money region is good; a 10-cent-wide market at the money |

**TQQQ is captured but not a registered asset**: the five others are
`served` equities/ETFs with quarterly dividends in `corporate_actions` (SPY
trailing 12 months $7.58, ~1.0% of spot); TQQQ has no bars, so no spot
history, no realised vol and no dividend yield to price against.

scipy 1.17 is already in the environment (`norm.cdf`, `brentq`).

## What exists to build on

| piece | where | reuse |
|---|---|---|
| chain archive + capture job | `scripts/capture_option_chains.py`, `OPTION_CAPTURE_*` settings | read-only input; the plan adds nothing to the capture |
| read-time adjusted OHLC | `fetch_range(adjust="total")` | realised vol input; `split` mode for a spot series that matches strikes |
| dividends per asset | `corporate_actions` kind `dividend` | trailing-12-month continuous yield |
| GBM draw | `simulation/resample.gbm` | NOT reused for pricing — it is fitted to the series' drift; a pricer needs the risk-neutral drift. Same shape, different function |
| seeded numpy, explicit `Generator` | `simulation/` convention | same |
| request → sync worker → payload; JSON results | `api/routers/simulate.py`, `core/results.py` | copy |
| pure-row-module + mocked chart | `fanRows.ts` / `FanChart.tsx` | same split for the smile and the cone |
| analysis registry | `analysis/registry.py` `TestSpec` | model: a `PricerSpec` registry so models are listed, not hardcoded in the router |

## Design

A `pricing/` package, pure numpy/scipy, no I/O, explicit parameters:

1. **`pricing/black_scholes.py`** — Black–Scholes–Merton with continuous
   dividend yield: price, delta, gamma, vega, theta, rho, vectorised over
   arrays of strikes/expiries. `implied_vol(price, S, K, T, r, q, right)`
   by `brentq` on [1e-4, 5], returning NaN with a reason when the price is
   outside the no-arbitrage bounds (below intrinsic or above the forward
   bound) rather than a wrong number.
2. **`pricing/binomial.py`** — Cox–Ross–Rubinstein tree, European and
   American, with early-exercise premium reported separately. Every option in
   the archive is American-style (SPY/QQQ/IWM/TQQQ ETF options included), so
   the European pricer is the reference and the tree is the price.
3. **`pricing/monte_carlo.py`** — risk-neutral GBM paths with antithetic
   variates, price with standard error, pathwise delta. Exists to (a) check
   the closed form, (b) be the base for anything path-dependent later.
4. **`pricing/volatility.py`** — realised vol from OHLC: close-to-close,
   Parkinson, Garman–Klass, Yang–Zhang; a **vol cone** (min / p25 / median /
   p75 / max of rolling realised vol over 10, 20, 60, 120-day windows across
   the history) so an implied level can be read against where realised has
   been.
5. **`pricing/chains.py`** — one archived day → a normalised frame: `mid`,
   `T` in years (calendar ACT/365 to 16:00 ET on expiry), `moneyness = K/F`,
   quality flags (`zero_bid`, `crossed`, `stale_last_trade`, `zero_dte`), the
   **parity-implied forward** per expiry from the tightest put/call pair, our
   IV from the mid, and the vendor IV beside it. Then a surface: per-expiry
   smile (IV vs moneyness, flagged points excluded), ATM term structure
   (interpolated at K = F), and the 25-delta skew per expiry.
6. **`api/routers/options.py`** — `GET /options/archive` (tickers, dates,
   row counts, partial flags), `GET /options/surface?ticker&date` (smiles,
   term structure, parity forwards, quality counts, plus the realised cone
   for the same ticker from stored bars), `POST /options/price` (S, K, T,
   r, q, σ, right, style → every model's price and Greeks side by side, with
   the MC standard error), `GET /options/realised-vol?symbol&window`.
   Archive reads go through a small `ChainArchive` class the tests point at a
   temp directory, the way `ResultStore` is.
7. **Frontend: an Options page**, a new nav entry — unlike tier 1 it has its
   own inputs (ticker, archive date) and is not derived from a backtest. Three
   panels: smile per expiry (Recharts scatter+line, expiries as series), ATM
   term structure drawn over the realised-vol cone, and a pricer form that
   lists the models' prices and Greeks. Row-building in pure modules
   (`smileRows.ts`, `coneRows.ts`); charts mocked in tests.
8. **Settings**: `RISK_FREE_RATE_SOURCE`, `OPTIONS_IV_BOUNDS = (1e-4, 5.0)`,
   `OPTIONS_MIN_BID = 0.01`, `OPTIONS_EXCLUDE_DTE_BELOW = 1`,
   `REALISED_VOL_WINDOWS = (10, 20, 60, 120)`, `BINOMIAL_STEPS = 500`,
   `MC_PRICER_PATHS = 200_000`.

Retired by this: `ml_models/option_pricing.py` (deleted, not rewritten; the
barrier premise and the C++ hook are not coming back).

## Phases and estimates (agent wall-clock, assuming the decisions below)

| phase | work | estimate |
|---|---|---|
| 0 | Delete the stub; register TQQQ as an ETF and backfill its bars; ingest `^IRX` daily as the rate series (or the constant, per decision 1) | 1 h |
| 1 | `pricing/` kernels + tests: put–call parity to 1e-10; every Greek against a central finite difference; CRR European → BSM within 1e-3 at 500 steps and American put ≥ European; MC within 3 SE of BSM at 200k paths; IV round-trips a BSM price to 1e-8 and returns NaN-with-reason outside bounds; each realised estimator recovers σ on simulated GBM; mutation checks | 3–4 h |
| 2 | `chains.py` + surface on a synthetic chain priced from BSM (recovers σ and the forward exactly) and on a 200-row SPY slice committed as a fixture; measured on the real 2026-10-07 file | 2–3 h |
| 3 | Router, `PricerSpec` registry, `ChainArchive` on a temp dir, OpenAPI contract test, FakeRepo for the cone | 2–3 h |
| 4 | Options page, three panels, pure row modules, mocked charts, types regenerated | 3–4 h |
| 5 | Run on all six tickers for the latest date, record ATM IV vs realised cone here; CLAUDE.md, CHANGELOG, README | 1 h |
| | **total** | **12–16 h, over 3–4 sessions** |

Your hands-on time: 20–30 minutes for the decisions, plus a look at one smile
and one term structure on a real day before phase 5 closes.

## Decisions needed

1. **Risk-free rate.** Recommend ingesting `^IRX` (13-week T-bill, Yahoo
   serves it) as a daily series and reading the rate as of the chain date.
   The alternative, a `RISK_FREE_RATE` constant, is what exists today under
   another name and is wrong by about a point whenever nobody updates it —
   small in price, visible in the parity-implied forward.
2. **TQQQ.** Recommend registering it as an ETF so the capture has a spot
   history behind it; the alternative is dropping it from capture.
3. **Implied vol source.** Recommend recomputing from mids and keeping Yahoo's
   as `vendor_iv` for the comparison column; never plotting the vendor value.
4. **Dividends.** Recommend a continuous yield from the trailing 12 months
   in `corporate_actions`. Discrete-dividend treatment matters for AAPL/NVDA
   expiries that straddle an ex-date and is a later refinement, noted in the
   surface output as a flag on those expiries.
5. **Page, not panel.** Recommend a new Options page with its own nav entry.
6. **Delete `ml_models/option_pricing.py`.** Recommend yes.
7. **Scope line.** Recommend NO Heston/SABR calibration, no barrier or Asian
   payoffs, no American Monte Carlo (Longstaff–Schwartz), and no option
   strategies in the backtester in this tier. Each is a plan of its own once
   there is a surface to calibrate to.
8. **The archive still has one copy** (`data/` is gitignored, no Time
   Machine). Not this plan's work, but this plan makes the archive worth
   more; a second copy is a 10-minute decision that is still open from
   2026-09-14.

## Why not the alternatives

- **Keep Yahoo's IV and skip the solver.** Measured above: it is wrong by
  construction away from the money, and a surface built on it would show a
  smile that is an artefact.
- **A library** (`QuantLib`, `py_vollib`, `vollib`). QuantLib is a large C++
  dependency for four closed-form functions and a tree; `py_vollib` gives
  BSM and IV but nothing for chains, surfaces, realised vol or American
  style. The kernels here are a few hundred lines with known answers to test
  against, which is also how `simulation/` was done.
- **Load the archive into TimescaleDB first.** The capture note says "capture
  first, schema second — loading is a later, re-runnable step". Reading
  Parquet per (ticker, day) is fast at 15 MB total and keeps that order; a
  table can come when a query needs more than one day at a time.

## Pitfalls to design around

- **Post-close captures.** The 17:00 run's quotes are stale and the `spot`
  column is the capture-time price; `market_state` distinguishes them. The
  surface uses the REGULAR capture by default and says which it used.
- **0DTE rows** (expiry = trade date, T → 0) blow up any IV solver; excluded
  and counted.
- **Time to expiry** is calendar time to 16:00 ET, ACT/365, not a bar count.
- **Strikes are unadjusted**; price the spot in `split` mode, not `total`.
- **American style everywhere** — the European price is a lower bound and a
  reference, never the quoted price's model.
- **Invariants**: parameters in `config/settings.py`; unique schema class
  names; router tests without a database; charts mocked under jsdom; keep
  the pricer vectorised so the surface for 5,000 rows is one call.

## Learned while building

(empty — filled in as phases close)
