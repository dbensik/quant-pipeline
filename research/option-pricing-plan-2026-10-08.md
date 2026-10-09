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

**Deviations forced by phase 2 measurements (2026-10-08).** Both serve
decisions 3 and 4 as approved; neither needed a new decision.

- *No recorded spot.* The capture fetches each expiry in its own request and
  reads spot once, so quotes and spot are seconds apart, differently per
  expiry (AAPL: parity forward 0.09% above spot same-day, 0.2% by
  mid-November, which read as a −4% dividend yield). Every expiry is priced
  off its own parity forward in the forward measure (Black-76: the BSM pricer
  with S=F, q=r). Item 5's "parity-implied forward" became the only spot.
- *De-americanised IV, discrete dividends.* On SPY, near-money puts a month
  or more out carry an early-exercise premium up to 20x the half-spread in
  vol, so BSM on mids is wrong exactly where the surface is densest. Each mid
  has the premium (American − European on one escrowed-dividend tree)
  subtracted before inversion, and the forward is solved jointly, because
  parity read from American quotes carries the same premium. And decision
  4's continuous trailing yield misprices that premium by 4.5–16x the
  half-spread around an ex-date (before one there is no dividend at all), so
  dividends are discrete, projected from `corporate_actions` (last ex-date +
  median gap, last amount); expiries within 5 days of a projected ex-date are
  flagged.

Retired by this: `ml_models/option_pricing.py` (deleted, not rewritten; the
barrier premise and the C++ hook are not coming back).

## Phases and estimates (agent wall-clock, assuming the decisions below)

| phase | work | estimate |
|---|---|---|
| 0 | Delete the stub; register TQQQ as an ETF and backfill its bars; ingest `^IRX` daily as the rate series — **done 2026-10-08**. Stub deleted (`ca47cf3`). TQQQ registered as `etf` through the API and backfilled from 2015-01-02: 2,958 bars, all with served values, 5 splits and 20 dividends; `price_basis = 'served'`; full-history returns match Yahoo's auto-adjusted series to 0.000062pp with every split day equal (`5b4b4fd`). `^IRX` went into its own table, `rate_observations` (Alembic 0009), not `assets`: over 2015→2026 it has zero volume on every row, 7 closes at or below zero (low −0.105 on 2020-03-26), 434 at or below 0.05 and 112 days moving more than 50% — it would have entered the symbol picker, backtests, the missing-day check and the dollar-volume check. Quote convention checked against Treasury's Daily Treasury Bill Rates rather than assumed: `^IRX` 3.982–4.037 vs the 13-week bank-discount close 4.00–4.05 on 2026-10-01..07 and ~0.11 below coupon-equivalent, so it is a bank-discount rate; `discount_to_bond_equivalent` reproduces Treasury's 4.10 and 4.15 from 4.00 and 4.05 (the known-answer test). `core/rates.py` stores as served, insert-only, never today's New York date, 14-day overlap; reads the value on or before a date and returns its age, stale past 7 days. Backfill: 2,957 observations 2015-01-02→2026-10-07; a re-run inserted 0. Yahoo repeats values, and not only on bond holidays: 374 of 2,957 rows equal the row before (84 in 2021, near zero rates); the longest flat run is 7 rows over 8 calendar days (0.050, 2021-10-19..27). Against Treasury's bank-discount close, some runs are genuinely flat (2024-03-13..19: 5.238 vs 5.25 throughout) and some are a stale feed (2025-10-09..14: 3.853 four rows while Treasury went 3.87, 3.86, 3.85; 2021-10: 0.050 while Treasury stepped to 0.06). Error found: up to ~0.02pp, negligible in an option price. The stale flag checks the observation DATE and cannot see a flat-lined value; a no-movement check is not built — a decision for later, recorded here. 16 unit + 2 integration tests; 8 of 8 module mutations and 2 of 2 SQL mutations caught (today stored, UTC date, 360-day BEY, 360-day continuous, silent empty fetch, no overlap, stale at the limit, look-ahead allowed; exact-date read, upsert). Daily job step added after price ingest. Took ~2 h against the 1 h estimate: the separate table, its reader and the convention check | 1 h |
| 1 | `pricing/` kernels + tests: put–call parity to 1e-10; every Greek against a central finite difference; CRR European → BSM within 1e-3 at 500 steps and American put ≥ European; MC within 3 SE of BSM at 200k paths; IV round-trips a BSM price to 1e-8 and returns NaN-with-reason outside bounds; each realised estimator recovers σ on simulated GBM; mutation checks — **done 2026-10-08**. `pricing/black_scholes.py` (price, Greeks with documented units, `implied_vol` returning NaN with a reason, `style='american'` inverting the tree), `binomial.py` (CRR, early-exercise premium separate), `monte_carlo.py` (antithetic, SE over pair averages, pathwise delta), `volatility.py` (close-to-close, Parkinson, Garman–Klass, Rogers–Satchell, Yang–Zhang, rolling, cone). Known answers: Hull's call 4.76 / put 0.81; Hull's American put 5-step 4.49, converged 4.28, European 4.08. **CRR at 500 steps does not meet 1e-3 at the money as planned**: plain N=500 is off by 2.0e-3 at K=S, flipping sign at N=501. Averaging N and N+1 (`smooth=True`, the default) gives worst 7.2e-4 on S=100, T=0.25, σ=0.2, but absolute error scales with the option's width (3.3e-3 at T=1, σ=0.5), so the test bound is relative: ≤ 1e-4 · S σ √T, measured worst 7e-5. IV round-trips σ to 1e-8 where vega > 0.01; elsewhere only the price is pinned. Bounds are dividend-discounted, and the lower bound has a 1e-10 relative tolerance: a deep-ITM American put past its exercise boundary prices at intrinsic for every σ and brentq would otherwise return an arbitrary σ (it returned 0.27 for a 0.25 input). The American search floor is σ > |r−q|√dt, below which CRR's probability leaves (0,1). Inverting BSM on an American put 10% ITM reads 27.3% for a true 25%: the defect `style='american'` exists for. Realised vol on simulated intraday GBM (390 steps/day, 4,000 days): range estimators read 3–5% low from discrete high/low; with a 20% overnight gap only close-to-close and Yang–Zhang see it; with 1%/day drift Parkinson rises 12% and Garman–Klass 4% while Rogers–Satchell and Yang–Zhang move under 1%. Timings: European IV 0.49 ms, smoothed American tree 2.5 ms, **American IV 30 ms** (≈ 90 s for one ticker-day of ~3,000 rows, 2.5 h for the archive single-threaded — phase 2 must choose: precompute, BSM where the early-exercise premium is negligible, or fewer steps), MC 200k paths 2.6 ms, BSM over 10k strikes 0.43 ms. 237 tests; **20 of 20 mutations caught on a clean worktree of the commit, each by its intended test** (d1 without q, theta sign, put delta without e^{−qT} and without −1, gamma without e^{−qT}, call rho sign, tree p on r not r−q, early exercise only at expiry, no smoothing, MC undiscounted, antithetic SE over 2N, Parkinson 4 not 4 ln 2, GK coefficient, Rogers–Satchell sign, Yang–Zhang k=1 and α=1, annualise by 365, IV bound without q, American floor removed, lower-bound tolerance removed). The first pass caught 20 too, but Yang–Zhang k=1 only through the gapped fixture's seed: without gaps YZ with k=1 equals close-to-close to the bit. A hand-computed 3-bar known answer with no wicks (Rogers–Satchell exactly 0, so the result is linear in k: 0.254530) was added and fails against k=1, α=1 and ddof=0. Phase 0's 10 rate mutants re-run the same way: 10 of 10. Learned, twice: (1) after the first in-place mutation run restored `volatility.py` and compared it clean, the file was found holding a mutant again — overwritten by an outside writer, most likely the open IDE; the next full-suite run caught it. (2) On the re-run, two same-length mutants made within one second shared a stale `.pyc`, crediting a catch to the wrong test. Mutate a detached worktree outside the project, with PYTHONDONTWRITEBYTECODE=1 and `__pycache__` purged per mutant, and record which test catches each | 3–4 h |
| 2 | `chains.py` + surface on a synthetic chain priced from BSM (recovers σ and the forward exactly) and on a 200-row SPY slice committed as a fixture; measured on the real 2026-10-07 file — **done 2026-10-08**, with two measured deviations from the design (below). `pricing/chains.py` (normalise, flags incl. `locked`, the joint forward/premium solve, de-americanised IV, smiles, ATM term structure, 25-delta skew), `pricing/dividends.py` (projected discrete dividends), a vectorised escrowed-dividend tree (`escrowed_prices`, equal to the scalar tree to 2e-15 without dividends) and a vectorised Black-76 IV (`implied_vol_black76_many`, bisection, 6,000 rows in 45 ms, equal to brentq to 2e-10). Measured on 2026-10-07: the de-americanised IV is within 0.0085 vol pts of a full American inversion (at most 0.25 of the half-spread) on the 15 largest-premium OTM quotes per ticker; SPY same-strike call/put IV gap, |k|<0.05 and T>0.05, median 0.253 → 0.024 vol pts, pairs beyond the spread 351/453 → 20/453 (AAPL 4/41 → 0, NVDA 0 → 0); the raw parity forward sits up to 10 bp below the solved one on SPY's longest expiries (2–5 passes to converge), and flips sign on the 2026-12-18 expiry that coincides with the projected ex-date (flagged `dividend_date_uncertain`). Per-expiry implied spot (F e^{-rT} + PV of dividends) spreads 13 bp on SPY, and NOT smoothly in T: about −2.6 bp to 10-16, 0 to +1.5 bp from 11-06 to 12-31, then +8.2 and +10.1 bp at the two January expiries; the same jump recurs at the same expiries on 10-08 (+7.5, +9.6), so it is structural, not fetch timing. Of the 20 SPY pairs still beyond the spread, 10 are in the January expiries (17% of their 58 pairs, ~2% elsewhere) and 6 on the 12-18 ex-date expiry (flagged). A January near-money put's premium moves +$0.08–0.10 for +0.3% on r, 2.5–4x its half-spread, and +0.3% over T≈0.27 is what an 8 bp spot jump implies — consistent with a year-end funding premium on expiries that cross 12-31, not proven. The forward is unaffected (read from parity); the premium, and so January IVs, carry the T-bill rate's error. Candidate fix, NOT built (it changes decision 1, Danny's call): a per-expiry market-implied discount factor from the slope of C − P across strikes. In the money the shortcut degrades: 1.0e-3 vol at 17% ITM on a synthetic chain; the surface is OTM-only. 2026-10-08, all six tickers: every expiry converged; priced/rows SPY 4489/5404, QQQ 4623/5244, IWM 1778/2164, TQQQ 940/1144, AAPL 732/1223, NVDA 1093/1877; implied-spot spread 9–19 bp, TQQQ 45 bp; one-month (10-30) ATM IV SPY 12.0%, QQQ 18.8%, IWM 18.3%, TQQQ 54.8%, AAPL 23.0%, NVDA 31.2%. **95 s for the six tickers, 10–30 s each**: phase 3 must cache surfaces, not compute per request. 17 tests (synthetic European chain with the recorded spot 0.2% off gives identical output; per-expiry spots; a tree-priced American chain whose raw parity is shown biased >2 bp before the solve recovers F to 2e-6 and σ to 2e-4; a 198-row real SPY fixture where >50% of near-money pairs disagree beyond the spread before correction and <15% after). Mutations on a clean worktree: 11 of 15 on the first pass; the 3 non-equivalent survivors (add-back compounded instead of discounted, the vectorised lower-bound tolerance, a one-sided ex-date window) each got a test that now fails against it; the 4th (premium not clipped at 0) is equivalent, since American ≥ European on one lattice by construction. Then two robustness fixes: `escrowed_prices` raised for the whole batch if ONE row's sigma sat below the tree floor (|r|√(T/steps)), so one bad quote could fail a ticker-day; such rows now get `below_tree_floor` and the rest price (test fails against the old code with that ValueError, and on a clean worktree against removal of the guard). **Whole archive, 108 captures (6 tickers × 18 days, 2026-09-15→10-08), each with its own day's ^IRX and the dividend history known that day: 0 files fail, 0 stale rates, 4 expiries in 4 files do not converge** (AAPL 10-05, SPY 09-16 and 09-17, TQQQ 09-21; reported in `forward_converged`). 1,924 s in all, median 12.8 s per file, max 34.9 s; SPY and QQQ ~30 s. Rows without an IV: 20,544 at or below the lower bound (in the money at intrinsic: expected), 2,255 above the upper bound and 772 outside the search bounds. Out of the money, every one is NVDA: the same 154 calls every day, all on the 2026-12-18 expiry, strikes 470 and up, quoted ~$800 against a $231 spot — price ≈ 5.6 × spot − strike, not a standard 100-share contract, most likely an adjusted deliverable left in the chain. The upper-bound check already keeps them out of the smile, and they are far from the strikes the forward is read from. Took ~5 h against 2–3 h | 2–3 h |
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
