# Short positions for multi-leg strategies (decision 48, 2026-10-19)

Danny's goal: be able to backtest and paper trade short positions. Decision
47 (`research/short-side-2026-10-19.md`) settled only that the six
single-asset strategies' exits make poor shorts, so `Backtester` stays
long-only. This proposes where shorts DO belong, how they are accounted for,
and how the first short-using strategy is proven. The design was reviewed by
a second model (Fable) reading the code; its corrections are folded in and
were checked.

## What exists today

- **Marking is already signed.** Both backtesters value a holding as
  `position * close`, and `core/portfolio.derive_state` already computes
  short P&L with the right sign (`tests/unit/test_portfolio_state.py`). The
  gap is entry logic, collateral, borrow and margin, not valuation.
- **`PortfolioBacktester` reads -1 as "exit"** and sells only a holding
  above 0, so no strategy can open a short through it. `pairs_trading`
  never holds its short leg and buys the leg that should be short when the
  spread trade closes (now in its caveat).
- **A negative weight used to open a short with no accounting.** It is 422
  since decision 47 (884ab4f).
- **Paper portfolios** (`core/portfolio.py`, REST and gRPC) are a manual
  trade log. A sale larger than the holding leaves a negative quantity and
  its proceeds count as spendable cash: no collateral, borrow or margin, and
  no dividends either way (longs don't receive them either). The REST cash
  check covers buys only; the gRPC path records costs as 0. Nothing turns a
  strategy's signals into paper trades, and the signal service clips
  positions to 0..1, so it cannot express a short.
- **Dividends are already right in backtests.** They read total-return
  prices, so a short pays the dividend implicitly through the price.
  Debiting it again from `corporate_actions` would count it twice. Only the
  paper book, marked at served prices, needs explicit dividend flows.
- **The universe for a first test is thin.** Only 12 ETFs are registered
  (AGG, DBC, EFA, GLD, IEF, IWM, QQQ, SHY, SPY, TLT, TQQQ, VNQ), mostly
  different asset classes, which are poor pair candidates. (I told Danny
  ~130 earlier; that was wrong.) Single-stock pairs among today's S&P names
  are survivor-biased in pairs' FAVOUR: the pairs that blew up are those
  with a delisted leg, which Yahoo doesn't serve. They wait for Sharadar.

## The design

### Where shorts live

`PortfolioBacktester` only. Every strategy whose edge has a short leg (pairs,
long/short factors, a hedged basket) is multi-leg and already runs there.
`Backtester` (backtest, compare, optimize, simulate) is unchanged.

### Signals: a target shape, opt-in

A strategy that shorts declares `signal_shape="target"` in the registry and
returns, per leg and bar, the position it wants as a signed fraction of
equity (NaN = unchanged). Events (1 / -1 / 0) cannot say whether a -1 from
flat means "short" or "nothing", and changing their meaning would alter
every existing run. The engine trades each leg to its target at the next
open: exposure-reducing orders first (sells of longs, covers of shorts),
then exposure-increasing ones, so a rebalance's sales fund its purchases as
today. Lots apply to the size, the sign kept apart.

Existing shapes take the existing code path untouched, with no new random
draws, so every current run is bit-identical by construction. Pinned before
and after: the 6 real baskets used at `7108a63` and the decision 29
allocation runs.

### One accounting module for backtests and paper portfolios

`core/ledger.py`, pure functions, used per bar by `PortfolioBacktester` and,
in decision 49, by `derive_state` for paper portfolios (derived from the
trade log, never stored, like everything else there):

- **Collateral.** A short sale's proceeds are held as collateral. They earn
  nothing (decision 47's sensitivity: paying interest on them was worth
  0.01-0.05 Sharpe). Only the remaining free cash earns ^IRX.
- **Borrow.** Charged daily on the short's value at the previous close, by
  calendar day, at a flat annual rate.
- **Buying power.** Gross exposure (long value plus short value) may not
  exceed `max_gross` x equity. An order that would breach it is resized,
  as an unaffordable buy is today.
- **Maintenance.** At each close, equity must be at least `maintenance` x
  gross exposure. A breach covers and sells pro rata at the next open,
  enough to restore it, logged as fills with the reason `margin`.
- **Ruin.** Equity at or below 0 stops the run, reported.

The borrow rate, `max_gross` and `maintenance` are facts about a broker the
repo cannot verify, so they are settings with no default, like `cash_accrual`
and lot sizes. A short run with any of them unset is 422; a request may
override them; the response echoes the values used. Starting points to
choose from:
- Reg T initial margin is 50%, which is `max_gross` 2.
- FINRA's minimum maintenance on a short is 30% for stocks at $5 or more;
  brokers often require more.
- Borrow on easy-to-borrow ETFs is usually well under 1% a year; check your
  broker's rate.

Crypto shorts are 422: borrow and margin on crypto work differently by
venue. Hard-to-borrow names and locates are not modelled (there is no data
source): one flat rate is an assumption, stated in every response.

### The response

Adds: borrow paid, maximum gross and net exposure, the share of time short,
margin covers (count and dates), and the settings used. The Allocation page
shows them beside the existing metrics. No other UI.

## The first strategy and its proof

### `pairs_trading`, rebuilt in place

The id stays (it is what users see), the class is rewritten to the target
shape:
- the hedge ratio is an OLS fit on the trailing `formation` bars
  (statsmodels is a dependency), re-estimated as the window rolls;
- it enters only while the trailing Engle-Granger test says the pair is
  cointegrated (p < 0.05);
- long the spread at z < -2, short at z > +2, out when z crosses 0, out on
  a stop at |z| > 4;
- dollar-neutral legs, half of the pair's capital long and half short.

The textbook defaults (formation 252, entry 2, exit 0, stop 4) are fixed in
advance, not tuned.

### The study

- **Register about 25 ETFs, insert-only from 2010, served.** These are the
  classic pair candidates, each listed before 2010 (checked on Yahoo before
  registering):
  - sector SPDRs: XLB, XLE, XLF, XLI, XLK, XLP, XLU, XLV, XLY;
  - industry funds: XOP, GDX, KRE, SMH;
  - countries: EWA, EWC, EWG, EWJ, EWU, EEM;
  - bonds and metals: LQD, HYG, TIP, SLV.

  Survivorship is mild: the sector SPDRs and major country funds all
  survived, but choosing funds that exist today is still a choice and is
  stated.
- **Pairs are chosen only from the past.** Candidate pairs are formed only
  within a group (sectors with sectors, countries with countries, bonds with
  bonds), which keeps the number of hypotheses countable; that count is
  reported. Each year the pairs most cointegrated over the prior three years
  are selected, then traded through that year. This is decision 41's
  structure (validation from 2013), in one continuous account that pays for
  each switch.
- **The benchmark is cash, not buy and hold.** Results are reported as the
  cash-account Sharpe (excess over ^IRX), with beta and correlation to SPY
  (|beta| above 0.2 is not market-neutral), and drawdown in 2020 and 2022.
- **Controls:**
  - the same pairs long-only, which isolates what the short leg adds;
  - 100 draws of random pairs from the same groups under the same rules,
    which isolates what the selection adds.
- **Success, stated before running:** a positive cash-account Sharpe after
  costs and 1% borrow over 2013-2026, |beta| below 0.2, and better than at
  least 90 of the 100 random draws.
- **The study decides only the strategy's caveat,** as for every other
  strategy (`measured()`). The engine ships either way. That is the
  capability asked for, and its tests prove it without the strategy.

### Engine tests

- Shorts off is byte-identical (the pinned baskets).
- A short on a falling series profits, net of borrow.
- Borrow matches a hand computation.
- A forced cover fires at the maintenance line and fills at the next open.
- Gross exposure never exceeds `max_gross`.
- A -1 target from flat opens a short; an event-shape -1 from flat still
  does nothing.
- A crypto short is 422.
- Each test is checked to fail against the code it guards.

## What comes after (each its own decision)

- **49: paper shorts done properly (4-6 h).**
  - A buying-power check on both paths (REST and gRPC).
  - Borrow, plus dividends on BOTH longs and shorts, derived from the trade
    log and `corporate_actions` through the ledger.
  - gRPC records real costs and checks cash, matching REST.
  - The signal service may sign -1.
  - The 06:00 job flags a paper portfolio below maintenance.
- **50: a strategy-driven paper trader (6-10 h).** The backtester run
  forward one bar a day, writing next-open fills into the trade log. That is
  paper trading a strategy, long or short, and the reason for one shared
  ledger.
- **After Sharadar:** stock pairs and long/short across the S&P, on the same
  engine. `PortfolioBacktester` loops per bar in pandas: fine for pairs, too
  slow for 500 names. A numpy refactor like `Backtester`'s (2026-10-07) is
  that decision's cost.

## Decision

48. **Recommend A.**

    - **A: engine plus proof.**
      - `core/ledger.py`, the target shape and short accounting in
        `PortfolioBacktester`.
      - Settings with no default; the response fields; the Allocation page.
      - `pairs_trading` rebuilt; the engine tests.
      - Register the ETFs (a data change: about 25 assets, insert-only).
      - The pre-registered study, written into this file with the
        strategy's caveat.

      About 14-18 h of my time, in three commits you check in turn:
      1. Engine and tests, with shorts-off results proven identical
         (6-8 h).
      2. `pairs_trading`, the API and the page (4-5 h).
      3. ETFs and the study (4-5 h).

      Estimate assumes Yahoo serves the ETFs from 2010 and no surprise in
      the pinned runs.
    - **B: measure first.** Run the study in a scratchpad prototype (as
      decisions 41 and 47 were) before building anything. Build the engine
      only if pairs clear the bar. About 4-5 h first. It answers a
      different question from yours: whether pairs work, not whether shorts
      can be tested and paper traded. A failed study would leave the
      capability unbuilt.
    - **C: engine only.** Build it with synthetic tests and no strategy or
      ETFs. About 8-10 h. The capability would ship with no consumer, and
      the first real use would find what synthetic tests missed.

## Built

### Step 1: the engine (2026-10-19, A approved)

- **`core/ledger.py`**: `ShortTerms` (borrow rate, max gross, maintenance;
  no defaults, refused if negative or if `maintenance` x `max_gross` >= 1,
  where a position opened at the cap would be cut at once), and pure
  functions for long, short and gross value, equity, free cash, borrow, room
  under the cap, and the margin cut.
- **`PortfolioBacktester._run_targets`**: a separate loop, entered only when
  every signal frame has a `target` column. Decided here, as the doc left it
  open: a target is a signed fraction of EQUITY at the open; `weights` are
  not read. Not reachable from the API until step 2.
  - Each bar: borrow and interest on free cash; then, at the open, a margin
    cut flagged at the previous close; then the targets, closing fills
    before opening ones.
  - Opening orders, long or short, go through the risk manager as if buys;
    covers and sales never do, so a drawdown halt cannot trap a short. Its
    "off" setting, 1.0, still caps one opening at 100% of equity, so a
    single target above 1 is cut to 1.
  - Equity at or below 0 with a short held is ruin: `ruined_at`, total 0
    from there on.
- **Event-shaped runs are unchanged.** 28 real runs through the API, against
  the real database, before and after:
  - 6 baskets x the 5 allocation strategies that apply, `pairs_trading` on
    the two pairs, one volatility-targeted and one with the risk caps on.
  - Response JSON, engine frame (values and columns) and trade log (6,865
    fills) are identical.
- **Tests:** `tests/unit/test_short_engine.py`, 21. Each expectation is
  worked by hand (borrow $997.26 on $100k over 364 days at 1%; a weekend
  charges 3 days; a cover of 167 shares when a short at 100 reaches 160 at
  30% maintenance). Sixteen mutants of the code, each removing one rule,
  were run on a copy of the repo. Each was caught by at least one test.
- **Real-data check:** SPY long 0.5 / TLT short 0.5, 2016-2025, never
  rebalanced: 2 fills, $5,579 borrow, gross drifting to 1.62x equity, no
  margin call, 0.02 s.
