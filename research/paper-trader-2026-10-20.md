# A paper trader driven by a strategy (decision 50, 2026-10-20)

Decisions 48 and 49 gave backtests and paper portfolios the same short
accounting. The point of a shared ledger is this one: a strategy, attached
to a paper portfolio, trades it forward every morning under the rules its
backtest used. A real-time record, long or short, that nobody can tune in
hindsight.

## What exists

- **Paper portfolios** (decision 49): a trade log folded by
  `core.portfolio.derive_state` with splits, dividends, ^IRX interest,
  borrow and margin. `trade_refusal` is the one check for REST and gRPC.
  Trades are written by a person, on the Portfolios page or through gRPC.
- **Strategies:** `PortfolioBacktester` runs every multi-asset rule
  (allocation rules as events, `pairs_trading` as targets), fills at the
  next open and sizes on equity at that open. A single-asset strategy is a
  one-symbol basket at weight 1.0. `_build_signals` in
  `api/routers/portfolio_backtest.py` turns a strategy, its params and a
  basket into the frames the engine reads, for every `signal_shape`.
- **The signal service** (gRPC) signs single-asset positions. It is not a
  trader and is not changed here.
- **Cost of a day's signals:** `pairs_trading` on GLD/SLV over all 4,224
  bars since 2010 takes 0.49 s. One session a morning, or a missed week
  caught up, is seconds.

## The design

### Binding a strategy to a portfolio

- `PUT /portfolios/{name}/strategy` stores `{strategy, params, symbols,
  transaction_cost, attached_on}` in `metadata["strategy"]`, beside
  `short_terms`. Nothing new in the accounting path, and no migration.
- **Only on a portfolio with no trades,** so its record is the strategy's
  alone. While attached, a manual trade is 409. `DELETE` detaches: the
  positions stay and the portfolio becomes a manual one.
- A target strategy (today only `pairs_trading`) needs the portfolio's
  short terms; attaching one without them is 422 (decision 48: no
  defaults, `SHORT_*` stay empty). Crypto in a target strategy is 422, as
  in backtests.
- **Params are fixed at attach.** No re-tuning; walk-forward tuned params
  would be their own decision.
- **Forward only.** The first session traded is the first after
  `attached_on`. A portfolio cannot be attached in the past: that would be
  a backtest wearing a paper label.

### One session, the backtest's own rule

On morning D the 06:00 job has ingested bar D-1. The backtester's rule is
"signal on bar t's close, fill at bar t+1's open", so for each session s
not yet traded, up to the newest bar:

1. **Signals** from the strategy's bars through s-1 only, read as backtests
   read them (`adjust="total"`). The frame is cut at s-1 BEFORE
   `generate_signals`, so no strategy can see bar s, whatever its code. It
   always starts at the same first bar (`signal_from`, stored at attach),
   so a refit schedule counted from the frame's start (pairs: every 21
   bars) does not shift from day to day.
2. **What the signal asks for at s-1,** by the engine's rules:
   - events: 1 from flat buys weight x equity; -1 sells the holding; 2
     rebalances to the weights; 0 holds;
   - targets: a value trades to target x equity; NaN holds.
3. **The trades that close the gap** between that and what the portfolio
   actually holds (`derive_state` at s). They are sized on the portfolio's
   equity at s's open and filled at the RAW open (`adjust="none"`),
   because the paper portfolio values at raw prices and takes dividends as
   cash. Lots per asset class (`lot_size_for`). Sells run before buys, as in
   the engine.
4. **Every fill goes through `trade_refusal`.** A refused fill is not
   written; it is flagged.
5. **Maintenance**, for a target strategy: the engine's own rule, a pro
   rata cut at the next open, written as trades marked `margin`. At equity
   0 the strategy is detached and flagged. This is the "the trader covers
   inside its own rules" decision 49 left to this one. A manual portfolio
   is still never force-covered.

`metadata["strategy"]["last_session"]` records the session traded. A run
trades only the sessions after it, in order, so a second run the same
morning writes nothing, and launchd's catch-up after a sleeping Mac trades
each missed session as it would have been traded on time.

The record is one session behind by construction. On D the job fills the
orders signalled at D-2's close at D-1's open and shows the order D-1's
close asks for at D's open. Someone following along by hand could place
that order before D's open; the paper record is the same.

### Prices and costs

- **Costs:** the backtests' $1 a fill plus `transaction_cost` (10 bps,
  stored at attach), recorded in `costs`. The price written is the raw
  open itself.
- **No random slippage draw.** A seeded draw adds noise to a record, not
  information. The backtests' slippage averages 0.025%; a paper fill is
  that much more favourable. Said on the page.
- **Trades are tagged** in the existing columns: `broker = "paper-trader"`,
  and `notes` gives the strategy, the signal or target acted on, and its
  bar date.
- **A backtest of the same window will not be bit-identical, and that is
  inherent.** It reinvests dividends through adjusted prices where paper
  takes cash; it draws slippage; and a later corporate action re-adjusts
  the bars its signals see. A recorded paper trade is never rewritten for
  any of these.

### Drift, gaps and flags

- **Wanted vs held,** per ticker, in the state response and the job
  output. A gap beyond one lot after a session is flagged (exit 1 and a
  notification), never corrected silently.
- **A missing bar** for any basket symbol holds the whole session; three
  held sessions are flagged.
- **Job step** `scripts/run_paper_strategies.py`, after ingest, the rate
  and the fresh-return check, BEFORE the margin check, so the margin check
  sees today's fills. Flag name `paper-trader`. `--dry-run` writes
  nothing.
- **Nothing trades until a strategy is attached.** launchd runs the
  working tree, so the step is live the morning after the commit, but no
  portfolio has a strategy today.

### Surfaces

- REST:
  - `PUT` and `DELETE /portfolios/{name}/strategy`;
  - `POST /portfolios/{name}/run`, with `dry_run` (the default) showing
    the trades the next run would write;
  - the state adds the strategy, the last session and wanted vs held.
- `_build_signals` moves to `core/`, so the backtest router and the trader
  build signals with one function. Portfolio backtests must stay
  bit-identical across the move; the pin script checks 26 real runs.
- The page: a strategy card on the Portfolios page (the Allocation page's
  strategy and params inputs), the next-open preview, and trades marked as
  the strategy's.
- gRPC and GraphQL: `ExecuteTrade` on an attached portfolio is refused,
  like REST. There is nothing else to add there.

## Tests (each checked failing against the code it guards)

- **Parity, the headline.** Walk the trader one session at a time over a
  past year on a long-only event strategy (trend following on a 3-ETF
  basket), with the engine's slippage at 0 and no dividends, and assert its
  trade log equals `PortfolioBacktester`'s for that year: dates, sides,
  quantities and prices. Then the same for `pairs_trading` with terms. If
  they differ, the trader is wrong, not the test.
- No look-ahead: a strategy that reads the frame's last row trades on s-1,
  never s.
- Idempotent: a second run writes nothing; a missed week caught up equals
  the week run day by day.
- A refused fill is not written and is flagged; a missing bar holds the
  session.
- Attach rules: a portfolio with trades is 409, a target strategy without
  terms is 422, a manual trade on an attached portfolio is 409 on REST and
  gRPC.
- A maintenance breach cuts pro rata at the next open; ruin detaches.

**Real-data check:** a scratch portfolio attached with a test-only
`--as-of` replay over 2026-07-01 to today, beside a portfolio backtest of
the same strategy and window. The differences must be only the dividends,
the raw prices and the slippage. The portfolio is deleted afterwards, as
in decision 49.

## Not in scope

- A real broker, or intraday bars.
- A screen-driven or index universe: that still waits for delisted prices
  (Sharadar).
- Signing paper trades.

## Decision

50. **Recommend A.**

    - **A: the session-by-session trader above.** About 9-12 h of my time.
      Assumes `_build_signals` moves to `core/` with the 26 pinned runs
      unchanged, and that parity holds on the first pass. A mismatch is a
      trader bug to find, at 1-2 h more.
    - **B: replay.** Each morning, re-run `PortfolioBacktester` from
      `attached_on` and append the fills newer than the log. About 5-7 h.
      - Its fills are at adjusted prices, which `derive_state` (raw prices
        plus dividend cash) would mis-value.
      - A re-adjustment or a restated bar changes past fills in the replay.
        The log then either diverges from its own strategy or has to be
        rewritten, and decision 49 made the log a record that is never
        rewritten.
    - **C: orders only.** The page and the job show what the strategy wants
      at the next open against what is held; you record the fills. About
      3-4 h. Honest, and a start, but the record is then only as complete
      as the days someone typed it in.

**Approved: A, 2026-10-20.**

## Built (A, 2026-10-20)

- **`core/strategy_signals.build_signals`**: the signal builder, moved out
  of the portfolio backtest router unchanged. 27 real portfolio backtests
  (frames, fills, responses) are identical before and after.
- **`core/paper_trader.py`** (pure): the binding, the pending sessions, and
  `plan_session`, which applies `PortfolioBacktester`'s event or target
  rules to the paper state at the raw open. `wanted_vs_held` reports drift.
- **One finding changed the design: the session's DATE is visible.**
  - Cut strictly at the signal day, a monthly rule never traded.
    `rebalance_dates` drops the last period as possibly unfinished, so the
    31st is only known to close a month once a bar dated in the next month
    exists. The backtest, run on the whole history, always has that bar.
  - The cut frame now ends with one placeholder row dated at the session,
    carrying the signal day's own values: the exchange calendar's date,
    none of the session's prices. A test checks it holds no session data.
- **Interest from the attach date (`metadata.cash_from`).** Decision 49's
  interest started at the first trade, so cash a strategy held before its
  first entry earned nothing. That cost the pairs check $560 against its
  backtest. Attaching now sets `cash_from`, which is kept after detach, and
  every reader (REST, gRPC, the margin check, the trader) uses it. Manual
  portfolios are unchanged.
- **`core/paper_runner.py`** loads and writes. Each session's fills and its
  `last_session` go in one transaction (`record_session`), only if
  `last_session` is still what the run read. A concurrent run gets 409 and
  writes nothing.
- **REST:**
  - `PUT` and `DELETE /portfolios/{name}/strategy`;
  - `POST /portfolios/{name}/run` (`dry_run` by default);
  - `strategy` in the state;
  - a manual trade, or deleting a trade, on an attached portfolio is 409;
  - removing short terms while a shorting strategy is attached is 409.
- **gRPC:** `ExecuteTrade` on an attached portfolio is refused.
- **What may be attached:**
  - multi-asset strategies on 2-50 symbols, or a single-asset one on
    exactly 1;
  - not a basket mixing crypto with stocks or ETFs (no session has a bar for
    every symbol);
  - not `cointegrated_mean_reversion`, which needs weights the registry
    cannot default;
  - the risk manager is off (decision 32's defaults), and volatility
    targeting is not offered.
- **The 06:00 job:** `scripts/run_paper_strategies.py`, before the margin
  check. Flag name `paper-trader`.
- **The page:** a Strategy card that attaches (strategy, symbols, params,
  cost) or shows the dry run (the next fills, wanted vs held, flags), with
  "Trade now" and "Detach". The trade form and the delete buttons are
  hidden while a strategy is attached.

### Tests

- **Parity:** walked forward one session at a time, the trader makes
  exactly the backtest's fills (dates, sides, quantities, prices, costs)
  for asset_class_trend, momentum_allocation, paired_switching,
  basket_trading, ma_crossover on one symbol, and pairs_trading with
  shorts. Its final equity also equals the engine's.
- **Sessions, refusals and attach rules:** catch-up equals day by day;
  missing bars hold the trader; margin cut; ruin; refusal; drift.
- **Unit:** 19 tests. **API:** 17. **gRPC:** 1. **Page:** 4. **Accounting:** 1
  (`cash_from`).
- **21 mutants**, each caught by at least one test.

### Real-data check

Three scratch portfolios, attached as of 2026-07-01 by writing the binding
directly, were traded by the runner against the database through
2026-10-19 (76 sessions), compared with a portfolio backtest of the same
signals from the same day, then deleted. A second run wrote nothing in each.

| strategy | fills | same dates and sides | paper equity | backtest |
|---|---|---|---|---|
| asset_class_trend, weekly, 5 ETFs | 6 | yes | $98,563.50 | $98,554.83 |
| momentum_allocation, weekly, 4 ETFs | 4 | yes | $99,065.58 | $99,064.36 |
| pairs_trading GLD/SLV (shorted GLD) | 4 | yes | $101,499.36 | $101,487.70 |

- **Quantities:** two VNQ fills were 203 shares against the backtest's 205.
  The trader sizes on the raw open; the backtest sizes on an open lowered by
  later dividends.
- **Run time:** a run takes about 0.2 s a session (0.5 s for pairs), so one
  morning's session costs well under a second.

The two design changes (the session-date placeholder and `cash_from`) approved by Danny, 2026-10-20.
