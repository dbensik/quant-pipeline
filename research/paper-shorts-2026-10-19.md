# Short positions in paper portfolios (decision 49, 2026-10-19)

Decision 48 gave backtests short positions, with the accounting in
`core/ledger.py`. Danny's goal is also to paper trade them. This decides what
a paper portfolio must do for a short to be recorded and valued honestly.
Decision 50, a paper trader driven by a strategy, builds on it.

## What paper portfolios are today

- **A trade log and nothing else.** `core/portfolio.derive_state` folds the
  log into cash, positions and P&L on every read. Positions are marked at
  the latest stored close. Two paths write to it:
  - the REST router (`api/routers/portfolios.py`), used by the Portfolios
    page;
  - the gRPC `ExecuteTrade` (`services/execution_service/`), reached
    through the GraphQL gateway.

  Three portfolios exist. Only "Default Portfolio" has trades: 10 AAPL at
  $180 and 5 MSFT at $400, both on 2026-08-09.
- **A short is already possible, and accounted for wrongly.** A SELL larger
  than the holding leaves a negative quantity (the page says so), and P&L
  on it has the right sign. But:
  - its proceeds count as spendable cash, so a $100k account can short $1M
    and then buy with the proceeds;
  - it never pays borrow, and nothing compares its equity with a
    maintenance level;
  - the REST cash check covers buys only, and gRPC has no check at all.
- **Corporate actions are ignored for EVERY position, long or short.** This
  is the larger finding.
  - **Splits:** a held position keeps its pre-split quantity while its
    mark becomes the post-split close. A 4:1 split shows a 75% loss that
    did not happen.
  - **Dividends:** never credited to a long, never charged to a short.
    The Default Portfolio missed AAPL's $0.27 (ex 2026-08-10) and MSFT's
    $0.91 (ex 2026-08-20): $7.25 in all.
- **Cash earns nothing.** Every backtest credits idle cash with ^IRX (since
  2026-10-12), so a paper portfolio and a backtest of the same trades would
  disagree. Decision 50 needs them to agree.
- **The gRPC path records costs as 0.** `append_trade` hard-codes it, and
  `TradeRequest` has no field for it.
- **`ExecutionService`'s docstring says it is the "signed path"** with an
  audit log. It signs nothing and writes no audit log: only `SignalService`
  does.
- **The signal service is right to clip positions to 0..1.** Fable proposed
  allowing -1. But it runs single-asset strategies (mean reversion), and
  decision 47 kept those long-only, so a short there would be a short
  nobody measured. No change.

## The design

### Corporate actions and cash interest, for every position

`derive_state` takes the asset's corporate actions and the rate series, and
derives flows from them. Nothing new is stored: the trade log stays the only
state.
- **Splits:** on its ex-date, a held position's quantity is multiplied by
  the ratio and its average price divided by it. Value and P&L are
  unchanged.
- **Dividends:** on the ex-date, cash changes by quantity held x the amount
  per raw share. A long receives it; a short pays it. Amounts come from
  `core.price_adjustment.raw_dividends`, which already converts Yahoo's
  split-scaled amounts back to the share as it traded. That is the
  conversion the backtests use, including decision 27's same-day rule.
- **Interest:** free cash (see below) earns ^IRX by calendar day, the
  backtests' rule (`core/rates.py`).

Each flow is listed in the state response with its date, so a change in
cash can be traced to an event.

Existing portfolios change on the first read: the Default Portfolio gains
$7.25 of dividends and about two months of interest.

### Shorts, through `core/ledger.py`

- **Terms per portfolio.** Borrow rate, max gross and maintenance are
  stored in the portfolio's `metadata`, set when it is created or edited.
  Each paper portfolio can then mirror a different account. A portfolio
  with no terms cannot short: a SELL beyond the holding is 422. There are
  no defaults (decision 48), and nothing comes from the empty `SHORT_*`
  settings.
- **Collateral:** short proceeds are held against the short. Free cash is
  cash minus the shorts' market value.
- **Borrow:** charged daily on the short's value at the previous raw close,
  by calendar day, as in backtests.
- **Buying power, checked when a trade is recorded, on BOTH paths, through
  one function in `core/`:**
  - a BUY is paid from free cash;
  - a SELL that opens or adds to a short keeps gross exposure within
    `max_gross` x equity;
  - shorts are equities and ETFs only.

  `allow_overdraft` still bypasses the check when recording a trade made
  elsewhere.
- **Maintenance: FLAGGED, never forced.**
  - The state response reports gross exposure, the equity required, the
    excess or shortfall, and `below_maintenance`.
  - The 06:00 job adds a read-only check (`check_paper_margin.py`): exit 1
    and a notification for a portfolio below maintenance.
  - A paper portfolio is a record of decisions someone made, so a cover
    the user never placed is not written into it. The strategy-driven
    trader of decision 50 covers inside its own rules.

### The gRPC path

- `TradeRequest` gains `costs` (field 7, so old clients still parse), which
  is recorded.
- `ExecuteTrade` runs the same buying-power check and returns its refusal as
  `success=false`.
- The false docstring is corrected. Actually signing trades is not in scope.

### The page

The Portfolios page:
- sets a portfolio's short terms;
- shows free cash, collateral, borrow paid, dividends received and paid,
  interest, gross exposure and the maintenance status;
- lists the derived flows beside the trade log.

## Tests (each checked failing against the code it guards)

- A split mid-holding leaves value unchanged (4:1 at $400 to $100).
- A long receives a dividend and a short pays it; the amount is the raw
  one when the dividend was fetched after a later split.
- Interest accrues on free cash only.
- Borrow matches a hand computation, and a weekend charges three days.
- A BUY paid only with short proceeds is refused.
- A short past `max_gross` is refused, and so is one on a portfolio with no
  terms or on crypto.
- REST and gRPC refuse the same trade with the same reason.
- The margin flag fires at the line, and the 06:00 check exits 1.

## Decision

49. **Recommend A.**

    - **A: corporate actions, interest and shorts, on both paths** (the
      design above). About 7-9 h of my time; the estimate assumes the
      gRPC proto regenerates cleanly and `./run_pipeline.sh verify` needs
      no fixture change.
    - **B: shorts only.** Collateral, borrow, terms, buying power and the
      margin flag, without splits, dividends or interest. About 4-5 h. It
      leaves the split mismark and the missing dividends on every long, the
      larger error today, and decision 50 would have to fix them anyway.
    - **C: refuse shorts in paper portfolios** until decision 50 (a SELL
      beyond the holding is 422, on both paths). About 1 h. Honest but
      gives up the goal, and leaves the corporate-action errors in place.

## Built (A, approved 2026-10-19)

- **`core/portfolio.py`:**
  - `derive_state(..., history=, terms=)` folds a `MarketHistory` in, one
    New York day at a time. On each day, in order:
    1. the corporate actions on what was held at the previous close (a
       dividend before a split on the same ex-date);
    2. the day's trades;
    3. for days before `as_of`, the night's interest on free cash and the
       night's borrow on the shorts' previous raw close.

    A spinoff's manual factor is listed in `unhandled_actions`, not valued.
    The state adds free cash, long, short and gross value, dividends
    received and paid, interest, borrow, the flows, and maintenance
    required, excess and `below_maintenance`. Without a history it is the
    trade log alone, as before.
  - `trade_refusal`: the one check for REST and gRPC.
  - `short_terms_of(metadata)`.
- **`core/paper_history.py`:** loads the history (actions, closes read with
  `adjust="none"`, ^IRX as bond-equivalent) and the latest prices. Shared by
  the router, the gRPC store and the margin check.
- **REST (`api/routers/portfolios.py`):**
  - state, trades and rebalance all derive with the history;
  - `PUT`/`DELETE /portfolios/{name}/short-terms`, where a delete with a
    short open is 409;
  - `short_terms` on create;
  - `allow_overdraft` bypasses the amounts, never the terms.
- **gRPC:**
  - `TradeRequest.costs` (field 7), recorded;
  - `PortfolioStore` runs `trade_refusal` and derives state with the same
    history and terms;
  - its async reads run on an engine of their own (NullPool) under
    `asyncio.run`, never the module-level one;
  - the "signed path" docstrings are corrected, and the GraphQL mutation
    takes `costs`;
  - the store's own sync pricing (stored `close`) is gone, so it values
    through the same read as REST.
- **06:00 job:** `scripts/check_paper_margin.py` after the fresh-return
  check: exit 1 and a notification for a portfolio below maintenance, NOTE
  lines for an unmodelled action.
- **Page:**
  - free cash beside cash;
  - dividends received and paid, interest and borrow;
  - a table of the dividend and split flows;
  - a Short terms card, to set, change or remove them, showing the margin
    room;
  - a below-maintenance alert.
- **Tests:**
  - `tests/unit/test_paper_accounting.py` (14, worked by hand);
  - `tests/api/test_paper_portfolios.py` (10);
  - 3 gRPC store tests and 1 gRPC costs test;
  - 4 page tests.

  16 mutants were each caught by at least one test. Two existing tests
  changed meaning: a short now needs terms, and a sale that "is never
  blocked by cash" is now a sale of what is held.
- **Real data:**
  - The Default Portfolio now shows AAPL's $2.70 and MSFT's $4.55 dividends
    and $747.06 of interest since 2026-08-09 (about 4% on $96k for 71 days),
    read in 0.5 s.
  - A scratch portfolio was created and deleted again. It had terms of 1%,
    2x and 30%, and shorted 500 TLT on 2026-09-01:
    - it paid TLT's 2026-10-01 dividend ($156) and $52.53 of borrow;
    - a $320k SPY buy paid with the proceeds was refused, and so was a
      BTC short;
    - the margin check passed it.
