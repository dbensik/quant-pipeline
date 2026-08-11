# awesome-systematic-trading — what's worth taking, and a plan

Assessment date: 2026-08-10
Source: https://github.com/paperswithbacktest/awesome-systematic-trading

## What the repo actually is

A **curated index of links** — 97 libraries, 40+ strategy summaries, 55 books. It
contains **no code and no papers**. Each strategy entry is a one-paragraph
summary plus the author's own reported backtest metrics, linking out to
paperswithbacktest.com.

So there is nothing to port. The value is (a) which strategy *ideas* are worth
building here, and (b) a few library references. Every Sharpe figure below is
**the site's own unvalidated claim**, quoted as an attribute — not a ranking key
and not something to trust before reproducing.

## Three measured blockers

These decide almost everything, so I measured them rather than assuming.

### 1. The equity universe is survivorship-biased by construction

```
first_bar    | n_assets
2020-01-02   |      476   ← today's index members, backfilled
2015-01-02   |       22
```

476 of 516 equities begin on the same day. The registry is "whoever is in the
S&P 500 *today*, backfilled to 2020" — names that left the index in 2017 were
never added. **Any cross-sectional equity backtest over 2020–2026 is inflated**,
and the size of the inflation is unknown.

This session's investigation is direct evidence: the 11 dead tickers found were
only the ones that left recently enough to still be in the registry at all.

### 2. Point-in-time membership is 2 days deep and cannot be backdated

```
sp500          503 members   first_seen 2026-08-09
dow_jones       30 members   first_seen 2026-08-09
top_100_crypto 101 members   first_seen 2026-08-09
```

Per CLAUDE.md, snapshots cannot be backdated — a missed day is a permanent gap.
This is **calendar-bound, not engineering-bound**: no amount of work makes
honest cross-sectional backtests available sooner. At one snapshot per day,
meaningful depth is *years* away.

That gates roughly 15 of the ~30 equity strategies in the list, including every
high-claimed-Sharpe one: Asset Growth (0.835), Short Term Reversal (0.816),
Size (0.747), Low Volatility (0.717).

**The single highest-leverage item in this whole document is therefore not a
strategy — it is reconstructing historical membership.** Wikipedia's S&P 500
page carries a dated "selected changes" table that may allow a 2015→2026
reconstruction. It is known to be incomplete, so this is an option to
*evaluate*, not a solved path. If it works, it unlocks half the list at once.

### 3. Missing data classes

| Needed by | Have it? |
|---|---|
| Price/returns, daily OHLCV | **yes** — 1,032,218 bars, 614 symbols, 2015-01-02→2026-08-09 |
| ETFs (SPY, TLT, GLD, VNQ, IEF…) | **no** — zero ETFs registered |
| Fundamentals (asset growth, ROA, value, market cap) | **no** — `FundamentalPipeline` takes a `sqlite3.Connection`; dead code since the SQLite retirement |
| Earnings dates / surprises | no |
| Futures (term structure, roll) | no |
| Options (vol risk premium, dispersion) | no |
| Macro / rates (FED model, carry) | no |
| Intraday bars | no — daily only |

## Two entries settled by measurement, not judgement

**Overnight Seasonality in Bitcoin (claimed 0.892) — CUT, not implementable.**
Crypto trades continuously, so I tested whether a daily bar even has an
overnight gap:

| Symbol | stddev of `open[t]/close[t-1] − 1` |
|---|---|
| BTC-USD | 0.00134 |
| AAPL (control) | 0.01186 |

BTC's gap is ~9× smaller than a real equity overnight session — it is the
UTC-midnight bar boundary, i.e. microstructure noise, not a session. The
strategy needs hour-of-day resolution the data does not have. Cut it.

**Rebalancing Premium in Cryptocurrencies (claimed 0.698) — mostly already
built.** `alpha_models/basket_trading.py` is already "periodically rebalance an
equally weighted basket" and 99 crypto assets are registered. This is
*configuration*, not a new strategy — point existing code at the crypto
universe and measure. Low cost, and it should be tried before anything is
written.

## The interface has already fragmented into three shapes

Worth knowing before adding anything, because a fourth shape would be the
expensive mistake:

1. **Single-asset** — `generate_signals()` returns a `signal` column, values
   asserted to be exactly `{-1, 0, 1}` (`test_strategy_contract.py:105`).
2. **Rebalance trigger** — `basket_trading` emits `signal = 2.0`, which is
   *outside* that asserted set. It escapes the check only because the
   parametrized contract test covers `SINGLE_ASSET_STRATEGIES` and this is
   registered `input_contract="multi"`. Undocumented and untested.
3. **Per-asset position matrix** — `pairs_trading` returns one column per leg,
   no `signal` column at all. The contract test pins this and calls it
   *"an interface inconsistency worth unifying eventually"*
   (`test_strategy_contract.py:183-187`). `pairs_trading` also carries a
   registry caveat: *"not constructible from the API yet."*

Cross-sectional factors need per-asset **continuous weights** — a fourth shape.
Position sizing (vol-scaling) needs continuous values too, which is why even
Time Series Momentum can't be added faithfully today: the `{-1,0,1}` assertion
forbids the vol-scaled sizing that distinguishes it from the existing
`trend_following` (a plain price-vs-50-day-MA rule). Without vol scaling it is
a near-duplicate of what exists, and a near-duplicate has negative value.

## Plan

Ordered by **feasibility × marginal newness**, not by claimed Sharpe — the
correlation runs the wrong way here, since the highest claimed Sharpes are the
least feasible.

### Phase 0 — Register ETFs (~30 min, unblocks Phase 1)

Add SPY, TLT, IEF, SHY, GLD, DBC, VNQ, QQQ, IWM, EFA, AGG through the existing
`POST /api/v1/ingest/assets` + ingest path. No new code.

Why first: it is the cheapest unlock in the document, and **ETFs have no
survivorship problem** — they don't leave an index. This sidesteps Blocker 1
entirely for everything in Phase 1.

Verify: 11 assets with bars back to at least 2015, no gaps at the 2020 boundary.

### Phase 1 — Asset-allocation strategies (the honest sweet spot)

Fixed small baskets of ETFs. No cross-sectional ranking, so no PIT membership
needed, and no survivorship bias.

| Strategy | Claimed Sharpe | Shape |
|---|---|---|
| Paired Switching (equities↔bonds on trailing return) | 0.691 | 2 assets, quarterly |
| Asset Class Trend-Following (10-month SMA per sleeve) | 0.502 | ~5 assets, monthly |
| Momentum Asset Allocation | 0.321 | ~5 assets, monthly |

These are genuinely new capability — the pipeline has no multi-asset allocation
strategy today (`basket_trading` rebalances to *fixed* weights; it does not
*choose* them). They need the per-asset weights contract, which is Phase 2.

### Phase 2 — Ratify the multi-asset contract (the real prerequisite)

> **AMENDED 2026-08-10, after building it.** This section originally proposed
> unifying the shapes into one contract with *continuous* weights in `[-1, 1]`.
> That turned out to be both unnecessary and expensive, and was not built. See
> "What was actually built" below. The original text is kept above the line
> because the reasoning that replaced it is the useful part.

**Why continuous weights were wrong.** All three Phase 1 strategies need
per-asset **±1**, not fractions: Paired Switching holds SPY *or* TLT; Asset
Class Trend-Following is in-or-out per sleeve on a 10-month SMA; Momentum Asset
Allocation holds the top N equally. Equal-weight sizing already comes from the
caller's `weights` field, which defaults to equal-weight.

**And it would have been expensive.** `weights` is a published API request
field with validation, a response echo and tests; `PortfolioBacktester`
interprets `1` / `-1` / `2` with different sizing paths. Making strategies emit
weights means rewriting the execution loop and changing API semantics — a
rewrite, not a refactor.

**What was actually built.** The four real shapes are now *named and declared*
rather than unified:

| shape | in | out |
|---|---|---|
| `per_symbol` | one symbol's frame | one `signal` column, `{-1,0,1}` |
| `wide_per_asset` | wide close frame | one position column per asset |
| `wide_portfolio` | wide close frame | one `signal` column for the basket |
| `calendar_shared` | DatetimeIndex only | rebalance schedule, `signal == 2` |

`signal_shape` on `StrategySpec` replaces the hardcoded strategy-id sets that
`_build_signals` dispatched on. **That was the actual Phase 1 blocker**, and it
failed unsafely: an unlisted id fell through to the per-symbol branch and got
each symbol's frame in isolation, so a cross-asset strategy would silently
compare nothing and still return plausible numbers.

Also done: the contract is documented on `BaseAlphaModel`; the **look-ahead
check now runs against multi-asset strategies**, which it never had (all four
pass, and the check was verified to catch a deliberately-peeking strategy); and
`signal == 2` is pinned by an explicit test instead of being untested because it
fell outside the single-asset assertion.

A Phase 1 strategy now declares `signal_shape="wide_per_asset"` and needs no
router change.

### Phase 3 — Crypto rebalancing premium (configuration, not code)

Point `basket_trading` at the 99 crypto assets, measure against buy-and-hold
BTC. Cheap. If the premium doesn't reproduce, that is a finding worth keeping.

### Phase 4 — BLOCKED: cross-sectional equity factors

Short Term Reversal, Low Volatility, Size, Asset Growth, Value, and ~10 others.

**Blocked on Blocker 2 (calendar) and Blocker 1 (survivorship), not on effort.**
The fundamentals-based ones are additionally blocked on Blocker 3 — there is no
fundamentals store at all, and reviving `FundamentalPipeline` means writing a
TimescaleDB path plus a point-in-time fundamentals model (restatements make
naive fundamentals look-ahead-biased in the same way stale membership does).

The action here is **not** to build strategies. It is to decide whether the
Wikipedia membership reconstruction is viable. That decision gates everything
in this phase.

### Explicitly not recommended

- Anything requiring futures, options, or macro data — 4 of the negative-Sharpe
  entries live here anyway (WTI/BRENT −0.199, Currency Value −0.103, Futures
  Short Term Reversal −0.05, Currency Momentum −0.01).
- Time Series Momentum until Phase 2 lands — a near-duplicate of
  `trend_following` without vol-scaled sizing.
- Overnight Seasonality in Bitcoin — measured as not implementable (above).

## Library references worth a look, separately from strategies

The repo's tooling section is arguably more immediately useful than its
strategy list, given the blockers:

- **Portfolio/risk analytics libraries** — the pipeline computes its own KPIs
  (`backtesting/backtester.py`); a cross-check against an established library
  would validate them.
- **`ml_random_forest` is currently look-ahead-biased** — trained on full
  history, marked `xfail(strict=True)` in the contract test. The repo's
  ML/timeseries section is the natural place to look for a walk-forward
  reference implementation. Fixing an *invalid existing* strategy likely beats
  adding a new one.

## Bottom line

The list's headline strategies are its least usable ones here. The three items
with the best effort-to-value ratio are: **register ETFs** (30 min, unblocks
real capability), **ratify the multi-asset contract** (pays down flagged debt
and gates everything else), and **evaluate historical membership
reconstruction** (gates half the list, and is a research question rather than a
build). Two entries were cut on measurement, and one turned out to be
configuration of code that already exists.
