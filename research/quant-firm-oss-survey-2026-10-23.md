# Twenty quant firms' public GitHub orgs: what is worth taking, and where the line runs

Assessment date: 2026-10-23
Source: the r/quant "Open Source Leaderboard" (kadoa.com/quant/open-source), 20 orgs
Method: every non-fork, non-archived public repo in each org listed by stars and last
push (`gh api orgs/<org>/repos`), then the README of each repo that touches anything this
pipeline does. The question asked: what to incorporate that is usable for real trading
and not already built by a million people.

## The finding

**None of these orgs publishes a trading idea.** The 20 orgs' top repos are an OCaml
standard library (Jane Street), a DataFrame visualiser (Man), Jupyter extensions (Two
Sigma, D. E. Shaw), a .NET Consul client (G-Research), an exabyte filesystem (XTX), Nix
build tooling (PDT), SystemVerilog language servers (Hudson River, IMC). Three orgs
(WorldQuant, AQR, Wellington) hold only forks of other people's repos and have no code of
their own at all; the leaderboard counts the forks' stars.

The firms open-source what is not their edge: storage, streaming, notebooks, build
systems, test tooling. The one item with trading content, Man Group's 2026 Imperial
hackathon, is a teaching exercise whose slides are textbook trend momentum, Markowitz with
a long-only constraint, and a news-sentiment z-score.

So the line Danny asked about is not drawn by these repos. It is drawn by this pipeline's
own measurements, which already cover the "million people" side:

| What a million people have built | Measured here |
|---|---|
| Single-asset technical rules at defaults | Beat buy and hold on at most 26% of 428 names (rsi, the best), and which names does not persist (decision 28) |
| The same rules, tuned | Walk-forward beats buy and hold on 7-30% of names, the same as the defaults (decision 41) |
| Pairs trading on ETFs | Sharpe -0.27 against cash, beats 50 of 100 random pair sets (decision 48) |
| Allocation rules on ETF baskets vs SPY | No alpha told from zero, 2011-2026 (decision 53) |

The commodity strategies have been run honestly and found empty. What the pipeline has
that the million people do not is **data nobody else bothers to keep**: a daily option
chain archive with SVI fits and arbitrage flags per capture day, an insert-only Form 4
table, a point-in-time headline archive with fetch provenance, reconstructed S&P
membership, SEC facts with instance-level gap fill, fill timing and costs that make a
backtest believable. That is the moat, and these repos do not move it. They do say
something about how the firms work, and three of them fit a gap.

## Three pickups, ranked by fit

### 1. The algothon's signal shape: multi-speed, vol-normalised trend (Man Group)

`man-group/man-imperial-algothon-2026` ships `signals.csv` with four trend speeds per
instrument (`trend4`, `trend8`, `trend16`, `trend32`), 10 anonymised instruments, the
full Treasury curve from 1990, and asks for long-only weights summing to 1 that maximise
out-of-sample Sharpe. The slides give the sizing rule explicitly: momentum return over
lookback k, divided by the asset's volatility, position = sign(s) times f(s).

What is new to this pipeline is not trend following (`trend_following`,
`asset_class_trend` exist) but **the ensemble of speeds and the vol-scaled, continuous
position**. The 2026-08-10 assessment found the `{-1, 0, 1}` contract forbade that sizing;
since decision 47 the `target` shape (a signed fraction of equity per asset, NaN =
unchanged) makes it expressible without a contract change. One wrinkle: the registry
says a `target` run "needs borrow and margin terms", so a long-only target strategy would
be refused without short terms as the code stands. The fix is small (terms needed only
when a target goes negative) and should be made before the strategy, not inside it.

Why this and not another rule: trend on fixed ETF baskets is the one family that showed
any value here (drawdown cut at about its Sharpe, decision 32). Vol-scaling is what AHL
has done since the 1980s and what `trend_following`'s in-or-out rule lacks. Measure it
the way decision 53 measures everything: against SPY and the cash account, with the
random-switch control from the Regime idea note.

Cost: the term-relaxation 1-2 h, the strategy 3-4 h, measurement an afternoon. Agent
wall-clock, assuming `_run_targets` needs no change for all-positive targets.

### 2. `G-Research/ahocorasick_rs` for the headline archive

The archive stores every symbol's 50 newest stories, insert-only, with Yahoo's tags as
served, and CLAUDE.md says "nothing reads the archive yet" and "nothing scores sentiment
yet". Relevance is a read-time rule, and today it leans on Yahoo's own tagging.

Aho-Corasick finds every one of several thousand patterns in a string in one pass. With
the registry's ~580 symbols plus their verified names and the `NEWS_SEARCH_TERMS` terms
as patterns, one pass over each story's title tags it with every company it names,
independently of Yahoo. That gives two things the archive cannot answer now: which
stories mention a company Yahoo did not tag (the MEASURED problem with KEY, HASBRO and
the coins), and co-mentions (a story naming two registry names is a pair-event, which no
price series shows). Sentiment is the algothon's z-score idea and is commodity; the point
is that commodity scoring on a non-commodity, point-in-time archive is where the moat
compounds. Score at read time, never stored, like price adjustment.

It is a Rust wheel on PyPI (`ahocorasick-rs`), stdlib-free, one class. Cost: a tagger
over the archive with a count of newly-tagged stories per symbol, 2-3 h agent time; the
scoring rule is a separate decision.

### 3. Portfolio construction: `blackrock/lcso` and `blackrock/HOLA`, second tier

The weights optimizer scores 5,000 random weightings as cost-free daily-rebalanced
backtests. That is a search, not a solve. `lcso` is an ADMM solver for a quadratic with
per-asset piecewise-quadratic penalties and linear constraints: exactly mean-variance with
a transaction-cost penalty and position bounds, which is the algothon's slide 4. It is
Rust with no Python binding in the README, so taking it means binding it or taking the
formulation and solving it in cvxpy, which a million people have done.

`HOLA` is an ask/tell black-box optimizer with a Rust engine. It would make the parameter
grid faster. Do not take it for that: decision 41 measured that tuning this parameter
space finds nothing that survives walk-forward, so a better search of it is a faster road
to the same overfit. Its only honest use here is the same walk-forward frame, with the
search replacing the grid, and the gain is seconds.

## What was read and set aside

- **`Point72/csp`** (reactive stream processing, simulation/realtime switch): the right
  tool for a live strategy on streaming data. This pipeline has daily bars and a 06:00
  job; the paper trader already walks sessions one at a time. Revisit if intraday data
  ever arrives.
- **`Point72/gamma-lake`** (Delta Lake feature store, 7 stars, new): point-in-time
  versioned features. The one ML strategy here is a walk-forward random forest whose
  features are computed in the strategy. A feature store earns its place at the second
  ML model, not the first.
- **`man-group/ArcticDB`**: a versioned DataFrame store with time travel. Production use
  needs a paid licence, and TimescaleDB with insert-only writes already gives the
  property that matters (a stored bar is never rewritten).
- **`deshaw/versioned-hdf5`, `twosigma/memento`**: the same discipline (versioned,
  reproducible data with provenance) in other containers. The pipeline's
  `news_fetches` rows and `fetched_at` columns are that discipline already.
- **`man-group/notebooker`, `PyBloqs`**: scheduled notebooks and reports. The research
  notes here are markdown with the measurement beside the claim; a report generator
  would add a format, not evidence.
- **`G-Research/tdigest-rs`**: streaming quantiles. Nothing here is too big for numpy.
- **`hudson-trading/inside-at-hrt`**: an order-book builder exercise. No order-book data.
- **`JumpCrypto/crypto-reading-list`**: a reading list; `TradingDynamics.md` and `MEV.md`
  are the two pages relevant to crypto execution, for reading, not code.
- **`deshaw/pyflyby`, `jupyterlab-execute-time`, `twosigma/marbles`**: developer
  tooling. Fine, unrelated to the pipeline.
- **`blackrock/aladdinsdk`**: needs an Aladdin subscription.
- Everything else: compilers, filesystems, hardware, Kubernetes, build systems.

## What changed since the 2026-08-10 assessment

That note's three blockers have moved. ETFs are registered (Phase 0 done). Membership is
reconstructed to 2014-12, so the calendar blocker is gone and the cross-sectional phase
is blocked on delisted prices instead (its 2026-10-04 amendment). The fundamentals store
exists (SEC facts, insiders, the DCF). Its Phase 1 allocation rules were built and
measured, and the measurement is the table above. Its "Time Series Momentum until Phase 2
lands" item is pickup 1 here, now unblocked by the `target` shape.

## Bottom line

The leaderboard answers the question by what it leaves out: the firms publish plumbing,
and their one trading artefact is a student exercise. The pipeline already knows the
commodity strategies are empty, by measurement, and already holds the data assets the
commodity builders skip. Two pickups fit that: a vol-scaled multi-speed trend ensemble on
the `target` shape (the one family with measured value here, now expressible), and
Aho-Corasick tagging over the headline archive (commodity scoring on an archive nobody
else keeps). The constrained-optimizer item is real but is a formulation to adopt, not a
repo to take. Nothing here should be built before its measurement plan is written.
