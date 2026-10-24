# Measurement plan: a vol-scaled, multi-speed trend ensemble on the `target` shape

Plan date: 2026-10-23. Follows `quant-firm-oss-survey-2026-10-23.md` (pickup 1).
Nothing here has been run. Decision 65 B (Danny, 2026-10-23).

**Revised 2026-10-23 on Danny's word, before any run** (review in session): the
success rule no longer passes a worse Sharpe; a control E' (the vol-scaled sizing
with no signal) is added and E's sigma window fixed; every cell runs through the
target engine, A included as A'; nothing is registered before the verdict, so the
`shorts` flag waits for it; the flag's readers are listed in full; the harness
reproduces decision 52's figures on its own window; the look-ahead entry uses small
parameters; the turnover kill is recast so costs are not counted twice.

## What is being tested, as two separate claims

The pickup bundles two ideas, and a plan that tests them together cannot say which one
paid. They are tested apart, and each can fail on its own:

- **H1 (speeds):** an ensemble of four trend speeds beats the single 200-day rule
  (`asset_class_trend`) on the same basket.
- **H2 (sizing):** vol-scaled, continuous positions beat binary 1/N sizing of the same
  signal.

An acceptable outcome is that H2 holds and H1 does not. Then the honest change is
vol-scaled sizing added to `asset_class_trend`, not a new strategy.

## The runs: a 2x2 and two controls, on two fixed baskets

| | binary 1/N | vol-scaled |
|---|---|---|
| 200-day rule | **A** = `asset_class_trend`, re-run | **B** |
| 4-speed ensemble | **C** | **D** |

- **Control E, inverse-vol static sizing with no signal:** each sleeve at a weight
  proportional to 1/sigma_i, summing to 1, set at each monthly rebalance date from
  the same trailing 63-day sigma the cells use (known at that date's close, never a
  full-sample sigma). Vol-scaling alone cuts drawdown;
  without E a win for B or D says nothing about trend. E is the discriminator for H2,
  and B against A is the discriminator for H1: if a single vol-scaled speed captures
  nearly all of D's gain, the ensemble adds nothing.
- **Control E', the vol-scaled cells' own sizing with no signal:** w_i =
  min(sigma_target / sigma_i, 1) / N at each rebalance, f(s) = 1 always. B and D
  hold a sleeve at under 1/N whenever its sigma exceeds 10% (equity sleeves usually do),
  so part of the basket sits in cash and the drawdown falls by holding less, trend
  or not. E is always fully invested and does not control for that; E' does. H2's
  trend claim is judged against E' (below); E is reported beside it.
- **Every cell through one engine.** `asset_class_trend` is event-shaped and runs
  through `PortfolioBacktester`'s event loop with `held_weight`; the other cells are
  targets through `_run_targets`. A against C would then compare engines as well as
  signals. A' is the 200-day binary rule written as targets (1[P > SMA_200] / N);
  it is checked against A on the same rebalance dates and the same sides (entries
  and exits per sleeve), and then used in A's place in every rule below. Equity is
  NOT expected to match: A' trims each held sleeve back to 1/N at every emission,
  while A's event rule never resizes a held position and lets it drift; the month-end
  gap is reported as that rebalancing effect. A is reported for reference. If dates
  or sides disagree, stop and report: the engines disagree on a long-only basket,
  which is its own finding.
- **Existing controls, re-run:** the basket equal-weighted monthly
  (`index_rebalancing`), 60/40 SPY/AGG, SPY bought and held.
- **Baskets:** `faber5` (SPY, EFA, IEF, VNQ, DBC) and `faber7` (+ GLD, TLT), the
  decision 29 baskets, for comparability. Fixed in advance; not re-chosen on this data.
  A third, broader basket is not added: it would be chosen with 2010-2026 in view.
- **Window:** one run per cell over 2010-01 to 2026-10, statistics on 2011-2026,
  2011-2019 and 2020-2026, as decision 29 did, so no slice starts warming up.
- **Everything re-run, nothing quoted.** The 200-day rule's drawdown figures in
  `multi-asset-strategies-2026-10-15.md` (-14.0% against -26.2%) are same-close;
  `fill-timing-2026-10-15.md` restated only its Sharpe (0.45 against the basket's
  0.46; 0.51 against 0.52). A baseline that is not re-run at next-open fills in the
  same script is not in the results table.

## The signal and sizing, fixed before the first run

Zero fitted parameters. The fill-timing note found that fitting trend's window at
next-open fills picked 100 and 50 days and scored 0.32 and 0.42 in 2020-2026 against the
default's 0.57 and 0.60. Searching these parameters is how the result gets manufactured,
so every one below is set from the literature now and never searched:

- **Speeds:** lookbacks of 16, 32, 64 and 128 bars. These are the algothon's
  (`signals.csv` loses its first 16, 32, 64 and 128 rows per speed). The longest is
  shorter than the 200-day rule, so A against C confounds horizon with ensemble; that
  is deliberate (the set is taken, not chosen) and the results note names it. Its signals are raw
  k-bar returns, not vol-normalised; the slide leaves normalisation to the participant.
- **Per-speed signal:** s_k = (P_t / P_{t-k} - 1) / (sigma_t x sqrt(k)), where sigma_t
  is the standard deviation of the last 63 daily returns. Ensemble s = mean of the four.
- **Response:** f(s) = clip(s, 0, 1), the fraction of a full position, saturating at a
  one-sigma trend. Long-only: nothing below zero. No short leg: decision 47 measured
  that the trend rules' exits make poor shorts.
- **Vol-scaled weight:** w_i = f(s_i) x min(sigma_target / sigma_i, 1) / N, with
  sigma_target = 10% annualised and sigma_i the same 63-day std annualised by
  sqrt(252) (ETFs have no weekend bars, decision 38). Binary cells use w_i = 1[signal > 0] / N; the 200-day
  cells read 1[P > SMA_200] in place of f(s), vol-scaled or not.
- **Sum cap:** if the weights sum past 0.995 they are scaled down inside the strategy.
  The engine pays a long from free cash only, each fill costs $1 plus 10 bps, and an
  unaffordable fill is silently trimmed in ticker order, so a basket asking for exactly
  1.0 is short by its costs on the last buy. The 0.5% buffer is set now. The trade log
  carries no "resized" reason, so the harness checks quantities: the held dollar weight
  at each rebalance open equals the target within one lot plus that fill's costs.
- **Emission:** monthly, at `rebalance_dates`, NaN between (primary). A daily variant
  with a no-trade band (re-emit only when |w_new - w_held| > 0.05) is secondary and
  run on D only. The engine trades every difference between the wanted and held size,
  so a daily continuous target would churn lots as equity drifts, at $1 plus 10 bps a
  fill; that is why emission is a design decision and turnover a kill criterion.

The only out-of-sample view is the split decision 29 used: nothing is fitted on
2011-2019, so the 2020-2026 slice is a second sample of the same fixed rule, not a test
of a fit.

## Harness

In process through the endpoint's own helpers (`build_signals`, `PortfolioBacktester`,
`resolve_risk_free`, lots), checked equal to `POST /api/v1/backtest/portfolio` on one run,
as decision 29 did. 10 bps per fill plus $1, adverse slippage seed 1, idle cash earns
^IRX, Sharpe against that cash account, next-open fills. `max_trade_risk_pct` and
`max_portfolio_drawdown_pct` at 1.0. `enable_vol_targeting` False: `_run_targets`
raises on it, and the strategy's sizing is the vol targeting. `weights` are not sent:
the router refuses them on a target run ("sizes its own legs"), correctly. Each run's
`benchmark_report` (decision 53) supplies beta, alpha with its Newey-West t, the rolling
Sharpe and the regime-third Sharpes; no new analysis code is written for this.

## Reported on every row

CAGR, vol, Sharpe with its standard error, max drawdown, the 2020 crash
(2020-02-19 to 2020-03-23), 2022, average invested fraction, fill count, cost paid as a
fraction of final equity, and the benchmark report's alpha t. Sharpe by decade beside it.

## Pre-registered success and failure

- **"Beats", defined once:** X beats Y when X's Sharpe point estimate is higher AND
  X's max drawdown is no worse. A Sharpe difference inside one standard error is
  still reported as no difference (power, below), so a "beat" inside one standard
  error is written as "higher, not distinguishable". A lower Sharpe is never a beat,
  however close. (The first draft passed "at least as high within one standard
  error", which over 16 years, one standard error about 0.25, let a rule 0.2 worse
  hold.)
- **H1 holds** if C beats A', and D beats B, on both baskets in both decades. Fails
  if either basket or either decade says otherwise.
- **H2 holds** if B beats A', and D beats C, by the same rule, and neither vol-scaled
  cell is matched by E': B and D must each beat E' on the same basket and decade. If
  E' matches or beats a vol-scaled cell, its gain was holding less, not trend; H2
  fails as a trend claim. E is reported beside E' and decides nothing.
- **Turnover, net and gross:** every Sharpe in the table is already net of the $1
  plus 10 bps per fill, so costs are not subtracted again. Each vol-scaled cell is
  also run at zero cost; if it beats its binary cell gross but not net, H2 fails on
  cost, and the note says so.
- **Power, stated now:** 16 years holds few regimes. Decision 53 measured a regime
  third's Sharpe at about +/-0.45; a difference inside one standard error is reported as
  no difference. The two decades are two samples, not a hundred.
- **Outcome table:** H1 and H2 hold: register the ensemble with its measured caveat. H2
  only: propose vol-scaled sizing for `asset_class_trend` to Danny as a model change,
  no new strategy; if he takes it, its registry caveat and decision 52's figures are
  re-measured, since both describe the binary rule. H1 only: register the ensemble
  binary-sized. Neither: a note, and nothing registered. The 2026-08-10 assessment's
  "Time Series Momentum" item closes either way.

## Does what it says

As decision 29: holdings rebuilt from each default run's trade log and compared, on every
rebalance date, with the rule recomputed from prices. The check must fail when pointed
at the wrong parameters (speeds 8/16/32/64, sigma over 21 days), or it is vacuous.

## Build, before any cell runs

1. **The cells as unregistered strategy classes**, in `alpha_models/` (A', B, C, D, E,
   E': one class for the 2x2 with `signal` and `sizing` arguments, one for the two
   controls), `signal_shape` "target" in the harness's own spec object
   (`build_signals` reads only `spec.signal_shape`). Nothing enters
   `alpha_models/registry.py` before the verdict: under "neither" nothing is
   registered, and a registered strategy is visible to the UI, the API and paper
   attach the moment it lands. `speeds`, `vol_window`, `sigma_target` and
   `rebalance_frequency` are constructor arguments; no grid exists anywhere.
2. **Contract tests by hand.** `_multi_wide_cases` in `tests/test_strategy_contract.py`
   is a hand list and `test_wide_shapes_emit_only_directional_values` skips the target
   shape, so each class needs its own contract test like `test_pairs_trading_contract`
   and a look-ahead entry, or it is never checked. The fixtures are 300 bars with the
   first 150 ignored (`N`, `WARMUP`); a 128-bar speed plus a 63-day sigma decides
   almost nothing inside that, so the entries use small parameters (speeds 4/8/16/32,
   `vol_window` 21), as `asset_class_trend` uses `window=50` there. Part of the 3-4 h.
3. **Only if the outcome table says register: a `shorts` flag on `StrategySpec`**,
   defaulting to `signal_shape == "target"`, False for the registered strategy. The
   engine already runs all-positive targets without terms (`_run_targets` raises only
   on a negative target). Every read of the shape, classified before any is touched:
   - **needs terms, moves to the flag:** `api/routers/portfolio_backtest.py:381` (a
     run without terms is 422), `api/routers/strategies.py:97` (the `shorts` field the
     UI reads), `api/routers/portfolios.py:860` (attaching to a paper portfolio
     without terms is 422) and `api/routers/portfolios.py:777` (clearing terms while
     a target strategy is attached is 409). Left on the shape, a long-only target
     strategy could be backtested without terms but not attached to paper.
   - **classify by reading:** `api/paper_tracking.py:195` (terms passed to the
     tracking backtest only for a target strategy) and the paper runner's own terms
     check (`short_terms_of`, `core/paper_runner.py:152`).
   - **the column is `target`, stays on the shape:** `core/paper_trader.py:302, 556,
     592`, `core/paper_runner.py:258`, `core/strategy_signals.py:58`,
     `api/paper_tracking.py:105`.
   Tested: a long-only target backtest and paper attach without terms are 200,
   pairs_trading without terms is still 422 on both. 1-2 h, inside the register step.

## Effort

Agent wall-clock: harness 1-2 h, the classes and tests 3-4 h, the cells (A' check, the
2x2, E, E', a zero-cost rerun of B and D, the existing controls, two baskets) 2-3 h,
the holdings check and the note 1-2 h. 7-11 h, one working day, assuming `_run_targets`
needs no change for all-positive targets, which its code says. Plus 1-2 h for the
`shorts` flag only if the outcome registers.

## Hand-off brief for the agent that runs this plan

Repo: `~/PycharmProjects/quant-pipeline`. Interpreter:
`~/Library/Caches/pypoetry/virtualenvs/quant-pipeline-nPGsYOTA-py3.12/bin/python`
(Poetry venv; never a bare `python` or `pip`, never conda). Read first, in full:
`CLAUDE.md`, this note, then `research/multi-asset-strategies-2026-10-15.md`,
`research/fill-timing-2026-10-15.md`, `research/portfolio-equity-lag-2026-10-20.md`,
`research/short-engine-2026-10-19.md`, `research/benchmark-regime-report-2026-10-20.md`,
`_run_targets` in `backtesting/portfolio_backtester.py`, `alpha_models/pairs_trading.py`
(the only target-shape strategy), `alpha_models/asset_class_trend.py`.

Steps, in order, each with its done criterion:

1. **Harness, before any new cell.** A scratch script (uncommitted, as decision 29's
   were) running `asset_class_trend` and `index_rebalancing` on faber5 and faber7
   through the endpoint's own helpers at next-open fills, 10 bps plus $1, seed 1, ^IRX
   cash, on decision 52's window (2010-01-04 to 2026-10-14). Done: it reproduces
   decision 52's re-measured figures (`portfolio-equity-lag-2026-10-20.md`, "after")
   within 0.005 on all three slices: faber5 trend 0.449 / 0.345 / 0.580, equal weight
   0.457 / 0.499 / 0.422; faber7 trend 0.516 / 0.437 / 0.598, equal weight
   0.522 / 0.630 / 0.435. It also matches one `POST /api/v1/backtest/portfolio` run
   to the last digit, with `"seed": 1` sent (the endpoint's default is 42). STOP if
   either misses: new cells mean nothing on a harness that cannot land the old ones.
   The cells then run on 2010-01-04 to the newest bar; the window is the harness
   check's, not the study's.
2. **The classes**, unregistered, with their contract tests, look-ahead entries (small
   parameters) and the holdings check, including the wrong-parameter negative test.
   Speeds, sigma window, response, cap and sigma_target exactly as this note fixes
   them.
3. **A' against A**: the same rebalance dates and sides. STOP if those disagree. The
   month-end equity gap is reported as the rebalancing effect, not a failure.
4. **The cells:** A', B, C, D, E, E', B and D again at zero cost, the existing
   controls, both baskets, three slices, every column in "Reported on every row" plus
   gross Sharpe for B and D. Apply the pre-registered rules and the outcome table as
   written.
5. **Results note** beside this one (`research/trend-ensemble-results-<date>.md`): the
   full table, the verdict on H1 and H2 separately, the horizon confound named. Nothing
   is registered and no CLAUDE.md paragraph is written in this run: if the outcome says
   register (or, for H2 only, change `asset_class_trend`), say so and stop; that is
   Danny's call, and then the `shorts` flag above comes with it.

Stop and report instead of working around: `_run_targets` needing any change for
all-positive targets; the harness missing step 1's reproduction; A' missing A; any
existing run changing. Do not search any parameter. Do not commit, push or restart
anything: Danny says when. Report with measured figures beside each claim, no em
dashes, and say plainly what was not verified.
