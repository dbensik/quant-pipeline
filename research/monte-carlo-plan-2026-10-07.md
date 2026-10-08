# Monte Carlo over the backtester — plan (2026-10-07, approved)

Tier 1 of the three-tier assessment made in chat on 2026-10-07: simulation and
risk models on prices and strategy returns. Tier 2 (replace the option-pricing
stub) and tier 3 (DCF / three-statement, which needs a point-in-time
fundamentals ingest the pipeline does not have) are out of scope here.

Approved 2026-10-07 with the recommended answer to every decision:

| # | decision | approved |
|---|---|---|
| 1 | default method | stationary bootstrap, 20-day mean block, GBM beside it |
| 2 | default mode | `returns`; `prices` opt-in, capped at 1,000 paths |
| 3 | where it lives | inside `BacktestResults` |
| 4 | asset gate | served-only by default, `allow_unverified` |
| 5 | phase 0 | do it, bit identity required |
| 6 | horizon | default = history length; 1–5 years forward allowed |
| 7 | parallelism | single-threaded under `run_in_threadpool` |

## The gap, measured

A backtest returns **one** equity curve and ten KPIs. `Max Drawdown` is a point
estimate of a single historical path; nothing says how likely a worse one is,
how long recovery usually takes, or what the 95th-percentile loss over the
next month looks like. The Optimize page already draws 5,000 random *weight*
vectors (`portfolio.portfolio_optimizer.simulate_random_portfolios`, seeded
since the global-RNG bug) — that is the only Monte Carlo in the system, and it
samples allocations, never paths.

Costs measured 2026-10-07 on a 2,520-bar (10-year) synthetic daily series,
Poetry venv, M-series Mac, single thread:

| what | time | note |
|---|---|---|
| `Backtester.run`, `ma_crossover` | 239 ms | 26 trades |
| `Backtester.run`, `buy_and_hold` | 264 ms | 1 trade — **same cost**, so it is not the trading |
| `Backtester.run`, `rsi` / `trend_following` | 237 / 239 ms | 62 / 146 trades |
| `get_performance_metrics` | 0.4–1.6 ms | negligible |
| iid bootstrap, 10,000 paths × 2,520 days, numpy | 312 ms | incl. equity curves and max drawdown per path |

cProfile on one `ma_crossover` run: **0.511 s of 0.628 s (81%) is 7,560 calls to
pandas `iloc.__setitem__`** — three per bar, writing `position`, `cash` and
`holdings` into the DataFrame inside the loop (`backtester.py:88-90`). Signal
generation and the execution handler do not register. So:

- resampling a strategy's **returns** is effectively free (10k paths in 0.3 s);
- re-running the strategy on resampled **prices** costs 240 ms a path, 1,000
  paths = 4 minutes, and the whole cost is a fixable pandas pattern, not the
  simulation.

## What exists to build on

| piece | where | reuse |
|---|---|---|
| request → sync worker → JSON payload split | `api/routers/backtest.py`, `optimize.py` | copy the shape exactly |
| progress out of a worker thread | `_ProgressBridge` in `api/routers/ws.py` | unchanged; it was written for "grid search and Monte Carlo" |
| `seed` default 42, `None` = unseeded | `BacktestRequest.seed` | same convention, per-request `np.random.default_rng(seed)` |
| read-time adjusted prices | `fetch_range(adjust="total")`, 516 served assets | the input series |
| per-path KPIs incl. drawdown and duration | `analysis/performance_analyzer.py` | drawdown logic re-implemented vectorised over paths; the analyzer stays for the historical run |
| `_json_safe` (NaN/Inf → null) | `backtest.py` | needed: percentiles of an empty tail are NaN |
| saved results as JSON | `core/results.py`, Results page | unchanged; a simulation payload saves like a backtest |
| equity chart | `components/charts/EquityCurveChart.tsx` (Recharts `LineChart`) | fan chart is a sibling `AreaChart`; rows computed in a pure module per the jsdom rule |
| socket client | `api/ws.ts`, `useBacktestSocket.ts` | same accepted / progress / result / error protocol |
| DB-free router tests | `tests/api/conftest.py` `FakeRepo`, `ws_client` | single-asset, so the AAPL/BTC identical-returns trap does not apply |

## Design

A `simulation/` package of pure numpy functions: no I/O, no FastAPI imports,
every function takes an `rng: np.random.Generator`. Routers call it the way
`backtest.py` calls `Backtester`.

1. **`simulation/resample.py`** — three ways to draw `(n_paths, horizon)` daily
   simple returns from one observed series:
   - `stationary_bootstrap(returns, n_paths, horizon, mean_block_days, rng)` —
     Politis–Romano: geometric block lengths, wrap-around. **Default.** Keeps
     volatility clustering, which is what makes drawdown tails real.
   - `iid_bootstrap(...)` — shown for contrast; it destroys autocorrelation and
     understates tails, and the tests use that fact (below).
   - `gbm(mu, sigma, n_paths, horizon, rng)` — log-normal, fitted to the same
     series. A parametric baseline, reported beside the bootstrap, never alone.
2. **`simulation/paths.py`** — vectorised along axis 1: `equity_from_returns`,
   `max_drawdown`, `max_drawdown_duration`, `terminal_wealth`, and
   `bands(equity, percentiles)` → one row per step with p05/p25/p50/p75/p95.
3. **`simulation/risk.py`** — `var` and `cvar` at 1, 10 and 21 trading days
   from path returns; `prob_ruin(equity, threshold)` = share of paths whose
   minimum falls below `threshold × initial`; `prob_worse_drawdown` = share of
   paths whose max drawdown exceeds the historical one.
4. **Two modes**, both in the request:
   - **`returns`** (default) — resample the backtest's realised daily
     `returns` column (net of costs and seeded slippage). Assumes the
     strategy's return distribution is stationary and ignores that signals
     depend on the path. 10k paths, instant. Resampling starts at the first
     invested bar, so a strategy that sat in cash for its first year does not
     bootstrap a flat prefix.
   - **`prices`** — resample the *price* returns, rebuild a synthetic OHLCV
     series per path (O/H/L scaled by the same factor as Close), run the
     strategy through the real `Backtester` on each. Honest for
     path-dependent signals (every MA, RSI, breakout and trend strategy in
     the registry). Slippage seeded `seed + i` per path so the result never
     depends on execution order. Cost = N × backtester time → phase 0.
5. **Schemas** in `api/routers/simulate.py`, names unique across routers
   (`tests/api/test_openapi_contract.py` enforces it):
   `SimulationRequest` = `BacktestRequest`'s fields plus `mode`, `method`,
   `n_paths`, `horizon_days` (default = bars in history, so the fan is
   comparable to the historical curve), `block_length_days`,
   `ruin_threshold`, `include_paths` (a sampled subset, capped like
   `frontier_points`). `SimulationResponse` = echo of inputs, `historical`
   (the real run's metrics, identical to what `/backtest` returns for the same
   request), `bands`, `terminal` (percentiles, mean, P(loss)), `drawdown`
   (percentiles of depth and duration, P(worse than historical)), `risk`
   (VaR/CVaR table, P(ruin)), optional `paths`, and `caveat` — the strategy's
   own caveat plus a mode caveat (`returns` mode on a path-dependent strategy
   says so).
6. **Routes**: `POST /api/v1/simulate` (REST, tests and small N) and
   `ws /simulate` (`prices` mode publishes every 25 paths through the bridge;
   `returns` mode reports stages only).
7. **Frontend**: a Simulate section *inside* `BacktestResults`, enabled once a
   backtest has run — the simulation has no input the backtest does not
   already hold, and a second place to choose symbol, strategy and dates would
   drift. Fan chart (`AreaChart`, bands stacked p05→p95 with the historical
   curve overlaid) with rows from a pure `fanRows.ts`; a percentile table for
   terminal wealth, drawdown and VaR; Save to Results. Zustand gains only the
   mode/method toggles. Types regenerated with `npm run gen:api`.
8. **Settings** (`config/settings.py`, nothing hardcoded in the package):
   `SIM_MAX_PATHS = 20_000` (returns mode), `SIM_MAX_RERUN_PATHS = 1_000`
   (prices mode), `SIM_BLOCK_LENGTH_DAYS = 20`, `SIM_PERCENTILES = (5, 25,
   50, 75, 95)`, `SIM_RUIN_THRESHOLD = 0.5`, `SIM_VAR_HORIZONS_DAYS = (1, 10,
   21)`.
9. **Asset gate**: by default only `price_basis = 'served'` assets. A bootstrap
   over a series with an impossible move resamples that move into every
   path — the 2026-09-17 sweep found 35 of 99 crypto assets with one, and the
   11 legacy names read unadjusted stored columns. `allow_unverified: true`
   runs anyway and sets `caveat`.

## Phase 0 — move the backtester's state writes out of pandas

Measured above: 81% of a run is three `iloc` writes per bar. Accumulate
`position`, `cash` and `holdings` in preallocated numpy arrays and assign the
columns once after the loop. The loop itself stays — orders are sequential and
the execution handler owns the RNG — only the recording moves. Expected
~240 ms → ~40 ms (the profile's non-setitem remainder was 0.117 s with
profiler overhead), which was expected to make 1,000 re-run paths ~40 s instead of 4 min (measured after building: ~5 s) and
speeds up every grid search in `parameter_optimizer.py` for free.

Acceptance is **bit identity**: every saved backtest in `results/` reproduces
exactly, and the unit and API suites pass unchanged. A speed-up that moves a
KPI by 1e-12 is a bug, because saved results are the regression test.

## Phases and estimates (agent wall-clock, assuming phase 0 holds bit identity)

| phase | work | estimate |
|---|---|---|
| 0 | Backtester state writes → numpy — **done 2026-10-07**. `results/` holds no backtest (one weight optimisation only), so identity was proven against a fixture instead: 8 real symbols (MO, NVDA, SPY, AAPL, HWM, MSFT, KO, BTC-USD; 1,698–2,957 bars each, 2015→today) × 7 single-asset strategies, 56 runs, equity frame + trade log + metrics all `equals()` before and after. Per-run median **291 ms → 5 ms** (synthetic 2,520 bars: 239 → 5 ms); the only slow run left is `push_response`'s walk-forward model at 1.9 s, untouched. The estimate above said ~40 ms; the per-bar `iloc` *reads* of Close and signal were most of the remainder | 1–1.5 h |
| 1 | `simulation/` package: resample, paths, risk — **done 2026-10-07**: `resample.py` (stationary / iid / gbm, `draw` dispatcher), `paths.py` (equity, drawdown depth and duration matching the analyzer's conventions, bands), `risk.py` (horizon returns, VaR, CVaR, P(ruin), P(worse drawdown)). 25 tests: GBM terminal mean within 3 SE of exp((μ+σ²/2)T) at 40k paths; AR(1) φ=0.8 keeps lag-1 autocorrelation 0.6+ under blocks and <0.1 under iid; seed two-sided; known answers by hand for every kernel; duration cross-checked against the analyzer's `groupby` on a random curve. **11 of 11 deliberate mutations caught** (ignored block length, repeat-not-continue, log-not-simple GBM, absolute drawdown, `<=` on duration and ruin, uncarried reset, missing initial column, VaR sign, CVaR→VaR, summed horizon return). 10k paths × 2,520 days: 378 ms to draw, 965 ms for equity + drawdowns + bands | 2–3 h |
| 2 | `SimulationRequest/Response`, REST route — **done 2026-10-07**, both modes (the `prices` worker was needed for the sync function the socket reuses). `Asset.price_basis` added to the domain object (filled by `find_asset` only) for the served-only gate; fixture AAPL/MSFT are `served`, BTC-USD is the unverified case. 31 router tests, **7 of 7 router mutations caught** after four tests were added for the ones a first pass missed (flat-prefix resampling, per-path seeds, risk rows from price vs strategy returns, `prob_loss` at equality). Real MO 2015→today: `returns` 2,000 paths 124–158 ms; `prices` 200 paths 1.7 s, 1,000 paths 8.7 s (synthetic-frame build per path, not the backtester). MO buy-and-hold: historical final $233,692, simulated p05/p50/p95 $94,302 / $238,542 / $579,678, P(drawdown worse than historical) 0.44, P(ruin at 50%) 0.025. `SIM_MAX_PATHS` is 5,000 not 20,000: three (paths × horizon) float arrays are alive at once | 1.5–2 h |
| 3 | Websocket `ws /simulate` — **done 2026-10-07**. Same accepted / progress / result / error protocol; `prices` mode publishes every 25 paths from the worker through `_ProgressBridge`, `returns` mode reports stages only. Validation, the asset gate and response assembly were factored out of the REST route (`validate_simulation_request`, `gate_asset`, `build_simulation_response`) so the two cannot drift. 9 socket tests, one of which asserts the socket's bands, summaries and historical metrics equal the REST response for the same request | 1–1.5 h |
| 4 | Frontend — **done 2026-10-07**. `schema.d.ts` regenerated from `app.openapi()` (295 lines added, 0 removed); `runOverSocket` generalised so the backtest and simulation sockets share one runner; `useSimulationSocket`, `useRunSimulation`, `useSaveResult`; `SimulationPanel` under the backtest result (mode, method, paths, horizon, block length, unverified override; socket or REST following the existing Stream-progress toggle; Save to Results); `FanChart` (Recharts `ComposedChart`, two range areas + median + realised + start line); `fanRows.ts` holds the overlay alignment — step j ↔ historical bar `(bars − resampled_from) + j − 1` — with 6 tests for buy-and-hold, a waiting strategy, prices mode and the history edge; `SimulationResults` tested with the chart mocked. 212 frontend tests (was 202), typecheck clean, build keeps the Plotly chunk separate. Mode/method stayed component state rather than Zustand: they are one panel's controls, not a cross-page selection | 2.5–3.5 h |
| 5 | Real names run and recorded below — **done 2026-10-07**; CLAUDE.md (architecture entries for `simulation/` and the backtester change), CHANGELOG, README | 0.5–1 h |
| | **total** | **9–12.5 h, over 2–3 sessions** |

Your hands-on time: 20–30 minutes for the decisions below, plus a look at the
fan chart on a real name before phase 5 closes.

## Testing rules that bite here

- The response echoes `n_paths`, `seed`, `method`: do not assert on them.
- The seed test must be two-sided — same seed, identical bands; different
  seed, different bands — and the bootstrap-vs-iid test must assert the *gap*
  in retained autocorrelation, not a magic number.
- `prices` mode: path *i* seeded `seed + i`; a test that reorders path
  execution must give the same response.
- The `returns`-mode caveat on a path-dependent strategy is output; test that
  it appears for `ma_crossover` and not for `buy_and_hold`.
- jsdom renders no chart: mock the fan chart, assert on the rows `fanRows.ts`
  hands it.
- Verify each test fails against the bug it covers before committing it — the
  project's own history (CLAUDE.md, Test section) is why this line is here.

## Decisions needed

1. **Default method.** Recommend stationary bootstrap, 20-day mean block, GBM
   shown beside it. (Politis–White automatic block selection is a later
   option; statsmodels is already a dependency.)
2. **Default mode.** Recommend `returns`, instant; `prices` opt-in, capped at
   1,000 paths. Both exposed from day one so the fan charts can be compared.
3. **Where it lives.** Recommend inside `BacktestResults`, not a new page or
   nav entry.
4. **Asset gate.** Recommend served-only by default with `allow_unverified`.
5. **Phase 0.** Recommend doing it — pure speed-up with an exact acceptance
   test, and grid search benefits. The alternative is to skip it and cap
   `prices` mode at 200 paths (~50 s).
6. **Horizon.** Recommend default = history length; allow 1–5 years forward.
7. **Parallelism.** Recommend single-threaded under `run_in_threadpool` for
   now; a process pool adds pickling and lifecycle for a mode that phase 0
   already brings under a minute.

## Why not the alternatives

- **Parametric only** (GBM / normal): understates tails, which is the thing a
  risk estimate exists to show. It is in the design as the baseline that makes
  the bootstrap's fatter tail visible, not as the answer.
- **Trade-level bootstrap** (reshuffle the trade log, common in retail
  tools): loses time structure, cash drag and time-in-market, and degenerates
  on a one-trade `buy_and_hold`.
- **A new page.** No new inputs; two places to pick symbol/strategy/dates
  drift apart.
- **A library.** `quantstats` gives tearsheets, no block bootstrap;
  `vectorbt` would replace the backtester, which is tier 1's host, not its
  subject. numpy plus the existing analyzer covers it.

## Pitfalls to design around

- **Seeding** — the portfolio optimizer already learned this against the
  global numpy RNG; every draw comes from a per-request generator.
- **Survivorship** — a bootstrap resamples one asset's *own* history; it says
  nothing about assets that died. A cross-sectional simulation would inherit
  the membership problem in
  `sp500-membership-reconstruction-2026-10-04.md`.
- **Contaminated inputs reproduce themselves** — hence the served-only gate.
- **Invariants** — parameters in `config/settings.py`; unique schema class
  names; router tests through the repository Protocol; charts mocked under
  jsdom; Plotly not needed, so no new chunk.

## Real names, 2015-01-01 → 2026-10-07, seed 42 (recorded 2026-10-07)

`stationary` = block bootstrap, 20-day mean block. P(worse) = share of paths
whose max drawdown is deeper than the historical one. VaR/CVaR are losses on
the strategy's equity, so a strategy mostly in cash shows 0 — correct, and the
reason the risk table is built from strategy equity rather than price draws.

| symbol | strategy | mode / method | paths | time | historical final · max DD | simulated final p05 / p50 / p95 | DD p50 · P(worse) | VaR95 1d · 21d | CVaR99 21d | P(ruin 50%) |
|---|---|---|---|---|---|---|---|---|---|---|
| MO | buy_and_hold | returns / stationary | 2,000 | 158 ms | $233,692 · −38.7% | $94,302 / $238,542 / $579,678 | −37.5% · 0.44 | 2.27% · 9.50% | — | 2.5% |
| NVDA | buy_and_hold | returns / stationary | 2,000 | 305 ms | $50.5M · −66.3% | $4.1M / $50.9M / $641M | −54.9% · 0.14 | 4.11% · 16.3% | 30.6% | 4.0% |
| NVDA | trend_following | prices / stationary | 200 | 3.0 s | $111,317 · −35.1% | $65,009 / $101,776 / $156,105 | −23.8% · 0.18 | 0 · 0 | 0 | 2.0% |
| SPY | buy_and_hold | returns / stationary | 2,000 | 284 ms | $459,780 · −33.7% | $198,826 / $464,907 / $1,030,053 | −32.9% · 0.47 | 1.60% · 6.23% | **17.6%** | 0.2% |
| SPY | buy_and_hold | returns / **gbm** | 2,000 | 249 ms | same | $172,932 / $474,413 / $1,233,420 | −29.7% · 0.31 | 1.71% · 6.57% | **10.8%** | 0.3% |
| BTC-USD (unverified) | buy_and_hold | returns / stationary | 2,000 | 235 ms | $1.12M · −76.1% | $88,037 / $1.15M / $17.3M | −67.9% · 0.29 | 4.21% · 18.9% | 35.5% | 24.2% |
| AAPL | rsi | prices / stationary | 200 | 3.0 s | $111,599 · −6.1% | $81,888 / $101,420 / $121,959 | −10.2% · 0.86 | 0 · 0.90% | 3.45% | 0 |

The SPY pair is the plan's fat-tail claim measured: GBM puts the 21-day 99%
expected shortfall at 10.8%, the block bootstrap at 17.6%, from the same
series. GBM's wider p95 and narrower tail is the normal-returns shape.

## Learned while building

- **Phase 5 (2026-10-07).** The simulated median final value sits within 2%
  of the historical one on every buy-and-hold run (MO, NVDA, SPY, BTC), which
  is the sanity check a resampling scheme has to pass before its tails mean
  anything. `prices` mode on a trading strategy puts the historical outcome
  near the top of the fan (AAPL rsi: P(worse drawdown) 0.86, historical
  final above p75) — the realised path was a good one for that rule, which
  is exactly what a single backtest cannot tell you.

- **Phase 2 (2026-10-07).** A first set of 27 router tests passed first
  time and missed 4 of 7 deliberate mutations — every assertion was on a
  single output, none on the relation between two outputs. The tests that
  caught them tie one output to another computed separately: the step-1 p05
  band to the 1-day VaR, `prob_loss` to the returned sample paths,
  `resampled_from` to `bars`. Per-path seeding is invisible in the response
  and is observed by patching the Backtester the worker constructs.
- **Phase 0 (2026-10-07).** The per-bar pandas cost was reads as well as
  writes: pre-extracting `Close` and `signal` to numpy alongside the array
  writes took a run from 239 ms to 5 ms, not the ~40 ms the profile implied.
  `SIM_MAX_RERUN_PATHS = 1_000` is now ~5 s of worker time, so `prices` mode
  needs the websocket for progress, not for survival. `results/` turned out
  to hold no saved backtest, so the plan's "reproduce every saved result"
  acceptance was replaced by a 56-run fixture on real bars; the fixture
  script lives in the session scratchpad, not the repo, because it depends
  on the live database and today's bars.
