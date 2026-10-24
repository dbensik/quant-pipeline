# Results: a vol-scaled, multi-speed trend ensemble on the `target` shape (decision 65 B)

Run 2026-10-23, from `trend-ensemble-plan-2026-10-23.md` (revised 2026-10-23, before any run).
The step 3 check was corrected by the orchestrating session before it ran: A' against A is
judged on rebalance dates and sides only, and the month-end equity gap is reported as the
rebalancing effect, not a stop. Nothing is registered and no CLAUDE.md paragraph is written. Only this note is
committed (2026-10-24); the code below is not.

**Verdict: neither H1 nor H2 holds.** By the outcome table: a note, and nothing registered.
The 2026-08-10 assessment's "Time Series Momentum" item closes. The `shorts` flag is not
needed.

## What was run

- **Cells:** `alpha_models/trend_ensemble.py`, unregistered. `TrendSizingStrategy(signal, sizing)`
  is the 2x2 (A' = sma/binary, B = sma/vol, C = ensemble/binary, D = ensemble/vol);
  `SizingControlStrategy(sizing)` is E (`inverse_vol`) and E' (`vol_scaled`). Every
  parameter as the plan fixes it: speeds 16/32/64/128, sigma over 63 daily returns,
  f(s) = clip(s, 0, 1), sigma_target 10% (sigma x sqrt(252)), SMA 200, cap 0.995,
  monthly emission at `rebalance_dates`, NaN between and NaN while a sleeve warms up.
  No parameter was searched.
- **Harness** (session scratchpad `d65b-k7q2/`, not committed, as decision 29's were):
  bars through `TimescaleMarketDataRepo.fetch_range`, ^IRX through `resolve_risk_free`,
  signals through `build_signals`, `PortfolioBacktester` with the risk caps at 1.0, seed 1,
  10 bps plus $1 a fill, next-open fills, idle cash at ^IRX. Target cells through
  `_run_targets`, which needed no change for all-positive targets.
- **Window:** 2010-01-04 to the newest bar (2026-10-22). Statistics on 2011-2026,
  2011-2019 and 2020-2026. A slice's metrics come from `window_metrics` with the base at
  the slice's first bar (the convention that reproduces decision 52; see below).
- **Sharpe standard error:** the iid formula in `analysis/benchmark_report._sharpe_and_error`,
  sqrt((1 + SR_bar^2 / 2) / n x 252), applied to the reported Sharpe. About 0.25 over
  2011-2026, 0.33 over 2011-2019, 0.38 over 2020-2026.
- **Alpha t and beta:** `benchmark_report` against SPY on each slice's curve.
- **Cost paid:** the trade log's `commission` ($1 plus 10 bps) over final equity.
  Slippage is inside `fill_price` and is not in that column.
- **Gross runs:** commission 0 and fee 0, slippage kept (seed 1). A' and C were run
  gross as well, since the turnover rule compares each vol-scaled cell with its binary
  cell gross.
- **SPY buy and hold:** the single-asset `Backtester` with `buy_and_hold`, the same
  accrual, seed 1, 10 bps plus $1. The portfolio endpoint refuses one symbol.

## Checks before the cells

### Step 1, the harness reproduces decision 52 (passed)

On decision 52's window (2010-01-04 to 2026-10-14), Sharpe 2011-26 / 2011-19 / 2020-26:

| run | measured | decision 52 "after" |
|---|---|---|
| faber5 trend | 0.4489 / 0.3448 / 0.5800 | 0.449 / 0.345 / 0.580 |
| faber5 equal weight | 0.4570 / 0.4992 / 0.4219 | 0.457 / 0.499 / 0.422 |
| faber7 trend | 0.5162 / 0.4375 / 0.5987 | 0.516 / 0.437 / 0.598 |
| faber7 equal weight | 0.5217 / 0.6301 / 0.4350 | 0.522 / 0.630 / 0.435 |

All twelve within 0.0005. The first attempt put the slice base at the bar BEFORE the
slice and missed (faber5 trend 2011-19 0.3546, faber5 equal weight 2011-19 0.5079); the
base at the slice's first bar lands every figure, so that is the convention used
throughout.

`POST /api/v1/backtest/portfolio` (faber5, `asset_class_trend`, same window, `"seed": 1`)
against the in-process run: all ten metrics identical to the last digit (Final Value
230314.85966118198, Sharpe 0.45568730849184824, Max Drawdown -0.15322884979569165, 155
trades), all 4,221 equity points identical, and the response's `benchmark_report` (beta
0.28275395725220137, alpha_t 0.031192663431582952, R^2 0.38946483147572963) equal to the
harness's own `benchmark_report` call.

### Step 3, A' against A (passed on dates and sides)

| | faber5 | faber7 |
|---|---|---|
| entries and exits, A / A' | 155 / 155 | 223 / 223 |
| identical (date, sleeve, side) | yes | yes |
| A' emitted only on rebalance dates | yes (192 of 201; 9 before the 200-day warm-up) | yes |
| fills, A / A' | 155 / 695 | 223 / 919 |
| A' re-weighting fills (not entries or exits) | 540 | 696 |
| largest month-end equity gap, A' vs A | 7.9% | 9.5% |
| final equity, A' vs A | -7.5% | -7.7% |
| Sharpe 2011-26, A / A' / A' gross | 0.449 / 0.414 / 0.447 | 0.516 / 0.495 / 0.538 |

The gap is the rebalancing effect the correction names: A buys 1/N on entry and lets a
held sleeve drift; A' trades every held sleeve back to 1/N (0.995/N when all are held) at
each month end. Most of the Sharpe difference is the cost of those trims: A' gross is
within 0.002 of A net on faber5, and above it on faber7.

### Holdings check (passed, and not vacuous)

For each default target run (A', B, C, D, E, E', both baskets), holdings were rebuilt from
the TRADE LOG and compared, at each rebalance open, with the rule recomputed from closes by
code in the check script that does not import the class. Held dollars after the open's
fills must equal target x equity at the open within one lot plus that fill's costs
(commission and slippage on its quantity); an undecided sleeve must be untouched; no fill
may fall off the schedule.

- **Default parameters:** 0 differences in every run: 1,005 sleeve-dates on faber5 and
  1,407 on faber7 per cell, 201 rebalance opens each, 0 fills off schedule.
- **Wrong parameters** (speeds 8/16/32/64 with sigma over 21 days; B sigma 21 only, since
  it has no speeds; A' SMA 100, since it reads neither):

| cell | faber5 sleeve-dates differing (rebalance opens) | faber7 |
|---|---|---|
| A' (SMA 100) | 253 (107) | 340 (129) |
| B (sigma 21) | 494 (183) | 665 (186) |
| C | 157 (73) | 217 (100) |
| D | 645 (191) | 880 (194) |
| E | 967 (200) | 1,355 (200) |
| E' | 756 (199) | 1,081 (200) |

## Results, every row, 2011-2026 (2010 is warm-up)

"2020 crash" is 2020-02-19 to 2020-03-23. "Invested" is holdings / equity, averaged over
the slice's bars. Fills are over the whole run (2010 included). "Gross" rows pay no
commission or fee; slippage is kept.

| run | CAGR | vol | Sharpe +/- se | max DD | 2020 crash | 2022 | invested | fills | cost / final | alpha t | beta | Sharpe 2011-19 / 2020-26 | max DD 2011-19 / 2020-26 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| faber5 A (reference) | 5.2% | 7.8% | 0.449 +/- 0.25 | -15.3% | -5.5% | -4.5% | 72.5% | 155 | 1.89% | -0.05 | 0.30 | 0.345 / 0.581 | -15.3% / -9.1% |
| faber5 A' | 4.7% | 7.3% | 0.414 +/- 0.25 | -14.9% | -5.4% | -4.7% | 68.8% | 695 | 2.41% | -0.23 | 0.28 | 0.312 / 0.544 | -14.9% / -8.7% |
| faber5 A' gross | 4.9% | 7.3% | 0.447 +/- 0.25 | -14.6% | -5.3% | -4.5% | 68.8% | 695 | 0.00% | -0.07 | 0.28 | 0.343 / 0.579 | -14.6% / -8.7% |
| faber5 B | 3.7% | 5.2% | 0.390 +/- 0.25 | -8.3% | -5.1% | -1.8% | 53.8% | 715 | 2.50% | -0.25 | 0.19 | 0.421 / 0.343 | -8.3% / -6.5% |
| faber5 B gross | 3.9% | 5.2% | 0.432 +/- 0.25 | -8.1% | -5.0% | -1.7% | 53.8% | 712 | 0.00% | -0.06 | 0.19 | 0.463 / 0.385 | -8.1% / -6.3% |
| faber5 C | 3.9% | 7.0% | 0.322 +/- 0.25 | -13.8% | -3.4% | -7.0% | 67.4% | 762 | 4.44% | -0.59 | 0.25 | 0.231 / 0.427 | -13.8% / -12.2% |
| faber5 C gross | 4.3% | 7.0% | 0.380 +/- 0.25 | -13.4% | -3.4% | -6.5% | 67.4% | 764 | 0.00% | -0.31 | 0.25 | 0.289 / 0.484 | -13.4% / -11.4% |
| faber5 D | 2.4% | 3.6% | 0.211 +/- 0.25 | -6.0% | -2.0% | -0.2% | 34.3% | 798 | 4.09% | -0.55 | 0.10 | 0.248 / 0.154 | -5.4% / -6.0% |
| faber5 D gross | 2.7% | 3.6% | 0.300 +/- 0.25 | -5.8% | -2.0% | 0.0% | 34.3% | 795 | 0.00% | -0.16 | 0.10 | 0.342 / 0.237 | -4.9% / -5.8% |
| faber5 E | 4.9% | 8.3% | 0.385 +/- 0.25 | -19.5% | -19.3% | -13.4% | 99.4% | 973 | 1.42% | -1.54 | 0.41 | 0.606 / 0.188 | -11.4% / -19.5% |
| faber5 E' | 4.2% | 6.9% | 0.360 +/- 0.25 | -18.7% | -18.6% | -6.1% | 74.6% | 959 | 1.37% | -1.74 | 0.34 | 0.478 / 0.232 | -12.0% / -18.7% |
| faber5 D, daily band (secondary) | 1.9% | 3.1% | 0.083 +/- 0.25 | -4.5% | -1.7% | +0.4% | 34.3% | 1,325 | 8.21% | -1.36 | 0.10 | 0.190 / -0.057 | -4.5% / -3.9% |
| faber5 equal weight | 6.9% | 11.3% | 0.462 +/- 0.25 | -26.2% | -26.2% | -11.3% | 99.9% | 960 | 0.56% | -1.43 | 0.59 | 0.499 / 0.431 | -16.3% / -26.2% |
| faber7 A (reference) | 5.2% | 6.8% | 0.516 +/- 0.25 | -7.8% | -2.2% | -4.1% | 70.0% | 223 | 1.97% | 0.81 | 0.19 | 0.438 / 0.599 | -7.8% / -7.7% |
| faber7 A' | 4.6% | 6.0% | 0.495 +/- 0.25 | -7.0% | -2.4% | -4.0% | 65.4% | 919 | 2.62% | 0.70 | 0.17 | 0.434 / 0.559 | -7.0% / -6.0% |
| faber7 A' gross | 4.9% | 6.0% | 0.538 +/- 0.25 | -6.8% | -2.4% | -3.8% | 65.4% | 921 | 0.00% | 0.89 | 0.17 | 0.479 / 0.599 | -6.8% / -5.9% |
| faber7 B | 3.8% | 4.3% | 0.494 +/- 0.25 | -5.4% | -2.6% | -1.5% | 50.4% | 956 | 2.67% | 0.75 | 0.11 | 0.565 / 0.403 | -4.7% / -5.4% |
| faber7 B gross | 4.0% | 4.3% | 0.550 +/- 0.25 | -5.3% | -2.6% | -1.4% | 50.4% | 956 | 0.00% | 1.00 | 0.11 | 0.625 / 0.455 | -4.5% / -5.3% |
| faber7 C | 3.7% | 5.9% | 0.348 +/- 0.25 | -12.2% | -1.0% | -6.2% | 64.3% | 1,021 | 4.85% | 0.18 | 0.15 | 0.308 / 0.389 | -11.8% / -12.2% |
| faber7 C gross | 4.2% | 5.9% | 0.422 +/- 0.25 | -11.3% | -1.0% | -5.8% | 64.3% | 1,018 | 0.00% | 0.51 | 0.15 | 0.390 / 0.456 | -11.3% / -11.2% |
| faber7 D | 2.6% | 3.2% | 0.310 +/- 0.25 | -4.7% | -0.3% | +0.2% | 31.7% | 1,080 | 4.06% | 0.46 | 0.05 | 0.311 / 0.302 | -4.7% / -4.7% |
| faber7 D gross | 2.9% | 3.2% | 0.413 +/- 0.25 | -4.6% | -0.3% | +0.4% | 31.7% | 1,080 | 0.00% | 0.88 | 0.05 | 0.423 / 0.392 | -4.5% / -4.6% |
| faber7 E | 5.1% | 7.4% | 0.466 +/- 0.25 | -19.5% | -13.4% | -13.9% | 99.3% | 1,360 | 1.58% | 0.04 | 0.28 | 0.723 / 0.248 | -9.7% / -19.5% |
| faber7 E' | 4.3% | 5.7% | 0.457 +/- 0.25 | -14.3% | -12.6% | -6.7% | 74.0% | 1,327 | 1.54% | -0.11 | 0.22 | 0.623 / 0.290 | -8.5% / -14.3% |
| faber7 D, daily band (secondary) | 2.7% | 2.8% | 0.368 +/- 0.25 | -4.1% | -2.1% | -0.6% | 32.7% | 907 | 4.91% | 0.51 | 0.06 | 0.492 / 0.218 | -4.1% / -3.9% |
| faber7 equal weight | 6.4% | 9.0% | 0.527 +/- 0.25 | -19.5% | -17.7% | -12.8% | 99.8% | 1,332 | 0.75% | -0.03 | 0.39 | 0.630 / 0.446 | -12.0% / -19.5% |
| 60/40 SPY/AGG | 9.4% | 10.3% | 0.737 +/- 0.25 | -21.6% | -21.6% | -15.8% | 99.9% | 386 | 0.26% | 0.29 | 0.60 | 1.048 / 0.490 | -11.1% / -21.6% |
| SPY buy and hold | 14.0% | 17.0% | 0.717 +/- 0.25 | -33.7% | -33.7% | -18.2% | 100.0% | 1 | 0.01% | -0.53 | 1.00 | 0.870 / 0.584 | -19.3% / -33.7% |

The largest alpha t in absolute value are E' and E on faber5 (-1.74, -1.54), the faber5
equal-weight basket (-1.43) and faber5 daily-band D (-1.36); every other row's |t| is 1.00 or less.
Nothing has an alpha told from zero.

## The pre-registered rules

"X beats Y": X's Sharpe higher AND X's max drawdown no worse, per basket and decade. A
beat inside one standard error is "higher, not distinguishable".

### H1 (speeds): fails, on every comparison

| | 2011-2019 | 2020-2026 |
|---|---|---|
| faber5 C vs A' | 0.231 vs 0.312: no | 0.427 vs 0.544: no |
| faber5 D vs B | 0.248 vs 0.421: no | 0.154 vs 0.343: no |
| faber7 C vs A' | 0.308 vs 0.434: no | 0.389 vs 0.559: no |
| faber7 D vs B | 0.311 vs 0.565: no | 0.302 vs 0.403: no |

The ensemble's Sharpe is lower in all eight, by 0.08 to 0.25. It fails gross as well
(2011-26: C gross 0.380 vs A' gross 0.447 on faber5, 0.422 vs 0.538 on faber7), so it is
not the ensemble's higher turnover alone (C pays 4.4-4.9% of final equity against A''s
2.4-2.6%).

**The horizon confound, named as the plan requires:** the ensemble's longest speed (128
bars) is shorter than the 200-day rule, so C against A' compares a shorter horizon as well
as an ensemble against a single speed. This study cannot say which of the two lost.

### H2 (sizing): fails

| | 2011-2019 | 2020-2026 |
|---|---|---|
| faber5 B vs A' | 0.421 vs 0.312, DD -8.3% vs -14.9%: higher, not distinguishable | 0.343 vs 0.544: **no** |
| faber5 D vs C | 0.248 vs 0.231, DD -5.4% vs -13.8%: higher, not distinguishable | 0.154 vs 0.427: **no** |
| faber7 B vs A' | 0.565 vs 0.434, DD -4.7% vs -7.0%: higher, not distinguishable | 0.403 vs 0.559: **no** |
| faber7 D vs C | 0.311 vs 0.308, DD -4.7% vs -11.8%: higher, not distinguishable | 0.302 vs 0.389: **no** |
| faber5 B vs E' | 0.421 vs 0.478: **no** | 0.343 vs 0.232, DD -6.5% vs -18.7%: higher, not distinguishable |
| faber5 D vs E' | 0.248 vs 0.478: **no** | 0.154 vs 0.232: **no** |
| faber7 B vs E' | 0.565 vs 0.623: **no** | 0.403 vs 0.290, DD -5.4% vs -14.3%: higher, not distinguishable |
| faber7 D vs E' | 0.311 vs 0.623: **no** | 0.302 vs 0.290, DD -4.7% vs -14.3%: higher, not distinguishable |

- **Against the binary cells** vol-scaling wins 2011-2019 (inside one standard error) and
  loses 2020-2026 on both baskets, so B does not beat A' and D does not beat C.
- **Against E'** neither vol-scaled cell holds in 2011-2019: E' (the same sizing, no
  signal) had the higher Sharpe on both baskets. By the plan's rule, H2 fails as a trend
  claim too.
- **E**, reported only: B beats it 2020-2026 and 2011-2026 (inside one standard error),
  loses 2011-2019; D loses to it except faber7 2020-2026.

**Turnover, net and gross.** The rule ("beats its binary cell gross but not net: H2 fails
on cost") does not apply in any decade: gross, B still loses to A' 2020-2026 (0.385 vs
0.579; 0.455 vs 0.599) and D to C (0.237 vs 0.484; 0.392 vs 0.456). H2 fails on the sizing
itself, not on cost. Only on the whole 2011-2026 span of faber7 does cost decide B against
A' (gross 0.550 vs 0.538, net 0.494 vs 0.495), and the rule is judged by decade.

### What vol-scaling did do

It held less and lost less: B averaged 54% / 50% invested against A''s 69% / 65%, and D 34%
/ 32%. Worst drawdowns roughly halved (faber5 B -8.3% against A''s -14.9%; D -6.0% against
C's -13.8%), and 2022 went from -4.7% to -1.8% (faber5 B) and from -7.0% to -0.2%
(faber5 D). It paid 0.8-1.5 pp a year of CAGR against the same signal binary-sized (B vs
A' 1.0 / 0.8 pp, D vs C 1.5 / 1.1 pp, faber5 / faber7), and the Sharpe did not rise over
2011-2026 on either basket (B 0.390 vs A' 0.414; 0.494 vs 0.495). E' is not an exact
exposure match: it averaged 75% / 74% invested against B's 54% / 50%, so B holds less than
E' as well as holding at better times.

### Secondary: D with a daily no-trade band

Re-emitting a sleeve only when its weight moves more than 0.05 from the last emitted
target: faber5 1,325 fills and 8.2% of final equity in costs, Sharpe 0.083 against monthly
D's 0.211; faber7 907 fills, Sharpe 0.368 against 0.310. Neither changes a verdict; the
two baskets disagree.

## Outcome

**Neither H1 nor H2 holds: a note, and nothing registered.** No change to
`asset_class_trend` is proposed, and the `shorts` flag on `StrategySpec` (build step 3) is
not built, since it was conditional on registering. The 2026-08-10 assessment's "Time Series
Momentum" item closes on this note. The classes and their tests were not committed:
they stay on the local branch `worktree-agent-aaf25b7bb01d0c5ca`, unregistered
(`test_trend_ensemble_is_not_registered`); whether to keep or delete them is Danny's call.

## Files

On the local branch `worktree-agent-aaf25b7bb01d0c5ca`, not in this commit:

- `alpha_models/trend_ensemble.py`: the two classes, unregistered.
- `tests/test_strategy_contract.py`: seven look-ahead entries (`_trend_ensemble_cases`,
  speeds 4/8/16/32, `vol_window` 21, SMA 50), `TARGET_SHAPE_CASES` for the directional-value
  skip, `test_trend_ensemble_contract` (2 and 5 sleeves), `test_trend_ensemble_is_not_registered`.
- `tests/unit/test_trend_ensemble.py`: 12 tests, each figure worked by hand (the
  ensemble score, vol sizing, the binary cap, warm-up NaN, E and E', the daily band,
  refused arguments).
- A copy of the module with sigma taken over the whole sample (a look-ahead) was caught
  by the look-ahead comparison on four of five configurations; the binary ensemble is
  unaffected by construction (dividing by a positive sigma does not change a sign).
- Scratch scripts (`common.py`, `step1.py`, `post_check.py`, `bench_check.py`, `step3.py`,
  `holdings_check.py`, `cells.py`, `table.py`) and their JSON outputs are in the session
  scratchpad, `d65b-k7q2/`, not committed.

## Not verified

- **The cells were not run through the API.** `POST /backtest/portfolio` takes only
  registered strategies, so the endpoint check is on `asset_class_trend` (event path); the
  target cells run through the same `_run_targets` in process, unchecked against the API.
- **The daily-band variant has no holdings check**, and its band is against the last
  emitted target, not the drifted held weight, which the strategy cannot see.
- **Standard errors are iid** (no autocorrelation adjustment), as decision 53's regime
  errors are.
- **Slippage is not in "cost paid"** and is kept in the gross runs.
- **Per-slice alpha t** is `benchmark_report` on the sliced curve; it was checked equal to
  the API only on a whole run.
- **Data as of the 2026-10-22 bar.** A dividend recorded later rescales history at read
  time and moves these figures slightly.
- The horizon confound above: a 200-day ensemble, or a 128-day single rule, was not run,
  because the plan fixes the speeds and forbids searching them.
