# Price history back to 2010, and what it changes (decision 26, 2026-10-14)

Decisions 21 and 25 were both measured on bars that start in 2020 for most
names: one six-year path, mostly one rally. Decision 26 (approved: "go on
26") was to add 2010-2019, insert-only, and re-measure both.

## 1. The backfill

- **Bars:** 1,133,222 inserted for the 518 served equities and ETFs,
  2010-01-01 to 2020-01-01, through `core.ingest.ingest_symbols` with an
  explicit END of 2020-01-01, so nothing on or after 2020 was even fetched.
  `market_data` went from ~1.10M to 2,229,741 rows. 0 failures, 0 implausible
  jumps, 0 new delisting flags. 18 names have nothing before 2020 because
  they did not trade (ABNB, APP, ARM, CARR, CEG, COIN, DASH, EXE, GEHC, GEV,
  GFS, KVUE, OTIS, PLTR, PSKY, SOLV, VLTO, WBD). 6,853 corporate actions
  came with them (first row kept, as always).
- **Risk-free rate:** `^IRX` started on 2015-01-02, and every backtest
  refuses (422) a window the rate does not cover, so it was extended
  insert-only from 2009-12-01: 1,279 new observations, the existing 2,961
  hashed identically before and after. 2010-01-04 reads 0.055% (discount).
- **Legacy and crypto assets were not touched** (PARA has a ceiling but no
  floor; crypto has its own identity gates).
- **Nothing a reader saw changed.** Every served asset was fingerprinted
  before the first write (OHLCV of `fetch_range` in all three modes, from
  2010): after the backfill, all 1,554 reads from each name's old first bar
  are bit-identical. Every new bar carries served values (0 NULL of
  1,186,341 pre-2020 bars).
- **Restore point:** the 2026-10-14 06:06 backup (restore-checked).

### Checks over the new history

| check | result |
|---|---|
| `check_reassigned.py` (all) | 0 suspected |
| `check_missing_days.py` (all) | 1,134 holes, all SW -> repaired, now 0 |
| `check_fresh_returns.py --sessions 4200` (518 names, 2010-2026) | 3 days off -> decision 27 below |
| zero-volume / flat-run sweep of new bars | SW only (HWM, CHTR explained below) |

**SW (Smurfit Westrock) — repaired.** It listed on the NYSE on 2024-07-08,
and Yahoo serves 2010-2019 placeholder bars under SW: 1,866 of 2,516 on zero
volume, flat runs up to 57 days — AMCR's pattern. Removed the same day in
one checked transaction (2,516 bars, all fetched by this backfill, plus the 2
actions it added); `history_valid_from = 2024-07-08` so ingestion refuses
them again. Backups: `research/sw-prelisting-bars-removed-2026-10-14.csv`,
`research/sw-prelisting-actions-removed-2026-10-14.csv`.

**Kept, explained:** HWM's history begins 2016-11-01 (Arconic Inc. after the
Alcoa separation; CIK 4281 is Howmet today); its first 35 sessions have
prices but no volume. CHTR has 18 scattered zero-volume days in 2010, the
year after its bankruptcy exit. Both match Yahoo's returns on every day.

## 2. Decision 27 — a dividend paid on a split's own ex-date

All three fresh-return mismatches are one rule. Yahoo lists a split AND a
dividend on the same ex-date for a spinoff-with-reverse-split or a
cash-plus-split merger:

| symbol | ex-date | event | split | dividend | our day vs Yahoo |
|---|---|---|---|---|---|
| EXPE | 2011-12-21 | TripAdvisor spinoff + 1:2 reverse split | 0.5 | 15.125 | +37.4 pp |
| TMUS | 2013-05-01 | MetroPCS combination: 1:2 reverse split + cash | 0.5 | 4.06 | +22.0 pp |
| DHR | 2016-07-05 | Fortive spinoff | 1.319 | 24.56 | -19.1 pp |
| IRM | 2014-09-26 | REIT conversion special distribution | 1.082 | 0.669 | under 0.5 pp |

`raw_dividends` in `core/price_adjustment.py` converts a dividend to the raw
basis by the splits in `(ex_date, fetch]` — the interval EXCLUDES the
dividend's own day. Yahoo's amount is already scaled by that day's split.
Worked by hand on TMUS: excluding gives +6.2% for 2013-05-01, including gives
-15.8%, which is Yahoo's adjusted return. These are the only four same-day
split+dividend pairs stored, and all four are in the history added today, so
the rule had never been exercised before.

Probed in memory (no file edited): with the interval made `[ex_date, fetch]`,
the 4,200-session fresh-return check on the four names goes from 3 days off
to 0, and of all 1,554 fingerprinted reads only `DHR|total`, `EXPE|total`,
`IRM|total`, `TMUS|total` change — no split or raw read, no other name.

27. **Recommend: count a split on the dividend's own ex-date when scaling the
    dividend** (one interval in `raw_dividends`, a unit test built from the
    TMUS numbers that fails on the current rule, the docstring's statement of
    the rule). About 30-45 min agent time. It changes total-return reads of
    four names before 2017 and nothing else. Alternative: leave the rule, and
    let the four names carry one wrong day each (EXPE's +37 pp is large
    enough to move any 2011 backtest of it).

    The re-measures below exclude those four names, so they hold either way.

    **Approved 2026-10-15 and built:** `raw_dividends` scales a dividend by
    splits and manual factors in `[ex_date, fetch]`;
    `test_a_split_on_the_dividends_own_ex_date_scales_it_too` (TMUS numbers)
    fails on the old rule. On 2026-10-15's data, of 1,554 fingerprinted reads
    only the four total-return series changed, and the 4,200-session
    fresh-return check on the four names reads 0 days off.

## 3. Value/price ranking on 2010-2026 (decision 21 re-run)

Same method as decision 21 (`rank_capture.py`, `rank_analyse.py`): every
point-in-time valuation the day after a 10-K/10-Q, forward return relative to
SPY, monthly quintiles, Newey-West t. Today's model, point-in-time facts.
DHR, EXPE, IRM and TMUS excluded (decision 27); with them in, 20,699
observations give +1.1% (t 0.22) and IC 0.010 (t 0.43) — no difference.

| | decision 21 (2020-2025) | now (2010-2026) |
|---|---|---|
| observations / names | 10,078 / 394 | 20,507 / 386 |
| months with a cross-section (12m) | 66 | 172 |
| 12m Q5-Q1 (t) | +7.7% (0.69) | +0.8% (0.17) |
| 12m IC (t) | 0.043 (1.23) | 0.009 (0.40) |
| 12m Q5-Q1, positive value only (t) | — | +5.5% (0.89) |
| 12m Q5-Q1 within sector (t) | nothing | -2.7% (-1.58) |
| smallest IC the test can see (2 SE) | ~0.07 | ~0.045 |

- **Twice the history made the signal weaker, not clearer.** The 2020-25 IC
  of 0.043 falls to 0.009 over 2010-26.
- **The sign does not persist.** 12-month IC by year runs from -0.158
  (2019) to +0.189 (2021); positive 2010-13, negative 2014-15 and 2017-19.
- **Quintile medians are flat; the extremes carry the mean** (Q1 8.9%, Q5
  9.8%, Q2-Q4 1.5-2.7%), the same U-shape as before: both tails are volatile
  names.
- **Survivorship cuts the other way here.** Every name is a 2026 index
  member, so a 2012 "cheap" name is one that survived to 2026. That should
  flatter value; it still found nothing.

Decision 21 stands with more evidence: value/price is a reading tool, not a
ranking signal. Nothing to build.

## 4. Strategies on 2010-2026 (decision 25 re-run)

**These figures are at same-close fills.** Decision 31 (2026-10-15) moved
both backtesters to next-open fills; the restated figures — rsi 26%, not
28% — are in `research/fill-timing-2026-10-15.md` and in the caveats.

Same method as decision 25 (`eval_run26.py`, `eval_analyse26.py`, through
the live API: 10 bps + adverse slippage, idle cash earns `^IRX`, Sharpe
against cash, lots, seed 1), on the same 515 names; analysed on the **428
with bars from 2010-01-04**, excluding DHR, EXPE, IRM, TMUS (decision 27).
0 of 10,272 part-A runs failed. The 2020-2026 column on these 428 names
reproduces decision 25 (rsi 50% against 48%, buy-and-hold Sharpe 0.18
against 0.19), so the subset is not what moved anything.

### A. Defaults against buy and hold on the same name — share of names with a better Sharpe

| strategy | 2010-2026 | 2010-2019 | 2020-2026 | CAGR vs B&H, 2010-26 (median) | in market |
|---|---|---|---|---|---|
| rsi | 28% | 25% | 50% | -5.1 pp | 38% |
| mean_reversion | 14% | 19% | 28% | -7.6 pp | 28% |
| ma_crossover | 13% | 16% | 29% | -6.8 pp | 64% |
| trend_following | 7% | 7% | 21% | -9.4 pp | 60% |
| atr_breakout | 6% | 8% | 19% | -11.0 pp | 31% |
| ml_random_forest | 0% | 0% | 3% | -16.6 pp | 43% |
| push_response | 0% | 0% | 3% | -18.1 pp | 54% |

Buy and hold, median name: Sharpe 0.40 / 0.63 / 0.18, CAGR 12.8% / 15.6% /
8.9%, max drawdown -53.2% / -36.4% / -49.5% over the three windows.

- **The longer history is worse for every strategy, not better.** In
  2010-2019, a calmer, steadily rising market, the best default (rsi) beats
  buy and hold on 25% of names and gives up 7.4 pp a year. 2020-2026 was
  the strategies' most favourable stretch, because its crashes rewarded
  sitting in cash.
- **The drawdown benefit holds up; the return cost is larger.** rsi and
  mean_reversion have a shallower drawdown than buy and hold on 97% / 91%
  of names over 2010-2026, at 5-8 pp a year of return.
- **ETFs:** ma_crossover beat buy and hold on 9 of 11 ETFs in 2020-2026
  (decision 25 reported 9 of 12) but on 1 of 11 in 2010-2019; 7 of 11 over
  the whole period. Its ETF result was the 2020s, not the rule.
- **Beating buy and hold does not persist from decade to decade.** Of the
  107 names where rsi beat buy and hold in 2010-2019, 51% beat it in
  2020-2026, against 50% of all names. mean_reversion: 21% against 28%,
  worse than chance. Only trend_following (32 names, 47% against 21%) and
  ma_crossover (68 names, 37% against 29%) lean the other way, both on few
  names.

### B. Fitted by Sharpe in-sample, tested out of sample

| fit -> test | strategy | in-sample | OOS fitted | OOS default | OOS B&H | fitted > default | fitted > B&H |
|---|---|---|---|---|---|---|---|
| 2010-14 -> 2015-19 | mean_reversion | 0.86 | 0.26 | 0.31 | 0.53 | 42% | 26% |
| 2010-14 -> 2015-19 | ma_crossover | 0.69 | 0.29 | 0.25 | 0.53 | 53% | 19% |
| 2010-19 -> 2020-26 | mean_reversion | 0.65 | 0.10 | 0.04 | 0.18 | 53% | 36% |
| 2010-19 -> 2020-26 | ma_crossover | 0.52 | 0.04 | 0.03 | 0.18 | 52% | 29% |

- **Fits lose 58-92% of their Sharpe out of sample**, as in decision 25
  (there 0.72 -> 0.19).
- **Fitting is a coin flip against the defaults** (42-53%), and fitting on
  ten years instead of five leaves it one (52-53%).
- **Fitted parameters lose to buy and hold out of sample on 64-81% of
  names.**

### What this means

Decision 25's finding ("no strategy beats buy and hold at its defaults")
holds on 2010-2026, and more strongly. The one encouraging number in it,
rsi level with buy and hold on about half the names, belongs to 2020-2026
alone: over 2010-2019 it beat buy and hold on 25%, and which names it beats
is unrelated from one decade to the next.

The caveats users see (`measured()` in `alpha_models/registry.py`) quote
2020-2026, so they overstate every strategy, rsi most of all ("48% of
names, 1.9 pp a year less").

28. **Recommend: rewrite each single-asset caveat from the 2010-2026
    figures, keeping the 2020-2026 figure beside it** — e.g. rsi: "Measured
    at its default parameters on 428 names, 2010-2026, with costs: better
    Sharpe than buy and hold on 28% of names (25% in 2010-2019, 50% in
    2020-2026), 5.1 pp a year less return; which names it beats does not
    persist from one decade to the next." About 30-45 min agent time
    (seven caveats, the `measured()` docstring, any test that pins the
    text, then restart 8001). Alternative: keep the 2020-2026 caveats and
    record this here only.

    **Approved 2026-10-15 and built:** `measured()` now says "428 names,
    2010-2026", and each of the seven caveats gives its 2010-2026 share with
    the 2010-2019 and 2020-2026 shares beside it. Shown once 8001 restarts.

    Not proposed: changing `DEFAULT_START_DATE` (2020-01-01) in
    `config/settings.py`; it is left as it was.
