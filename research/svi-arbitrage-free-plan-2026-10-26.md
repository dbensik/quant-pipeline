# Decision 56 B: arbitrage-free SVI fits, a plan (2026-10-26, measured, closed)

Decision 56 A (built 2026-10-22, `research/svi-surface-2026-10-20.md`) fits raw SVI per
expiry and REPORTS arbitrage, each flag saying whose it is: the fit's or the quotes'. Option B
was "A, plus fits forced to be free of arbitrage", left unbuilt. The bounded-m measurement of
2026-10-22 ended on "stable, arbitrage-free parameters are what Option B is for". This note
plans B and runs a first prototype on every cached surface, so the plan can rest on numbers.
No repo code was changed.

**Closed, 2026-10-26: Danny picked B (close on this note).** The best prototype already
missed the step-1 gate below by a wide margin: 93 of 683 refits (14%) had no feasible fit
against at most 1%, and the worst extra error was 65 vol points against at most 3. Nothing
reads an arbitrage-free curve today; decision 55 prices one smile at a time. Reopen when a
reader needs one curve across expiries (a calendar priced from the surface, a local-vol
model), starting from the joint solver in step 1.

## Verdict

- **Arbitrage-free can only mean the FIT's arbitrage.** The quotes' butterflies and calendars
  are trades at bid and ask (model-free). No fit can remove them, and an arbitrage-free fit
  through a slice whose quotes show one must leave those quotes. Today 447 of 2,155 slices
  show a quote butterfly; 27 of 1,993 pairs show a calendar credit.
- **It must sit BESIDE the raw fit, never replace it.** The raw fit is the picture of the
  market; the constrained fit is the curve to price from. Replacing the raw fit would silently
  change every `/options/svi` number, and the "fit's or quotes'" verdicts would lose their
  meaning: a constrained fit is never flagged, by construction.
- **Per-slice raw SVI with constraints, not SSVI.** SSVI was rejected on 2026-10-20: 2.6-6.3
  vol points of error against 0.4-2.1, and free SSVI broke its own condition on 3 of 6
  surfaces. eSSVI (Hendriks-Martini) loosens SSVI's shape but keeps one ρ-φ family across all
  expiries. The measured equity smiles (57% put the bottom past the quotes) are exactly where
  a shared family fits worst.
- **The obvious build does not work as-is.** Two prototypes on the 162 cached surfaces:
  - A penalty on negative g and on crossing the previous slice did not remove flags at any
    weight. Light weights left them in place. Heavy weights got stuck in local minima, with
    errors up to 130 vol points.
  - Hard constraints by SLSQP, refitting only infeasible slices, cleared most flags at a
    median cost of 0.04 vol points. But 93 slices found no feasible fit, and the tail is fat.
    One QQQ surface cascaded: a calendar constraint against a moved near slice pushed four
    later expiries 52-65 vol points off.
- **Recommended: A below. Spend about 3 h on a measurement-grade solver and adopt it only if
  it passes fixed criteria. Build the endpoint fields only after that.** If the criteria fail,
  close B on the note: the Greeks (decision 55) read one smile at a time and need no
  arbitrage-free surface.

## What "arbitrage-free" means here

| condition | where | the fit's test today | the constraint B adds |
|---|---|---|---|
| butterfly | each slice | Durrleman g(k) < 0 on the quoted k range ± `SVI_BUTTERFLY_MARGIN` (0.1), 801 points | g(k) ≥ 0 on that same range and grid |
| calendar | adjacent fitted expiries | w_far(k) < w_near(k) on their shared quoted range | w_far(k) ≥ w_near(k) on that range |
| wings | each slice | `wings_within_lee` (slopes ≤ 2) is reported, not a flag | b(1 + \|ρ\|) ≤ 2, a sufficient Lee condition, as a bound (cheap, linear) |

Notes:
- **Only the checked range is enforced.** Arbitrage outside the quotes plus the margin would
  be enforced on extrapolation the data does not inform. That range is the one the report
  already checks, so "constrained" and "unflagged" mean the same thing.
- **The constraint must be checked on the 801-point grid, not a coarser one.** In the
  prototype, 58 butterfly and 71 calendar flags survived on "solved" slices because the
  constraints used 81 points and the report checks 801.
- **Gatheral-Jacquier's closed-form conditions are not a shortcut.** Their sufficient
  conditions for raw SVI (e.g. a + bσ√(1−ρ²) ≥ 0 together with the Lee bound) do not imply
  g ≥ 0. Only SSVI's conditions do, and SSVI is rejected. The grid constraint is the honest
  test.

## Data (prototype runs, 2026-10-26)

Every cached surface: 162 captures, 6 tickers (AAPL, IWM, NVDA, QQQ, SPY, TQQQ), 2,155
fitted slices, 1,993 adjacent pairs. The archive has grown since the 2026-10-22 measurement
(2,013 fits, 1,863 pairs).

**Baseline, the built code (`core.svi_report.build_report`):**
- fit butterfly flags: 223 slices (10.3%).
- fit calendar flags: 458 pairs (23.0%).
- quote butterfly: 447 slices. Quote calendar credits: 27 pairs.
- RMSE: median 0.49 vol points, p90 1.22.
- runtime: 173 s for all 162 reports, about 1.1 s a surface.

**Prototype 1, penalty in least squares from the raw fit** (λ on min(g, 0) and on crossing the
previous constrained slice):

| λ | fit butterflies | fit calendars | RMSE median / p90 | worst slice |
|---|---|---|---|---|
| raw | 223 | 458 | 0.49 / 1.22 | — |
| 10 | 227 | 549 | 0.52 / 1.24 | +12.7 |
| 1,000 | 194 | 566 | 0.61 / 1.57 | +16.8 |
| 100,000 | 238 | 629 | 0.88 / 6.51 | +129.9 |

Calendar flags RISE, because each slice is pushed against a previous slice that has itself
moved. A penalty is not a constraint here.

**Prototype 2, SLSQP with hard constraints, refitting only infeasible slices** (start from the
raw fit and the 3 standard starts; keep the best feasible result, else keep the raw fit):
- **683 of 2,155 slices needed a refit (32%).** 178 for a butterfly, 389 for a calendar, and
  65 only because the slice before them moved.
- **590 were solved; 93 had no feasible result** from any start, and they keep their raw fit.
- **Flags fell:** fit butterflies 223 → 103, fit calendars 458 → 154. The remainder is the 93
  unsolved slices plus the grid gap above.
- **RMSE over all slices:** median 0.49 → 0.53, p90 1.22 → 1.39.
- **RMSE cost on the 590 refitted slices:** median +0.04 vol points, p75 +0.18, p90 +1.32,
  p99 +25.9, max +65.0. 68 cost more than 1 vol point.
- **The tail is a cascade.** QQQ 2026-10-07: the 10-23 slice was moved to clear a calendar,
  and the 10-30, 11-06 and 11-13 slices, raw-clean, then moved 52-65 vol points to stay above
  it. One expiry under 3 days (IWM 2026-09-30) cost 28 points.
- **Latest capture per ticker:** 16 of 71 slices refitted (9 of them on SPY), 1 unsolved
  (AAPL), median cost 0.00.
- **Runtime:** 89 s for all 162 surfaces, 0.55 s a surface on top of the raw fit.

Read: most slices are already feasible or cheap to make feasible. A slice-by-slice
near-to-far solver lets one bad expiry drag every later one off its quotes. The design has
to stop that before any adoption test makes sense.

## The measurement to run before building (Option A, step 1)

**Solver changes**, from what the prototype taught:
1. **Constraints on the 801-point check grid,** or 201 points plus a final 801-point check.
   A result that fails the final check counts as unsolved.
2. **No near-to-far cascade.** Fit all infeasible slices of a surface JOINTLY in one
   problem: raw-fit error over every slice, with butterfly constraints per slice and calendar
   constraints per pair. Feasible slices keep their raw parameters as the starting point and
   may move. This is about 5n parameters for n of 9-15 expiries, small for SLSQP or
   trust-constr. A fallback lets the solver drop the shortest expiry (under
   `SVI_MIN_DAYS`+3) from the calendar chain, with the reason reported.
3. **A slice no solver makes feasible is reported as such** (`arbitrage_free: null`, with a
   reason). It is never filled with the raw fit under the arbitrage-free name.

**Numbers to compare, raw against constrained,** on every cached surface (162 today):
- fit butterfly and fit calendar flags under the existing 801-point checks (must reach 0 on
  solved slices);
- unsolved slices and pairs;
- RMSE in vol points: median, p90, p99 and max, overall and on the refitted slices;
- `within_spread_share` overall, and on slices with no quote-side flag;
- the quote-side counts (unchanged by construction; reported so nobody reads the
  constrained flags as the market's);
- day-to-day stability of each parameter on the same expiry (b, ρ, m medians), against the
  0.25 / 0.16 / 0.20 moves measured on the raw fit;
- seconds per surface, median and max.

**Adopt only if all of these hold:**
- 0 fit flags on solved slices under the 801-point checks;
- unsolved ≤ 1% of slices (≤ 21 of 2,155);
- RMSE cost on refitted slices: median ≤ 0.10, p90 ≤ 0.50 and max ≤ 3 vol points. A worse
  curve than that is a different market, not a repair;
- `within_spread_share` falls by at most 2 points on slices with no quote-side flag;
- under 3 s a surface added to the request (the raw report is about 1 s).

If they fail, B closes on this note and the measurements, as the bounded-m pick did.

## Where it would go (Option A, step 2, only after the criteria pass)

- **`pricing/svi.py`:** `fit_arbitrage_free(slices, check_margin, points)`, joint and pure
  numpy/scipy. It returns per slice the params, rmse and `solved`. The raw `fit` is unchanged.
- **`pricing/arbitrage.py`:** unchanged. The same `butterfly` and `calendar` checks run on
  the constrained params, which is how the tests prove them clean.
- **`core/svi_report.py`:** each fit gains an `arbitrage_free` block: params, rmse_vol_pts,
  within_spread_share, curve, moved (yes or no), and reason when unsolved. The report gains
  `arbitrage_free_summary`: refitted, unsolved, and median and max RMSE cost. Existing fields
  and verdicts are unchanged, so the raw fit stays the market's picture. It is kept in
  process by the same surface key.
- **Settings:** `SVI_ARBFREE_*` (grid points, solver iterations, the RMSE ceiling past which
  a result is reported as unsolved). These stay out of the surface fingerprint, so no surface
  is recomputed.
- **API:** `GET /options/svi` returns the new block, with `include_arbitrage_free` (default
  true) to skip its cost.
- **Page:** a dashed second curve per smile and an "Arbitrage-free" column (RMSE cost, moved,
  unsolved). The arbitrage-free parameters are greyed where `bottom_quoted` is false, as the
  raw ones are.
- **Not wired anywhere else.** Decision 55's position curve keeps the linear market smile.
  Pricing from the constrained fit would be a separate, later decision.

**Tests:**
- **One that fails on today's raw fit:** take a real flagged slice from the cached SPY
  fixture (a short expiry with raw g < 0 inside the checked range). Assert raw
  `butterfly(...).fit` is non-empty, and the constrained fit's is empty with RMSE within
  the ceiling.
- **A calendar pair** built to cross: the constrained pair does not cross on the shared
  range, and the quote-side verdict is unchanged.
- **No cascade:** a three-expiry surface whose middle slice is bad must not move the last
  slice by more than the ceiling. The prototype fails this.
- **A clean surface is returned bit-identical** (raw params, `moved` false everywhere).
- **Unsolvable:** a slice with an impossible combination gives `arbitrage_free: null` with a
  reason, never the raw params.
- Mutants on a copy, per house rule: grid coarsened, a constraint dropped, unsolved filled
  with raw.

## Options

- **A. Measure a joint solver against the criteria above, then build only if it passes
  (recommended).**
  - Step 1 is the solver, the measurement and a short results section in this note, about
    3 h of agent time. Nothing in the repo changes.
  - Step 2, on your go after reading step 1: code, endpoint, page and tests, about 5-7 h.
  - Total if adopted: 8-10 h.
- **B. Close Option B now, on this note.** The prototypes already show a 32% refit rate and
  a fat tail. No reader needs an arbitrage-free curve today: decision 55 prices from the
  linear market smile, slice by slice. Cost: nothing. The raw report already says whose each
  arbitrage is.
- **Not offered: replacing the raw fit with the constrained one.** It would change every SVI
  number without a decision, and erase the "fit's or quotes'" verdicts that decision 56 A was
  built to give.
- **Not offered: SSVI or eSSVI.** SSVI was rejected on error (2.6-6.3 vol points). eSSVI
  shares its single-family shape across expiries, which is what the equity smiles measured
  here do not have.

## Reproduce

Scratch, not committed, in this session's scratchpad `svi_b/`:
- `measure.py <λ>`: prototype 1 (penalty), with the baseline counts.
- `slsqp.py`: prototype 2 (hard constraints, refit infeasible only).
- `rows_*.csv`: per-slice results.

Both scripts are read-only against `data/surfaces/*/*.json` and use the built
`core.svi_report.build_report` for the raw side. No database, no network. Run time is about
4 and 5 min.
