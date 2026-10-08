# Financial models — roadmap (2026-10-08)

The "financial models" ask of 2026-10-07 was split into three tiers. This is
the sequence for what remains, with the plan each tier follows.

| tier | what | plan | status |
|---|---|---|---|
| 1 | Monte Carlo over the backtester | `monte-carlo-plan-2026-10-07.md` | **shipped 2026-10-07** (`c75acf6`..`49f73ff`) |
| 2 | option pricing and the volatility surface from the chain archive | `option-pricing-plan-2026-10-08.md` | **approved 2026-10-08**, all eight recommendations; not built |
| 3 | DCF on point-in-time SEC fundamentals | `dcf-plan-2026-10-08.md` | draft 2026-10-08; ten decisions open |

## Sequence

Tier 2 first, then tier 3. Two reasons: tier 2 phase 0 builds the `^IRX`
rate series that tier 3's discount rate reads, and the chain archive is
already captured and measured while the fundamentals store does not exist
yet. Tier 3 phase 0 (SEC gateway, CIK resolution) does not depend on tier 2
and can be started in a session where tier 2 is waiting on a look at a smile.

| step | work | agent wall-clock | your hands-on |
|---|---|---|---|
| 1 | tier 2 phases 0–5 | 12–16 h, 3–4 sessions | ~10 min: look at one smile and one term structure before phase 5 closes |
| 2 | tier 3 decisions | — | 20–30 min on the ten decisions; `SEC_USER_AGENT` in `.env` |
| 3 | tier 3 phases 0–6 | 13–18 h, 3–4 sessions | ~10 min: look at one valuation and its history before phase 6 closes |
| | **total** | **25–34 h, 6–8 sessions** | **~45 min** |

Each phase ends with its measurements written into the plan, the way tier 1
did; a phase that cannot be measured is not closed.

## What is built once and shared

- **`^IRX` daily series** — tier 2 phase 0; tier 3 reads it.
- **Insert-only storage with `fetched_at`** — the served-prices rule from
  2026-09-28, applied to chain rows (already) and to SEC facts (tier 3).
- **Header-only credentials from `.env`** — `coingecko_headers()` pattern,
  reused for the SEC User-Agent.
- **NaN-with-reason from a solver** — tier 2's implied-vol solver and tier
  3's reverse DCF both return a reason instead of a wrong number.

## Open items outside these tiers that touch them

- **Chain archive has one copy** (`data/` is gitignored). Tier 2 makes it
  worth more. A second copy is a 10-minute decision open since 2026-09-14.
- **Tiingo probe** needs `TIINGO_API_KEY`. Its result decides whether the
  145 departed S&P names without a CIK are worth resolving (tier 3 decision
  8) and whether a survivorship-free cross-sectional backtest is possible at
  all.
- **`COINGECKO_API_KEY`** still absent; the crypto snapshot fails
  intermittently without it. Unrelated to these tiers, same five-minute fix.

## Not in any tier (each would be its own plan)

Heston/SABR calibration, exotic payoffs, option strategies in the
backtester; an integrated three-statement projection; a cross-sectional
value screen on the facts table; walk-forward rewrite of `ml_random_forest`.
