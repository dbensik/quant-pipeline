# Dividend drift — plan (2026-09-27, approved)

Nothing here is built yet. Approved 2026-09-27 with the recommended answers to
every decision:

| # | decision | approved |
|---|---|---|
| 1 | default `adjust` mode | `total` |
| 2 | gate tolerance | 0.5pp on any single day |
| 3 | `legacy` names | kept as-is, flagged `price_basis = legacy` |
| 4 | storage | new columns on `market_data` |
| 5 | `--full-backfill` | removed after cutover (the design section's recommendation) |
| 6 | saved `results/` | accepted that they stop reproducing |

## The problem, measured

Every bar is stored as Yahoo served it with `auto_adjust=True`: adjusted for
every split, spinoff and dividend Yahoo knew of **on the day it was fetched**.
Bars fetched on different days are adjusted to different as-of dates, so the
stored series has steps that are not market moves. Three kinds, all visible on
MO against a fresh Yahoo fetch:

| where | MO | cause |
|---|---|---|
| 2025-07-16 | **-6.39pp** | the seam: bulk load (as of 2025-07) meets the outage refill (as of 2026-08) |
| 2026-09-15 (ex-date) | -1.58pp | bars ingested daily since 2026-08 were stored before the dividend existed, so the ex-date shows the raw drop |
| 2026-08-28 / 08-31 | +-1.59pp | the 2026-09-25 fill: fetched after the 09-15 dividend, so the filled bar carries it and its neighbours do not |

Registry-wide: 121 dividend payers carry a 3pp+ fake day at the seam (CAG
-8.8%, PFE -8.1%). **It grows**: every ex-date of every payer now adds one wrong
day the size of the dividend yield — roughly 500 payers x 4 a year.

Splits and spinoffs have the same shape (NFLX 2026-08, APH, MNST, HWM), handled
today one symbol at a time by `detect_drift` plus `--full-backfill` — the
command that destroyed PARA's history.

## What Yahoo actually provides (checked 2026-09-27)

There is **no raw price** from Yahoo. With `auto_adjust=False`:

- `Close` is **split-adjusted and spinoff-adjusted as of the fetch**, but not
  dividend-adjusted. NVDA reads $121 on 2024-06-07, three days before its 10:1
  split (it traded ~$1,200). HWM reads $12.58 before the 2020-04-01 spinoff.
- `Adj Close` adds dividends — it is what `auto_adjust=True` returns now.
- `Dividends` amounts are split-adjusted as of the fetch (NVDA 0.01).
- Listed splits appear in `Stock Splits`, including spinoffs Yahoo models as
  splits (GE, MMM, T, BDX, WDC). **Unlisted spinoffs appear nowhere**: HWM's
  factor is baked into `Close` and absent from the actions.

A bar fetched the morning after it trades is effectively raw — no later event
exists yet. History fetched later is not.

## Design

**Store what the provider served, and when; derive everything at read time.**

1. `market_data` gains `served_open/high/low/close/volume` and `fetched_at` —
   Yahoo's `auto_adjust=False` values exactly as returned. Never rewritten.
2. A new `corporate_actions` table: `(asset_id, ex_date, kind, value,
   source, fetched_at, evidence)`. Kinds: `split`, `dividend`, and
   `manual_factor` for events Yahoo applies but does not list (HWM: 0.76687,
   2020-04-01, evidence = the research note).
3. **True raw = served x every split/manual factor with an ex-date in
   (bar date, fetched_at]**. Storing `fetched_at` is what makes this exact: a
   bar never has to be restated when a later split arrives, and the awkward case
   — a split effective the morning of a 06:00 fetch, before its action row is
   in any window — resolves itself once any later fetch sees the event.
4. **Adjusted = raw x the standard factors for events after the bar**: splits
   divide, dividends multiply by `(1 - D / prior raw close)`, manual factors
   multiply. Prices get all of it; volume gets splits only.
5. `fetch_range(..., adjust="total" | "split" | "none")`. `total` equals what
   every backtest and screener reads today, so callers do not change.

What this retires: split drift cannot exist, so `detect_drift`, the `/health`
drift list, `last_full_refresh_at` and `--full-backfill` lose their purpose.
`--full-backfill` should be removed, or at least refuse any asset with a floor,
ceiling or identity flag.

Crypto is out of scope: no splits or dividends. Its bars stay as they are.

## Migration — additive, gated, reversible

- One fetch per equity and ETF: full history, `auto_adjust=False,
  actions=True`. About 5 minutes, into the NEW columns only. The current
  columns are untouched until cutover, so backing out is ignoring the new ones.
- Hypertable compression is off (0 of 49 chunks), so this is an ordinary
  update of about 1M rows.
- **Per-symbol acceptance gate**, reusing the 2026-09-25 fresh-Yahoo
  comparison: the read-time `total` series must match Yahoo's `auto_adjust`
  returns within tolerance, and may differ from today's stored returns only on
  the known error days (seam, post-2026-08 ex-dates, 08-28/31 fill). HWM is
  checked against Yahoo directly after its manual factor is recorded.
- A symbol that fails, or that Yahoo cannot serve correctly, gets a per-asset
  `price_basis = legacy` and keeps its current series: PARA (Yahoo serves
  another company), the delisted names Yahoo dropped (ANSS, AVB, HES, CTRA,
  DAY...), and anything clipped by a floor or ceiling. **These still carry the
  seam step.**

## Readers that bypass `fetch_range`

All 36 API call sites go through `fetch_range`. These do not, and each needs a
decision at cutover:

- `scripts/find_successors.py` compares stored closes against an adjusted
  Yahoo fetch. Its constant-ratio test breaks if the bases differ.
- `scripts/check_reassigned.py`, `check_missing_days.py`,
  `audit_crypto_bounds.py`, `repair_bad_extremes.py`.
- `services/execution_service/portfolio_store.py` reads the latest close. The
  latest bar is identical in every mode, but it is `services/`, so it needs
  `./run_pipeline.sh verify`.
- Ingest's `implausible_jump` would read an unadjusted 20:1 split day as a 20x
  move. It must judge split-adjusted values.

## Also in scope

- **A daily fresh-return check** (last ~30 sessions against one batch Yahoo
  download) in the 06:00 job. It is what found HWM, and it is the only thing
  that sees a future unlisted spinoff. After the migration it doubles as the
  standing test of the whole adjustment engine.
- **Known limit**: an unlisted spinoff we have NOT found stays baked into its
  `served` values. `total` reads are still right, since Yahoo applied the
  factor. Only `split` and `none` are off, until the daily check finds it.

## Phases and estimates (my wall-clock, assuming no surprises in the gate)

| phase | work | estimate |
|---|---|---|
| 1 | Alembic 0007: served columns, `fetched_at`, `corporate_actions`, `assets.price_basis` — **done 2026-09-27**, applied to the live DB, downgrade round-tripped, all constraints verified, no existing value changed | 1-1.5 h |
| 2 | Adjustment engine as pure functions, tests incl. the morning-split and HWM cases — **done 2026-09-27**: `core/price_adjustment.py`. Evidence: `total` reproduces Yahoo's auto_adjust series to within 1e-6 over 11 symbols' full history (54 splits, ~1,700 dividends, HWM's manual factor), which checks the dividend math; `none` recovers real trades (NVDA $1,209.98, Arconic $16.40), which checks the direction of un-adjustment; staggered-fetch unit tests cover what a single-fetch check cannot. (`split` matching Yahoo's Close under one fetch time is true by construction and proves nothing.) 16 tests, 7 mutations all caught; fetch dates taken in New York time; 5.8 ms per symbol | 2-3 h |
| 3 | Ingest writes served values, `fetched_at` and actions; `implausible_jump` on split-adjusted values — **done 2026-09-27**: one `auto_adjust=False, actions=True` download yields both bases (the adjusted columns reproduce `auto_adjust=True` to the bit); `fill_served` fills only NULLs, `write_actions` keeps the first row, a full backfill never touches served values — all proven against real SQL and mutation-tested. Live run on MO/NVDA/AAPL/BTC-USD: 48 overlap bars filled, MO's 09-15 dividend recorded, every existing adjusted value unchanged. `implausible_jump` still judges the adjusted `ohlcv`, which is correct until cutover; moved to phase 5 | 1.5-2 h |
| 4 | Migration fetch, gate, `price_basis`, investigate failures | 2-3 h |
| 5 | `fetch_range(adjust=)`, Protocol and test fake, the bypassing readers, `verify` | 2-3 h |
| 6 | Cutover, daily fresh-return check, retire or guard `--full-backfill`, docs. The fresh-return check has a NAMED job beyond HWM-type spinoffs: an action keeps its first row, so if Yahoo later corrects a dividend amount the correction is never picked up — the check sees it as a `total`-mode mismatch on that ex-date | 1.5-2 h |
| | **total** | **10-15 h, over 2-3 sessions** |

Your hands-on time: about 30-45 minutes for the decisions below, plus reviewing
the gate results before cutover.

The "1-2 days" I quoted on 2026-09-24 was a guess made before this list
existed. This estimate replaces it.

## Learned while building

- **Yahoo scales dividend amounts by spinoff factors too**, not only splits:
  HWM's 2020-02-06 dividend is served as 0.015337, Arconic's $0.02 x 0.7669.
  `raw_dividends` undoes both.
- **The morning-of-a-split rule** (`APPLIED_ON_EX_DATE = True`) is still
  unverified; it is one constant, pinned by a test, and phase 6's daily
  fresh-return check is what will confirm it on the next real split.

## Phase 3 constraints (recorded before building it)

- **Filling `served_*` on rows that already exist.** Ingest writes with ON
  CONFLICT DO NOTHING, so every bar already stored — including the ones the
  14-day overlap re-serves every morning — would keep `served_*` NULL forever.
  "Written once, never rewritten" must mean: set `served_*` and `fetched_at`
  only where `served_close IS NULL`, in the ingest upsert and in the phase-4
  backfill alike. The legacy adjusted columns stay DO NOTHING. What `written`
  and `filled` count then needs deciding (proposal: they keep counting new
  rows; a separate `served_filled` counts served values added to old rows).
- **Re-fetched actions.** A dividend fetched again after a split comes back
  under the same (asset, ex_date, kind) key with a different served value
  (0.04 becomes 0.004). Keep the FIRST row, never update. Updating `value`
  without `fetched_at`, or the reverse, would un-split the amount by the wrong
  ratio and nothing downstream would notice. Keeping the first row is also
  what makes a stored bar and a stored action immutable in the same way.

## Decisions needed

1. **Default `adjust` mode.** Recommend `total`: it is what everything reads
   today, and backtests want total return.
2. **Gate tolerance.** Recommend 0.5pp on any single day. A dividend-sized
   miss is 0.2-2pp, so 1pp would hide exactly the error being fixed.
3. **`legacy` names.** Keep them as-is and flagged (recommended), or exclude
   them from screens and backtests after cutover.
4. **Columns on `market_data` or a separate table.** Recommend columns: adding
   a nullable column to a Postgres table is instant, one row per bar keeps the
   primary key and joins simple, and backing out is ignoring them. A separate
   hypertable isolates better but doubles storage and every join.
5. **`--full-backfill`**: remove, or keep with the guard above.
6. **Accept that saved `results/` stop reproducing**, as they already did at
   each re-adjustment.

## Why not the alternatives

- **Scheduled restatement** (a periodic scoped `--full-backfill` to re-adjust
  everything to one as-of date): drifts again between runs, and every run is an
  overwrite of stored history by a provider that has served wrong companies
  (PARA), placeholder bars (AMCR) and wrong tokens.
- **Store `Close` (split-adjusted) plus dividends only**: fixes dividends but
  keeps split drift and every tool built around it, and still has no answer for
  HWM-type spinoffs.
