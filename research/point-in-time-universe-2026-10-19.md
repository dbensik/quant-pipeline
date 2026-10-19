# Point-in-time universes: what exists, what is missing (decision 42, 2026-10-19)

Danny, 2026-10-19: "we need to build the point in time universe." The vault
task ("Make universes survivorship-bias aware") and the agent brief (Task 4)
both describe it as "the tables exist, nothing uses them". This is not quite
right, so this note starts from what the code does today.

## What exists

- **Observed membership** (`universe_membership`, `universe_snapshots`):
  daily snapshots since 2026-08-09 of sp500 (511 symbols seen), dow_jones
  (30) and top_100_crypto (127).
- **Reconstructed S&P 500 membership** (`universe_membership_reconstructed`):
  month-end lists from 2014-12 to 2026-07, read from past revisions of the
  Wikipedia page. Kept separate from the observed lists, and nothing reads
  it.
- **The screener API already resolves membership as of a date.**
  `POST /api/v1/screeners/run` with `index` takes the members on `start`
  (`members_as_of`). It refuses with a 422 a date before the first
  snapshot, rather than substituting today's list. A member with no bars
  is counted in `with_data` and not silently dropped.

## What is missing

1. **An S&P 500 screen can never run.** `MAX_SYMBOLS = 200` in
   `api/routers/screeners.py`, and the index has about 503 members, so
   `index: "sp500"` always returns 422. Only the Dow (30) and crypto (about
   100) work.
2. **The Screeners page has no index picker.** It takes a typed symbol
   list, so the point-in-time path is reachable only through the API.
3. **Observed history is ten weeks long.** Any screen dated before
   2026-08-09 is refused, which is correct.
4. **The reconstructed lists are unused.** They are the only membership
   before August 2026.
5. **No backtest takes a universe.** Single-asset backtests take one
   symbol, and allocation backtests take a basket the user types. Nothing
   asks for "the S&P 500 as it stood on each date".

## How much of a reconstructed membership we can price

Each year-end S&P 500 list, against our stored bars within a week of that
date. Symbols are the tickers in use at the time, so renames (FB to META,
WAG to WBA) are mapped through `research/sp500-ticker-renames-2026-10-04.csv`
(50 renames):

| year-end | members | priced as listed | priced after rename mapping |
|---|---|---|---|
| 2014 | 502 | 297 (59%) | 316 (63%) |
| 2016 | 505 | 334 (66%) | 352 (70%) |
| 2018 | 505 | 368 (73%) | 385 (76%) |
| 2020 | 505 | 416 (82%) | 430 (85%) |
| 2022 | 503 | 453 (90%) | 460 (91%) |
| 2024 | 503 | 489 (97%) | 492 (98%) |

The unpriced members are the ones that later left the index. The
2026-10-04 reconstruction found Yahoo has no prices for 151 of the 268
tickers that left since 2015, and the leavers it can price are mostly
survivors: renames and acquisitions. Failures, bankruptcies and
take-unders are what is missing.

- **For a screen, that is acceptable if it is visible.** A screen as of
  2016 that says "505 members, 352 with data" tells the reader what was
  left out.
- **For a backtest it is not.** A strategy run over 2014-2026 "S&P 500
  members" would hold 59-70% of the early index, and the missing part is
  biased towards losers. It would look survivorship-free without being so.
  That is why backtesting waits on a delisted-price source (Sharadar).

## Decision

42. **Recommend B.**

    - **A: observed membership in screeners, made usable.** Lift the cap
      for `index` requests (measure the time for about 503 symbols over a
      year first; if it is slow, stream it over the websocket). Add an
      index picker to the Screeners page showing the observed-from date,
      and keep refusing a date before the first snapshot. Useful only from
      2026-08-09 on. About 2-3 h of my time.
    - **B (recommended): A, plus reconstructed S&P 500 membership for
      2014-12 to 2026-07.** When no observed snapshot covers `start`,
      resolve from the month-end reconstructed list on or before it, with
      tickers of the day mapped through the rename file. The response says
      which source it used (`observed` or `reconstructed`, with the
      revision date) and lists the members without prices by name, beside
      the coverage percentage. The two tables stay separate. Nothing is
      written to `universe_membership`, and no prices are fetched. About
      4-6 h of my time, assuming the rename CSV moves into a table or
      config, not into code.
    - **C: universe backtests.** An allocation backtest whose basket is
      "index members as of each rebalance". **Not proposed now.** Without
      prices for delisted names it reports a survivorship-free figure that
      is not survivorship-free. Revisit after Sharadar; it would then be
      the same membership resolver as B plus a price source for the
      leavers.

    **Not in scope:** ingesting the 117 departed S&P tickers Yahoo does
    price. That is a data backfill (your yes), and it fills mostly the
    survivors, which leaves the backtest bias in place.

## What to check about Sharadar (for Danny)

Verify these on Nasdaq Data Link's own pages; this list is not a
recommendation:

- Whether its US equity prices include **delisted** tickers with full
  history, and from what year.
- Whether it has **historical S&P 500 membership**. If it does, that would
  replace the Wikipedia reconstruction.
- How it maps ticker changes. A permanent identifier is what fixes the
  rename class above.
- The licence for personal research use, and the price of the smallest
  plan that includes delisted names.

## Built (B, approved 2026-10-19)

- `TimescaleUniverseRepo.reconstructed_members_as_of`, and the screener's
  fallback to it for sp500 before the first daily snapshot. The response
  carries `membership_source`, `membership_date`, `membership_note`,
  `renamed` and `without_data`.
- Index-resolved universes may reach 600 symbols (`MAX_INDEX_SYMBOLS`); typed
  lists keep 200.
- **Found while building:** the snapshots write share classes as BRK.B and
  BF.B, but the registry stores them as BRK-B and BF-B, so both read as
  members without prices. They are now mapped, and listed in `renamed`.
- The Screeners page has a Universe picker (typed symbols, or the S&P 500,
  the Dow or the top 100 crypto as of the start date). It shows the
  membership source and date, the priced share, and the members without
  prices.
- Real runs through the API, with one screening step:

  | window start | source, list date | members | with data | renamed |
  |---|---|---|---|---|
  | 2026-08-10 | observed | 503 | 477 before the share-class fix | 0 |
  | 2016-01-04 | reconstructed, 2015-12-31 | 504 | 336 | 29 (AA to HWM, ANTM to ELV, ...) |
  | 2024-06-03 | reconstructed, 2024-05-31 | 503 | 480 | 2 (BK to BNY, MMC to MRSH) |

  Each screen took 23-26 s.

**Two findings for Danny, not fixed:**

1. **30 current S&P 500 members were not registered** (first counted as
   26, wrongly), so they had never been ingested. **Done 2026-10-19 on
   Danny's yes, for 29 of them:**
   - Registered through `POST /ingest/assets` and backfilled insert-only
     from 2010: 81,573 bars and 538 corporate actions.
   - FERG trimmed to its 2021-03-08 NYSE listing. Yahoo serves an illiquid
     line back to 2010: 2,811 bars, 2,079 of them with zero volume. P (Pure
     Storage, renamed Everpure) trimmed to its 2015-10-07 IPO, dropping a
     zero-volume placeholder bar. Both have `history_valid_from` set, and the
     removed rows are kept in `research/ferg-p-prelisting-*-2026-10-19.csv`.
   - Then marked `served`. Every bar has served values, and read-time
     returns match Yahoo's adjusted series on every day of every name
     (worst 0.0001pp).
   - CIKs recorded by `scripts/resolve_ciks.py --write`. Diffed against the
     06:00 backup, the only other row it changed was PSKY, which gained its
     CIK.
   - `check_missing_days`: clean. `check_reassigned` flags VRT at
     2019-04-08: a volume collapse inside its pre-merger SPAC history (GS
     Acquisition Holdings, merged 2020-02-07), which needs a human verdict.
   - **SKYD was not added as a new row.** It is PSKY renamed: same CIK
     2041610, renamed 2026-10-06, moved from Nasdaq to NYSE, with identical
     Yahoo closes. On Danny's yes the PSKY row was re-keyed to SKYD, with the
     evidence in its metadata, and PSKY -> SKYD added to the rename CSV.
   - **VRT floored at 2020-02-07**, the close of its merger with GS
     Acquisition Holdings, on Danny's decision. The 381 SPAC-shell bars are
     in `research/vrt-spac-bars-removed-2026-10-19.csv`, and
     `check_reassigned` is clean again.
   - Still open: nothing registers a member that joins later. VYLR and SKYD
     both joined on 2026-10-08.
2. **Re-issued tickers in the rename map (fixed 2026-10-19).** Q was
   Quintiles, which became IQV in 2017, and is Qnity Electronics since
   2025. Undated, the map priced Qnity in 2026 lists as IQVIA. A rename now
   applies only to lists dated before the new ticker was first listed. Renames
   also apply to observed membership, so PSKY in a snapshot before
   2026-10-08 is priced as SKYD.

3. **The `low_volatility` screener (fixed by decision 43) never passed anything through the API.**
   It reads a `volatility_90d` column that nothing has computed since the
   API port. Every page and API screen that uses it returns an empty list.
   The tests only ever used it as a step that lets everything through.
