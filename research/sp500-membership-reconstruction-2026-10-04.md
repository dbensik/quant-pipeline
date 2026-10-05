# S&P 500 membership reconstruction — findings, 2026-10-04

Evaluates the option `awesome-systematic-trading-assessment.md` left open:
can historical S&P 500 membership be rebuilt from Wikipedia, so that
cross-sectional equity backtests (its Phase 4) stop being blocked on the
calendar?

**Membership: yes. It is rebuilt and stored, 2014-12 to 2026-07, at month-end
resolution. Phase 4: still blocked, on a different thing — Yahoo has no prices
for most of the names that left.**

## What was built

| piece | where |
|---|---|
| table `universe_membership_reconstructed` | migration `0008_reconstructed_membership`, `ReconstructedMembershipORM` |
| parsing and the guard, no I/O | `core/membership_reconstruction.py` |
| loader | `scripts/reconstruct_sp500_membership.py` |
| a second copy of every accepted list | `research/sp500-membership-reconstructed-2026-10-04.csv` |

Reconstructed rows live in their own table. `universe_membership` stays what
its docstring says it is: observations only. **Nothing reads the new table
yet** — wiring it into a screener is a separate decision (see the last
section).

## What was loaded

| | |
|---|---|
| month-end lists | 140, 2014-12-31 to 2026-07-31, none missing |
| rows | 70,559 |
| distinct tickers | 789 |
| members per list | 501 to 505 |
| largest month-over-month change | 16 symbols |
| revisions rejected by the guard | 0 |
| age of the revision used, at month-end | median 2 days, at most 27 |

It stops at 2026-07-31 because observed snapshots begin 2026-08-09; the
loader never writes a month on or after the first real snapshot. A second run
found all 140 months stored and made no requests.

Month by month against the fja file: 83 of the 91 months from 2019 on are
identical and the rest differ by at most 6 symbols. For 2014-2018 every month
differs, by a median of 25 symbols — the ticker-convention difference
described under Validation, not a membership disagreement.

## Method: past revisions, not the changes table

The assessment proposed walking the page's dated "selected changes" table
backward from today's list. That was tested and set aside for two reasons.

1. **The table is gone from the live page.** It was present in the revision
   of 2026-07-21 (406 rows, 1976 to 2026-06-30) and absent by 2026-08-19. It
   survives only in the revision history. The same thing happened to the DJIA
   page in August.
2. **It cannot undo a rename that happened after the change.** Each row is
   written under the ticker of the day. EchoStar was added as SATS on
   2026-03-23 and is listed today as ECHO, so the walk-back never removes it
   and carries ECHO into every earlier year as a member it was not.

A past revision of the page has neither problem: it is the complete list,
under the tickers in use then. So the loader reads, for each month-end, the
newest revision saved on or before it.

### The walk-back still works as a cross-check

Walked back from the 2026-07-21 list and compared with the page's own
revision at twelve year-ends (2014 to 2025), the two agree once renames are
paired by CIK. What is left over:

- **ECHO** in every year (the rename case above), and **ACT** and **MWV** in
  2015;
- changes dated within days of the revision (CHD/ALTR 2015-12-29;
  CRH, CVNA, FIX for LKQ, SOLS, MHK on 2025-12-22).

Everything else that differs is a ticker change, most of them with a new CIK
as well: LB/BBWI, COG/CTRA, CTL/LUMN, MYL/VTRS, KORS/CPRI, LUK/JEF, PX/LIN,
TSO/ANDV, WRK/SW, CBS/VIAC/PARA/PSKY, WAG/WBA, GCI/TGNA.

## Validation against an independent source

`fja05680/sp500` on GitHub publishes dated member lists. Its 1996-2019 portion
is Andreas Clenow's file (from *Trading Evolved*), which does not come from
Wikipedia; from 2019 on it is maintained from Wikipedia and so is NOT
independent.

Wikipedia revision vs that file, at each year-end:

| year-end | Wikipedia | Clenow/fja | same | differ |
|---|---|---|---|---|
| 2014 | 502 | 499 | 480 | renames, plus 3 names the file lacks |
| 2015 | 504 | 501 | 487 | renames |
| 2016 | 505 | 506 | 494 | renames |
| 2017 | 505 | 506 | 499 | renames |
| 2018 | 505 | 505 | 504 | KORS/CPRI |
| 2019-2024 | 503-505 | same | all | none |
| 2025 | 501 | 503 | 500 | the 12-22 changes, a day's lag |

For 2014-2017 the differences are the file using LATER tickers for earlier
dates (AABA for YHOO, BKNG for PCLN, WELL for HCN), which also makes it lose
a member where two companies later shared a ticker (ACE and the old Chubb;
TYC and JCI). The revisions carry the ticker of the day and are the better
record for those years.

So the independent check covers 2014-2018 and passes. For 2019 onward the
agreement is real but is Wikipedia agreeing with itself.

## The guard

Anyone can edit the page, so the revision current at a month-end may be
vandalised or half-edited. `judge()` rejects a list when

- its count is outside `SP500_EXPECTED_RANGE` (480-520),
- a symbol repeats, or a row is not a ticker,
- more than `SP500_RECONSTRUCTION_MAX_MONTHLY_CHANGE` (40) symbols differ from
  the previous accepted month. Real months peak at 16 (2019-12).

A rejected revision is skipped and the one before it tried, up to ten. A
month with none usable is left out and the run exits 1 — a missing month is
visible, a wrong one is not.

## Limits of what is stored

- **Month-end resolution.** A name that joined on the 5th is a member from
  that month's end. Do not read an exact join date out of this table.
- **Wikipedia's edit timing, not S&P's effective date.** Editors usually
  update on the announcement or the effective day; either can put a change a
  few days on the wrong side of a month-end.
- **Tickers as written then.** FB before 2022, not META. Joining to `assets`
  needs a rename map; three renames are already recorded in
  `assets.metadata.renamed_from`, the rest are not.
- **Symbols as Wikipedia writes them** (`BRK.B`), the same convention as
  `universe_membership`. `assets` uses `BRK-B`.

## Why this does not unblock Phase 4

268 tickers were members at some point since 2015 and are not now. Asked of
Yahoo, each over its own membership window:

| result | tickers |
|---|---|
| priced over the window | 95 — every one still trading today |
| no data at all | 151 |
| data only OUTSIDE the window | 22 |

The 22 are the PARA case: the ticker now belongs to someone else. And the 95
that can be priced are, without exception, companies that survived. The
acquired, merged and bankrupt are the 151.

Share of the members at each year-start that can be priced:

| year | Yahoo can price | registry has bars today |
|---|---|---|
| 2015 | 73% | 3% |
| 2016 | 76% | 3% |
| 2017 | 79% | 3% |
| 2018 | 81% | 3% |
| 2019 | 83% | 3% |
| 2020 | 86% | 80% |
| 2021 | 88% | 83% |
| 2022 | 90% | 86% |
| 2023 | 94% | 90% |
| 2024 | 95% | 94% |
| 2025 | 97% | 98% |
| 2026 | 98% | 97% |

The Yahoo column is low by up to about 3 points: some of the 268 are old
tickers of companies still in the index (about 251 real removals since 2015,
by the changes table). The registry column is near zero before 2020 because
492 of 516 equities begin on 2020-01-02.

This is the `empty-fetch-proves-nothing` finding from the other side: Yahoo
keeps history only for symbols it still lists. A survivorship-free equity
backtest needs a source that keeps delisted history. The fja README names
Norgate and EODHD; neither has been priced or tested here.

## What it does buy, and the decisions left

1. **A delisted-price source** is the decision that gates Phase 4. Without it
   no amount of membership data makes a 2015-2022 cross-sectional backtest
   honest.
2. **Screens from 2023 on** could use true membership with 94%+ price
   coverage and report the names they could not price, instead of being
   refused. The missing names are not random, so the result is still
   flattered — less, and by a stated amount. Not built; it changes what the
   screeners accept, which is a call for Danny.
3. **A rename map** (ticker-then to ticker-now) is needed by either of the
   above. The CIK pairing used in this evaluation resolves most of it.
