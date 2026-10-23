# Spin-offs and the DCF: HON's flag (decision 58, 2026-10-23)

Read-only investigation. Nothing was changed.

## The flag

The 06:00 job of 2026-10-23 printed, for the first time:

```
CONCEPT HON cfo FY 2025-12-31: uses NetCashProvidedByUsedInOperatingActivitiesContinuingOperations 6,075M;
NetCashProvidedByUsedInOperatingActivities reports 6,408M; value/price 1.05 -> 0.35
```

It came from Honeywell's Q3 10-Q (accession 0000773840-26-000143, filed
2026-10-22, 175 new facts). Note 3 of that filing: **the Aerospace spin-off
completed 2026-06-29** (HONA, one share for every two) and Aerospace is now
discontinued operations "for all periods presented". Solstice (Advanced
Materials) was spun 2025-10-30 and is discontinued too.

## Neither concept is right: the TTM mixes two bases

The DCF's cash-flow TTM is FY + YTD(current) - YTD(prior)
(`Statements._ttm_ytd`). HON's terms, $M:

| term | continuing (chosen) | total | filed | basis |
|---|---|---|---|---|
| FY 2025 | 6,075 | 6,408 | 2026-02-17 (10-K) | Solstice out, **Aerospace in** |
| 9M 2026 | 932 | 743 | 2026-10-22 | both out |
| 9M 2025 | 2,610 | 5,204 | 2026-10-22 | both out |
| **TTM** | **4,397** | **1,947** | | |

The 10-Q's own cash-flow statement shows the hole: 9M 2025 discontinued
operating cash is $2,594M, in a year whose 10-K reported only $333M
discontinued in total. So:

- **Continuing (4,397)** subtracts an ex-Aerospace prior YTD from a fiscal
  year with Aerospace in it. About $2.6B of Aerospace's 2025 cash flow stays in
  as HON's.
- **Total (1,947)** is internally consistent but the wrong quantity: it carries
  Aerospace's cash through Q2 2026 and Solstice's through October 2025, and
  not after.
- **The right figure**, RemainCo's trailing year, needs Q4 2025 ex-Aerospace.
  It is not on file. It arrives with the FY2026 10-K (~Feb 2027) or an 8-K
  recast. Missing is never zero, so this should **refuse**.

The other lines are contaminated the same way:
- EBITDA: operating income TTM uses a derived Q4 2025 = FY (with Aerospace) -
  the quarters, the latest of them re-presented.
- Growth: the revenue CAGR runs to FY2025, Aerospace included.
- Beta: five years of the pre-spin company.

A `LINE_CONCEPTS_REVIEWED` entry fixes none of these. It would clear the flag
and ship a known-wrong 1.05. The check cannot see the failure itself:
`same_basis` compares only the window kind (TTM vs FY), not the filings' basis.
It caught HON because the two concepts happened to diverge.

**Yesterday's 1.30 was wrong as well**, for a different reason. The TTM to
2026-06-30 was on one basis (Aerospace in throughout), and so was the balance
sheet (debt $34.0B; $16.6B at 09-30 after Aerospace took its debt). But the
price after 2026-06-29 is the ex-Aerospace stock. From the spin until the Q3
10-Q, the DCF compared the post-spin price with the whole pre-spin company.

## A second defect: a spin-off is stored as a split, and the DCF multiplies shares by it

Yahoo serves a spin-off as a "split" with a non-rational ratio (HON 2025-10-30
1.061, 2026-06-29 0.9535). `Statements` multiplies a share count filed before
a split by its ratio. That is right for a split and wrong for a spin, which
leaves the share count unchanged. 87 equity split rows are not a multiple of
1/4 either way. All but a few are spins. A few combine a real reverse split
with a spin (MSI 2011 0.247, LDOS 2013 0.405, HLT 2017 0.487), or are
something else (GOOG 2015 1.0027, the Class C payment).

Live today:
- **CTVA** (separation 2026-10-01, factor 6.665): shares 4,447M = 667M x
  6.665, so the value per share is $3.07 against a $12.86 price.
- **SPGI** (spin 2026-07-01, factor 1.057): shares filed 2026-04-28 x 1.057.
  Its Q2 10-Q facts are missing (below).

In history, every point-in-time valuation between a spin and the next
periodic filing is affected. Examples: BDX 2026-02-10 to 05-07 x1.272, WDC
2025-02-24 to 05-02 x1.323, CMCSA 2026-01-05 to 02-03 x1.067.

## Options

- **A. Review entry for HON.** Clears the flag; HON keeps a wrong value.
  CTVA's shares stay wrong. 5 min. Rejected.
- **B. Refuse HON by name** (`DCF_REFUSED_SYMBOLS`). Honest for HON. Misses
  CTVA's shares and every next spin. 10 min.
- **C. Treat spins as spins**, in three parts:
  1. A reviewed `SPINOFF_ACTIONS` list in settings: (symbol, ex-date) ->
     spin, or split part + spin part for the combined rows. Built from the 87
     candidates, each read. Share counts ignore a spin's factor.
     `check_line_concepts.py` (or the reassigned check) flags any new
     non-rational split row not on the list, so the next one is a review, not
     a silent multiply.
  2. Refuse a DCF from a spin's ex-date until a periodic filing whose period
     ends after it. This covers price-after/company-before: HON 06-29 to
     10-22, CTVA now.
  3. A basis guard in `_ttm_ytd`: refuse the TTM when the FY term's filing
     and the prior-YTD term's filing disagree on discontinued operations.
     Measure: discontinued share of total CFO, FY from its own filing vs
     prior YTD from its own (HON 5% vs 50%). Threshold to be measured on the
     538. This is general, so it also catches a divestiture classified
     discontinued without a spin. It refuses with a named reason. HON stays
     refused until the FY2026 10-K.

**Recommendation: C.** Cost: 3-4 h of my time, assuming about 30 min to
classify the 87 rows and that the basis guard's threshold separates cleanly
on the 538. Includes tests that fail on today's HON and CTVA facts, and a
before/after census of every valued name. Expected refusals today: HON and
CTVA, plus any name the guard finds. Likely candidates are CMCSA, BDX, FDX and
SPGI, whose spins fall inside their current TTM windows. Each is reported in
the census before anything is committed.

## Separate finding: SEC companyfacts lacks recent 10-Qs for 67 filers

67 of 538 registry filers have no 10-Q or 10-K stored after 2026-07-15:
KO, SPGI, V, NEE, ABT, CB, HCA and others, mostly last filed 04-22 to 05-08.
This is not the 7-day refresh missing them. A live companyfacts pull today
has no fact filed after May for KO (and only an S-4 for SPGI), while SEC's
submissions list shows KO's 10-Q of 2026-07-29 and SPGI's of 2026-07-28. So
SEC's API itself does not carry them, and the 2026-10-16 `--all` reload could
not have either. Their DCFs run a quarter stale or fall under the 200-day
flow rule. To measure and propose a source (parsing the filing's XBRL
instance, or retrying companyfacts): about 1 h. Its own pick, not part of 58.

## Built (C, on "go on 58", 2026-10-23)

1. **`settings.SPLIT_ROWS_REVIEWED`**: all 87 odd-ratio equity split rows.
   Each is classified by the cover-page share count across it (stored
   facts), and by the separation where the count is missing.
   - 79 spin-offs. Their share ratio is 1.0, or the reverse split done with
     the spin: MSI 1/7, LDOS 1/4, HLT 1/3, DD 2019 1/3, HON 2026 1/2. HON
     actually went from 634M to 317M shares on 2026-06-29; this was not in
     the first write-up above.
   - 4 real odd splits, Yahoo's ratio kept: IRM 2014 (stock distribution,
     193.7M to 210.1M), WTW 2016, JCI 2016 (merger consolidations), LEN 2017
     (Class B dividend).
   - GOOG 2015 is a payment. DELL 2018, J 2025-05-16 and AVB 2026 are other.
     None of these four is a share change.

   `core/valuation_service.share_splits` applies the table, and
   `Statements` takes `spinoffs`. An unlisted odd row stays a split and is
   flagged `SPLIT?` by `scripts/check_line_concepts.py`.
2. **Spin window refusal** in `build_inputs`: from the ex-date until a 10-Q
   or 10-K for a period ending `SPINOFF_FILING_MIN_DAYS` (7) after it.
3. **Basis guard**: `Statements.discontinued_cash`, refusing when the prior
   YTD carries more discontinued CFO than the fiscal year by
   `DCF_DISCONTINUED_BREAK_SHARE` (5%) of the year's total CFO.

   The first measure, the gap between the two discontinued shares, was
   dropped. It also fires on one basis whenever a disposed business's cash
   is lumpy: OXY 22 points, HON's own Q2. The test kept is the one a single
   basis cannot fail by much, because a part of the year cannot carry more
   than the whole.
   - Over 501 filers at every filing day + 1 since 2012: 98 points above 5%
     (50 filers). HON's Q2 2026 is 2.2% and stays valued.
   - The rule also refuses when disposed cash is negative and lumpy. Nothing
     in this measurement separates that case from a real re-presentation.

**Census, as of 2026-10-23:**
- Valued went from 365 to 361. Newly refused:
  - HON (two bases)
  - CTVA (spin window; was v/p 0.24 on 4,447M shares)
  - SPGI (spin window; its Q2 10-Q facts are absent)
  - APTV (two bases: 6M 2025 carries $252M discontinued CFO against $0 in
    FY2025. A 10-Q moved a business into discontinued operations, which was
    not known before the census)
- CMCSA, BDX and FDX stay valued, but **not because they are on one basis.**
  The guard cannot see them. CMCSA and FDX tag no continuing-operations CFO
  at all (CMCSA reads as $0 vs $0). For BDX, `discontinued_cash` finds no
  pair of terms. Their total-concept TTMs still straddle the spin: FY (with
  the spun business) + YTD (without) - prior YTD (with). That leaves Versant's
  Q4 2025 inside CMCSA's trailing year, FedEx Freight's last nine months
  inside FDX's, and the spun units' remaining months inside BDX's.
- No other valuation changed.

**History, 101 affected filers, filing days + 1 since 2012:**
- 93 of 4,181 valued points newly refused (spin windows 18, two bases 75).
  None was un-refused and none changed value.
- The share fix moves no history point, because every point is the day
  after a filing whose cover count postdates any spin. It mattered only
  between filings (CTVA now).
- Pre-existing and unrelated: `history()` for ROP raises "equity market
  value must be positive" (an uncaught ValueError from `wacc`) both before
  and after.

**Tests:** `tests/unit/test_spinoffs.py`, 21 cases on HON's filed figures
and CTVA's count. Each guard was removed in turn on a copy, and each removal
fails a test. Python suite: 1,985 passed. `check_line_concepts.py`: 538
checked, 0 flagged (HON's CONCEPT flag is gone because HON now refuses).

**Not done:**
- ~~The hole above~~: built as 58b, below.
- The guard also runs on the rare four-quarter TTM, where no prior YTD is
  subtracted. Not worth code now.
- Growth's revenue CAGR still crosses a spin once the next 10-K
re-presents one year and not the older ones. Not measured.

## Built: 58b, the straddle rule (on "go on 58b", 2026-10-23)

A filer that tags only total CFO gives the basis guard nothing to compare.
The rule now refuses by date alone: while a spin ex-date falls after the
fiscal year the cash-flow TTM builds on (the latest annual CFO) and on or
before `as_of`, until a 10-K for a fiscal year ending after the spin is
filed. It runs after the basis guard, so HON keeps its more specific reason.

**A decision 58 bug found by its census.** `reports_period_ending` counted
any fact, so a 10-K's cover-page share count (dated weeks after the period)
read as a period after the spin. FDX's FY2026 10-K (period to 2026-05-31,
cover count dated 2026-07-16) ended its 2026-06-01 spin window, and FDX was
valued the day after. Only durations count now. 21 history points that
first read as straddles are spin windows again (A 2014, ABT 2013, DHR 2016
and 2023, GE 2023, HLT 2017, T 2022 and others), with the window's reason.

**Census, as of 2026-10-23:** valued went from 361 to 358. Newly refused:
CMCSA (until its FY2026 10-K, ~Feb 2027), BDX (FY2026 10-K, ~Nov 2026), FDX
(FY2027 10-K, ~Jul 2027). Nothing else changed. SPGI's reason is still its
spin window, but once its 10-Qs arrive the straddle keeps it refused until
its FY2026 10-K (~Feb 2027): that is the rule, not a stuck refusal.

**History, the same 101 filers since 2012:** 79 of 4,088 valued points
newly refused (61 straddles, 18 spin windows the cover-date fix restored),
on 31 filers; none changed value. ROP's `history()` still raises (unrelated).

**Tests:** `tests/unit/test_spinoffs.py`, 22 cases. The new one is HON's
figures with only total CFO: refused as of 2026-10-23, valued on FY2026 once
that 10-K is filed. The window test asserts its own reason and adds a cover
count dated after the spin (FDX's shape), which must not end the window. Each change was undone on a copy:
each undo fails a test. Python suite: 1,986 passed. `check_line_concepts.py`:
538 checked, 0 flagged.
