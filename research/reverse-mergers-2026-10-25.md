# Reverse mergers: KDP and JCI (decision 74)

Date: 2026-10-25
Status: built. Follows `research/revenue-start-years-2026-10-24.md`
(decision 72), "Not done".

## The gap

In a reverse merger the legal registrant keeps its CIK and ticker, but for
accounting it is the company acquired. Two valued filers are like this:

| symbol | filer (CIK) | accounting acquirer | close |
|---|---|---|---|
| KDP | Dr Pepper Snapple (1418135) | Maple Parent / Keurig | 2018-07-09 |
| JCI | Tyco International (833444) | Johnson Controls, Inc. | 2016-09-02 |

The filer's 10-Ks before the close describe its own past: DPS, Tyco. From the
close on, its 10-Ks present the accounting acquirer's past: Keurig's fiscal year
to 2017-09-30 ($4,269M), and Johnson Controls Inc.'s FY2014-16. Yahoo's price
series follow the registrant (JCI's split rows are Tyco's 2012 spins and its
0.955 consolidation at the merger), so every point BEFORE the close is
consistent: DPS's statements against DPS's price, Tyco's against Tyco's.

The DCF read across the close in two places:

1. **Growth start years.** `annual_before` took the year 3 or 5 years back by
   date, from any filing. KDP's FY2019 ($11,120M) was set against DPS's FY2016
   ($6,440M), giving a 3-year CAGR of 19.97%, clamped to 10%. JCI's FY2016
   ($37,674M) was set against Tyco's FY2013, giving 55.30%.
2. **The trailing year after the close.** Until the first 10-K for a year ending
   after the close, the cash flows build on the registrant's own last fiscal
   year, against the combined company's shares and price:
   - 2018-08-08 (DPS's Q2 10-Q): 11.42.
   - 2018-11-09 (the Q3 10-Q, Keurig's year to date on DPS's FY2017): 0.29.

## Built

`settings.REVERSE_MERGERS`: symbol -> (close date, what was read). It reaches
`Statements(reverse_merger=)` through `load_statements`.

- **Start years.** When the latest year was filed on or after the close, a start
  year filed before it is not read. A year the combined company re-filed (its
  10-K comparatives) is read: that is the accounting acquirer's history, and
  growth through the merger is then growth by acquisition, labelled so, as for
  WTW 2016 and MPC 2018 (decision 72).
- **The trailing year.** `build_inputs` refuses from the close while the latest
  annual CFO's year ends before it, by date alone, as 58b does for a spin. The
  first 10-K for a year ending after the close ends it. That 10-K's year mixes
  the two companies for part of the year, as an acquisition year does.

## Measured

This is the history of both filers, before and after, on the same database.

**KDP, 15 of 66 points:**
- 2018-08-08, 2018-08-09 and 2018-11-09 are now refused (they were 11.42, 11.34
  and 0.29).
- 2019-03-01 to 2021-10-29: 12 points move from a growth on DPS's years to
  terminal growth (no year filed after the close lies 3 years back). For
  example, 2020-02-28 goes 39.06 -> 31.32 and 2021-10-29 goes 41.84 -> 33.87.
- From 2022-02-26 on, FY2021 against KDP's own FY2018 ($7,442M, half combined)
  gives 19.45%, clamped to 10%: the same value as before, with the 5-year rate
  no longer shown.

**JCI, 1 of 69 points:**
- 2016-11-24 goes -0.02 -> -2.65 (Tyco's FY2013 start replaced by terminal
  growth).
- From 2017-11-23, FY2017 against Johnson Controls Inc.'s FY2014 gives 21.75%,
  clamped to 10%, the same as before. Its points 2017-02 to 2019 are refused
  for NCI or mixed bases anyway.

Today's values are unchanged for both. The line check flags 0 for both. The
rule acts only on the two listed symbols, so no other filer's history moves.

## Not done

- No rule finds a reverse merger. The two were found by the decision 72 census
  of start years. A new one shows as a level break at a merger in the
  statements, and needs reading.

Tests: `tests/unit/test_reverse_mergers.py`:
- DPS's year is not a start year for KDP's;
- DPS's own points still read DPS's years;
- a year re-filed after the close is a start year;
- the trailing-year refusal and its end;
- the two entries.

Each fails on a copy with the matching rule switched off.
