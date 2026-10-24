# A quarterly TTM that mixes two lines (decision 68)

2026-10-24. Measured on "what's next?" after decision 67; A built the same
day on "go on 68 A".

## Built (A)

- `Statements._ttm_quarters` returns None when one of its four quarters
  comes from a concept that also reports that quarter's fiscal year 0.5% or
  more off the year's own concept (`_separate_line`, the census's rule). The
  TTM then takes FY + YTD - prior YTD, or the annual. A quarter of the year
  in progress has no fiscal year to judge it by and is not checked, as in
  the census.
- Live, every history point of the 69 filers (4,351) valued with the
  committed and the built `_ttm_quarters`: 119 move, and they equal the
  measured table below exactly; no other point moves.
- Today, one filer moves: DVA, $414.54 to $410.47 (-1.0%, price $183.64),
  its 2026-08-05 point (D&A and interest expense each read from four
  concepts). "Today's value of every filer is unchanged" below was wrong.
- Tests `tests/unit/test_ttm_quarter_mix.py` (3; the FIS-shaped one fails
  on the committed code, the other two pin what still mixes). 6 mutants on
  a copy: 5 killed. The survivor drops the same-concept shortcut, and no
  test tells it apart: the shortcut only skips a lookup whose answer would
  be the year itself. Suite 2,033 pass.
- `research/negative-capex-2026-10-24.md`'s FIS line is corrected.

## What was seen

Decision 67's census flagged FIS on 59 points: its derived Q4 capex is
negative every year (FY2022 -$511M). FIS's 10-K and Q1 10-Q tag gross capex
as `PaymentsToAcquirePropertyPlantAndEquipment` (FY2022 $268M). Its Q2 and Q3
10-Qs file a three-month `PaymentsToAcquireProductiveAssets`, which is
property plus software (FY2022 $1,453M). PPE has only a year-to-date fact for
those quarters. ProductiveAssets is property, software and a small
remainder (FY2021: 320 + 931 = 1,251 against 1,286; FY2022: 268 + 1,122 =
1,390 against 1,453), and the software is already read as its own line
(`intangible_capex`). `quarterly()` takes each quarter from the first concept that
has one, so Q2 and Q3 come from the larger line, and Q4 = FY(PPE) - Q1..Q3
goes negative.

This is a rule the code already states and does not apply everywhere.
`_ttm_ytd` refuses to mix a 10-Q concept that ALSO reports the fiscal year
at a different value: that is a separate line, not another name (DAL,
2026-10-12). `_ttm_quarters` has no such check, and `ttm()` takes the
quarters when both paths end on the same date.

## The census

At every history date of every filer (26,290 points), for each flow the DCF
reads through `ttm()`, the four-quarter TTM was flagged when it won and one
of its quarters came from a concept that also reports that quarter's fiscal
year more than 0.5% away from the year's own concept (scratch
`d68/census.py`).

- 887 item-points across 69 filers: D&A 578, interest expense 215, capex
  78, interest paid 16.
- Usually the mix cancels out. Q2 + Q3 + Q4 is FY - Q1 whatever Q2 and Q3
  read, so a TTM that holds the derived Q4 is right. Only the new year's
  quarters, before their Q4 exists, carry the other line. Where the YTD path
  also ends on the same date, the two differ by more than 5% on 13 capex,
  20 D&A and 17 interest points.

## What it does

The 840 flagged points re-valued in memory with `_ttm_quarters` refusing
such a mix, so the TTM falls to the YTD path or the annual (scratch
`d68/impact.py`):

- 119 points move. None is refused or un-refused.
- 72 move under 2%, 33 move 2-10%, and 14 move 10% or more.
- The 14 are mostly values already negative or near zero:
  - APA 2016-08 (-$85.67 to -$33.03)
  - EQT 2017-07 and 2018-07
  - BE 2019 twice
  - COP 2021-05 ($0.03 to -$0.22)
  - ED 2020-05
- Five of the 14 are positive values that fall:
  - ED 2014-05 ($35.96 to $28.00, price $56.20)
  - ED 2014-08 ($43.53 to $35.45)
  - ECHO 2013 twice (-17% and -10%)
  - APA 2017-08 (-24%)
- Two of the 14 are positive values that rise: MDLZ 2013-08 (+17%) and
  TTWO 2025-11 (+17%).
- FIS: 11 points move, all under 2.2% except 2020-08-05 (+8.1%, $41.49 to
  $44.83 against $146.50). The "off by up to about 40%" in decision 67's
  "Seen, not checked" compared the TTM with the fiscal year, not with a
  correct TTM. This note supersedes that line, which is to be amended in
  the same commit as A or B.
- Today's value of every filer is unchanged. (Wrong: DVA moves 1.0%, see Built.)

## Options

**A. The quarters path follows the YTD path's rule (recommended).** A
quarter from a concept that reports its own fiscal year at a different
value ends the four-quarter TTM. The TTM then falls to FY + YTD - prior YTD
(which takes one concept for both YTDs) or to the annual. This is the rule
`modeling/CLAUDE.md` already states ("a TTM never mixes two concepts that
each report the fiscal year"), which the quarters path breaks today. It
moves 119 points, 72 of them under 2%, and changes no refusal. For FIS, the one filer read, the fallback
is PPE alone: at 2022-08-05 it is FY2021 320 + 6M 2022 173 - 6M 2021 143 =
350, where the quarters read 385. Software stays in `intangible_capex`, so
the PPE-only figure is the right line there. The other 68 filers have not
been checked against a statement, so for them A is consistency, not a
confirmed correction. Cost: 1-1.5 h of my time (rule,
a test that fails on today's code, mutants on a copy, history re-run that
only the 119 move).

**B. Record it and correct the doc.** Leave the code. Change the
CLAUDE.md line to say the quarters path mixes and why it mostly cancels.
15 min.

**C. FIS only, by map.** Map FIS capex in `LINE_CONCEPTS_BY_SYMBOL` to PPE
alone. A first-in-order entry would not help, because PPE is already first
and has no Q2 or Q3 quarter. It leaves the other 68 filers as they are.
30-45 min.
