# One concept, two meanings: EQT's capex in 2014-2016 (decision 66)

2026-10-24. Measured on "go on 66"; A built the same morning on "go on 66 A".

## Built (A)

- `settings.FACTS_SET_ASIDE_BY_SYMBOL`: the three (concept, accession)
  entries, each with what its statement shows.
- `Statements(..., reviewed_aside=)` drops those facts on load
  (`core/valuation_service.filer_statements` passes the filer's entries, so
  the Research page's statements read the same).
- `build_inputs` names each set-aside filing once while a fact of it is
  within the annual age and no kept fact of the concept reports its period
  (`Statements.reviewed_aside_unreplaced`).
- Live, unpatched: all 69 EQT history points equal the measured column
  under A below; the 8 moved and 61 did not. Notes appear on the 8 (e.g.
  2015-02-13 names 15-000004 and 14-000004); 2017-02-10 and today carry
  none. Tests `tests/unit/test_reviewed_aside.py` (5); 4 mutants on a copy
  all killed; suite 2,025 pass. No other filer has an entry, so no other
  filer's statements change.

## What is wrong

The DCF reads capex from `PaymentsToAcquirePropertyPlantAndEquipment` first,
for every filer. In three EQT filings, EQT used that concept for its
**acquisitions** line, not its capex. Its real capex sits on an extension
concept, `eqt_PaymentsToAcquirePropertyPlantAndEquipmentContinuingOperations`,
which companyfacts does not carry. Each filing's own cash-flow statement
(read through `FilingSummary.xml`):

| accession | form, filed | `PaymentsToAcquirePropertyPlantAndEquipment` is labelled | its values | capex on the same statement (eqt: concept) |
|---|---|---|---|---|
| 0000033213-14-000004 | 10-Q, 2014-10-23 | "Payments to Acquire Property, Plant, and Equipment", beside "Capital expenditures from continuing operations" | 9M 2014: $163M | $1,606M |
| 0000033213-15-000004 | 10-K, 2015-02-12 | "Capital expenditures for acquisitions" | FY2012 $0, FY2013 $114M, FY2014 $174M | FY2014 $2,277M |
| 0000033213-16-000018 | 10-K, 2016-02-11 | "Capital expenditures for acquisitions" | FY2013 $114M, FY2014 $174M, FY2015 $0 | FY2015 $2,434M |

Every other EQT filing from 2013 to 2017 that tags the concept uses it for
capex: the 2013 10-Qs, all 2016-2017 10-Qs, and the 10-K filed 2017-02-09,
which restates FY2014 as $2,277M and FY2015 as $2,434M under it. That 10-K's
figures match the extension concept in the 2015 and 2016 10-Ks to the dollar.
So the concept changed meaning in those three filings and changed back.

Since 64, no per-filer order can fix it. Any order that skips the concept in
2015-2016 also skips it in 2017 onward, where it is right.

## What it does to EQT's history

There are 69 history points. The 8 from 2015-02-13 to 2016-10-28 read capex
of -$476M to $233M, against about $2B on both sides. Those 8 come out
**positive** at $139-186 a share, while the points on both sides are
negative, at -$246 (2014-10) and -$117 (2017-02). The negative TTMs come
from mixing a 10-K's acquisitions figure with 10-Q capex quarters.

## Options

**A. Set aside those facts by accession (recommended).** Add a settings
entry for each (filer, concept, accession), with what the statement shows,
read off the filing. The loader drops those facts. A point where a dropped
fact would have been read gets a note, as with decision 61. The point then
reads the next concept, `PaymentsToAcquireProductiveAssets` (EQT's segment
"expenditures for segment assets"). That is the concept every EQT point
before 2014-10 already reads.

Measured, with the facts dropped in memory (scratch `hist2.py`):

| as of | capex now | capex under A | per share now | per share under A | price |
|---|---|---|---|---|---|
| 2015-02-13 | 174 | 2,900 | 163.26 | -246.31 | 81.18 |
| 2015-04-24 | 233 | 2,959 | 148.46 | -259.11 | 86.59 |
| 2015-07-24 | -127 | 2,599 | 185.71 | -221.71 | 74.28 |
| 2015-10-23 | -194 | 2,532 | 169.66 | -237.40 | 69.44 |
| 2016-02-12 | 0 | 2,344 | 146.29 | -203.73 | 59.11 |
| 2016-04-29 | -230 | 2,115 | 142.70 | -189.82 | 70.10 |
| 2016-07-29 | -476 | 1,868 | 167.52 | -141.61 | 72.86 |
| 2016-10-28 | -265 | 2,080 | 138.55 | -170.55 | 66.01 |

($M TTM; per share in $.) Exactly these 8 points move. The other 61 EQT
points, and today's valuation, are unchanged.

Under A the figures are not exact:

- **Accrual basis.** Segment expenditures include accruals and acquisitions.
  FY2014 reads $2,900M against $2,277M cash capex plus $174M of
  acquisitions. FY2015 is close: $2,344M against $2,434M.
- **TTM mixing.** The 2016 TTMs combine FY2015 segment expenditures with
  10-Q quarters on the restored concept. Against the cash basis of the 2017
  10-K they are -4%, -3% and +20%.

That is the same basis EQT's 2010-2014 points already use, and it gets the
direction of every point right. Cost: 1-1.5 h. That covers the settings
entry, the loader filter, the note, a test that fails on today's code,
mutants on a copy, and a history re-run checking that only these 8 points
move.

**B. Read EQT's extension concept.** This would be exact: $2,277M and
$2,434M. But it is the first non-us-gaap concept the DCF would read.
companyfacts does not serve extensions, so it would come through the XBRL
instance parser (decision 59) for these filings, plus a new path for
extension concepts in statements. That is 3-4 h, for 8 points on one filer.

**C. Refuse the 8 points.** This is honest, but it throws away a usable
figure. Under A, the next concept is one the same filings tag and the
earlier points already read.

**Not recommended: a standing check.** I counted every annual capex fact
whose value moves 3x or more between filings: 43 pairs on 11 filers. Most
are real restatements:

- discontinued operations: CCI's fiber sale, NRG, DD
- mergers and predecessors: TMUS with MetroPCS, ECHO (decision 63)
- per-filer orders already in place: ED, EXE

A check that fires on those would be ignored, then deleted.

## Seen, not checked

- **EQIX FY2012-13**: `PaymentsToAcquireProductiveAssets` was $25M/$74M,
  later restated to $1,099M/$697M. If EQIX tags
  `PaymentsToAcquirePropertyPlantAndEquipment` in those years, this concept
  is never read and nothing is wrong. Check 15 min.
- **MTCH FY2019**: $137M became $39M, which looks like the IAC separation.
  BE FY2017 went from $5M to $61M. Neither is checked.
- Negative TTM capex from concept mixing is a general question: open pick
  (e), about 30 min to count across filers.
