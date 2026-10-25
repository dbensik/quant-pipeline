# COP's asset-sale proceeds read as capex (decision 70)

Date: 2026-10-24
Status: A built (uncommitted). B named, not started.

## What was wrong

From Q1 2022 to Q3 2024, every ConocoPhillips 10-Q tags **"Proceeds from asset
dispositions"** as `PaymentsForProceedsFromProductiveAssets`. Each value is filed
negative, as an inflow. Its capex line, **"Capital expenditures and investments"**,
is on its own concept, `cop_PaymentToAcquireProductiveAssetsAndInvestments`, which
is not stored: companyfacts carries only us-gaap and dei. The 10-Ks up to FY2022
file gross capex as `PaymentsToAcquireProductiveAssets`.

`_ttm_ytd` mixes a 10-Q concept with a different fiscal-year concept when the
10-Q concept reports no fiscal year of its own. That is what lets a rename through
(WEC). COP's proceeds concept reports no fiscal year, so the TTM became
FY + YTD(proceeds) - prior YTD(proceeds):

| as_of | TTM read | made of |
|---|---|---|
| 2022-05-06 | $2,975M | FY2021 $5,324M + Q1 2022 -$2,332M - Q1 2021 $17M |
| 2022-08-05 | $2,533M | FY2021 + H1 2022 -$2,951M - H1 2021 -$160M |
| 2022-11-04 | $2,762M | FY2021 + 9M 2022 -$3,354M - 9M 2021 -$792M |
| 2023-05-05 | $12,303M | FY2022 $10,159M + Q1 2023 -$188M - Q1 2022 -$2,332M |
| 2023-08-04 | $12,684M | FY2022 + H1 2023 -$426M - H1 2022 -$2,951M |
| 2023-11-03 | $12,900M | FY2022 + 9M 2023 -$613M - 9M 2022 -$3,354M |
| 2024-02-16 | $12,900M | the same (the FY2023 10-K tags no stored capex concept) |

In 2022 the proceeds lowered capex. In 2023 they raised it, because a year of
large disposals sat in the prior-year YTD.

## Read off the statements

I read all nine 10-Qs' cash flow statements (R pages, EDGAR). The row tagged
`us-gaap_PaymentsForProceedsFromProductiveAssets` is labelled "Proceeds from asset
dispositions" in every one. The amounts below are the current-year YTD.

| accession | period | proceeds | capital expenditures and investments |
|---|---|---|---|
| 0001163165-22-000006 | Q1 2022 | 2,332 | 3,161 |
| 0001163165-22-000013 | H1 2022 | 2,951 | 5,129 |
| 0001163165-22-000017 | 9M 2022 | 3,354 | 7,626 |
| 0001163165-23-000011 | Q1 2023 | 188 | 2,897 |
| 0001163165-23-000023 | H1 2023 | 426 | 5,820 |
| 0001163165-23-000031 | 9M 2023 | 613 | 8,365 |
| 0001163165-24-000023 | Q1 2024 | 173 | 2,916 |
| 0001163165-24-000037 | H1 2024 | 178 | 5,885 |
| 0001163165-24-000052 | 9M 2024 | 217 | 8,801 |

No COP filing after 2024-10-31 uses the concept.

## Why this is COP alone, not a rule

I surveyed every filer that tags the net concept in 10-Qs but never for a fiscal
year (29 filers, 6 of them refused as Financials). At each history date, I listed
the capex TTMs that mix it with another concept:

- **LLY, IBM, AMZN, RSG, TDG, GLW** mix it, but there it is the filer's own capex,
  positive and in line with the fiscal year (for example, LLY's 9M 2014 $754M
  against FY2014 $1,163M). This is a rename, and the mix is right.
- **MCD (72 negative rows), NRG (52) and HWM (19)** file it negative, but no capex
  read reaches it at any history date, whether through the TTM or the annual path.
- **COP** is the only filer where it is a different line.

A rule would have to tell COP from LLY. Their structure is the same: the fiscal-year
concept has no YTD of its own in the year in progress. The sign would separate
them, but D and GLW use this concept as a true net, and decision 67 exempted it
from the negative rule for that reason. The decision-66 mechanism exists for this
case: a statement read once, one filer, set aside by accession.

## A (built): nine entries in `FACTS_SET_ASIDE_BY_SYMBOL["COP"]`

The capex reads fall back to the fiscal year. COP's history has 70 points. 7 values
move, and none is refused or un-refused:

| as_of | price | per share before | after | capex before | after |
|---|---|---|---|---|---|
| 2022-05-06 | 107.69 | 341.15 | 294.26 | TTM $2,975M | FY2021 $5,324M |
| 2022-08-05 | 92.85 | 431.55 | 374.94 | TTM $2,533M | FY2021 $5,324M |
| 2022-11-04 | 132.32 | 395.96 | 355.25 | TTM $2,762M | FY2021 $5,324M |
| 2023-05-05 | 100.37 | 243.93 | 275.94 | TTM $12,303M | FY2022 $10,159M |
| 2023-08-04 | 113.71 | 171.37 | 208.00 | TTM $12,684M | FY2022 $10,159M |
| 2023-11-03 | 119.75 | 123.06 | 163.75 | TTM $12,900M | FY2022 $10,159M |
| 2024-02-16 | 110.57 | 94.39 | 132.41 | TTM $12,900M | FY2022 $10,159M |

The 10 refused points from 2024-05-03 stay refused. Their reason now reads "not
reported since 2022-12-31" instead of "since 2023-09-30". This is a label change
only: it now names the last period with real capex.

**What A does not fix.** The TTM is honest now, but it is stale:

- The 2022 points read FY2021 capex ($5.3B). COP's 2022 spending ran at twice that
  ($7.6B in the first nine months; FY2022 $10.2B).
- 2024-02-16 reads a fiscal year that ended 14 months earlier.
- Every moved point is still 2-3x the price.
- COP is refused from 2024-05 onwards, because its capex is filed only on its own
  concept from the FY2023 10-K on.

Tests: `tests/unit/test_reviewed_aside.py` has two COP tests. They fail when one
accession is wrong (checked).

## B (named, not started): read COP's own capex concept

`cop_PaymentToAcquireProductiveAssetsAndInvestments` is the statement's capex line
in every 10-Q and 10-K since 2022. It is not in companyfacts. It is in each
filing's XBRL instance, which `core/xbrl_instance.py` already parses (decision 59).
Reading it needs three things:

- The instance loader stores one named extension concept for one filer.
  `fundamental_facts.taxonomy` would hold `cop`.
- `LINE_CONCEPTS_BY_SYMBOL["COP"]["capex"]` names it (`map_part` reads a bare
  concept name today; a non-us-gaap taxonomy is unchecked).
- The 06:00 SEC step keeps it current for new filings.

This would value the 7 points on the real line and un-refuse 10. The line includes
"investments" (equity-affiliate funding), which COP reports as its headline capital
program. I estimate 2-3 h of agent time, assuming the instance parse needs no new
rules for extension facts. It is its own decision because it changes what the
loader stores.
