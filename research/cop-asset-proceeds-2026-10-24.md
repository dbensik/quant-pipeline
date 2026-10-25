# COP's asset-sale proceeds read as capex (decision 70)

Date: 2026-10-24
Status: A shipped (c4f568a). B built 2026-10-24; its 37 rows are stored.

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

## B (built): read COP's own capex concept

`cop_PaymentToAcquireProductiveAssetsAndInvestments` is the statement's capex line,
"Capital expenditures and investments". companyfacts does not carry it, but each
filing's XBRL instance does, and `core/xbrl_instance.py` already parses those
(decision 59).

**Where COP uses it.** I read every COP instance EDGAR has: 29 filings, from the
10-Q of 2019-08-01. Earlier filings have no `*_htm.xml`.

- From the Q1 2022 10-Q on, it is in every filing: 17 filings, 37 facts.
- The FY2021 and FY2022 10-Ks still tag gross capex as
  `PaymentsToAcquireProductiveAssets`.
- Where both concepts state a period, the figures are the same: FY2021 $5,324M,
  FY2022 $10,159M, and the 10-Qs' 2021 comparatives against the 2021 10-Qs.

So the concept is read FIRST and the list after it. It goes in
`LINE_CONCEPTS_FIRST_BY_SYMBOL`, not in `LINE_CONCEPTS_BY_SYMBOL`: a one-part sum
would drop every year before 2022.

`cop_SegmentReportingInformationCapitalExpendituresAndInvestments` is not stored.
It has the same figures, plus three-month periods from 2025, which would feed the
quarterly path.

**What was built**

- `settings.EXTENSION_CONCEPTS_BY_CIK` maps CIK 1163165 to `("cop",
  "PaymentToAcquireProductiveAssetsAndInvestments")`.
- `instance_company_facts(..., extensions)` keeps the named extension concepts
  under taxonomy = the prefix. It resolves the prefix from the instance's own
  namespace declarations, because the namespace is dated with each filing.
- `ingest_instance(..., extensions_only=True)` stores only those concepts.
  Companyfacts stays the source for the us-gaap rows.
- `load_fundamentals.py` adds an `OWN` pass. A filer in the map, with a filing in
  the window and none of its own concepts stored, has that filing read for those
  concepts.
  - The window is the 06:00 step's 30-day index, or the `--instances-since` sweep.
  - A filing that never tags the concept (the FY2021 and FY2022 10-Ks) is read
    again on each run while it is in the window: 2 requests, inserting 0.
- `LINE_CONCEPTS_FIRST_BY_SYMBOL["COP"]["capex"]` names the concept.
- The read side needed no change. Facts are fetched by concept name, and
  `ALL_CONCEPTS` already includes the per-filer orders.
- Decision 67's negative rule does not cover the concept, because
  `GROSS_CAPEX_CONCEPTS` is built from the list. All 37 values are positive.
- A's nine set-asides stay in place. The concept read first makes them rarely
  reached, but they record what the statements show.

**Stored.** `load_fundamentals.py --instances-since 2022-01-01 --symbols COP`
wrote 37 rows: taxonomy `cop`, source `instance`, 17 accessions. A rerun inserts 0.
Undo: `DELETE FROM fundamental_facts WHERE cik = 1163165 AND taxonomy = 'cop'`.

**Impact.** Of 70 history points, 17 change: 7 move and 10 that were refused are
now valued. Every TTM matches the arithmetic off the statements (for example,
2022-05-06: 5,324 + 3,161 - 1,200).

| as_of | price | per share after A | after B, capex only | capex after A | after B |
|---|---|---|---|---|---|
| 2022-05-06 | 107.69 | 294.26 | 255.11 | FY2021 5,324 | TTM 7,285 |
| 2022-08-05 | 92.85 | 374.94 | 320.91 | FY2021 5,324 | TTM 7,988 |
| 2022-11-04 | 132.32 | 355.25 | 293.92 | FY2021 5,324 | TTM 9,183 |
| 2023-05-05 | 100.37 | 275.94 | 279.88 | FY2022 10,159 | TTM 9,895 |
| 2023-08-04 | 113.71 | 208.00 | 197.97 | FY2022 10,159 | TTM 10,850 |
| 2023-11-03 | 119.75 | 163.75 | 152.78 | FY2022 10,159 | TTM 10,898 |
| 2024-02-16 | 110.57 | 132.41 | 117.30 | FY2022 10,159 | FY2023 11,248 |
| 2024-05-03 | 122.23 | refused | 111.05 | not reported | TTM 11,267 |
| 2024-08-02 | 105.77 | refused | 130.93 | not reported | TTM 11,313 |
| 2024-11-01 | 107.84 | refused | 149.14 | not reported | TTM 11,684 |
| 2025-02-19 | 99.57 | refused | 133.11 | not reported | FY2024 12,118 |
| 2025-05-09 | 88.59 | refused | 158.76 | not reported | TTM 12,580 |
| 2025-08-08 | 93.91 | refused | 132.26 | not reported | TTM 12,897 |
| 2025-11-07 | 86.83 | refused | 150.31 | not reported | TTM 12,847 |
| 2026-02-18 | 110.52 | refused | 109.44 | not reported | FY2025 12,553 |
| 2026-05-01 | 123.19 | refused | 87.86 | not reported | TTM 12,123 |
| 2026-08-07 | 117.61 | refused | 155.86 | not reported | TTM 11,861 |

With COP valued again, the 06:00 line check reads its revenue line, which it had
skipped while COP was refused. It flagged it: contract revenue, $51,824M, is a part.
The FY2025 statement's "Sales and other operating revenues" is $58,944M, filed as
`Revenues`.

`LINE_CONCEPTS_FIRST_BY_SYMBOL["COP"]["revenue"] = ("Revenues",)` fixes that. It
moves 23 points through growth alone:
- 2019-2022: by 0.2-9%.
- 2024-02-16: 117.30 to 122.57.
- 2026-08-07: 155.86 to 140.45. Growth is -9.11%, the 3-year CAGR against FY2022's
  $78,494M.

After this, the full line check flags none of the 538 equities.

Today, COP was refused for capex. It is now valued at 140.45 against a price of
150.80. The 2022 points are still about 2.5x the price, now on the real capex.
What is left there is the model, not the capex line.

Tests:
- `test_xbrl_instance.py`: the concept is kept only when named, and an `OWN` read
  stores only it.
- `test_reviewed_aside.py`: the TTM reads the concept ahead of the list.

Each test fails on a copy with the change removed.
