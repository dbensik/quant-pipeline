# Revenue and CFO read from the wrong concept in past years (decision 65)

Built 2026-10-23. Follows `capex-concept-order-2026-10-23.md` (decision 64), whose
census also listed 64 revenue and CFO filer-years nobody had read.

## The census, redone point in time

Decision 64's census valued every fiscal year as of today. That flags restatements
that no history point ever read: BAX's FY2013 continuing CFO ($1,289M against
$3,198M) is the post-Baxalta recast, first filed in 2016, and on 2014-02-22 the DCF
read the 10-K's $3,198M. So the census was rerun at every history date (the day
after each 10-K/10-Q), on the values the DCF actually uses there: the latest fiscal
year, and for revenue the 3- and 5-year growth start years (`annual_before`).
Flagged: another listed concept reports the same period over twice as large, by over
$50M. Revenue, CFO and capex, 546 equities, 23 s.

73 rows. Capex has 25: 24 are decision 64's verdicts again (EQT among them), plus one it missed (GLW,
below): FY2019 was replaced by a later filing, so today's census saw a different
concept. The 48 revenue and CFO rows, each read off the filing's own pages
(`FilingSummary.xml`, the row carrying the concept):

| filer | years reached | the concept read is | verdict | change |
|---|---|---|---|---|
| ETN | FY2010-16 | Revenues: "Entity-wide revenue, major customer, amount", $0; "Net sales" is SalesRevenueNet ($22,046M FY2013) | wrong | SalesRevenueNet first |
| HAS | FY2013-16 | Revenues: "Net revenues by geographical location", US and Canada ($1,972M of $4,448M FY2015) | wrong | SalesRevenueNet first |
| HSY | FY2013 | Revenues: "Total Net Sales to McLane, Inc." ($1.8B of $7,146M) | wrong | SalesRevenueNet first |
| LHX | FY2010-13 | Revenues: business segments ($420M of "Total Revenue" $5,112M FY2013) | wrong | SalesRevenueNet first |
| PG | FY2014 | Revenues: "Net sales in the U.S." ($29.4B of $74,401M) | wrong | SalesRevenueNet first |
| TDY | FY2011-15 | Revenues: "Sales to international customers" ($1,020M of "Net Sales" $2,298M FY2015) | wrong | SalesRevenueNet first |
| EXC | FY2018 | contract revenue incl. tax: "Revenues from alternative revenue programs", -$62M; "Total operating revenues" $35,985M is Revenues | wrong | Revenues first |
| URI | FY2019-25 | contract revenue: the non-rental part ($3,695M of "Total revenues" $16,099M FY2025; rental is a lease) | wrong | Revenues first |
| IBKR | FY2018-19 | contract revenue: commissions ($925M of "Total revenues" $2,366M) | wrong, not changed | none: never valued (no debt balance), and from FY2020 no total is tagged under a listed concept |
| PM | FY2016-22 | "Net revenues" excluding excise taxes ($29,625M FY2018; $79,823M includes them) | right | none |
| HWM, DD, RTX, EBAY | one start year each | a recast onto the later basis: HWM's FY2018 $6,778M after the Arconic Corp separation, DD's FY2017 $11,672M (Dow as predecessor), RTX's FY2017 $29,713M (8-K, after Carrier and Otis), EBAY's FY2014 $8,790M without PayPal | judged from the figures and filing dates (each first filed after a separation or merger), not read off the statements: right, the latest year is on the same basis | none |
| EQT | FY2021 start year | "Total revenues" $3,065M, after a $3.8B derivative loss; gas sales $6,804M | the statement's total, kept | none |
| GE, MPC | FY2021, FY2020 | continuing-operations CFO ($888M, $807M) beside the total with discontinued | right by rule (continuing first, matching capex) | none |

And capex, outside the revenue and CFO scope proposed, found by the same census:

| GLW | FY2019 | PaymentsToAcquireMachineryAndEquipment: "Purchase of equipment for related party", $9M; "Capital expenditures" $1,978M is PaymentsForProceedsFromProductiveAssets, PaymentsForCapitalImprovements from the FY2020 10-K | wrong | those two first |

## What was built

- `settings.LINE_CONCEPTS_FIRST_BY_SYMBOL` gains revenue orders for ETN, HAS, HSY,
  LHX, PG, TDY (SalesRevenueNet), EXC and URI (Revenues), and a capex order for GLW,
  each with the statement's line beside it. No new mechanism for the line itself.
- **One change of rule, beyond the plan:** `Statements.annual_before` reads a start
  year by the FirstOf order, per period, as `annual()` does, instead of on the latest
  year's own concept. The orders alone did not fix HAS, HSY, LHX and PG: each tags
  its total as Revenues in later years and a segment as Revenues in earlier ones, so
  "latest's own concept" still read the segment ($1,972M against $4,580M for HAS's
  FY2015-18 growth). Only filers with an order for the line are affected; sums and
  every other filer keep the old rule.
- `tests/unit/test_revenue_concept_order.py`: 4 tests. Removing the new branch from
  a copy of `statements.py` fails the first.

## Effect

History, against decision 64's sweep: 92 of 26,275 points changed, all on the 9
filers. 0 crashes, 0 extra points. Today: all 546 equities' answers (value or refusal) unchanged (URI's growth
is capped at 10% on either figure). PIT census after: the 31 fixed rows are gone,
nothing new. The 06:00 line check on the 9 symbols: 0 flags. Suite: 2,021 tests, 0
failures, 1 skipped. Ruff clean.

| filer | points | example |
|---|---|---|
| TDY | 20 | 2014-02-26 76.55 -> 84.77; 2019-02-26 257.55 -> 224.97 |
| ETN | 16 | 2014-02-27 71.79 -> 88.56; 2017-02-23 97.38 -> 81.83 |
| PG | 16 | 2019-08-07 95.92 -> 85.40 |
| HAS | 12 | 2019-02-27 114.33 -> 92.21 |
| LHX | 12 | 2013-08-27 169.11 -> 139.20 |
| URI | 8 | 2023-01-26 -13.29 -> 63.82 |
| GLW | 4 | 2020-02-19 60.86 -> -4.29 (capex $9M -> $1,978M) |
| HSY | 4 | 2014-02-22 83.16 -> 92.31 |

EXC's FY2018 points (2019) are refused for NCI already, so they did not move.

## Seen, not done

- `annual_before`'s rename rule accepts a concept that shares NO period with the
  latest's (`all([])` is true). That is how HAS's segment slipped in before the
  order. Measured after the build, on every filer without a revenue order: of 45,628
  start-year reads at history dates, 32,238 are on latest's own concept, 4,533 a
  rename with shared periods, 8,251 none, and 606 (on 43 filers) the vacuous case.
  None of the 606 is under half another concept's figure for its period, so the
  census could not see them. Some look like the HSY pattern by their figures alone
  (not read): CAG's Revenues of $1.5-1.7B (FY2011-13) beside contract revenue of
  $9.5-11.2B, JCI's $10.1B Revenues beside $20.8-37.7B sales; DD's FY2017 $62,484M
  start beside $13,017M is the pre-separation basis. MA's latest contract revenue
  ($21,831M) may be gross before rebates, a latest-year problem, not a start year's.
  Others are real growth by acquisition (WTW, MPC, XYZ, DXCM), which "require one
  shared period" would push to terminal growth: the honest fix is per-filer orders.
- The PIT census is a scratch script, not a 06:00 step (as for decision 64).
- IBKR's revenue has no listed total from FY2020; irrelevant while it is refused.
- The Research page reads the same orders, so its revenue rows for these filers'
  past years change too.
