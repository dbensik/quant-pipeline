# Capex read from the wrong concept in past years (decision 64)

2026-10-23. Built the same evening on "go on 64".

## What was wrong

The DCF reads capex from the first concept on one ordered list, for every
filer. The 06:00 line check (`scripts/check_line_concepts.py`) looks for a
better concept, but only in the newest fiscal year. Older years were never
checked, and every history point reads them.

ETR is the case that showed it. Entergy tags two capex concepts:

- "Construction/capital expenditures" as `PaymentsForCapitalImprovements`:
  $1,931M (FY2009), $2,040M (FY2011) and $6,087M (FY2021).
- A smaller line as `PaymentsToAcquireOtherPropertyPlantAndEquipment`, which
  the list ranks first: "Payment for purchase of plant", $646M in FY2011 and
  $0 in most years.

So ETR's points from 2010 to 2024 were valued on capex of $0 to $949M, at 2 to
5 times their price.

## The count

Every equity, every fiscal year from 2008: where the concept the reader chose
is under half of another capex concept the filer tagged for the same year
(and more than $50M under it). This is the reader's own choice, so the
per-filer maps and refusals apply (scratch `census.py`). 17 symbols, 84
filer-years. All but four were read off one or more of their 10-K cash-flow statements
(through `FilingSummary.xml` and the statement page):

| symbol | years | list read | statement's capex | fix |
|---|---|---|---|---|
| ETR | FY2008-21 | "purchase of plant", $0-949M | `PaymentsForCapitalImprovements` | first |
| URI | FY2009-20 | non-rental equipment only, $28-218M | `PaymentsToAcquireProductiveAssets` = rental + non-rental | first |
| ED | FY2009-15 | non-utility construction, $9-492M | `PaymentsForConstructionInProcess` (utility) | first |
| XEL | FY2008-12 | $0 (FY2010 $732M) | `PaymentsToAcquireProductiveAssets`, $2,206M FY2011 | first |
| DVN | FY2013-19 | "Acquisitions of property, equipment and businesses" | `PaymentsToAcquireProductiveAssets` | first |
| MCHP | FY2008-17 | "Investments in other assets", $5-22M | `PaymentsToAcquireOtherPropertyPlantAndEquipment` | first |
| EXE | FY2017-19 | $35-128M | "Drilling and completion costs", `PaymentsToExploreAndDevelopOilAndGasProperties` | after ProductiveAssets |
| APA | FY2008-09 (Apache) | "Acquisition of Anadarko properties", $0 | "Additions to oil and gas property", `PaymentsToAcquireOilAndGasProperty` | first |
| FIS | FY2008-23 | additions to property and equipment | the same; software is its own line, already added as intangible capex | none |
| WMB, ABT | 3 + 1 years | capex | the same; the larger concept includes acquisitions | none |
| FANG, CHTR | 2 + 1 years | capex | not read: the larger figure is the size of an acquisition (FANG's FY2024 with Endeavor, $8,920M; CHTR's TWC, $28,810M) | none |
| CHRW, NRG | 1 + 1 | capex | not read: the other concept is negative, a net figure | none |
| EQT | FY2012-17 | see below | depends on the filing | none (seen) |
| QCOM | FY2010 | "Capital expenditures" $426M | the same; the larger concept is "Advance payment on spectrum" ($1,064M) | none |

## What was built

- `settings.LINE_CONCEPTS_FIRST_BY_SYMBOL`: for each filer and line, concepts
  read AHEAD of the list, with the rest of the list after them. Each entry
  says what the statement shows.
- `modeling.statements.FirstOf` carries it inside `line_concepts`, so every
  copy of a filer's statements keeps it (the line check's, the intangible
  capex fallback's). Per period, the first concept in that order with a row
  wins, as for any filer. The TTM's concept rules are unchanged.
- `core/valuation_service.filer_line_concepts` combines a filer's sums and
  orders. `ALL_CONCEPTS` loads the order's concepts too.
- It is an order and not a sum because the filers changed concepts:
  - ETR's capex is `PaymentsToAcquirePropertyPlantAndEquipment` from FY2022.
    A one-concept map would have dropped those years and refused today's
    valuation.
  - ED's `PaymentsToAcquirePropertyPlantAndEquipment` is non-utility capex
    to FY2015, then a figure on no statement line ($5,235M in FY2016, against
    $2,835M utility + $845M non-utility).
- The 06:00 line check treats an order as its review, as it does a map. An
  order falls back to the list, so it is never stale.

`tests/unit/test_capex_concept_order.py` has 3 tests. Three mutants were each
run on a copy, and each was killed:
- the order ignored: the TTM test fails;
- the order read as a sum: the fall-back test fails;
- the settings not read: the per-filer test fails.

Suite: 2,017 tests, 0 failures, 1 skipped. Ruff is clean.

## Effect

History over every equity: 26,275 points, before (6d8f323) and after.

- 199 points on these 8 symbols moved, and no other point. Nothing crashed,
  and no point was added or lost.

  | symbol | points | dates | median value/price, before -> after |
  |---|---|---|---|
  | ETR | 54 | 2010-05 to 2024-11 | 3.77 -> -0.66 |
  | ED | 48 | 2012-02 to 2023-11 | 1.98 -> -0.04 |
  | URI | 39 | 2012-04 to 2021-10 | 4.79 -> 0.52 |
  | DVN | 24 | 2015-02 to 2020-10 | 3.03 -> -0.37 |
  | XEL | 12 | 2011-03 to 2013-10 | 2.93 -> -0.66 |
  | MCHP | 12 | 2017-05 to 2020-02 | 1.64 -> 1.44 |
  | APA | 6 | 2010-02 to 2014-11 | 3.16 -> 0.87 |
  | EXE | 4 | 2021-05 to 2022-02 | 4.72 -> 1.07 |

- DVN 2018-05-03 and 2018-11-08 are now refused: the partners' share of
  income is over half of base FCF (decision 17's rule) once base FCF is
  measured on real capex.
- The utilities (ETR, ED, XEL) now value at about zero or below in these
  years. A growing regulated utility spends more than its operating cash
  flow and funds the gap with debt, so its free cash flow is negative. That
  is what the DCF says of every utility; before, these three had looked
  cheap only because their capex was missing.
- Valuations as of 2026-10-23: 389 valued before and after, and no value
  moved. Every fix reaches past years only.

## Seen, not done

- **Not what was proposed.** Decision 64 A said `LINE_CONCEPTS_BY_SYMBOL`
  entries. Those are sums. ETR's would have dropped FY2022-25, the years it
  tags under the list's first concept, and so refused today's valuation.
  Hence the second mechanism.
- **The line check skips these 8 filers' capex**, as it skips every mapped
  line. For ETR, the newest year is read from the list
  (`PaymentsToAcquirePropertyPlantAndEquipment`), not from the concept the
  order names, so that year is unchecked. It was right when read.
- **The Research page changes too.** `filer_statements` reads the order, so
  these 8 names' capex lines change there as well: ETR's FY2009 goes from $0
  to $1,931M.
- **EQT** (FY2012-17): one concept means different things in different
  filings. In the 10-K filed 2015, `PaymentsToAcquirePropertyPlantAndEquipment`
  is acquisitions ($0, $114M, $174M) and `PaymentsToAcquireProductiveAssets`
  is capex. From the 10-K filed 2017, the first is capex ($2,277M, $2,434M,
  $1,539M) and the second is capex plus acquisitions. No order is right for
  every filing. Points from 2013 to 2016 read about $0.1B against about $1.8B.
  Fixing this needs a rule by filing date.
- **Under-read, not wrong:**
  - ED leaves out non-utility capex: 5-23% of capex in FY2009-22, under its
    own extension concept from FY2023.
  - URI's FY2009-10 leave out non-rental equipment ($51M and $28M).
- **The census as a standing check.** The 06:00 line check still reads only
  the newest year. A new filer whose old years are wrong would only be found
  by running the census again. Making it a job step would mean reviewing
  about 30 lines a run (FIS's 16 are right as read), or a reviewed list for
  them.
- **The census also turned up revenue and CFO lines** (PM 9 years, URI 7,
  ETN 6, TDY 5, and others: 62 filer-years on 22 symbols) where another concept is
  over twice the one read. Revenue feeds only growth, and CFO's continuing-
  operations-first rule is deliberate. None of these was read.
- FANG, CHTR, CHRW and NRG were judged from the figures, not read off the
  statement. Each reads the list's first concept, which is the usual capex
  line.
