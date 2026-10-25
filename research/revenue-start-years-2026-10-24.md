# Growth start years from a concept that shares no period (decision 72)

Date: 2026-10-24
Status: built. Follows `research/revenue-cfo-concept-order-2026-10-23.md`
(decision 65), "Seen, not done".

## The gap

`Statements.annual_before` reads a growth start year from latest's own concept.
Failing that, it falls back to another concept, provided the two agree on every
period both report. When they report NO period in common, the check is `all([])`,
which is true, so the other concept is accepted unchecked.

I re-ran the census on every valued filer at every history date: **606 reads on 43
filers**, the same as decision 65 counted. Most are the clean ASC 606 switch, where
`SalesRevenueNet` stops at FY2017 and contract revenue starts at FY2018. Those are
renames, and right.

## Triage

I first measured which reads move a value, before reading any statement:
- For each read, I recomputed growth with the start year dropped.
- 247 points on 30 filers change growth.
- 109 points on 19 filers move value/price by 0.05 or more and by 5% or more, the
  line check's thresholds.

For those 19, I printed each concept's annual series and looked for a level break
where the concept changes. Then I read the statement of each suspect.

| filer | the start year read | the statement | verdict |
|---|---|---|---|
| CAG | Revenues, $1,500-1,700M (FY2012-16) | FY2016 10-K "Net sales" $11,642.9M, on a concept not stored | a part, read as the start AND, before FY2017, as the latest year |
| EBAY | Revenues FY2014 $17,902M (with PayPal, the FY2014 10-K) | FY2015 10-K "Net revenues" FY2014 $8,790M, `SalesRevenueNet` | wrong basis |
| GEN | Revenues FY2014 $6,676M (with Veritas) | FY2016 10-K "Net revenues" FY2014 $4,183M, `SalesRevenueNet` | wrong basis |
| FE | `SalesRevenueNet` FY2016 $14,562M (with FES) | FY2018 10-K "Total revenues" FY2016 $10,700M, `Revenues` | wrong basis; contract revenue is also a part of the total |
| MA | latest year: contract revenue FY2018 $21,831M | FY2018 10-K "Net revenue" $14,950M, `Revenues` | contract revenue is before rebates and incentives, tagged FY2018-21 only |
| KDP | DPS's `SalesRevenueNet` (FY2013-17) | FY2018 10-K: Keurig is the accounting predecessor ($4,269M FY2017) | wrong company; not fixed (below) |
| JCI | Tyco's fiscal-year Revenues (CIK 833444 is Tyco plc) | FY2016 10-K "Net sales" $37,674M | wrong company; 1 point, value/price -0.00 to -0.06; not fixed |
| CTSH, FIX, IP, CLX, TGT, XYL, FCX, MPC, CVX, SNA, MGM, WTW | | continuous across the switch, or real growth by acquisition (WTW 2016, MPC 2018) | right as read |

DD (pre-separation, $62,484M) moves no value.

## Built

- `LINE_CONCEPTS_FIRST_BY_SYMBOL` (orders, read per period):
  - EBAY: contract, then `SalesRevenueNet`.
  - GEN: contract (both), then `SalesRevenueNet`.
  - FE: `Revenues`.
  - MA: `Revenues`.

  Each puts the recast or statement concept ahead of the one the list ranks
  first.
- `LINE_CONCEPTS_BY_SYMBOL["CAG"]["revenue"]` = contract revenue alone. No stored
  concept carries CAG's total before FY2017. Its points to FY2016 read no revenue,
  so growth is terminal ("too little revenue history"). Before this, growth was the
  part's CAGR.

**History re-run** of the five filers: 48 points move, and none is refused or
un-refused.

| filer | points | example |
|---|---|---|
| EBAY | 7 | 2020-02-01 39.32 -> 58.91 (growth -9.6% -> +5.1%) |
| GEN | 6 | 2019-05-25 24.25 -> 31.31 |
| FE | 10 | 2022-02-17 2.91 -> 8.22; 2024-2026 unchanged |
| MA | 16 | 2021-02-13 183.65 -> 171.45; 2017-18 by 0.3% |
| CAG | 9 | 2019-07-20 34.75 -> 25.92 (growth 10%, clamped, -> 2.5%) |

Today's value is unchanged for all five, because their newest start years were
already right. The full line check flags 0 of 538. The Research page reads the same
orders, so these filers' past revenue rows change there too.

## Not done

KDP and JCI: built as decision 74 (`research/reverse-mergers-2026-10-25.md`).

- **KDP and JCI.** Both are reverse mergers: the legal registrant's past (DPS,
  Tyco) is not the accounting predecessor's.
  - An order cannot fix it, because the right figures are under another CIK, or not
    stored.
  - Dropping the old concept would also take revenue away from those companies' own
    pre-merger points.
  - The honest rule is a growth start that refuses to cross a reverse merger. That
    is its own decision.
  - KDP: 11 points (2019-2020), growth 4.4% today.
  - JCI: 1 point.
- **The census is a scratch script**
  (`scratchpad/rev/census.py`, `impact.py`, `series.py`), as for decisions 64-65.

Tests: `tests/unit/test_revenue_concept_order.py`:
- EBAY's recast start year;
- CAG's part never read as a start;
- the five entries.

Each test fails on a copy with the entries removed.
