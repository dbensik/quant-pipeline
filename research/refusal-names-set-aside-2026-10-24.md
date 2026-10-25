# A refusal names what was set aside (decision 73)

Date: 2026-10-24
Status: built.

## The gap

Decisions 61, 66 and 67 set some facts aside rather than read them:
- dated after their own filing;
- set aside by review;
- a payment filed below zero.

A valued point names each one in its `notes`. But a refusal is a single reason
string, so a point refused BECAUSE of a set-aside fact said only "not reported". COP
from 2024-05 to 70 B read "capital expenditure not reported in a standard concept (not
reported since 2022-12-31)", and gave no sign of the nine 10-Qs set aside.

## Measured

This covers every equity's history (26,290 points, 4,149 refused), after 70 B:
- **Set-aside facts.** 138 refused points on 39 filers have a set-aside fact a
  reader would notice at that date.
- **Unrelated refusals.** Most refusals among them are about something else:
  - 2009 dates with no price;
  - beta;
  - NCI;
  - IBKR's interest refusals beside an amortization fact dated after filing;
  - CPB and JKHY's cash-flow refusals beside a negative capex fact;
  - ECHO's share-count refusals beside a debt fact dated after filing.
- **Capex refusals.** Where the set-aside fact is the refused line's own, there are
  35 capex refusals on 11 filers (CNP, CSX, EMR, GWW, HON, LUV, OMC, PCG, TPL, VEEV, WMB), each beside a capex
  payment filed negative.
- **COP.** Its 10 refused points would have been the reviewed-aside case. 70 B now
  values them.

## Built

`build_inputs` keeps each set-aside note with its concept. The four refusals of a
missing line now append the notes whose concept that line reads
(`Statements.concepts_read`: its order, or its sum's parts):
- CFO;
- capex;
- debt (the lines `Statements.debt()` reads);
- share count.

A set-aside fact of another line is not named.

For example, CNP 2011-05-06 now reads:

> capital expenditure not reported in a standard concept (not reported since
> 2010-09-30); free cash flow cannot be formed; PaymentsToAcquireProductiveAssets
> in 0001130310-11-000020 (filed 2011-03-30) set aside: filed negative ($-1,509M),
> and a payment is an outflow

**Result.** 35 refusal reasons gain the note, and every other reason is unchanged:
the 103 other refused points with a set-aside fact are byte for byte the same. No
value changes and no point is newly refused or valued. The reason is the HTTP 422
detail and the history point's `reason`, so the Valuation page shows it as is.

Tests (`tests/unit/test_filed_negative.py`):
- a capex refusal names the negative filing;
- a CFO refusal beside it does not.

Each test fails on a copy with the matching mutation.
