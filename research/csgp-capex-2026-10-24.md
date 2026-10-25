# CSGP's capex is two lines, and the DCF reads one (decision 71)

Date: 2026-10-24
Status: measured. Nothing built. One recommendation (A), awaiting a decision.

## The question

The negative-capex note (decision 67) left this open: "CSGP Q1 2022 reads $0
against $123M a year earlier". Was $0 really filed?

## What the filings show

Yes, $0 was filed. But that $0 is not CSGP's capex. From its FY2021 10-K on,
CoStar's cash-flow statement has two purchase lines. I read the Q1 2022 10-Q, the
FY2024 10-K and the Q1 2021 10-Q, and the tags of all 28 instances from 2019:

| line on the statement | concept | FY2021 | FY2022 | FY2023 | FY2024 | FY2025 |
|---|---|---|---|---|---|---|
| "Purchase of Richmond assets" (2021-22), "Purchases of property, equipment, and other assets for new campuses" (2023-) | us-gaap `PaymentsToAcquirePropertyPlantAndEquipment` | 123.8 | 35.2 | 117.5 | 579.0 | 307 |
| "Purchases of property and equipment and other assets" | `csgp_PaymentsToAcquirePropertyAndEquipmentAndOtherAssets` | 65.2 | 58.6 | 25.3 | 58.9 | 82 |

Figures are in $M.

- **The Q1 2022 10-Q.** Richmond is $0 against $123,259K a year earlier. The
  ordinary line is $12,416K, against $10,619K.
- **Before the FY2021 10-K.** The ordinary line was us-gaap
  `PaymentsToAcquireProductiveAssets`. The Richmond purchase in the 2021 10-Qs was
  `PaymentsToAcquireAssetsInvestingActivities`, which is not on the capex list.

So the DCF read two different things over time:

- **To 2021-10.** It read the ordinary line alone, because Richmond's concept is not
  listed. The TTMs were $28-52M.
- **From the FY2021 10-K on.** It read the campus line alone, because the ordinary
  line moved to CSGP's own concept, which is not stored. Q1 2022 got
  $123.8M + $0 - $123.3M = $0.5M. 2024 got the $579M campus purchase without the
  $59M ordinary capex.

Neither period reads both lines, which together are CoStar's capex.

## Impact today (what is read)

| as_of | capex read | the two lines together (the statement) |
|---|---|---|
| 2022-04-29 | TTM 0.5 | 67.5 |
| 2023-02-23 | FY2022 35.2 | 93.8 |
| 2025-02-21 | FY2024 579.0 | 637.9 |
| 2026-07-30 | TTM 217.0 | 270 |

From 2024, the campus spending already puts CSGP's DCF value under $6 a share
against a price of $29-92. Adding the ordinary line lowers it further. 2022-2023 is
where the missing line matters most, relative to what is read.

## Options

**A. Store CSGP's own concept and read both lines (recommended).**
- Store the concept through `EXTENSION_CONCEPTS_BY_CIK`, built in decision 70 B. No
  new loader code is needed.
- From the FY2021 10-K on, read capex as the SUM of the campus and ordinary lines.
  Before it, read the list, as today.
- This needs one new reading rule: a per-filer sum, used where all its parts report
  a period, and the order otherwise.
  - A plain `LINE_CONCEPTS_BY_SYMBOL` sum would drop every period before 2022, so
    CSGP would be refused from 2010 to 2021.
  - A `FirstOf` cannot add two concepts.
- Richmond's 2021 purchase, on `PaymentsToAcquireAssetsInvestingActivities`, would
  stay out of the 2021 TTMs, as today. That concept is also every filer's total of
  investing purchases, so I would not list it.
- Cost: 1.5-2 h of agent time. That covers the rule in `Statements`, the sweep (28
  CSGP instances), tests that fail on today's code, and a history re-run showing that
  only CSGP points move. It assumes the rule needs no change to the TTM mix check,
  because both concepts are tagged in the same filings.

**B. Read the ordinary line only (`FirstOf` the CSGP concept).**
- No new rule, 30 min.
- It treats the campus purchases ($579M in 2024) as not capex, but they are
  property CoStar bought for its own use.
- It matches what was read before 2022.

**C. Nothing.**
- CSGP is valued on the campus line alone from 2022: $0.5M for Q1 2022, and the
  ordinary $25-82M a year missing throughout.
