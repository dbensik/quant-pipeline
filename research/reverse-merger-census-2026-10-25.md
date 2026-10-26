# Other reverse mergers (decision 76)

Date: 2026-10-25
Status: built (A), 2026-10-25. Follows `research/reverse-mergers-2026-10-25.md`
(decision 74: KDP, JCI).

## The census

KDP and JCI were found by hand. In a reverse merger the registrant's PAST is replaced
by the accounting acquirer's, so its later 10-Ks re-report its old balance sheets as
another company's. Two signals in `fundamental_facts`, total assets (us-gaap
`Assets`) only:

- **A. Restated.** A later 10-K re-reports a balance date at under 0.8x or over 1.25x
  the first-filed value. This catches KDP (2017-12-31: 10.02 bn -> 15.74 bn).
- **B. New past.** A 10-K's comparative balance date was never reported by the CIK
  before, though it had filed 10-Ks. This catches JCI (Tyco's 2015-09-25 replaced by
  2015-09-30).

Both signals catch both known cases. Together they return 29 rows on 25 filers. Read
one by one:

| filer | what it is | verdict |
|---|---|---|
| KDP, JCI | decision 74 | listed |
| **TMUS** | MetroPCS (the registrant) acquired by T-Mobile USA for accounting, closed 2013-04-30; 2012-12-31 assets 10.19 -> 33.62 bn | **reverse merger** |
| **MRK** | Schering-Plough (CIK 310158) legally acquired Merck, closed 2009-11-03; 2008 assets 28.1 -> 47.2 bn | **reverse merger** |
| **VRT** | GS Acquisition Holdings (a SPAC) and Vertiv, closed 2020-02-07; 2019 assets 0.71 -> 4.66 bn | **reverse merger** (SPAC) |
| **ECHO** | EchoStar and DISH, common control, closed 2023-12-31; prior years recast as combined, 2022 assets 6.2 -> 58.8 bn | **same shape** (pooled, not acquired) |
| LIN, SW, ARES | a shell registrant first (holdco formation, or an IPO) | the past is a shell with no figures to mix |
| COIN, HOOD | customers' crypto assets off the balance sheet (SAB 121 rescinded) | not a merger |
| ERIE | the Exchange deconsolidated (2015 ASU) | not a merger |
| MRSH | fiduciary assets grossed up | not a merger |
| BAC, EQT | a zero re-reported | a data error; no DCF line reads `Assets` |
| IDXX, APD, BE, MRNA, ODFL | a balance date tagged one day off (01-01 for 12-31) | not a merger |
| HON, HST, TAP, SMCI | an earlier balance date simply not stored before | not a merger |
| CIK 6769 | Apache, read as APA's predecessor only before APA's first filing (decision 63) | not reached |

## Impact

Measured by adding each one to `REVERSE_MERGERS` in a scratch run of the full history
(`rm/impact.py`):

| filer | close | points | move | what moves |
|---|---|---|---|---|
| TMUS | 2013-04-30 | 65 | 6 | 2013-08-09 and 2013-11-08 newly refused: they were valued (-21.56, -27.82) on MetroPCS's FY2012 cash flows with T-Mobile's price and shares. 2014-02 to 2014-10: four values move as growth's start year stops being MetroPCS's (2014-10-29 6.22 -> 2.97). |
| ECHO | 2023-12-31 | 65 | 2 | 2024-03-01 -147.02 -> -135.43; 2024-05-09 -154.97 -> -142.79 (EchoStar-alone start years) |
| VRT | 2020-02-07 | 33 | 3 | already refused (capex); only the reason changes |
| MRK | 2009-11-03 | 69 | 0 | nothing reads across the close |

Today's values do not change.

## Options

- **A. List TMUS, ECHO, VRT and MRK in `REVERSE_MERGERS` (recommended, 20-30 min).**
  - Notes: ECHO's is a common-control combination, not an acquisition, but the rule
    is the same: the registrant's own past stops being comparable at the close.
  - MRK and VRT move nothing beyond a reason, but the list then says what they are.
- **B. TMUS only.** Its two newly refused points are the real error.
- **C. Turn signal A + B into a 06:00 line-check flag** (`RECAST?`), so a new reverse
  merger is caught when its first 10-K lands (1-1.5 h). It would need a reviewed list
  for the filers above that are not mergers.

## Built

Option A. TMUS, MRK, VRT and ECHO are listed in `settings.REVERSE_MERGERS`, each with
what its 10-K re-reports. A re-run of the four histories moves exactly the measured
points: TMUS 6, ECHO 2, VRT 3 (reason only), MRK 0. The line check flags 0 for them,
KDP and JCI. Test: `tests/unit/test_reverse_mergers.py::test_the_census_finds_are_listed`.

Queries: session scratchpad `rm/assets_recast.sql`, `rm/new_dates.sql`, `rm/impact.py`.
