# Zero-volume prefixes in stored series (decision 45, 2026-10-19)

Decision 44's placeholder rule, run over all 558 stored equity and ETF
series, flagged 7 that open on zero-volume bars. Danny approved cleaning
them up. Each was read before anything was deleted.

## Trimmed: IPO offer-price placeholders (3)

Yahoo serves one zero-volume bar the day before the IPO, at the OFFER
price. The first real session then reads as a jump that never traded:

| symbol | placeholder bar | offer price | first session close | phantom return |
|---|---|---|---|---|
| GDDY | 2015-03-31 | $20 | $26.15 | +31% |
| SHOP | 2015-05-20 | $17 (1.70 split-adjusted) | 2.568 | +51% |
| TEAM | 2015-12-09 | $21 | $27.78 | +32% |

P (Pure Storage), trimmed earlier the same day, was the same case: a $17
offer-price bar on 2015-10-06.

The three bars are deleted, and the rows are kept in
`research/ipo-offer-price-bars-removed-2026-10-19.csv`. Each asset has
`history_valid_from` set to its IPO date, so ingestion refuses them again.
None of the three had a corporate action on or before the removed bar.

## Kept (4)

- **CHTR and HWM** were reviewed and kept by decision 26
  (`research/history-2010-backfill-2026-10-14.md`).
  - CHTR's 18 zero-volume days are scattered through 2010, while its
    post-bankruptcy shares traded thinly (13k-75k shares a day in the other
    sessions).
  - HWM's first 35 sessions (Arconic after the Alcoa separation) have prices
    but no volume.
  - Both match Yahoo's returns on every day. Nothing new argues for
    reversing that.
- **VST:** one no-trade day (2016-11-25, the day after Thanksgiving) during
  real over-the-counter trading before its 2017 NYSE listing. It is a
  genuine price series.
- **VRTX:** one halt day in 2011. A false positive of the rule.
