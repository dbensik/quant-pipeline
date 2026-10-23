# A cover share count of 0 is not a count (decision 60)

2026-10-23. Built the same day on "go on 60" (A).

## What was wrong

`GET /valuation/ROP/history` answered 500. ROP's Q3 2015 10-Q
(0000882835-15-000025, filed 2015-11-03) tags
`EntityCommonStockSharesOutstanding` = 0 beside a diluted average of
101,607,000. The cover count was used as given, market value came to 0, and
`modeling.dcf.wacc` raised "equity market value must be positive". The
ValueError is not a refusal, so one bad point took down the whole series.

31 cover rows on 24 filers are 0 (2009-2022). History over every equity
(546 symbols, 26,275 points) crashed on 15 points of 11: BALL, DOV, EOG,
ETR (2), OXY, RCL, ROP, RSG, T, TAP (4), VRSK; the other zeros did not
reach a valued point (why, per filer, was not read). No current valuation
was affected (the newest zero is BALL's FY2021 10-K).

## What was built

`modeling/valuation.py`: a cover count of 0 or less is treated like a stale
one: the newest positive diluted weighted average within
`MAX_TTM_AGE_DAYS` is used, with the note "cover-page share count for
<date> is filed as 0: diluted average used"; with none, the point is
refused ("no share count on file"). Diluted averages of 0 (18 rows on 4
filers) are skipped too. `tests/unit/test_zero_share_count.py`: 2 of its 3
tests fail with the ValueError against the previous code (run on a copy).

## Effect

The same 26,275 points before and after: exactly the 15 crash points
changed (to valued), nothing else moved, 0 crashes after. ROP 2015-11-04
values at 204.98 against 187.80.

## Seen, not done

- **Cover counts in the wrong scale** (option B, not built): CRM 133.9
  (2011), LH 96.8 (2012), CMG 27,962 (2022), RMD 145,681 (2021) value at
  50x price and more. A band against the diluted average would catch them;
  it must allow a reverse split (HON 0.5).
- **20 more points over 50x price** on APA (11, 2022-24), ECHO (6), FOX (2,
  the known 1.0-share cover), UHS, CVNA: share counts look right, cause not
  read.
- **Balances dated after their filing**: 211 rows on 84 filers have a
  period ending after the filing date. Most are dividends declared ahead
  (106) but some are balances: ROP's Q1 and Q2 2015 10-Qs tag cash at
  "2015-12-31" (610M, really 2014-12-31), which the 2015-11-04 point reads
  as the newest cash balance.
- **ETR 2010-11-06** values at 497.82 against 75.40 on capex 0 (FY2009).
