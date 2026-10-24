# A fact dated after its own filing is set aside (decision 61)

2026-10-23. Built the same day on "go on 61".

## What was wrong

A period cannot end after the filing that reports it, yet 211 stored rows
on 84 filers do. Most are concepts no DCF line reads: dividends declared
ahead (106 rows, the one real case), tax rates (14), buyback share counts
(7). The rest were read as the NEWEST value of their line:

- ROP's Q1 and Q2 2015 10-Qs tag their 2014-12-31 cash ($610M) as
  "2015-12-31". Every ROP point through the Q3 10-Q took it as the latest
  balance, ahead of the real 2015-03-31 / 06-30 / 09-30 ones.
- The same on GD (2012), MKC (2013), WMT (2012): cash dated the coming year
  end.
- ECHO's 2023 10-K and 2024 10-Qs tag notes as `LongTermDebt` at their
  maturity dates (2024-03-31 $951M, 2024-11-30 $1,980M). The DCF took
  $1.98B as debt at "2024-11-30" against the $21.8B the 10-Qs report.
- PTC's 10-Q filed 2023-05-03 tags interest expense for a quarter ending
  2023-06-30.
- Cover share counts: 24 rows. 22 are dated 1-19 days after the filing
  (NEM's 10-K filed 2011-02-24 says 2011-02-28): the filer's "latest
  practicable date", a real count. ADBE (filed 2010-01-22, dated
  2010-06-15) and CL (filed 2010-02-25, dated 2010-12-31) are typos.

## What was built

`modeling/statements.py`: `Statements` sets aside any fact whose period ends
after its filing date (`dated_after_filing`). It is never moved to another
date and never becomes a zero: the line falls back to the previous real
value, or is unreported. The cover-page count may be dated up to
`COVER_DATE_GRACE_DAYS` (31) past its filing. `Statements.set_aside(as_of)`
lists the set-aside facts a reader would have taken as its newest, and
`build_inputs` notes each within `MAX_ANNUAL_AGE_DAYS` ("... is dated
2015-12-31, after its own filing: set aside").

The first version had no grace for cover counts. Measured: it moved NEM,
NRG and XEL by cents and sent ON 2013-02-27 to an older cover count filed
as 448 trillion shares (per share 1.19 -> 0.00; that row is option 60 B's).
The grace put all four back.

`tests/unit/test_dated_after_filing.py`, 4 tests: 3 fail against the
previous code (run on a copy); the cover-grace test fails on a copy with
the grace set to 0.

## Effect

History over every equity (546 symbols, 26,275 points), before and after:
21 points on 9 symbols moved, nothing refused or added, 0 crashes.

| symbol | points | what changed |
|---|---|---|
| ROP | 3 (2015) | real cash; 204.98 -> 205.87 at 2015-11-04 |
| WMT | 4 (2012) | real cash; within 0.6% |
| GD | 3 (2012) | real cash; within 0.3% |
| MKC | 1 (2013) | real cash; 58.72 -> 58.75 |
| CL | 4 (2010) | the older real cover count; within 2.3% |
| ADBE | 1 (2010) | the newer real cover count; 30.79 -> 30.66 |
| PTC | 1 (2023) | interest expense; 46.95 -> 43.84 |
| ECHO | 4 (2024) | debt $1.98B -> $21.8B; still nonsense (see below) |

Valuations as of 2026-10-23: 389 valued before and after, no value moved.

## Seen, not done

- ECHO values at -$155k a share because its diluted average is stored as
  271,519 (thousands): the scale problem of option 60 B, on the diluted
  average as well as the cover count.
- A date that is wrong but still before its filing passes this rule.
