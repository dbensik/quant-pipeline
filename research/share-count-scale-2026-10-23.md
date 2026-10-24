# A share count in the wrong unit is set aside (decision 62)

2026-10-23. Built the same evening on "go on 62" (pick 60 B).

## What was wrong

Filers sometimes tag a share count in thousands or millions, or a thousand
or a million times too large. The DCF read the count as filed:

- Cover counts (`EntityCommonStockSharesOutstanding`), the count every
  valuation uses. ON's 2012-13 covers say 455 trillion shares, GRMN's 2016-18
  208 billion, PKG's FY2025 10-K (filed 2026-02-26) 89 billion. CRM's 2011
  cover says 134 and LH's 2012 cover 97 (millions). CMG's 2022 and RMD's 2021
  covers are in thousands.
- Diluted weighted averages, read when the cover is 0 or stale. COP's are in
  thousands for a decade, MCD's in millions, and UHS's and ETR's are read on
  some points.

Each such point was valued at about 0.00 or about 1,000 to 1,000,000 times
its price: CRM 2011-05-27 at $52M a share, CMG 2022-04-29 at $670,825.

## What was built

`modeling/statements.py` (`Statements.__init__`, `off_scale`). A cover count
or a diluted average is checked against witnesses in its own split basis:

- From the same filing:
  - the cover page;
  - the weighted averages, basic and diluted together as one witness, since
    they share a table and its unit;
  - net income divided by diluted EPS, taken only where EPS is at least
    $0.05.
- The newest count of the same kind kept from an earlier filing, within
  400 days.

A count is set aside when it is at least 200 times off every witness (or
1/200 of it), there are at least two witnesses, and they agree with each
other within 5 times. It is never rescaled: the line falls back to the
previous kept count, or the diluted average, as for a stale cover. A witness
under 1M shares condemns nothing, for two reasons:

- The averages and EPS share a table. When both are in thousands they agree
  against a right cover: ATO's 2011 10-K says 90.6M on the cover and 91
  thousand in the averages and from earnings. LUV, COHR and SW are the same.
- A shell's count is real but not the company's. Linde plc filed 25,000
  shares before its 551M, and SW filed 100 before its 519M.

`build_inputs` notes a set-aside count that would have been read ("... is
317,359,000,000,000, in another unit than its filing's other share counts:
set aside"). Two new witness lines are read for this check only:
`basic_shares` and `diluted_eps`.

`tests/unit/test_share_count_scale.py` has 6 tests, each run against a
mutant copy:
- With the rule switched off, the cover test and the diluted test fail.
- With no 1M floor, the ATO test and the shell test fail.
- With the first ordering, the instant-tagged test fails. It is the crash
  the first census found: CZR, GEV and MTCH tag an average with no start
  date.

## Effect

Across all equities, 49 cover rows (31 filers) and 862 diluted rows (81
filers) are set aside.

History over every equity: 546 symbols, 26,275 points, before and after.

- 38 points on 24 symbols moved, each from about 0 or a
  value thousands to millions of times its price to one in the price's
  units (some negative, as DCF values can be). Examples:

  | symbol | point | before | after |
  |---|---|---|---|
  | CRM | 2011-05-27 | 52,460,128.95 | 52.85 |
  | LH | 2012-04-28 | 161,739,596.01 | 161.07 |
  | CMG | 2022-04-29 | 670,825.12 | 669.15 |
  | ON | 2012-08-04 | 0.00 | 9.17 |
  | PKG | 2026-02-27 | 0.14 | 151.32 |
  | UHS | 2012-11-09 | 57,501.49 | 57.43 |

- CSX 2011-04-21 is now valued at 73.95. It had been refused for NCI
  against a market cap of 1 share.
- PNR 2011-04-27 is now refused. Its 115M of NCI is 1% or more of the real
  market cap, and no NCI income is on file (decision 17's rule).
- Nothing crashed and no point was added.

Valuations as of 2026-10-23: 389 valued before and after, and no value
moved.

## Seen, not done

- ECHO is not caught. It files no cover and tags earnings in few 10-Qs, so
  its 10-Qs' averages in thousands (271,519) have one witness, the 10-K
  before, and one witness cannot say which count is wrong. Its 2024-05 to
  2025-11 points stay nonsense. Today's point reads the 2026-08-03 10-Q, in
  units.
- A wrong count with too few witnesses is kept, and then serves as the
  earlier count for the next filing. An amendment that re-files only the
  cover has one witness, so a wrong cover filed twice survives the second
  time. None was found on a point: RMD's 2021-08-19 copy has no history
  point of its own.
- Points still at about 0 or above $10,000 a share: 40 before, 20 after.
  They are APA 11 (the over-50x pick), ECHO 6, FOX 1, FOXA 1 and PEG 1.
- ATO's, LUV's and COHR's filings with averages and EPS in thousands may
  carry net income in the same unit. Only the share counts were checked.
