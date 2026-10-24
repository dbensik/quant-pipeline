# A predecessor's later filings and counts under 1M shares are set aside (decision 63)

2026-10-23. Built the same evening on "go on 63".

## What was wrong

After decision 62, 20 history points were still more than 50 times their
price. 13 of them had two causes.

**A predecessor kept filing as a subsidiary.** `CIK_PREDECESSORS` reads an
old filer's facts with the new one's, for a holding-company reorganisation.
The rule was "latest filed wins per period, across all of them". Apache (CIK
6769) kept filing its own 10-Qs and 10-Ks after APA Corp took over in 2021,
because it still has public debt. Those filings are a subsidiary's:

- The cover says 1,000 shares, all owned by APA.
- Apache's cash, CFO and capex are its own. In the 2021-05-07 10-Q, 24 of
  94 shared lines differ, for example CFO $691M against APA's $671M and
  cash $281M against $538M.

Both CIKs file on the same day, so the reader took Apache's 1,000 shares
on APA's 11 points from 2022 to 2024, at 0.5 to 2.2 million times the price.

The table counts predecessor rows filed on or after the successor's first
10-Q/10-K:

| symbol | filings | rows |
|---|---|---|
| APA | 15 | 1,747 |
| APO | 6 | 631 (old Apollo, also 1,000 shares) |
| WBA | 1 | 111 (the transition day) |
| XOM, BLK, MRVL, TPL, BG | 0 | 0 |

WBA is the other way round. Its first 10-Q (2014-12-30) belongs to the
holdco before the Boots merger: 100 shares, revenue 0 and CFO 0. Walgreen
Co's 10-Q filed the same day is the business.

**A cover count of 1 share.** Fox's 10-K covers count each share class
separately, and companyfacts leaves out counts split by class. The only cover
count on file was the 1 share from Fox's 2019 registration, 145 days old on
2019-08-10. That is within the age limit, so it was read ahead of the 621M
diluted average, at 1.4 billion times the price (FOX and FOXA).

## What was built

- `core/valuation_service.successor_facts` (used by `filer_statements`, so
  by the DCF, the Research page and the desk pack) reads a predecessor's
  facts only from filings before the successor's first periodic one.
  `settings.CIK_PREDECESSOR_READ_THROUGH` names a later last day where the
  successor's first filing is a shell's. Today that is WBA, read through
  2014-12-30.
- `modeling/statements.py`: a cover count or diluted average above 0 and
  under `MIN_WITNESS_SHARES` (1M) is set aside (`too_small`), never
  rescaled. The smallest registry filer is NVR, at about 3M shares. A count
  that small is one of three things:
  - a shell's (Fox's 1, Linde plc's 25,000);
  - a subsidiary's (Apache's 1,000);
  - a table in thousands (ECHO's 271,519, ATO's 2011 averages).
- `build_inputs` notes a set-aside count that would have been read: "... is
  1, under 1,000,000 shares (a shell's, a subsidiary's or a table in
  thousands): set aside".

`tests/unit/test_predecessor_filings.py` has 5 tests. Three mutants were each
run on a copy, and each was killed:
- no cutoff: the predecessor test fails;
- no floor: the shell and thousands tests fail;
- no read-through: the WBA test fails.

Two existing tests used counts that the floor now catches, and both were
updated:
- Decision 62's diluted test now uses an average a thousand times too large
  (318.5 billion), not one in thousands.
- The stale-cover test's 1-share cover is now a 2.9-billion one dated 2019.

Suite: 2,014 tests, 0 failures, 1 skipped. Ruff is clean.

## Effect

History over every equity: 443 symbols with points, 26,275 points, before
(06d7ae1) and after.

- 20 points on 4 symbols moved. There were 0 crashes, and no point was
  added or lost.

  | symbol | points | before | after |
  |---|---|---|---|
  | APA | 11 (2022-2024) | 15.7M to 68.7M | 16.12 to 136.49 |
  | APA | 2023-02-24 | 125.23 | 118.83 (APA's own lines, not Apache's) |
  | FOX, FOXA | 2019-08-10 | 50,123,939,498 | 80.71 |
  | ECHO | 2024-05-09, 2025-05-10 | -154,583, -22,536 | -154.97, -23.56 |
  | ECHO | 4 others | -121k to -35k | refused: no share count within 200 days |

- ECHO is now caught (decision 62 missed it with one witness). Where its
  last average in units is older than 200 days, the point is refused. It is
  not valued on the wrong count.
- Points more than 50 times their price: 20 before, 1 after (CVNA
  2023-02-24, -$484, a negative DCF on 2022's losses. It was not checked
  against the 10-K).

Valuations as of 2026-10-23: 389 valued before and after, and no value
moved.

## Seen, not done

- APA's own FY2021 10-K has no pretax income under the listed concept, so
  APA's 2022 points now note "effective tax rate unavailable; 21% statutory
  used". They had used Apache's 29%.
- The floor reaches past what was proposed. Decision 63 B named cover
  counts only. The build also sets aside diluted averages under 1M, which is
  what caught ECHO; 4 of ECHO's points are now refused rather than valued.
  Counts too small by a unit (CRM's 2011 cover of 134, LH's 97) are now set
  aside by the floor before decision 62's witness rule sees them. The values
  are the same; only the note's wording changed.
- WBA's read-through reads the shell's 10-Q and Walgreen Co's on the same
  day. On a tie the higher accession wins, and that is Walgreen Co's
  (...046621 against ...046619), so the business's lines are read. A line
  only the shell tags (its $10B bridge debt) is still read for 2014-11-30. No
  WBA point is valued before 2020 (no prices), so this reaches only the
  Research page's statements.
- A filing both CIKs list (XOM's 2026-08-03 10-Q) is read from its
  instance under the CIK its contexts name (`core/xbrl_instance._contexts`),
  so it lands under the successor and is not cut.
- History points are still dated from every CIK's filing days. A predecessor
  filing after the cutoff falls on the successor's own filing day in every
  case here, so it adds no point.
- A new predecessor added to `CIK_PREDECESSORS` gets the cutoff
  automatically. A shell-first successor like WBA needs a read-through
  entry, found by reading its first filing.
