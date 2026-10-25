# modeling/ — the DCF (tier 3)

- `core/sec.py` + `core/fundamentals.py` + `modeling/` + `api/routers/valuation.py` —
  tier 3, the DCF. SEC XBRL company facts (or, for a filing companyfacts
  left out, that filing's own instance: `source`, decision 59) are stored RAW and insert-only in
  `fundamental_facts` (allow-list `FUNDAMENTAL_CONCEPTS`; a new concept needs
  a `load_fundamentals.py --all` reload, insert-only) and standardised at READ
  time by `modeling/statements.py`: a fact is visible only if filed STRICTLY
  before `as_of`, ordered concept lists per line item, Q4 = FY − 3Q, cash-flow
  TTM = FY + YTD − prior YTD (10-Qs file cash flows year-to-date only), share
  counts in the split basis of the day they were FILED. `modeling/valuation.py`
  builds inputs with the source of every line; prices against share counts are
  AS TRADED (`adjust="none"`), beta uses total returns. **Missing is never
  zero**: missing capex refuses; debt older than 460 days or absent while
  interest is reported refuses (LDOS was valued on 2015 debt, CMCSA on none of
  its $90B); flows older than 200 days (annual 460) count as unreported.
  `Statements.debt()` takes the newest balance date, then the LARGEST of three
  shapes at it (ON tags a $1.6M lease figure beside $4.5B of debt). A TTM
  never mixes two concepts that each report the fiscal year (DAL's aircraft
  and ground-equipment capex are parts, not names); a concept used only in
  the 10-Qs mixes with the 10-K's. Both TTM paths apply it; the
  four-quarter one did not until decision 68
  (`research/ttm-quarter-mix-2026-10-24.md`: FIS's Q2 and Q3 10-Qs tag
  property plus software where its 10-K tags property, and its derived Q4
  went negative), and it still cannot judge a quarter of the year in
  progress, which has no fiscal year to compare. A quarter and its year-to-date are both
  kept, and facts are read in a TOTAL order: DAL was valued 1.28 or 2.28
  depending on which valuations ran first in the process (2026-10-12).
  Debt concept scope is read from the filers' series, not the labels:
  ConvertibleLongTermNotesPayable is NONCURRENT in practice. Capex for AEP,
  LNT, EOG and revenue for CHTR, ADM, BG, KMI, SRE, DVN, WMB come from
  `LINE_CONCEPTS_BY_SYMBOL` (verified against each 10-K; summed per period,
  a missing part drops the period). Add a filer only after reading its
  statement: no rule picks the right line (NTAP's `Revenues` is a part, ADM's
  contract revenue is). `LINE_CONCEPTS_FIRST_BY_SYMBOL` is an ORDER, not a
  sum (decision 64, `research/capex-concept-order-2026-10-23.md`): concepts
  read ahead of the list, the list after them, for filers whose PAST years
  the list reads from a smaller line (ETR's capex was $0 for FY2009; ETR,
  URI, ED, XEL, DVN, MCHP, EXE, APA). The 06:00 check reads only the newest
  year; old years were found by a census of every year. Decision 65
  (`research/revenue-cfo-concept-order-2026-10-23.md`) adds revenue orders
  (ETN HAS HSY LHX PG TDY EXC URI: Revenues was a segment or a customer's
  figure in their past years) and GLW's capex, found by the census at each
  HISTORY DATE (today's census flags recasts no point read); with an order,
  growth's start years are read by the order per period, not on the latest
  year's concept. A start year from a concept sharing NO period with the
  latest's passes the rename check vacuously (606 reads, 43 filers); decision
  72 (`research/revenue-start-years-2026-10-24.md`) read the 19 where it
  moves a value and ordered EBAY, GEN, FE, MA and mapped CAG; KDP and JCI
  (reverse mergers) are open.
  CFO is continuing operations first, matching capex.
  Capex includes `intangible_capex`: capitalised software by rule (its own
  line on every filer that tags it), PaymentsToAcquireIntangibleAssets only
  by name (CMCSA, ADP, SLB; it is also spectrum, acquisitions, allowances).
  A map part may be SUBTRACTED ("-Concept"): VST's capex adds its emission
  allowance purchases net of resales (decision 23); purchases reported
  without the resales for the same periods refuse, never drop to nothing.
  Noncontrolling interests (decision 17): the flows are CONSOLIDATED, so
  the partners' trailing share of income comes off base FCF (total concept,
  else redeemable + nonredeemable parts over one window) and the exit
  multiple values only the parent's share of EBITDA (1 - NCI / consolidated
  profit, the same window). A NEGATIVE share (partners sharing a loss, ROK
  -$110M) is shown and not added back. Parts on different windows (MPC's
  redeemable part stops after FY2025) use their common latest fiscal year.
  Refused when NCI income exceeds
  `DCF_NCI_MAX_FCF_SHARE` (50%) of a positive base FCF (FCX 99%: the proxy breaks),
  and when an NCI balance of 1% of market cap or more has no NCI income on
  file (missing is never zero). Preferred stock (decision 18) comes off
  equity value after net debt (`DCFInputs.preferred`, on the Gordon and
  exit values, the grid and the reverse DCF): the largest of its three
  shapes at the newest balance date (`Statements.preferred_stock`; some
  filers carry it at par), annual age rule; none tagged is none. Temporary
  equity attributable to the parent is NOT preferred (J's was NCI).
  Equity-method stakes are never added (their dividends are in CFO; book
  is cost): one of 5% of market cap or more is named in the notes.
  Unfunded pensions and OPEB (decision 19) are never subtracted either
  (contributions are inside CFO; filers that tag per plan, GE and T, have
  no total, so absent is not none): `Statements.pension_deficit`, the
  balance-sheet lines less overfunded plans' assets, at an ANNUAL report's
  date only (TAP's 10-Q tags the liability without the surplus), is named
  at 5%.
  Spin-offs (decision 58, `research/spinoff-basis-2026-10-23.md`): Yahoo
  serves one as a SPLIT whose ratio is the price factor, and share counts
  had been multiplied by it (CTVA 667M x 6.665). Every odd-ratio split row
  is read into `settings.SPLIT_ROWS_REVIEWED` (kind + the ratio counts
  really move by: 1.0, or HON's 1-for-2 reverse split with Aerospace);
  `valuation_service.share_splits` applies it, and a new unlisted row is a
  `SPLIT?` flag in the 06:00 line check. A valuation refuses from a spin's
  ex-date until a 10-Q/10-K for a period ending `SPINOFF_FILING_MIN_DAYS`
  after it (price after, statements before), and while the cash-flow TTM
  bridges two bases: the prior YTD carrying more discontinued CFO than the
  fiscal year by `DCF_DISCONTINUED_BREAK_SHARE` of its total
  (`Statements.discontinued_cash`; HON 9M 2025 $2,594M vs FY $333M);
  then by date alone (58b) while the spin falls after the latest annual
  CFO's year, until a 10-K for a year ending after it. A filed period is
  a DURATION: a 10-K's cover share count is dated after its period (FDX).
  A cover share count filed as 0 is no count (decision 60,
  `research/zero-share-count-2026-10-23.md`): the diluted average is used
  and noted, as for a stale one (24 filers' covers say 0; ROP's made
  `wacc()` raise and the history endpoint 500). A fact whose period ends
  after its own filing is set aside, never re-dated (decision 61,
  `research/dated-after-filing-2026-10-23.md`: ROP's 2014 cash tagged
  "2015-12-31", ECHO's notes tagged at their maturities); a cover count
  may be dated `COVER_DATE_GRACE_DAYS` past its filing (a real count).
  A share count in the wrong unit is set aside, never rescaled (decision
  62, `research/share-count-scale-2026-10-23.md`: ON's 455 trillion, CRM's
  134): `SCALE_OFF` from every witness while they agree, at least two (its
  filing's cover, weighted averages, net income / diluted EPS; the last
  kept count of its kind); a witness under `MIN_WITNESS_SHARES` (a shell,
  a table in thousands) condemns nothing. Such a count is itself set
  aside (decision 63, `research/predecessor-filings-2026-10-23.md`:
  Fox's 1-share registration cover, Apache's 1,000, ECHO's averages in
  thousands; CRM's 134 and LH's 97 now fall here first): no registry filer has under 1M shares (NVR ~3M). A
  predecessor CIK is read only from filings before its successor's first
  10-Q/10-K (`successor_facts`; Apache kept filing as APA's subsidiary),
  or through `CIK_PREDECESSOR_READ_THROUGH` (WBA: its first 10-Q was the
  pre-merger shell's).
  A concept a filing used for ANOTHER line is set aside by (concept,
  accession) in `FACTS_SET_ASIDE_BY_SYMBOL` (decision 66,
  `research/eqt-capex-meaning-2026-10-24.md`: EQT's 2015 and 2016 10-Ks
  tag acquisitions as PaymentsToAcquirePropertyPlantAndEquipment, every
  other EQT filing tags capex), for a meaning that flips and flips back,
  where no order serves every filing. The line reads the next concept and
  the valuation names the filing until a later one reports the period.
  COP's nine 2022-2024 10-Qs are there too (decision 70,
  `research/cop-asset-proceeds-2026-10-24.md`): asset-sale proceeds on
  PaymentsForProceedsFromProductiveAssets had entered its YTD TTM. Its
  capex line since 2022 is its own concept, stored from each filing's
  instance (`settings.EXTENSION_CONCEPTS_BY_CIK`, taxonomy `cop`; 70 B) and
  read first through `LINE_CONCEPTS_FIRST_BY_SYMBOL`: companyfacts serves
  only us-gaap and dei. CSGP's capex is two lines from its FY2021 10-K
  (campus purchases on us-gaap PPE, ordinary ones on its own concept):
  `LINE_CONCEPTS_SUM_WHERE_FILED_BY_SYMBOL` (`SumWhereFiled`, decision 71,
  `research/csgp-capex-2026-10-24.md`) reads their SUM for every period
  both report and the list for the others, where a plain sum would drop
  every earlier year.
  A capex PAYMENT filed below zero is set aside the same way, by rule
  (decision 67, `research/negative-capex-2026-10-24.md`,
  `GROSS_CAPEX_CONCEPTS`: every capex concept but the net
  PaymentsForProceedsFromProductiveAssets): ten filers' 2009-2011 10-Ks
  file capex negative (CSX FY2009 -1,447, then 1,426), which FCF = CFO -
  capex ADDED (CSX 2010-02-20 $207 against a $47 price). Never flipped; a
  derived quarter may still be negative. A refusal of a missing line (CFO,
  capex, debt, shares) appends the set-aside notes of that line's own
  concepts, since a refusal has no notes (decision 73,
  `research/refusal-names-set-aside-2026-10-24.md`).
  Growth's start years come from `Statements.annual_before`: by date, and
  on the latest year's concept unless another agrees with it everywhere. Decisions
  in `research/dcf-plan-2026-10-08.md`: weekly 5y Blume beta, 7% WACC floor
  (labelled; `wacc_unfloored` in the response), market cost of debt, growth =
  lower of 3y/5y revenue CAGR capped 10%; financials, REITs, managed care and
  captive finance refused, except 22 fee businesses admitted BY NAME
  (`DCF_ALLOWED_SYMBOLS`, decision 16: exchanges/data, insurance brokers,
  payment processors, real-estate services; never by sub-industry). Predecessor CIKs (`CIK_PREDECESSORS`) cover holdco
  reorganisations (XOM, WBA). `core/valuation_service.py` assembles a value and
  the point-in-time history (the day after each 10-K/10-Q). The Research page's
  statements read the same layer (`filer_statements`, no refusals;
  `core/research_financials.py`); `Statements.discrete_quarters` is for that
  display only, and `quarterly()` (which feeds the TTM) is left alone.
