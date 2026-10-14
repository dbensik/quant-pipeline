# modeling/ — the DCF (tier 3)

- `core/sec.py` + `core/fundamentals.py` + `modeling/` + `api/routers/valuation.py` —
  tier 3, the DCF. SEC XBRL company facts are stored RAW and insert-only in
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
  the 10-Qs mixes with the 10-K's. A quarter and its year-to-date are both
  kept, and facts are read in a TOTAL order: DAL was valued 1.28 or 2.28
  depending on which valuations ran first in the process (2026-10-12).
  Debt concept scope is read from the filers' series, not the labels:
  ConvertibleLongTermNotesPayable is NONCURRENT in practice. Capex for AEP,
  LNT, EOG and revenue for CHTR, ADM, BG, KMI, SRE, DVN, WMB come from
  `LINE_CONCEPTS_BY_SYMBOL` (verified against each 10-K; summed per period,
  a missing part drops the period). Add a filer only after reading its
  statement: no rule picks the right line (NTAP's `Revenues` is a part, ADM's
  contract revenue is). CFO is continuing operations first, matching capex.
  Capex includes `intangible_capex`: capitalised software by rule (its own
  line on every filer that tags it), PaymentsToAcquireIntangibleAssets only
  by name (CMCSA, ADP, SLB; it is also spectrum, acquisitions, allowances).
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
