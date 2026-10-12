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
  shapes at it (ON tags a $1.6M lease figure beside $4.5B of debt). Decisions
  in `research/dcf-plan-2026-10-08.md`: weekly 5y Blume beta, 7% WACC floor
  (labelled; `wacc_unfloored` in the response), market cost of debt, growth =
  lower of 3y/5y revenue CAGR capped 10%; financials, REITs, managed care and
  captive finance refused. Predecessor CIKs (`CIK_PREDECESSORS`) cover holdco
  reorganisations (XOM, WBA). `core/valuation_service.py` assembles a value and
  the point-in-time history (the day after each 10-K/10-Q).
