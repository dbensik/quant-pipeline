# SEC companyfacts gap: filings it leaves out, and filers never loaded (decision 59)

2026-10-23. Built the same day on "go on 59" (A + B).

## What was wrong

At 2026-10-23, 74 filers' newest stored 10-Q/10-K was more than 140 days old
(PPL 2026-05-08 the newest of them) while 434 had one within 100 days. Two
separate causes.

### A. companyfacts omits whole filings

Each of the 66 registry filers was compared against SEC itself (submissions
list + companyfacts, 132 requests):

- 61 had a 10-Q (or CLX's 10-K) on their submissions list that companyfacts
  did not contain AT ALL: 63 filings, almost all filed 2026-07-22..08-07
  (ABT, KO, V, DUK, SO, PYPL, SPGI, C, NEE...). companyfacts itself was not
  stale: for many of them it carried facts filed later (C to 2026-10-23,
  CLX 10-07), so SEC drops individual filings, not whole companies.
- Every missing filing is XBRL (`isXBRL` 1, inline 1) and its folder holds
  the extracted instance `*_htm.xml`.
- CTRA, DAY, WBA had nothing later on file: merged or taken private.
- FDX: SEC has no Q1 FY2027 10-Q from FedEx (its newest periodic filing is
  the FY2026 10-K of 2026-07-20). Its refusal (spin window, decision 58) is
  right, not this gap.

A sweep of all 535 registry CIKs' submissions since 2024-01-01 found 80
periodic filings on 73 filers with no stored row (after B, below: 84,
including a few filed within the day).

### B. 27 registered equities had no facts at all

The 2026-10-19 index joiners and recent spin-offs (ARES, CASY, CIEN, COHR,
CRH, CVNA, ECHO, EME, FDXF, FERG, FLEX, HONA, HOOD, IBKR, ILMN, IP, LITE, P,
Q, RDDT, SKYD, SNDK, STX, TWLO, VEEV, VRT, XYZ). `register_new_members.py`
(decision 44) fetches bars only, and the daily SEC step reloads a filer
only when it files, so their facts never arrived.

## Fidelity of the instance

`core/xbrl_instance.py` reads an instance into a companyfacts-shaped
document, which then goes through `parse_company_facts` unchanged. Compared
with the stored companyfacts rows of filings companyfacts DID serve
(taxonomy, concept, unit, start, end, value; key and value):

| sample | filings | result |
|---|---|---|
| hand-picked (AAPL, HON, MSFT, TSLA, ABT, KO, AEP, JPM) | 8 | 7 exact; MSFT 3 rows differ |
| random since 2025 | 40 | 37 exact; 3 filings differ on 3 rows each |
| held out, random since 2024, after both rules | 30 | 30 exact, 3,861 facts |

The two rules the differences taught:

- **A zero-length duration is an instant.** MSFT tags GoodwillImpairmentLoss
  on a 2026-05-01..2026-05-01 context; companyfacts serves it with no start.
- **A fact tagged twice keeps the higher `decimals`.** AMZN's FY2024
  Depreciation is 30.2B in the text and 30,225M in the table; the 3 random
  differences (AMZN, a tax rate, weighted shares) were all this.

`fy`/`fp` come from the filing's DocumentFiscalYearFocus/PeriodFocus and
match companyfacts on the AAPL fixture; `frame` is absent. Nothing reads any
of the three.

## What was built

- `core/xbrl_instance.py`: the reader (lxml, no entities, no network).
  It keeps only contexts whose entity identifier is the filer's own CIK:
  none of the 68 filings loaded had another (checked after the load, 136
  requests, including the combined utility filings DUK, SO, D, ETR, EXC...),
  and the shipped parser re-matched companyfacts exactly on 38 of the
  sample filings.
- `core/sec.py`: `get_bytes`, `filing_instance` (the ONE `*_htm.xml` of a
  filing, else LookupError: no other file is guessed), and the daily index
  rows now carry the accession (`recent_fact_filings`).
- `fundamental_facts.source` (migration 0014): `companyfacts` (every row
  before it) or `instance`.
- `core/fundamentals.py`: `ingest_instance`; `ingest_company` returns
  DISAGREEMENTS: a companyfacts row whose key holds an instance row with a
  different value. The key has no value in it, so the instance row is kept
  and the difference is printed, exact comparison.
- `scripts/load_fundamentals.py`:
  - `--recent` (06:00) also reads `SEC_INSTANCE_LOOKBACK_DAYS` (30) of the
    daily index; a registry filing at least `SEC_INSTANCE_GRACE_DAYS` (3)
    old with no stored row under ANY CIK is read from its instance.
    companyfacts usually has a filing the same day (OMC and DOW's of
    2026-10-23 were in it hours later), so a younger one is left to it.
  - `--instances-since DATE`: the sweep, from each filer's submissions list
    (paged files included).
  - Exit 0 ok, 1 failed, 2 DISAGREE (the job FLAGS it, `sec-instance-disagree`).
- `register_new_members.py` loads each joiner's companyfacts after its bars
  (a filer SEC has none for yet is named, not failed).

Any CIK, not the listing one: XOM's 2026-08-03 10-Q (accession 0000034088-)
is listed by both its predecessor and its successor CIK. The first sweep
checked (cik, accn) and stored it a second time under 34088, 115 rows; they
were deleted the same hour, and the check is now by accession alone.

## Loads (2026-10-23)

- B: `load_fundamentals.py --symbols` on the 27, 140,813 rows. CASY and TWLO
  print MISMATCH? on punctuation only ("CASEY’S" / "TWILIO INC.").
- A: `--instances-since 2024-01-01`: 81 filings read, 69 with facts (9,461
  rows, less XOM's 115 = 9,346 on 68 filings), 12 with none (IBKR, SHOP, ECHO,
  SKYD, LEN 10-K/As, HSY 10-Q/As: cover pages; AZN's 20-Fs: IFRS). A rerun
  found only those 12.
- `--recent` end to end: 49 filers reloaded, 0 instances due, exit 0.

## Effect on the DCF (census as of 2026-10-23, 538 equities)

The running API (8001) reads the new rows without a restart: ABT 200
(per share 80.78 at 102.51, TTM to 2026-06-30), CASY 200, KO 200.


| | valued |
|---|---|
| before | 358 |
| after B | 376 |
| after A | 389 |

- B newly valued 18: CASY, CIEN, COHR, CRH, CVNA, ECHO, FDXF, FERG, FLEX,
  IP, LITE, Q, RDDT, SNDK, STX, TWLO, VRT, XYZ. The rest are refused for
  reasons the model already has (no debt balance, capex not in a standard
  concept, HONA and SKYD no CFO yet). FDXF (-0.52) values on CFO of $167M
  against $379M capex for FY to 2026-05-31: to read against its 10-K before
  relying on it.
- A newly valued 14 (ABT, CDNS, CLX, DOW, ETR, EXC, FE, GD, JCI, LNT, MAS,
  PNR, PPG, SO: most had been refused for no share count within 200 days),
  moved 29, refused FTV (its Q2 10-Q puts the Ralliant cash in discontinued
  operations: the decision 58 basis guard). KO 0.28 -> 0.79: its Q1 TTM had
  aged past 200 days, so it had fallen back to FY2025 CFO ($7.4B, after the
  2025 payment that made Q1 2025 -$5.2B); on the Q2 10-Q the TTM is $16.3B.
- History since 2012 on the 66 affected equities, with and without instance
  rows: 2,761 points, 46 values changed, 6 refused -> valued, 1 valued ->
  refused, none added or lost (a point is the day after a filing date, and
  the dates come from every stored row).
- `check_line_concepts.py`: 538 checked, 0 flagged.

## Not done

- Filings before 2024: the sweep did not look. A filing before inline XBRL
  (2019-2021) has no `*_htm.xml` and `filing_instance` refuses it.
- The sweep re-reads the 12 empty filings each time it runs, and the daily
  step re-reads an empty one daily while it is inside its 30 days.
