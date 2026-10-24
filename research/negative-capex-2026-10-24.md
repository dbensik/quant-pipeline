# Capex filed with a minus sign (decision 67)

2026-10-24. Measured on "go on 67"; A built the same day on "go on 67 A".

## Built (A)

- `modeling/statements.GROSS_CAPEX_CONCEPTS`: the capex list less
  `PaymentsForProceedsFromProductiveAssets`. `Statements` sets aside a
  duration fact of one of them filed below zero (`filed_negative`).
- `build_inputs` names each such filing once while a fact of it is within
  the annual age and no kept fact of the concept reports its period
  (`filed_negative_unreplaced`, sharing decision 66's helper): "... set
  aside: filed negative ($-493M), and a payment is an outflow".
- Live, unpatched, all 42 filers' 2,369 history points re-run: the 43 that
  moved equal the measured "set aside" column exactly, and no other moved.
  CSX 2010-02-20 now refuses for capex; LUV 2011-02-09 reads FY2009 at
  $34.23 and names three filings. CSX today: unchanged ($31.82).
- A refusal says capex is "not reported", not that a filing was set aside:
  a `Refusal` carries no notes.
- Tests `tests/unit/test_filed_negative.py` (5, all fail on the committed
  code); 5 mutants on a copy all killed; suite 2,030 pass.

## The census

Every history point of every valued filer (443 filers, 24,848 points with a
capex read), the capex the DCF reads (`_ttm_or_annual`), flagged when:

- the read is zero or negative (42 points),
- a TTM is under a quarter of the latest fiscal year's capex (7),
- a part of the TTM is negative (96).

EQT is clean since 66 A. With the 66 entries removed, the census flags 6 of
EQT's 8 bad points, so it sees that shape.

## What is wrong

Ten filers' 2010-2011 10-Ks (or a 10-K/A) tag a gross capex concept with a
**minus sign**: the year and its comparatives. Every later filing reports the
same periods positive, at the same size:

| filer | filing | concept | FY value filed | later filings |
|---|---|---|---|---|
| CSX | 10-K 2010-02-19 | PaymentsToAcquirePropertyPlantAndEquipment | FY2009 -1,447 | 1,427; 1,426 |
| GWW | 10-K 2010-02-25 | same | FY2009 -142 | 142 |
| HON | 10-K 2010-02-12 | same | FY2009 -609 | 609; 605 |
| OMC | 10-K 2010-02-19 | same | FY2009 -131 | 131 (10-K/A); 131 |
| LUV | 10-K 2011-02-08 | same | FY2010 -493 | 493; 493 |
| WBA | 10-K 2010-10-26 | same | FY2010 -1,014 | (predecessor CIK) |
| EMR | 10-K 2009-11-23 | PaymentsToAcquireProductiveAssets | FY2009 -531 | none |
| PCG | 10-K 2010-02-19 | same | FY2009 -3,958 | none |
| WMB | 10-K 2010-02-26 (and an 8-K) | same | FY2009 -2,387 | 2,387 |
| CNP | 10-K/A 2011-03-30 | same | FY2010 -1,509 | 1,509; 1,509 |

($M.) The DCF reads that year, and TTMs built on it, until the next 10-K.
FCF = CFO - capex, so a negative capex is ADDED to FCF.

## What it does

All 42 filers with a negative fact of a gross capex concept, every history
point valued three ways in memory (scratch `d67/impact.py`): as now, with
those facts' sign flipped, and with them set aside.

- **27 valued points read negative capex** (8 filers). For example:

  | as of | price | per share now | flipped | set aside |
  |---|---|---|---|---|
  | CSX 2010-02-20 | 46.77 | 207.32 | 42.23 | refused (capex) |
  | WMB 2010-02-27 | 21.54 | 186.85 | 12.12 | refused |
  | GWW 2010-04-30 | 110.54 | 289.86 | 200.85 | refused |
  | HON 2010-04-24 | 47.72 | 145.70 | 111.81 | refused |
  | OMC 2010-02-20 | 37.07 | 132.43 | 113.27 | refused |
  | CNP 2011-05-06 | 18.72 | 139.74 | -2.02 | refused |
  | LUV 2011-02-09 | 12.27 | 70.72 | 37.34 | 34.23 (FY2009, in age) |
  | EMR 2010-05-06 | 48.44 | 124.02 | 91.90 | refused |

  Counts: CSX 4, GWW 5, LUV 4, OMC 4, CNP 3, WMB 3, EMR 2, HON 2.
- 10 more points move but were refused anyway (PCG's 4 for capex, WBA's 3
  and EMR's and HON's first for price or beta).
- 6 points move by under 4% (A, D, DAL, LUV 2010-08, VEEV twice), where a
  negative comparative or quarter enters a derived quarter.
- Every other point of the 42 filers, and every filer's value today, is
  unchanged.

## Options

**A. Set aside a negative reported fact of a gross capex concept
(recommended).** The concepts are `PaymentsTo...` / `PaymentsFor...`
outflows; `PaymentsForProceedsFromProductiveAssets` is a net and stays
read. A derived quarter (Q4 = FY - 3Q) may still be negative; that is
arithmetic, not a filing. The line falls to the next concept, or the
point refuses for capex, and the valuation names the filing, as in
decisions 61 and 66. 27 points refuse; LUV 2011-02-09 reads FY2009 (585).
This follows decision 62: a value in the wrong form is set aside, never
repaired, because at the as-of date nothing on file says what it should be.
Cost: 1-1.5 h (rule in `Statements`, note, a test that fails on today's
code, mutants on a copy, history re-run that only these points move).

**B. Flip the sign, with a note.** It keeps the 27 points valued, and every
later filing confirms the magnitude for 8 of the 10. But it repairs a value
with no witness on file at the time. And a negative is not always a sign
error: CVX tags `PaymentsToAcquireOtherProductiveAssets` negative in three
10-Ks running (not a concept its DCF reads). Same cost as A.

**C. Do nothing.** 27 points of 2010-2011 history stay overvalued by
$19-186 a share against the flipped figure (WMB 2010-10-29: $190.94
against $4.98).

## Seen, not checked

- **FIS (59 points, 2010-2025).** Its derived Q4 is negative every year
  (FY2023: -$309M). The 10-K and the 10-Qs read different concepts, so the
  TTM mixes them. Every TTM stays positive. (This said each was "off its
  fiscal year by up to about 40%"; that compared a TTM with a fiscal year,
  not with a correct TTM. Measured in decision 68,
  `research/ttm-quarter-mix-2026-10-24.md`: 11 points move, all by 2.2% or
  less but one, +8.1%. Fixed there for every filer.) This is beside its
  `PaymentsToDevelopSoftware` ($835M), which is correct.
- **COP (7 points, 2022-2024).** `PaymentsForProceedsFromProductiveAssets`, a
  net concept, enters a TTM beside the 10-K's gross capex (Q1 2022 -$2,332M:
  asset sales). TTM $2,975M against FY2021 $5,324M.
- **INTU, LII, VEEV.** A small negative Q4 (INTU 2011 -$52M against $114M).
- **Low TTMs.** UAL 2021 and RCL 2017 look like real cuts. CSGP Q1 2022 reads
  $0 against $123M a year earlier, and GEN 2020 looks like its Enterprise
  sale. Neither is checked.
- **EQIX FY2012-13** (from decision 66): closed. EQIX is refused as Real
  Estate, so no valuation reads those years. Only the Research page shows them.
