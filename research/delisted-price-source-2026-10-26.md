# A price source for departed S&P 500 names — 2026-10-26

The gate named in `sp500-membership-reconstruction-2026-10-04.md` and
`point-in-time-universe-2026-10-19.md`: every strategy figure, screen and
membership list here is survivors-only, because Yahoo keeps history only for
symbols it still lists. This note runs the "What to check about Sharadar"
checklist against four vendors and measures coverage where it could be
measured without an account. **Nothing was bought, no account was opened, no
identity was sent anywhere.** It is the evidence for a decision, not a build.

## The list to cover

Every ticker in the reconstructed month-end lists (2014-12 to 2026-07, 789
tickers), less today's observed snapshot (503, 2026-10-26), less renames in
`sp500-ticker-renames-2026-10-04.csv` whose new ticker is a member today:

| | tickers |
|---|---|
| in a reconstructed list, not in today's snapshot | 292 |
| renamed, the new ticker still a member | 34 |
| **departed** | **258** |
| departed, already in our registry (38, mostly 2026 leavers or legacy assets) | 38 |
| **departed, not in the registry: the list to cover** | **220** |

This is not the 10-04 note's 268 / 151. That note counted against the
membership of 2026-10-04 and asked Yahoo (151 had no data at all); this one
counts against today's snapshot and asks our registry. The 220 include 86
names that left the index but still trade (AAL, AAP, AIV...), which Yahoo
prices: they are unregistered, not unpriceable. Seven names left the index
after the reconstruction ends (AVB, BLDR, CTVA, EQR, TAP, TTD, WBD).

**Found:** the rename map only holds renames made while in the index. At
least 11 departed names are renames made after leaving (GPS to GAP, EQR to
VMRK, FI to FISV, FBHS to FBIN, HFC to DINO, KORS to CPRI, CTL to LUMN, ADS
to BFH, TSO to ANDV, ESV to VAL, plus bankruptcies trading on as BBBYQ,
ENDPQ, MNKKQ, WINMQ, SIVBQ, FRCB). Any vendor build needs that second map.

Per-ticker detail: `delisted-price-source-2026-10-26.csv`.

## Coverage measured: Tiingo

Tiingo publishes its full ticker list with each listing's first and last
date as a keyless file (`supported_tickers.zip`, 109,139 rows, fetched
2026-10-26). A ticker re-used by another company appears as separate rows
(ADT, ALTR, APC, VAL), so a match can be checked against the membership
window. Matched where a row spans the whole window:

| result | tickers |
|---|---|
| covers the window, listing ended (acquired, merged, bankrupt) | 116 (6 by successor ticker) |
| covers the window, still trading | 86 (11 by successor ticker) |
| ticker listed, but not over the window | 13 |
| absent | 5 |
| covered, all 220 | 202 (92%) |

**The number that matters is the ended names.** The 86 still trading are
priceable on Yahoo today. Of the 134 that no longer trade under any ticker in
the window (116 covered + 18 not), Tiingo covers **116 (87%)**. That 134 is
this note's counterpart to the 10-04 note's 151 with no Yahoo data; the
difference is mostly the renames-after-leaving above, which Yahoo serves
under the new ticker.

Six of the successor-ticker matches are bankruptcies now on PINK/EXPM
(BBBYQ, ENDPQ, MNKKQ, WINMQ, SIVBQ, FRCB; SBNY too). Their rows reach back
to the original listing, but prints after the filing are OTC data of a
different quality.

The 18 not covered:

- **History from 2016-01-04 only** for names gone in 2016-17: BRCM, CVC,
  HAR, HOT, LLTC, PCP. 2,495 rows in the file share that start date while
  others reach the 1970s, so it is a batch: names already gone by 2016-17
  were loaded only from then.
- **Old holder of a re-used ticker missing:** ACT, CAM, NE, TE, LB, UA, DTV.
- **Absent:** ACE (merged into Chubb as CB), TYC (into JCI), CBS (to PARA),
  CCE (to CCEP, a new entity), PCL (into WY). Each is a merger survivor's
  history problem, not a missing company.

**Not measured:** price quality. A list says a series exists; whether its
adjusted closes match Yahoo where both exist, and how splits and spin-offs
are handled, needs an API key. How the API addresses the OLD holder of a
re-used ticker is also unread.

## The four vendors, against the checklist

| | delisted EOD | S&P 500 membership | ticker identity | smallest plan with delisted | licence |
|---|---|---|---|---|---|
| **Tiingo** | yes, by the list above; floor 2016-01-04 for some | no | separate rows per listing; API addressing unread | free Starter (500 symbols/month, 50 requests/hour, 1,000/day) covers the 220 in one month; Power $30/month | personal "internal use" on both |
| **Sharadar** (Nasdaq Data Link) | yes, "active and delisted", from 1998 | yes, from 1957, bundled with fundamentals | not confirmed: no public page read says whether it has a permanent id | price shown only after login; not measured | professional vs non-professional; non-pro certifies sole use and gives name and profile URL |
| **Norgate** | Platinum: 25,222 delisted, 1990 on | Platinum: S&P 500 from Mar 1957, as a per-day true/false, no change list | delisted symbols carry a suffix (ALOG-201806) | Platinum $630/year ($346.50 / 6 months) | not read |
| **EODHD** | listed with `delisted=1`; EOD only for names gone before 2018 | separate add-on, $29.99/month, "up to 12 years" | re-used tickers return the NEW company (their own blog's ABS example) | Historian $19.99/month ($199/year) | personal and commercial plan pages, not read |

Blockers found:

- **Norgate's updater runs on Windows 10/11 only** (its installation page);
  its Python package reads the updater's local database. This Mac would need a
  Windows VM. Its 3-week trial is Platinum but limited to 2 years of history,
  too short to test a 2015 window.
- **EODHD's ticker re-use** is the PARA failure this project already paid
  for; it would need the identity audit the crypto mappings have.
- **Sharadar's price** cannot be read without an account.

## What this means

- **Coverage is not the obstacle it looked like.** A keyless list shows one
  vendor holds history for 116 of the 134 ended names (87%), and the 18 are
  mostly the 2016 batch, re-used tickers and merged entities. The remaining questions are price quality
  and identity, which need a key.
- **Membership we already have** (the reconstruction). A vendor's own
  membership (Sharadar, Norgate) would be a cross-check, not a need.
- **Cheapest test with real data:** a free Tiingo key. 220 symbols fit the
  Starter month; compare each departed series against Yahoo on the survivors
  where both exist (the `check_fresh_returns.py` method), then read the 116
  ended ones around their last date. Opening it needs Danny's email, so it is
  his to do; the key would go in `.env` like the CoinGecko one.

## Decisions for Danny

- **A.** Open a free Tiingo key and measure price quality on the 220
  No cost. Assumes the free tier's EOD endpoint answers for an ENDED ticker
  (the pricing page does not say; the public list does not prove it); if it
  does not, Power is $30/month. The Starter cap is 50 requests an hour, so 220 full histories take
  ~4.5 h of paced fetching (unattended), plus ~2 h agent wall-clock to
  compare and write up; assumes one request returns a full history.
- **B.** Log in to Nasdaq Data Link and read Sharadar's non-professional
  price and its TICKERS table's columns. It is the one candidate with
  delisted prices AND membership in one place; if it also has a permanent
  identifier (unconfirmed), it would retire the rename maps. ~10 min of
  your time.
- **C.** Neither now: cross-sectional backtests stay blocked; nothing else
  waits on this.

Norgate (Windows-only) and EODHD (ticker re-use) are not recommended to test
first.

## Sources (read 2026-10-26)

- Tiingo pricing: https://www.tiingo.com/about/pricing; ticker list:
  https://apimedia.tiingo.com/docs/tiingo/daily/supported_tickers.zip
- Sharadar coverage and licence types (via QuantRocket):
  https://www.quantrocket.com/pricing/data/sharadar/
- Norgate packages, content tables, trial, installation:
  https://norgatedata.com/stockmarketpackages.php,
  https://norgatedata.com/data-content-tables.php,
  https://norgatedata.com/freetrial.php,
  https://norgatedata.com/ndu-installation.php
- EODHD pricing: https://eodhd.com/pricing; delisted data:
  https://eodhd.com/financial-apis/delisted-stock-companies-data-2; ticker
  re-use: https://incoherency.co.uk/notes/20260507.html
