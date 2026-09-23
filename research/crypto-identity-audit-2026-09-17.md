# Crypto data — why a third of it is wrong (2026-09-17)

The 2026-09-17 integrity sweep found 35 of 99 crypto assets with physically
impossible one-day moves. This is the diagnosis. There are **two separate
faults**, and neither is an ingest bug — our stored bars match the provider
byte for byte.

## Fault 1: 22 assets hold a DIFFERENT TOKEN'S entire history

`data_pipeline/dynamic_universe.py:_fetch_top_100_crypto_tickers` asks
CoinGecko for the top 100 by market cap, keeps only the **symbol**, throws away
CoinGecko's stable `id` and `name`, appends `-USD`, and hands the result to
Yahoo:

```python
symbol = item.get("symbol", "").upper()      # "mnt" -> "MNT"
clean_symbol = symbol.split("-")[0].split(" ")[0]
tickers.append(f"{clean_symbol}-USD")        # -> "MNT-USD"
```

A ticker is not an identifier. Yahoo's `MNT-USD` is a micro-cap called MINTY.
Cross-checking every crypto asset against CoinGecko on **both name and price**:

| our symbol | we meant | Yahoo actually serves | price gap |
|---|---|---|---|
| SPX-USD | SPX6900 | SPEXY | 1.4e11 x |
| TAO-USD | Bittensor | Together As One | 1.9e9 x |
| PENGU-USD | Pudgy Penguins | Pengu (different) | 1.5e8 x |
| HYPE-USD | Hyperliquid | Supreme Finance | 1.6e7 x |
| **UNI-USD** | **Uniswap** | UNICORN Token | 49,065 x |
| **APT-USD** | **Aptos** | Apricot Finance | 4,710 x |
| **MNT-USD** | **Mantle** | MINTY | 2,942 x |
| **SUI-USD** | **Sui** | Salmonation | 2,534 x |
| JUP-USD | Jupiter | Jupiter (different) | 815 x |
| **ARB-USD** | **Arbitrum** | ARbit | 316 x |
| STX-USD | Stacks | Stox | 91 x |
| PI-USD | Pi Network | Plian | 41 x |
| POL-USD | POL (ex-MATIC) | Proof Of Liquidity | 12 x |
| SKY-USD | Sky | Skycoin | 4 x |
| PEPE-USD | Pepe | PEPEGOLD | 2 x |
| BUIDL-USD | BlackRock BUIDL | DFOhub | ~1 x |
| USDS-USD | USDS | Stably | ~1 x |
| USD1-USD | USD1 | Currency One | ~1 x |
| USDTB-USD | USDtb | USDG (ysec.finance) | ~1 x |
| LEO-USD | LEO Token | UNUS SED LEO | ~1 x |
| TON-USD | Toncoin | "TON Token" | — |
| TRUMP-USD | Official Trump | FreeTrump | — |

**22 assets, 27,076 bars, all belonging to a coin nobody chose.** These are not
obscure names — Uniswap, Aptos, Sui, Arbitrum, Mantle, Bittensor and
Hyperliquid are top-50 assets. The last five in the table are the dangerous
ones: the price gap is ~1x, so a sanity check on magnitude alone would pass
them, and only the name reveals the substitution.

**53 assets verified correct** on name and price (BTC, ETH, SOL, XRP, ADA,
DOGE, LINK, AVAX, DOT, LTC...). The majors are fine — their tickers are
unambiguous. It is the mid-caps that collide.

**26 unverified**, not necessarily wrong: mostly liquid-staking derivatives
(STETH, WSTETH, RETH, WEETH, CBBTC, LBTC...) that sit outside CoinGecko's top
200, so this audit had no reference price for them.

## Fault 2: corrupt bars inside CORRECTLY identified series

TIA-USD passes the identity check — Yahoo agrees it is Celestia and the price
is right. It still contains this:

```
2024-03-25   0.0105
2024-03-26   7149.4150     <-- 680,637x in one day
2024-03-27   298.8585
2024-03-28   0.0131
2024-03-30   68.2502
```

Verified against a live fetch: **the spikes are in Yahoo's own data.** We
stored them faithfully. USDE-USD (a stablecoin, so its true range is ~1.00)
shows the same shape at 2121x.

So even for a correctly resolved symbol, provider bars cannot be trusted
without validation. Note `open` always equals the prior `close` in these
series — Yahoo is synthesizing OHLC for thin crypto, which is a further
reason not to treat these as real bars.

## Blast radius

- 27,076 bogus bars.
- **11 of the wrong-asset symbols are live members of `top_100_crypto`**
  membership (APT, ARB, HYPE, MNT, PEPE, PI, POL, SUI, TAO, TRUMP, UNI), so
  any point-in-time crypto screen silently includes them.
- No watchlist and no portfolio trade references any `-USD` symbol, so nothing
  in portfolio accounting is affected.

## What the fix has to be

1. **Stop treating a ticker as an identifier.** CoinGecko returns a stable
   `id` ("mantle", "uniswap"). Keep it, store it on the asset, and resolve
   prices by it — either from CoinGecko directly, or through a verified
   symbol -> Yahoo-ticker map.
2. **Validate identity at registration**, not by eye: compare the provider's
   `shortName` AND price against the reference. The ~1x rows above prove the
   price check alone is insufficient; the name check alone would pass
   "Jupiter" vs "Jupiter". Both are needed.
3. **Validate bars at ingest.** A >10x day-over-day move on an established
   series is not a price, it is a defect. `core/ingest.py` already drops empty
   bars via `is_empty_bar`; this belongs beside it.
4. **Re-fetch the 22 once identity is fixed.** Their stored history is not
   repairable — it is a different asset. Delete and re-ingest under the
   correct mapping.

Until 1-3 exist, re-ingesting crypto just re-imports the same wrong coins.

## What this is NOT

Not an ingest bug, not drift, not staleness. Our pipeline stored exactly what
it was given. The failure is upstream identity resolution plus an absent
validation layer — the same class of error as inferring a corporate fact from
a string lookup, which this project has now been bitten by three times
(delistings, PARA, and here).


---

# The identity layer (built 2026-09-18)

Steps 1-3 of the fix above. Step 4 — deleting and re-ingesting the wrong
assets — is deliberately NOT done: it depends on this layer being trusted
first, and under the current mapping a re-ingest just re-imports the same
wrong coins.

## What shipped

- **`core/crypto_identity.py`** — pure, no network, no database.
  `verify_identity(symbol, reference, quote)` returns one of four verdicts.
- **`data_pipeline/dynamic_universe.fetch_crypto_references(pages=N)`** — a
  SIBLING of `_fetch_top_100_crypto_tickers`, which keeps its `List[str]`
  signature untouched because `get_tickers()` feeds the daily snapshot job and
  point-in-time membership cannot be backdated.
- **`core/ingest.implausible_jump`** + `SymbolOutcome.implausible_jumps`.
- **`scripts/audit_crypto_identity.py`** — reports; `--write` records verdicts.
- 22 new tests; 757 pass.

## Four verdicts, because two signals disagree in two different ways

| verdict | meaning | safe to ingest |
|---|---|---|
| MATCH | name and price both agree | yes |
| WRONG_ASSET | decisive price gap; a different coin | no |
| SUSPECT | prices agree, names do not | no — a live question |
| UNVERIFIABLE | no reference to check against | yes, recorded as unverified |

**Price is checked first and alone**, because a large gap is decisive whatever
the names say — CoinGecko's "Jupiter" and Yahoo's "Jupiter" are 864x apart and
are two different coins. Only when prices agree does the name get a vote, and
there a mismatch buys SUSPECT rather than a verdict:

- **Price cannot settle a stablecoin.** Every stablecoin is $1, so BlackRock's
  BUIDL and DFOhub's BUIDL sit at a 1.4x gap while being unrelated.
- **Name cannot settle an alias.** CoinGecko's "LEO Token" and Yahoo's "UNUS
  SED LEO" are the SAME asset. **The first pass of this audit filed LEO-USD as
  a substitution.** It is a MATCH; dropping the noise word "token" makes "leo"
  a substring of "unus sed leo". Hand reasoning got this wrong, which is the
  reason the rule is tested against all 99 real rows rather than by eye.

## Verdicts over 397 reference coins (4 pages)

**WRONG_ASSET (17)** — SPX6900→SPEXY (1.5e11x), Bittensor→"Together As One"
(2.0e9x), Pudgy Penguins→Pengu, Hyperliquid→"Supreme Finance",
**Sonic→"Holdenomics" (5.1e5x)**, Uniswap→"UNICORN Token" (5.6e4x),
**Official Trump→"FreeTrump" (6,821x)**, Aptos→"Apricot Finance",
Mantle→MINTY, Sui→"Salmonation", Jupiter→Jupiter (864x), Arbitrum→"ARbit",
Stacks→"Stox", Pi Network→"Plian", POL→"Proof Of Liquidity", Sky→Skycoin,
Pepe→PEPEGOLD (1.64x, the narrowest real substitution).

**SUSPECT (4)** — BUIDL→DFOhub, USDS→"Stably USD", USD1→"Currency One USD",
USDTB→"USDG (ysec.finance)". All stablecoins; price cannot decide.

**UNVERIFIABLE (23)** — no reference in the top 397, mostly liquid-staking
derivatives (stETH, wstETH, rETH, weETH, cbBTC). Several look wrong by eye
(IP→"TRUMP IP", LBTC→"Lightning Bitcoin", mETH→"Mirrored Ether",
TON→"TON Token") but the layer refuses to claim what it cannot check.

**MATCH (55)**.

**Reference breadth is the binding constraint, and it is a dial.** Two pages
left 26 unverifiable and found 16 wrong; four pages leaves 23 and finds 17 —
Sonic and Official Trump only became provable at 397 coins.

## Bars are FLAGGED, never dropped

`implausible_jump` counts a move beyond `MAX_DAILY_MOVE_MULTIPLE` (10x) and
stores the bar anyway. The opposite of `is_empty_bar`, on purpose:

- An all-NULL bar carries no information. A 680,637x move carries plenty.
- Crypto genuinely does 10x in a day — BONK, WIF and FARTCOIN are all here.
- Dropping is self-defeating: remove the spike from
  `0.0105 -> 7149.41 -> 298.86` and `0.0105 -> 298.86` is still impossible.
- The hole left behind would be indistinguishable from a provider outage — a
  shape this project already spent a day diagnosing once (PARA).

Jumps are judged in DATE order, so a provider returning bars out of sequence
cannot manufacture them.

## Known limits

- `_get_or_create_asset` short-circuits on the hot path, so metadata attached
  at insert never reaches an existing row. `--write` updates explicitly.
## The gate (wired 2026-09-18)

`core/ingest.py` now refuses to fetch a symbol whose RECORDED verdict says the
provider serves a different asset. Verified against the live database:

```
$ python -m cli.run_pipeline --symbols UNI-USD BTC-USD
WARNING  UNI-USD: skipped — recorded identity is wrong_asset. ...
INFO     Done: 0 row(s) persisted across 2 symbol(s).
WARNING  Skipped 1 symbol(s) whose recorded identity says the provider serves
         a DIFFERENT asset under that ticker: UNI-USD.
```

UNI-USD still holds exactly 1934 bars; BTC-USD proceeded normally.

**A backfill does NOT bypass it — the asymmetry is the point.** `--full-backfill`
deliberately ignores the `delisted` gate, because a backfill REPAIRS a stale
adjustment or a wrongly-flagged rename and must reach those. It cannot repair a
wrong asset: refetching UNI-USD imports more UNICORN Token. Verified — the
backfill skips and the bar count stays at 1934. The only way past the gate is
to fix the mapping and re-verify, which flips the recorded verdict.

Three rules the gate depends on, each with a test that fails without it:

1. **No verdict means no opinion.** 516 equities and 11 ETFs carry no identity
   metadata; treating "unchecked" as "unsafe" would have halted the entire
   daily run the day this shipped.
2. **UNVERIFIABLE passes.** Absence of evidence, not evidence of substitution.
3. **An unrecognised value is not a verdict.** Someone else's metadata must not
   silently block ingestion.

`skip_unsafe_identity=False` is the programmatic override. No CLI flag exists
on purpose: the intended path is to fix the mapping and re-verify, and a
`--force` would just route around the finding.

The CLI summary now also reports `implausible` symbols — bars stored despite a
>10x move. A counter nobody prints is how 35 corrupt crypto series went two
years unnoticed.

## Verdicts recorded 2026-09-19

`--pages 4 --write` wrote identity to all **99** crypto asset rows:
`match 55, unverifiable 23, wrong_asset 17, suspect 4`. Nothing was deleted
and nothing re-ingested — 163,632 crypto bars and 3 flagged assets, both
unchanged.

Each verdict carries the mapping the repair will need:

```json
UNI-USD  {"coingecko_id": "uniswap", "verified_name": "Uniswap",
          "identity_status": "wrong_asset", "identity_checked_at": "..."}
RETH-USD {"identity_status": "unverifiable", "identity_checked_at": "..."}
```

So the 17 wrong assets now record **what they should have been** —
`UNI-USD -> uniswap` — which is exactly the input step 4 needs, and the reason
the `coingecko_id` is stored even on a row whose bars are wrong.

Query the state at any time with:

```sql
SELECT metadata->>'identity_status' AS status, count(*)
FROM assets WHERE asset_class='crypto' GROUP BY 1 ORDER BY 2 DESC;
```


---

# Cleanup, stage 1 (2026-09-19)

## Done: the 17 wrong assets are empty

`DELETE 20431` — every bar belonging to the 17 symbols whose recorded verdict
is `wrong_asset`. Backed up first to
`archive/wrong-asset-bars-2026-09-19.csv` (2.7MB, gitignored), which carries
the intended `coingecko_id` alongside each row. The bars are also trivially
re-fetchable from Yahoo under the same ticker — they are the substituting
token's real history.

**Correction to an earlier figure in this file: 20,431 bars, not 27,076.**
That larger number came from a hand-written list of 22 symbols that included
the 4 SUSPECT and TON-USD. The recorded `wrong_asset` set is 17 symbols.

State now:

| identity | assets | bars |
|---|---|---|
| match | 55 | 113,234 |
| unverifiable | 23 | 27,950 |
| suspect | 4 | 2,088 |
| **wrong_asset** | **17** | **0** |

The asset rows were kept, not dropped: each holds the `coingecko_id` it should
have been, and the ingest gate stops anything refilling them with the wrong
coin. They are a clean "known empty, known wrong source" state, ready for a
price source keyed by id.

## Stop: a numeric rule CANNOT finish this job

Symbols with an impossible one-day move fell from 35 to 21. The remaining 21
are **a mix of genuine corruption and real market history**, and that is the
finding that matters:

| symbol | identity | worst | verdict |
|---|---|---|---|
| TIA-USD | match | 680,637x | corrupt (verified at provider) |
| USDE-USD | match | 2,121x | corrupt — a STABLECOIN; provider now serves nothing at all |
| METH-USD | unverifiable | 1,021,281x | almost certainly the wrong asset (Mirrored Ether vs Mantle Staked Ether) |
| TON-USD | unverifiable | 90x, 40 days | almost certainly wrong ("TON Token" vs Toncoin) |
| OP-USD | match | 2,001x | pre-launch junk before Optimism listed |
| WLD-USD | match | 312x | oscillating 2.3x/0.4x, mixed data |
| **AAVE-USD** | match | **103x** | **REAL** — 2020-10-03, consistent with the 100:1 LEND->AAVE migration. Needs confirming, but it is a redenomination, not a defect |
| **DOGE-USD** | match | **4.6x** | **REAL** — 2021-01-28 is the actual squeeze; 2021-04-16 the April rally. Both present at the provider |
| SHIB, BONK, KAS, HBAR, GT, FIL, OKB | match | 2-5x | plausibly real; crypto does this |

So **no threshold separates these.** 100% would delete the real Dogecoin
squeeze. Even 10x — the `implausible_jump` setting, chosen to sit above
ordinary crypto violence — would delete AAVE's token migration. A
redenomination is a corporate action, and this project already knows that a
constant multiplicative step is exactly what one looks like.

This is the same shape as the delisting work: the numbers narrow the field to
a handful, and each survivor needs a per-symbol judgement against a source.
Stage 2 is that triage, not a bigger DELETE.

## Recommended stage 2, in order

1. **METH-USD and TON-USD** — widen the CoinGecko reference until they are
   provable, then treat as wrong assets. Highest confidence, cleanest fix.
2. **USDE-USD** — a stablecoin with 7 impossible days whose provider now
   returns nothing. Decide whether to keep any of it.
3. **TIA / OP / WLD** — correctly identified coins with corrupt segments.
   Needs a rule for trimming a bad segment without opening a hole, which
   `find_successors.py` already shows is dangerous to get wrong.
4. **Leave AAVE and DOGE alone.** Record why, so the next sweep does not
   re-raise them.


---

# Cleanup, stage 2 — METH-USD and TON-USD (2026-09-20)

Both confirmed WRONG_ASSET and cleared: **`DELETE 2264`** (TON-USD 2213 bars,
METH-USD 51). Appended to `archive/wrong-asset-bars-2026-09-19.csv`.
`wrong_asset` is now 19 assets / 0 bars.

## Why widening the reference set could never have worked

The plan was "widen CoinGecko until they are provable". That was wrong for
both, for two different structural reasons:

- **`mantle-staked-ether` has `market_cap_rank = None`.** `/coins/markets` is
  ORDERED BY market cap, so an unranked coin is on no page at all, ever. No
  depth of paging reaches it.
- **Toncoin has been RENAMED.** CoinGecko id `the-open-network` now carries the
  symbol **GRAM** — "Gram (prev. Toncoin)", rank 30 — so a lookup keyed on
  "TON" finds nothing however deep you page. A crypto ticker rename, the same
  shape as BK->BNY, and the second rename this session has turned up.

The fix is lookup by stable id, not more pages: `CRYPTO_ID_OVERRIDES` in
`config/settings.py` plus `fetch_crypto_references_by_id`. Verdicts:

```
METH-USD  WRONG_ASSET  'Mantle Staked Ether' vs 'Mirrored Ether'   1.6e4x
TON-USD   WRONG_ASSET  'Gram (prev. Toncoin)' vs 'TON Token'         261x
```

## Three defects this exposed in the audit itself

1. **A throttled fetch looked like a missing coin.** CoinGecko 429s, and
   `fetch_crypto_references` swallowed it and returned a SHORT list — so
   `mantle-staked-ether` read as "does not exist" and both symbols were
   recorded UNVERIFIABLE when they are wrong assets. Now retried with
   exponential backoff starting at 15s, because the free-tier window is about
   a minute and a 2s seed exhausted four attempts in 14s.

2. **`--write` would commit verdicts from a partial reference set.** A run
   holding 200 of 400 references was seconds from committing when it was
   killed; it would have DOWNGRADED correct WRONG_ASSET verdicts to
   UNVERIFIABLE, because a missing reference and a missing coin are
   indistinguishable downstream. The audit now refuses to write a short
   reference set unless `--allow-partial` is passed, and `--symbols` allows a
   targeted run that needs only one page.

3. **CoinGecko's own symbols are not unique.** USDF, USDA and PC0000023 each
   appear twice in the top 400. The reference map kept the LAST occurrence,
   letting the smaller coin become ground truth — a bug that never errors, it
   just quietly answers a different question. `index_by_symbol` now keeps the
   highest market cap, with a test.

## Where the remaining 19 stand

Unchanged in character from stage 1: still a MIX, still not separable by a
threshold. AAVE-USD (103x, the LEND->AAVE redenomination) and DOGE-USD (4.6x,
the 2021 squeeze) remain REAL and must not be touched. The corrupt ones are
TIA-USD, USDE-USD, OP-USD, WLD-USD, and the unverifiable BSC/FTN/LBTC/JITOSOL.


---

# Cleanup, stage 3 — USDE-USD (2026-09-21)

**`DELETE 531`**, all of it. Backed up to
`archive/usde-unusable-bars-2026-09-21.csv`.

## This one is NOT an identity failure

USDE-USD's identity is a correct **MATCH** — Yahoo's quote really is Ethena
USDe ($1.009 against CoinGecko's $0.9998), and the name agrees. The identity
layer was right. The HISTORY was fabricated anyway.

Ground truth, from CoinGecko's coin endpoint (all-time, so not subject to the
free tier's 365-day history cap): **USDe has never traded outside
$0.929486 - $1.034** (ATL 2024-10-03, ATH 2025-12-09, rank 25).

Against that, our 531 stored bars held **90 impossible ones (16.9%)** — 69
below the all-time low, 21 above the all-time high — **spread across every
quarter**:

| quarter | bars | impossible |
|---|---|---|
| 2023 Q2 | 68 | 16 |
| 2023 Q4 | 32 | 11 |
| 2024 Q1 | 91 | 3 |
| 2024 Q2 | 91 | 4 |
| 2024 Q3 | 92 | 25 |
| 2024 Q4 | 92 | 22 |
| 2025 Q1 | 20 | 9 |

The lowest stored close is **$0.000021**, for a dollar-pegged stablecoin.

**The all-time low also proves the early data is not USDe.** If USDe had ever
traded at $0.000021, its ATL would be $0.000021, not $0.929486. So the series
is at least two instruments stitched together — the TIA-USD pattern again.

## Why all 531 rather than the 90

Deleting only the impossible bars would leave holes whose SEAMS are still
impossible: take out the $0.000021 bar and the days either side still imply a
move no stablecoin made. That is the same trap documented for TIA-USD — one
bad bar traded for another, plus a hole indistinguishable from an outage.
There is no clean segment to preserve: every quarter from 2023 Q2 on is
affected, and the remaining 441 bars cannot be verified against anything,
because Yahoo now serves **zero** history rows for the symbol and CoinGecko's
free tier caps history at 365 days, which does not reach 2023-2025.

## A second axis on the asset: data quality

Identity answers "is this the coin we meant". Data quality answers "is the
history usable". USDE-USD proves they diverge, so `assets.metadata` now
carries `data_quality`, `data_quality_reason` and `data_quality_checked_at`
beside the identity keys, and the ingest gate blocks on EITHER.

Nothing would be refetched today — Yahoo has no history to give — but if the
provider ever restores that series, a backfill would re-import the same
fabricated bars and the identity check would wave it through, because the
identity was never the problem.

## The gate was telling operators the wrong thing

Wiring this exposed a real defect in the previous stage. Blocked on data
quality, the gate logged:

> USDE-USD: skipped — recorded identity is match. **The provider serves a
> different asset under this ticker** ...

which is false. `ingest_block_reason` now states the actual cause per symbol:

```
USDE-USD: skipped — its stored history is marked unusable (90 of 531 bars
  outside USDe all-time range ...). Identity is not the problem; refetching
  would re-import the same bad bars.
UNI-USD:  skipped — the provider serves a DIFFERENT asset under this ticker ...
```

A confident wrong reason is worse than no message. It is the failure mode this
entire body of work exists to correct, and it had reappeared in the fix.


---

# Cleanup, stage 4 — TIA-USD (2026-09-21)

**`DELETE 1105`**, the contaminated PREFIX only. 561 bars kept. Backed up to
`archive/tia-pre-2025-03-09-bars.csv`.

The first of these that had a segment worth saving, and the first where the
work was a trim rather than a clearance.

## Two instruments spliced at a single date

Celestia's true all-time range is **$0.279235 - $20.85** (CoinGecko, rank
121). Our 1666 stored bars ran $0.001253 - $7,149.42, with **1104 (66.3%)
outside the real range**. By year:

| year | bars | impossible | stored range |
|---|---|---|---|
| 2022 | 307 | 306 | 0.0034 - 0.30 |
| 2023 | 365 | 365 | 0.0013 - 0.03 |
| 2024 | 366 | 366 | 0.0023 - 7149.42 |
| 2025 | 365 | 67 | 0.0056 - 3.71 |
| 2026 | 263 | **0** | 0.285 - 0.61 |

The splice is a single day:

```
2025-03-08   0.006994
2025-03-09   3.025053     <- 432x, the instrument changes here
```

Before it, a micro-cap trading around a cent. After it, Celestia.

## The kept segment is VERIFIED, not assumed

CoinGecko's free tier caps history at 365 days — which for once overlaps.
Comparing our post-splice bars against `coins/celestia/market_chart` over
**361 shared days**: median stored/CoinGecko ratio **0.9808**. The kept
segment really is Celestia.

That is why this is a trim and USDE-USD was a clearance: there, corruption was
scattered through every quarter with nothing verifiable left; here it is one
clean prefix, and the remainder can be checked against an independent source.

## A trim does not stay trimmed by itself

Deleting the prefix is not durable. The provider still serves those bars,
`--full-backfill` starts at 2015, and the identity gate does **not** block
TIA-USD because its identity is a correct MATCH. One backfill would have put
all 1105 back.

So a cleaned asset now records `history_valid_from`, and `core/ingest.py`
honours it two ways: the fetch window is clamped to it, and any bar that comes
back earlier anyway is refused and counted (`skipped_before_floor`). Both are
mutation-tested — removing either one fails its own test.

Verified live: `--full-backfill TIA-USD` fetched **561** records, not eleven
years, and the series was unchanged.

## Where this leaves crypto

TIA-USD is off the impossible-move list entirely. Remaining:

| symbol | identity | worst | note |
|---|---|---|---|
| OP-USD | match | 2001x | pre-launch junk, same prefix shape |
| WLD-USD | match | 312x | oscillating 2.3x/0.4x |
| BSC-USD | unverifiable | 9x | 9 days |
| USDS-USD | suspect | 11x | stablecoin, identity unresolved |
| FTN/LBTC/JITOSOL | unverifiable | 3-4x | |
| **AAVE-USD** | match | **103x** | **REAL** — LEND->AAVE redenomination |
| **DOGE/SHIB/BONK/KAS** | match | 3-5x | **REAL** — leave alone |

OP-USD looks like the same single-splice shape as TIA and should be the next
one; it is a trim, not a clearance.


---

# Cleanup, stage 5 — OP-USD (2026-09-22)

**`DELETE 206`**, prefix only, 1447 bars kept. Backed up to
`archive/op-pre-2022-10-06-bars.csv`. The TIA-USD shape exactly, and the
method transferred without modification.

Optimism's true all-time range is **$0.080689 - $4.84** (CoinGecko, rank 152).
Our 1653 bars ran $0.000320 - $4.7033 — the top is fine, the bottom is not.
**206 bars (12.5%) below the all-time low, ALL of them in one prefix.**

The splice is one day, and the prefix is a dead stub rather than a series:

```
2022-10-01 .. 10-05   0.000425   (flat, five identical closes)
2022-10-06            0.850509   <- 2001x
```

A frozen repeated close is the same shape as the EQR stub found in the
corporate-actions work — a provider serving a placeholder under a key nothing
trades on.

Kept segment verified against `coins/optimism/market_chart`: **362 overlapping
days, median stored/CoinGecko ratio 0.9933**. After the trim the series runs
$0.0820 - $4.7033, entirely inside the true range, with the low a whisker
above the real all-time low.

`history_valid_from = 2022-10-06` recorded. Verified live: `--full-backfill
OP-USD` fetched **1447** records, not eleven years.

## Crypto after five stages

| stage | symbol | action | bars |
|---|---|---|---|
| 1 | 17 wrong assets | cleared | -20,431 |
| 2 | METH, TON | cleared | -2,264 |
| 3 | USDE | cleared | -531 |
| 4 | TIA | trimmed | -1,105 |
| 5 | OP | trimmed | -206 |

Worst remaining move is now **312x (WLD-USD)**, down from 680,637x. WLD is the
natural next one — an oscillating 2.3x/0.4x pattern rather than a single
splice, so it may be neither a trim nor a clearance.

**Do not touch AAVE-USD (103x, the LEND->AAVE redenomination) or
DOGE/SHIB/BONK/KAS (3-5x).** Those are real.


---

# Cleanup, stage 6 — WLD-USD (2026-09-22), and where this stops

**`DELETE 503`**, prefix only, 1147 bars kept. Backed up to
`archive/wld-pre-2023-08-02-bars.csv`.

I expected this one to need a different technique — its 2.3x/0.4x oscillation
across separate days looked like interleaved bad bars rather than a prefix,
which would have hit the seam problem. **That was wrong.** The oscillation sits
entirely inside the prefix, and WLD is the same single splice as TIA and OP.

Worldcoin's true range is **$0.229961 - $11.74** (rank 57). Of 1650 bars, 503
sat below the all-time low and **zero impossible bars fall after 2023-08-02**.
The two segments are even separated by an 11-day hole — last junk bar
2023-07-22, real WLD resumes 2023-08-02 — which is `max_gap`'s signal showing
up again, on an asset whose identity was never in question.

Kept segment verified against `coins/worldcoin-wld/market_chart`: 362
overlapping days, median ratio **0.9963**. Range after the trim is
$0.2344 - $11.6924, inside the true range at both ends.

## The severe corruption is gone

| stage | symbol(s) | action | bars |
|---|---|---|---|
| 1 | 17 wrong assets | cleared | -20,431 |
| 2 | METH, TON | cleared | -2,264 |
| 3 | USDE | cleared | -531 |
| 4 | TIA | trimmed | -1,105 |
| 5 | OP | trimmed | -206 |
| 6 | WLD | trimmed | -503 |

**Worst remaining one-day move is 102.9x — AAVE-USD, which is REAL** (the
100:1 LEND->AAVE redenomination). Everything above 11x is now either genuine
market history or an identity question already recorded. From 680,637x to
"the largest anomaly is a real corporate action" in six stages.

## Why per-symbol triage should stop here

The remaining list is no longer about corrupt bars:

- **Real, leave alone:** AAVE (102.9x), SHIB (5.3x), DOGE (4.6x), BONK, KAS,
  OKB, FIL, GT, HBAR (2-3x). Crypto does this.
- **Identity unresolved:** USDS-USD and BUIDL-USD (SUSPECT — stablecoins,
  where price cannot separate an alias from a substitution), and BSC-USD,
  FTN-USD, LBTC-USD, JITOSOL-USD (UNVERIFIABLE — outside the reference set).

Those need identity work, not bar surgery, and the identity layer already
knows it cannot settle them alone.

## The technique that did the work should be automated

Every one of stages 3-6 was decided by the same cheap test: **compare stored
bars against the coin's CoinGecko all-time high and low.** It is decisive in a
way a day-over-day threshold never was — it cleared AAVE's 102.9x as real and
condemned USDe's 2,121x, which no single multiplier could have separated.

That belongs in a script beside `audit_crypto_identity.py`, run over the whole
crypto universe, rather than being re-derived by hand per symbol. It would also
catch contamination that never produces a big day-over-day jump — a wholly
wrong series at a plausible-looking level, which is exactly what the 17 wrong
assets were.


---

# The bounds check, automated (2026-09-22)

`core/price_bounds.py` + `scripts/audit_crypto_bounds.py`. The test that
decided stages 3-6, now run over the whole universe in one pass instead of
being re-derived by hand per symbol.

## Why this test and not a jump threshold

The question is not "is this move large" but **"has this asset ever been worth
that"**. Only bounds answer it, and the two cases that prove it cannot be done
with a multiplier are both from this cleanup:

- AAVE-USD moved **102.9x** in a day and the move is CORRECT — the 100:1
  LEND->AAVE redenomination.
- USDE-USD moved **2,121x** and was fabricated — a dollar-pegged stablecoin.

Bounds also catch what a jump test *structurally cannot see*: a wholly wrong
series with **no jump in it at all**, sitting at a plausible-looking level.
That is precisely what the seventeen wrong assets were.

Four verdicts: CLEAN, TRIM (violations form a contiguous prefix — the TIA/OP/
WLD shape, so the cut date is reported), UNUSABLE (every bar violates),
SCATTERED (violations throughout, so no single cut works — USDe's shape, and
removing bars piecemeal leaves seams that are still impossible).

One CoinGecko call covers the universe: `/coins/markets` carries `ath` and
`atl` and takes up to 250 ids, which matters because that API throttles hard.

## Results over 99 crypto assets

**55 CLEAN and now positively verified** — not "no alarm raised", but every bar
inside the coin's published all-time range.

**AAVE-USD: TRIM at 2020-10-03, one bar.** `2020-10-02 = $0.516571` is a
pre-redenomination LEND price sitting in an AAVE series.

**This corrects an earlier call in this file.** Stages 1-6 repeatedly said
"leave AAVE alone, the 102.9x is real". Half right: it is not corruption, and
it must not be deleted as such. But the single LEND-priced bar still
manufactures a 102.9x return **no holder ever experienced** — holders received
1 AAVE per 100 LEND and their value was unchanged. It is the same class of
defect as unadjusted split drift, and a day-over-day rule could never have
distinguished it, because the size of the move is exactly what made it look
legitimate. Suggested, not applied.

**BUIDL-USD: SCATTERED, 763 of 792 bars (96.3%) outside range.**
**USDS-USD: SCATTERED, 67 of 1091 (6.1%), violations dating from 2020.**
Both are the SUSPECT stablecoins, where price alone could not separate an
alias from a substitution. Bounds are independent corroboration that these are
the wrong assets: BlackRock's BUIDL did not exist in 2020, and 96% of a series
cannot sit outside its own range.

**21 have no bars** — the cleared wrong assets. Reported separately rather than
counted CLEAN, because an empty series is vacuously inside any range; calling
them clean would be true and useless.

**20 have no CoinGecko id**, so nothing to check against: the UNVERIFIABLE
cohort, mostly liquid-staking derivatives below the reference set. Reference
coverage remains the binding constraint, exactly as it was for identity.

## What it deliberately does not do

`--write` records `bounds_status`, `bounds_checked_at` and, for a TRIM,
`bounds_suggested_valid_from` — a **suggestion**. `history_valid_from` is what
ingestion obeys, and setting it means deleting bars, which stays a human
decision. The script never deletes and never changes what ingestion fetches.

It also skips bars before an existing `history_valid_from`: those were already
deliberately removed, and judging a series on history someone has explicitly
disowned would re-raise closed findings.


---

# AAVE-USD trimmed (2026-09-22) — and the check had a blind spot

**`DELETE 2`**, cut at **2020-10-04**, not the 2020-10-03 the bounds check
first suggested. 2179 bars remain.

## The suggestion was one bar short

Inspecting before applying showed the cut date was wrong:

```
            open      high      low       close     vol
2020-10-02  0.0000    0.5166    0.0000    0.5166    0      <- LEND stub
2020-10-03  0.5238   65.3059    0.5238   53.1515    0      <- STRADDLES, 124.7x range
2020-10-04 53.1799   55.0704   50.6890   52.6750    0      <- clean AAVE
```

2020-10-03 opens and lows on the **old LEND basis** and highs and closes on
the **new AAVE basis**. Its CLOSE is perfectly in range, so a close-only check
called it clean and proposed cutting at that very bar — which would have left
a phantom **124.7x intraday range** as the first bar of the series, poisoning
every range, ATR and candlestick consumer while the close series looked fine.

`check_bounds` now judges `low` and `high` as well as `close` (zero treated as
an absent price, not a claim the asset was worthless), and the script passes
them. With the full range the verdict moves to **trim at 2020-10-04**, and a
test pins both answers so the regression cannot come back.

This is the second time a check in this work asserted something its evidence
did not support. The fix is the same each time: make the test look at what it
was actually claiming to judge.

## Result

AAVE-USD now runs 2020-10-04 onward, $27.72 - $632.27, entirely inside its
published range, **worst intraday ratio 2.5** (was 124.7). `history_valid_from`
is set and survived a `--full-backfill`.

And the 102.9x close-to-close move is gone — a return **no holder ever
earned**, since they received 1 AAVE per 100 LEND and their value was
unchanged. It was never corruption, which is why six rounds of day-over-day
scanning kept correctly declining to delete it and never noticed what was
actually wrong with it.

**Worst remaining one-day move across all crypto is now 11.1x (USDS-USD)**,
down from 680,637x — and USDS is a SUSPECT identity, not a bar defect.


---

# BUIDL-USD and USDS-USD reclassified WRONG_ASSET (2026-09-22)

## A hand-edit would not have survived

`audit_crypto_identity.py --write` recomputes every verdict from name and
price on each run. Editing `identity_status` in the database would have been
reverted the next time it ran, and both symbols would flip back to SUSPECT for
ever.

So the decision lives in `CRYPTO_IDENTITY_OVERRIDES` (`config/settings.py`),
applied by `apply_identity_override`, which **only ever overrides SUSPECT**. A
MATCH or a WRONG_ASSET rests on a decisive price gap that no stored opinion
should talk it out of, and UNVERIFIABLE means there was no reference at all —
overriding that would assert a comparison nobody made. The override also keeps
the measured evidence: it changes the conclusion, not the numbers it was drawn
from. An override that no longer applies is logged rather than ignored,
because a stale human decision is worth noticing.

## The evidence, and why it is not a threshold

SUSPECT exists because price AT A POINT cannot separate two $1 stablecoins.
Price HISTORY can, because **a coin cannot have traded before it existed**:

| symbol | reference | provider | bars outside range | earliest violation |
|---|---|---|---|---|
| BUIDL-USD | BlackRock BUIDL | DFOhub | 763 of 792 (96.3%) | 2020-06 |
| USDS-USD | USDS | Stably USD | 67 of 1091 (6.1%) | 2020-02 |

Neither BlackRock's fund nor Sky's USDS existed in 2020.

**Deliberately not automated as a percentage rule.** USDS violates on 6.1% of
bars and BUIDL on 96.3%; any threshold that promotes the first is arbitrary
enough to misfire elsewhere. What settles both is the DATE of the violations —
a judgement about each coin's history, not a number.

## Bounds split the SUSPECT cohort rather than dissolving it

Of the four SUSPECT stablecoins, bounds condemned two and **cleared the other
two**: USD1-USD and USDTB-USD came back CLEAN, every bar inside range. They
stay SUSPECT, which is the honest answer — their names disagree with the
reference but nothing in their price history contradicts them.

That is the result worth having. The evidence resolved half the cohort on its
merits instead of sweeping all four into one bucket.

## State

    match         55 assets   111,050 bars
    unverifiable  21 assets    25,735 bars
    wrong_asset   21 assets     1,883 bars   <- the two just reclassified
    suspect        2 assets       205 bars

The gate now blocks both symbols from ingestion. **But this is the first time
`wrong_asset` holds any bars** — the other nineteen are at zero. Those 1,883
bars are a different company's prices and any crypto screen still consumes
them; clearing them is the obvious next step and a destructive one, so it is
not taken here.


---

# BUIDL-USD and USDS-USD cleared (2026-09-23) — crypto cleanup complete

**`DELETE 1883`** (BUIDL 792, USDS 1091), appended to
`archive/wrong-asset-bars-2026-09-19.csv`. `wrong_asset` is back to 21 assets
and **0 bars**, consistent with the other nineteen.

The bounds keys were dropped from both rows at the same time: they described a
series that no longer exists, and the other nineteen carry no bounds verdict
either (they were already empty when that check ran). `identity_status` plus
the documented override in `CRYPTO_IDENTITY_OVERRIDES` remain the durable
explanation for why these rows are empty.

## Final state

    match         55 assets   111,050 bars
    unverifiable  21 assets    25,735 bars
    suspect        2 assets       205 bars
    wrong_asset   21 assets         0 bars

**Worst one-day move anywhere in crypto: 9.0x (BSC-USD).** It began at
680,637x.

Nothing on the remaining list is known to be wrong:

| symbol | identity | worst | reading |
|---|---|---|---|
| BSC-USD | unverifiable | 9.0x | no reference — outside CoinGecko's top 400 |
| SHIB / DOGE | match | 5.3x / 4.6x | REAL — the 2021 squeezes |
| FTN / LBTC / JITOSOL | unverifiable | 3-4x | no reference |
| BONK / KAS / OKB / FIL / HBAR / GT | match | 2-3x | plausible crypto moves |

Every `match` on that list has passed the bounds check: every bar inside the
coin's published all-time range. The only real unknowns are the UNVERIFIABLE
cohort, and they are unknown for one structural reason — **no reference exists
to check them against**, mostly liquid-staking derivatives below CoinGecko's
top 400.

## What would actually move the needle now

Reference coverage, not more cleanup. 21 assets carry no `coingecko_id`, so
neither the identity check nor the bounds check can say anything about them.
CoinGecko has ids for most (`stETH`, `wstETH`, `rETH`, `weETH`, `cbBTC` all
exist there); they are simply unranked, which is the same structural gap that
hid `mantle-staked-ether`. The fix is the one already built —
`CRYPTO_ID_OVERRIDES` plus `fetch_crypto_references_by_id` — applied to the
remaining 21 rather than to two.

That is a mapping exercise, not an investigation, and it would let both checks
finally cover 100% of the universe instead of 79%.


---

# Mapping the unranked coins (2026-09-23) — coverage 79% -> 98%

18 of the 20 unmapped assets now carry a `coingecko_id`, resolved from
CoinGecko's full **21,382-coin index** rather than from market-cap paging,
which structurally cannot reach an unranked coin.

Where a symbol had several candidates the largest by market cap wins, and the
margin was decisive every time — WBTC $10.1B against $635M for the next (an
Arbitrum bridge wrapper), WETH $5.75B against $1.4B, wstETH $12.9B against
$216M. The also-rans are chain-specific bridge wrappers, not the token.

**BSC-USD is the original bug in miniature.** Its CoinGecko symbol is literally
`bsc-usd`, so stripping "-USD" to get a base symbol leaves "BSC", which
resolves to a $119k micro-cap called Binance Super Cycle. Looked up by the full
symbol it is `binance-bridged-usdt-bnb-smart-chain`.

## Three new wrong assets, invisible until now

| symbol | we meant | Yahoo serves | gap |
|---|---|---|---|
| CBBTC-USD | Coinbase Wrapped BTC | cbBTC | **5.0e9x** |
| LBTC-USD | Lombard BTC | Lightning Bitcoin | **1.7e6x** |
| BSC-USD | Binance Bridged USDT | BowsCoin | 1,586x |

None could have been found before: with no reference, they were UNVERIFIABLE,
and UNVERIFIABLE passes the ingest gate. Coverage was not a tidiness exercise.

## The mapping broke three assets, and that is worth recording

SUSPECT **blocks ingestion**. Mapping ids turned SOLVBTC, WSTETH and JLP from
UNVERIFIABLE (allowed) into SUSPECT (blocked) — so an improvement in coverage
silently stopped three legitimate assets from updating. Caught by running the
ingest immediately after.

All three are one asset written two ways, prices agreeing within 0.5%:

    SOLVBTC   "Solv Protocol BTC" vs "SolvBTC"                   1.005x
    WSTETH    "Wrapped stETH"     vs "Lido wstETH"               1.003x
    JLP       "Jupiter Perpetuals Liquidity Provider Token"
                                  vs "Jupiter Perps LP"          1.000x

The matcher uses containment after dropping noise words, which cannot see an
abbreviation ("Perps") or a concatenation ("SolvBTC"). **Loosening it was
rejected**: at a ~1x price gap the name is the ONLY signal, and that is exactly
where a looser match would start waving through real substitutions like
BUIDL/DFOhub. Settled as `match` in `CRYPTO_IDENTITY_OVERRIDES` instead — a
narrow matcher plus explicit, auditable human decisions.

## Two left unmapped, on purpose

- **FTN-USD** — "Fasttoken" appears NOWHERE in the 21,382-coin index: zero hits
  on symbol, name or id. It was top-100 when registered, so it has been
  delisted from the reference itself. Nothing to check against.
- **IP-USD** — the only lead is id `story-2`, now "Data Network" (symbol DATA,
  rank 332). Story Protocol's ticker was IP and CoinGecko keeps an id across a
  rename, which is how `the-open-network` still holds Toncoin's history under
  GRAM. But that is an inference from an ID STRING with no name or price
  agreeing — the exact mistake this subsystem exists to correct.

## A new defect class: bad high/low ticks

The wider bounds run surfaced **83 bars across 20 symbols whose high or low is
implausible while the CLOSE is correct** — e.g. 2021-11-16 DAI high 3.67 on a
$1 stablecoin, USDC high 2.35, WBTC high 162,188 (above its own all-time high),
WETH high 33,329 with low 0.0000.

Only 2021-11-16 hits more than two symbols, so this is scattered provider
noise rather than one bad day. It is invisible to a close-only check and was
found only because low/high judging was added for the AAVE redenomination.

Not repaired: closes are sound, so close-to-close returns are unaffected, but
any range-based calculation (ATR, true range, candlesticks, intraday
volatility) is wrong on those bars. Nulling the extremes while keeping the
close is a different kind of repair from anything done so far.

## Coverage

    identity + bounds now cover   97 of 99 crypto assets
    unreferenceable                2 (FTN-USD, IP-USD)


---

# Bad-tick extremes repaired (2026-09-23)

**38 bars across 12 symbols** had a sound close and a corrupt high or low.
High and low nulled; **no close changed, no bar deleted.** Backed up to
`archive/bad-extreme-bars-before-2026-09-23.csv`.

## Null rather than clamp

The true extremes are unrecoverable — no arithmetic gets the real intraday
range back from garbage. Clamping to the open/close envelope would put a
plausible-looking number in the database that nobody could later tell from a
real one. NULL says "unknown", which is true.

The frontend already handles it: `candlestickData.ts` drops bars without a
complete OHLC quartet and COUNTS them as `droppedIncomplete`, precisely so a
candle view showing fewer bars than the line view does not look like a
rendering bug. 392 migrated rows were already this shape; there are now 430.
Its 12 tests still pass.

## The detector was wrong twice, and the data said so both times

**First version judged extremes against the asset's ALL-TIME RANGE.** It
flagged CRO at a high 1.14x its close and ETC at 1.31x — ordinary intraday
moves that merely grazed CoinGecko's recorded high, a figure drawn from a
different exchange set than the bar. Judging an extreme against its own open
and close needs no reference data at all, which is also why this now covers
equities and ETFs.

**Second version used a SYMMETRIC 2x threshold.** It flagged ONDO, RENDER,
TIA, WIF and WLD all wicking to 0.31-0.48x of their close on **2025-10-10** —
five unrelated alts on one day, which is a liquidation cascade, not provider
noise. A symmetric rule would have deleted real market history: the AAVE
mistake again, one step from being repeated.

So the thresholds are asymmetric, because a market can crash 60% intraday and
recover but cannot double and retrace:

    high > 2.0x the bar's body      real: 2.08x - 3.66x; widest legitimate 1.46x
    low  < body / 10                real: SEI at 0.045x; deepest real wick 0.31x
    any extreme <= 0                always wrong

## And it flagged its own repairs

The first `--apply` worked, then the rescan still reported 38 bars — a nulled
extreme tripped the "missing" branch. A None is ABSENT (a bar that never had
one, or one already repaired); only a zero is WRONG. Without that distinction
a fix looks like it never worked. Rescan now returns zero.

## Still outstanding

**BSC-USD (2456 bars), LBTC-USD (1960) and CBBTC-USD (183)** — the three wrong
assets found by the id mapping — still hold 4,599 bars between them. They were
skipped by this repair, since nulling two extremes in a series where every bar
belongs to another coin is meaningless work that also makes the series look
tended-to. Clearing them, as the other 21 were cleared, is the open item.
