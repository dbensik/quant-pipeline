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
