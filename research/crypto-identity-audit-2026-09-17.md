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
