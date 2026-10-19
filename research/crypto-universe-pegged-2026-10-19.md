# Stablecoins and tokenized assets in the crypto universe (decision 46, 2026-10-19)

The `top_100_crypto` snapshot is CoinGecko's top 100 by market cap. 39 of
its 103 current members were unregistered, and 12 registered crypto assets
are pegged to the dollar. Question: should pegged and tokenized assets be in
the crypto universe the screens and strategies use?

## Method

- Every current member was looked up in CoinGecko's top 750 by symbol, with
  the demo key and no other identity sent. For each, I read CoinGecko's own
  categories from `/coins/{id}` and a year of daily prices from
  `market_chart`. 103 of 103 resolved.
- A member's class comes from its CoinGecko categories:
  - **stablecoin:** it has "Stablecoins".
  - **tokenized:** it has "Tokenized Assets" or "Tokenized Gold". These are
    off-chain assets on a chain: T-bill and money-market funds, gold, a VC
    fund, loans.
  - **crypto:** everything else.
- "Real World Assets (RWA)" alone is NOT pegged. ONDO, ALGO, XLM, QNT and
  XDC carry it and are ordinary volatile tokens (70-140% a year).
- Names would have misclassified at least one: STABLE is a layer-1 chain
  with 120% volatility, not a stablecoin.
- For each unregistered `crypto`-class member, I searched Yahoo by the coin's
  NAME and checked each candidate ticker with the existing identity check
  (`core.crypto_identity.verify_identity`: price agreement first, then
  name), recording history length and distinct closes. Scripts: `cg46.py`
  and `yh46.py` in the session scratchpad.

## The 103 members

| class | members | registered | annualized volatility over a year |
|---|---|---|---|
| crypto | 76 | 55 | 24% (TRX) to 293% (BTW); BTC 44% |
| stablecoin | 16 | 6 (USDT, USDC, DAI, USDE, USDS, USD1) | 0.4-4.6% |
| tokenized | 10 | 1 (BUIDL) | T-bill and money funds 0-5.5%; gold (PAXG, XAUT) 29%; USDY 32%, BCAP 132%, FIGR_HELOC 245% (thin, erratic quotes) |

Not current members but registered and pegged: SUSDE, FDUSD and USDT0
(priced), USDTB, and BSC (in `UNPRICED_ASSETS`).

## Why pegged assets should not be in the crypto universe

- **A low-volatility screen ranks every stablecoin first.** Their volatility
  is 0.4-4.6% a year against 24-293% for actual crypto, so the bottom
  quartile of the top 100 would be mostly dollars. The screener fixed in
  decision 43 would return USDT, USDC, PYUSD...
- **Trend and breakout rules trade noise around $1,** and a "crypto
  momentum" ranking that holds a T-bill fund is not measuring crypto.
- **Tokenized gold is gold.** Its 29% volatility is real, but it belongs with
  GLD in the allocation baskets, not in a crypto cross-section.
- **The index itself is correct as recorded.** The snapshot is what
  CoinGecko's top 100 was, and it stays untouched. The question is only
  which members a crypto screen or strategy uses.

## The 21 unregistered real coins

| outcome | coins |
|---|---|
| **Usable Yahoo ticker, identity MATCH (15)** | AERO (`AERO29270-USD`), AKE, BTW (`BTW39158-USD`), CAKE, CC (`CC37263-USD`), ETHFI, JST, LIT (`LIT39125-USD`), M (`M35491-USD`), MORPHO (`MORPHO34104-USD`), NIGHT (`NIGHT39064-USD`), VVV (`VVV35509-USD`), WLFI (`WLFI33251-USD`), ZEC, ZRO (`ZRO26997-USD`) |
| **Rename of a registered asset (1)** | GRAM is Toncoin renamed (CoinGecko id `the-open-network`, "Gram (prev. Toncoin)"), registered as TON-USD in `UNPRICED_ASSETS`. Yahoo's `GRAM-USD` matches: 916 bars from 2024-03-13. |
| **Matches, but unusable (1)** | HTX: 1,000 bars with 5 distinct closes (a $0.0000016 price rounded away), the PEPE case |
| **No Yahoo ticker (4)** | ASTER, PRL, RAIN, STABLE |

- **History is short.** Only CAKE (2020), JST (2020) and ZEC (2017) predate
  2024; eight of the 15 start in 2025-2026.
- **Two cautions for the build:**
  - The bare tickers AERO-USD, M-USD, PRL-USD and WLFI-USD are other
    tokens ("Aerovek Aviation", "MetaVerse-M", "The Parallel", a
    "wlfi.club" copy). The id tickers are the right ones, the same lesson as
    2026-10-04.
  - Yahoo's bare `NIGHT-USD` is "Midnight (midnight.vip)", from 2024 and
    within 0.4% of the real coin's price. The identity check called it a
    MATCH on name similarity. The right ticker is `NIGHT39064-USD`, from the
    2025-12-09 mainnet. Every mapping therefore still goes through the
    bounds audit against CoinGecko's own history before a bar is stored.

## Decision

46. **Recommend A.**

    - **A: classify, exclude pegged from crypto screens by default, add the
      real coins.**
      1. Record `crypto_class` (`crypto`, `stablecoin`, `tokenized`) from
         CoinGecko's categories on every registered crypto asset (99
         rows). Nothing is deleted.
      2. A `top_100_crypto` screen leaves out stablecoins and tokenized
         assets by default and names them in the response, with
         `include_pegged` to keep them.
      3. The 06:00 new-member step classifies a crypto joiner and logs a
         pegged one as excluded, so its unregistered list shows only coins
         that need a human.
      4. Map the 15 coins in `CRYPTO_PROVIDER_SYMBOLS`, register them, run
         the identity audit with `--write` and the bounds audit, and
         backfill their full Yahoo history. A coin that fails the bounds
         audit stays unloaded and is reported.
      5. Re-key TON-USD to GRAM-USD (CoinGecko renamed it). Fetch it as
         `GRAM-USD` if the bounds audit passes its 2024-2026 history;
         otherwise it stays unpriced.
      6. Record HTX, ASTER, PRL, RAIN and STABLE with their reasons, so the
         daily log stops listing them as unknown.

      About 3-4 h of my time, most of it the per-coin audits.
    - **B: classify and exclude only.** Points 1-3. The screens stop
      ranking dollars as crypto; no new coins. About 1.5 h.
    - **C: as now.** A crypto low-volatility screen returns stablecoins.

## Built (A, approved 2026-10-19)

- **Classes recorded.** `core/crypto_class.py` and
  `scripts/classify_crypto.py --write` (112 registered crypto rows): 100
  crypto, 11 stablecoin, 1 tokenized (BUIDL). The first pass matched only
  "Stablecoins" and read SUSDE, USDT0 and BSC-USD as crypto: staked and
  bridged dollars carry only "Yield-Bearing Stablecoin", "Bridged
  Stablecoin" and "Bridged USDT". Any category naming a stablecoin now
  counts, except "Stablecoin Issuer" (WLFI, SKY). FTN-USD and IP-USD have no
  `coingecko_id` and stay unclassified, so they are treated as crypto.
- **Screens.** A `top_100_crypto` screen leaves pegged members out unless
  `include_pegged`, and lists them in `excluded_pegged`. The Screeners page
  has the checkbox.
  - Real run, 60-day volatility from 2026-08-10, bottom 10%: by default BGB,
    BNB, BTC, KCS, LEO, TRX and WLFI. With pegged assets included: DAI,
    LEO, TRX, USDC, USDE, USDS and USDT.
- **New members.** `CRYPTO_MEMBERS_NOT_REGISTERED` lists the 24 members
  left out on purpose: 10 stablecoins, 9 tokenized assets, HTX, ASTER, PRL,
  RAIN and STABLE. The 06:00 step counts them and FLAGS any other
  unregistered crypto member as a new coin, with its CoinGecko class. That
  changes decision 44, where crypto never flagged: the backlog is now
  reviewed, so what remains is a few joiners a month.
- **15 coins added.** Mapped where the bare ticker is another token,
  registered, and identity-audited: 16 of 16 MATCH, GRAM included. Four
  floors were set BEFORE ingest, so no bar was stored and then deleted:
  BTW 2026-03-04, CAKE 2020-09-30, NIGHT 2025-12-10 and WLFI 2025-09-02,
  each a launch-day bar outside CoinGecko's range. 14,511 bars were loaded.
  - The bounds audit after loading: 13 clean and 3 SCATTERED (JST
    2021-04-05, M 2025-07-04, MORPHO 2025-10-10). Each is one bar whose
    close is in range and whose intraday wick grazes CoinGecko's aggregated
    all-time low or high. By the bar's own body none is a bad extreme,
    which is the CRO/ETC false-positive class `repair_bad_extremes.py`
    describes. Kept.
- **TON-USD re-keyed to GRAM-USD** and priced from Yahoo's `GRAM-USD`,
  floored at 2026-06-12. Before that date the key served another token
  (~$0.008; 786 of 916 bars outside Toncoin's range). Strictly, the plan
  said "if the audit passes"; the audit said TRIM, treated as for the other
  four floors. That leaves 129 bars, and Toncoin's history before June 2026
  is not available from Yahoo.
