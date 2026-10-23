# News fallbacks: title match, crypto and word tickers on the Research page (2026-10-22)

Open pick 57 had two unbuilt amendments to the Research page's news rule,
both from `research/news-source-2026-10-22.md`:

1. **A title match:** keep an untagged story whose title names the ticker.
2. **A crypto base-symbol fallback:** search `ETH`, not `ETH-USD`.

Both were measured tonight. A and B below were approved ("build A+B") and
built the same night; see "Built" at the end.

## Verdict

- **Title match: measured, NOT adopted.** About 2 of every 3 stories it lets
  back in are about something else.
- **Crypto:** the base symbol alone is the wrong fix. Searching the pair,
  the base symbol AND the verified name gives 19 of 20 large coins a feed
  where today they get none.
- **Word tickers (not in pick 57):** the Research page still searches `KEY`,
  `DE` and `TECH` by the ticker alone and gets 0 of 10 stories about the
  company. The archive's measured `NEWS_SEARCH_TERMS` gets 10 of 10.
- **Side finding:** the archive does not count a mapped coin's own stories.
  Hyperliquid's page is never refetched hourly because of it.

**Recommended:** A (the Research page searches with the archive's terms) and
B (count a coin's provider ticker as its own) together. About 2–2.5 h of
agent time.

## 1. Title match (measured on the archive)

**Data.** The archive's 28,280 equity and ETF links (570 symbols, stored
2026-10-22 18:51 to 2026-10-23 01:04 UTC). 2,140 of them fail today's tag
rule: the story is not tagged with the ticker or another share class of it.
Of those 2,140, 816 carry no tags at all.

A whole-word match of the ticker in the title (no letter or digit on either
side) would re-admit:

| form | links re-admitted | symbols |
|---|---|---|
| case-sensitive | 687 | 157 |
| case-insensitive | 1,943 | 196 |
| strong only: `(SYM)`, `$SYM`, `NYSE: SYM` | 37 | 28 |

Case-insensitive is out without labelling. Most of what it adds beyond the
case-sensitive form is ordinary words: KEY 99, ARE 72, NOW 54, GEN 52, LOW 48.

**A random sample of 150 case-sensitive matches, labelled by hand (seed 7):**

- **47 right (31%).** For example: KKR (8 of 8 in the sample), CRH and APA
  price-target notes, TKO Group, PDD, SKYD, EPAM, MAA and BNY transcripts,
  HCA hospitals, and BRK-B.
- **103 wrong (69%).** Acronyms dominate:
  - RMD is a required minimum distribution (8 in the sample, 35 in all).
  - COO is a chief operating officer (10).
  - CMS is the Centers for Medicare & Medicaid Services (7).
  - ESS is a BW ESS battery project or ESS Tech (8).
  - CPT is a medical billing code; PPG is photoplethysmography.
  - Others are different companies with the same letters: EQT is the Swedish
    EQT AB, not EQT Corp (7); SOLV is SOLV Energy, not Solventum (4); there
    are also MSI (Micro-Star), HPQ Silicon, TKO Hotels, FIS Water and NVIDIA
    DGX.
- Scaled to the 687 matches, that is about 215 right stories bought with
  about 470 wrong ones. The tag rule already keeps 26,140.

**The strong form** (`Company (SYM)`) gets 25 of 37 right (68%). It misses for
three reasons:
- The parentheses also cite other things: a Marketing Authorisation
  Application (MAA), Tethys Petroleum (TPL), Tesco (TSCO).
- The PARA ticker now belongs to Banzai International.
- Market reports.

That is 25 stories against 26,140.

**Why a text rule can't do much better:**
- Only 22.8% of correctly tagged titles name their own ticker at all, so a
  title rule can never recover most untagged stories.
- The right untagged stories are mostly Yahoo leaving a story untagged:
  earnings-call transcripts, "Company (SYM) Stock Looks..." headlines and
  price-target notes. Those are a tagging gap at Yahoo, not something
  matching the title reliably fixes.
- 47 of the 687 matches are tagged with a foreign listing of the same letters
  (`EQT.ST`, `HPQ.V`, `FOX.TA`, `TPL.V`). Excluding them would remove some of
  the wrong ones, but not the acronyms, which are most of the wrong ones.

The relevance cut stays the tag rule.

## 2. Crypto on the Research page (measured live, 20 coins)

The page asks Yahoo for 10 stories a term. Today it searches the pair
(`ETH-USD`). Measured 2026-10-22 about 21:45 ET on 20 large coins, with an
empty answer retried twice 20 s apart. A story counts when Yahoo tags it with
the pair or with the coin's `CRYPTO_PROVIDER_SYMBOLS` ticker.

| coin | pair | base | name | distinct tagged |
|---|---|---|---|---|
| BTC | 9/10 | 9/9 | 7/7 | 16 |
| ETH | 0/0 | 10/10 | 10/10 | 17 |
| SOL | 0/0 | 1/10 | 9/9 | 9 |
| LTC | 0/0 | 1/10 | 10/10 | 10 |
| DOT | 0/0 | 0/10 | 4/5 | 4 |
| NEAR | 0/0 | 0/10 | 5/5 | 5 |
| UNI | 0/4 | 1/10 | 10/10 | 10 |
| LINK | 0/0 | 2/10 | 9/10 | 10 |
| TRX | 0/0 | 3/9 | 4/10 | 4 |
| the other 11 | 0 each | 3–10 | 4–10 | 5–13 |

Each cell is tagged stories over stories returned. "Distinct tagged" counts
the base and name searches together.

- **Today 19 of 20 coins get nothing.** Only BTC's pair answers.
- **The base symbol alone is not enough:** DOT and NEAR get 0 from it, and
  SOL, LTC and UNI get 1. The verified name does most of the work, as it did
  in the archive (349 tagged stories by base symbol, 749 with the name added).
- **The pair must stay.** BTC's pair gave 9 tagged stories, and only 1 of
  them came back from the base or name search.
- **Yahoo tags a mapped coin with its provider ticker.** All 10 Uniswap
  stories carry `UNI7083-USD`, and 3 of them do not carry `UNI-USD`. So for a
  coin, the relevance rule's aliases must include the
  `CRYPTO_PROVIDER_SYMBOLS` ticker. `share_classes` covers equities only and
  does not supply it.

## 3. Word tickers on the Research page (measured live, 24 names)

Same run: each of the 24 `NEWS_SEARCH_TERMS` names searched by the ticker,
then by its term, 10 stories each. The cells count stories tagged with the
ticker.

| | stories tagged with the ticker, out of 10 |
|---|---|
| ticker (today) | mean 3.0: KEY, DE, AMP, APP, HAS, TECH 0; ALL, FAST, GEN, NOW, TEAM 1 |
| term | mean 9.7: 23 of 24 names at 9 or 10 |

FOX is the exception: its term gets 5 of 9. Searching the term adds to the
ticker's results and replaces nothing. The term's tagged stories already
include everything the ticker found, on every one of the 24.

## 4. Side finding: the archive undercounts a mapped coin's own stories

`own_tagged` (core/news_archive.py) counts stories tagged with the SYMBOL
alone. 142 archive links on 22 mapped coins are tagged only with the
provider ticker: HYPE 43, UNI 23, SUI 13, ARB 10.

This matters because `is_full` needs half the page to be the symbol's own,
and `due` never schedules a page it judges not full for the hourly pass. The
same judgement keeps `window_truncated` from ever being set on that page.

- Hyperliquid's last page held 49 stories, and its own count was 0. Of the
  40 stories stored from it, 34 carry `HYPE32196-USD`.
- So the busiest mapped coin's name page has not been refetched since 20:15
  UTC, and any stories it lost would not have been recorded.
- Correction made while building: the WORD TICKER? and WEAK TERM review
  lines are not affected. A coin's terms are never its symbol and never a
  `NEWS_SEARCH_TERMS` entry, so coins never reach either line.
- Nothing stored is wrong; every story was kept unfiltered. The fix changes
  how new fetches are counted. Past `news_fetches` rows are never rewritten.

## Proposed (a pick for Danny)

- **A. The Research page searches with the archive's terms.**
  - **Terms:** each symbol's terms come from the registry: its asset class,
    `verified_name` and provider ticker. The page then searches `[symbol] +
    search_terms(...)` with duplicates removed:
    - a coin by its pair, base symbol and name;
    - a `NEWS_SEARCH_TERMS` equity by its ticker and term;
    - every other symbol by its ticker alone, as today.
  - **Relevance:**
    - a story counts when it is tagged with the symbol, a share class, or a
      coin's provider ticker;
    - the rule still runs per symbol before de-duplication;
    - every drop is still counted in `unrelated_dropped`.
  - **Failures:**
    - each term is cached on its own;
    - an empty answer is still never cached;
    - a symbol fails only when every one of its terms fails;
    - the feed is 503 only when every symbol fails.
  - **Cost:**
    - up to 3 searches a coin and 2 for a listed equity, run in the existing
      5-thread pool;
    - the default market feed (SPY, QQQ, DIA, BTC-USD) goes from 4 to 6
      searches on an uncached load.
  - **Estimate:** about 1.5–2 h of agent time, with tests that run without a
    network or database and checks that the tests fail on the old code. This
    assumes the registry lookup can be one new repository method beside
    `share_classes`.
- **B. The archive counts a coin's provider ticker as its own.**
  - `own_tagged` and the step's review lines accept the
    `CRYPTO_PROVIDER_SYMBOLS` ticker.
  - **Estimate:** about 30–45 min with tests. Hyperliquid is then refetched
    hourly once its next 06:00 page is counted.
- **Not adopted:** the title match, in either form, for the reasons in
  section 1.

**Recommended:** A and B together, about 2–2.5 h.
- B is the smaller and more urgent of the two: it is a coverage gap in data
  that cannot be backdated.
- A gives the Research page news for coins and for word tickers that it does
  not get today.

## Reproduce

The scratch scripts are not committed:
- `title_match.py` reads the archive export.
- `live_page.py` makes about 108 read-only Yahoo searches. No identity is
  sent.

Labels for the 150 sampled links are by hand, from the title and the tags.

## Built (2026-10-22, "build A+B")

- **B.** `core.news_archive.own_tickers(symbol)` is the symbol plus a coin's
  `CRYPTO_PROVIDER_SYMBOLS` ticker, and `own_tagged` counts a story once when
  it carries any of them. Past `news_fetches` rows are unchanged. Each mapped
  coin's next fetch is counted right, so Hyperliquid becomes due hourly after
  its next page.
- **A.**
  - `ValuationData.news_identities` reads each registered equity, ETF or
    coin's asset class and `verified_name`.
  - `/research/news` searches `[symbol] + search_terms(...)` for a
    registered symbol, and an unregistered one by itself as before.
  - A coin's aliases include its provider ticker (`own_tickers`).
  - `YFinanceGateway.fetch_related_news(..., terms=)` caches each term on its
    own.
  - A story found by two of one symbol's terms is judged, and counted, once.
  - A symbol fails only when all its terms fail, and the feed is 503 only
    when every symbol failed.
  - This also fixes a latent 503: a cached symbol beside one failing symbol
    used to fail the whole feed, because only the uncached fetches were
    counted.
- **Tests.**
  - 8 new, and 6 of them fail on the old code. The 2 that pass there guard
    against regressions: an unregistered symbol is still searched by itself
    only, and one failing term does not fail its symbol.
  - Suite: 1,965 tests, 0 failures, 1 skipped.
  - `frontend/src/api/schema.d.ts` was regenerated; only descriptions
    changed.
- **Live, through the app in process (not yet the running server, which
  needs a restart):**

  | request | result | before the change |
  |---|---|---|
  | ETH, SOL, DOT, UNI | 17, 7, 4, 10 stories | 0 each |
  | KEY, DE, TECH | 10 each | 0, 0, 0 tagged |
  | AAPL | 9 | 9 |
  | market feed (SPY, QQQ, DIA, BTC) | 10, 10, 9, 21; 50 in all | BTC about 9 |
  | GOOGL, FOX | 7, 9 (FOX 6 dropped) | |

  Each request took 0.3–1.2 s uncached. The market feed now reaches its
  default 50-story limit, and BTC-USD holds 21 of the 50.
