# News from Yahoo's search (decision 57, 2026-10-22)

Danny asked on 2026-10-21 about the News section, a socials feed and
sentiment. The News section had been blank for every symbol.

## What was wrong

`GET /api/v1/research/news` answered 200 with `items: []` for every symbol.
yfinance 1.2.0's `Ticker.news` POSTs to `finance.yahoo.com/xhr/ncp`, which
now answers `404 {"message":"Not Found"}`. yfinance parses that body, finds
no stream in it and returns `[]` without raising. The gateway cached the
empty list for 600 s as "no news". That is the silent empty the gateway's
docstring forbids, and its all-failed check never fired because nothing
failed.

Upgrading would not help: yfinance 1.7.0's `get_news` still POSTs to
`/xhr/ncp` (checked in its `base.py`, 2026-10-22).

## The source that works

`yf.Search(symbol, news_count=10, max_results=1)` calls Yahoo's
`/v1/finance/search`. Its stories come in the flat layout that
`normalise_news_item` already reads (uuid, title, publisher, link,
providerPublishTime), plus `relatedTickers`. A live answer always has a
`news` key, even when it is empty. The dead endpoint's body has none.

Measured 2026-10-22, 10 stories asked per symbol. "Tagged" counts the stories
whose `relatedTickers` include the symbol:

| symbol | stories | tagged | | symbol | stories | tagged |
|---|---|---|---|---|---|---|
| SPY | 10 | 10 | | GOOGL | 10 | 0 (tagged GOOG) |
| QQQ | 0, then 10 | 10 | | BTC-USD | 10 | 9 |
| DIA | 10 | 10 | | ETH-USD | 0 | 0 |
| AAPL | 10 | 10 | | SOL-USD | 0 | 0 |
| MSFT | 10 | 10 | | TQQQ | 10 | 10 |
| NVDA | 10 | 10 | | GLD | 10 | 10 |
| JPM | 10 | 10 | | AGG | 10 | 10 |
| XOM | 9 | 9 | | PARA | 10 | 9 |
| KO | 10 | 10 | | SKYD | 10 | 8 |
| MO | 10 | 10 | | F, T, A, O | 10 each | 10 each |
| BRK-B | 9 | 8 | | ZZZZQ (no such ticker) | 0 | 0 |

- **QQQ came back empty, then full a minute later.** The response had a
  `news` key both times, so the shape alone cannot catch this.
- **Crypto pairs:** every `-USD` pair except BTC-USD returned nothing. The
  base symbol did return stories, but with uneven tags: ETH 10 of 10 tagged
  ETH-USD, XRP 10, AVAX 7, DOGE 4, ADA 4, LINK 2, and SOL, LTC and DOT 0 of
  10.

## Built (option A)

- `api/upstream.py`: news comes from a `_search` seam (`yf.Search`).
  `_news_for` treats a response without a `news` field as a failure. As
  before, the feed is 503 only when every symbol failed, and one broken
  symbol does not blank it.
- An empty answer is returned but **not cached**, so QQQ's transient empty
  is asked again on the next request rather than served for ten minutes.
- Each story carries `related_tickers` in the response, never filtered on.
  The page does not show it yet. This field goes beyond A as approved, so it
  can be dropped.
- Profiles and financials still use `yf.Ticker`, unchanged.

Live through the gateway after the change: the market feed (SPY, QQQ, DIA,
BTC-USD) gave 37 stories in 1.2 s, the newest 13 minutes old. The Mag 7 gave
46 stories after de-duplication.

## Not built (picks for Danny)

1. **Relevance.** Built the same day; see "Relevance filter" below.
2. **Crypto.** For a `-USD` pair that returns nothing, query the base symbol
   and keep only the stories tagged with the pair. That is about 10 lines and
   would give ETH and XRP full feeds, but SOL, LTC and DOT would still get
   none.
3. **Decision 57 B and C** as proposed on 2026-10-21. B is a daily
   insert-only headline archive with FinBERT scores. C is social feeds.

## Relevance filter (built 2026-10-22, pick 1 above)

Search matches text, so a story need not be about the symbol it was found
under. Rule: a story found under a ticker is kept only when Yahoo's
`relatedTickers` name that ticker or another share class of it. Share
classes come from the registry: equities carrying the same OWN CIK
(predecessor CIKs name an earlier filer, not a second class). Today that is
exactly GOOG/GOOGL, FOX/FOXA and NWS/NWSA; BRK-A is not registered.

The rule runs per ticker BEFORE de-duplication. A story found under two
tickers survives if either tags it; filtering the merged list would judge it
only as the first ticker's. The cache stays unfiltered. Every drop is counted
in `unrelated_dropped` and shown on the Research page.

### Measured, 34 tickers, 10 stories asked each

| ticker | stories | dropped | what was dropped |
|---|---|---|---|
| FOX | 9 | 6 | Fox Factory (4), Fox Tungsten, Terry Fox Foundation: all wrong |
| PARA | 10 | 1 | a para-rowing partnership: wrong |
| CAT | 10 | 1 | "The Cat in the Hat" on Roblox: wrong |
| BRK-B | 9 | 1 | "...Owning BRK-B Directly", no tags: **about the name** |
| SKYD | 10 | 2 | "SKYD Surges...", "...SKYD, IBM In Focus": **about the name** |
| BTC-USD | 10 | 1 | "BTC USD Touched $83,000...", tagged SOL-USD: **about the name** |
| GOOGL, GOOG, FOXA, NWS, NWSA | 10/10/9/10/10 | 0 | kept through the share class |

The other 22 (SPY, QQQ, AAPL, MSFT, NVDA, AMZN, META, TSLA, JPM, XOM, KO, MO,
F, T, A, O, TQQQ, GLD, AGG, UNH, V, LLY) lost nothing. Without the alias,
GOOGL would have kept 1 of 10, FOXA 6 of 9 and NWSA 2 of 10, though every
one of those stories was tagged with the sibling class.

In total: 8 wrong stories removed and 4 relevant ones lost, of about 330.
The live run through the registry and Yahoo after the build matched the
table exactly. The market feed (SPY, QQQ, DIA, BTC-USD) then gave 37
stories after de-duplication, with BTC-USD's 1 counted as left out.

### Not built: a title match (a pick for Danny)

All 4 lost stories name the ticker in the title. Keeping an untagged story
whose title contains the ticker as a whole word would recover them, at a
cost:

- Case-sensitive, it re-admits the all-caps "...PARA ROWING PARTNERSHIP..."
  headline. Case-insensitive, it would also re-admit "The Cat in the Hat".
- "BTC USD" is not "BTC-USD", so crypto needs the base symbol, which is pick
  2's territory (the crypto fallback).
- One-letter and word tickers (A, O, T, F, CAT, FOX) are where a text match
  is weakest, and they are exactly the names the tag rule protects.

The relevance cut is a model choice, so it is left at the tag rule.
