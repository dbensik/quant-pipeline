# Headline pages that never answer (decision 69, proposed)

**Status: measured 2026-10-24 about 14:00 New York time; built the same day on
Danny's word ("go on 69"). See Built at the end.**
Numbered 69 because 68 (the quarterly TTM mix) is open in another session;
renumber freely.

This is the open item from step 2 (`research/news-archive-gaps-2026-10-22.md`):
"pages that never answer could stop being retried after N days". The hourly
pass refetches every page whose last answer was empty (`due()` in
`core/news_archive.py`), so a page Yahoo never answers is searched 24 times a
day for nothing.

## What was measured

The sample is 48 hours of `news_fetches` (4,793 rows from 2026-10-22 14:51 to
2026-10-24 14:09 New York time; no failed rows) and the 47 hourly runs in
`logs/news_hourly.log`.

- **The hourly pass is 98% empty searches.** Over 47 runs it made 2,431
  searches. 2,376 came back empty. The 55 that answered stored 1,573 new
  symbol-story rows. The last 33 runs were 50 or 51 searches each, almost all
  empty.
- **51 pages (symbol, term) have never returned a story.** All 51 are crypto.
  Every equity and ETF page has answered at least once. Together they wrote
  2,470 rows, about 1,235 a day, which matches the ~1,200 estimated on
  2026-10-22.
  - 12 coins have both pages dead: BNSOL, EZETH, FDUSD, JITOSOL, JLP, LBTC,
    OSETH, RETH, SOLVBTC, SUSDE, WEETH, WIF. Most are liquid-staking or
    wrapped tokens.
  - 27 are a coin's verified-name page whose ticker page does answer. Some
    ticker pages answer little (CBBTC, INJ, STETH: 1 story at most). Some
    answer a full page (ALGO, AERO, FET, LEO, METH, SPX, VIRTUAL: 50; VET,
    PUMP: 48-49).
  - BDX-USD 'Beldex' was registered today and has 6 fetches. Its other page,
    `BDX`, returned 49 stories, all of them Becton Dickinson's (0 tagged
    BDX-USD).
- **Empty streaks come in two kinds.** Each run of consecutive empty answers
  on a page:

  | streak (fetches) | streaks | ended by an answer | longest (hours) |
  |---|---|---|---|
  | 1 | 18 | 18 | 0 |
  | 2 | 5 | 5 | 1 |
  | 3 | 2 | 2 | 2 |
  | 6 | 1 | 0 (Beldex, new today) | 5 |
  | 14 | 1 | 1 (ICP 'Internet Computer') | 13 |
  | 49-50 | 50 | 0 | 47 |

- **A new page can start empty and then answer.** 22 of the 26 streaks that
  ended in an answer were on a page that had never answered before (SBAC, O,
  COP, NWS, PLD, Cardano, Celestia...). All 22 answered within 14 hours of the
  page's first fetch. So "never answered" cannot be the rule on its own. It
  needs an age.
- **The one long streak that ended cost nothing.** ICP's 'Internet Computer'
  page was empty 14 times (16:15 to 05:05). The hourly pass at 06:05 found 1
  story, and the 06:00 run would have stored the same story at 06:07.

## Recommendation: an old page that never answered is no longer due hourly

Change `due()` so the hourly pass skips a page that has never returned a story
and whose first fetch was at least `NEWS_ARCHIVE_NEVER_ANSWERED_HOURS` (24)
ago. Nothing else changes:

- **The 06:00 run still searches every page**, with its two retries. A dead
  page that comes alive is found the next morning. If it then fills a page
  quickly, the existing span rule makes it due hourly.
- **A page that has answered before keeps today's behaviour.** When it goes
  empty it is refetched every hour, as it is now (all 4 such streaks in the
  sample ended within 2 fetches).
- **Data at risk:** only a dead page that comes alive and then outruns 50 of
  its own stories in less than a day. In the sample none of the 51 pages has
  answered even once.
- **Saving:** about 51 of the ~52 hourly searches, so about 1,200
  `news_fetches` rows a day. NO NEWS lines in the hourly log would show
  real empties again, not the same 51 pages every hour.

To build it, `LastFetch` already knows when a page never answered
(`stories_baseline is None`). `last_fetches` needs one more aggregate, the
first fetch of each page. Add a unit test in `tests/unit/test_news_archive.py`:
a 23-hour-old dead page is due, a 25-hour-old one is not, and a page that
answered once is due whatever its age. Re-measure after a week.

**Estimate:** 30-45 minutes of agent wall-clock, assuming no schema change
(none is needed). Both launchd news jobs run from the working tree, so the
change takes effect on the next hourly run after the edit, before any commit.
The test suite runs first.

## Not recommended: cleaning verified names

Three coins' `verified_name` carries a parenthetical that kills the search.
Tested live 2026-10-24:

| page | as stored | stripped |
|---|---|---|
| GRAM-USD | 'Gram (prev. Toncoin)': 0 | 'Gram': 3 (2 tagged GRAM-USD) |
| POL-USD | 'POL (ex-MATIC)': 0 | 'POL', the same as its ticker page |
| BSC-USD | 'Binance Bridged USDT (BNB Smart Chain)': 0 | 'Binance Bridged USDT': 0 |

Plain names that are already clean also return 0 (Algorand, Aerodrome
Finance, VeChain, Pump.fun, Ethereum Classic, Lido Staked Ether). Stripping
names would gain one coin 2 stories, so it isn't worth a rule. 'Lido' returned
13 stories, but only 3 were tagged STETH-USD (11 LDO-USD): that is a different
coin's page.

## Aside, not part of this decision

Outside the 06:00 run, 6 fetches were TRUNCATED: NVDA, META and TSLA
(2026-10-23, 18:06-20:06) and GOOG, AMZN and MSFT (2026-10-24, 00:08-03:08).
Stories were missed on those pages even though `due()` was refetching them.
Not investigated. Worth a look on its own.

## Built (2026-10-24)

- `NEWS_ARCHIVE_NEVER_ANSWERED_HOURS = 24.0` in `config/settings.py`.
- `LastFetch.first_fetched_at` (the first fetch that did not fail, one more
  aggregate in `last_fetches`) and `LastFetch.never_answered`. `due()` returns
  False for an empty page that never answered and was first fetched 24 h or
  more ago. Every other case is unchanged.
- Tests: 4 unit tests (a 23 h page is due, 24 h and 25 h are not, a page that
  answered once is due at any age, a row without a first fetch is due) and 1
  integration test (`first_fetched_at` is the earliest fetch, read from the
  database). The unit tests fail against the previous `core/news_archive.py`.
  Full suite 2,037 passed; news integration 8 passed.
- Checked against the live database before the next hourly run: of 799 pages,
  the rule makes 1 due where the old one made 51. The 50 dropped are the
  never-answering pages above. The one still due is BDX-USD 'Beldex', first
  fetched at 09:09 today; it drops out at 09:09 tomorrow unless it answers.
