# Truncated hourly news pages (decision 75)

Date: 2026-10-25
Status: measured, nothing built. Follows `research/news-never-answering-2026-10-24.md`.

## The gap

`news_fetches` holds 5,680 fetches from 2026-10-22 18:51 UTC to 2026-10-26 00:10 UTC.
Ten were `window_truncated`: the page's oldest story was newer than the previous fetch.
Every story between those two times is lost, and Yahoo will not serve it again.

| symbol | truncated fetch (ET) | previous fetch (ET) | previous span | page span | gap | est. stories lost |
|---|---|---|---|---|---|---|
| TSLA | 10-23 06:08 | 10-22 14:52 | 4.2 h | 14.8 h | 0.5 h | 2 |
| NVDA | 10-23 18:06 | 10-23 06:08 | 12.2 h | 5.4 h | 6.6 h | 60 |
| META | 10-23 19:06 | 10-23 06:08 | 13.7 h | 8.3 h | 4.7 h | 27 |
| TSLA | 10-23 20:06 | 10-23 06:08 | 14.8 h | 12.8 h | 1.1 h | 4 |
| GOOG | 10-24 00:08 | 10-23 06:07 | 19.0 h | 17.9 h | 0.1 h | 0 |
| AMZN | 10-24 03:08 | 10-23 06:07 | 21.1 h | 20.1 h | 0.9 h | 2 |
| MSFT | 10-24 03:08 | 10-23 06:08 | 21.5 h | 20.1 h | 0.9 h | 2 |
| AXP | 10-24 06:08 | 10-23 06:07 | 209 h | 22.0 h | 2.1 h | 4 |
| NVDA | 10-24 19:09 | 10-24 06:09 | 13.6 h | 9.8 h | 3.2 h | 16 |
| NVDA | 10-25 20:10 | 10-25 06:07 | 14.3 h | 9.8 h | 4.2 h | 21 |

"Est. stories lost" is the gap multiplied by the truncated page's own rate (stories / span).
The total is about 138 stories in three days, 97 of them NVDA's and 27 META's.

## Why

`due()` refetches a page when the time since the last fetch, plus the interval,
reaches that fetch's span. It reads the next hours' news rate from the LAST page.

The 06:00 page covers the night, when news is slow, so its span is 12-21 h. The day
is denser: NVDA's 50 stories then cover 3-5 h. So the hourly pass waits about 11 h,
and by then the page no longer reaches back to 06:00.

## Options

- **A. Span = the shorter of the last page's span and the symbol's shortest full
  span in 7 days (recommended, 30-45 min).**
  - NVDA's shortest is 3.2 h, so it is refetched about every 2.2 h. META's is 3.5 h.
  - Every other symbol's shortest is 12 h or more (FDS, TSLA, GOOG, AMZN, MSFT, IBM,
    LRCX, AXP, AAPL, LMT): one or two extra searches a day each.
  - Cost: about 30 extra searches a day. The hourly pass made 21 in all of
    2026-10-25, after decision 69.
  - It still misses a burst denser than anything seen that week, e.g. earnings.
- **B. Refetch every hour any symbol truncated in the last 7 days.** Simpler. NVDA,
  META, TSLA, GOOG, AMZN, MSFT and AXP would then be searched about 170 times a day.
- **C. Leave it.** The loss is counted in `window_truncated`, and nothing reads the
  archive yet.

Not measured: whether any story lost was ever served under another term. These are
ticker pages only.

Queries: session scratchpad `terms/` and `rm/`; all against `news_fetches`.
