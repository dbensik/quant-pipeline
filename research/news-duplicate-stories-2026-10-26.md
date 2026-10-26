# Decision 78: one headline under several story ids (2026-10-26)

The decision 77 note (`research/headline-tagging-2026-10-26.md`) found titles stored
under more than one `story_id` and left them uninvestigated. This note measures them.
Danny picked B; it is built as described under "Built" below.

## Verdict

- **Not our double insert.** `story_id` is Yahoo's uuid (36 characters on every row)
  and the key we insert on. Every duplicated title has a distinct URL per row.
  No two rows share an id or a URL.
- **Half of them are not duplicates at all.** 201 of the 428 titles are recurring
  headlines published 24 h or more apart. Examples: "Sector Update: Healthcare"
  (30 rows), "FRN Variable Rate Fix" (66), "Form 8.3" (22), "Top Midday Gainers".
  Each row is a different story under the same title.
- **The real copies are small.** 191 titles were published 2 h or less apart. They
  add 246 rows of 30,585 (0.8%) and 282 symbol links of 40,244 (0.7%), spread over
  141 symbols.
- **On the Research page, a copy shows twice.** The page de-duplicates on URL, and
  every copy has its own URL. 138 of the multi-publisher pairs and 109 of the
  same-publisher pairs came from a single fetch of a single symbol.

**Recommended: build nothing now.** The archive should keep storing what Yahoo
served: a copy is a fact about the feed, and a published-time rule decides which
rows are copies. Any count that needs one row per story should collapse copies
when it reads, the way prices are adjusted at read time. Today the only reader of
this kind is the Research page, where the cost is a repeated headline in about
0.7% of links. Revisit this when a sentiment or event count reads the archive,
because that count would weigh a wire release 2–3 times.

## Data

The archive as of 2026-10-26, about 19:00 UTC: 30,585 stories with 29,791 distinct
titles, and 40,244 story-symbol links. All counts are on exact title equality.

| class (published-time spread of the copies) | titles | extra rows | same symbol linked twice or more | extra links |
|---|---|---|---|---|
| A. recurring title, 24 h or more apart | 201 | 510 | 150 | 420 |
| B. several publishers, under 24 h | 123 | 177 | 139 | 201 |
| C. one publisher, under 24 h | 104 | 107 | 112 | 115 |

- **B is wire distribution.** One release goes out on several wires at the same
  instant (median spread 0 min). Examples:
  - Rockwell Automation's report, on CNW Group and twice on PR Newswire: 3 ids, 3 URLs.
  - Abbott's nutrition spin-off, on Just Drinks and Just Food.
  - An ACCA story, on International Accounting Bulletin and The Accountant.
- **C is a publisher reposting** within minutes (median 11 min). Example: PR
  Newswire's "NBA Cares x Sands Cares" releases each went out twice, about 20 min
  apart.
- **The 24 h line is loose.** 36 titles have copies 2–24 h apart. They are a mix of
  republished stories ("Evercore ISI Adjusts Price Target on Honeywell...", ~21 h
  apart) and daily formats ("What's Moving Markets Today?"). The 2 h figures above
  leave those 36 out, so they are the conservative count.
- **The copies' tags mostly agree.** Tags differ between copies on 14 of 123 B
  titles and 27 of 104 C titles. They differ on 146 of 201 A titles, as you would
  expect when the stories are different.
- **Most-affected symbols** (extra links from copies 2 h or less apart): ROK 14,
  CC-USD 12, ICE 11, CCL 7, LINK-USD 7, AON 6, QNT-USD 6, GDDY 5.

## What it means for earlier counts

- Counts per `story_id`, in the decision 77 note, `name_rescued`, `unrelated_dropped`
  and `own_tagged`, count a copy as a second story. At about 0.7% of links this
  changes no conclusion in that note.
- `window_truncated` and the page spans are unaffected. They read
  `published_at`, which copies share.

## Options

- **A. Build nothing (recommended).** Record the finding. Collapse copies at read
  time when a counting reader is built.
- **B. Collapse copies on the Research page now.** Key on (title, published within
  2 h) beside the URL, and count the collapsed ones apart, never silently. About
  45 min including a test. The gain is one fewer repeated headline in roughly 1 of
  140 links.
- **Not offered: de-duplicating in the archive.** That would rewrite or skip what
  Yahoo served. It goes against the archive's insert-only, as-served rule, and a
  wrong rule could never be undone.

## Built (B, 2026-10-26)

Danny picked B as well, on 2026-10-26.

- **Where:** `collapse_copies` in `api/upstream.py`, run on the merged feed after the
  relevance rule. A row is a copy when its title (exact, trimmed) equals a row
  already shown and it was published within `NEWS_COPY_WINDOW_HOURS` (2.0, in
  `config/settings.py`) of it. The earliest is shown, with its own publisher and
  tags. A row with no publish time is never collapsed.
- **Counted, never silent:** `NewsFeed.copies_collapsed`, per ticker the dropped copy
  was found under. The Research page prints "Copies of a headline shown once".
- **Tests:** a Rockwell fixture in `tests/api/conftest.py` with two wires at 16:00, a
  repost at 16:20 and the same title at 19:00: 2 rows, 2 counted. The test fails
  with the window disabled. The frontend test checks that the line appears.
- **Live check, 2026-10-26** (10 symbols through the gateway against Yahoo, no
  restart): 82 rows became 75. All 7 removed rows were the same release on CNW Group
  and PR Newswire at the same minute: ROK 2, ICE 2, GDDY 2, CCL 1.
- **Not changed:** the archive. Every copy is still stored.

## Reproduce

The scratch SQL is in this session's scratchpad (`dup.sql` to `dup4.sql`). It is
read-only against `news_stories` and `news_story_symbols`. No external requests.
