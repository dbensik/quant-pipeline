# Decision 77: tag headlines by company name? (2026-10-26)

The survey of quant firms' public repos (`research/quant-firm-oss-survey-2026-10-23.md`,
pickup 2) proposed running a multi-pattern matcher (Aho-Corasick, `ahocorasick-rs`)
over every archived title, with each registry company's name as a pattern. The claim:
it would show (a) stories about a company that Yahoo did not tag and (b) co-mentions,
"a pair-event no price series shows". It also said nothing should be built before this
was measured. This note does the measuring.

## Verdict

- **Co-mentions need no tagger.** Yahoo's own tags already name 2 or more registry
  symbols on 9,132 of 30,386 stories.
- **A name tagger over the whole archive adds mostly noise.** It adds 6,191 links
  Yahoo did not make. By hand labels, about 5% are about the company they name.
- **One narrow slice works:** a story the archive found by searching symbol X, which
  Yahoo left with no tags at all, whose title names X by a distinctive name. That is
  86 links, 70 of them (81%) about the company. Examples: PDD's Q2 earnings, Trane's
  acquisition, Ulta, Uber, Skydance, Ondo, World Liberty. The Research page drops
  these today, because its rule keeps only stories Yahoo tagged.
- **No new dependency.** 758 patterns over 30,386 titles run in 17 s with Python's
  `re`, including loading the exports. Aho-Corasick solves a scale this archive does
  not have, and the Research page tags at most a few hundred titles a request.

**Recommended: A.** Keep that slice on the Research page as a read-time rule. About
1.5–2 h of agent time. Close pickup 2 otherwise. **Danny picked A on 2026-10-26; built
as described under "Built" below.**

## Data

- **Stories:** the archive as of 2026-10-26 14:35 UTC. 30,386 stories, 39,893
  story-symbol links to 672 symbols. First fetched 2026-10-22 to 10-26; published
  2026-07-24 to 10-26.
- **Yahoo's tags:**
  - 25,981 stories (85.5%) carry at least one registry symbol.
  - 1,703 carry no tag at all.
  - Registry tags per story: 1 on 16,849 stories; 2 or more on 9,132.
- **Names:**
  - Equities: companyfacts `entityName` as logged by the SEC step (507 filers),
    else the EDGAR ticker-file title, plus `NEWS_SEARCH_TERMS`.
  - Coins classed `crypto`: their `verified_name`.
  - ETFs: none.
  - Each name was reduced to a full form (legal-form words, state tags and a
    leading "The" removed) and a short form (trailing Group, Holdings, Companies,
    etc. removed): 758 patterns.
  - Matching is case-insensitive and whole-word. Names of 3 characters or fewer, and
    all-caps names of 4 letters or fewer, match case-sensitively.
- **Agreement:** a hit agrees with Yahoo when Yahoo tagged the symbol, another share
  class under the same CIK, or the coin's `CRYPTO_PROVIDER_SYMBOLS` ticker. A first
  count without the provider tickers called Yahoo-tagged coin stories (UNI7083-USD,
  HYPE32196-USD...) "new". The corrected count is used throughout.

## 1. The whole archive

25,846 story-symbol hits on 18,033 stories. 19,655 agree with Yahoo and 6,191 are
new. Yahoo tagged 41,322 equity links, and 39.9% of them name the company in the
title: Yahoo tags from the body, so the title can never stand in for its tags.

| name tier | hits | agree with Yahoo | new |
|---|---|---|---|
| distinctive (several words, or a word not in `/usr/share/dict/words`) | 17,881 | 91% | 1,557 |
| a dictionary word (Target, Block, Dash, Sky, Southern...) | 5,670 | 33% | 3,791 |
| a short acronym (US, Dow, Fox, KKR...) | 2,295 | 63% | 843 |

**Labels.** Random samples (seed 7) of the new hits, labelled by hand from title and
tags:
- **about:** the story is about the named company.
- **mention:** it names the right company but is about something else, typically a
  broker as the source of a note on another company.
- **wrong:** a different entity.

| sample | n | about | mention | wrong |
|---|---|---|---|---|
| distinctive, new | 91 | 13 (14%) | 53 (58%) | 25 (27%) |
| word + acronym, new | 49 | 1 (2%) | 1 (2%) | 47 (96%) |

- **"Mention" is mostly brokers.** "Goldman Sachs Adjusts Price Target on Lockheed
  Martin" names GS correctly and is about LMT, which Yahoo already tagged. 651 of the
  1,557 distinctive new hits have that form.
- **"Wrong" is shared names.** Nasdaq-100 or "(NASDAQ:TTD)" is not Nasdaq Inc. Con
  Edison and Sherwin-Williams contain EDISON and WILLIAMS. FedEx Freight and Honeywell
  Aerospace are now separate companies. "stacks up" is not Stacks.
- **The word tier is hopeless.** "Target" alone is 2,804 of its new hits, all "price
  target".

**Estimate:** about 1,557 × 14% + 4,634 × 2% ≈ 300 links about their company, among
6,191 added. Roughly 1 in 20.

## 2. The slice that works: untagged stories from the symbol's own search

The archive stores what each symbol's search returned, unfiltered, so a story with no
Yahoo tag still has a link to the symbol that found it. 146 distinctive-name hits fall
on stories with no tags. I labelled all 146, not a sample:

| | pairs | about | mention | wrong |
|---|---|---|---|---|
| all untagged hits | 146 | 85 | 42 | 19 |
| found by that symbol's own search (first-pass labels) | 99 | 72 | 8 | 19 |
| same, without a plural of a dictionary word ("Stacks"; first-pass labels) | 86 | 72 (84%) | 8 | 6 |
| same 86, relabelled for the fixtures (see Built) | 86 | **70 (81%)** | 10 | 6 |
| NOT found by that symbol's search | 47 | 13 | 34 | 1 |

- **Restricting to the symbol's own search is what removes the mentions.** Of the 47
  hits outside it, 34 are brokers, market reports and product listings ("...on NVIDIA
  DGX Spark").
- **The remaining wrong 6 are coins whose names another entity uses:** Cronos (the
  cannabis company Cronos Group, CRON; 3), Polkadot FX (a beverage), "Pepe" Gorbea,
  and ONDO the apparel company. (The first pass listed Glenn Williams here; he is
  outside the own-search slice.)
- **The plural rule was chosen after seeing Stacks.** 13 of the 19 errors were
  "stacks up", and `web2` lists "stack", not "stacks". Treat 84% as optimistic until
  the next labelled sample. The same rule would also class CUMMINS, STERIS, SYNOPSYS
  and Starbucks as words. None of them fall in this slice; in a rule they would lose
  recall, never precision.
- **Recall is roughly 70%, a low-precision estimate.** 1,396 untagged own-search links
  are NOT hit by any name. In a sample of 50 (seed 7), 1 is about its company (FIS
  Global). The other 49 are word-ticker searches returning Yahoo's generic feed: RMD,
  GEN Z, COST of living, ALL-New, Pope LEO. So about 28 relevant links are missed
  against 70 found.

**What it is worth.**
- **Small in total:** 70 links about their company against 39,893, +0.2%.
- **It lands where Yahoo is silent** (links rescued, all labels): ULTA 6 (2 about), UBER 5 (3), WLFI 6, ONDO 6 (5), SKYD 5, DOGE 6 (5),
  PDD 3. Those include earnings and acquisitions the Research page hides today.

## Options

- **A. Read-time rescue on the Research page (recommended).**
  - Keep an untagged story when the symbol's own search returned it and its title
    names the symbol's company by a distinctive name.
  - Distinctive means several words, or one word that is neither a dictionary word
    nor its plural. Legal forms are dropped from the name as in section 1.
  - Count rescued stories apart (`name_rescued`) next to `unrelated_dropped`, never
    silently.
  - Nothing is stored, as with price adjustment.
  - Tests: the labelled pairs as fixtures (70/10/6 after the relabel, see Built), including Cronos and Stacks as
    known misses and known exclusions.
  - About 1.5–2 h.
- **B. Build nothing; close pickup 2 on this note.** The archive has no reader yet
  besides the Research page, and the gain is 0.2% of links.
- **Not offered: the full-archive tagger, with or without Aho-Corasick.** About 1 new
  link in 20 is about its company.

## Built (A, 2026-10-26)

- **Where:** `fetch_related_news` in `api/upstream.py`, inside the per-symbol relevance
  rule. A story under symbol X whose tags miss X, its share classes and its provider
  ticker is still dropped, unless Yahoo tagged it with NOTHING and its title names X's
  company. Kept stories keep their empty tags. `NewsFeed.name_rescued` counts them per
  ticker beside `unrelated_dropped`; the Research page prints both.
- **Names, as the code sees them** (`core.news_archive.company_names`): an equity's
  `cik_sec_title` (the EDGAR ticker-file title) and its `NEWS_SEARCH_TERMS` entry; a
  coin's `verified_name` when `crypto_class` is `crypto`. Not companyfacts'
  `entityName`, which the measurement used and the database does not store.
  Rerun over the archive with these names, the rule rescues exactly the same 86
  links and nothing else.
- **Distinctive** (`is_distinctive`): not 3 characters or fewer, not all-caps of 4
  letters or fewer, and either several words or a word outside
  `settings.NEWS_NAME_WORDS`. That set (89 words) is every single-word name form in
  the registry today that is in `/usr/share/dict/words`, or is one less a trailing
  "s". It is stored, not read from the dictionary at request time, so the answer
  does not depend on the host. A joiner with a one-word name that is a word (a new
  "Target") is matched until added to it: the same exposure `NEWS_SEARCH_TERMS` has.
- **Fixtures:** `tests/unit/test_news_names.py` holds all 97 distinct (symbol, title)
  rows of the own-search slice, relabelled by hand: 69 about, 10 mention, 6 wrong, 12
  Stacks (86 links / 70 about counted per `story_id`, as above). Cronos and the ONDO
  apparel story are asserted RESCUED (known false positives); every Stacks row is
  asserted not. The relabel moved 2 Ulta retail-launch stories from about to
  mention; the first pass's labels were not saved.
- **Live check, 2026-10-26** (10 symbols through the router against the database
  and Yahoo): `name_rescued` = WLFI-USD 6, SKYD 1, ONDO-USD 1 (the apparel story).
  20 of CRO-USD's stories were dropped and none qualified for rescue.
- **Not changed:** the archive (nothing is stored), the hourly and 06:00 jobs,
  `own_tagged`.

## What this does not settle

- **Sentiment scoring stays a separate decision** (FinBERT in the news-archive memory;
  the survey's z-score).
- **Any test of whether headlines move prices needs months of archive.** The archive
  has four days of fetches. Tagging accuracy is the only thing measurable today.
- **Side finding, not investigated:** some stories are stored under several
  `story_id`s with one title. "Entuity Software ... NVIDIA" appears 3 times, "Pilot-Era
  Agentic AI Stacks" twice. Counts above are per `story_id`.

## Reproduce

Scratch scripts, not committed, in this session's scratchpad `n77/`:
- `base.py`, `tagger.py`: the dictionary and census.
- `tiers.py`: the name tiers.
- `derive_words.py`: `NEWS_NAME_WORDS` from the registry's names.
- `census_prod.py`: the rule as built, rerun over the archive (86 links).
- Inline samplers with seed 7.

Inputs are three `\copy` exports (`news_stories`, `news_story_symbols`, `assets`), the
SEC step's logged entity names, and `NEWS_SEARCH_TERMS`. No external requests. Labels
are by hand, from title and tags.
