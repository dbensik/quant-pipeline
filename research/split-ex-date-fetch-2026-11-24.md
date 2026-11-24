# A split Yahoo had already applied on its ex-date morning — 2026-11-24

The fresh-return check has flagged `MISMATCH SNDK 2026-11-19: -85.126pp`
every morning since 2026-11-20. Readers see SNDK's 2026-11-19 close as
**$11.80 on 959M shares**; Yahoo's is $94.39 on 120M. Every backtest, screen,
chart and DCF price that reads SNDK is affected. Nothing else is flagged.

## What happened

| | stored served close | fetched (New York) |
|---|---|---|
| 2026-11-18 | 776.22 | 2026-11-19 06:02 |
| 2026-11-19 | **94.39** | 2026-11-20 06:03 (the ex-date) |
| split 8:1, ex 2026-11-20 | | first served 2026-11-21 |

- The 06:00 fetch on the ex-date served 2026-11-19 **already divided by 8**
  (raw $755.16). A fresh fetch today agrees: 2026-11-19 is $94.39 in
  Yahoo's split basis, with the split dated 2026-11-20.
- `core/price_adjustment.APPLIED_ON_EX_DATE = False` says a fetch made ON
  an ex-date does not yet reflect it. So the read takes $94.39 as raw, then
  divides it by 8 for the later split: $11.80.
- Nothing stored is wrong. The served value and its fetch time are what
  Yahoo gave; the reading of them is.

## Why the rule cannot just be flipped

It was set from the other answer. The only other split to reach the rule
since the cutover (2026-09-28) is CTVA's 6.665 (Corteva's separation, ex
2026-10-01): its ex-date-morning fetch served the PRE-split price, and True
had multiplied it to $517.54 (decision of 2026-10-12, memory
`ctva-separation-bar`).

| split | ex-date | ex-date-morning fetch | stored vs prior bar | applied? |
|---|---|---|---|---|
| CTVA 6.665 (spin served as split) | 2026-10-01 | 06:00 | 77.65 vs 77.87 | no |
| SNDK 8 | 2026-11-20 | 06:03 | 94.39 vs 776.22 | yes |

Two cases, one each way, both fetched at 06:00. Nothing in the metadata
tells them apart: the split row arrived AFTER the bar in both (CTVA a week
later, SNDK a day later), so "applied once Yahoo listed it" gets SNDK wrong
too. Only the price itself shows which basis a bar is in.

The rule reaches one bar per split: the bar fetched on the morning of its
ex-date. These two are all of them.

## Also open: two days Yahoo has no bar for

Yahoo serves 2026-11-20 as an empty row carrying the split, and no row at
all for 2026-11-23; ingest drops the empty row each morning. Whether SNDK
traded on them is not shown. Nothing to do yet: if Yahoo still has nothing
once they are older than the 14-day overlap, `check_missing_days.py` names
them and they go to `missing_days_accepted` or are filled.

## Decision 80

- **A (recommended). A reviewed list of events a fetch on the ex-date
  already reflected,** in `config/settings.py`, starting with
  `("SNDK", 2026-11-20)`. `to_raw` counts a listed event as applied by a
  fetch on its ex-date; the rule stays False for everything else. Read-time
  only: no stored row changes, and every SNDK read is right as soon as the
  API restarts. The fresh-return check is what finds the next one (it found
  this the first morning). Test: SNDK's 2026-11-19 read equals Yahoo's, and
  CTVA's stays as is. About 1-1.5 h of my time.
- **B. Infer it from the price.** For a bar fetched on a split's ex-date,
  take whichever basis puts its move from the prior bar nearer zero, and
  flag when the ratio is too close to 1 to tell (spins are often served as
  1.03-1.10 splits). It would have read both cases right, but it is a
  heuristic built on two examples. About 2-3 h.
- **C. Change the bar's `fetched_at` to 2026-11-21.** That yields the right
  number, but it is rejected here because it falsifies a stored fact. It is
  listed so it is not suggested later.

## Built (A, approved 2026-11-24)

- `config.settings.SPLITS_APPLIED_ON_EX_DATE`, with SNDK 2026-11-20 and what
  its bar showed. The repository sets `Action.applied_on_ex_date` from it;
  `core/price_adjustment` counts a listed split or manual factor as applied
  by a fetch made on its own ex-date, in prices, volumes and dividend
  amounts alike. Every other event reads as before.
- **Live, read through the repository:** SNDK 2026-11-19 reads $94.39 on
  119.9M shares (raw $755.16 on 15.0M), Yahoo's own figures; CTVA's
  2026-09-30 still reads raw $77.65. `check_fresh_returns.py --symbols SNDK
  CTVA`: 0 days off.
- **Tests:** 5 new in `tests/unit/test_price_adjustment.py` (the unlisted
  reading that readers got, the listed one, no other fetch day moves, CTVA
  unchanged, the repository marks only the listed event); 4 mutants, each
  caught. Suite: 2192 passed.
