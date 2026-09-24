# Data integrity — full assessment, 2026-09-17

Written after the five corporate-action repairs of 2026-09-17
(`corporate-actions-findings-2026-09-15.md`). This is the sweep that answers
"what else is wrong", ranked by how WRONG the data is, not how much of it.

## Registry shape

| class | assets | flagged | never restated | no bars |
|---|---|---|---|---|
| equity | 516 | 10 | 495 | 0 |
| crypto | 99 | 3 | 99 | 1 |
| etf | 11 | 0 | 11 | 0 |

## 1. PARA — ~30 bars of an unrelated company (decided, awaiting a call)

Covered in the corporate-actions findings. `PARA -> PSKY` scores 1.97e-07, but
PSKY is CIK 0002041610 against Paramount Global's 0000813828 — a different
registrant, so linking the series is an editorial decision.

## 2. MNT-USD — the same shape as PARA, unflagged

882-day hole ending 2026-06-09, then the series resumes at **$0.0002 on ~2000
volume**. Mantle traded near $0.60-1.00. A resumption four orders of magnitude
down after a 29-month hole is a different token or a dead husk, not a price
move. Unflagged, so every check treats it as healthy. Not yet tested against a
candidate — the tool can do it, nobody has run it.

## 3. Crypto is in materially worse shape than equities

**35 of 99 crypto assets have impossible single-day moves (>100%). None are
flagged.** These are not volatility; they are two instruments interleaved in
one series:

| symbol | impossible days | worst single-day multiple |
|---|---|---|
| JUP-USD | 23 | 687x |
| TON-USD | 8 | 90x |
| TIA-USD | 3 | **680,637x** |
| USDE-USD | 4 | 2121x |
| TRUMP-USD | 4 | 29x |
| MNT-USD | 4 | 13x |

TIA-USD closes 0.0105 on 2024-03-25 and 7149.41 on 2024-03-26. No backtest
over crypto is trustworthy until this is understood. This was NOT known before
today and is the largest untouched problem in the database.

Separately, 19 crypto names are stale 60-1649 days and unflagged (S-USD,
UNI-USD, STX-USD, PI-USD, APT-USD...). Already triaged 2026-09-14 and handed
to Head of Research — UNI-USD and STX-USD are live tokens, so this is the
`empty-fetch-proves-nothing` case, not delisting.

## 4. Dividend re-adjustment drift — systemic, mild, everywhere

Every dividend-paying equity carries spurious one-day steps, because
`auto_adjust=True` restates as of the FETCH date and incremental ingest never
rewrites an existing bar.

| symbol | regimes | worst spurious step |
|---|---|---|
| MO | 3 | **8.49%** |
| T | 2 | 4.72% |
| XOM | 3 | 3.86% |
| PG | 2 | 3.64% |
| KO | 3 | 3.45% |
| GOOGL | 3 | 0.35% |
| **TSLA (no dividend)** | **1** | **0.00%** — the control |

Mechanism confirmed exactly. KO has two boundaries: **2025-07-16**, the
bulk-load seam (1.0345 -> 1.006), and **2026-09-15**, yesterday's ex-dividend
(1.006 -> 1.0). So there are two components:

- a **one-time** 3-8.5% seam at the 2025-07-15 bulk load, fixable for good;
- a **recurring** ~0.2-0.6% step at every ex-dividend, which comes back.

This has been in the data since the bulk load and nobody noticed — which is
itself evidence about urgency. It is a bad single-day return, not a fake -65%.

### The backfill that fixes it must be SCOPED, not registry-wide

A blanket `--full-backfill` would make things worse in two specific ways:

1. It writes with `replace=True`, so for any symbol whose provider key now
   serves a different instrument it **bakes the wrong company's prices in**.
   PARA is the known case, MNT-USD the live suspect.
2. `mark_full_refresh` sets `delisted_at = NULL`, so it would **un-flag all 13
   flagged assets**, and the next daily run would re-fetch AVB and EA and
   re-ingest the stubs deleted today — undoing three of the five repairs.

So: exclude flagged symbols, exclude anything `max_gap` reports over 30 days,
and batch it.

### The durable fix is architectural

Store raw OHLCV plus adjustment factors and adjust at read time; then no stored
bar is ever wrong and no restatement is needed. That is a schema change to the
single write path with every consumer reading adjusted closes today, so it is a
project, not a patch. Noted, not designed.

## 5. PUMP-USD — one crypto asset with zero bars

Registry hygiene, same class as the `BSC-USD-USD` row deleted in August.

## 6. The only detector for the PARA class runs by hand

`max_gap` is what catches a ticker that was reassigned while nobody was
fetching — the failure that `looks_unresolved` structurally cannot see,
because it needs an EMPTY fetch and these symbols fetch fine. It currently runs
only when someone invokes `scripts/find_successors.py`. It belongs in
`GET /api/v1/ingest/health` beside the drift and delisting checks.

## Coverage of this sweep

Checked: equity discontinuities (>40% since 2025), crypto discontinuities
(>60% since 2024), holes >30d registry-wide, zero-volume bars, zero-bar
assets, staleness by class, and every equity/ETF that split since the bulk
load (18 of 527), each tested for two adjustment regimes.

NOT checked: equity moves in the 15-40% band, where a 3:2 or 5:4 split drift
would hide among real earnings moves; crypto holes under 30 days; fundamental
data; any table other than `market_data` and `assets`.
