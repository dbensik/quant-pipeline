# HWM — pre-spinoff bars rescaled for the Arconic separation (2026-09-25)

## The problem

On 2020-04-01 Arconic Inc. spun off Arconic Corporation and renamed itself
Howmet Aerospace (HWM). Yahoo now adjusts HWM's earlier bars for the value that
left with the spinoff, but lists no split for it, so the split-drift check
(`detect_drift`, which reads Yahoo's split history) cannot see it. Our copy was
bulk-loaded without that adjustment, so it stored the spinoff as a loss:

| 2020-04-01 | stored | Yahoo |
|---|---|---|
| one-day return | **-17.8%** | +7.2% |

Found by the full fresh-Yahoo return comparison in
`moves-15-40-sweep-2026-09-25.md` — the only one of 907,129 stored daily
returns, outside the 2025-07-16 dividend seam and PARA, off by 3pp or more.

## The factor

HWM's stored series starts 2020-01-02, so only **62 bars** predate the spinoff.
Yahoo/stored close ratio over those 62: 0.7647-0.7648 — constant, one factor
explains all of it. Just after the spinoff the ratio is 0.9973 (ordinary
dividend drift, left alone). The spinoff factor is the step between them:

    0.76477 / 0.99726 = 0.76687

Applied to open/high/low/close; volume divided by it, as Yahoo does (so dollar
volume is unchanged).

## What was done

In one transaction, guarded on exactly 62 updated rows. The original 62 bars
are in `hwm-prespinoff-bars-before-rescale-2026-09-25.csv`.

## Verified

- 2020-04-01 now +7.18%, Yahoo +7.18%. Pre-spinoff volumes match Yahoo's to
  within 13 shares (8,343,005 vs 8,342,992 on 2020-03-27).
- All 1,690 HWM daily returns within 0.27pp of Yahoo; the largest is the
  2025-07-16 dividend seam, a separate problem.
- `check_reassigned.py --symbols HWM`: nothing flagged.

## The class this belongs to

A spinoff Yahoo adjusts for without listing as a split. The split-drift check
is blind to it; only a fresh-fetch return comparison sees it. That comparison
found no other case across the registry, but it runs only by hand.
