# AMCR — placeholder bars before the NYSE listing, trimmed (2026-09-24)

## What was there

Amcor plc began trading on the NYSE on **2019-06-11**, the day the Bemis
acquisition closed. The stored series started on 2015-01-02: **1116 bars before
the listing**, 897 of them on zero volume, median volume 0, closes stepping
between flat runs at $27.53-$43.89. The first real session trades 7,180,480
shares; the day before trades none.

Found while calibrating `scripts/check_reassigned.py`: in 2019 the placeholder
run's median dollar volume swung 81x on single prints of a few thousand shares,
the largest collapse in any equity's history. The detector's $1M/day floor on
the prior market exists because of it.

A read-only sweep of all 516 equities and 11 ETFs found **no other asset** with
a placeholder run ahead of its first real market.

## What was done

In one transaction, checked for exactly 1116 deleted rows:

- `assets.metadata.history_valid_from = "2019-06-11"` on AMCR (id 30), so
  ingestion refuses bars before the listing — a `--full-backfill` would
  otherwise re-import them, since Yahoo still serves them.
- Deleted the 1116 bars before 2019-06-11.

After: 1831 bars from 2019-06-11 to 2026-09-23, zero zero-volume days;
`check_reassigned.py --symbols AMCR` flags nothing.

## Recovery

Every removed bar is in `amcr-prelisting-bars-removed-2026-09-24.csv`, in
`market_data` column order.

## What this affects

Any backtest or screen that covered AMCR before mid-2019 ran on a flat,
untradeable price. Saved results in `results/` computed over that window are
not reproducible.
