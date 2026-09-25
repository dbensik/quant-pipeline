# PARA — a reassigned ticker, and a self-inflicted wound (2026-09-24)

## The problem

Paramount Global filed a **Form 25-NSE on 2025-08-07** (CIK 0000813828).
During the 388-day ingest outage the `PARA` key was reassigned, and from
2026-08-07 it served an unrelated penny stock at ~$1.76 on 100-400k shares.
Thirty-two of those bars had accumulated, and the daily job was adding more:
every guard built for crypto keys off `coingecko_id`, so none of the 516
equities were covered.

## What I got wrong, and the damage

I trimmed the 32 contaminated bars, set a ceiling, then ran
`--full-backfill PARA` **to test the ceiling**. It worked — no bars after
2025-08-07 — and it also **destroyed 1390 genuine Paramount bars**, because
`--full-backfill` writes with `replace=True` and **Yahoo has re-keyed its
ENTIRE PARA history to the new instrument**, not merely the recent part:

| year | after the backfill | avg volume |
|---|---|---|
| 2021 | up to $84,760 | 1,943,692 |
| 2022 | $79,382 - $82,717 | **6** |
| 2023 | $15,366 - $93,097 | **4** |

The ceiling is only an upper bound. It cannot help when the contamination
reaches all the way back, and I had verified the tail without checking whether
the rest of the provider's series was still the same company.

**The stored data was better than the provider's, and I overwrote it to test a
guard.** The lesson is narrow and worth keeping: a backfill is not a read.
Testing a write path against live data destroys the thing being protected.

## Recovery

The legacy SQLite snapshot still held it — `quant_pipeline.db`, `price_data`,
1256 bars from 2020-07-13 to 2025-07-11, and unmistakably the real company:

| year | range | avg volume |
|---|---|---|
| 2021 | $26.25 - **$90.04** | 20,752,004 |
| 2023 | $10.38 - $24.04 | 13,327,628 |
| 2024 | $9.44 - $14.30 | 14,368,894 |

Restored in full. 1256 bars, avg volume 13.9M.

The 134 bars the TimescaleDB copy had before 2020-07-13 are not recoverable —
they came from a later ingest, not the migration, and nothing holds them now.

## The protection

A ceiling alone was not enough, because every fetch of this key returns another
company at ANY date. So `PARA` is marked `identity_status = wrong_asset`, which
the gate refuses **including on a full backfill** — the identity gate, unlike
the delisted gate, is deliberately not bypassed by one.

**This row KEEPS its bars**, unlike every crypto `wrong_asset`. The flag
describes the PROVIDER, not the data: the stored series is the last genuine
Paramount Global history we have.

Also fixed: the block message said "the wrong coin's history" for a stock.

## Still open — PSKY

`PARA -> PSKY` scores **1.97e-07** on the constant-ratio test, an economically
continuous series. But PSKY is **CIK 0002041610**, a different registrant — a
new holding company, where EQR->VMRK kept CIK 0000906107 and file number
001-12252 throughout. Linking them is an editorial decision about continuity
and has not been made.

## The general gap this exposes

516 equities and 11 ETFs carry no identity metadata at all, so the gate has no
opinion on any of them. PARA is the one known instance; the class is unguarded.
Equities have no CoinGecko equivalent, but they do have SEC filings — a Form
25-NSE is exactly the signal that a ticker is free to be reassigned.
