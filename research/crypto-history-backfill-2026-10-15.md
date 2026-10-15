# Crypto history before 2020 (decision 33, 2026-10-15)

Decision 26 extended equities and ETFs to 2010 and left crypto alone. 32
coins began on exactly 2020-01-01 — the default start, not their launch —
so no crypto backtest saw 2017-2019. Decision 33 (approved: "go on 33")
extended those 32, insert-only.

## Scope

Only coins whose first stored bar was 2020-01-01 and that are not delisted.
A coin starting later starts there for a reason (a later launch or a
recorded `history_valid_from`), and older bars under its ticker are more
likely another token than missing history. All 32 have identity `match`;
ingestion ran with the identity gates on, through `core.ingest.ingest_symbols`
with an explicit END of 2020-01-01, so nothing from 2020 on was fetched.

## Result

- **20,476 bars**, 0 failures, 0 implausible jumps, 0 identity skips. BTC and
  LTC from 2014-09-17; most others from 2017-11-09, where Yahoo's coverage
  begins; later coins from their launch (ATOM 2019-03, ALGO 2019-06, HBAR
  2019-09, multi-collateral DAI 2019-11-22, 40 bars).
- **Nothing a reader saw changed.** All 96 reads (32 coins x 3 modes) from
  2020-01-01 are bit-identical to a fingerprint taken before the first write.
- **Two days Yahoo does not serve:** NEXO 2018-05-06, WETH 2018-08-22. The
  daily missing-day check covers equities and ETFs only, so no job fails.

## Bounds (`scripts/audit_crypto_bounds.py`, report only, nothing written)

Before: 25 clean, 7 scattered (1-2 bars each, all after 2020). After: 17
clean, 15 scattered. The backfill added **17 out-of-range bars in 10 coins**,
of about 20,000:

| coin | bars | dates | reading |
|---|---|---|---|
| XDC | 4 | 2019-05-11..14 | below the published low |
| QNT | 3 | 2018-08-23..25 | its first days of trading |
| WETH | 2 | 2019-02-05, 06 | **69.32 when ETH traded near 105: a bad print** |
| XLM | 2 | 2018-01-03, 04 | the January 2018 peak |
| CRO | 1 | 2018-12-17 | its first day |
| USDC | 1 | 2018-10-15 | 1.0268, the Tether scare that day |
| BCH | 1 | 2017-12-20 | its all-time-high day |
| LINK | 1 | 2017-11-29 | near its all-time low |
| TRX | 1 | 2017-11-12 | near its all-time low |
| XRP | 1 | 2018-01-04 | its January 2018 peak |

All but WETH's sit on a launch day or an all-time high or low, where
Yahoo's source and CoinGecko's published range differ by a few percent —
the shape the seven existing `scattered` coins already have. Kept, as
those were.

## Filecoin (FIL) — not repaired, a decision

Filecoin's mainnet launched on **2020-10-15**. Yahoo's FIL-USD runs from
2017-12-13, so 749 of the new bars — and the 288 already stored for
2020-01-01..2020-10-14 — price the pre-launch IOU/futures traded on a few
exchanges, not the coin. The series shows the seam: 30.22 on 2020-10-14,
59.26 on 10-15. Bounds call it clean (the IOU traded inside FIL's later
range), and identity is `match` because the ticker is the same project; neither
test can see an instrument change. The stored pre-launch stretch predates
this work: decision 33 lengthened it.

## Decision

34. **Recommend: floor FIL at its mainnet** — `history_valid_from =
    2020-10-15` with the reason, and delete the 1,037 pre-launch bars (749
    added today, 288 from before), backed up to `research/` first in one
    checked transaction, as AMCR and SW were. A backtest of FIL should
    start when FIL existed. About 15 min agent time; it deletes bars, so it
    is yours to approve. Alternative: keep them and add a note to the
    asset's metadata. Either way, the WETH 2019-02-05 print stays a
    `scattered` finding for a human, as the existing ones are; and running
    the bounds audit with `--write` would record the 9 coins whose status
    moved from clean to scattered (metadata only, never deletes).

    **Approved and done 2026-10-15.** In one checked transaction: FIL
    (asset 223) `history_valid_from = 2020-10-15` with the reason, and
    exactly 1,037 bars before it deleted (backup:
    `research/fil-premainnet-bars-removed-2026-10-15.csv`). FIL now starts
    2020-10-15 with 2,191 bars; its reads from that day are bit-identical to
    before; bounds clean. Ingestion obeys the floor: asked for 2017 it fetches
    nothing, asked for 2017-2021 it fetches from 2020-10-15. The bounds audit
    was NOT re-run with `--write`; the 9 coins that moved to scattered still
    carry their old status.

