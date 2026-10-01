# Served-price backfill and gate — phase 4 (2026-09-27, applied)

Phase 4 of `dividend-drift-plan-2026-09-27.md`. The backfill is done; the
per-symbol verdicts below were **applied 2026-09-27 on approval**: `price_basis =
'served'` for 516 (the 512 passes plus the 4 failures, per the recommendation)
and `'legacy'` for the 11; the 99 crypto assets stay NULL. No reader uses
`price_basis` until phase 5, so every read is still what it was.

## What the gate proves, and what it does not

The engine's `total` output equals Yahoo's CURRENT auto_adjust series to 1e-6
(phase 2). So the gate compares **stored history against Yahoo today**. A pass
means: switching this symbol changes its daily returns only on days the plan
meant to fix. It does not re-test the engine. Per-symbol detail:
`served-gate-2026-09-27.json`; rules: `scripts/gate_served.py`.

## The backfill

`scripts/backfill_served.py` over all 527 equities and ETFs: **890,960 bars
filled, 18,362 actions recorded (18,407 dividends and 170 splits in total with
the earlier pilot, plus HWM's manual factor), 0 fetch errors.** It fills only
`served_*` on existing rows and never inserts a bar.

- Adjusted columns: a fingerprint of all 600 symbols (every row before
  2026-09-28), taken before the backfill, is **identical** after it.
- Coverage: 907,988 of 923,669 equity/ETF bars now have served values. Every
  missing one belongs to the 11 assets below.
- `VACUUM ANALYZE` run on `market_data` and `corporate_actions`.

## Verdicts

| verdict | assets |
|---|---|
| **pass** — every mismatch explained | **512** |
| **fail** — unexplained mismatch, engine = Yahoo on every such day | **4**: CHRW, COR, FAST, MSFT |
| **legacy** — no served values | **11** |

### Legacy (keep current series)

- **Yahoo serves nothing**: ANSS, CTRA, DAY, HES, HOLX, IPG, K, WBA — every
  bar uncovered.
- **Yahoo serves only a post-deal stub**: AVB (1 of 1,663 bars), EA (1 of
  1,644).
- **Identity-blocked**: PARA — Yahoo's key serves another company, so the
  backfill skipped it by design.

These keep the 2025-07-16 seam step; nothing Yahoo serves can fix them.

### Mismatch classes (days where engine and stored differ by >= 0.5pp)

| class | days | what it is |
|---|---|---|
| seam | 364 | 2025-07-16: bulk load (as of 2025-07) meets the outage refill (as of 2026-08). Two-sided: the gap must be within 0.2pp of the COMPOUNDED yields (1 - prod(1 - y)) of the dividends in between, or of a running product of them. Measured best residual: median 0.015pp, p95 0.084, max 0.178 |
| fill | 138 | the 2026-08-28/31 pair (and DXCM/MGM 09-22/23), equal and opposite |
| dividend | 124 | the symbol's own ex-date, gap within 0.1pp of the dividend's yield |
| schedule | 23 | from 2025-07-16 on, gap within 0.05pp of ONE nearby dividend's yield (worst 0.022pp): bars either side fetched on opposite sides of it. The 2026-08-17 boundary (Mac asleep 08-18..24, 22 symbols), 2026-08-27 (the DB outage), and filled bars that caught a later ex-date |
| **unexplained** | **18** | the 4 failures |

Seam dates were found from the data, not assumed: dates where >= 20 symbols
mismatch together were 2025-07-16 (364), 2026-08-17 (22), 2026-08-28 and 08-31
(70 each, the fill). **No 2020-07-13 seam** (bulk load vs migrated legacy)
appeared at 0.5pp.

### The 4 failures — stored is wrong, and switching would correct it

On all 18 unexplained days the engine matches Yahoo's current auto_adjust
return to 0.0000pp; the stored value is the outlier. That alone would only
show stored and Yahoo disagree. What shows STORED is wrong: the stored/engine
price ratio is constant on each side of every one of these days and steps on
the day itself — not a one-day spike that reverts. So stored is two
differently adjusted segments joined there, and its return that day is an
artefact of the join:

| symbol | ratio before -> after |
|---|---|
| MSFT 2020-01-02 | 1.00188 -> 1.00989 |
| CHRW 2021-12-10 | 1.01462 -> 1.02019 |
| COR 2020-03-30 | 1.11101 -> 1.09443 |
| FAST 2020-01-30 (one of 15) | 0.93458 -> 0.94118 |

| symbol | days | stored vs Yahoo |
|---|---|---|
| FAST | 15 ex-dates, 2020-01-30 .. 2025-04-25 | stored is higher by the dividend's yield every time (2020-01-30: +2.09% vs +1.37%) — the bulk load **double-counted** FAST's dividends |
| MSFT | 2020-01-02 | +2.67% vs +1.85% — a step inside the bulk load |
| COR | 2020-03-30 | +7.73% vs +9.36% on a $1.22 ex-date — dividend missing from stored |
| CHRW | 2021-12-10 | +4.62% vs +4.05%, no action that day |

These fall outside the gate's rules on purpose: a dividend-sized gap before
2025-07-16 cannot be a fetch-schedule effect, so it is shown to a human
rather than explained away.

### Opens, highs and lows

The gate compares closes; cutover serves the whole bar. Checked separately:
the 38 extremes nulled on 2026-09-22 (`cb538b8`) were all crypto, untouched
here — 0 equity/ETF rows have a served extreme where the repair left NULL. One
served bar is internally inconsistent — HUBB 2021-05-05, low above open — and
the stored bar has the same fault: it is Yahoo's, not introduced here.

## Decisions for Danny

1. **Apply the verdicts**: `price_basis = 'served'` for the 512 passes and
   `'legacy'` for the 11. This changes no reads until phase 5 wires
   `fetch_range` to it.
2. **The 4 failures**: recommend `served` as well — each is a stored level
   step (ratio test above) that the switch removes, and the engine matches
   Yahoo on every one of the 18 days. The alternative is
   `legacy`, keeping FAST's double-counted dividends.

## Recorded for phase 5

- **`served` requires full coverage.** A read must never fall back to the
  adjusted columns bar by bar — that mixes two bases, which is the problem
  being fixed. The gate enforces it; `fetch_range` must too.
- New bars now get served values on insert (phase 3). On 2026-09-28 the
  daily log's "Served prices filled" should be about 1,400 — the 14-day
  overlap on the ~99 crypto assets, which this backfill skipped — and about 0
  for equities; from 09-29 on, about 0 in total. Thousands every morning
  would mean the insert path or the NULL guard is broken.

## Addendum 2026-09-30: where the 2025-07-16 seam still reaches

Measured after cutover, return on 2025-07-16 per asset: read-time (`fetch_range`,
`total`), stored columns, and a fresh Yahoo `auto_adjust` fetch.

- **Served (516): closed.** Read-time vs Yahoo: 0 of 515 off by 0.5pp, max gap
  0.00003pp (BSX failed to download in that run). Stored vs Yahoo still shows
  the step (364 at 0.5pp, 121 at 3pp), as expected: those columns are frozen
  and nothing serves them to readers of served assets.
- **Legacy: only AVB carries it, not all 11** as the line above implied. Nine
  (ANSS CTRA DAY HES HOLX IPG K WBA, and PARA) have no bar after 2025-07-15,
  so there is nothing on the other side of the seam. EA spans it but pays
  ~0.13% a quarter; its step is ~0.5pp at most, inside noise.
- **AVB: about -4pp on 2025-07-16, left in place.** Stored -2.67% that day.
  Apartment-REIT peers rose +1.4 to +2.0% (read-time = Yahoo), and their own
  stored-minus-true seam gaps were 3.9-4.6pp (ESS MAA UDR CPT), consistent
  with AVB's ~3.5% yield. Yahoo no longer serves AVB, so there is no exact
  factor to apply. Fixing it means rescaling 1,390 stored bars, a restating
  write that the 2026-09-28 rule forbids. It is one day in one delisted name.
- **Readers that bypass `fetch_range`: none affected.** `find_successors`
  reads stored closes over a 400-day lookback that starts 2025-08-26 and moves
  away from the seam. `check_reassigned` keys on a 20x dollar-volume collapse.
  `audit_crypto_bounds` and `repair_bad_extremes` are crypto-only.
  `portfolio_store` reads the latest close.
