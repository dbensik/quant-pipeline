# The low-volatility screener returns nothing (decision 43, 2026-10-19)

Found while building decision 42. `LowVolatilityScreener`
(`screeners/low_volatility_screener.py`) ranks names on a `volatility_90d`
column that it expects to be precomputed. That column came from the SQLite
store. Since the TimescaleDB cutover (`00a3702`), `records_to_frame` builds
Open, High, Low, Close and Volume only, so the screener finds no values and
returns an empty list every time.

Measured through the API: an S&P 500 screen over 2025 (reconstructed list
of 2024-12-31) with `low_volatility` at `quantile` 1.0, which should keep
everything, gave **495 names with data and 0 passed**. The tests never
caught it, because they only used it as a step that lets everything through,
and an empty step after a filter reads like a tight filter.

## What a working screener would return

On the same 495 names, with the annualized standard deviation of the last 90
daily returns in the window:

- Median volatility 24.6%. The bottom-quartile cutoff is 20.2%, which keeps
  124 names.
- Lowest ten: K 6.2% (Kellanova, pinned to its takeover price), LNT, FE,
  BRK-B, EVRG, DUK, DTE, AEE, CNP and WEC, all 12.9-13.9%. Mostly utilities,
  as expected.

## Decision

43. **Recommend A.**

    - **A: compute it in the screener from the bars it is given.** Use the
      annualized standard deviation of the last `window` daily returns,
      with a new parameter `window` defaulting to 90, which keeps the old
      meaning. Annualize by `periods_per_year` (252 for equities, 365 for
      crypto; decision 38). Measure at the end of the window, as before. A
      name with fewer than `window` + 1 bars is not ranked, and the step
      count shows the drop. `get_analysis_metric`, which nothing calls today,
      uses the same function. Tests: a fixture with known volatilities keeps exactly
      the bottom quantile, `quantile` 1.0 keeps every name with enough
      bars, and both fail against the current code. About 1-1.5 h of my
      time.
    - **B: store `volatility_90d` as a column.** Rejected. It is derived
      data, and prices are adjusted at read time, so a stored figure would
      go stale at every split or dividend restatement.
    - **C: remove the screener.** Not recommended. Low volatility is a
      standard factor, and the fix is small.

## Built (A, approved 2026-10-19)

- `screeners/low_volatility_screener.py` measures `realised_volatility`
  itself, with the new `window` parameter (default 90).
- On 2025's S&P 500 list it now keeps 124 of 495 names, against 0 before.
- A window shorter than `window` + 1 bars ranks nothing; that is by design,
  and the empty step shows where.
- Tests: `tests/unit/test_low_volatility_screener.py` and an API case in
  `tests/api/test_screeners_router.py`. Each fails against the old screener,
  which kept nothing.
