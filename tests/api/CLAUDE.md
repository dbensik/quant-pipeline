# tests/api/

Know the `tests/api/conftest.py` fixture before asserting on relationships
*between* symbols. AAPL and BTC-USD come from one price formula differing only
in scale, so their **returns are identical** — any test about correlation,
diversification or portfolio weights is vacuous over that pair. Use MSFT, which
follows an independent seeded random walk, as the decorrelated counterpart.
