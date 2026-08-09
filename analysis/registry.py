"""
analysis/registry.py

Catalogue of the statistical tests the API exposes.

Unlike strategies and screeners, these do NOT share one call signature — they
differ in arity (one series, a pair, or N) and in whether they consume price
levels or returns. So this registry describes each test rather than
constructing a uniform object: the router dispatches on `test_id`, and the
catalogue is what lets a UI build the right controls and ask for the right
number of symbols.

Phase 5 — decommissioning Streamlit
"""

from dataclasses import dataclass, field
from typing import Dict, List, Literal, Optional

from alpha_models.registry import ParamSpec

# 'single' takes one symbol, 'pair' exactly two, 'multi' two or more.
Arity = Literal["single", "pair", "multi"]

# Whether the test consumes price LEVELS or percentage RETURNS.
#
# This is not cosmetic — the two answer different questions:
#
#   PCA on price levels measures shared TREND, not co-movement. Price series
#   are non-stationary, so any two drifting upward look correlated regardless
#   of whether their day-to-day moves relate at all. Measured on four synthetic
#   independent random walks: PC1 explains 0.510 of variance on levels but only
#   0.278 on returns — the levels figure is drift, not structure.
#
#   (Note it is NOT a scale problem: PrincipalComponentAnalyzer standardises
#   each column, so a $30,000 BTC series does not swamp a $100 equity. The
#   issue is non-stationarity, which standardising does not fix.)
#
#   ADF on returns is near-meaningless: returns are almost always stationary,
#   so it would report stationarity for everything.
#
# The conversion therefore has to be declared per test, not left to the caller.
InputKind = Literal["price", "returns"]


@dataclass(frozen=True)
class TestSpec:
    id: str
    display_name: str
    description: str
    arity: Arity
    input_kind: InputKind
    params: List[ParamSpec] = field(default_factory=list)
    caveat: Optional[str] = None


_SPECS: List[TestSpec] = [
    TestSpec(
        id="adf",
        display_name="Augmented Dickey-Fuller",
        description=(
            "Tests a price series for stationarity. A stationary series is "
            "mean-reverting; a non-stationary one wanders."
        ),
        arity="single",
        input_kind="price",
    ),
    TestSpec(
        id="kalman",
        display_name="Kalman Filter / Smoother",
        description="Smooths a price series into an estimated hidden state.",
        arity="single",
        input_kind="price",
    ),
    TestSpec(
        id="ols",
        display_name="OLS Regression (Alpha / Beta)",
        description=(
            "Regresses an asset's returns on a benchmark's to estimate alpha "
            "and beta. The FIRST symbol is the asset, the second the benchmark."
        ),
        arity="pair",
        input_kind="returns",
    ),
    TestSpec(
        id="engle_granger",
        display_name="Engle-Granger Cointegration",
        description="Tests whether two price series are cointegrated.",
        arity="pair",
        input_kind="price",
    ),
    TestSpec(
        id="johansen",
        display_name="Johansen Cointegration",
        description=(
            "Tests two or more price series for cointegrating relationships, "
            "and yields the weights the Cointegrated Mean Reversion strategy "
            "needs."
        ),
        arity="multi",
        input_kind="price",
        params=[
            ParamSpec(
                "det_order", "int", 0, "Deterministic order",
                "-1 no constant, 0 constant term, 1 linear trend", -1, 1,
            ),
            ParamSpec(
                "k_ar_diff", "int", 1, "Lagged differences",
                "Number of lagged differences in the VAR", 1, 20,
            ),
        ],
    ),
    TestSpec(
        id="pca",
        display_name="Principal Component Analysis",
        description=(
            "Decomposes a set of return series into orthogonal components, "
            "showing how much variance each explains."
        ),
        arity="multi",
        input_kind="returns",
        params=[
            ParamSpec(
                "n_components", "int", 3, "Components",
                "How many principal components to compute", 1, 50,
            ),
        ],
    ),
]

REGISTRY: Dict[str, TestSpec] = {s.id: s for s in _SPECS}

MIN_SYMBOLS: Dict[str, int] = {"single": 1, "pair": 2, "multi": 2}


def all_tests() -> List[TestSpec]:
    """Every registered test, in registration order."""
    return list(_SPECS)


def get(test_id: str) -> TestSpec:
    """Look up a test, raising KeyError with the valid ids if unknown."""
    try:
        return REGISTRY[test_id]
    except KeyError:
        raise KeyError(
            f"Unknown statistical test '{test_id}'. Valid ids: {sorted(REGISTRY)}"
        ) from None


def resolve_params(spec: TestSpec, supplied: Optional[Dict] = None) -> Dict:
    """
    Fill defaults and reject unknown names — the same contract as the strategy
    and screener registries, so a typo fails loudly rather than silently
    running with defaults.
    """
    given = dict(supplied or {})
    known = {p.name for p in spec.params}
    unknown = set(given) - known
    if unknown:
        raise ValueError(
            f"Unknown parameter(s) for test '{spec.id}': {sorted(unknown)}. "
            f"Valid: {sorted(known) or '(none)'}"
        )

    out = {}
    for p in spec.params:
        value = given.get(p.name, p.default)
        if value is not None and p.type == "int":
            value = int(value)
        elif value is not None and p.type == "float":
            value = float(value)
        out[p.name] = value
    return out
