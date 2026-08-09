"""
screeners/registry.py

The single source of truth for "what screeners exist and what do they take".

Deliberately mirrors alpha_models/registry.py. Screeners have the same shape as
strategies — an id, a display name, a description, and parameters with types,
defaults and bounds — so reusing the shape means the API serves them the same
way and both UIs generate their controls the same way. Registering a screener
here is all that is required for it to appear in the React UI with working
controls and no frontend change.

Before this module, screener identity lived in
dashboard_app/ui_components/sidebar.py as hardcoded checkboxes and sliders,
which is the same duplication the strategy registry was created to end.

Phase 5 — decommissioning Streamlit
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type

from alpha_models.registry import ParamSpec
from screeners.base_screener import BaseScreener
from screeners.fundamental_screener import FundamentalScreener
from screeners.low_volatility_screener import LowVolatilityScreener
from screeners.momentum_screener import MomentumScreener

# ParamSpec is shared with the strategy registry rather than redefined: the two
# catalogues describe parameters identically, and one shape means the API
# schema and both UIs' widget-building code work for either without a branch.
__all__ = [
    "ParamSpec",
    "ScreenerSpec",
    "REGISTRY",
    "all_screeners",
    "get",
    "build",
]


@dataclass(frozen=True)
class ScreenerSpec:
    """Everything a consumer needs to list, configure and instantiate a screener."""

    id: str
    display_name: str
    cls: Type[BaseScreener]
    description: str
    params: List[ParamSpec] = field(default_factory=list)
    caveat: Optional[str] = None

    def build(self, params: Optional[Dict[str, Any]] = None) -> BaseScreener:
        """
        Instantiate the screener, filling unspecified parameters with defaults.

        Unknown names are rejected rather than ignored — the same contract as
        the strategy registry, so a typo fails loudly instead of silently
        screening on defaults.
        """
        supplied = dict(params or {})
        known = {p.name for p in self.params}
        unknown = set(supplied) - known
        if unknown:
            raise ValueError(
                f"Unknown parameter(s) for screener '{self.id}': "
                f"{sorted(unknown)}. Valid: {sorted(known) or '(none)'}"
            )

        kwargs = {p.name: supplied.get(p.name, p.default) for p in self.params}
        for p in self.params:
            if kwargs[p.name] is None:
                continue
            if p.type == "int":
                kwargs[p.name] = int(kwargs[p.name])
            elif p.type == "float":
                kwargs[p.name] = float(kwargs[p.name])
        return self.cls(**kwargs)


_SPECS: List[ScreenerSpec] = [
    ScreenerSpec(
        id="low_volatility",
        display_name="Low Volatility",
        cls=LowVolatilityScreener,
        description=(
            "Keep the least volatile names — those whose realised volatility "
            "falls in the bottom `quantile` of the universe."
        ),
        params=[
            ParamSpec(
                "quantile", "float", 0.25, "Volatility quantile",
                "Fraction of the universe to keep, lowest volatility first", 0.01, 1.0,
            ),
        ],
    ),
    ScreenerSpec(
        id="momentum",
        display_name="Momentum",
        cls=MomentumScreener,
        description="Keep names whose return over the lookback window clears a floor.",
        params=[
            ParamSpec(
                "momentum_window", "int", 126, "Lookback window",
                "Trading days over which momentum is measured (126 ~ 6 months)", 2, 756,
            ),
            ParamSpec(
                "min_momentum", "float", 0.10, "Minimum return",
                "Return over the window required to pass, e.g. 0.10 = 10%", -1.0, 10.0,
            ),
        ],
    ),
    ScreenerSpec(
        id="fundamental",
        display_name="Price / Volume",
        cls=FundamentalScreener,
        description=(
            "Liquidity floor: drop names trading below a price or an average "
            "volume. Despite the class name this screens price and volume, not "
            "fundamentals."
        ),
        params=[
            ParamSpec(
                "min_price", "float", 5.0, "Minimum price",
                "Drop names trading below this", 0.0, 10_000.0,
            ),
            ParamSpec(
                "min_avg_volume", "float", 100_000.0, "Minimum average volume",
                "Drop names below this average daily volume", 0.0, 1e9,
            ),
            ParamSpec(
                "avg_volume_days", "int", 20, "Volume window",
                "Trading days used to average volume", 1, 252,
            ),
        ],
    ),
]

REGISTRY: Dict[str, ScreenerSpec] = {s.id: s for s in _SPECS}


def all_screeners() -> List[ScreenerSpec]:
    """Every registered screener, in registration order."""
    return list(_SPECS)


def get(screener_id: str) -> ScreenerSpec:
    """Look up a screener, raising KeyError with the valid ids if unknown."""
    try:
        return REGISTRY[screener_id]
    except KeyError:
        raise KeyError(
            f"Unknown screener '{screener_id}'. Valid ids: {sorted(REGISTRY)}"
        ) from None


def build(screener_id: str, params: Optional[Dict[str, Any]] = None) -> BaseScreener:
    """Instantiate a registered screener by id."""
    return get(screener_id).build(params)
