"""
api/routers/strategies.py
Strategy catalogue, served straight from alpha_models/registry.py.

This is the endpoint that makes the migration guide's Phase 4 payoff real:
registering a strategy in the registry makes it appear here, with its own
parameter definitions, and therefore in any UI driven by this endpoint — no
frontend change required.

Phase 3 — FastAPI routers for the React UI
"""

from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, HTTPException, Path, Query
from pydantic import BaseModel, Field

from alpha_models import registry

router = APIRouter(prefix="/api/v1/strategies", tags=["strategies"])


class ParamSchema(BaseModel):
    name: str
    type: Literal["int", "float", "str"]
    default: Any
    label: str
    description: str = ""
    minimum: Optional[float] = None
    maximum: Optional[float] = None


class StrategySchema(BaseModel):
    id: str
    display_name: str
    description: str
    input_contract: Literal["single", "multi"] = Field(
        description=(
            "'single' takes one symbol's OHLCV frame; 'multi' takes a wide "
            "multi-symbol frame and cannot be backtested on a single symbol."
        )
    )
    params: List[ParamSchema]
    caveat: Optional[str] = Field(
        default=None,
        description="Set when the strategy is known to be unsound — surface it to the user.",
    )
    default_grid: Optional[Dict[str, List[Any]]] = Field(
        default=None,
        description=(
            "Parameter sweep used when a caller asks to optimize or compare "
            "without supplying a grid. Null means no default sweep — pass one "
            "explicitly."
        ),
    )


class StrategyListResponse(BaseModel):
    count: int
    strategies: List[StrategySchema]


def _to_schema(spec: registry.StrategySpec) -> StrategySchema:
    return StrategySchema(
        id=spec.id,
        display_name=spec.display_name,
        description=spec.description,
        input_contract=spec.input_contract,
        params=[
            ParamSchema(
                name=p.name,
                type=p.type,
                default=p.default,
                label=p.label,
                description=p.description,
                minimum=p.minimum,
                maximum=p.maximum,
            )
            for p in spec.params
        ],
        caveat=spec.caveat,
        default_grid=spec.default_grid,
    )


@router.get("", response_model=StrategyListResponse, summary="List available strategies")
async def list_strategies(
    input_contract: Optional[Literal["single", "multi"]] = Query(
        default=None,
        description="Filter by input contract. Pass 'single' for symbol-by-symbol backtests.",
    ),
) -> StrategyListResponse:
    """
    Every registered strategy and its parameter schema.

    A UI can build its entire strategy picker — including per-strategy
    parameter controls with defaults and bounds — from this response alone.
    """
    specs = registry.all_strategies()
    if input_contract:
        specs = [s for s in specs if s.input_contract == input_contract]
    return StrategyListResponse(
        count=len(specs), strategies=[_to_schema(s) for s in specs]
    )


@router.get(
    "/{strategy_id}",
    response_model=StrategySchema,
    summary="One strategy's schema",
    responses={404: {"description": "No such strategy"}},
)
async def get_strategy(
    strategy_id: str = Path(description="Registry id, e.g. 'ma_crossover'"),
) -> StrategySchema:
    try:
        return _to_schema(registry.get(strategy_id))
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from None
