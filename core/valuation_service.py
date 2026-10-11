"""
core/valuation_service.py

One DCF valuation, and the point-in-time value-vs-price history, assembled
from repositories. Tier 3 phase 4 of research/dcf-plan-2026-10-08.md. The
router is a thin layer over these two functions.

Every input is read AS OF the valuation date: facts filed strictly before
it, the price as traded on it, beta from bars ending on it, the ^IRX
observation on or before it. The history repeats that on the day after each
10-K/10-Q filing, so each point uses only what was public by then.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple

from config import settings
from core.fundamentals import Fact
from core.rates import RateRepository, risk_free_rate
from modeling.dcf import Beta, DCFInputs, blume, reverse_dcf, run, sensitivity
from modeling.statements import LINE_ITEMS, Statements
from modeling.valuation import Refusal, beta_from_bars, build_inputs, price_as_traded

ALL_CONCEPTS = sorted({c for cs in LINE_ITEMS.values() for c in cs})


class ValuationData(Protocol):
    async def equity(self, symbol: str) -> Optional[Tuple[List[int], Optional[str]]]:
        """([cik, *predecessor ciks], sector); an empty list when no CIK is recorded."""
        ...
    async def facts(self, ciks: Iterable[int], concepts: Iterable[str]) -> List[Fact]: ...
    async def splits(self, symbol: str) -> List[Tuple[date, float]]: ...
    async def filing_dates(self, ciks: Iterable[int]) -> List[date]: ...
    async def coverage(self) -> Dict[str, object]: ...


class ValuationError(Exception):
    """A valuation that cannot be made, with the reason; `status` for HTTP."""

    def __init__(self, status: int, reason: str) -> None:
        super().__init__(reason)
        self.status, self.reason = status, reason


@dataclass
class Overrides:
    growth_first: Optional[float] = None
    #: Replaces the decision-4 beta in the cost of equity; the computed betas
    #: are still reported beside it.
    beta: Optional[float] = None
    growth_terminal: Optional[float] = None
    wacc: Optional[float] = None
    exit_multiple: Optional[float] = None
    years: Optional[int] = None


async def load_statements(data: ValuationData, symbol: str) -> Tuple[Statements, int, Optional[str], List[int]]:
    """
    Facts from the asset's CIK AND its predecessors (settings.CIK_PREDECESSORS):
    a holding-company reorganisation leaves the history under the old filer.
    Per period the latest-filed row still wins, across all of them.
    """
    eq = await data.equity(symbol)
    if eq is None:
        raise ValuationError(404, f"{symbol} is not a registered equity (ETFs and crypto have no filer)")
    ciks, sector = eq
    cik = ciks[0] if ciks else None
    if sector in settings.DCF_REFUSED_SECTORS:
        raise ValuationError(422, f"{symbol} is in {sector}: free cash flow is not meaningful for banks and insurers")
    if sector in settings.DCF_PENDING_SECTORS:
        raise ValuationError(422, f"{symbol} is in {sector}: {settings.DCF_PENDING_SECTORS[sector]}")
    if cik is None:
        raise ValuationError(422, f"{symbol} has no CIK: run scripts/resolve_ciks.py, or the ticker is reassigned")
    facts = await data.facts(ciks, ALL_CONCEPTS)
    if not facts:
        raise ValuationError(422, f"no SEC facts stored for {symbol} (CIK {cik}): run scripts/load_fundamentals.py")
    return Statements(facts, await data.splits(symbol)), cik, sector, ciks


async def value(
    data: ValuationData,
    prices: Any,
    rates: RateRepository,
    symbol: str,
    as_of: date,
    overrides: Overrides = Overrides(),
    statements: Optional[Tuple[Statements, int, Optional[str], List[int]]] = None,
    with_grid: bool = True,
) -> Dict[str, Any]:
    st, cik, sector, ciks = statements or await load_statements(data, symbol)
    traded = await price_as_traded(prices, symbol, as_of)
    if traded is None:
        raise ValuationError(422, f"no {symbol} price on or before {as_of}")
    price_day, price = traded
    rate = await risk_free_rate(rates, as_of)
    if rate is None:
        raise ValuationError(409, f"no {settings.RISK_FREE_RATE_SERIES} observation on or before {as_of}")
    try:
        beta = await beta_from_bars(prices, symbol, as_of)
    except ValueError as exc:
        raise ValuationError(422, f"beta unavailable: {exc}") from None
    try:
        weekly = await beta_from_bars(prices, symbol, as_of, window=260, frequency="weekly")
    except ValueError:
        weekly = None
    raw_beta = beta
    if overrides.beta is not None:
        beta = Beta(overrides.beta, float("nan"), 0)

    built = build_inputs(st, as_of, price, rate.continuous, beta, sector=sector, growth_first=overrides.growth_first)
    if isinstance(built, Refusal):
        raise ValuationError(422, built.reason)
    x = built.dcf
    changes = {k: v for k, v in {
        "growth_terminal": overrides.growth_terminal, "wacc": overrides.wacc,
        "exit_multiple": overrides.exit_multiple, "years": overrides.years,
    }.items() if v is not None}
    if changes:
        x = DCFInputs(**{**asdict(x), **changes})
    result = run(x)
    implied = reverse_dcf(x, price)
    out = {
        "symbol": symbol.upper(),
        "cik": cik,
        "predecessor_ciks": ciks[1:],
        "sector": sector,
        "as_of": as_of,
        "price": price,
        "price_date": price_day,
        "market_cap": built.market_cap,
        "result": result.as_dict(),
        "discount": asdict(built.discount),
        "lines": {k: {"value": v, "source": src} for k, (v, src) in built.lines.items()},
        "implied_growth_first": None if math.isnan(implied.growth_first) else implied.growth_first,
        "implied_reason": implied.reason,
        "notes": built.notes,
        "betas": {
            "daily_3y": {"beta": raw_beta.beta, "std_error": raw_beta.std_error, "observations": raw_beta.observations},
            "daily_3y_blume": {"beta": blume(raw_beta.beta), "std_error": None, "observations": raw_beta.observations},
            "weekly_5y": None if weekly is None else {"beta": weekly.beta, "std_error": weekly.std_error, "observations": weekly.observations},
        },
        "overridden": sorted(changes) + (["growth_first"] if overrides.growth_first is not None else [])
                      + (["beta"] if overrides.beta is not None else []),
    }
    if with_grid:
        out["sensitivity"] = sensitivity(x)
    return out


async def history(
    data: ValuationData, prices: Any, rates: RateRepository, symbol: str, since: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """Value and price on the day after each 10-K/10-Q filing. Points that cannot be valued say why."""
    st = await load_statements(data, symbol)
    points: List[Dict[str, Any]] = []
    for filed in await data.filing_dates(st[3]):
        as_of = filed + timedelta(days=1)
        if since is not None and as_of < since:
            continue
        try:
            v = await value(data, prices, rates, symbol, as_of, statements=st, with_grid=False)
        except ValuationError as exc:
            points.append({"as_of": as_of, "price": None, "per_share": None, "reason": exc.reason})
            continue
        r = v["result"]
        points.append({
            "as_of": as_of, "price": v["price"], "per_share": r["per_share"],
            "exit_per_share": r["exit_per_share"], "wacc": v["discount"]["wacc"],
            "implied_growth_first": v["implied_growth_first"], "reason": r["reason"],
        })
    return points
