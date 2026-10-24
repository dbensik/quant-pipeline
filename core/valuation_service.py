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
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Protocol, Tuple

from config import settings
from core.fundamentals import Fact
from core.rates import RateRepository, risk_free_rate
from modeling.dcf import Beta, DCFInputs, blume, reverse_dcf, run, sensitivity
from modeling.statements import LINE_ITEMS, FirstOf, Statements, map_part
from modeling.valuation import Refusal, beta_from_bars, build_inputs, price_as_traded

#: Every concept a line can read: the lists, and the per-filer maps (a mapped
#: concept need not be on a list: CMCSA's intangible capex is read by name).
ALL_CONCEPTS = sorted(
    {c for cs in LINE_ITEMS.values() for c in cs}
    | {
        map_part(c)[0]
        for maps in (settings.LINE_CONCEPTS_BY_SYMBOL, settings.LINE_CONCEPTS_FIRST_BY_SYMBOL)
        for lines in maps.values()
        for cs in lines.values()
        for c in cs
    }
)


class ValuationData(Protocol):
    async def equity(self, symbol: str) -> Optional[Tuple[List[int], Optional[str]]]:
        """([cik, *predecessor ciks], sector); an empty list when no CIK is recorded."""
        ...

    async def share_classes(self, symbols: Iterable[str]) -> Dict[str, List[str]]:
        """Each registered equity -> the other equities carrying its own CIK (GOOGL -> [GOOG])."""
        ...

    async def news_identities(self, symbols: Iterable[str]) -> Dict[str, Tuple[str, Optional[str]]]:
        """Each registered equity, ETF or coin -> (asset class, metadata.verified_name)."""
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


async def filer_statements(
    data: ValuationData, symbol: str
) -> Tuple[Statements, int, Optional[str], List[int]]:
    """
    A registered filer's statements with NO valuation refusals: what the
    Research page shows, banks included. Facts from the asset's CIK AND its
    predecessors (settings.CIK_PREDECESSORS): a holding-company
    reorganisation leaves the history under the old filer. Per period the
    latest-filed row still wins, across all of them.
    """
    eq = await data.equity(symbol)
    if eq is None:
        raise ValuationError(
            404, f"{symbol} is not a registered equity (ETFs and crypto have no filer)"
        )
    ciks, sector = eq
    cik = ciks[0] if ciks else None
    if cik is None:
        raise ValuationError(
            422, f"{symbol} has no CIK: run scripts/resolve_ciks.py, or the ticker is reassigned"
        )
    facts = successor_facts(symbol, ciks, await data.facts(ciks, ALL_CONCEPTS))
    if not facts:
        raise ValuationError(
            422, f"no SEC facts stored for {symbol} (CIK {cik}): run scripts/load_fundamentals.py"
        )
    line_concepts = filer_line_concepts(symbol)
    splits, spinoffs = share_splits(symbol, await data.splits(symbol))
    aside = settings.FACTS_SET_ASIDE_BY_SYMBOL.get(symbol.upper(), {})
    return Statements(facts, splits, line_concepts, spinoffs, aside), cik, sector, ciks


def filer_line_concepts(symbol: str) -> Dict[str, Tuple[str, ...]]:
    """A filer's per-line reading: its sums, then its orders (a line has one or the other)."""
    sym = symbol.upper()
    return {
        **{
            item: FirstOf(cs)
            for item, cs in settings.LINE_CONCEPTS_FIRST_BY_SYMBOL.get(sym, {}).items()
        },
        **settings.LINE_CONCEPTS_BY_SYMBOL.get(sym, {}),
    }


def successor_facts(symbol: str, ciks: List[int], facts: List[Fact]) -> List[Fact]:
    """
    The facts of `ciks` (own CIK first) as one filer: a predecessor's only
    from filings before the successor's first periodic one, or through
    settings.CIK_PREDECESSOR_READ_THROUGH (decision 63). APA's Apache kept
    filing as its subsidiary, 1,000 shares on the cover, and on each shared
    filing day its rows stood beside APA's own.
    """
    own = ciks[0] if ciks else None
    first = min(
        (f.filed for f in facts if f.cik == own and f.form in settings.SEC_FACT_FORMS),
        default=None,
    )
    through = settings.CIK_PREDECESSOR_READ_THROUGH.get(symbol.upper())
    if first is None and through is None:
        return facts

    def read(f: Fact) -> bool:
        if f.cik == own:
            return True
        if through is not None:
            return f.filed <= date.fromisoformat(through)
        return f.filed < first

    return [f for f in facts if read(f)]


def share_splits(
    symbol: str, rows: List[Tuple[date, float]]
) -> Tuple[List[Tuple[date, float]], List[date]]:
    """
    Yahoo's split rows as share-count ratios, and the spin-offs among them
    (decision 58). A reviewed row moves share counts by its listed ratio: 1.0
    for a spin or a payment, the reverse split done with a spin (HON 0.5 on
    2026-06-29). An odd row not yet reviewed stays a split, as before, and is
    flagged by scripts/check_line_concepts.py.
    """
    splits: List[Tuple[date, float]] = []
    spinoffs: List[date] = []
    for ex_date, ratio in rows:
        reviewed = settings.SPLIT_ROWS_REVIEWED.get((symbol.upper(), ex_date.isoformat()))
        if reviewed is None:
            splits.append((ex_date, ratio))
            continue
        kind, share_ratio, _ = reviewed
        if kind == "spin-off":
            spinoffs.append(ex_date)
        if share_ratio != 1.0:
            splits.append((ex_date, share_ratio))
    return splits, spinoffs


async def load_statements(
    data: ValuationData, symbol: str
) -> Tuple[Statements, int, Optional[str], List[int]]:
    """filer_statements, refused first for a sector or symbol the DCF does not value."""
    eq = await data.equity(symbol)
    if eq is not None:
        sector = eq[1]
        sector_refusal = settings.dcf_sector_refusal(symbol, sector)
        if sector_refusal:
            raise ValuationError(422, f"{symbol} is in {sector}: {sector_refusal}")
        if symbol.upper() in settings.DCF_REFUSED_SYMBOLS:
            raise ValuationError(
                422, f"{symbol} is refused: {settings.DCF_REFUSED_SYMBOLS[symbol.upper()]}"
            )
    return await filer_statements(data, symbol)


async def value(
    data: ValuationData,
    prices: Any,
    rates: RateRepository,
    symbol: str,
    as_of: date,
    overrides: Optional[Overrides] = None,
    statements: Optional[Tuple[Statements, int, Optional[str], List[int]]] = None,
    with_grid: bool = True,
) -> Dict[str, Any]:
    if overrides is None:
        overrides = Overrides()
    st, cik, sector, ciks = statements or await load_statements(data, symbol)
    traded = await price_as_traded(prices, symbol, as_of)
    if traded is None:
        raise ValuationError(422, f"no {symbol} price on or before {as_of}")
    price_day, price = traded
    rate = await risk_free_rate(rates, as_of)
    if rate is None:
        raise ValuationError(
            409, f"no {settings.RISK_FREE_RATE_SERIES} observation on or before {as_of}"
        )
    try:
        raw_beta = await beta_from_bars(prices, symbol, as_of)
    except ValueError as exc:
        raise ValuationError(422, f"beta unavailable: {exc}") from None
    try:
        weekly = await beta_from_bars(
            prices, symbol, as_of, window=settings.DCF_BETA_WEEKLY_WINDOW, frequency="weekly"
        )
    except ValueError:
        weekly = None
    # Decision 11: the beta USED is weekly 5y Blume-adjusted; daily 3y when
    # the weekly series is too short (said so in the beta line).
    if overrides.beta is not None:
        beta, beta_src = Beta(overrides.beta, float("nan"), 0), "given (override)"
    elif weekly is not None:
        beta = Beta(blume(weekly.beta), weekly.std_error * 2 / 3, weekly.observations)
        beta_src = (
            f"weekly 5y vs {settings.DCF_BETA_BENCHMARK}, Blume-adjusted "
            f"(raw {weekly.beta:.3f}, SE {weekly.std_error:.3f}, {weekly.observations} weeks)"
        )
    else:
        beta = raw_beta
        beta_src = f"daily 3y vs {settings.DCF_BETA_BENCHMARK} (weekly series too short), SE {raw_beta.std_error:.3f}"

    built = build_inputs(
        st,
        as_of,
        price,
        rate.continuous,
        beta,
        sector=sector,
        growth_first=overrides.growth_first,
        symbol=symbol,
    )
    if isinstance(built, Refusal):
        raise ValuationError(422, built.reason)
    built.lines["beta"] = (beta.beta, beta_src)
    x = built.dcf
    changes = {
        k: v
        for k, v in {
            "growth_terminal": overrides.growth_terminal,
            "wacc": overrides.wacc,
            "exit_multiple": overrides.exit_multiple,
            "years": overrides.years,
        }.items()
        if v is not None
    }
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
            "daily_3y": {
                "beta": raw_beta.beta,
                "std_error": raw_beta.std_error,
                "observations": raw_beta.observations,
            },
            "daily_3y_blume": {
                "beta": blume(raw_beta.beta),
                "std_error": None,
                "observations": raw_beta.observations,
            },
            "weekly_5y": None
            if weekly is None
            else {
                "beta": weekly.beta,
                "std_error": weekly.std_error,
                "observations": weekly.observations,
            },
        },
        "overridden": sorted(changes)
        + (["growth_first"] if overrides.growth_first is not None else [])
        + (["beta"] if overrides.beta is not None else []),
    }
    if with_grid:
        out["sensitivity"] = sensitivity(x)
    return out


class CachedPrices:
    """
    Fetch each (symbol, adjust) series ONCE and slice it in memory. A history
    re-values at every filing date, and each valuation reads the price and
    two betas: AAPL's 69 filings took 22.5 s re-fetching years of bars each
    time (measured 2026-10-11). Slicing is exact for these reads: an as-traded
    price does not change with the window, and returns are ratios of closes
    adjusted to one as-of date, so the window does not move them either.
    """

    def __init__(self, inner: Any) -> None:
        self.inner, self.cache, self.fetches = inner, {}, 0

    async def fetch_range(self, symbol, asset_class, start, end, source=None, adjust="total"):
        key = (symbol, asset_class, source, adjust)
        if key not in self.cache:
            self.fetches += 1
            self.cache[key] = await self.inner.fetch_range(
                symbol,
                asset_class,
                datetime(1990, 1, 1, tzinfo=timezone.utc),
                datetime(2100, 1, 1, tzinfo=timezone.utc),
                source,
                adjust,
            )
        return [r for r in self.cache[key] if start <= r.ohlcv.timestamp.utc <= end]

    async def find_asset(self, *args, **kwargs):
        return await self.inner.find_asset(*args, **kwargs)


async def history(
    data: ValuationData,
    prices: Any,
    rates: RateRepository,
    symbol: str,
    since: Optional[date] = None,
) -> List[Dict[str, Any]]:
    """Value and price on the day after each 10-K/10-Q filing. Points that cannot be valued say why."""
    st = await load_statements(data, symbol)
    prices = CachedPrices(prices)
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
        points.append(
            {
                "as_of": as_of,
                "price": v["price"],
                "per_share": r["per_share"],
                "exit_per_share": r["exit_per_share"],
                "wacc": v["discount"]["wacc"],
                "implied_growth_first": v["implied_growth_first"],
                "reason": r["reason"],
            }
        )
    return points
