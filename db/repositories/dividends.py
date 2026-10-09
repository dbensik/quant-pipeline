"""
db/repositories/dividends.py

Dividend history from `corporate_actions`, for projecting the discrete
dividends an option surface prices with (core/options_surface.py).

Point in time: only ex-dates on or before the requested date are returned,
so a surface for a past capture projects from what was known then.

Amounts are stored in the split basis of the day they were fetched (see
TimescaleMarketDataRepo.write_actions). The projection uses the latest
amount, so this matters only if a split falls between the last dividend and
the capture; none has for the six capture tickers since they were added.
"""

from __future__ import annotations

from datetime import date
from typing import List, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import AssetORM, CorporateActionORM


class TimescaleDividendRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def dividend_history(self, symbol: str, on_or_before: date) -> List[Tuple[date, float]]:
        result = await self.session.execute(
            select(CorporateActionORM.ex_date, CorporateActionORM.value)
            .join(AssetORM, AssetORM.id == CorporateActionORM.asset_id)
            .where(
                AssetORM.symbol == symbol.upper(),
                CorporateActionORM.kind == "dividend",
                CorporateActionORM.ex_date <= on_or_before,
            )
            .order_by(CorporateActionORM.ex_date)
        )
        return [(row.ex_date, float(row.value)) for row in result]
