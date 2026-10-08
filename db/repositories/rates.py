"""
db/repositories/rates.py

TimescaleDB implementation of core.rates.RateRepository.

Insert-only: ON CONFLICT DO NOTHING on (series, obs_date), so a re-served
value never replaces a stored one. Reads take the newest observation ON OR
BEFORE a date; there is no exact-date lookup to misuse.
"""

from __future__ import annotations

from datetime import date
from typing import List, Optional

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.rates import RateObservation
from db.models import RateObservationORM


class TimescaleRateRepo:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def insert(self, observations: List[RateObservation]) -> int:
        if not observations:
            return 0
        stmt = (
            pg_insert(RateObservationORM)
            .values(
                [
                    {
                        "series": o.series,
                        "obs_date": o.obs_date,
                        "value": o.value,
                        "source": o.source,
                        "fetched_at": o.fetched_at,
                    }
                    for o in observations
                ]
            )
            .on_conflict_do_nothing(constraint="uq_rate_observation")
            .returning(RateObservationORM.id)
        )
        result = await self.session.execute(stmt)
        inserted = len(result.fetchall())
        await self.session.commit()
        return inserted

    async def newest_date(self, series: str) -> Optional[date]:
        result = await self.session.execute(
            select(func.max(RateObservationORM.obs_date)).where(
                RateObservationORM.series == series
            )
        )
        return result.scalar_one_or_none()

    async def latest_on_or_before(
        self, series: str, as_of: date
    ) -> Optional[RateObservation]:
        result = await self.session.execute(
            select(RateObservationORM)
            .where(
                RateObservationORM.series == series,
                RateObservationORM.obs_date <= as_of,
            )
            .order_by(RateObservationORM.obs_date.desc())
            .limit(1)
        )
        row = result.scalars().first()
        if row is None:
            return None
        return RateObservation(
            series=row.series,
            obs_date=row.obs_date,
            value=row.value,
            source=row.source,
            fetched_at=row.fetched_at,
        )
