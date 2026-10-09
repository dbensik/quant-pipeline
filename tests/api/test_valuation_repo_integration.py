"""
db/repositories/valuation.py against the real TimescaleDB, read-only: Apple's
CIK and sector from the registry, its split, its filing dates, coverage.
"""

from datetime import date

import pytest

from db.repositories.valuation import TimescaleValuationRepo
from db.session import get_session

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def test_apple_from_the_registry_and_facts():
    async with get_session() as session:
        repo = TimescaleValuationRepo(session)
        assert await repo.equity("aapl") == (320193, "Information Technology")
        assert await repo.equity("SPY") is None  # an ETF has no filer
        assert (date(2020, 8, 31), 4.0) in await repo.splits("AAPL")
        filed = await repo.filing_dates(320193)
        assert date(2025, 10, 31) in filed and filed == sorted(filed)
        cov = await repo.coverage()
        assert cov["with_cik"] >= 500 and cov["ciks_with_facts"] >= 1
