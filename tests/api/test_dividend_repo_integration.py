"""
db/repositories/dividends.py against the real TimescaleDB, read-only: SPY's
stored dividends, filtered point-in-time.

    poetry run pytest -m integration tests/api/test_dividend_repo_integration.py
"""

from datetime import date

import pytest

from db.repositories.dividends import TimescaleDividendRepo
from db.session import get_session

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope="session")]


async def test_spy_dividends_up_to_a_date_only():
    async with get_session() as session:
        repo = TimescaleDividendRepo(session)
        upto = await repo.dividend_history("spy", date(2026, 9, 18))
        before = await repo.dividend_history("SPY", date(2026, 9, 17))
    assert upto[-1] == (date(2026, 9, 18), pytest.approx(1.889))
    assert len(upto) == len(before) + 1
    assert all(d <= date(2026, 9, 17) for d, _ in before)
    assert [d for d, _ in upto] == sorted(d for d, _ in upto)
