"""
scripts/check_paper_margin.py

Flag paper portfolios below their maintenance margin (decision 49,
research/paper-shorts-2026-10-19.md). Read-only: it never covers a short. A
paper portfolio is a record of decisions someone made, so a forced cover
nobody placed is not written into it.

    python scripts/check_paper_margin.py

Exit 0 when every portfolio with short terms is above maintenance, 1 when one
is below, 2 if the check itself failed. Run by the 06:00 job after ingest, so
the marks are the latest closes. A portfolio without short terms cannot short
and is not checked. A corporate action on a holding that is not modelled (a
spinoff's manual factor) is printed as NOTE, not flagged.
"""

from __future__ import annotations

import asyncio
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.paper_history import latest_prices, load_history, new_york_today  # noqa: E402
from core.portfolio import cash_from, derive_state, short_terms_of, trade_day  # noqa: E402
from db.repositories.market_data import TimescaleMarketDataRepo  # noqa: E402
from db.repositories.portfolios import TimescalePortfolioRepo  # noqa: E402
from db.repositories.rates import TimescaleRateRepo  # noqa: E402
from db.session import get_session  # noqa: E402


async def run() -> int:
    flagged = 0
    checked = 0
    async with get_session() as session:
        repo = TimescalePortfolioRepo(session)
        market = TimescaleMarketDataRepo(session)
        rates = TimescaleRateRepo(session)
        for listed in await repo.list_portfolios():
            portfolio = await repo.get_portfolio(listed.name)
            terms = short_terms_of(portfolio.metadata)
            if not portfolio.trades:
                continue
            tickers = sorted({t.ticker for t in portfolio.trades})
            start = cash_from(portfolio.metadata)
            first = min([trade_day(t) for t in portfolio.trades] + ([start] if start else []))
            history = await load_history(
                tickers, first, new_york_today(), market, rates, cash_from=start
            )
            prices, _ = await latest_prices(tickers, market)
            state = derive_state(
                portfolio.trades, portfolio.initial_cash, prices, history=history, terms=terms
            )
            for action in state.unhandled_actions:
                print(
                    f"NOTE {portfolio.name}: {action.ticker} {action.kind} {action.amount} "
                    f"on {action.day} is not modelled"
                )
            if terms is None:
                continue
            checked += 1
            if state.unpriced:
                print(f"UNPRICED {portfolio.name}: {', '.join(state.unpriced)}; margin not checked")
                flagged += 1
            elif state.below_maintenance:
                print(
                    f"BELOW MAINTENANCE {portfolio.name}: equity {state.total_equity:,.2f} "
                    f"against {state.maintenance_required:,.2f} required "
                    f"({terms.maintenance:.0%} of gross {state.gross_exposure:,.2f})"
                )
                flagged += 1
    print(f"Checked {checked} portfolio(s) with short terms; {flagged} flagged.")
    return 1 if flagged else 0


def main() -> int:
    try:
        return asyncio.run(run())
    except Exception:  # noqa: BLE001 — the wrapper needs 2, not a traceback exit 1
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    sys.exit(main())
