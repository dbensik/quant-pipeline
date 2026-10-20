"""
scripts/run_paper_strategies.py

Trade every paper portfolio's attached strategy over the sessions it has
not traded yet (decision 50, research/paper-trader-2026-10-20.md).

    python scripts/run_paper_strategies.py            # write the fills
    python scripts/run_paper_strategies.py --dry-run  # print them, write nothing

Run by the 06:00 job after ingest, the rate and the fresh-return check, and
BEFORE the paper margin check, so that check sees today's fills. On morning
D the newest bar is D-1: the run fills at D-1's open what D-2's close
decided, and prints what D-1's close asks for at D's open (NEXT lines).
A missed morning is caught up session by session.

Exit 0 when nothing needs a look, 1 when something is flagged (a fill the
trade check refused, a holding that is not what the strategy wants, a basket
symbol's missing bar holding the trader for
PAPER_TRADER_HELD_SESSIONS_FLAGGED sessions, a strategy detached at ruin),
2 if the run itself failed. A portfolio with no strategy is not touched.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.paper_runner import StaleSession, run_strategy  # noqa: E402
from core.paper_trader import binding_of  # noqa: E402
from db.repositories.market_data import TimescaleMarketDataRepo  # noqa: E402
from db.repositories.portfolios import TimescalePortfolioRepo  # noqa: E402
from db.repositories.rates import TimescaleRateRepo  # noqa: E402
from db.session import get_session  # noqa: E402


async def run(dry_run: bool) -> int:
    flagged = 0
    attached = 0
    async with get_session() as session:
        portfolios = TimescalePortfolioRepo(session)
        market = TimescaleMarketDataRepo(session)
        rates = TimescaleRateRepo(session)
        for listed in await portfolios.list_portfolios():
            if binding_of(listed.metadata) is None:
                continue
            attached += 1
            try:
                report = await run_strategy(
                    listed.name, portfolios, market, rates, write=not dry_run
                )
            except StaleSession as exc:
                print(f"SKIPPED {listed.name}: {exc}")
                continue
            for plan in report.sessions:
                for t in plan.fills:
                    print(
                        f"{'WOULD FILL' if dry_run else 'FILLED'} {report.name} {plan.session}: "
                        f"{t.action} {t.quantity:g} {t.ticker} at {t.price:,.4f} "
                        f"(costs {t.costs:,.2f}; {t.notes})"
                    )
            for key, value in sorted(report.next_signals.items()):
                print(f"NEXT {report.name}: {key} {value:+g} at the open after {report.newest_bar}")
            for line in report.flags:
                print(line)
                flagged += 1
            print(
                f"{report.name}: {len(report.sessions)} session(s) "
                f"{'planned' if dry_run else 'traded'}, through {report.last_session}"
            )
    print(f"Ran {attached} attached portfolio(s); {flagged} flag(s).")
    return 1 if flagged else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[1])
    parser.add_argument("--dry-run", action="store_true", help="print the fills, write nothing")
    args = parser.parse_args()
    try:
        return asyncio.run(run(args.dry_run))
    except Exception:  # noqa: BLE001 — the wrapper needs 2, not a traceback exit 1
        traceback.print_exc()
        return 2


if __name__ == "__main__":
    sys.exit(main())
