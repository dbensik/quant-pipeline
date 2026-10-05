"""
scripts/reconstruct_sp500_membership.py

Load month-end S&P 500 member lists from past revisions of the Wikipedia page
into `universe_membership_reconstructed`.

    python scripts/reconstruct_sp500_membership.py --dry-run   # fetch, judge, write nothing
    python scripts/reconstruct_sp500_membership.py             # load months not yet stored
    python scripts/reconstruct_sp500_membership.py --archive research/x.csv

WHAT IT IS FOR
    The daily snapshot only knows membership from 2026-08-09. This fills in
    2014-12 to the month before that from the page's own history, into a
    separate table that says "reconstructed" in its name. Method, validation
    and limits: research/sp500-membership-reconstruction-2026-10-04.md.

WHAT IT DOES NOT DO
    Touch `universe_membership` or `universe_snapshots` — those are
    observations. Rewrite a month already stored: a stored month is skipped,
    so a re-run only adds what is missing. Make anything read the table.

A month whose revision fails `judge` is retried against the nine revisions
before it. A month with no usable revision is reported and left out, and the
run exits 1: a missing month is visible, a wrong one is not.

--archive also writes every accepted list to a CSV. The changes table this
work first relied on was deleted from the live page in 2026; a local copy is
the hedge against the same happening to something else.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import logging
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import select  # noqa: E402
from sqlalchemy.dialects.postgresql import insert  # noqa: E402

from config.settings import (  # noqa: E402
    SP500_EXPECTED_RANGE,
    SP500_RECONSTRUCTION_MAX_MONTHLY_CHANGE,
    SP500_RECONSTRUCTION_START,
    URL_WIKIPEDIA_API,
    WIKIPEDIA_REQUEST_DELAY_SECONDS,
    WIKIPEDIA_SP500_PAGE_TITLE,
)
from core.membership_reconstruction import (  # noqa: E402
    judge,
    month_ends,
    parse_constituents,
)
from db.models import ReconstructedMembershipORM, UniverseSnapshotORM  # noqa: E402
from db.session import get_session  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("reconstruct-sp500")

INDEX_NAME = "sp500"
SOURCE = "wikipedia_revision"
#: Revisions tried per month before giving up on it.
REVISIONS_PER_MONTH = 10
HEADERS = {
    "User-Agent": (
        "quant-pipeline/1.0 (research tool; "
        "https://github.com/dbensik/quant-pipeline)"
    )
}


class Wikipedia:
    """The two MediaWiki calls this needs, paced and retried."""

    def __init__(self, delay: float = WIKIPEDIA_REQUEST_DELAY_SECONDS):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.delay = delay

    def _get(self, params: dict) -> dict:
        for attempt in range(1, 6):
            response = self.session.get(URL_WIKIPEDIA_API, params=params, timeout=60)
            time.sleep(self.delay)
            # A refused request comes back as an HTML error page, sometimes
            # with status 200, so the body is checked as well as the status.
            if response.status_code == 200 and response.text.startswith("{"):
                return response.json()
            time.sleep(5 * attempt)
        raise RuntimeError(
            f"MediaWiki refused the request five times "
            f"(last status {response.status_code})"
        )

    def revisions_at(self, day: date, limit: int) -> list[tuple[int, datetime]]:
        """The `limit` newest revisions saved on or before the end of `day`."""
        data = self._get(
            {
                "action": "query",
                "prop": "revisions",
                "titles": WIKIPEDIA_SP500_PAGE_TITLE,
                "rvlimit": limit,
                "rvdir": "older",
                "rvstart": f"{day.isoformat()}T23:59:59Z",
                "rvprop": "ids|timestamp",
                "format": "json",
            }
        )
        page = next(iter(data["query"]["pages"].values()))
        return [
            (
                int(revision["revid"]),
                datetime.fromisoformat(revision["timestamp"].replace("Z", "+00:00")),
            )
            for revision in page.get("revisions", [])
        ]

    def html(self, revision_id: int) -> str:
        data = self._get(
            {"action": "parse", "oldid": revision_id, "prop": "text", "format": "json"}
        )
        return data["parse"]["text"]["*"]


def reconstruct_month(
    wiki, day: date, previous: Optional[list[str]]
) -> Optional[tuple[int, datetime, list[str]]]:
    """
    (revision id, saved at, symbols) for the newest usable revision at `day`,
    or None when none of the last REVISIONS_PER_MONTH passes `judge`.
    """
    for revision_id, saved_at in wiki.revisions_at(day, REVISIONS_PER_MONTH):
        symbols = parse_constituents(wiki.html(revision_id))
        reason = judge(
            symbols,
            previous,
            SP500_EXPECTED_RANGE,
            SP500_RECONSTRUCTION_MAX_MONTHLY_CHANGE,
        )
        if reason is None:
            return revision_id, saved_at, symbols
        logger.warning(
            "%s: revision %d rejected (%s); trying the one before it.",
            day, revision_id, reason,
        )
    return None


async def first_observed(session) -> Optional[date]:
    """The date of the first real snapshot — reconstruction stops before it."""
    taken_at = (
        await session.execute(
            select(UniverseSnapshotORM.taken_at)
            .where(UniverseSnapshotORM.index_name == INDEX_NAME)
            .order_by(UniverseSnapshotORM.taken_at)
            .limit(1)
        )
    ).scalar_one_or_none()
    return taken_at.date() if taken_at else None


async def stored_lists(session) -> dict[date, list[str]]:
    rows = (
        await session.execute(
            select(
                ReconstructedMembershipORM.as_of, ReconstructedMembershipORM.symbol
            ).where(ReconstructedMembershipORM.index_name == INDEX_NAME)
        )
    ).all()
    lists: dict[date, list[str]] = {}
    for as_of, symbol in rows:
        lists.setdefault(as_of, []).append(symbol)
    return lists


async def run(dry_run: bool, archive: Optional[Path], wiki=None) -> int:
    wiki = wiki or Wikipedia()
    start = date.fromisoformat(SP500_RECONSTRUCTION_START)
    accepted: list[tuple[date, int, datetime, list[str]]] = []
    missing: list[date] = []

    async with get_session() as session:
        # Observed snapshots take over from their first day; a reconstructed
        # list dated after that would be a second, weaker answer for a date
        # that already has a real one.
        end = await first_observed(session) or date.today()
        stored = await stored_lists(session)
        previous: Optional[list[str]] = None

        for day in month_ends(start, end):
            if day >= end:
                break
            if day in stored:
                previous = stored[day]
                continue

            found = await asyncio.to_thread(reconstruct_month, wiki, day, previous)
            if found is None:
                # `previous` is deliberately left alone: the next month is
                # judged against the last list that was actually accepted.
                logger.error("%s: no usable revision; month left out.", day)
                missing.append(day)
                continue

            revision_id, saved_at, symbols = found
            accepted.append((day, revision_id, saved_at, symbols))
            change = len(set(symbols) ^ set(previous)) if previous else 0
            logger.info(
                "%s: %d members from revision %d (%s), %d changed",
                day, len(symbols), revision_id, saved_at.date(), change,
            )
            previous = symbols

            if not dry_run:
                await session.execute(
                    insert(ReconstructedMembershipORM)
                    .values(
                        [
                            {
                                "index_name": INDEX_NAME,
                                "as_of": day,
                                "symbol": symbol,
                                "source": SOURCE,
                                "source_revision": revision_id,
                                "revision_at": saved_at,
                            }
                            for symbol in symbols
                        ]
                    )
                    .on_conflict_do_nothing(constraint="uq_reconstructed_member")
                )
                await session.commit()

    if archive and accepted:
        with archive.open("w", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(
                ["as_of", "source_revision", "revision_at", "member_count", "symbols"]
            )
            for day, revision_id, saved_at, symbols in accepted:
                writer.writerow(
                    [day, revision_id, saved_at.isoformat(), len(symbols),
                     " ".join(symbols)]
                )
        logger.info("Archived %d list(s) to %s", len(accepted), archive)

    logger.info(
        "%s %d month(s); %d already stored; %d with no usable revision%s",
        "Would load" if dry_run else "Loaded",
        len(accepted), len(stored), len(missing),
        f": {', '.join(map(str, missing))}" if missing else "",
    )
    return 1 if missing else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reconstruct month-end S&P 500 membership from Wikipedia revisions."
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--archive", type=Path, help="Also write accepted lists to this CSV.")
    args = parser.parse_args()

    try:
        return asyncio.run(run(args.dry_run, args.archive))
    except KeyboardInterrupt:
        return 130
    except Exception:
        logger.exception("Reconstruction failed.")
        return 2


if __name__ == "__main__":
    sys.exit(main())
