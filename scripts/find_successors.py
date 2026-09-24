"""
scripts/find_successors.py

Propose successor tickers for symbols the provider has stopped serving.

    python scripts/find_successors.py                  # every flagged/stalled symbol
    python scripts/find_successors.py --symbols AVB EQR
    python scripts/find_successors.py --index sp500

WHAT PROBLEM THIS SOLVES
    `looks_unresolved` can tell you a symbol stopped resolving. It cannot tell
    you WHY, and the two causes need opposite responses: a delisting should
    stay flagged, a RENAME needs the asset row renamed so thirteen months of
    bars do not go missing. On 2026-08-09 three live S&P 500 constituents
    (BK->BNY, FI->FISV, MMC->MRSH) sat marked dead for exactly that reason,
    and finding their successors took a human doing company-name searches.

    This does the search. Candidates come from index membership churn — the
    event that renames a ticker usually also changes the index — and each is
    scored by the constant-ratio test in core/corporate_actions.py.

WHAT IT DELIBERATELY DOES NOT DO
    Write anything. It prints proposals. Renaming an asset row moves every bar
    attached to it, and this project's own history says that is a decision a
    human should make with the evidence in front of them, not a side effect of
    a detection run. The evidence is printed in full, near misses included, so
    the call can actually be made.

    The default sweep is EQUITY-ONLY. Candidates come from index membership
    churn and the indexes snapshotted here are equity indexes, so a crypto
    symbol has nothing to be compared against. The three flagged crypto names
    (IP-USD, PENGU-USD, SPX-USD) are skipped rather than silently scored;
    name them with --symbols if a candidate is known.

    It also does not find mergers. A rename re-keys one instrument and leaves a
    successor series; a stock-for-stock acquisition genuinely ends the target
    and leaves none. Finding nothing for an acquired symbol is the correct
    answer, not a failure — see the module docstring in core/corporate_actions.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import date, timedelta
from typing import Optional
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import func, select  # noqa: E402

from core.corporate_actions import (  # noqa: E402
    MAX_CONTINUOUS_HOLE,
    Membership,
    max_gap,
    index_churn,
    normalize_symbol,
    rank_successors,
)
from db.models import AssetORM, MarketDataORM, UniverseMembershipORM  # noqa: E402
from db.session import get_session  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("find-successors")

#: How far back to pull closes for the comparison. Only the tail is used, but
#: the event date is not known in advance, so the window has to cover it.
LOOKBACK = timedelta(days=400)

#: A symbol with no bar this recent is worth investigating even if nothing has
#: flagged it yet — `looks_unresolved` needs 21 days, and a rename is easiest
#: to confirm while both series still overlap.
STALLED_AFTER = timedelta(days=5)


async def stalled_symbols(session, latest_bar_overall: date) -> list[tuple[str, date]]:
    """Symbols flagged unresolved, or simply not keeping up with the others."""
    rows = (
        await session.execute(
            select(
                AssetORM.symbol,
                AssetORM.delisted_at,
                func.max(MarketDataORM.time).label("last_bar"),
            )
            .join(MarketDataORM, MarketDataORM.asset_id == AssetORM.id)
            .where(AssetORM.asset_class == "equity")
            .group_by(AssetORM.symbol, AssetORM.delisted_at)
        )
    ).all()

    out = []
    for symbol, delisted_at, last_bar in rows:
        if last_bar is None:
            continue
        behind = latest_bar_overall - last_bar.date()
        if delisted_at is not None or behind > STALLED_AFTER:
            out.append((symbol, last_bar.date()))
    return sorted(out)


async def longest_hole(session, symbol: str) -> tuple[int, Optional[date]]:
    """
    Largest run of calendar days with no bar, over the symbol's WHOLE history.

    Deliberately not computed from the comparison window: PARA's 388-day hole
    ends 2026-08-07, which is outside the 400-day lookback, so a windowed
    version sees a tidy continuous series and misses the one fact that matters.
    """
    rows = (
        await session.execute(
            select(MarketDataORM.time)
            .join(AssetORM, MarketDataORM.asset_id == AssetORM.id)
            .where(AssetORM.symbol == symbol)
            .order_by(MarketDataORM.time)
        )
    ).all()
    return max_gap([t.date() for (t,) in rows])


async def flagged_symbols(session) -> set[str]:
    """Symbols already marked unresolved. The provider may still serve bars
    under these keys, but they are not this instrument's bars."""
    rows = await session.execute(
        select(AssetORM.symbol).where(AssetORM.delisted_at.is_not(None))
    )
    return set(rows.scalars())


async def closes_for(session, symbol: str, since: date) -> dict[date, float]:
    rows = (
        await session.execute(
            select(MarketDataORM.time, MarketDataORM.close)
            .join(AssetORM, MarketDataORM.asset_id == AssetORM.id)
            .where(AssetORM.symbol == symbol, MarketDataORM.time >= since)
        )
    ).all()
    return {t.date(): float(c) for t, c in rows if c is not None}


async def membership_rows(session, index_name: str) -> list[Membership]:
    rows = (
        await session.execute(
            select(
                UniverseMembershipORM.symbol,
                UniverseMembershipORM.first_seen,
                UniverseMembershipORM.last_seen,
            ).where(UniverseMembershipORM.index_name == index_name)
        )
    ).all()
    return [Membership(s, f, l) for s, f, l in rows]


def fetch_closes(symbol: str, since: date) -> dict[date, float]:
    """Candidate closes from the provider. The one network call here."""
    import yfinance as yf

    try:
        history = yf.Ticker(symbol).history(start=since.isoformat(), auto_adjust=True)
    except Exception as exc:  # noqa: BLE001 — one bad candidate is not fatal
        logger.warning("Could not fetch %s: %s", symbol, exc)
        return {}
    if history is None or history.empty:
        return {}
    return {i.date(): float(v) for i, v in history["Close"].items()}


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--index", default="sp500", help="index to take churn from")
    parser.add_argument("--symbols", nargs="*", help="investigate only these")
    args = parser.parse_args(argv)

    async with get_session() as session:
        newest = (await session.execute(select(func.max(MarketDataORM.time)))).scalar()
        if newest is None:
            logger.error("No market data stored — nothing to investigate.")
            return 1
        latest = newest.date()
        since = latest - LOOKBACK

        rows = await membership_rows(session, args.index)
        churn = index_churn(rows)
        registry = {
            normalize_symbol(s)
            for s in (
                await session.execute(select(AssetORM.symbol))
            ).scalars()
        }
        #: Names that joined the index, plus any current member we have never
        #: registered. The second half catches a successor that arrived before
        #: snapshots began. Class shares are normalised so BRK.B does not read
        #: as a missing symbol.
        joined = {normalize_symbol(s) for s in churn.joined}
        members = {normalize_symbol(r.symbol) for r in rows}
        candidates = sorted((joined | members) - registry)

        flagged = await flagged_symbols(session)
        targets = args.symbols or [s for s, _ in await stalled_symbols(session, latest)]
        if not targets:
            print("No stalled or flagged symbols. Nothing to investigate.")
            return 0

        print(f"\nIndex: {args.index}   snapshots through {latest}")
        print(f"Left the index:   {', '.join(churn.left) or '(none)'}")
        print(f"Joined the index: {', '.join(churn.joined) or '(none)'}")
        print(f"Candidates to test: {', '.join(candidates) or '(none)'}\n")

        if not candidates:
            print("No candidate successors available — cannot test anything.")
            return 0

        # Fetched once and reused: the candidate set is small, the targets are not.
        candidate_closes = {c: fetch_closes(c, since) for c in candidates}
        candidate_closes = {c: v for c, v in candidate_closes.items() if v}

        left_on = {r.symbol: r.last_seen.date() for r in rows if r.symbol in set(churn.left)}
        renames, orphans, gaps, too_old, unverified, suspect = [], [], [], [], [], []

        for symbol in targets:
            stored = await closes_for(session, symbol, since)
            if not stored:
                # Nothing inside the lookback to compare against. True of the
                # 2025 cohort (ANSS, HES, WBA...), which stopped 13 months ago
                # and was confirmed delisted against primary filings. Reported
                # rather than dropped, so a silent absence is never mistaken
                # for a clean result.
                too_old.append(symbol)
                continue

            # Before blaming a corporate action, rule out the dull explanation.
            # If the provider still serves this ticker with bars NEWER than
            # ours, nothing happened to the company — we just failed to fetch.
            # Measured 2026-09-15: KHC was four days behind and would have been
            # flagged unresolved on 2026-10-02, while yfinance had every bar and
            # the overlap matched ours to 1.0 exactly. That is an ingest gap,
            # and calling it a delisting is the error this whole module exists
            # to stop.
            # NOT asked for a symbol already flagged unresolved. A dead or
            # reassigned ticker often KEEPS returning bars — they just belong to
            # another instrument. Measured 2026-09-17: after AVB's contaminated
            # tail was deleted, the provider still served 2026-08-17..08-24
            # under the AVB key (they are VMRK's prices), so this check called
            # it an ingest gap and advised a re-ingest that would have restored
            # the exact corruption just removed. "Provider is ahead of us" only
            # means "we are behind" for a symbol still trading as itself.
            live = (
                {}
                if symbol in flagged
                else fetch_closes(symbol, max(stored) - timedelta(days=5))
            )
            if live and max(live) > max(stored):
                # "The provider is ahead of us" is only an ingest gap if the
                # series is CONTINUOUS. A long hole followed by a resumption is
                # the shape of a ticker that was reassigned while nobody was
                # fetching: PARA has real bars to 2025-07-15, a 388-day hole,
                # then resumes 2026-08-07 as an unrelated penny stock. The tail
                # cannot settle this — we already ingested the contamination, so
                # stored and live agree perfectly. Only the hole gives it away.
                hole, at = await longest_hole(session, symbol)
                print(f"--- {symbol} (last bar {max(stored)}) ---")
                if hole > MAX_CONTINUOUS_HOLE:
                    suspect.append((symbol, hole, at))
                    print(f"    provider has bars through {max(live)}, BUT this "
                          f"series has a {hole}-day hole ending {at}. Bars after "
                          "a hole that long may belong to a DIFFERENT instrument "
                          "that inherited the ticker. Do not re-ingest blindly — "
                          "compare the pre-hole segment against a candidate.\n")
                else:
                    gaps.append((symbol, max(stored), max(live)))
                    print(f"    provider has bars through {max(live)} — INGEST "
                          "GAP, not a corporate action. Re-ingest this symbol.\n")
                continue
            # Bars a provider serves under a dead ticker after the event are
            # the SUCCESSOR's prices, not this symbol's. Cut at the day it
            # left the index when we know it.
            cutoff = left_on.get(symbol)
            scored = rank_successors(symbol, stored, candidate_closes, before=cutoff)
            hit = next((e for e in scored if e.proposable), None)

            print(f"--- {symbol} (last bar {max(stored)})"
                  + (f", left {args.index} {cutoff}" if cutoff else "") + " ---")
            for evidence in scored[:3]:
                print("    " + evidence.describe())
            if hit:
                renames.append((symbol, hit))
                print(f"    => PROPOSE RENAME {symbol} -> {hit.candidate}")
            elif near := next((e for e in scored if e.same_instrument), None):
                # Matches the statistic but we never learned when the event
                # happened, so the tail was not trimmed — and an untrimmed
                # window full of the successor's prices scores 1.000000/0.0
                # whether the company was renamed or acquired outright.
                unverified.append((symbol, near))
                print(f"    => {near.candidate} matches, but UNVERIFIED: no "
                      "index-exit date, so post-event bars were not trimmed. "
                      "Establish the event date before acting.")
            else:
                orphans.append(symbol)
                print("    => no successor found; consistent with a real delisting")
            print()

        print("=" * 72)
        for symbol, hole, at in suspect:
            print(f"SUSPECT  {symbol}   {hole}-day hole ending {at}; bars after it "
                  "may be another instrument — verify before re-ingesting")
        for symbol, stored_end, live_end in gaps:
            print(f"GAP      {symbol}   stored to {stored_end}, provider to "
                  f"{live_end} — re-ingest, no corporate action")
        for symbol, hit in renames:
            print(f"RENAME   {symbol} -> {hit.candidate}   "
                  f"(ratio {hit.ratio:.6f}, spread {hit.spread:.2e})")
        for symbol, near in unverified:
            print(f"UNVERIFIED {symbol} -> {near.candidate}   "
                  f"(ratio {near.ratio:.6f}, spread {near.spread:.2e}) — no event "
                  "date; an acquisition scores identically. Do not act on this alone")
        for symbol in orphans:
            print(f"NO MATCH {symbol}   leave flagged, or confirm against a filing")
        if too_old:
            print(f"\nNot testable ({len(too_old)}): no bars within "
                  f"{LOOKBACK.days} days, so no series overlaps a candidate — "
                  f"{', '.join(too_old)}")
        if renames:
            print(
                "\nNothing was written. To act on a rename, rename the asset row "
                "so its bars follow it, then re-ingest; confirm against a primary "
                "source (Form 25-NSE / 8-K / exchange notice) first."
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
