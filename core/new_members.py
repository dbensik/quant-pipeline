"""
core/new_members.py

Which index members to register, and what to flag instead (decision 44,
research/new-index-members-2026-10-19.md). Pure: no network, no database, so
every rule here is testable. scripts/register_new_members.py does the I/O.

WHY NOT REGISTER EVERY UNKNOWN TICKER
    Of the 30 S&P members registered by hand on 2026-10-19, one was a RENAME
    (SKYD is PSKY: same CIK, identical prices), two carried PLACEHOLDER
    history (FERG back to 2010 on an untraded line, P one bar before its IPO)
    and one a shell company's history (VRT's SPAC). Registering the rename
    would have made a second row for one instrument, so a joiner whose CIK a
    registered asset already carries is flagged for a human to re-key. A
    placeholder prefix is flagged with a suggested floor, never trimmed:
    deleting bars is a human decision.

    Crypto is never registered here. A ticker is not an identifier: 22 coins
    once held another token's history. Each needs the identity audit first.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from core.membership_reconstruction import todays_symbol

#: Bars at the start of a new series inspected for placeholder history. A
#: zero-volume bar in this stretch flags the series; one later in a long
#: history is an ordinary halt (IBKR has two in 2019).
PLACEHOLDER_LOOKBACK_BARS = 504


def registry_form(symbol: str) -> str:
    """A listed share class in Yahoo's form: BRK.B -> BRK-B."""
    return symbol.replace(".", "-")


@dataclass
class RegistrationPlan:
    #: (symbol, CIK): register as an equity and backfill.
    register: List[Tuple[str, int]] = field(default_factory=list)
    #: (symbol, CIK, registered symbols carrying that CIK): probably a rename.
    rename_candidates: List[Tuple[str, int, List[str]]] = field(default_factory=list)
    #: Equity joiners with no CIK in SEC's ticker map: cannot be guarded.
    no_cik: List[str] = field(default_factory=list)
    #: Unregistered crypto members: reported, never registered.
    crypto_unregistered: List[str] = field(default_factory=list)
    #: symbol -> the index it was found in, for the asset's metadata.
    index_of: Dict[str, str] = field(default_factory=dict)


def plan_registrations(
    members: Dict[str, Iterable[str]],
    index_classes: Dict[str, str],
    registered: Set[str],
    ticker_ciks: Dict[str, int],
    registered_ciks: Dict[int, List[str]],
    renames: Dict[str, Tuple[str, Optional[date]]],
    today: date,
) -> RegistrationPlan:
    """
    members:         index -> current members, as the snapshot lists them.
    index_classes:   index -> 'equity' or 'crypto'.
    registered:      every registered symbol, delisted included.
    ticker_ciks:     SEC's ticker map, in registry form.
    registered_ciks: CIK -> registered symbols carrying it (own or predecessor).
    renames:         the dated rename map (decision 42).
    """
    plan = RegistrationPlan()
    seen: Set[str] = set()
    for index, listed in members.items():
        asset_class = index_classes[index]
        for raw in listed:
            symbol = registry_form(todays_symbol(raw, renames, today))
            if symbol in registered or symbol in seen:
                continue
            seen.add(symbol)
            if asset_class == "crypto":
                plan.crypto_unregistered.append(symbol)
                continue
            plan.index_of[symbol] = index
            cik = ticker_ciks.get(symbol)
            if cik is None:
                plan.no_cik.append(symbol)
            elif registered_ciks.get(cik):
                plan.rename_candidates.append((symbol, cik, sorted(registered_ciks[cik])))
            else:
                plan.register.append((symbol, cik))
    plan.crypto_unregistered.sort()
    return plan


def placeholder_floor(
    dates: Sequence[date], volumes: Sequence[float], lookback: int = PLACEHOLDER_LOOKBACK_BARS
) -> Optional[date]:
    """
    A suggested first valid date when a new series opens on placeholder bars,
    else None.

    Flagged when any of its first `lookback` bars traded zero volume. The
    suggestion is the bar after the LAST zero-volume bar, which is where
    FERG's untraded line ends (its 2021-03-08 NYSE listing) and P's
    pre-IPO bar ends (2015-10-07). It is a suggestion for a human.
    """
    if not dates or len(dates) != len(volumes):
        return None
    head = range(min(lookback, len(volumes)))
    if not any(volumes[i] == 0 for i in head):
        return None
    last_zero = max(i for i, v in enumerate(volumes) if v == 0)
    return dates[last_zero + 1] if last_zero + 1 < len(dates) else None


def report(
    plan: RegistrationPlan,
    registered: Dict[str, Tuple[int, Optional[date]]],
    placeholders: Dict[str, date],
    empty: List[str],
) -> Tuple[List[str], int]:
    """
    The run's log lines and exit code: 0 clean, 1 when anything needs a human.

    registered:   symbol -> (bars written, first bar) for what was registered.
    placeholders: symbol -> suggested floor (placeholder_floor).
    empty:        registered symbols the backfill found no bars for.

    Unregistered crypto never sets the exit code: dozens are listed every day
    by design, and a flag that always fires is a flag nobody reads.
    """
    lines: List[str] = []
    for symbol, (bars, first) in sorted(registered.items()):
        lines.append(f"REGISTERED {symbol} ({plan.index_of.get(symbol)}): {bars} bars from {first}")
    for symbol, cik, holders in plan.rename_candidates:
        lines.append(
            f"RENAME? {symbol}: CIK {cik} is already registered as {', '.join(holders)}. "
            "Not registered: re-key that row if it is the same instrument."
        )
    for symbol in plan.no_cik:
        lines.append(f"NO CIK {symbol}: not in SEC's ticker map, so not registered.")
    for symbol, floor in sorted(placeholders.items()):
        lines.append(
            f"PLACEHOLDER {symbol}: zero-volume bars at the start of its history; "
            f"suggested history_valid_from {floor}. Nothing trimmed."
        )
    for symbol in empty:
        lines.append(f"EMPTY {symbol}: registered, but the backfill found no bars.")
    if plan.crypto_unregistered:
        lines.append(
            f"crypto: {len(plan.crypto_unregistered)} index members unregistered, never "
            f"registered automatically (identity audit first): "
            + ", ".join(plan.crypto_unregistered)
        )
    needs_human = plan.rename_candidates or plan.no_cik or placeholders or empty
    return lines, 1 if needs_human else 0
