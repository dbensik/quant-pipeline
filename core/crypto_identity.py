"""
core/crypto_identity.py
Is the ticker we asked for actually the coin we meant?

THE PROBLEM, precisely. `data_pipeline/dynamic_universe.py` asks CoinGecko for
the top 100 coins by market cap, keeps only the SYMBOL, discards CoinGecko's
stable `id` and `name`, appends "-USD", and hands the result to Yahoo. A ticker
is not an identifier. Yahoo's `MNT-USD` is a micro-cap called MINTY; Mantle is
somewhere else entirely.

Audited 2026-09-17: 22 of 99 crypto assets held a DIFFERENT token's entire
price history — 27,076 bars. Not obscure names either:

    UNI-USD    Uniswap      -> UNICORN Token       49,065x price gap
    APT-USD    Aptos        -> Apricot Finance      4,710x
    MNT-USD    Mantle       -> MINTY                2,942x
    SUI-USD    Sui          -> Salmonation          2,534x
    ARB-USD    Arbitrum     -> ARbit                  316x
    SPX-USD    SPX6900      -> SPEXY                 1.4e11x

This is the same class of error as inferring a delisting from an empty fetch
(see `core/corporate_actions.looks_unresolved`): a string lookup mistaken for
an identity. It is the third time this project has been bitten by it.

WHY TWO SIGNALS, AND WHY THE VERDICT HAS FOUR VALUES

    Price alone cannot settle a stablecoin. Every stablecoin is $1, so
    BlackRock's BUIDL and DFOhub's BUIDL have a price gap of ~1.0 while being
    unrelated. Name alone cannot settle an alias: CoinGecko's "Jupiter" and
    Yahoo's "Jupiter" are DIFFERENT tokens 815x apart, while CoinGecko's "LEO
    Token" and Yahoo's "UNUS SED LEO" are the SAME asset under its full name.

    So a name mismatch at a ~1x price gap is genuinely ambiguous — a legitimate
    alias and a substitution look identical — and this module says SUSPECT
    rather than guessing. Guessing is what produced the bug.

Pure of the network and of the database: callers fetch the quotes and this
decides. `scripts/audit_crypto_identity.py` wires it up.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from config.settings import CRYPTO_PRICE_GAP_TOLERANCE

#: Keys written into `assets.metadata` (jsonb, so no migration). Named here
#: once because the re-ingest of the wrong assets, and any later audit, have to
#: read exactly these — three files guessing at inline strings is how the
#: original bug got its foothold.
META_COINGECKO_ID = "coingecko_id"
META_VERIFIED_NAME = "verified_name"
META_IDENTITY_STATUS = "identity_status"
META_IDENTITY_CHECKED_AT = "identity_checked_at"


class Identity(str, Enum):
    """What we are entitled to say about a symbol."""

    #: Name and price both agree with the reference. Safe to ingest.
    MATCH = "match"
    #: The provider is serving a different asset. Its history is not repairable
    #: by refetching — it belongs to another coin.
    WRONG_ASSET = "wrong_asset"
    #: Prices agree but the names do not. An alias or a substitution; they look
    #: the same from here. Needs a human, must not be auto-registered as either.
    SUSPECT = "suspect"
    #: No reference to check against — the coin is outside the reference set
    #: (liquid-staking derivatives sit below CoinGecko's top 200). NOT a
    #: failure: register it, but record that it was never verified.
    UNVERIFIABLE = "unverifiable"


@dataclass(frozen=True)
class CoinReference:
    """Ground truth: what CoinGecko says this symbol is."""

    coingecko_id: str
    symbol: str
    name: str
    price: Optional[float] = None


@dataclass(frozen=True)
class ProviderQuote:
    """What the price provider serves under the ticker we built."""

    name: Optional[str] = None
    price: Optional[float] = None


@dataclass(frozen=True)
class IdentityCheck:
    symbol: str
    status: Identity
    reference_name: Optional[str] = None
    provider_name: Optional[str] = None
    price_gap: Optional[float] = None

    @property
    def safe_to_ingest(self) -> bool:
        """UNVERIFIABLE is allowed through; it is absence of evidence, not
        evidence of a substitution. SUSPECT is not — it is a live question."""
        return self.status in (Identity.MATCH, Identity.UNVERIFIABLE)

    def describe(self) -> str:
        gap = f"{self.price_gap:,.4g}x" if self.price_gap is not None else "n/a"
        return (
            f"{self.symbol}: {self.status.value.upper()} — reference "
            f"{self.reference_name!r}, provider {self.provider_name!r}, "
            f"price gap {gap}"
        )


#: Words that carry no identifying information, so "LEO Token" and "LEO" are
#: the same claim. Kept small on purpose: every word removed here is a word
#: that can no longer distinguish two coins.
_NOISE = {"token", "coin", "usd", "protocol", "network", "finance", "the"}


def normalize_name(name: Optional[str]) -> str:
    """Lowercase alphanumeric words, minus noise words, joined."""
    if not name:
        return ""
    cleaned = "".join(c if c.isalnum() else " " for c in name.lower())
    return " ".join(w for w in cleaned.split() if w and w not in _NOISE)


def names_agree(reference: Optional[str], provider: Optional[str]) -> bool:
    """
    Whether two names make the same claim.

    Containment, not equality, so "LEO" matches "UNUS SED LEO" and "Pepe"
    matches "Pepe". It is deliberately generous: a name mismatch only ever
    downgrades a verdict to SUSPECT (a human looks), while a name FALSE
    match combined with a large price gap still reads WRONG_ASSET, because
    price is checked first and independently.
    """
    a, b = normalize_name(reference), normalize_name(provider)
    if not a or not b:
        return False
    return a == b or a in b or b in a


def price_gap(reference: Optional[float], provider: Optional[float]) -> Optional[float]:
    """Ratio of the larger price to the smaller. None when either is missing."""
    if not reference or not provider or reference <= 0 or provider <= 0:
        return None
    hi, lo = max(reference, provider), min(reference, provider)
    return hi / lo


def verify_identity(
    symbol: str,
    reference: Optional[CoinReference],
    quote: ProviderQuote,
    tolerance: float = CRYPTO_PRICE_GAP_TOLERANCE,
) -> IdentityCheck:
    """
    Decide what we may claim about `symbol`.

    Price is checked FIRST and on its own, because a large gap is decisive
    whatever the names say — "Jupiter" vs "Jupiter" at 815x is two coins. Only
    when prices agree does the name get a vote, and there a mismatch buys
    SUSPECT rather than a verdict.
    """
    if reference is None:
        return IdentityCheck(
            symbol=symbol,
            status=Identity.UNVERIFIABLE,
            provider_name=quote.name,
        )

    gap = price_gap(reference.price, quote.price)
    agree = names_agree(reference.name, quote.name)

    if gap is not None and gap >= tolerance:
        status = Identity.WRONG_ASSET
    elif gap is None:
        # No price to compare. A name match is still worth something; a
        # mismatch with nothing to corroborate it is not a verdict.
        status = Identity.MATCH if agree else Identity.UNVERIFIABLE
    elif agree:
        status = Identity.MATCH
    else:
        status = Identity.SUSPECT

    return IdentityCheck(
        symbol=symbol,
        status=status,
        reference_name=reference.name,
        provider_name=quote.name,
        price_gap=gap,
    )


def recorded_status(metadata: Optional[dict]) -> Optional[Identity]:
    """
    The verdict stored on an asset row, or None if it was never checked.

    None is the normal case for everything that is not crypto — no equity has
    ever been through this audit — so callers must treat it as "no opinion",
    never as a failure.
    """
    if not metadata:
        return None
    raw = metadata.get(META_IDENTITY_STATUS)
    if not raw:
        return None
    try:
        return Identity(raw)
    except ValueError:
        # An unrecognised value is someone else's data, not a verdict.
        return None


def metadata_allows_ingest(metadata: Optional[dict]) -> bool:
    """
    Whether a recorded verdict permits fetching this symbol.

    Absence of a verdict permits: 516 equities and 11 ETFs carry no identity
    metadata, and a gate that treated "unchecked" as "unsafe" would stop the
    entire daily run the day it shipped.

    Only a RECORDED wrong_asset or suspect blocks. unverifiable passes — it is
    absence of evidence (a coin below the reference set), not evidence of a
    substitution.
    """
    status = recorded_status(metadata)
    if status is None:
        return True
    return IdentityCheck(symbol="", status=status).safe_to_ingest
