"""
core/crypto_class.py

What kind of asset a crypto ticker is, from CoinGecko's own categories
(decision 46, research/crypto-universe-pegged-2026-10-19.md).

    crypto      a crypto-risk asset: everything not below
    stablecoin  CoinGecko lists it under "Stablecoins"
    tokenized   "Tokenized Assets" or "Tokenized Gold": an off-chain asset on a
                chain (T-bill and money funds, gold, a VC fund, loans)

Stablecoins and tokenized assets are PEGGED: kept in the index record, left
out of crypto screens by default. Their volatility was 0.4-5.5% a year
(gold 29%) against 24-293% for the top 100's real crypto, so a low-volatility
screen ranked every dollar first.

A stablecoin is any category naming one ("USD Stablecoin", "Yield-Bearing
Stablecoin", "Bridged Stablecoin", "Bridged USDT"), not only "Stablecoins":
staked and bridged dollars carry only the narrower tags.

Classified on categories, never on names: "Stable" (STABLE) is a layer-1
chain at 120% volatility. "Real World Assets (RWA)" alone is NOT pegged:
ONDO, ALGO, XLM, QNT and XDC carry it and trade at 70-140%.
"""

from __future__ import annotations

from typing import Iterable

CRYPTO = "crypto"
STABLECOIN = "stablecoin"
TOKENIZED = "tokenized"
PEGGED = frozenset({STABLECOIN, TOKENIZED})

#: metadata key on a crypto asset row.
META_CRYPTO_CLASS = "crypto_class"

_TOKENIZED_CATEGORIES = {"Tokenized Assets", "Tokenized Gold"}
#: Carries "stablecoin" without being one: WLFI and SKY issue stablecoins.
_NOT_A_STABLECOIN = {"stablecoin issuer"}


def _is_stablecoin_category(category: str) -> bool:
    """
    "Stablecoins", "USD Stablecoin", "Yield-Bearing Stablecoin", "Bridged
    Stablecoin", "Bridged USDT"... CoinGecko tags a staked or bridged dollar
    (SUSDE, USDT0, BSC-USD) with one of the narrower names and NOT with
    "Stablecoins": matched on the exact name alone, all three read as crypto.
    """
    name = category.strip().lower()
    if name in _NOT_A_STABLECOIN:
        return False
    return "stablecoin" in name or name.startswith("bridged usd")


def classify(categories: Iterable[str]) -> str:
    """crypto, stablecoin or tokenized, from CoinGecko's category names."""
    found = [c for c in (categories or []) if c]
    if any(_is_stablecoin_category(c) for c in found):
        return STABLECOIN
    if set(found) & _TOKENIZED_CATEGORIES:
        return TOKENIZED
    return CRYPTO
