"""
core.crypto_class (decision 46). Categories as CoinGecko returned them on
2026-10-19, so each case is a real coin.
"""

from core.crypto_class import CRYPTO, STABLECOIN, TOKENIZED, classify


def test_a_plain_stablecoin():
    assert classify(["Stablecoins", "USD Stablecoin", "Solana Ecosystem"]) == STABLECOIN  # USDT


def test_staked_and_bridged_dollars_are_stablecoins_without_the_plain_tag():
    assert classify(["Liquid Staking Tokens", "Yield-Bearing Stablecoin"]) == STABLECOIN  # SUSDE
    assert classify(["Bridged USDT", "Bridged-Tokens", "Bridged Stablecoin"]) == STABLECOIN  # USDT0


def test_tokenized_funds_and_gold():
    assert classify(["Tokenized Assets", "Real World Assets (RWA)"]) == TOKENIZED  # USTB
    assert classify(["Tokenized Assets", "Tokenized Gold"]) == TOKENIZED  # PAXG


def test_a_stablecoin_issuer_is_not_a_stablecoin():
    # WLFI and SKY issue stablecoins and trade at 73% volatility.
    assert classify(["BNB Chain Ecosystem", "Stablecoin Issuer"]) == CRYPTO


def test_real_world_assets_alone_is_not_pegged():
    # ONDO, ALGO, XLM, QNT, XDC: 70-140% volatility.
    assert classify(["Decentralized Finance (DeFi)", "Real World Assets (RWA)"]) == CRYPTO


def test_names_play_no_part():
    # "Stable" (STABLE) is a layer-1 chain at 120% volatility.
    assert classify(["Infrastructure", "Smart Contract Platform", "Layer 1 (L1)"]) == CRYPTO
    assert classify([]) == CRYPTO
