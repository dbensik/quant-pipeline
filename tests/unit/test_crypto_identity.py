"""
core/crypto_identity.py — is the ticker the coin we meant?

No network: both quotes are injected. Every case below is a REAL row from the
2026-09-17 audit (`research/crypto-identity-audit-2026-09-17.json`), because
the failure being guarded is one that hand-reasoning got wrong — the first
pass of that audit filed LEO-USD as a substitution when "UNUS SED LEO" is
simply LEO Token's full name.
"""

from core.crypto_identity import (
    CoinReference,
    Identity,
    ProviderQuote,
    names_agree,
    normalize_name,
    price_gap,
    verify_identity,
)


def check(symbol, ref_name, ref_price, prov_name, prov_price):
    return verify_identity(
        symbol,
        CoinReference("id", symbol, ref_name, ref_price),
        ProviderQuote(prov_name, prov_price),
    )


# ---------------------------------------------------------------------------
# WRONG_ASSET — a decisive price gap
# ---------------------------------------------------------------------------

def test_uniswap_is_not_unicorn_token():
    """Measured 49,065x. The headline case: a top-50 coin, wholly wrong."""
    c = check("UNI-USD", "Uniswap", 7.24, "UNICORN Token", 0.0001475)
    assert c.status is Identity.WRONG_ASSET
    assert not c.safe_to_ingest
    assert c.price_gap > 1000


def test_the_same_name_can_still_be_a_different_coin():
    """
    JUP: CoinGecko "Jupiter", Yahoo "Jupiter", 815x apart. Names agreeing must
    NOT rescue a decisive price gap — price is checked first and alone.
    """
    c = check("JUP-USD", "Jupiter", 0.2417, "Jupiter", 0.0002966)
    assert c.status is Identity.WRONG_ASSET
    assert names_agree("Jupiter", "Jupiter")


def test_the_narrowest_measured_substitution_still_reads_wrong():
    """PEPE vs PEPEGOLD at 1.697x — the smallest real gap in the audit."""
    assert check("PEPE-USD", "Pepe", 1e-5, "PEPEGOLD", 5.9e-6).status is (
        Identity.WRONG_ASSET
    )


# ---------------------------------------------------------------------------
# MATCH — including the alias that fooled a human
# ---------------------------------------------------------------------------

def test_an_alias_is_the_same_asset():
    """
    LEO Token IS "UNUS SED LEO". The first audit pass called this a
    substitution because neither string contains the other verbatim; dropping
    the noise word "token" makes "leo" a substring of "unus sed leo".
    """
    c = check("LEO-USD", "LEO Token", 9.51, "UNUS SED LEO", 9.51)
    assert c.status is Identity.MATCH
    assert c.safe_to_ingest


def test_a_plain_match_is_a_match():
    assert check("BTC-USD", "Bitcoin", 76677.0, "Bitcoin", 76677.06).status is (
        Identity.MATCH
    )


def test_a_small_price_move_between_quotes_is_tolerated():
    """The two prices are fetched seconds apart; they will not be equal."""
    assert check("ETH-USD", "Ethereum", 2453.7, "Ethereum", 2461.2).status is (
        Identity.MATCH
    )


# ---------------------------------------------------------------------------
# SUSPECT — price cannot settle a stablecoin
# ---------------------------------------------------------------------------

def test_a_stablecoin_with_a_wrong_name_is_suspect_not_wrong():
    """
    Every stablecoin is $1, so BlackRock's BUIDL and DFOhub's BUIDL have a gap
    of ~1.0 while being unrelated. A legitimate alias looks identical from
    here, so the only honest verdict is "a human must look".
    """
    c = check("BUIDL-USD", "BlackRock USD Institutional", 1.0, "DFOhub", 0.708)
    assert c.status is Identity.SUSPECT
    assert not c.safe_to_ingest  # a live question, not a clearance


def test_suspect_is_reserved_for_agreeing_prices():
    """A name mismatch with a real gap is WRONG_ASSET, not SUSPECT."""
    assert check("SUI-USD", "Sui", 1.42, "Salmonation", 0.00056).status is (
        Identity.WRONG_ASSET
    )


# ---------------------------------------------------------------------------
# UNVERIFIABLE — absence of evidence
# ---------------------------------------------------------------------------

def test_no_reference_is_not_a_failure():
    """
    26 of 99 sit outside CoinGecko's top 200 — mostly liquid-staking
    derivatives. They must not be condemned as substitutions, and must not be
    quietly promoted to MATCH: recorded as unverified, and allowed through.
    """
    c = verify_identity("RETH-USD", None, ProviderQuote("Rocket Pool ETH", 2700.0))
    assert c.status is Identity.UNVERIFIABLE
    assert c.safe_to_ingest
    assert c.reference_name is None


def test_a_missing_price_cannot_convict():
    """PUMP-USD: the provider serves no quote at all."""
    c = check("PUMP-USD", "Pump.fun", 0.004, None, None)
    assert c.status is Identity.UNVERIFIABLE
    assert c.price_gap is None


def test_a_missing_price_with_a_matching_name_is_still_a_match():
    c = check("WETH-USD", "WETH", None, "WETH", None)
    assert c.status is Identity.MATCH


# ---------------------------------------------------------------------------
# the primitives
# ---------------------------------------------------------------------------

def test_noise_words_do_not_distinguish_coins():
    assert normalize_name("LEO Token") == "leo"
    assert normalize_name("Pi Network") == "pi"
    assert normalize_name("  POL (ex-MATIC) ") == "pol ex matic"


def test_names_agree_needs_something_to_compare():
    assert not names_agree(None, "Bitcoin")
    assert not names_agree("Bitcoin", None)
    assert not names_agree("", "")


def test_price_gap_is_orientation_independent():
    assert price_gap(10.0, 1.0) == price_gap(1.0, 10.0) == 10.0
    assert price_gap(0, 5.0) is None
    assert price_gap(None, 5.0) is None


def test_describe_names_the_verdict():
    text = check("UNI-USD", "Uniswap", 7.24, "UNICORN Token", 0.0001475).describe()
    assert "WRONG_ASSET" in text and "UNICORN" in text
