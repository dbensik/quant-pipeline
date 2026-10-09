"""
modeling/valuation.py on Apple's real facts: the inputs a DCF gets, where
each came from, and the two price bases. SVB (a bank) is refused.
"""

import gzip
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from core.fundamentals import parse_company_facts
from core.models import OHLCV, Asset, MarketDataRecord, Timestamp
from modeling.dcf import Beta
from modeling.statements import Statements
from modeling.valuation import Refusal, beta_from_bars, build_inputs, price_as_traded

FIX = Path(__file__).resolve().parents[1] / "fixtures"
BETA = Beta(1.05, 0.05, 756)


def statements(name, splits=()):
    doc = json.loads(gzip.open(FIX / name).read())
    return Statements(parse_company_facts(doc, datetime(2026, 10, 8, tzinfo=timezone.utc)), splits)


@pytest.fixture(scope="module")
def aapl():
    return statements("companyfacts_aapl_2026-10-08.json.gz", [(date(2020, 8, 31), 4.0)])


def test_financials_are_refused():
    out = build_inputs(statements("companyfacts_sivb_2026-10-08.json.gz"), date(2022, 6, 1), 400.0, 0.01, BETA, sector="Financials")
    assert isinstance(out, Refusal) and "banks" in out.reason


def test_cash_flow_is_trailing_twelve_months_from_year_to_date_filings(aapl):
    v = build_inputs(aapl, date(2026, 10, 8), 340.42, 0.0412, BETA)
    cfo, src = v.lines["cfo"]
    # FY2025 111,482 + 9M FY2026 116,996 - 9M FY2025 81,754 (millions)
    assert src == "TTM to 2026-06-27"
    assert cfo == pytest.approx((111_482 + 116_996 - 81_754) * 1e6)


def test_stale_interest_is_not_used_and_the_estimate_is_labelled(aapl):
    # Apple's last tagged interest expense ends 2023-09-30: three years stale.
    v = build_inputs(aapl, date(2026, 10, 8), 340.42, 0.0412, BETA)
    assert v.discount.cost_of_debt_estimated
    assert v.lines["after_tax_interest"][1].startswith("estimated")
    # Two years earlier the filer's own interest is fresh and is used.
    w = build_inputs(aapl, date(2023, 11, 10), 180.0, 0.05, BETA)
    assert not w.discount.cost_of_debt_estimated


def test_sbc_is_subtracted_and_shown(aapl):
    v = build_inputs(aapl, date(2026, 10, 8), 340.42, 0.0412, BETA)
    L = {k: val for k, (val, _) in v.lines.items()}
    assert L["base_fcf"] == pytest.approx(L["cfo"] - L["capex"] + L["after_tax_interest"] - L["share_based_comp"])
    assert "subtracted" in v.lines["share_based_comp"][1]


def test_net_debt_counts_noncurrent_securities(aapl):
    v = build_inputs(aapl, date(2025, 11, 1), 270.0, 0.04, BETA)
    # 2025-09-27: debt 90,678 + CP 7,979 - cash 35,934 - current 18,763 - noncurrent 77,723
    assert v.lines["net_debt"][0] == pytest.approx((90_678 + 7_979 - 35_934 - 18_763 - 77_723) * 1e6)


def test_market_cap_uses_the_price_as_traded_basis(aapl):
    # Before the 2020-08-31 4:1 split Apple traded at $459.63 with 4.276B shares.
    v = build_inputs(aapl, date(2020, 8, 14), 459.63, 0.0009, BETA)
    assert v.dcf.shares == pytest.approx(4_275_634_000)
    assert v.market_cap == pytest.approx(459.63 * 4_275_634_000)
    assert 1.9e12 < v.market_cap < 2.0e12  # a split-adjusted price would give ~0.49T


class BarsRepo:
    """Records the `adjust` each fetch asked for; SPY lacks one AAPL session."""

    def __init__(self):
        self.calls = []
        self.data = {}
        for sym, scale in (("AAPL", 2.0), ("SPY", 1.0)):
            rows = []
            for i in range(900):
                d = datetime.fromordinal(date(2023, 1, 2).toordinal() + i).replace(tzinfo=timezone.utc)
                if sym == "SPY" and i == 850:  # 2025-05-01: inside the beta window
                    continue
                px = 100 * scale * (1 + 0.01 * ((i * 7919) % 13 - 6) / 6) ** (1 if sym == "SPY" else 1.5)
                rows.append(MarketDataRecord(asset=Asset(symbol=sym, asset_class="etf", source="yfinance"),
                                             ohlcv=OHLCV(open=px, high=px, low=px, close=px, volume=1.0, timestamp=Timestamp(utc=d))))
            self.data[sym] = rows

    async def fetch_range(self, symbol, asset_class, start, end, source=None, adjust="total"):
        self.calls.append((symbol, adjust))
        return [r for r in self.data[symbol] if start <= r.ohlcv.timestamp.utc <= end]


@pytest.mark.asyncio
async def test_price_is_fetched_as_traded_and_beta_on_total_returns_joined_by_date():
    repo = BarsRepo()
    day, px = await price_as_traded(repo, "AAPL", date(2025, 6, 1))
    assert repo.calls[-1] == ("AAPL", "none") and day <= date(2025, 6, 1)
    b = await beta_from_bars(repo, "AAPL", date(2025, 6, 1), window=200)
    assert ("AAPL", "total") in repo.calls and ("SPY", "total") in repo.calls
    assert b.observations == 200
    # Joined on date: the session SPY lacks is dropped from both series, so the
    # 200 returns span 201 COMMON sessions reaching back past the gap.


def test_interest_add_back_is_after_tax(aapl):
    # Two years ago Apple's own interest is fresh: TTM to 2023-09-30 = 3,933 M.
    v = build_inputs(aapl, date(2023, 11, 10), 180.0, 0.05, BETA)
    tax = v.lines["tax_rate"][0]
    assert v.lines["after_tax_interest"][1] == "TTM to 2023-09-30"
    assert v.lines["after_tax_interest"][0] == pytest.approx(3_933e6 * (1 - tax))
