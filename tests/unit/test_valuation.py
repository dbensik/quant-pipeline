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


def test_stale_revenue_is_not_a_growth_rate():
    # Revenue known only to FY2020 (APA's case: its current filer's revenue
    # concept is not on the list), valued in 2026: terminal growth, and why.
    from core.fundamentals import Fact
    from modeling.valuation import default_growth

    def rev(year, v):
        return Fact(1, "us-gaap", "Revenues", "USD", date(year, 1, 1), date(year, 12, 31), v, year, "FY", "10-K",
                    date(year + 1, 2, 20), f"a{year}", None, datetime(2026, 1, 1, tzinfo=timezone.utc))
    st = Statements([rev(y, 100.0 * 1.2 ** (y - 2016)) for y in range(2016, 2021)])
    g, why = default_growth(st, date(2021, 6, 1))
    assert g == 0.10 and "clamped" in why  # fresh: a 20% CAGR, capped at decision 15's 10%
    g, why = default_growth(st, date(2026, 10, 8))
    assert g == 0.025 and "stale" in why


def test_missing_capex_refuses_instead_of_counting_as_zero():
    from core.fundamentals import Fact

    def f(concept, v, filed=date(2026, 8, 1)):
        return Fact(1, "us-gaap", concept, "USD", date(2025, 7, 1), date(2026, 6, 30), v, 2026, "FY", "10-K",
                    filed, f"a-{concept}", None, datetime(2026, 1, 1, tzinfo=timezone.utc))
    st = Statements([f("NetCashProvidedByUsedInOperatingActivities", 4.5e9)])
    out = build_inputs(st, date(2026, 10, 8), 45.0, 0.04, BETA)
    assert isinstance(out, Refusal) and "capital expenditure" in out.reason


@pytest.mark.asyncio
async def test_weekly_beta_uses_the_last_close_of_each_week():
    repo = BarsRepo()
    weekly = await beta_from_bars(repo, "AAPL", date(2025, 6, 1), window=50, frequency="weekly")
    assert weekly.observations == 50
    with pytest.raises(ValueError):
        await beta_from_bars(repo, "AAPL", date(2025, 6, 1), frequency="monthly")


def test_a_stale_cover_page_count_falls_back_to_diluted_shares():
    # Fox's real case: the only untagged cover-page count is 1.0 share (2019).
    from core.fundamentals import Fact

    def f(concept, v, start, end, filed, tax="us-gaap"):
        return Fact(1, tax, concept, "shares" if "Share" in concept else "USD", start, end, v, None, "FY", "10-K",
                    filed, f"a-{concept}-{end}", None, datetime(2026, 1, 1, tzinfo=timezone.utc))
    st = Statements([
        f("EntityCommonStockSharesOutstanding", 1.0, None, date(2019, 3, 18), date(2019, 3, 18), "dei"),
        f("WeightedAverageNumberOfDilutedSharesOutstanding", 439e6, date(2025, 7, 1), date(2026, 6, 30), date(2026, 8, 6)),
        f("NetCashProvidedByUsedInOperatingActivities", 3e9, date(2025, 7, 1), date(2026, 6, 30), date(2026, 8, 6)),
        f("PaymentsToAcquirePropertyPlantAndEquipment", 4e8, date(2025, 7, 1), date(2026, 6, 30), date(2026, 8, 6)),
    ])
    v = build_inputs(st, date(2026, 10, 8), 57.0, 0.04, BETA)
    assert v.dcf.shares == 439e6 and v.lines["shares"][1].startswith("latest diluted")
    assert any("stale" in n for n in v.notes)


def test_cash_interest_paid_is_used_as_a_labelled_proxy():
    from core.fundamentals import Fact

    def f(concept, v):
        return Fact(1, "us-gaap", concept, "shares" if "Share" in concept else "USD", date(2025, 7, 1),
                    date(2026, 6, 30), v, None, "FY", "10-K", date(2026, 8, 6), f"a-{concept}", None,
                    datetime(2026, 1, 1, tzinfo=timezone.utc))
    st = Statements([f("NetCashProvidedByUsedInOperatingActivities", 3e9), f("PaymentsToAcquirePropertyPlantAndEquipment", 4e8),
                     f("WeightedAverageNumberOfDilutedSharesOutstanding", 1e8), f("InterestPaidNet", 2e8),
                     Fact(1, "us-gaap", "LongTermDebt", "USD", None, date(2026, 6, 30), 4e9, None, "FY", "10-K",
                          date(2026, 8, 6), "a-debt", None, datetime(2026, 1, 1, tzinfo=timezone.utc))])
    v = build_inputs(st, date(2026, 10, 8), 50.0, 0.04, BETA)
    assert v.discount.cost_of_debt_pre_tax == pytest.approx(2e8 / 4e9)  # the proxy, not risk-free + spread
    assert v.lines["after_tax_interest"][1].startswith("PROXY: cash interest paid")
    assert any("proxy" in n for n in v.notes) and not v.discount.cost_of_debt_estimated


def test_the_share_fallback_takes_the_newest_average_annual_or_quarterly():
    from core.fundamentals import Fact

    def f(concept, v, start, end, filed, fp):
        return Fact(1, "us-gaap", concept, "shares" if "Share" in concept else "USD", start, end, v, None, fp, "10-K",
                    filed, f"a-{concept}-{end}-{start}", None, datetime(2026, 1, 1, tzinfo=timezone.utc))
    st = Statements([
        f("WeightedAverageNumberOfDilutedSharesOutstanding", 443e6, date(2026, 1, 1), date(2026, 3, 31), date(2026, 5, 11), "Q3"),
        f("WeightedAverageNumberOfDilutedSharesOutstanding", 439e6, date(2025, 7, 1), date(2026, 6, 30), date(2026, 8, 6), "FY"),
        f("NetCashProvidedByUsedInOperatingActivities", 3e9, date(2025, 7, 1), date(2026, 6, 30), date(2026, 8, 6), "FY"),
        f("PaymentsToAcquirePropertyPlantAndEquipment", 4e8, date(2025, 7, 1), date(2026, 6, 30), date(2026, 8, 6), "FY"),
    ])
    assert build_inputs(st, date(2026, 10, 8), 57.0, 0.04, BETA).dcf.shares == 439e6


def _flow_and_balance(concept, v, end=date(2026, 6, 30), filed=date(2026, 8, 6), duration=True):
    from core.fundamentals import Fact
    start = date(end.year - 1, end.month, end.day) + (date(2026, 1, 2) - date(2026, 1, 1)) if duration else None
    return Fact(1, "us-gaap", concept, "shares" if "Share" in concept else "USD", start, end, v, None, "FY", "10-K",
                filed, f"a-{concept}-{end}", None, datetime(2026, 1, 1, tzinfo=timezone.utc))


def _operating(interest=2.2e8):
    flows = [_flow_and_balance("NetCashProvidedByUsedInOperatingActivities", 2.3e9),
             _flow_and_balance("PaymentsToAcquirePropertyPlantAndEquipment", 1.4e8),
             _flow_and_balance("WeightedAverageNumberOfDilutedSharesOutstanding", 1.25e8)]
    if interest:
        flows.append(_flow_and_balance("InterestPaidNet", interest))
    return flows


def test_debt_older_than_the_age_limit_with_interest_paid_refuses():
    # LDOS's real shape: LongTermDebt last tagged for 2015 ($1.1B), interest
    # paid today. Using the 2015 balance gave a 20% cost of debt.
    st = Statements(_operating() + [
        _flow_and_balance("LongTermDebt", 1.1e9, date(2015, 10, 2), date(2015, 11, 20), duration=False)])
    out = build_inputs(st, date(2026, 10, 8), 121.0, 0.04, BETA)
    assert isinstance(out, Refusal) and "no debt balance" in out.reason and "2015-10-02" in out.reason


def test_the_capital_lease_variants_are_debt_at_the_newest_balance_date():
    # LDOS today: the 2015 LongTermDebt is older than the capital-lease total.
    st = Statements(_operating() + [
        _flow_and_balance("LongTermDebt", 1.1e9, date(2015, 10, 2), date(2015, 11, 20), duration=False),
        _flow_and_balance("LongTermDebtAndCapitalLeaseObligations", 6.01e9, duration=False),
        _flow_and_balance("LongTermDebtAndCapitalLeaseObligationsCurrent", 0.02e9, duration=False),
        _flow_and_balance("CashAndCashEquivalentsAtCarryingValue", 0.9e9, duration=False)])
    v = build_inputs(st, date(2026, 10, 8), 121.0, 0.04, BETA)
    assert v.lines["total_debt"][0] == pytest.approx(6.03e9)
    assert "LongTermDebtAndCapitalLeaseObligations" in v.lines["total_debt"][1] and "2026-06-30" in v.lines["total_debt"][1]
    assert v.dcf.net_debt == pytest.approx(6.03e9 - 0.9e9)
    # 3.65% reported is below risk-free + 1%: the market cost of debt is used (decision 14).
    assert v.discount.cost_of_debt_pre_tax == pytest.approx(0.05)


def test_a_combined_debt_figure_is_used_alone():
    # CMCSA: DebtAndCapitalLeaseObligations already holds the current part;
    # adding DebtCurrent to it would count $6B twice.
    st = Statements(_operating() + [
        _flow_and_balance("DebtAndCapitalLeaseObligations", 90.4e9, duration=False),
        _flow_and_balance("ShortTermBorrowings", 6.1e9, duration=False)])
    assert st.debt(date(2026, 10, 8)).value == pytest.approx(90.4e9)


def test_noncurrent_plus_current_takes_debt_current_or_its_parts_never_both():
    st = Statements([
        _flow_and_balance("LongTermDebtNoncurrent", 10e9, duration=False),
        _flow_and_balance("LongTermDebtCurrent", 1e9, duration=False),
        _flow_and_balance("ShortTermBorrowings", 0.5e9, duration=False),
        _flow_and_balance("CommercialPaper", 0.3e9, duration=False)])
    assert st.debt(date(2026, 10, 8)).value == pytest.approx(11.5e9)  # paper inside borrowings
    st2 = Statements([
        _flow_and_balance("LongTermDebtNoncurrent", 10e9, duration=False),
        _flow_and_balance("LongTermDebtCurrent", 1e9, duration=False),
        _flow_and_balance("DebtCurrent", 1.5e9, duration=False)])
    assert st2.debt(date(2026, 10, 8)).value == pytest.approx(11.5e9)


def test_a_newer_balance_date_beats_a_preferred_but_older_shape():
    # VZ: the combined figure is for 2025-12-31, noncurrent + current for 2026-06-30.
    st = Statements([
        _flow_and_balance("DebtLongtermAndShorttermCombinedAmount", 158e9, date(2025, 12, 31), date(2026, 2, 10), False),
        _flow_and_balance("LongTermDebtAndCapitalLeaseObligations", 143.45e9, duration=False),
        _flow_and_balance("LongTermDebtCurrent", 21.78e9, duration=False)])
    d = st.debt(date(2026, 10, 8))
    assert d.value == pytest.approx(165.23e9) and d.period_end == date(2026, 6, 30)


def test_no_debt_and_no_interest_is_debt_free_and_says_so():
    st = Statements(_operating(interest=None))
    v = build_inputs(st, date(2026, 10, 8), 50.0, 0.04, BETA)
    assert v.lines["total_debt"][0] == 0.0 and any("debt-free" in n for n in v.notes)


def test_stale_cash_is_not_netted():
    st = Statements(_operating() + [
        _flow_and_balance("LongTermDebt", 4e9, duration=False),
        _flow_and_balance("CashAndCashEquivalentsAtCarryingValue", 9e9, date(2014, 12, 31), date(2015, 2, 1), False)])
    v = build_inputs(st, date(2026, 10, 8), 50.0, 0.04, BETA)
    assert v.dcf.net_debt == 4e9 and any("cash not reported since 2014-12-31" in n for n in v.notes)


def test_a_fiscal_year_end_debt_balance_is_used_and_noted():
    # CAT, LEN, PHM: debt tagged only in the 10-K, 284 days before the valuation.
    st = Statements(_operating() + [
        _flow_and_balance("LongTermDebt", 4e9, date(2025, 12, 31), date(2026, 2, 15), False)])
    v = build_inputs(st, date(2026, 10, 10), 50.0, 0.04, BETA)
    assert v.lines["total_debt"][0] == 4e9 and any("fiscal year end" in n for n in v.notes)


def test_interest_filed_negative_is_its_magnitude():
    # LKQ tags InterestExpenseNonoperating as -$216M; its cost of debt read -5.5%.
    st = Statements(_operating(interest=None) + [
        _flow_and_balance("InterestExpenseNonoperating", -2.16e8),
        _flow_and_balance("LongTermDebt", 4e9, duration=False)])
    v = build_inputs(st, date(2026, 10, 8), 50.0, 0.04, BETA)
    assert v.discount.cost_of_debt_pre_tax == pytest.approx(2.16e8 / 4e9)
    assert any("negative" in n for n in v.notes)


def test_at_one_balance_date_a_component_shape_loses_to_the_whole():
    # ON 2026-07-03: DebtAndCapitalLeaseObligations $1.6M (finance leases),
    # LongTermDebt $4.5B. Preferring the combined shape cost of debt 3,800%.
    st = Statements([
        _flow_and_balance("DebtAndCapitalLeaseObligations", 1.6e6, duration=False),
        _flow_and_balance("LongTermDebt", 4.505e9, duration=False)])
    d = st.debt(date(2026, 10, 8))
    assert d.value == pytest.approx(4.505e9) and d.concept.startswith("LongTermDebt")


def test_default_growth_is_the_lower_of_three_and_five_year_cagr():
    # LVS's shape: a rebound inflates the 3-year CAGR; the 5-year is lower.
    from core.fundamentals import Fact
    from modeling.valuation import default_growth

    def rev(year, v):
        return Fact(1, "us-gaap", "Revenues", "USD", date(year, 1, 1), date(year, 12, 31), v, year, "FY", "10-K",
                    date(year + 1, 2, 20), f"a{year}", None, datetime(2026, 1, 1, tzinfo=timezone.utc))
    levels = {2020: 100.0, 2021: 60.0, 2022: 70.0, 2023: 80.0, 2024: 95.0, 2025: 110.0}
    g, why = default_growth(Statements([rev(y, v) for y, v in levels.items()]), date(2026, 6, 1))
    assert g == pytest.approx((110 / 100) ** (1 / 5) - 1)  # 5-year 1.9%, not 3-year 16.3%
    assert "3-year" in why and "5-year" in why and "acquisitions" in why


def test_ebitda_sums_depreciation_and_amortisation_when_there_is_no_total():
    # MSFT tags Depreciation and AmortizationOfIntangibleAssets, no D&A total.
    st = Statements(_operating() + [
        _flow_and_balance("LongTermDebt", 4e9, duration=False),
        _flow_and_balance("OperatingIncomeLoss", 155e9),
        _flow_and_balance("Depreciation", 34e9),
        _flow_and_balance("AmortizationOfIntangibleAssets", 4e9)])
    v = build_inputs(st, date(2026, 10, 8), 50.0, 0.04, BETA)
    assert v.lines["ebitda"][0] == pytest.approx(193e9)
    assert "depreciation + amortisation" in v.lines["ebitda"][1]
    alone = Statements(_operating() + [_flow_and_balance("LongTermDebt", 4e9, duration=False),
                                       _flow_and_balance("OperatingIncomeLoss", 155e9),
                                       _flow_and_balance("Depreciation", 34e9)])
    w = build_inputs(alone, date(2026, 10, 8), 50.0, 0.04, BETA)
    assert w.lines["ebitda"][0] == pytest.approx(189e9) and "depreciation only" in w.lines["ebitda"][1]
