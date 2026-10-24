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
    out = build_inputs(
        statements("companyfacts_sivb_2026-10-08.json.gz"),
        date(2022, 6, 1),
        400.0,
        0.01,
        BETA,
        sector="Financials",
    )
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
    assert L["base_fcf"] == pytest.approx(
        L["cfo"] - L["capex"] + L["after_tax_interest"] - L["share_based_comp"]
    )
    assert "subtracted" in v.lines["share_based_comp"][1]


def test_net_debt_counts_noncurrent_securities(aapl):
    v = build_inputs(aapl, date(2025, 11, 1), 270.0, 0.04, BETA)
    # 2025-09-27: debt 90,678 + CP 7,979 - cash 35,934 - current 18,763 - noncurrent 77,723
    assert v.lines["net_debt"][0] == pytest.approx(
        (90_678 + 7_979 - 35_934 - 18_763 - 77_723) * 1e6
    )


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
                d = datetime.fromordinal(date(2023, 1, 2).toordinal() + i).replace(
                    tzinfo=timezone.utc
                )
                if sym == "SPY" and i == 850:  # 2025-05-01: inside the beta window
                    continue
                px = (
                    100
                    * scale
                    * (1 + 0.01 * ((i * 7919) % 13 - 6) / 6) ** (1 if sym == "SPY" else 1.5)
                )
                rows.append(
                    MarketDataRecord(
                        asset=Asset(symbol=sym, asset_class="etf", source="yfinance"),
                        ohlcv=OHLCV(
                            open=px,
                            high=px,
                            low=px,
                            close=px,
                            volume=1.0,
                            timestamp=Timestamp(utc=d),
                        ),
                    )
                )
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
        return Fact(
            1,
            "us-gaap",
            "Revenues",
            "USD",
            date(year, 1, 1),
            date(year, 12, 31),
            v,
            year,
            "FY",
            "10-K",
            date(year + 1, 2, 20),
            f"a{year}",
            None,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    st = Statements([rev(y, 100.0 * 1.2 ** (y - 2016)) for y in range(2016, 2021)])
    g, why = default_growth(st, date(2021, 6, 1))
    assert g == 0.10 and "clamped" in why  # fresh: a 20% CAGR, capped at decision 15's 10%
    g, why = default_growth(st, date(2026, 10, 8))
    assert g == 0.025 and "stale" in why


def test_missing_capex_refuses_instead_of_counting_as_zero():
    from core.fundamentals import Fact

    def f(concept, v, filed=date(2026, 8, 1)):
        return Fact(
            1,
            "us-gaap",
            concept,
            "USD",
            date(2025, 7, 1),
            date(2026, 6, 30),
            v,
            2026,
            "FY",
            "10-K",
            filed,
            f"a-{concept}",
            None,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

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
    # CMCSA's cover count is from 2009. (Fox's 1 share from 2019 was this
    # test's case until decision 63 set counts under 1M aside.)
    from core.fundamentals import Fact

    def f(concept, v, start, end, filed, tax="us-gaap"):
        return Fact(
            1,
            tax,
            concept,
            "shares" if "Share" in concept else "USD",
            start,
            end,
            v,
            None,
            "FY",
            "10-K",
            filed,
            f"a-{concept}-{end}",
            None,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    st = Statements(
        [
            f(
                "EntityCommonStockSharesOutstanding",
                2.9e9,
                None,
                date(2019, 3, 18),
                date(2019, 3, 18),
                "dei",
            ),
            f(
                "WeightedAverageNumberOfDilutedSharesOutstanding",
                439e6,
                date(2025, 7, 1),
                date(2026, 6, 30),
                date(2026, 8, 6),
            ),
            f(
                "NetCashProvidedByUsedInOperatingActivities",
                3e9,
                date(2025, 7, 1),
                date(2026, 6, 30),
                date(2026, 8, 6),
            ),
            f(
                "PaymentsToAcquirePropertyPlantAndEquipment",
                4e8,
                date(2025, 7, 1),
                date(2026, 6, 30),
                date(2026, 8, 6),
            ),
        ]
    )
    v = build_inputs(st, date(2026, 10, 8), 57.0, 0.04, BETA)
    assert v.dcf.shares == 439e6 and v.lines["shares"][1].startswith("latest diluted")
    assert any("stale" in n for n in v.notes)


def test_cash_interest_paid_is_used_as_a_labelled_proxy():
    from core.fundamentals import Fact

    def f(concept, v):
        return Fact(
            1,
            "us-gaap",
            concept,
            "shares" if "Share" in concept else "USD",
            date(2025, 7, 1),
            date(2026, 6, 30),
            v,
            None,
            "FY",
            "10-K",
            date(2026, 8, 6),
            f"a-{concept}",
            None,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    st = Statements(
        [
            f("NetCashProvidedByUsedInOperatingActivities", 3e9),
            f("PaymentsToAcquirePropertyPlantAndEquipment", 4e8),
            f("WeightedAverageNumberOfDilutedSharesOutstanding", 1e8),
            f("InterestPaidNet", 2e8),
            Fact(
                1,
                "us-gaap",
                "LongTermDebt",
                "USD",
                None,
                date(2026, 6, 30),
                4e9,
                None,
                "FY",
                "10-K",
                date(2026, 8, 6),
                "a-debt",
                None,
                datetime(2026, 1, 1, tzinfo=timezone.utc),
            ),
        ]
    )
    v = build_inputs(st, date(2026, 10, 8), 50.0, 0.04, BETA)
    assert v.discount.cost_of_debt_pre_tax == pytest.approx(
        2e8 / 4e9
    )  # the proxy, not risk-free + spread
    assert v.lines["after_tax_interest"][1].startswith("PROXY: cash interest paid")
    assert any("proxy" in n for n in v.notes) and not v.discount.cost_of_debt_estimated


def test_the_share_fallback_takes_the_newest_average_annual_or_quarterly():
    from core.fundamentals import Fact

    def f(concept, v, start, end, filed, fp):
        return Fact(
            1,
            "us-gaap",
            concept,
            "shares" if "Share" in concept else "USD",
            start,
            end,
            v,
            None,
            fp,
            "10-K",
            filed,
            f"a-{concept}-{end}-{start}",
            None,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    st = Statements(
        [
            f(
                "WeightedAverageNumberOfDilutedSharesOutstanding",
                443e6,
                date(2026, 1, 1),
                date(2026, 3, 31),
                date(2026, 5, 11),
                "Q3",
            ),
            f(
                "WeightedAverageNumberOfDilutedSharesOutstanding",
                439e6,
                date(2025, 7, 1),
                date(2026, 6, 30),
                date(2026, 8, 6),
                "FY",
            ),
            f(
                "NetCashProvidedByUsedInOperatingActivities",
                3e9,
                date(2025, 7, 1),
                date(2026, 6, 30),
                date(2026, 8, 6),
                "FY",
            ),
            f(
                "PaymentsToAcquirePropertyPlantAndEquipment",
                4e8,
                date(2025, 7, 1),
                date(2026, 6, 30),
                date(2026, 8, 6),
                "FY",
            ),
        ]
    )
    assert build_inputs(st, date(2026, 10, 8), 57.0, 0.04, BETA).dcf.shares == 439e6


def _flow_and_balance(concept, v, end=date(2026, 6, 30), filed=date(2026, 8, 6), duration=True):
    from core.fundamentals import Fact

    start = (
        date(end.year - 1, end.month, end.day) + (date(2026, 1, 2) - date(2026, 1, 1))
        if duration
        else None
    )
    return Fact(
        1,
        "us-gaap",
        concept,
        "shares" if "Share" in concept else "USD",
        start,
        end,
        v,
        None,
        "FY",
        "10-K",
        filed,
        f"a-{concept}-{end}",
        None,
        datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def _operating(interest=2.2e8):
    flows = [
        _flow_and_balance("NetCashProvidedByUsedInOperatingActivities", 2.3e9),
        _flow_and_balance("PaymentsToAcquirePropertyPlantAndEquipment", 1.4e8),
        _flow_and_balance("WeightedAverageNumberOfDilutedSharesOutstanding", 1.25e8),
    ]
    if interest:
        flows.append(_flow_and_balance("InterestPaidNet", interest))
    return flows


def test_debt_older_than_the_age_limit_with_interest_paid_refuses():
    # LDOS's real shape: LongTermDebt last tagged for 2015 ($1.1B), interest
    # paid today. Using the 2015 balance gave a 20% cost of debt.
    st = Statements(
        _operating()
        + [
            _flow_and_balance(
                "LongTermDebt", 1.1e9, date(2015, 10, 2), date(2015, 11, 20), duration=False
            )
        ]
    )
    out = build_inputs(st, date(2026, 10, 8), 121.0, 0.04, BETA)
    assert (
        isinstance(out, Refusal) and "no debt balance" in out.reason and "2015-10-02" in out.reason
    )


def test_the_capital_lease_variants_are_debt_at_the_newest_balance_date():
    # LDOS today: the 2015 LongTermDebt is older than the capital-lease total.
    st = Statements(
        _operating()
        + [
            _flow_and_balance(
                "LongTermDebt", 1.1e9, date(2015, 10, 2), date(2015, 11, 20), duration=False
            ),
            _flow_and_balance("LongTermDebtAndCapitalLeaseObligations", 6.01e9, duration=False),
            _flow_and_balance(
                "LongTermDebtAndCapitalLeaseObligationsCurrent", 0.02e9, duration=False
            ),
            _flow_and_balance("CashAndCashEquivalentsAtCarryingValue", 0.9e9, duration=False),
        ]
    )
    v = build_inputs(st, date(2026, 10, 8), 121.0, 0.04, BETA)
    assert v.lines["total_debt"][0] == pytest.approx(6.03e9)
    assert (
        "LongTermDebtAndCapitalLeaseObligations" in v.lines["total_debt"][1]
        and "2026-06-30" in v.lines["total_debt"][1]
    )
    assert v.dcf.net_debt == pytest.approx(6.03e9 - 0.9e9)
    # 3.65% reported is below risk-free + 1%: the market cost of debt is used (decision 14).
    assert v.discount.cost_of_debt_pre_tax == pytest.approx(0.05)


def test_a_combined_debt_figure_is_used_alone():
    # CMCSA: DebtAndCapitalLeaseObligations already holds the current part;
    # adding DebtCurrent to it would count $6B twice.
    st = Statements(
        _operating()
        + [
            _flow_and_balance("DebtAndCapitalLeaseObligations", 90.4e9, duration=False),
            _flow_and_balance("ShortTermBorrowings", 6.1e9, duration=False),
        ]
    )
    assert st.debt(date(2026, 10, 8)).value == pytest.approx(90.4e9)


def test_noncurrent_plus_current_takes_debt_current_or_its_parts_never_both():
    st = Statements(
        [
            _flow_and_balance("LongTermDebtNoncurrent", 10e9, duration=False),
            _flow_and_balance("LongTermDebtCurrent", 1e9, duration=False),
            _flow_and_balance("ShortTermBorrowings", 0.5e9, duration=False),
            _flow_and_balance("CommercialPaper", 0.3e9, duration=False),
        ]
    )
    assert st.debt(date(2026, 10, 8)).value == pytest.approx(11.5e9)  # paper inside borrowings
    st2 = Statements(
        [
            _flow_and_balance("LongTermDebtNoncurrent", 10e9, duration=False),
            _flow_and_balance("LongTermDebtCurrent", 1e9, duration=False),
            _flow_and_balance("DebtCurrent", 1.5e9, duration=False),
        ]
    )
    assert st2.debt(date(2026, 10, 8)).value == pytest.approx(11.5e9)


def test_a_newer_balance_date_beats_a_preferred_but_older_shape():
    # VZ: the combined figure is for 2025-12-31, noncurrent + current for 2026-06-30.
    st = Statements(
        [
            _flow_and_balance(
                "DebtLongtermAndShorttermCombinedAmount",
                158e9,
                date(2025, 12, 31),
                date(2026, 2, 10),
                False,
            ),
            _flow_and_balance("LongTermDebtAndCapitalLeaseObligations", 143.45e9, duration=False),
            _flow_and_balance("LongTermDebtCurrent", 21.78e9, duration=False),
        ]
    )
    d = st.debt(date(2026, 10, 8))
    assert d.value == pytest.approx(165.23e9) and d.period_end == date(2026, 6, 30)


def test_no_debt_and_no_interest_is_debt_free_and_says_so():
    st = Statements(_operating(interest=None))
    v = build_inputs(st, date(2026, 10, 8), 50.0, 0.04, BETA)
    assert v.lines["total_debt"][0] == 0.0 and any("debt-free" in n for n in v.notes)


def test_stale_cash_is_not_netted():
    st = Statements(
        _operating()
        + [
            _flow_and_balance("LongTermDebt", 4e9, duration=False),
            _flow_and_balance(
                "CashAndCashEquivalentsAtCarryingValue",
                9e9,
                date(2014, 12, 31),
                date(2015, 2, 1),
                False,
            ),
        ]
    )
    v = build_inputs(st, date(2026, 10, 8), 50.0, 0.04, BETA)
    assert v.dcf.net_debt == 4e9 and any("cash not reported since 2014-12-31" in n for n in v.notes)


def test_a_fiscal_year_end_debt_balance_is_used_and_noted():
    # CAT, LEN, PHM: debt tagged only in the 10-K, 284 days before the valuation.
    st = Statements(
        _operating()
        + [_flow_and_balance("LongTermDebt", 4e9, date(2025, 12, 31), date(2026, 2, 15), False)]
    )
    v = build_inputs(st, date(2026, 10, 10), 50.0, 0.04, BETA)
    assert v.lines["total_debt"][0] == 4e9 and any("fiscal year end" in n for n in v.notes)


def test_interest_filed_negative_is_its_magnitude():
    # LKQ tags InterestExpenseNonoperating as -$216M; its cost of debt read -5.5%.
    st = Statements(
        _operating(interest=None)
        + [
            _flow_and_balance("InterestExpenseNonoperating", -2.16e8),
            _flow_and_balance("LongTermDebt", 4e9, duration=False),
        ]
    )
    v = build_inputs(st, date(2026, 10, 8), 50.0, 0.04, BETA)
    assert v.discount.cost_of_debt_pre_tax == pytest.approx(2.16e8 / 4e9)
    assert any("negative" in n for n in v.notes)


def test_at_one_balance_date_a_component_shape_loses_to_the_whole():
    # ON 2026-07-03: DebtAndCapitalLeaseObligations $1.6M (finance leases),
    # LongTermDebt $4.5B. Preferring the combined shape cost of debt 3,800%.
    st = Statements(
        [
            _flow_and_balance("DebtAndCapitalLeaseObligations", 1.6e6, duration=False),
            _flow_and_balance("LongTermDebt", 4.505e9, duration=False),
        ]
    )
    d = st.debt(date(2026, 10, 8))
    assert d.value == pytest.approx(4.505e9) and d.concept.startswith("LongTermDebt")


def test_default_growth_is_the_lower_of_three_and_five_year_cagr():
    # LVS's shape: a rebound inflates the 3-year CAGR; the 5-year is lower.
    from core.fundamentals import Fact
    from modeling.valuation import default_growth

    def rev(year, v):
        return Fact(
            1,
            "us-gaap",
            "Revenues",
            "USD",
            date(year, 1, 1),
            date(year, 12, 31),
            v,
            year,
            "FY",
            "10-K",
            date(year + 1, 2, 20),
            f"a{year}",
            None,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    levels = {2020: 100.0, 2021: 60.0, 2022: 70.0, 2023: 80.0, 2024: 95.0, 2025: 110.0}
    g, why = default_growth(Statements([rev(y, v) for y, v in levels.items()]), date(2026, 6, 1))
    assert g == pytest.approx((110 / 100) ** (1 / 5) - 1)  # 5-year 1.9%, not 3-year 16.3%
    assert "3-year" in why and "5-year" in why and "acquisitions" in why


def test_ebitda_sums_depreciation_and_amortisation_when_there_is_no_total():
    # MSFT tags Depreciation and AmortizationOfIntangibleAssets, no D&A total.
    st = Statements(
        _operating()
        + [
            _flow_and_balance("LongTermDebt", 4e9, duration=False),
            _flow_and_balance("OperatingIncomeLoss", 155e9),
            _flow_and_balance("Depreciation", 34e9),
            _flow_and_balance("AmortizationOfIntangibleAssets", 4e9),
        ]
    )
    v = build_inputs(st, date(2026, 10, 8), 50.0, 0.04, BETA)
    assert v.lines["ebitda"][0] == pytest.approx(193e9)
    assert "depreciation + amortisation" in v.lines["ebitda"][1]
    alone = Statements(
        _operating()
        + [
            _flow_and_balance("LongTermDebt", 4e9, duration=False),
            _flow_and_balance("OperatingIncomeLoss", 155e9),
            _flow_and_balance("Depreciation", 34e9),
        ]
    )
    w = build_inputs(alone, date(2026, 10, 8), 50.0, 0.04, BETA)
    assert (
        w.lines["ebitda"][0] == pytest.approx(189e9) and "depreciation only" in w.lines["ebitda"][1]
    )


@pytest.mark.parametrize(
    "concept",
    [
        "PaymentsToAcquireOtherPropertyPlantAndEquipment",  # LLY, WEC, LNT, ADP
        "PaymentsToAcquireOtherProductiveAssets",  # VZ
        "PaymentsForConstructionInProcess",  # ED
        "PaymentsForCapitalImprovements",  # GLW, IT, SNA
        "PaymentsToAcquireMachineryAndEquipment",  # RL
        "PaymentsForProceedsFromProductiveAssets",  # D, net of disposals
    ],
)
def test_a_filers_only_capex_line_is_capex(concept):
    st = Statements(
        [
            _flow_and_balance("NetCashProvidedByUsedInOperatingActivities", 37e9),
            _flow_and_balance(concept, 17e9),
            _flow_and_balance("WeightedAverageNumberOfDilutedSharesOutstanding", 4.2e9),
            _flow_and_balance("LongTermDebt", 1e9, duration=False),
            _flow_and_balance("InterestPaidNet", 5e7),
        ]
    )
    v = build_inputs(st, date(2026, 10, 8), 40.0, 0.04, BETA)
    assert not isinstance(v, Refusal) and v.lines["capex"][0] == 17e9


@pytest.mark.parametrize(
    ("symbol", "refused"), [("MA", False), ("ma", False), ("JPM", True), (None, True)]
)
def test_a_named_fee_business_is_valued_in_a_refused_sector(aapl, symbol, refused):
    """Decision 16: listed fee businesses pass the Financials refusal, by name only."""
    out = build_inputs(
        aapl, date(2026, 10, 8), 340.42, 0.0412, BETA, sector="Financials", symbol=symbol
    )
    assert isinstance(out, Refusal) is refused
    if not refused:
        assert any("valued by name" in n for n in out.notes)


def test_allowed_and_refused_symbol_lists_do_not_overlap():
    from config import settings

    assert not set(settings.DCF_ALLOWED_SYMBOLS) & set(settings.DCF_REFUSED_SYMBOLS)
    assert all(settings.DCF_ALLOWED_SYMBOLS.values())


def test_growth_starts_from_the_latest_years_basis():
    """
    ADM's contract revenue (a part) for 2022-2025 beside Revenues (the total)
    for every year: the contract series' 2025 against 2020's total read as
    -17% a year. The mapped total gives -7.5% (3-year) and +4.5% (5-year).
    """
    from core.fundamentals import Fact
    from modeling.valuation import default_growth

    def rev(concept, year, v):
        return Fact(
            1,
            "us-gaap",
            concept,
            "USD",
            date(year, 1, 1),
            date(year, 12, 31),
            v,
            year,
            "FY",
            "10-K",
            date(year + 1, 2, 20),
            f"a{year}",
            None,
            datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    total = {2020: 64.36, 2021: 85.25, 2022: 101.56, 2023: 93.94, 2024: 85.53, 2025: 80.27}
    contract = {2022: 27.61, 2023: 25.69, 2024: 24.37, 2025: 24.96}
    facts = [rev("Revenues", y, v) for y, v in total.items()] + [
        rev("RevenueFromContractWithCustomerExcludingAssessedTax", y, v)
        for y, v in contract.items()
    ]
    as_of = date(2026, 6, 1)
    g, why = default_growth(Statements(facts), as_of)
    assert g == pytest.approx((24.96 / 27.61) ** (1 / 3) - 1)  # no 5-year: no contract 2020
    assert "5-year" not in why
    g, _ = default_growth(Statements(facts, line_concepts={"revenue": ("Revenues",)}), as_of)
    assert g == pytest.approx((80.27 / 101.56) ** (1 / 3) - 1)


def test_capitalised_software_is_capex_and_labelled(aapl):
    """
    FIS: "Additions to software" $835M beside $154M of capex, both cash spent
    on long-lived assets. Added in its own window, named in the source; a
    filer that tags none has nothing added and is not refused.
    """
    from core.fundamentals import Fact

    as_of = date(2026, 10, 8)
    plain = build_inputs(aapl, as_of, 340.42, 0.0412, BETA)
    capex, src = plain.lines["capex"]
    assert "software" not in src

    software = Fact(
        1,
        "us-gaap",
        "PaymentsToDevelopSoftware",
        "USD",
        date(2024, 9, 29),
        date(2025, 9, 27),
        835e6,
        2025,
        "FY",
        "10-K",
        date(2025, 10, 31),
        "a-software",
        None,
        datetime(2026, 1, 1, tzinfo=timezone.utc),
    )
    facts = [f for fs in aapl.by_concept.values() for f in fs] + [software]
    v = build_inputs(Statements(facts, aapl.splits), as_of, 340.42, 0.0412, BETA)
    with_sw, sw_src = v.lines["capex"]
    assert with_sw == pytest.approx(capex + 835e6)
    assert "835M capitalised software/intangibles (FY to 2025-09-27)" in sw_src
    L = {k: val for k, (val, _) in v.lines.items()}
    assert L["base_fcf"] == pytest.approx(plain.lines["base_fcf"][0] - 835e6)


def _nci_filer(*extra):
    return Statements(
        _operating()
        + [
            _flow_and_balance("LongTermDebt", 4e9, duration=False),
            _flow_and_balance("OperatingIncomeLoss", 9e8),
            _flow_and_balance("DepreciationDepletionAndAmortization", 1e8),
        ]
        + list(extra)
    )


AS_OF_NCI = date(2026, 10, 8)


def test_noncontrolling_interests_come_off_fcf_and_the_exit_base():
    """
    DVA's shape (decision 17): consolidated flows, partners take a share of
    profit. Their income comes off base FCF, labelled, and the exit multiple
    values only the parent's share of EBITDA.
    """
    plain = build_inputs(_nci_filer(), AS_OF_NCI, 50.0, 0.04, BETA)
    assert plain.lines["noncontrolling_interests"] == (None, "none on file")
    v = build_inputs(
        _nci_filer(
            _flow_and_balance("NetIncomeLossAttributableToNoncontrollingInterest", 3e8),
            _flow_and_balance("ProfitLoss", 1e9),
            _flow_and_balance("MinorityInterest", 1.8e9, duration=False),
        ),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
    )
    nci, src = v.lines["noncontrolling_interests"]
    assert nci == 3e8 and src.endswith("FY to 2026-06-30; subtracted")
    assert v.dcf.base_fcf == pytest.approx(plain.dcf.base_fcf - 3e8)
    assert v.lines["base_fcf"][1].endswith("- noncontrolling interests")
    # EBITDA 1.0B consolidated; the parent keeps 70% of consolidated profit.
    assert v.lines["ebitda"][0] == pytest.approx(1e9)
    assert v.dcf.exit_metric_base == pytest.approx(7e8)
    assert "x 0.700, the parent's share" in v.lines["ebitda"][1]
    assert plain.dcf.exit_metric_base == pytest.approx(1e9)


def test_the_parent_share_falls_back_to_net_income_plus_nci():
    v = build_inputs(
        _nci_filer(
            _flow_and_balance("NetIncomeLossAttributableToNoncontrollingInterest", 2e8),
            _flow_and_balance("NetIncomeLoss", 6e8),
        ),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
    )
    assert v.dcf.exit_metric_base == pytest.approx(1e9 * 0.75)


def test_redeemable_and_nonredeemable_parts_are_summed():
    # MPC, SPGI, ICE and WBD tag only the parts.
    v = build_inputs(
        _nci_filer(
            _flow_and_balance("NetIncomeLossAttributableToRedeemableNoncontrollingInterest", 1e8),
            _flow_and_balance(
                "NetIncomeLossAttributableToNonredeemableNoncontrollingInterest", 5e7
            ),
        ),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
    )
    nci, src = v.lines["noncontrolling_interests"]
    assert nci == pytest.approx(1.5e8) and "(redeemable + nonredeemable)" in src


def test_nci_income_over_half_of_fcf_refuses():
    # FCX: partners' income 118% of consolidated FCF; the proxy cannot carry it.
    out = build_inputs(
        _nci_filer(_flow_and_balance("NetIncomeLossAttributableToNoncontrollingInterest", 1.5e9)),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
    )
    assert isinstance(out, Refusal) and "over 50%" in out.reason


def test_an_nci_balance_without_its_income_refuses_unless_immaterial():
    # Missing is never zero: XOM, MPC, SPGI carry NCI balances of $5-7B.
    out = build_inputs(
        _nci_filer(
            _flow_and_balance(
                "RedeemableNoncontrollingInterestEquityCarryingAmount", 5e8, duration=False
            )
        ),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
    )
    assert isinstance(out, Refusal) and "noncontrolling interests of 500M" in out.reason
    # $1M against a $6.25B market cap (ITW, VLTO): noted, not refused.
    small = build_inputs(
        _nci_filer(_flow_and_balance("MinorityInterest", 1e6, duration=False)),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
    )
    plain = build_inputs(_nci_filer(), AS_OF_NCI, 50.0, 0.04, BETA)
    assert not isinstance(small, Refusal)
    assert small.dcf.base_fcf == plain.dcf.base_fcf
    assert any("immaterial, not subtracted" in n for n in small.notes)
    # A balance older than the annual age rule is not evidence of NCI today.
    old = build_inputs(
        _nci_filer(
            _flow_and_balance(
                "MinorityInterest", 5e8, date(2024, 6, 30), date(2024, 8, 1), duration=False
            )
        ),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
    )
    assert not isinstance(old, Refusal)


def test_the_exit_value_is_dropped_when_the_parent_share_is_undefined():
    v = build_inputs(
        _nci_filer(
            _flow_and_balance("NetIncomeLossAttributableToNoncontrollingInterest", 2e8),
            _flow_and_balance("ProfitLoss", -4e8),
        ),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
    )
    assert v.dcf.exit_metric_base is None and v.dcf.exit_multiple is None
    assert any("exit-multiple cross-check dropped" in n for n in v.notes)


def test_a_loss_shared_by_the_partners_is_not_added_back():
    # ROK: -$110M of NCI income (TTM); partners do not pay a loss in cash.
    plain = build_inputs(_nci_filer(), AS_OF_NCI, 50.0, 0.04, BETA)
    v = build_inputs(
        _nci_filer(
            _flow_and_balance("NetIncomeLossAttributableToNoncontrollingInterest", -1.1e8),
            _flow_and_balance("ProfitLoss", 5e8),
        ),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
    )
    assert v.dcf.base_fcf == plain.dcf.base_fcf
    assert v.dcf.exit_metric_base == plain.dcf.exit_metric_base
    nci, src = v.lines["noncontrolling_interests"]
    assert nci == -1.1e8 and src.endswith("not added back")
    assert not v.lines["base_fcf"][1].endswith("noncontrolling interests")


def test_parts_on_different_windows_use_their_common_fiscal_year():
    # MPC: redeemable NCI filed (as zero) only through FY2025, nonredeemable
    # quarterly into 2026. One fiscal year for both, not a refusal.
    from core.fundamentals import Fact

    def ytd(concept, start, end, value, form="10-Q"):
        return Fact(
            1, "us-gaap", concept, "USD", start, end, value, None, "Q2", form,
            date(end.year, end.month, 28) if end.month < 12 else date(end.year + 1, 2, 26),
            f"a-{concept}-{start}-{end}", None, datetime(2026, 1, 1, tzinfo=timezone.utc),
        )  # fmt: skip

    nonred = "NetIncomeLossAttributableToNonredeemableNoncontrollingInterest"
    red = "NetIncomeLossAttributableToRedeemableNoncontrollingInterest"
    v = build_inputs(
        _nci_filer(
            ytd(nonred, date(2025, 1, 1), date(2025, 6, 30), 81.4e6),
            ytd(nonred, date(2025, 1, 1), date(2025, 12, 31), 183.1e6, "10-K"),
            ytd(nonred, date(2026, 1, 1), date(2026, 6, 30), 74e6),
            ytd(red, date(2025, 1, 1), date(2025, 12, 31), 0.0, "10-K"),
            _flow_and_balance("MinorityInterest", 6.6e8, duration=False),
        ),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
        growth_first=0.02,
    )
    assert not isinstance(v, Refusal), v
    nci, src = v.lines["noncontrolling_interests"]
    assert nci == pytest.approx(183.1e6) and "FY to 2025-12-31" in src


def test_preferred_stock_is_subtracted_at_its_largest_shape_on_the_newest_date():
    # Some filers carry preferred at par; the liquidation preference (MCHP,
    # NRG tag only that) is the claim. Newest date first, then the largest.
    plain = build_inputs(_nci_filer(), AS_OF_NCI, 50.0, 0.04, BETA)
    assert plain.dcf.preferred == 0.0 and plain.lines["preferred_stock"] == (0.0, "none on file")
    v = build_inputs(
        _nci_filer(
            _flow_and_balance("PreferredStockValue", 1e6, duration=False),
            _flow_and_balance("PreferredStockLiquidationPreferenceValue", 6.5e8, duration=False),
            _flow_and_balance(
                "PreferredStockValueOutstanding",
                9e8,
                date(2025, 6, 30),
                date(2025, 8, 1),
                duration=False,
            ),
        ),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
    )
    assert v.dcf.preferred == 6.5e8
    assert (
        v.lines["preferred_stock"][1]
        == "PreferredStockLiquidationPreferenceValue, balance 2026-06-30"
    )
    assert v.dcf.net_debt == plain.dcf.net_debt and v.dcf.base_fcf == plain.dcf.base_fcf
    stale = build_inputs(
        _nci_filer(
            _flow_and_balance(
                "PreferredStockValue", 5e8, date(2024, 6, 30), date(2024, 8, 1), duration=False
            )
        ),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
    )
    assert (
        stale.dcf.preferred == 0.0
        and stale.lines["preferred_stock"][1] == "none on file since 2024-06-30"
    )


def test_a_large_equity_method_stake_is_named_not_added():
    # SRE's stakes are a third of its market cap; their dividends are in CFO.
    plain = build_inputs(_nci_filer(), AS_OF_NCI, 50.0, 0.04, BETA)
    big = build_inputs(
        _nci_filer(_flow_and_balance("EquityMethodInvestments", 4e8, duration=False)),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
    )
    assert big.dcf == plain.dcf
    assert any("equity-method stakes of 400M (6% of market cap" in n for n in big.notes)
    small = build_inputs(
        _nci_filer(_flow_and_balance("EquityMethodInvestments", 2e8, duration=False)),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
    )
    assert not any("equity-method" in n for n in small.notes)


def _pension(*facts):
    return build_inputs(_nci_filer(*facts), AS_OF_NCI, 50.0, 0.04, BETA)


def _bal(concept, v, end=date(2026, 6, 30), filed=date(2026, 8, 6)):
    return _flow_and_balance(concept, v, end, filed, duration=False)


def test_a_large_unfunded_pension_is_named_not_subtracted():
    # Decision 19: GEHC's $5.5B is 18% of market cap; contributions are in CFO.
    plain = _pension()
    big = _pension(
        _bal("PensionAndOtherPostretirementDefinedBenefitPlansLiabilitiesNoncurrent", 4e8)
    )
    assert big.dcf == plain.dcf
    assert any(
        "unfunded pension and retiree benefits of 400M (6% of market cap, 2026-06-30)" in n
        for n in big.notes
    )
    assert not any("pension" in n for n in plain.notes)
    # Below 5% of the 6,250M market cap: no note.
    small = _pension(
        _bal("PensionAndOtherPostretirementDefinedBenefitPlansLiabilitiesNoncurrent", 3e8)
    )
    assert not any("pension" in n for n in small.notes)


def test_the_pension_figure_is_net_of_overfunded_plans_and_includes_the_current_part():
    # IBM's shape: $9,018M owed beside $7,544M of overfunded plans' assets.
    netted = _pension(
        _bal("PensionAndOtherPostretirementDefinedBenefitPlansLiabilitiesNoncurrent", 4e8),
        _bal("DefinedBenefitPlanAssetsForPlanBenefitsNoncurrent", 2e8),
    )
    assert not any("pension" in n for n in netted.notes)
    # No combined line: pension + OPEB + the current portion (EXC's shape).
    parts = _pension(
        _bal("DefinedBenefitPensionPlanLiabilitiesNoncurrent", 2e8),
        _bal("OtherPostretirementDefinedBenefitPlanLiabilitiesNoncurrent", 1e8),
        _bal("PensionAndOtherPostretirementDefinedBenefitPlansCurrentLiabilities", 0.5e8),
    )
    assert any("benefits of 350M (6% of market cap" in n for n in parts.notes)
    # A combined line is the whole; its parts are not added to it.
    combined = _pension(
        _bal("PensionAndOtherPostretirementDefinedBenefitPlansLiabilitiesNoncurrent", 1e8),
        _bal("DefinedBenefitPensionPlanLiabilitiesNoncurrent", 2.5e8),
    )
    assert not any("pension" in n for n in combined.notes)


def test_a_stale_or_superseded_pension_balance_is_not_noted():
    stale = _pension(
        _bal(
            "PensionAndOtherPostretirementDefinedBenefitPlansLiabilitiesNoncurrent",
            5e8,
            date(2024, 6, 30),
            date(2024, 8, 1),
        )
    )
    assert not any("pension" in n for n in stale.notes)
    # The newest balance date wins over a larger, older (but not stale) line.
    newer = _pension(
        _bal(
            "PensionAndOtherPostretirementDefinedBenefitPlansLiabilitiesNoncurrent",
            5e8,
            date(2025, 12, 31),
            date(2026, 2, 15),
        ),
        _bal("DefinedBenefitPensionPlanLiabilitiesNoncurrent", 1e8),
    )
    assert not any("pension" in n for n in newer.notes)


def test_the_pension_balance_is_read_at_an_annual_report_date():
    # TAP's shape: the 10-K nets $427M owed against a $485M surplus; the next
    # 10-Q tags only the $411M owed. That quarter-end is not a funded status.
    from dataclasses import replace

    tap = _pension(
        _bal("PensionAndOtherPostretirementDefinedBenefitPlansLiabilitiesNoncurrent", 4.27e8),
        _bal("DefinedBenefitPlanAssetsForPlanBenefitsNoncurrent", 4.85e8),
        replace(
            _bal(
                "PensionAndOtherPostretirementDefinedBenefitPlansLiabilitiesNoncurrent",
                4.11e8,
                date(2026, 9, 30),
                date(2026, 10, 1),
            ),
            form="10-Q",
        ),
    )
    assert not any("pension" in n for n in tap.notes)
    st = _nci_filer(
        _bal("PensionAndOtherPostretirementDefinedBenefitPlansLiabilitiesNoncurrent", 4.27e8),
        _bal("DefinedBenefitPlanAssetsForPlanBenefitsNoncurrent", 4.85e8),
    )
    d = st.pension_deficit(AS_OF_NCI)
    assert d.period_end == date(2026, 6, 30) and d.value == pytest.approx(-5.8e7)
    assert d.concept == (
        "PensionAndOtherPostretirementDefinedBenefitPlansLiabilitiesNoncurrent"
        " - DefinedBenefitPlanAssetsForPlanBenefitsNoncurrent"
    )


def _netted(*facts):
    st = _nci_filer(*facts)
    return Statements(
        [f for fs in st.by_concept.values() for f in fs],
        (),
        {
            "intangible_capex": (
                "PaymentsToAcquireIntangibleAssets",
                "-ProceedsFromSaleOfIntangibleAssets",
            )
        },
    )


def test_a_netted_map_subtracts_its_signed_part():
    # VST's shape (decision 23): allowance purchases less resales, in capex.
    plain = build_inputs(_nci_filer(), AS_OF_NCI, 50.0, 0.04, BETA)
    v = build_inputs(
        _netted(
            _flow_and_balance("PaymentsToAcquireIntangibleAssets", 1.189e9),
            _flow_and_balance("ProceedsFromSaleOfIntangibleAssets", 2.75e8),
        ),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
    )
    capex, src = v.lines["capex"]
    assert capex == pytest.approx(plain.lines["capex"][0] + 9.14e8)
    assert "plus 914M net intangible purchases (FY to 2026-06-30)" in src


def test_purchases_without_their_netted_sales_refuse():
    # A missing resale figure is not zero, and must not drop the purchases.
    v = build_inputs(
        _netted(_flow_and_balance("PaymentsToAcquireIntangibleAssets", 1.189e9)),
        AS_OF_NCI,
        50.0,
        0.04,
        BETA,
    )
    assert isinstance(v, Refusal) and "sales netted against them" in v.reason
    # Neither reported: nothing to add and nothing refused.
    none = build_inputs(_netted(), AS_OF_NCI, 50.0, 0.04, BETA)
    assert not isinstance(none, Refusal)
