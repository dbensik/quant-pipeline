"""core/line_concept_check.py: what the daily DCF line-concept check flags."""

from datetime import date, datetime, timezone

from core.fundamentals import Fact
from core.line_concept_check import alternatives, material, same_basis, stale_map
from modeling.statements import Statements

CONTRACT = "RevenueFromContractWithCustomerIncludingAssessedTax"
AS_OF = date(2026, 10, 12)


def fy(concept, value, year):
    return Fact(
        1,
        "us-gaap",
        concept,
        "USD",
        date(year, 1, 1),
        date(year, 12, 31),
        value,
        year,
        "FY",
        "10-K",
        date(year + 1, 2, 20),
        f"a{year}",
        None,
        datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def test_a_larger_line_beside_the_chosen_one_is_an_alternative():
    """CHTR: $889M of contract revenue chosen, $54,774M of Revenues beside it."""
    st = Statements([fy(CONTRACT, 889e6, 2025), fy("Revenues", 54_774e6, 2025)])
    (a,) = alternatives(st, "revenue", AS_OF, 0.10)
    assert (a.used, a.concept, a.value) == (CONTRACT, "Revenues", 54_774e6)


def test_a_component_or_an_equal_figure_is_not():
    """PKG: $2M beside $829M of capex is a component; a 0.5% gap is the same line."""
    capex = "PaymentsToAcquirePropertyPlantAndEquipment"
    st = Statements(
        [
            fy(capex, 829e6, 2025),
            fy("PaymentsToAcquireOtherProductiveAssets", 2e6, 2025),
            fy("PaymentsToAcquireProductiveAssets", 833e6, 2025),
        ]
    )
    assert alternatives(st, "capex", AS_OF, 0.10) == []
    assert len(alternatives(st, "capex", AS_OF, 0.0)) == 1  # the floor is what drops PKG's $2M


def test_a_mapped_line_offers_only_concepts_outside_its_map():
    st = Statements(
        [fy(CONTRACT, 889e6, 2025), fy("Revenues", 54_774e6, 2025)],
        line_concepts={"revenue": ("Revenues",)},
    )
    (a,) = alternatives(st, "revenue", AS_OF)
    assert (a.used, a.concept) == ("Revenues", CONTRACT)


def test_a_map_naming_a_concept_the_filer_stopped_using_is_stale():
    facts = [fy("Revenues", v, y) for y, v in ((2023, 1.0), (2024, 1.1))] + [
        fy(CONTRACT, 1.2, 2025)
    ]
    stale = stale_map(Statements(facts, line_concepts={"revenue": ("Revenues",)}), "revenue", AS_OF)
    assert "ends FY 2024-12-31" in stale and "FY 2025-12-31" in stale
    current = Statements(
        facts + [fy("Revenues", 1.2, 2025)], line_concepts={"revenue": ("Revenues",)}
    )
    assert stale_map(current, "revenue", AS_OF) is None
    assert stale_map(Statements(facts), "revenue", AS_OF) is None  # unmapped: nothing to go stale


def test_materiality_is_absolute_and_relative():
    assert material(3.63, 5.81, 0.05, 0.05)
    assert not material(0.71, 0.67, 0.05, 0.05)  # MMM: 0.04
    assert not material(3.29, 3.25, 0.05, 0.05)  # 0.04 absolute
    assert not material(2.0, 2.06, 0.05, 0.05)  # 3% relative


def test_a_trailing_line_against_a_fiscal_year_is_not_compared():
    """LYV 2026-10-13: the alternative had no 10-Qs, so the window moved, not the concept."""
    assert not same_basis("TTM to 2026-06-30", "FY to 2025-12-31")
    assert not same_basis(
        "PROXY: cash interest paid, TTM to 2026-08-15",
        "PROXY: cash interest paid, FY to 2026-01-31",
    )
    assert same_basis("TTM to 2026-06-30", "TTM to 2026-06-30")
    assert same_basis(  # growth has no window
        "lower revenue CAGR (3-year -6.85%) to FY 2025-12-31",
        "lower revenue CAGR (3-year 0.46%) to FY 2025-12-31",
    )


def test_a_part_of_a_summed_map_is_not_an_alternative_to_the_sum():
    """LNT: utility $2,277M + other $206M is the line; neither part alone is another reading."""
    utility, other = (
        "PaymentsToAcquireOtherPropertyPlantAndEquipment",
        "PaymentsToAcquireOtherProductiveAssets",
    )
    st = Statements(
        [fy(utility, 2_277e6, 2025), fy(other, 206e6, 2025)],
        line_concepts={"capex": (utility, other)},
    )
    assert st.annual("capex", AS_OF)[-1].value == 2_483e6
    assert alternatives(st, "capex", AS_OF) == []
