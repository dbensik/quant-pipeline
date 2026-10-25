"""
Decision 71. CSGP's cash-flow statement has two purchase lines from its
FY2021 10-K: the campus purchases (us-gaap PaymentsToAcquirePropertyPlantAndEquipment,
"Purchase of Richmond assets") and the ordinary ones (its own concept). The
list read the first alone: Q1 2022's TTM was $123.8M + $0 - $123.3M = $0.5M.
A SumWhereFiled entry reads the sum where both report a period and the list
before that, where the ordinary line was PaymentsToAcquireProductiveAssets.
"""

from dataclasses import replace
from datetime import date

import pytest

from config import settings
from core.valuation_service import filer_line_concepts
from modeling.statements import Statements, SumWhereFiled
from tests.unit.test_statements import _fact

D = date
PPE = "PaymentsToAcquirePropertyPlantAndEquipment"
OWN = "PaymentsToAcquirePropertyAndEquipmentAndOtherAssets"
PRODUCTIVE = "PaymentsToAcquireProductiveAssets"
M = 1e6


def own(value, end, filed, start, accn):
    return replace(_fact(OWN, value, end, filed, start, accn), taxonomy="csgp")


def costar():
    return [
        # FY2019 10-K: the ordinary line, the only one.
        _fact(PRODUCTIVE, 46.2 * M, D(2019, 12, 31), D(2020, 2, 26), D(2019, 1, 1), "fy19"),
        # FY2020 10-K.
        _fact(PRODUCTIVE, 48.3 * M, D(2020, 12, 31), D(2021, 2, 24), D(2020, 1, 1), "fy20"),
        # FY2021 10-K: both lines, FY2020 and FY2021.
        own(48.3 * M, D(2020, 12, 31), D(2022, 2, 23), D(2020, 1, 1), "fy21"),
        own(65.2 * M, D(2021, 12, 31), D(2022, 2, 23), D(2021, 1, 1), "fy21"),
        _fact(PPE, 0.0, D(2020, 12, 31), D(2022, 2, 23), D(2020, 1, 1), "fy21"),
        _fact(PPE, 123.8 * M, D(2021, 12, 31), D(2022, 2, 23), D(2021, 1, 1), "fy21"),
        # Q1 2022 10-Q: both lines, Q1 2021 and Q1 2022.
        own(10.6 * M, D(2021, 3, 31), D(2022, 4, 28), D(2021, 1, 1), "q122"),
        own(12.4 * M, D(2022, 3, 31), D(2022, 4, 28), D(2022, 1, 1), "q122"),
        _fact(PPE, 123.3 * M, D(2021, 3, 31), D(2022, 4, 28), D(2021, 1, 1), "q122"),
        _fact(PPE, 0.0, D(2022, 3, 31), D(2022, 4, 28), D(2022, 1, 1), "q122"),
    ]


def test_csgps_ttm_reads_both_lines():
    as_of = D(2022, 4, 29)
    value, parts = Statements(costar(), line_concepts=filer_line_concepts("CSGP")).ttm(
        "capex", as_of
    )
    assert value == pytest.approx((65.2 + 123.8 + 12.4 - 10.6 - 123.3) * M)
    assert {p.concept for p in parts} == {f"{PPE} + {OWN}"}


def test_without_it_the_ttm_is_the_campus_line_alone():
    value, _ = Statements(costar()).ttm("capex", D(2022, 4, 29))
    assert value == pytest.approx(0.5 * M)


def test_a_year_before_the_second_line_reads_the_list():
    st = Statements(costar(), line_concepts=filer_line_concepts("CSGP"))
    years = {v.period_end.year: (v.concept, v.value) for v in st.annual("capex", D(2022, 3, 1))}
    assert years[2019] == (PRODUCTIVE, 46.2 * M)
    assert years[2020] == (f"{PPE} + {OWN}", pytest.approx(48.3 * M))
    assert years[2021] == (f"{PPE} + {OWN}", pytest.approx(189.0 * M))


def test_a_plain_sum_would_drop_those_years():
    st = Statements(costar(), line_concepts={"capex": (PPE, OWN)})
    assert [v.period_end.year for v in st.annual("capex", D(2022, 3, 1))] == [2020, 2021]


def test_csgps_line_and_its_own_concept_are_configured():
    assert filer_line_concepts("CSGP")["capex"] == SumWhereFiled((PPE, OWN))
    assert ("csgp", OWN) in settings.EXTENSION_CONCEPTS_BY_CIK[1057352]
    st = Statements(costar(), line_concepts=filer_line_concepts("CSGP"))
    assert {PPE, OWN, PRODUCTIVE} <= st.concepts_read("capex")
