"""
Registering index joiners (decision 44, research/new-index-members-2026-10-19.md).

The cases are the ones registering 30 S&P members by hand turned up on
2026-10-19: a rename (SKYD = PSKY), placeholder history (FERG, P), a shell
company's history (VRT), and crypto, where a ticker is not an identifier.
"""

from datetime import date, timedelta

from core.new_members import placeholder_floor, plan_registrations, report

CLASSES = {"sp500": "equity", "dow_jones": "equity", "top_100_crypto": "crypto"}
TODAY = date(2026, 10, 19)


def plan(members, registered=(), ciks=None, registered_ciks=None, renames=None):
    return plan_registrations(
        members,
        CLASSES,
        set(registered),
        ciks or {},
        registered_ciks or {},
        renames or {},
        TODAY,
    )


def test_a_new_equity_member_with_an_unknown_cik_is_registered():
    p = plan({"sp500": ["AAPL", "VYLR"]}, registered={"AAPL"}, ciks={"VYLR": 2128626})
    assert p.register == [("VYLR", 2128626)]
    assert p.index_of["VYLR"] == "sp500"


def test_a_joiner_whose_cik_is_already_registered_is_a_rename_not_a_new_row():
    """SKYD joined on 2026-10-08 under PSKY's CIK: one instrument, renamed."""
    p = plan(
        {"sp500": ["SKYD"]},
        registered={"PSKY"},
        ciks={"SKYD": 2041610},
        registered_ciks={2041610: ["PSKY"]},
    )
    assert p.register == []
    assert p.rename_candidates == [("SKYD", 2041610, ["PSKY"])]


def test_a_joiner_without_a_cik_is_flagged_not_registered():
    p = plan({"sp500": ["NEWCO"]})
    assert p.register == [] and p.no_cik == ["NEWCO"]


def test_crypto_members_are_listed_never_registered():
    p = plan(
        {"top_100_crypto": ["BTC-USD", "PYUSD-USD"]}, registered={"BTC-USD"}, ciks={"PYUSD-USD": 1}
    )
    assert p.register == [] and p.crypto_unregistered == ["PYUSD-USD"]


def test_share_classes_and_dated_renames_match_the_registry_first():
    p = plan(
        {"sp500": ["BRK.B", "FB"]},
        registered={"BRK-B", "META"},
        renames={"FB": ("META", date(2022, 6, 30))},
    )
    # FB is renamed only for lists before 2022-06-30; today it is not, so a
    # snapshot that still says FB would be a new (re-issued) ticker.
    assert p.register == [] and p.no_cik == ["FB"]
    p = plan({"sp500": ["BRK.B"]}, registered={"BRK-B"})
    assert p.register == [] and p.no_cik == []


def _days(n, start=date(2010, 1, 4)):
    return [start + timedelta(days=i) for i in range(n)]


def test_placeholder_prefixes_get_a_suggested_floor():
    # FERG: an untraded line, zero volume on most days, until the listing.
    dates = _days(1000)
    volumes = [0 if i % 3 else 100 for i in range(700)] + [1e6] * 300
    last_zero = max(i for i, v in enumerate(volumes) if v == 0)
    assert placeholder_floor(dates, volumes) == dates[last_zero + 1]
    # P: one zero-volume bar the day before its IPO.
    assert placeholder_floor(dates[:5], [0, 1e6, 1e6, 1e6, 1e6]) == dates[1]


def test_a_halt_deep_in_a_long_history_is_not_a_placeholder():
    # IBKR traded zero volume on two days in 2019, nine years into its series.
    volumes = [1e6] * 2400 + [0, 0] + [1e6] * 100
    assert placeholder_floor(_days(len(volumes)), volumes) is None


def test_clean_runs_exit_zero_and_crypto_alone_never_flags():
    p = plan({"sp500": ["VYLR"], "top_100_crypto": ["PYUSD-USD"]}, ciks={"VYLR": 1})
    lines, code = report(p, {"VYLR": (12, date(2026, 10, 1))}, {}, [])
    assert code == 0
    assert any(line.startswith("REGISTERED VYLR") for line in lines)
    assert any("PYUSD-USD" in line for line in lines)


def test_anything_a_human_must_judge_exits_one():
    rename = plan({"sp500": ["SKYD"]}, ciks={"SKYD": 1}, registered_ciks={1: ["PSKY"]})
    assert report(rename, {}, {}, [])[1] == 1
    assert report(plan({"sp500": ["NEWCO"]}), {}, {}, [])[1] == 1
    fine = plan({"sp500": ["FERG"]}, ciks={"FERG": 2})
    assert report(fine, {"FERG": (4222, date(2010, 1, 5))}, {"FERG": date(2021, 3, 8)}, [])[1] == 1
    assert report(fine, {}, {}, ["FERG"])[1] == 1
