"""
scripts/capture_option_chains.py — no network.

The vendor is replaced by FakeTicker, which serves a real-shaped chain (the
column set yfinance 1.2.0 returned for SPY on 2026-09-14) and can be told to
fail specific expiries or report a stale bar date. Every behaviour below has
lost, or would lose, days from an archive that cannot be backfilled.
"""

from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pandas as pd
import pytest

from scripts import capture_option_chains as cap

TODAY = date(2026, 9, 15)  # a Tuesday
NOW = datetime(2026, 9, 15, 19, 45, tzinfo=UTC)  # 15:45 ET

VENDOR_COLUMNS = [
    "contractSymbol",
    "lastTradeDate",
    "strike",
    "lastPrice",
    "bid",
    "ask",
    "change",
    "percentChange",
    "volume",
    "openInterest",
    "impliedVolatility",
    "inTheMoney",
    "contractSize",
    "currency",
]


def vendor_frame(n: int, bid: float = 1.0) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "contractSymbol": [f"X{i}" for i in range(n)],
            "lastTradeDate": pd.Timestamp("2026-09-15 19:40", tz="UTC"),
            "strike": [700.0 + i for i in range(n)],
            "lastPrice": 1.5,
            "bid": bid,
            "ask": 1.6,
            "change": 0.0,
            "percentChange": 0.0,
            "volume": 10.0,
            "openInterest": 5,
            "impliedVolatility": 0.2,
            "inTheMoney": False,
            "contractSize": "REGULAR",
            "currency": "USD",
        }
    )


class FakeTicker:
    def __init__(self, symbol, *, expiries, bar_date=TODAY, fail=(), rows=10, fail_times=None):
        self.symbol = symbol
        self._expiries = expiries
        self._bar_date = bar_date
        self._fail = set(fail)
        self._rows = rows
        self._fail_times = dict(fail_times or {})
        self.calls = []

    @property
    def options(self):
        return tuple(self._expiries)

    def history(self, period, interval):
        idx = pd.DatetimeIndex([pd.Timestamp(self._bar_date).tz_localize("America/New_York")])
        return pd.DataFrame({"Close": [1.0]}, index=idx)

    def option_chain(self, expiry):
        self.calls.append(expiry)
        if expiry in self._fail:
            raise RuntimeError("yahoo said no")
        if self._fail_times.get(expiry, 0) > 0:
            self._fail_times[expiry] -= 1
            raise RuntimeError("transient")
        return SimpleNamespace(
            calls=vendor_frame(self._rows),
            puts=vendor_frame(self._rows, bid=0.0),
            underlying={
                "regularMarketPrice": 761.0,
                "regularMarketTime": 1789457100,
                "marketState": "REGULAR",
            },
        )


def exp(days: int) -> str:
    return (TODAY + timedelta(days=days)).isoformat()


def run(tmp_path, fake, **kw):
    return cap.capture_ticker(
        fake.symbol,
        lambda _s: fake,
        root=tmp_path,
        today=TODAY,
        now_utc=NOW,
        max_dte=kw.pop("max_dte", 120),
        min_rows=kw.pop("min_rows", 20),
        sleep=lambda _s: None,
        vendor_version="1.2.0",
        **kw,
    )


# ---------------------------------------------------------------------------
# Expiry planning
# ---------------------------------------------------------------------------


def test_plan_keeps_today_through_max_dte_and_drops_the_rest():
    got = cap.plan_expiries([exp(121), exp(0), exp(120), exp(-1), exp(30)], TODAY, 120)
    assert got == [exp(0), exp(30), exp(120)]


# ---------------------------------------------------------------------------
# What gets written
# ---------------------------------------------------------------------------


def test_writes_raw_vendor_columns_plus_structure_and_provenance(tmp_path):
    fake = FakeTicker("SPY", expiries=[exp(1), exp(8)])
    r = run(tmp_path, fake)
    assert r.status == cap.WRITTEN
    frame = pd.read_parquet(tmp_path / "SPY" / "2026-09-15.parquet")

    assert len(frame) == 40  # 2 expiries x (10 calls + 10 puts)
    assert set(VENDOR_COLUMNS) <= set(frame.columns)
    assert sorted(frame["right"].unique()) == ["C", "P"]
    assert sorted(frame["expiry"].unique()) == [exp(1), exp(8)]
    assert (frame["underlying"] == "SPY").all()
    assert (frame["market_state"] == "REGULAR").all()
    assert (frame["trade_date"] == "2026-09-15").all()
    assert frame["spot"].iloc[0] == 761.0
    assert frame["source"].iloc[0] == "yfinance"


def test_zero_bids_are_kept_not_filtered(tmp_path):
    """Raw means raw: the fake's puts all bid 0.0 and must all survive."""
    run(tmp_path, FakeTicker("SPY", expiries=[exp(1)]))
    frame = pd.read_parquet(tmp_path / "SPY" / "2026-09-15.parquet")
    assert (frame.loc[frame["right"] == "P", "bid"] == 0.0).sum() == 10


# ---------------------------------------------------------------------------
# Skip, partial, retry
# ---------------------------------------------------------------------------


def test_an_existing_capture_is_skipped_without_touching_the_vendor(tmp_path):
    (tmp_path / "SPY").mkdir()
    (tmp_path / "SPY" / "2026-09-15.parquet").write_bytes(b"x")
    fake = FakeTicker("SPY", expiries=[exp(1)])
    assert run(tmp_path, fake).status == cap.SKIPPED
    assert fake.calls == []


def test_skip_is_per_ticker_so_a_half_finished_day_gets_completed(tmp_path):
    """
    The 17:00 safety net after a 15:45 run that wrote SPY and died. A
    date-level skip would see "today exists" and capture nothing for QQQ.
    """
    run(tmp_path, FakeTicker("SPY", expiries=[exp(1)]))
    r = run(tmp_path, FakeTicker("QQQ", expiries=[exp(1)]))
    assert r.status == cap.WRITTEN
    assert (tmp_path / "QQQ" / "2026-09-15.parquet").exists()


def test_a_failed_expiry_writes_a_partial_file_that_does_not_count_as_done(tmp_path):
    fake = FakeTicker("SPY", expiries=[exp(1), exp(8)], fail={exp(8)})
    r = run(tmp_path, fake)
    assert r.status == cap.PARTIAL
    assert r.failed_expiries == [exp(8)]
    assert (tmp_path / "SPY" / "2026-09-15.partial.parquet").exists()
    assert not (tmp_path / "SPY" / "2026-09-15.parquet").exists()

    # Next run: vendor recovered. It must retry, write the final, drop the partial.
    r2 = run(tmp_path, FakeTicker("SPY", expiries=[exp(1), exp(8)]))
    assert r2.status == cap.WRITTEN
    assert (tmp_path / "SPY" / "2026-09-15.parquet").exists()
    assert not (tmp_path / "SPY" / "2026-09-15.partial.parquet").exists()


def test_a_transient_failure_is_retried_within_the_run(tmp_path):
    fake = FakeTicker("SPY", expiries=[exp(1)], fail_times={exp(1): cap.RETRIES - 1})
    assert run(tmp_path, fake).status == cap.WRITTEN
    assert fake.calls.count(exp(1)) == cap.RETRIES


# ---------------------------------------------------------------------------
# Refusals and floors
# ---------------------------------------------------------------------------


def test_a_stale_bar_date_is_refused_and_nothing_is_written(tmp_path):
    """Holiday fire: yfinance serves yesterday's chain without an error."""
    fake = FakeTicker("SPY", expiries=[exp(1)], bar_date=TODAY - timedelta(days=1))
    r = run(tmp_path, fake)
    assert r.status == cap.REFUSED
    assert fake.calls == []
    assert not (tmp_path / "SPY").exists()


def test_an_empty_expiry_list_is_a_failure_not_a_quiet_day(tmp_path):
    r = run(tmp_path, FakeTicker("SPY", expiries=[]))
    assert r.status == cap.FAILED
    assert not (tmp_path / "SPY").exists()


def test_a_short_read_below_the_floor_is_not_written(tmp_path):
    r = run(tmp_path, FakeTicker("SPY", expiries=[exp(1)], rows=2), min_rows=20)
    assert r.status == cap.FAILED
    assert not (tmp_path / "SPY" / "2026-09-15.parquet").exists()


def test_dry_run_writes_nothing(tmp_path):
    r = run(tmp_path, FakeTicker("SPY", expiries=[exp(1)]), dry_run=True)
    assert r.status == cap.WRITTEN
    assert not any(tmp_path.rglob("*.parquet"))


# ---------------------------------------------------------------------------
# Exit code
# ---------------------------------------------------------------------------


def R(status, bar=TODAY):
    return cap.TickerResult("X", status, bar_date=bar)


@pytest.mark.parametrize(
    "statuses,code",
    [
        ([cap.WRITTEN, cap.SKIPPED], 0),
        ([cap.WRITTEN, cap.FAILED], 1),  # "5 of 6, exit 0" is how days vanish
        ([cap.WRITTEN, cap.PARTIAL], 1),
        ([cap.WRITTEN, cap.REFUSED], 1),  # mixed refusal: the market WAS open
        ([], 1),
    ],
)
def test_exit_code(statuses, code):
    assert cap.run_exit_code([R(s) for s in statuses]) == code


def test_every_ticker_refused_on_one_earlier_date_is_a_closed_market():
    y = TODAY - timedelta(days=1)
    assert cap.run_exit_code([R(cap.REFUSED, y), R(cap.REFUSED, y)]) == 0


def test_refusals_on_different_dates_are_not_a_closed_market():
    assert (
        cap.run_exit_code(
            [
                R(cap.REFUSED, TODAY - timedelta(days=1)),
                R(cap.REFUSED, TODAY - timedelta(days=4)),
            ]
        )
        == 1
    )


def test_in_the_money_mixing_bools_and_floats_is_written_as_booleans(tmp_path):
    """2026-10-09: Yahoo sent inTheMoney as False and 0.0 in one column; parquet refused it."""

    class Mixed(FakeTicker):
        def option_chain(self, expiry):
            chain = super().option_chain(expiry)
            chain.calls["inTheMoney"] = pd.Series([False, 0.0, True, 1.0, None] * 2, dtype=object)
            return chain

    r = run(tmp_path, Mixed("SPY", expiries=[exp(1), exp(2)]))
    assert r.status == cap.WRITTEN
    got = pd.read_parquet(cap.archive_path(tmp_path, "SPY", TODAY))
    calls = got[got["right"] == "C"]["inTheMoney"].tolist()
    assert calls[:5] == [False, False, True, True, pd.NA]


def test_a_non_boolean_in_the_money_fails_that_ticker_only(tmp_path):
    """Anything but bool/0/1 is a vendor change: that ticker FAILS, nothing is written, the run goes on."""

    class Odd(FakeTicker):
        def option_chain(self, expiry):
            chain = super().option_chain(expiry)
            chain.calls["inTheMoney"] = pd.Series(["yes"] * 10, dtype=object)
            return chain

    r = run(tmp_path, Odd("SPY", expiries=[exp(1)]))
    assert r.status == cap.FAILED and "not a boolean" in r.detail
    assert not cap.archive_path(tmp_path, "SPY", TODAY).exists()
    assert run(tmp_path, FakeTicker("QQQ", expiries=[exp(1)])).status == cap.WRITTEN


# ---------------------------------------------------------------------------
# History down: the gate falls back to the chain's own market time
# ---------------------------------------------------------------------------

MARKET_TODAY = int(datetime(2026, 9, 15, 19, 40, tzinfo=UTC).timestamp())  # 15:40 ET
MARKET_YESTERDAY = int(datetime(2026, 9, 14, 20, 0, tzinfo=UTC).timestamp())  # 16:00 ET


class HistoryDown(FakeTicker):
    """2026-10-21: history() raised TypeError for every ticker in both runs."""

    def __init__(self, symbol, *, market_times=None, **kw):
        super().__init__(symbol, **kw)
        self._market_times = market_times or {}

    def history(self, period, interval):
        raise TypeError("'NoneType' object is not subscriptable")

    def option_chain(self, expiry):
        chain = super().option_chain(expiry)
        chain.underlying["regularMarketTime"] = self._market_times.get(expiry, MARKET_TODAY)
        return chain


def test_history_down_and_chain_dated_today_is_written_with_the_fallback_gate(tmp_path):
    r = run(tmp_path, HistoryDown("SPY", expiries=[exp(1), exp(8)]))
    assert r.status == cap.WRITTEN
    assert r.gate == cap.GATE_CHAIN_TIME and r.bar_date == TODAY
    assert "history: TypeError" in r.detail
    frame = pd.read_parquet(cap.archive_path(tmp_path, "SPY", TODAY))
    assert len(frame) == 40
    assert (frame["trade_date_gate"] == cap.GATE_CHAIN_TIME).all()
    assert cap.run_exit_code([r]) == 0


def test_a_normal_capture_records_the_daily_bar_gate(tmp_path):
    r = run(tmp_path, FakeTicker("SPY", expiries=[exp(1)]))
    frame = pd.read_parquet(cap.archive_path(tmp_path, "SPY", TODAY))
    assert r.gate == cap.GATE_DAILY_BAR
    assert (frame["trade_date_gate"] == cap.GATE_DAILY_BAR).all()


def test_history_down_and_chain_dated_earlier_is_refused_as_a_closed_market(tmp_path):
    """A holiday with the chart endpoint down: the chain shows the last session, not today."""
    t = {exp(1): MARKET_YESTERDAY, exp(8): MARKET_YESTERDAY}
    results = [
        run(tmp_path, HistoryDown(s, expiries=[exp(1), exp(8)], market_times=t))
        for s in ("SPY", "QQQ")
    ]
    assert all(r.status == cap.REFUSED for r in results)
    assert results[0].bar_date == TODAY - timedelta(days=1)
    assert not any(tmp_path.rglob("*.parquet"))
    assert cap.run_exit_code(results) == 0


def test_history_down_and_one_expiry_dated_earlier_refuses_the_ticker(tmp_path):
    fake = HistoryDown("SPY", expiries=[exp(1), exp(8)], market_times={exp(8): MARKET_YESTERDAY})
    r = run(tmp_path, fake)
    assert r.status == cap.REFUSED and "chain market time is 2026-09-14" in r.detail
    assert not any(tmp_path.rglob("*.parquet"))


def test_history_down_and_no_market_time_fails_rather_than_guessing(tmp_path):
    r = run(tmp_path, HistoryDown("SPY", expiries=[exp(1)], market_times={exp(1): None}))
    assert r.status == cap.FAILED and "no market time" in r.detail
    assert not any(tmp_path.rglob("*.parquet"))


def test_a_stale_bar_that_was_read_is_refused_before_any_chain_is_fetched(tmp_path):
    """The fallback is for a FAILED read only; a bar that answers stale still decides."""
    fake = FakeTicker("SPY", expiries=[exp(1)], bar_date=TODAY - timedelta(days=1))
    r = run(tmp_path, fake)
    assert r.status == cap.REFUSED and r.gate == cap.GATE_DAILY_BAR
    assert fake.calls == []


def test_history_down_and_chains_failing_is_still_a_failure(tmp_path):
    r = run(tmp_path, HistoryDown("SPY", expiries=[exp(1)], fail=[exp(1)]))
    assert r.status == cap.FAILED and "history: TypeError" in r.detail


def test_market_date_reads_the_chain_time_in_new_york():
    # 2026-09-15 02:00 UTC is still 2026-09-14 in New York.
    late = int(datetime(2026, 9, 15, 2, 0, tzinfo=UTC).timestamp())
    assert cap.market_date({"regularMarketTime": late}) == date(2026, 9, 14)
    assert cap.market_date({}) is None and cap.market_date(None) is None
