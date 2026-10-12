"""
core/sec.py and core/cik_resolution.py, with a fake HTTP session: no network.

The gateway's four rules (see its docstring) each have a test here: no
identity without SEC_USER_AGENT, the identity only in a header, stop on the
HTML block page, and requests spaced under the limit.
"""

import pytest

from core import sec as S
from core.cik_resolution import Resolution, renames_from_csv, resolve, ticker_map, variants

UA = "Test Person test@example.com"


class FakeResponse:
    def __init__(self, status=200, body=None, ctype="application/json", text=""):
        self.status_code, self._body, self.headers, self.text = status, body or {}, {"Content-Type": ctype}, text

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), []

    def get(self, url, headers=None, timeout=None):
        self.calls.append((url, headers))
        return self.responses.pop(0)


class FakeClock:
    def __init__(self):
        self.t, self.slept = 0.0, []

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.slept.append(s)
        self.t += s


# --- identity -----------------------------------------------------------------

def test_no_user_agent_means_no_gateway(monkeypatch):
    monkeypatch.setattr(S.settings, "sec_user_agent", lambda: None)
    with pytest.raises(S.SECConfigError, match="SEC_USER_AGENT"):
        S.SECGateway(session=FakeSession([]))


@pytest.mark.parametrize("bad", ["test@example.com", "Just A Name", "Name not-an-email", "  "])
def test_a_user_agent_must_be_a_name_and_an_email(bad):
    with pytest.raises(S.SECConfigError):
        S.SECGateway(session=FakeSession([]), user_agent=bad)


def test_identity_travels_only_in_the_header():
    session = FakeSession([FakeResponse(body={"facts": {}})])
    S.SECGateway(session=session, user_agent=UA).company_facts(320193)
    url, headers = session.calls[0]
    assert url == "https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json"
    assert headers["User-Agent"] == UA
    assert "example.com" not in url and "Test" not in url


# --- refusals -------------------------------------------------------------------

def test_the_html_block_page_stops_the_run():
    page = "<html><head><title>SEC.gov | Your Request Originates from an Undeclared Automated Tool</title>"
    gw = S.SECGateway(session=FakeSession([FakeResponse(403, ctype="text/html", text=page)]), user_agent=UA)
    with pytest.raises(S.SECBlockedError, match="Undeclared Automated Tool"):
        gw.company_tickers()


def test_html_with_a_200_is_still_a_block():
    gw = S.SECGateway(session=FakeSession([FakeResponse(200, ctype="text/html; charset=utf-8", text="<html>")]), user_agent=UA)
    with pytest.raises(S.SECBlockedError):
        gw.submissions(320193)


def test_throttled_and_missing():
    gw = S.SECGateway(session=FakeSession([FakeResponse(429), FakeResponse(404)]), user_agent=UA)
    with pytest.raises(S.SECThrottledError):
        gw.company_facts(1)
    with pytest.raises(LookupError):
        gw.company_facts(2)


# --- rate -------------------------------------------------------------------------

def test_requests_are_spaced_under_the_limit():
    clock = FakeClock()
    gw = S.SECGateway(session=FakeSession([FakeResponse() for _ in range(5)]), user_agent=UA,
                      max_per_second=8, clock=clock, sleep=clock.sleep)
    for _ in range(5):
        gw.get_json("https://data.sec.gov/x")
    assert gw.requests_made == 5
    assert clock.slept == pytest.approx([0.125] * 4)
    assert clock.t / 4 >= 1 / 8 - 1e-12  # never faster than 8 per second


# --- CIK resolution -----------------------------------------------------------

TICKERS = ticker_map({
    "0": {"cik_str": 320193, "ticker": "AAPL", "title": "Apple Inc."},
    "1": {"cik_str": 1067983, "ticker": "BRK-B", "title": "BERKSHIRE HATHAWAY INC"},
    "2": {"cik_str": 1826011, "ticker": "PARA", "title": "Banzai International, Inc."},
})
RENAMES = renames_from_csv([
    {"old_symbol": "WAG", "new_symbol": "WBA", "evidence": "same_cik", "cik": "104207"},
    {"old_symbol": "XX", "new_symbol": "YY", "evidence": "name_only", "cik": "999"},
])


def test_resolution_order_and_provenance():
    out = {r.symbol: r for r in resolve(["AAPL", "BRK.B", "WBA", "YY", "ZZZZ"], TICKERS, RENAMES, overrides={}, predecessors={})}
    assert out["AAPL"] == Resolution("AAPL", 320193, "ticker_map", "Apple Inc.")
    assert out["BRK.B"].cik == 1067983 and out["BRK.B"].how == "ticker_map_variant:BRK-B"
    assert out["WBA"] == Resolution("WBA", 104207, "rename_csv")
    assert out["YY"].how == "unresolved"  # a rename row without same-CIK evidence is not used
    assert out["ZZZZ"] == Resolution("ZZZZ", None, "unresolved")


def test_a_reassigned_ticker_is_never_looked_up():
    # Today's PARA is Banzai International, not Paramount.
    (r,) = resolve(["PARA"], TICKERS, RENAMES, reassigned={"para"}, overrides={}, predecessors={})
    assert r == Resolution("PARA", None, "reassigned_ticker")


def test_overrides_win_and_predecessors_ride_along():
    out = {r.symbol: r for r in resolve(["WBA", "XOMX"], TICKERS, RENAMES,
                                        overrides={"WBA": 1618921}, predecessors={"WBA": (104207,), "XOMX": (34088,)})}
    assert out["WBA"] == Resolution("WBA", 1618921, "override", None, (104207,))
    assert out["XOMX"].cik is None and out["XOMX"].predecessors == ()  # no CIK: nothing to chain onto


def test_variants():
    assert variants("BRK-B") == ["BRK.B", "BRKB"]
    assert variants("AAPL") == []


def test_a_dropped_connection_is_retried_but_a_block_is_not():
    import requests

    class Flaky(FakeSession):
        def get(self, url, headers=None, timeout=None):
            self.calls.append((url, headers))
            item = self.responses.pop(0)
            if isinstance(item, Exception):
                raise item
            return item

    clock = FakeClock()
    ok = Flaky([requests.exceptions.ChunkedEncodingError("Response ended prematurely"), FakeResponse(body={"x": 1})])
    gw = S.SECGateway(session=ok, user_agent=UA, clock=clock, sleep=clock.sleep)
    assert gw.get_json("https://data.sec.gov/a") == {"x": 1} and len(ok.calls) == 2
    always = Flaky([requests.ConnectionError("down")] * 4)
    with pytest.raises(RuntimeError, match="failed 4 times"):
        S.SECGateway(session=always, user_agent=UA, clock=clock, sleep=clock.sleep).get_json("https://data.sec.gov/b")
    blocked = Flaky([FakeResponse(403, ctype="text/html", text="<title>blocked</title>"), FakeResponse()])
    with pytest.raises(S.SECBlockedError):
        S.SECGateway(session=blocked, user_agent=UA, clock=clock, sleep=clock.sleep).get_json("https://data.sec.gov/c")
    assert len(blocked.calls) == 1  # a block is an answer: not retried


# --- daily form index -----------------------------------------------------------

INDEX = """Description:           Daily Index of EDGAR Dissemination Feed by Form Type
Last Data Received:    Oct 9, 2026

Form Type   Company Name                                                  CIK
      Date Filed  File Name
---------------------------------------------------------------------------------------------------------------------------------------------
1-A              Timeplast, Inc.                                               1933967     20261009    edgar/data/1933967/0001104659-26-114962.txt
10-Q             DELTA AIR LINES, INC.                                         27904       20261009    edgar/data/27904/0000027904-26-000038.txt
10-K/A           SOME CO                                                       12345       20261009    edgar/data/12345/0000012345-26-000001.txt
SC 13G/A         VANGUARD GROUP INC                                            102909      20261009    edgar/data/102909/0000102909-26-000001.txt
8-K              APPLE INC                                                     320193      20261009    edgar/data/320193/0000320193-26-000099.txt
"""


def test_the_form_index_parses_multi_word_forms_and_ciks():
    rows = S.parse_form_index(INDEX)
    assert ("10-Q", 27904, S.date(2026, 10, 9)) in rows
    assert ("SC 13G/A", 102909, S.date(2026, 10, 9)) in rows
    assert len(rows) == 5


def test_recent_filers_reads_only_listed_days_and_fact_forms():
    listing = {"directory": {"item": [{"name": "form.20261008.idx"}, {"name": "form.20261009.idx"},
                                      {"name": "master.20261009.idx"}, {"name": "form.20260930.idx"}]}}
    session = FakeSession([FakeResponse(body=listing),
                           FakeResponse(ctype="application/octet-stream", text=""),
                           FakeResponse(ctype="application/octet-stream", text=INDEX)])
    gw = S.SECGateway(session=session, user_agent=UA, clock=FakeClock(), sleep=lambda s: None)
    ciks, days = gw.recent_fact_filers(S.date(2026, 10, 3), S.date(2026, 10, 10))
    assert ciks == {27904, 12345}  # 10-Q and 10-K/A; not the 8-K, 13G or 1-A
    assert days == [S.date(2026, 10, 8), S.date(2026, 10, 9)]  # the weekend is never requested
    assert [u.rsplit("/", 1)[-1] for u, _ in session.calls] == ["index.json", "form.20261008.idx", "form.20261009.idx"]


def test_a_window_across_quarters_lists_both():
    empty = {"directory": {"item": []}}
    session = FakeSession([FakeResponse(body=empty), FakeResponse(body=empty)])
    gw = S.SECGateway(session=session, user_agent=UA, clock=FakeClock(), sleep=lambda s: None)
    gw.recent_fact_filers(S.date(2026, 9, 28), S.date(2026, 10, 2))
    assert [u.split("daily-index/")[1] for u, _ in session.calls] == ["2026/QTR3/index.json", "2026/QTR4/index.json"]


def test_an_html_index_response_is_a_block():
    session = FakeSession([FakeResponse(status=403, ctype="text/html", text="<title>Undeclared Automated Tool</title>")])
    gw = S.SECGateway(session=session, user_agent=UA, clock=FakeClock(), sleep=lambda s: None)
    with pytest.raises(S.SECBlockedError):
        gw.get_text("https://www.sec.gov/Archives/x.idx")
