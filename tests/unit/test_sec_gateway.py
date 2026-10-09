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
    out = {r.symbol: r for r in resolve(["AAPL", "BRK.B", "WBA", "YY", "ZZZZ"], TICKERS, RENAMES)}
    assert out["AAPL"] == Resolution("AAPL", 320193, "ticker_map", "Apple Inc.")
    assert out["BRK.B"].cik == 1067983 and out["BRK.B"].how == "ticker_map_variant:BRK-B"
    assert out["WBA"] == Resolution("WBA", 104207, "rename_csv")
    assert out["YY"].how == "unresolved"  # a rename row without same-CIK evidence is not used
    assert out["ZZZZ"] == Resolution("ZZZZ", None, "unresolved")


def test_a_reassigned_ticker_is_never_looked_up():
    # Today's PARA is Banzai International, not Paramount.
    (r,) = resolve(["PARA"], TICKERS, RENAMES, reassigned={"para"})
    assert r == Resolution("PARA", None, "reassigned_ticker")


def test_variants():
    assert variants("BRK-B") == ["BRK.B", "BRKB"]
    assert variants("AAPL") == []
