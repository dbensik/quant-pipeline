"""
GET /api/v1/signals/{symbol}

This router had the one bug that made a whole feature subtly wrong: it accepted
strategy parameters and silently ignored them, returning 200 while computing
signals from registry defaults. An overlay built on that drew markers for
different parameters than the chart it sat on.

Phase 3/4 — API router tests
"""

from urllib.parse import quote

from fastapi.testclient import TestClient

RANGE = "start=2024-01-01&end=2024-12-31"


def url(symbol: str, strategy: str, params: str | None = None) -> str:
    path = f"/api/v1/signals/{symbol}?strategy_id={strategy}&{RANGE}"
    return f"{path}&params={quote(params)}" if params else path


def signal_values(body: dict) -> list:
    return [point["signal"] for point in body["signals"]]


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_returns_a_signal_per_bar(client: TestClient):
    body = client.get(url("AAPL", "ma_crossover")).json()
    assert body["symbol"] == "AAPL"
    assert body["count"] == len(body["signals"]) > 0
    assert set(body["signals"][0]) == {"time", "signal", "close"}


def test_signals_are_only_minus_one_zero_or_one(client: TestClient):
    """
    The contract the Backtester and every chart overlay depend on. Null is
    permitted — strategies legitimately emit NaN while warming up, and NaN is
    not valid JSON.
    """
    values = signal_values(client.get(url("AAPL", "mean_reversion")).json())
    assert set(values) <= {-1.0, 0.0, 1.0, None}
    assert any(v is not None for v in values)


def test_response_is_marked_unsigned(client: TestClient):
    """
    These are NOT the Ed25519-signed signals from the gRPC/GraphQL layer. The
    flag exists so no consumer can mistake one for the other; flipping it would
    misrepresent an unverifiable signal as verified.
    """
    assert client.get(url("AAPL", "ma_crossover")).json()["signed"] is False


def test_close_price_accompanies_each_signal(client: TestClient):
    """The overlay plots markers at the close, so it needs both together."""
    body = client.get(url("AAPL", "ma_crossover")).json()
    assert all(point["close"] is not None for point in body["signals"])


def test_include_close_false_omits_prices(client: TestClient):
    body = client.get(url("AAPL", "ma_crossover") + "&include_close=false").json()
    assert all(point["close"] is None for point in body["signals"])


# ---------------------------------------------------------------------------
# Parameters — the regression this suite exists for
# ---------------------------------------------------------------------------

def test_parameters_change_the_actual_signals(client: TestClient):
    """
    THE discriminating test. Asserting only that the echoed `params` match the
    request would pass against the exact bug this fixes: the old router echoed
    defaults and computed from defaults. What proves parameters are honoured is
    that the OUTPUT differs.
    """
    tight = client.get(
        url("AAPL", "ma_crossover", '{"short_window": 5, "long_window": 15}')
    ).json()
    loose = client.get(
        url("AAPL", "ma_crossover", '{"short_window": 40, "long_window": 100}')
    ).json()

    assert signal_values(tight) != signal_values(loose)
    # A shorter pair of windows reacts sooner and flips more often.
    assert _transitions(tight) > _transitions(loose)


def _transitions(body: dict) -> int:
    previous = 0
    count = 0
    for value in signal_values(body):
        if value is None:
            continue
        if value != previous:
            count += 1
        previous = value
    return count


def test_echoed_params_reflect_what_was_used(client: TestClient):
    body = client.get(
        url("AAPL", "mean_reversion", '{"window": 30, "threshold": 2.0}')
    ).json()
    assert body["params"] == {"window": 30, "threshold": 2.0}


def test_partial_params_fill_the_rest_from_defaults(client: TestClient):
    body = client.get(url("AAPL", "ma_crossover", '{"short_window": 15}')).json()
    assert body["params"] == {"short_window": 15, "long_window": 100}


def test_omitted_params_use_registry_defaults(client: TestClient):
    body = client.get(url("AAPL", "ma_crossover")).json()
    assert body["params"] == {"short_window": 40, "long_window": 100}


def test_unknown_param_name_is_rejected_not_ignored(client: TestClient):
    response = client.get(url("AAPL", "ma_crossover", '{"bogus": 1}'))
    assert response.status_code == 422
    assert "Unknown parameter" in response.json()["detail"]


def test_malformed_params_json_is_422(client: TestClient):
    response = client.get(url("AAPL", "ma_crossover", "not-json"))
    assert response.status_code == 422
    assert "not valid JSON" in response.json()["detail"]


def test_params_that_are_not_an_object_are_422(client: TestClient):
    response = client.get(url("AAPL", "ma_crossover", "[1, 2, 3]"))
    assert response.status_code == 422
    assert "JSON object" in response.json()["detail"]


def test_strategy_self_validation_surfaces_as_422(client: TestClient):
    response = client.get(
        url("AAPL", "ma_crossover", '{"short_window": 100, "long_window": 50}')
    )
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------

def test_unknown_symbol_is_404(client: TestClient):
    assert client.get(url("NOPE", "ma_crossover")).status_code == 404


def test_unknown_strategy_is_404(client: TestClient):
    assert client.get(url("AAPL", "not_a_strategy")).status_code == 404


def test_multi_asset_strategy_is_rejected(client: TestClient):
    response = client.get(url("AAPL", "basket_trading"))
    assert response.status_code == 422
    assert "multi-symbol" in response.json()["detail"]


def test_start_after_end_is_422(client: TestClient):
    response = client.get(
        "/api/v1/signals/AAPL?strategy_id=ma_crossover"
        "&start=2024-12-31&end=2024-01-01"
    )
    assert response.status_code == 422


def test_no_bars_in_range_is_422(client: TestClient):
    response = client.get(
        "/api/v1/signals/AAPL?strategy_id=ma_crossover"
        "&start=1990-01-01&end=1990-12-31"
    )
    assert response.status_code == 422


def test_unsound_strategy_returns_its_caveat(client: TestClient):
    body = client.get(url("AAPL", "ml_random_forest")).json()
    assert body["caveat"] and "look-ahead" in body["caveat"].lower()
