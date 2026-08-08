"""
GET /api/v1/strategies

This router touches no database — it serves alpha_models/registry.py directly.
Its contract is what the Streamlit sidebar and the React strategy selector both
build their controls from, so a change here silently changes two UIs.

Phase 3/4 — API router tests
"""

from fastapi.testclient import TestClient


def test_lists_every_registered_strategy(client: TestClient):
    body = client.get("/api/v1/strategies").json()
    assert body["count"] == len(body["strategies"])
    assert body["count"] == 12  # 8 single-asset + 4 multi-asset


def test_single_contract_filter(client: TestClient):
    """
    Both UIs request input_contract=single for per-symbol backtests. If the
    filter broke, they would offer multi-asset strategies that /backtest then
    rejects with a 422.
    """
    body = client.get("/api/v1/strategies?input_contract=single").json()
    assert body["count"] == 8
    assert all(s["input_contract"] == "single" for s in body["strategies"])


def test_multi_contract_filter(client: TestClient):
    body = client.get("/api/v1/strategies?input_contract=multi").json()
    assert {s["id"] for s in body["strategies"]} == {
        "pairs_trading",
        "cointegrated_mean_reversion",
        "basket_trading",
        "index_rebalancing",
    }


def test_each_strategy_carries_a_usable_param_schema(client: TestClient):
    """
    The UIs generate every parameter control from this schema. A missing type,
    label or default means a control that cannot be rendered.
    """
    for strategy in client.get("/api/v1/strategies").json()["strategies"]:
        assert strategy["id"] and strategy["display_name"] and strategy["description"]
        for param in strategy["params"]:
            assert param["name"]
            assert param["type"] in {"int", "float", "str"}
            assert param["default"] is not None
            assert param["label"]


def test_ma_crossover_schema_is_specific(client: TestClient):
    body = client.get("/api/v1/strategies/ma_crossover").json()
    assert body["display_name"] == "Moving Average Crossover"
    params = {p["name"]: p for p in body["params"]}
    assert params["short_window"]["default"] == 40
    assert params["long_window"]["default"] == 100
    assert params["short_window"]["type"] == "int"


def test_unsound_strategy_exposes_its_caveat(client: TestClient):
    """
    ml_random_forest has known look-ahead bias. Both UIs surface `caveat` as a
    warning; dropping it would present un-achievable results as trustworthy.
    """
    body = client.get("/api/v1/strategies/ml_random_forest").json()
    assert body["caveat"]
    assert "look-ahead" in body["caveat"].lower()


def test_sound_strategy_has_no_caveat(client: TestClient):
    assert client.get("/api/v1/strategies/buy_and_hold").json()["caveat"] is None


def test_unknown_strategy_404s_and_lists_valid_ids(client: TestClient):
    response = client.get("/api/v1/strategies/not_a_strategy")
    assert response.status_code == 404
    assert "buy_and_hold" in response.json()["detail"]
