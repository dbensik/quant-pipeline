"""
Contract checks on the generated OpenAPI document.

These exist because the frontend's types are GENERATED from this document
(`npm run gen:api`). A change that is harmless in Python can silently rename a
TypeScript type and break `npm run build` — which is exactly what happened
when api/routers/compare.py defined a second class called `EquityPoint`.

Phase 5 — React pages for the ported routers
"""

import collections

from fastapi.testclient import TestClient

from api.main import app


def schema_names() -> list[str]:
    with TestClient(app) as _client:
        pass
    return list(app.openapi()["components"]["schemas"])


def test_no_two_models_share_a_class_name():
    """
    THE regression. api/routers/backtest.py and api/routers/compare.py each
    defined `EquityPoint`, with different fields. FastAPI cannot name both
    "EquityPoint", so it qualifies them by module —
    `api__routers__backtest__EquityPoint` and
    `api__routers__compare__EquityPoint`. The frontend imported
    `components['schemas']['EquityPoint']`, which then did not exist, and
    `npm run build` failed. `npx tsc --noEmit` did NOT catch it; only `tsc -b`
    did.

    A qualified name in the document always means two models collided.
    Rename one (see ComparisonEquityPoint, PortfolioEquityPoint).
    """
    qualified = [name for name in schema_names() if "__" in name]
    assert qualified == [], (
        "These schema names were auto-qualified because two Pydantic models "
        f"share a class name: {qualified}. Give one of each pair a distinct "
        "name — the frontend imports these by name."
    )


def test_schema_names_are_unique():
    names = schema_names()
    duplicates = [n for n, count in collections.Counter(names).items() if count > 1]
    assert duplicates == []


def test_every_route_is_tagged():
    """
    Tags group the endpoints in Swagger and in the generated client. An
    untagged route lands in a "default" bucket that reads as an oversight.
    """
    untagged = []
    for path, operations in app.openapi()["paths"].items():
        for method, operation in operations.items():
            if not operation.get("tags"):
                untagged.append(f"{method.upper()} {path}")
    assert untagged == []


def test_every_route_has_a_summary():
    """A summary is what Swagger lists; without one it shows the function name."""
    missing = []
    for path, operations in app.openapi()["paths"].items():
        for method, operation in operations.items():
            if not operation.get("summary"):
                missing.append(f"{method.upper()} {path}")
    assert missing == []


def test_every_advertised_universe_source_is_actually_fetchable():
    """
    api/routers/ingest.py advertised "dow_jones" and "top_100_crypto" — names
    taken from DynamicUniverse's private METHOD names rather than its
    `_source_map` KEYS ("dowjones", "crypto"). Both silently returned [] and
    surfaced as a 503, so two of the four advertised sources never worked.

    This asserts the two lists agree, which is cheaper than discovering it
    from a 503 again.
    """
    from data_pipeline.dynamic_universe import DynamicUniverse

    from api.routers.ingest import UNIVERSE_SOURCES

    known = set(DynamicUniverse()._source_map)
    unmapped = sorted(set(UNIVERSE_SOURCES) - known)
    assert unmapped == [], (
        f"These sources are offered by the API but DynamicUniverse cannot "
        f"resolve them: {unmapped}. Known: {sorted(known)}."
    )
