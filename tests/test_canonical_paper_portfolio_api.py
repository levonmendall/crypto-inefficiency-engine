from inefficiency_engine.api import app


def test_canonical_portfolio_and_operating_routes_are_mounted():
    paths = set(app.openapi()["paths"])
    expected = {
        "/v3/portfolio/canonical",
        "/v3/portfolio/runtime-status",
        "/v3/portfolio/performance",
        "/v3/portfolio/positions",
        "/v3/portfolio/trades",
        "/v3/portfolio/skips",
        "/v3/portfolio/history",
        "/v3/portfolio/attribution",
        "/v3/portfolio/cycle",
        "/v3/operations/certification/latest",
        "/v3/operations/certification/history",
        "/v3/operations/certification/summary",
        "/v3/operations/mechanisms",
        "/v3/operations/action-queue",
    }
    assert expected.issubset(paths)
