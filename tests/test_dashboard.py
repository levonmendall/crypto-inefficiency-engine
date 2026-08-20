from fastapi.testclient import TestClient

from inefficiency_engine.api import app


def test_dashboard_is_available_at_root_and_dashboard_path():
    client = TestClient(app)
    for path in ("/", "/dashboard"):
        response = client.get(path)
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/html")
        assert "Portfolio Command Center" in response.text
        assert "PAPER · $250K GENESIS" in response.text
        assert "AUTO PAPER EXECUTION · ON" in response.text
        assert "LIVE MONEY · DISABLED" in response.text
        assert "/v3/portfolio/canonical" in response.text
        assert "/v3/portfolio/runtime-status" in response.text
        assert "/v3/portfolio/skips?limit=20" in response.text
        assert "/v3/operations/mechanisms" in response.text
        assert "Opportunity families" in response.text
        assert "valuationStatus" in response.text
        assert "settlement_evidence_blocked_count" in response.text
        assert "awaiting post-horizon settlement evidence" in response.text
        assert "Evidence accumulation" in response.text
        assert "Read-only progress · thresholds unchanged" in response.text
        assert "renderEvidenceProgress" in response.text
        assert "independent_forward_outcomes" in response.text
        assert "settled_allocator_outcomes" in response.text
        assert "Authoritative data" in response.text
        assert "Executable now" in response.text

        # Primary portfolio surfaces tolerate brief Render/API proxy restarts.
        assert "resilientJSON('/v3/portfolio/canonical','portfolio'" in response.text
        assert "resilientJSON('/v3/portfolio/performance','performance'" in response.text
        assert "resilientJSON('/v3/portfolio/runtime-status','runtime'" in response.text
        assert "e.status===502||e.status===503||e.status===504||e.status===429" in response.text
        assert "sessionStorage.setItem(`cie-dashboard-${key}`" in response.text
        assert "DASHBOARD_CACHE_TTL_MS=15*60*1000" in response.text
        assert "Temporary API issue; showing last known portfolio data where available" in response.text
        assert "Primary API calls retry transient failures and preserve short-lived last-known-good display" in response.text


def test_dashboard_routes_are_hidden_from_openapi_but_portfolio_api_remains_visible():
    paths = set(app.openapi()["paths"])
    assert "/" not in paths
    assert "/dashboard" not in paths
    assert "/v3/portfolio/skips" in paths
    assert "/v3/portfolio/runtime-status" in paths
    assert "/v3/portfolio/integrity/history" in paths
