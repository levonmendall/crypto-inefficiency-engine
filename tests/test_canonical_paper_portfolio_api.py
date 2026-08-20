from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from inefficiency_engine.api import app
from inefficiency_engine.evidence import EvidenceStore
import inefficiency_engine.canonical_paper_portfolio_api as portfolio_api


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


def test_portfolio_get_routes_do_not_construct_heavy_allocation_graph(tmp_path, monkeypatch):
    def forbidden_heavy_graph(*args, **kwargs):
        raise AssertionError("dashboard GET path constructed the heavy allocation graph")

    monkeypatch.setattr(portfolio_api, "UniversalOpportunityService", forbidden_heavy_graph)
    store = EvidenceStore(tmp_path / "portfolio-read-path.db")
    service = SimpleNamespace(
        settings=SimpleNamespace(shadow_cycle_interval_seconds=30.0),
    )
    local_app = FastAPI()
    local_app.include_router(
        portfolio_api.build_canonical_paper_portfolio_router(
            evidence_store=store,
            service=service,
        )
    )
    client = TestClient(local_app)

    canonical = client.get("/v3/portfolio/canonical")
    performance = client.get("/v3/portfolio/performance")
    runtime = client.get("/v3/portfolio/runtime-status")

    assert canonical.status_code == 200
    assert canonical.json()["available"] is False
    assert performance.status_code == 200
    assert performance.json()["available"] is False
    assert runtime.status_code == 200
    assert runtime.json()["operational"] is False
