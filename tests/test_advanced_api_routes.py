from inefficiency_engine.api import app


def test_advanced_paper_evidence_allocator_and_alpha_routes_are_registered():
    paths = set(app.openapi()["paths"])
    expected = {
        "/v1/system/capabilities",
        "/v2/alpha/strategies",
        "/v2/alpha/evidence/summary",
        "/v2/alpha/evidence/cycle",
        "/v2/alpha/qualifications/live",
        "/v2/alpha/health/live",
        "/v2/alpha/promoted/live",
        "/v2/alpha/fundamentals/summary",
        "/v1/cex-dex/composite-shadow/summary",
        "/v1/cex-dex/composite-statistical/live",
        "/v1/stablecoins/depth-shadow/cycle",
        "/v1/stablecoins/depth-shadow/summary",
        "/v1/stablecoins/depth-statistical-model",
        "/v1/cex-dex/operational/live",
        "/v1/cex-dex/paper-qualification/live",
        "/v1/cex-dex/allocation/live",
        "/v1/allocation/unified/candidates/live",
        "/v1/allocation/unified/live",
    }
    assert expected.issubset(paths)
