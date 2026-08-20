from pathlib import Path


def test_research_deployment_wrapper_preserves_lazy_read_only_bootstrap():
    source = Path("src/inefficiency_engine/read_api_research_deploy.py").read_text()

    assert "build_read_only_evidence_store" in source
    assert "evidence_module.build_evidence_store = build_read_only_evidence_store" in source
    assert "evidence_module.build_evidence_store = _original_builder" in source
    assert "read_api_research" in source
    assert '"database_check": "deferred_to_readiness"' in source
    assert '"schema_owner": "worker"' in source
    assert '"research_closure": True' in source

    health_body = source.split("def deployment_health():", 1)[1].split(
        "def deployment_readiness():", 1
    )[0]
    readiness_body = source.split("def deployment_readiness():", 1)[1]
    assert "store.ping()" not in health_body
    assert "store.ping()" in readiness_body
