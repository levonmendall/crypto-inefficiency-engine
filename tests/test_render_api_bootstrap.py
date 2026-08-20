from pathlib import Path

from sqlalchemy import inspect

from inefficiency_engine.read_evidence import ReadOnlyEvidenceStore


def test_read_only_evidence_store_is_lazy_and_never_creates_schema(tmp_path):
    database = tmp_path / "read-plane.sqlite3"
    store = ReadOnlyEvidenceStore(database)

    # SQLAlchemy metadata is available to inherited read helpers, but the web
    # process does not become a schema owner merely by importing its app.
    assert "scans" in store.metadata.tables
    assert store.schema_mutation_enabled is False
    assert inspect(store.engine).get_table_names() == []


def test_deployment_health_is_database_independent_and_readiness_is_separate():
    source = Path("src/inefficiency_engine/read_api_deploy.py").read_text()

    assert "evidence_module.build_evidence_store = build_read_only_evidence_store" in source
    assert "evidence_module.build_evidence_store = _original_builder" in source

    health_body = source.split("def deployment_health():", 1)[1].split(
        "def deployment_readiness():", 1
    )[0]
    readiness_body = source.split("def deployment_readiness():", 1)[1]

    assert "store.ping()" not in health_body
    assert '"database_check": "deferred_to_readiness"' in health_body
    assert "store.ping()" in readiness_body


def test_read_only_store_has_bounded_postgres_connection_waits():
    source = Path("src/inefficiency_engine/read_evidence.py").read_text()

    assert 'kwargs["connect_args"] = {"connect_timeout": timeout}' in source
    assert 'kwargs["pool_timeout"] = timeout' in source
    assert ".metadata.create_all(" not in source
