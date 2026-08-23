from __future__ import annotations

import inspect
from pathlib import Path

from sqlalchemy import inspect as sqlalchemy_inspect

from inefficiency_engine import render_combined_postbind
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.runtime_index_maintenance import (
    _create_index_sql,
    ensure_runtime_indexes_after_api_bind,
)


def test_postgres_runtime_index_creation_is_concurrent():
    statement = _create_index_sql(
        dialect_name="postgresql",
        index_name="ix_runtime_market_quotes_venue_observed_at",
        table_name="market_quotes",
        columns=("venue", "observed_at"),
    )

    assert statement.startswith("CREATE INDEX CONCURRENTLY IF NOT EXISTS")
    assert "market_quotes (venue,observed_at)" in statement


def test_sqlite_runtime_index_maintenance_remains_idempotent(tmp_path):
    store = EvidenceStore(tmp_path / "postbind-indexes.sqlite")

    first = ensure_runtime_indexes_after_api_bind(store)
    second = ensure_runtime_indexes_after_api_bind(store)

    assert first["complete"] is True
    assert second["complete"] is True
    assert first["startup_critical_path"] is False
    indexes = {
        item["name"]
        for item in sqlalchemy_inspect(store.engine).get_indexes("market_quotes")
    }
    assert "ix_runtime_market_quotes_venue_observed_at" in indexes


def test_production_bootstrap_does_not_build_large_runtime_indexes():
    bootstrap_source = inspect.getsource(
        render_combined_postbind.bootstrap_permanent_runtime_schema
    )
    maintenance_source = inspect.getsource(render_combined_postbind._runtime_index_guard)

    assert "_build_control_services" in bootstrap_source
    assert "ensure_runtime_indexes_after_api_bind" not in bootstrap_source
    assert maintenance_source.index("_api_is_bound") < maintenance_source.index(
        "ensure_runtime_indexes_after_api_bind"
    )


def test_canonical_control_waits_for_postbind_indexes():
    source = inspect.getsource(render_combined_postbind._control_guard_after_indexes)

    assert source.index("indexes_ready.wait") < source.index("_control_plane_guard")


def test_render_blueprint_uses_postbind_entrypoint():
    blueprint = Path("render.yaml").read_text()

    assert (
        "startCommand: PYTHONPATH=src python -m "
        "inefficiency_engine.render_combined_postbind"
    ) in blueprint
