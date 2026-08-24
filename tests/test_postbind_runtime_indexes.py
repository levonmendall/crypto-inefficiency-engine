from __future__ import annotations

import inspect
from pathlib import Path

from sqlalchemy import Column, Integer, MetaData, Table, Text
from sqlalchemy import inspect as sqlalchemy_inspect

from inefficiency_engine import render_combined_postbind
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.runtime_index_maintenance import (
    BACKGROUND_INDEX_SPECS,
    CONTROL_GATE_INDEX_SPECS,
    INDEX_SPECS,
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


def test_runtime_index_groups_keep_optional_and_legacy_indexes_out_of_control_gate():
    assert set(CONTROL_GATE_INDEX_SPECS).isdisjoint(BACKGROUND_INDEX_SPECS)
    assert set(INDEX_SPECS) == set(CONTROL_GATE_INDEX_SPECS) | set(BACKGROUND_INDEX_SPECS)
    assert "maker_shadow_outcomes" not in CONTROL_GATE_INDEX_SPECS
    assert "capital_transfer_outcomes" not in CONTROL_GATE_INDEX_SPECS
    assert "maker_shadow_outcomes" in BACKGROUND_INDEX_SPECS
    assert "capital_transfer_outcomes" in BACKGROUND_INDEX_SPECS
    assert "alpha_forward_events" not in CONTROL_GATE_INDEX_SPECS
    assert "allocation_forward_trials" not in CONTROL_GATE_INDEX_SPECS
    assert "allocation_forward_outcomes" not in CONTROL_GATE_INDEX_SPECS


def test_runtime_index_helper_can_maintain_one_scope_only(tmp_path):
    store = EvidenceStore(tmp_path / "postbind-index-scope.sqlite")
    progress: list[dict[str, object]] = []

    result = ensure_runtime_indexes_after_api_bind(
        store,
        index_specs={"market_quotes": CONTROL_GATE_INDEX_SPECS["market_quotes"]},
        progress=progress.append,
    )

    assert result["complete"] is True
    assert result["requested_tables"] == ["market_quotes"]
    assert progress[0]["phase"] == "starting"
    assert progress[-1]["phase"] == "complete"
    assert progress[-1]["ok"] is True


def test_legacy_auxiliary_table_missing_observed_at_is_terminally_skipped(tmp_path):
    store = EvidenceStore(tmp_path / "legacy-maker-index.sqlite")
    metadata = MetaData()
    Table(
        "maker_shadow_outcomes",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("outcome_id", Text, nullable=False),
        Column("venue", Text, nullable=False),
        Column("asset", Text, nullable=False),
        Column("payload_json", Text, nullable=False),
    )
    metadata.create_all(store.engine)
    progress: list[dict[str, object]] = []

    result = ensure_runtime_indexes_after_api_bind(
        store,
        index_specs={
            "maker_shadow_outcomes": BACKGROUND_INDEX_SPECS["maker_shadow_outcomes"]
        },
        progress=progress.append,
    )

    assert result["complete"] is True
    assert result["failures"] == []
    assert len(result["skipped"]) == 1
    skipped = result["skipped"][0]
    assert skipped["table"] == "maker_shadow_outcomes"
    assert skipped["error_type"] == "SchemaColumnMissing"
    assert skipped["missing_columns"] == ["observed_at"]
    assert skipped["optional"] is True
    assert progress[-1]["phase"] == "skipped"


def test_production_bootstrap_does_not_build_large_runtime_indexes():
    bootstrap_source = inspect.getsource(
        render_combined_postbind.bootstrap_permanent_runtime_schema
    )
    maintenance_source = inspect.getsource(render_combined_postbind._runtime_index_guard)

    assert "_build_control_services" in bootstrap_source
    assert bootstrap_source.index("_build_control_services") < bootstrap_source.index(
        "ensure_durable_control_cache_schema"
    )
    assert "ensure_runtime_indexes_after_api_bind" not in bootstrap_source
    assert maintenance_source.index("_api_is_bound") < maintenance_source.index(
        "ensure_runtime_indexes_after_api_bind"
    )


def test_canonical_control_waits_only_for_required_source_indexes():
    control_source = inspect.getsource(render_combined_postbind._control_guard_after_indexes)
    maintenance_source = inspect.getsource(render_combined_postbind._runtime_index_guard)

    assert control_source.index("indexes_ready.wait") < control_source.index(
        "_control_plane_guard"
    )
    assert maintenance_source.index("CONTROL_GATE_INDEX_SPECS") < maintenance_source.index(
        "indexes_ready.set()"
    )
    assert maintenance_source.index("indexes_ready.set()") < maintenance_source.index(
        "BACKGROUND_INDEX_SPECS"
    )


def test_runtime_index_progress_is_published_before_each_long_build():
    source = inspect.getsource(render_combined_postbind._progress_callback)

    assert '"current_index": row.get("index")' in source
    assert '"control_gate_released": control_gate_released' in source
    assert '"background_indexes_complete": False' in source


def test_render_blueprint_uses_postbind_entrypoint():
    blueprint = Path("render.yaml").read_text()

    assert (
        "startCommand: PYTHONPATH=src python -m "
        "inefficiency_engine.render_combined_postbind"
    ) in blueprint
