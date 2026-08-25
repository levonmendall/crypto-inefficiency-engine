from __future__ import annotations

import inspect
from pathlib import Path

from sqlalchemy import Column, Integer, MetaData, Table, Text
from sqlalchemy import inspect as sqlalchemy_inspect

from inefficiency_engine import render_combined_postbind
from inefficiency_engine import runtime_index_maintenance
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.runtime_index_maintenance import (
    BACKGROUND_INDEX_SPECS,
    CONTROL_GATE_INDEX_SPECS,
    CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS,
    CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS,
    INDEX_SPECS,
    POSTGRES_INDEX_LOCK_TIMEOUT_MS,
    POSTGRES_INDEX_STATEMENT_TIMEOUT_MS,
    _create_index_sql,
    _ensure_postgres_index,
    _next_replacement_index_name,
    _statement_timeout_for_index,
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


def test_cycle_history_bucket_index_matches_production_lookup_and_is_concurrent(tmp_path):
    columns = CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS["market_quotes"]
    assert columns == ("venue", "asset", "observed_at", "id")

    statement = _create_index_sql(
        dialect_name="postgresql",
        index_name="ix_runtime_market_quotes_venue_asset_observed_at_id",
        table_name="market_quotes",
        columns=columns,
    )
    assert statement.startswith("CREATE INDEX CONCURRENTLY IF NOT EXISTS")
    assert "market_quotes (venue,asset,observed_at,id)" in statement

    store = EvidenceStore(tmp_path / "cycle-history-index.sqlite")
    result = ensure_runtime_indexes_after_api_bind(
        store,
        index_specs=CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS,
    )
    assert result["complete"] is True
    indexes = {
        item["name"]: tuple(item["column_names"])
        for item in sqlalchemy_inspect(store.engine).get_indexes("market_quotes")
    }
    assert indexes["ix_runtime_market_quotes_venue_asset_observed_at_id"] == columns


class _RecordingDb:
    def __init__(self):
        self.statements: list[str] = []

    def execute(self, statement, _parameters=None):
        self.statements.append(str(statement).strip())
        return None


def test_postgres_invalid_runtime_index_uses_verified_dynamic_replacement_without_drop(
    monkeypatch,
):
    canonical_name = "ix_runtime_market_quotes_venue_asset_observed_at_id"
    replacement_name = f"{canonical_name}_v2"
    replacement_reads = 0

    def fake_state(_db, *, index_name):
        nonlocal replacement_reads
        if index_name == canonical_name:
            return {"valid": False, "ready": True}
        if index_name == replacement_name:
            replacement_reads += 1
            return {"valid": True, "ready": True}
        return None

    monkeypatch.setattr(runtime_index_maintenance, "_postgres_index_state", fake_state)
    monkeypatch.setattr(
        runtime_index_maintenance,
        "_postgres_replacement_index_states",
        lambda _db, *, index_name: {},
    )
    db = _RecordingDb()

    result = _ensure_postgres_index(
        db,
        index_name=canonical_name,
        table_name="market_quotes",
        columns=("venue", "asset", "observed_at", "id"),
        statement_timeout_ms=CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS,
    )

    assert replacement_reads == 1
    assert result["postgres_index_valid"] is True
    assert result["postgres_index_ready"] is True
    assert result["repaired_invalid_index"] is True
    assert result["existing_index_reused"] is False
    assert result["ddl_required"] is True
    assert result["replacement_index_used"] is True
    assert result["canonical_index_name"] == canonical_name
    assert result["effective_index_name"] == replacement_name
    assert result["invalid_index_cleanup_deferred"] is True
    assert result["deferred_invalid_index_name"] == canonical_name
    assert (
        result["postgres_statement_timeout_ms"]
        == CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS
    )
    assert result["postgres_lock_timeout_ms"] == POSTGRES_INDEX_LOCK_TIMEOUT_MS
    assert db.statements[0] == (
        "SET statement_timeout TO "
        f"'{CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS}ms'"
    )
    assert db.statements[1] == f"SET lock_timeout TO '{POSTGRES_INDEX_LOCK_TIMEOUT_MS}ms'"
    assert db.statements[2].startswith(
        "CREATE INDEX CONCURRENTLY ix_runtime_market_quotes_venue_asset_observed_at_id_v2"
    )
    assert "IF NOT EXISTS" not in db.statements[2]
    assert not any(statement.startswith("DROP INDEX") for statement in db.statements)


def test_postgres_reuses_newest_valid_dynamic_replacement(monkeypatch):
    canonical_name = "ix_runtime_market_quotes_venue_asset_observed_at_id"
    replacement_states = {
        f"{canonical_name}_v2": {"valid": False, "ready": False},
        f"{canonical_name}_v3": {"valid": True, "ready": True},
        f"{canonical_name}_v4": {"valid": True, "ready": True},
    }
    monkeypatch.setattr(
        runtime_index_maintenance,
        "_postgres_index_state",
        lambda _db, *, index_name: {"valid": False, "ready": True}
        if index_name == canonical_name
        else None,
    )
    monkeypatch.setattr(
        runtime_index_maintenance,
        "_postgres_replacement_index_states",
        lambda _db, *, index_name: replacement_states,
    )
    db = _RecordingDb()

    result = _ensure_postgres_index(
        db,
        index_name=canonical_name,
        table_name="market_quotes",
        columns=("venue", "asset", "observed_at", "id"),
    )

    assert result["effective_index_name"] == f"{canonical_name}_v4"
    assert result["replacement_index_used"] is True
    assert result["existing_index_reused"] is True
    assert result["ddl_required"] is False
    assert result["invalid_index_cleanup_deferred"] is True
    assert not any(statement.startswith("DROP INDEX") for statement in db.statements)
    assert not any(statement.startswith("CREATE INDEX") for statement in db.statements)


def test_postgres_dynamic_replacement_advances_beyond_exhausted_v2_v3_v4(monkeypatch):
    canonical_name = "ix_runtime_market_quotes_venue_asset_observed_at_id"
    replacement_states = {
        f"{canonical_name}_v2": {"valid": False, "ready": False},
        f"{canonical_name}_v3": {"valid": False, "ready": True},
        f"{canonical_name}_v4": {"valid": False, "ready": False},
    }

    def fake_state(_db, *, index_name):
        if index_name == canonical_name:
            return {"valid": False, "ready": True}
        if index_name == f"{canonical_name}_v5":
            return {"valid": True, "ready": True}
        return None

    monkeypatch.setattr(runtime_index_maintenance, "_postgres_index_state", fake_state)
    monkeypatch.setattr(
        runtime_index_maintenance,
        "_postgres_replacement_index_states",
        lambda _db, *, index_name: replacement_states,
    )
    db = _RecordingDb()

    result = _ensure_postgres_index(
        db,
        index_name=canonical_name,
        table_name="market_quotes",
        columns=("venue", "asset", "observed_at", "id"),
        statement_timeout_ms=CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS,
    )

    assert result["effective_index_name"] == f"{canonical_name}_v5"
    assert result["replacement_versions_observed"] == 3
    assert any(
        statement.startswith(
            "CREATE INDEX CONCURRENTLY "
            "ix_runtime_market_quotes_venue_asset_observed_at_id_v5"
        )
        for statement in db.statements
    )
    assert not any(statement.startswith("DROP INDEX") for statement in db.statements)


def test_dynamic_replacement_name_tracks_highest_catalog_version():
    canonical_name = "ix_runtime_market_quotes_venue_asset_observed_at_id"
    states = {
        f"{canonical_name}_v2": {"valid": False, "ready": False},
        f"{canonical_name}_v4": {"valid": False, "ready": False},
        f"{canonical_name}_v37": {"valid": False, "ready": False},
    }

    assert _next_replacement_index_name(
        index_name=canonical_name,
        existing_states=states,
    ) == f"{canonical_name}_v38"


def test_cycle_history_index_has_longer_but_finite_build_deadline():
    cycle_columns = CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS["market_quotes"]

    assert (
        _statement_timeout_for_index(
            table_name="market_quotes",
            columns=cycle_columns,
        )
        == CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS
    )
    assert (
        CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS
        > POSTGRES_INDEX_STATEMENT_TIMEOUT_MS
    )
    assert CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS == 120_000
    assert (
        _statement_timeout_for_index(
            table_name="opportunities",
            columns=("observed_at",),
        )
        == POSTGRES_INDEX_STATEMENT_TIMEOUT_MS
    )


def test_postgres_valid_runtime_index_is_reused_without_ddl(monkeypatch):
    monkeypatch.setattr(
        runtime_index_maintenance,
        "_postgres_index_state",
        lambda _db, *, index_name: {"valid": True, "ready": True},
    )
    db = _RecordingDb()
    index_name = "ix_runtime_market_quotes_venue_asset_observed_at_id"

    result = _ensure_postgres_index(
        db,
        index_name=index_name,
        table_name="market_quotes",
        columns=("venue", "asset", "observed_at", "id"),
    )

    assert result == {
        "postgres_index_valid": True,
        "postgres_index_ready": True,
        "repaired_invalid_index": False,
        "existing_index_reused": True,
        "ddl_required": False,
        "canonical_index_name": index_name,
        "effective_index_name": index_name,
        "replacement_index_used": False,
    }
    assert db.statements == []


def test_postgres_missing_runtime_index_build_is_database_time_bounded(monkeypatch):
    states = iter([None, {"valid": True, "ready": True}])
    monkeypatch.setattr(
        runtime_index_maintenance,
        "_postgres_index_state",
        lambda _db, *, index_name: next(states),
    )
    db = _RecordingDb()

    result = _ensure_postgres_index(
        db,
        index_name="ix_runtime_market_quotes_venue_observed_at",
        table_name="market_quotes",
        columns=("venue", "observed_at"),
    )

    assert result["ddl_required"] is True
    assert result["repaired_invalid_index"] is False
    assert result["replacement_index_used"] is False
    assert db.statements[0] == (
        f"SET statement_timeout TO '{POSTGRES_INDEX_STATEMENT_TIMEOUT_MS}ms'"
    )
    assert db.statements[1] == f"SET lock_timeout TO '{POSTGRES_INDEX_LOCK_TIMEOUT_MS}ms'"
    assert db.statements[2].startswith(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_runtime_market_quotes_venue_observed_at"
    )


def test_sqlite_runtime_index_maintenance_remains_idempotent(tmp_path):
    store = EvidenceStore(tmp_path / "postbind-indexes.sqlite")

    first = ensure_runtime_indexes_after_api_bind(store)
    second = ensure_runtime_indexes_after_api_bind(store)

    assert first["complete"] is True
    assert second["complete"] is True
    assert first["startup_critical_path"] is False
    assert first["postgres_index_validity_verified"] is False
    indexes = {
        item["name"]
        for item in sqlalchemy_inspect(store.engine).get_indexes("market_quotes")
    }
    assert "ix_runtime_market_quotes_venue_observed_at" in indexes


def test_runtime_index_groups_keep_optional_and_legacy_indexes_out_of_control_gate():
    assert set(CONTROL_GATE_INDEX_SPECS).isdisjoint(BACKGROUND_INDEX_SPECS)
    assert set(INDEX_SPECS) == set(CONTROL_GATE_INDEX_SPECS) | set(BACKGROUND_INDEX_SPECS)
    assert CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS == {
        "market_quotes": ("venue", "asset", "observed_at", "id")
    }
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


def test_canonical_control_waits_for_source_and_cycle_history_indexes():
    control_source = inspect.getsource(render_combined_postbind._control_guard_after_indexes)
    maintenance_source = inspect.getsource(render_combined_postbind._runtime_index_guard)

    assert control_source.index("indexes_ready.wait") < control_source.index(
        "_control_plane_guard"
    )
    assert maintenance_source.index("CONTROL_GATE_INDEX_SPECS") < maintenance_source.index(
        "CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS"
    )
    assert maintenance_source.index(
        "CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS"
    ) < maintenance_source.index("indexes_ready.set()")
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
