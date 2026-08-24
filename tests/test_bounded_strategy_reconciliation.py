from __future__ import annotations

import inspect
from types import SimpleNamespace

from sqlalchemy import event, text

from inefficiency_engine import permanent_control_worker, source_runtime_safety
from inefficiency_engine.bounded_strategy_evidence_runtime import (
    bounded_load_evidence,
    install_control_database_timeouts,
)
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.runtime_index_maintenance import INDEX_SPECS


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        alpha_cross_asset_evidence_weight=0.35,
        alpha_cross_asset_min_local_samples=3,
        alpha_min_forward_samples=30,
        alpha_min_hit_rate_lower_bound=0.50,
        alpha_min_regimes=1,
        alpha_min_regime_mean_return=0.0,
        alpha_multiple_testing_penalty_return=0.0,
        alpha_min_forward_mean_return=0.0,
        operating_certification_min_settled_trials=20,
        operating_certification_min_profitable_rate_lower=0.50,
    )


def _strategy_tables(store: EvidenceStore) -> None:
    with store.engine.begin() as db:
        db.execute(
            text(
                "CREATE TABLE IF NOT EXISTS alpha_forward_events ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "strategy_id TEXT NOT NULL, family TEXT NOT NULL, "
                "event_type TEXT NOT NULL, payload_json TEXT NOT NULL)"
            )
        )
        db.execute(
            text(
                "CREATE TABLE IF NOT EXISTS allocation_forward_trials ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, strategy TEXT NOT NULL, "
                "settlement_supported BOOLEAN NOT NULL)"
            )
        )
        db.execute(
            text(
                "CREATE TABLE IF NOT EXISTS allocation_forward_outcomes ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, strategy TEXT NOT NULL, "
                "payload_json TEXT NOT NULL)"
            )
        )


def test_strategy_reconciliation_aggregates_initial_signal_history_then_reads_tail(tmp_path):
    store = EvidenceStore(tmp_path / "bounded-strategy.sqlite")
    _strategy_tables(store)
    with store.engine.begin() as db:
        db.execute(
            text(
                "INSERT INTO alpha_forward_events "
                "(strategy_id,family,event_type,payload_json) "
                "VALUES ('time_series_momentum_v1','directional_time_series','signal','{}')"
            ),
            [{} for _ in range(5000)],
        )

    statements: list[str] = []

    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        statements.append(" ".join(statement.lower().split()))

    event.listen(store.engine, "before_cursor_execute", before_cursor_execute)
    try:
        first = bounded_load_evidence(store, _settings())
    finally:
        event.remove(store.engine, "before_cursor_execute", before_cursor_execute)

    momentum = next(
        row
        for row in first["trend_momentum"]
        if row["strategy_id"] == "time_series_momentum_v1"
    )
    assert momentum["forward_signal_count"] == 5000
    assert any(
        "count(*)" in statement
        and "from alpha_forward_events" in statement
        and "group by strategy_id, family" in statement
        for statement in statements
    )
    assert not any(
        "select strategy_id, family, event_type, payload_json from alpha_forward_events order by id"
        in statement
        for statement in statements
    )

    with store.engine.begin() as db:
        db.execute(
            text(
                "INSERT INTO alpha_forward_events "
                "(strategy_id,family,event_type,payload_json) "
                "VALUES ('time_series_momentum_v1','directional_time_series','signal','{}')"
            )
        )

    statements.clear()
    event.listen(store.engine, "before_cursor_execute", before_cursor_execute)
    try:
        second = bounded_load_evidence(store, _settings())
    finally:
        event.remove(store.engine, "before_cursor_execute", before_cursor_execute)

    momentum = next(
        row
        for row in second["trend_momentum"]
        if row["strategy_id"] == "time_series_momentum_v1"
    )
    assert momentum["forward_signal_count"] == 5001
    assert any(
        "from alpha_forward_events where id >" in statement
        for statement in statements
    )
    assert not any(
        "group by strategy_id, family" in statement
        for statement in statements
    )


def test_runtime_indexes_cover_strategy_reconciliation_ledgers():
    assert INDEX_SPECS["alpha_forward_events"] == (
        "event_type",
        "strategy_id",
        "family",
    )
    assert INDEX_SPECS["allocation_forward_trials"] == (
        "strategy",
        "settlement_supported",
        "id",
    )
    assert INDEX_SPECS["allocation_forward_outcomes"] == ("strategy", "id")


def test_api_and_control_installer_replaces_unbounded_strategy_loader():
    source = inspect.getsource(
        source_runtime_safety.install_source_coverage_reconciliation_runtime
    )
    assert source.index("install_bounded_strategy_evidence_runtime()") < source.index(
        "_COVERAGE_PATCH_MARKER"
    )


def test_control_process_installs_database_side_deadlines_before_service_graph():
    source = inspect.getsource(permanent_control_worker._run)
    assert source.index("install_control_database_timeouts(") < source.index(
        "_build_control_services("
    )
    assert '"provider_requests_used": 0' in source
    timeout_source = inspect.getsource(install_control_database_timeouts)
    assert 'event.listen(engine, "checkout"' in timeout_source
    assert "SET statement_timeout" in timeout_source
    assert "SET lock_timeout" in timeout_source
