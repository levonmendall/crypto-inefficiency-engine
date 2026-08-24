from __future__ import annotations

import inspect
from types import SimpleNamespace

from sqlalchemy import event, text

from inefficiency_engine import control_cycle_executor, permanent_control_worker, source_runtime_safety
from inefficiency_engine import bounded_strategy_evidence_runtime as bounded_strategy
from inefficiency_engine.bounded_strategy_evidence_runtime import (
    bounded_load_evidence,
    bounded_strategy_evidence_cache_diagnostics,
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


def _momentum(payload):
    return next(
        row
        for row in payload["trend_momentum"]
        if row["strategy_id"] == "time_series_momentum_v1"
    )


def test_strategy_reconciliation_cold_start_is_batched_and_fail_closed(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("CIE_CONTROL_STRATEGY_BOOTSTRAP_BATCH_ROWS", "100")
    store = EvidenceStore(tmp_path / "bounded-strategy.sqlite")
    _strategy_tables(store)
    with store.engine.begin() as db:
        db.execute(
            text(
                "INSERT INTO alpha_forward_events "
                "(strategy_id,family,event_type,payload_json) "
                "VALUES ('time_series_momentum_v1','directional_time_series','signal','{}')"
            ),
            [{} for _ in range(250)],
        )

    statements: list[str] = []

    def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        statements.append(" ".join(statement.lower().split()))

    event.listen(store.engine, "before_cursor_execute", before_cursor_execute)
    try:
        first = bounded_load_evidence(store, _settings())
    finally:
        event.remove(store.engine, "before_cursor_execute", before_cursor_execute)

    first_momentum = _momentum(first)
    assert first_momentum["state"] == "collecting"
    assert first_momentum["evidence_cache_complete"] is False
    assert "historical_evidence_cache_rebuilding" in first_momentum["failed_gates"]
    assert any(
        "from alpha_forward_events" in statement
        and "where id >" in statement
        and "limit" in statement
        for statement in statements
    )
    assert not any(
        "select strategy_id, family, payload_json from alpha_forward_events "
        "where event_type='outcome' order by id"
        in statement
        for statement in statements
    )

    diagnostics = bounded_strategy_evidence_cache_diagnostics()
    cache = diagnostics["caches"][-1]
    assert cache["cache_complete"] is False
    assert cache["alpha_processed_tail"] == 100
    assert cache["alpha_target_tail"] == 250

    second = bounded_load_evidence(store, _settings())
    assert _momentum(second)["evidence_cache_complete"] is False
    third = bounded_load_evidence(store, _settings())
    third_momentum = _momentum(third)
    assert third_momentum["evidence_cache_complete"] is True
    assert third_momentum["forward_signal_count"] == 250

    with store.engine.begin() as db:
        db.execute(
            text(
                "INSERT INTO alpha_forward_events "
                "(strategy_id,family,event_type,payload_json) "
                "VALUES ('time_series_momentum_v1','directional_time_series','signal','{}')"
            )
        )

    fourth = bounded_load_evidence(store, _settings())
    fourth_momentum = _momentum(fourth)
    assert fourth_momentum["evidence_cache_complete"] is True
    assert fourth_momentum["forward_signal_count"] == 251


def test_strategy_bootstrap_progress_survives_executor_process_cache_reset(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("CIE_CONTROL_CACHE_NAMESPACE", "test-control")
    monkeypatch.setenv("CIE_CONTROL_STRATEGY_BOOTSTRAP_BATCH_ROWS", "100")
    database = tmp_path / "durable-bounded-strategy.sqlite"
    bounded_strategy._CACHE.clear()
    store = EvidenceStore(database)
    _strategy_tables(store)
    with store.engine.begin() as db:
        db.execute(
            text(
                "INSERT INTO alpha_forward_events "
                "(strategy_id,family,event_type,payload_json) "
                "VALUES ('time_series_momentum_v1','directional_time_series','signal','{}')"
            ),
            [{} for _ in range(250)],
        )

    assert _momentum(bounded_load_evidence(store, _settings()))[
        "evidence_cache_complete"
    ] is False
    assert bounded_strategy_evidence_cache_diagnostics()["caches"][-1][
        "alpha_processed_tail"
    ] == 100

    store.engine.dispose()
    bounded_strategy._CACHE.clear()
    restarted_store = EvidenceStore(database)
    assert _momentum(bounded_load_evidence(restarted_store, _settings()))[
        "evidence_cache_complete"
    ] is False
    second = bounded_strategy_evidence_cache_diagnostics()["caches"][-1]
    assert second["durable_checkpoint_loaded"] is True
    assert second["alpha_processed_tail"] == 200

    restarted_store.engine.dispose()
    bounded_strategy._CACHE.clear()
    final_store = EvidenceStore(database)
    completed = _momentum(bounded_load_evidence(final_store, _settings()))
    assert completed["evidence_cache_complete"] is True
    assert completed["forward_signal_count"] == 250


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
    source = inspect.getsource(control_cycle_executor.run_one_control_cycle)
    assert source.index("install_control_database_timeouts(") < source.index(
        "_build_control_services("
    )
    parent_source = inspect.getsource(permanent_control_worker._run)
    assert '"provider_requests_used": 0' in parent_source
    timeout_source = inspect.getsource(install_control_database_timeouts)
    assert 'event.listen(engine, "checkout"' in timeout_source
    assert "SET statement_timeout" in timeout_source
    assert "SET lock_timeout" in timeout_source
