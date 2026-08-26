from __future__ import annotations

import inspect

from inefficiency_engine import render_combined_postbind
from inefficiency_engine.cycle_history_brin_runtime import (
    CYCLE_HISTORY_BRIN_INDEX_NAME,
    CYCLE_HISTORY_BRIN_PAGES_PER_RANGE,
    CYCLE_HISTORY_BRIN_STATEMENT_TIMEOUT_MS,
    _create_brin_index_sql,
)
from inefficiency_engine.runtime_index_maintenance import (
    CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS,
)


def test_cycle_history_brin_ddl_is_concurrent_small_and_bounded():
    statement = _create_brin_index_sql(
        index_name=CYCLE_HISTORY_BRIN_INDEX_NAME,
        if_not_exists=True,
    )

    assert statement.startswith(
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS "
        "ix_runtime_market_quotes_observed_at_brin"
    )
    assert "ON market_quotes USING BRIN (observed_at)" in statement
    assert f"pages_per_range={CYCLE_HISTORY_BRIN_PAGES_PER_RANGE}" in statement
    assert CYCLE_HISTORY_BRIN_PAGES_PER_RANGE == 32
    assert CYCLE_HISTORY_BRIN_STATEMENT_TIMEOUT_MS == 60_000
    assert (
        CYCLE_HISTORY_BRIN_STATEMENT_TIMEOUT_MS
        < CYCLE_HISTORY_POSTGRES_INDEX_STATEMENT_TIMEOUT_MS
    )


def test_postbind_builds_cycle_history_brin_before_large_runtime_indexes():
    source = inspect.getsource(render_combined_postbind._runtime_index_guard)

    release = source.index("indexes_ready.set()")
    brin_call = source.index("brin_result = ensure_cycle_history_brin_after_api_bind")
    source_scope = source.index('scope = "post_control_source_strategy"')
    source_maintenance = source.index(
        "result = ensure_runtime_indexes_after_api_bind",
        source_scope,
    )

    assert release < brin_call < source_scope < source_maintenance
    assert "CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS" not in source
    assert '"cycle_history_exact_index_maintained_here": False' in source
    assert "indexes_ready.clear" not in source


def test_brin_maintenance_is_not_in_startup_or_control_authority_path():
    bootstrap = inspect.getsource(render_combined_postbind.bootstrap_permanent_runtime_schema)
    guard = inspect.getsource(render_combined_postbind._runtime_index_guard)

    assert "ensure_cycle_history_brin_after_api_bind" not in bootstrap
    assert guard.index("indexes_ready.set()") < guard.index(
        "ensure_cycle_history_brin_after_api_bind"
    )
    assert '"cycle_history_index_authority_required": False' in guard
