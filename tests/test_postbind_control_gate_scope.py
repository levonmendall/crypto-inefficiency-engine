from __future__ import annotations

import inspect

from inefficiency_engine import render_combined_postbind


def test_generic_postbind_never_blocks_control_on_exact_cycle_history_index():
    source = inspect.getsource(render_combined_postbind._runtime_index_guard)

    release = source.index("indexes_ready.set()")
    post_control_group = source.index("post_control_index_specs = {")
    source_optimizations = source.index("**CONTROL_GATE_INDEX_SPECS", post_control_group)
    strategy_optimizations = source.index("**BACKGROUND_INDEX_SPECS", post_control_group)

    assert release < post_control_group
    assert release < source_optimizations
    assert release < strategy_optimizations
    assert "CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS" not in source
    assert '"cycle_history_exact_index_owner": "cycle-history-index-maintenance"' in source
    assert '"cycle_history_exact_index_maintained_here": False' in source


def test_optional_post_control_index_failures_cannot_reclose_gate():
    source = inspect.getsource(render_combined_postbind._runtime_index_guard)
    release = source.index("indexes_ready.set()")
    background_retry = source.index('"background_index_retry_pending"')

    assert release < background_retry
    assert '"control_gate_released": True' in source[release:background_retry + 500]
    assert "indexes_ready.clear" not in source


def test_background_index_retry_is_less_aggressive_than_required_gate_retry():
    assert (
        render_combined_postbind.BACKGROUND_INDEX_RETRY_SECONDS
        > render_combined_postbind.INDEX_RETRY_SECONDS
    )
