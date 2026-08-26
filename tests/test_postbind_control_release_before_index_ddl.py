from __future__ import annotations

import inspect

from inefficiency_engine import render_combined_postbind


def test_canonical_control_release_precedes_all_generic_runtime_index_ddl():
    source = inspect.getsource(render_combined_postbind._runtime_index_guard)

    release = source.index("indexes_ready.set()")
    post_control_group = source.index("post_control_index_specs = {", release)
    source_strategy_maintenance = source.index(
        "index_specs=post_control_index_specs",
        post_control_group,
    )

    assert release < post_control_group
    assert release < source_strategy_maintenance
    assert '"control_gate_released": True' in source[release:]
    assert '"cycle_history_index_authority_required": False' in source[release:]


def test_exact_cycle_history_index_is_absent_from_generic_post_control_maintenance():
    source = inspect.getsource(render_combined_postbind._runtime_index_guard)
    release = source.index("indexes_ready.set()")
    brin_scope = source.index('"post_control_cycle_history_brin"', release)

    assert release < brin_scope
    assert "CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS" not in source
    assert '"cycle_history_exact_index_owner": "cycle-history-index-maintenance"' in source
    assert '"cycle_history_exact_index_maintained_here": False' in source
    assert "control_gate_index_retry_pending" not in source
    assert "building_cycle_history_control_gate_index" not in source
    assert "indexes_ready.clear" not in source


def test_control_supervision_still_waits_for_post_bind_release_event():
    source = inspect.getsource(render_combined_postbind._control_guard_after_indexes)

    assert source.index("indexes_ready.wait") < source.index("_control_plane_guard")
    assert "API-bound post-bind release" in source
    assert "exact cycle-history bucket index" not in source
