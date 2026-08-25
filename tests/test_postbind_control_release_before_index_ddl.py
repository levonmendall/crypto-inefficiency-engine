from __future__ import annotations

import inspect

from inefficiency_engine import render_combined_postbind


def test_canonical_control_release_precedes_all_runtime_index_ddl():
    source = inspect.getsource(render_combined_postbind._runtime_index_guard)

    release = source.index("indexes_ready.set()")
    post_control_group = source.index("post_control_index_specs = {", release)
    source_strategy_maintenance = source.index(
        "index_specs=index_specs",
        post_control_group,
    )

    assert release < post_control_group
    assert release < source_strategy_maintenance
    assert '"control_gate_released": True' in source[release:]
    assert '"cycle_history_index_authority_required": False' in source[release:]


def test_cycle_history_index_is_only_post_control_background_maintenance():
    source = inspect.getsource(render_combined_postbind._runtime_index_guard)
    release = source.index("indexes_ready.set()")

    cycle_scope = source.index('"post_control_cycle_history"', release)
    cycle_spec = source.index("CYCLE_HISTORY_CONTROL_GATE_INDEX_SPECS", cycle_scope)
    maintenance_call = source.index("ensure_runtime_indexes_after_api_bind", cycle_scope)

    assert release < cycle_scope < cycle_spec < maintenance_call
    assert "control_gate_index_retry_pending" not in source
    assert "building_cycle_history_control_gate_index" not in source
    assert "indexes_ready.clear" not in source


def test_control_supervision_still_waits_for_post_bind_release_event():
    source = inspect.getsource(render_combined_postbind._control_guard_after_indexes)

    assert source.index("indexes_ready.wait") < source.index("_control_plane_guard")
    assert "API-bound post-bind release" in source
    assert "exact cycle-history bucket index" not in source
