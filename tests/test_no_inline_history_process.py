import inspect

from inefficiency_engine import render_combined, render_combined_runtime


def test_render_parent_does_not_reference_active_history_runtime_as_permanent_child():
    entrypoint_source = inspect.getsource(render_combined)
    runtime_source = inspect.getsource(render_combined_runtime)

    assert "inefficiency_engine.active_volume_runtime" not in entrypoint_source
    assert "inefficiency_engine.active_volume_runtime" not in runtime_source
    assert "inefficiency_engine.disposable_heavy_job" in runtime_source
    assert 'CANONICAL_API_APP = "inefficiency_engine.read_api_card_history_deploy:app"' in entrypoint_source
