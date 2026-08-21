import inspect

from inefficiency_engine import render_combined


def test_render_parent_does_not_reference_active_history_runtime_as_permanent_child():
    source = inspect.getsource(render_combined)
    assert "inefficiency_engine.active_volume_runtime" not in source
    assert "inefficiency_engine.disposable_heavy_job" in source
