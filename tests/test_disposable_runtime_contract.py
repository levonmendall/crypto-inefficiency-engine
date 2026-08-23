from inefficiency_engine.render_combined import child_commands, heavy_commands


def test_permanent_and_disposable_runtime_roles_are_disjoint():
    permanent = set(child_commands("10000"))
    disposable = set(heavy_commands())
    assert permanent == {"portfolio", "source", "mechanism", "api"}
    assert disposable == {"research", "history"}
    assert permanent.isdisjoint(disposable)
