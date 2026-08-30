from __future__ import annotations

import threading

from inefficiency_engine import local_persistence_migration_supervisor_repair as repair


def test_coarse_stage_one_command_is_scoped_to_child_launch(monkeypatch) -> None:
    original = list(repair.base.MIGRATION_COMMAND)
    observed = []

    monkeypatch.setattr(
        repair.base,
        "run_local_persistence_migration_supervisor",
        lambda _event: observed.append(list(repair.base.MIGRATION_COMMAND)),
    )

    repair._run_base_supervisor_with_coarse_command(threading.Event())

    assert observed == [repair.COARSE_MIGRATION_COMMAND]
    assert repair.base.MIGRATION_COMMAND == original
    assert original[-1] == "inefficiency_engine.stage_one_local_persistence_migration"
