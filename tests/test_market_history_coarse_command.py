from __future__ import annotations

import threading

from inefficiency_engine import local_persistence_migration_supervisor_repair as repair
from inefficiency_engine import stage_one_local_persistence_migration_coarse as coarse


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


def test_coarse_stage_one_installs_guard_after_stage_one_repair(monkeypatch) -> None:
    events: list[str] = []
    original_history = coarse.migration.PartitionedMarketHistory

    def stage_one_wrapper(*args, **kwargs):
        return {"state": "stage_one_wrapper"}

    def guarded_wrapper(*args, **kwargs):
        return {"state": "guarded_wrapper"}

    def install_stage_one_repair() -> None:
        events.append("stage_one_repair")
        monkeypatch.setattr(coarse.migration, "migrate_engines", stage_one_wrapper)

    def install_runtime_guard() -> None:
        events.append("guard")
        assert coarse.migration.migrate_engines is stage_one_wrapper
        monkeypatch.setattr(coarse.migration, "migrate_engines", guarded_wrapper)

    def migration_main() -> int:
        events.append("migration_main")
        assert coarse.migration.migrate_engines is guarded_wrapper
        return 0

    monkeypatch.setattr(coarse.stage_one, "install_stage_one_repair", install_stage_one_repair)
    monkeypatch.setattr(coarse, "_install_stage_one_runtime_memory_guard", install_runtime_guard)
    monkeypatch.setattr(coarse.migration, "main", migration_main)
    monkeypatch.setattr(coarse.migration, "PartitionedMarketHistory", original_history)

    assert coarse.main() == 0
    assert events == ["stage_one_repair", "guard", "migration_main"]
    assert coarse.migration.PartitionedMarketHistory is coarse.CoarsePartitionedMarketHistory
