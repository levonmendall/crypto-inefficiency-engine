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


def test_coarse_stage_one_installs_runtime_guard_before_canonical_main(monkeypatch) -> None:
    events: list[str] = []
    original_history = coarse.migration.PartitionedMarketHistory

    monkeypatch.setattr(
        coarse,
        "_install_stage_one_runtime_memory_guard",
        lambda: events.append("guard"),
    )
    monkeypatch.setattr(
        coarse.stage_one,
        "main",
        lambda: events.append("main") or 0,
    )
    monkeypatch.setattr(coarse.migration, "PartitionedMarketHistory", original_history)

    assert coarse.main() == 0
    assert events == ["guard", "main"]
    assert coarse.migration.PartitionedMarketHistory is coarse.CoarsePartitionedMarketHistory
