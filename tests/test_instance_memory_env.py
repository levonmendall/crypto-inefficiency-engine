from __future__ import annotations

from inefficiency_engine.instance_memory import InstanceMemorySnapshot


def test_aggregate_memory_budget_has_distinct_defer_and_terminate_states():
    soft_only = InstanceMemorySnapshot(
        usage_mb=1450.0,
        limit_mb=2048.0,
        soft_mb=1433.6,
        start_block_mb=1587.2,
        terminate_mb=1689.6,
        source="test",
    )
    assert soft_only.soft_exceeded is True
    assert soft_only.start_blocked is False
    assert soft_only.terminate_required is False

    start_blocked = InstanceMemorySnapshot(
        usage_mb=1600.0,
        limit_mb=2048.0,
        soft_mb=1433.6,
        start_block_mb=1587.2,
        terminate_mb=1689.6,
        source="test",
    )
    assert start_blocked.start_blocked is True
    assert start_blocked.terminate_required is False

    terminate = InstanceMemorySnapshot(
        usage_mb=1700.0,
        limit_mb=2048.0,
        soft_mb=1433.6,
        start_block_mb=1587.2,
        terminate_mb=1689.6,
        source="test",
    )
    assert terminate.terminate_required is True
