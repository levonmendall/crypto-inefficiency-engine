from __future__ import annotations

import inspect
from datetime import datetime, timedelta, timezone

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.heavy_work_lease import HeavyWorkLeaseLedger
from inefficiency_engine.history_batch_job import select_history_batch
from inefficiency_engine.instance_memory import InstanceMemorySnapshot
from inefficiency_engine import lightweight_portfolio_worker


NOW = datetime(2026, 8, 20, 19, 30, tzinfo=timezone.utc)


def test_heavy_work_lease_is_cross_owner_exclusive_and_sequence_is_durable(tmp_path):
    store = EvidenceStore(tmp_path / "lease.sqlite3")
    ledger = HeavyWorkLeaseLedger(store)

    assert ledger.try_acquire("research:1", now=NOW) is True
    assert ledger.current_owner(now=NOW + timedelta(seconds=1)) == "research:1"
    assert ledger.try_acquire("history:1", now=NOW + timedelta(seconds=1)) is False

    assert ledger.next_sequence("research", now=NOW) == 1
    assert ledger.next_sequence("research", now=NOW + timedelta(seconds=1)) == 2
    assert ledger.next_sequence("history", now=NOW) == 1

    ledger.release("research:1")
    assert ledger.try_acquire("history:1", now=NOW + timedelta(seconds=2)) is True
    ledger.release("history:1")


def test_history_batch_is_bounded_incomplete_first_then_oldest():
    assets = tuple(f"A{index:02d}" for index in range(40))
    rows = []
    for index, asset in enumerate(assets):
        rows.append(
            {
                "asset": asset,
                "complete": index >= 6,
                "observed_at": (NOW - timedelta(hours=index)).isoformat(),
            }
        )

    selected = select_history_batch(assets, {"assets": rows}, batch_size=4)

    # A00..A05 are incomplete; among those, the least recently maintained are
    # A05, A04, A03, A02. Completed assets can never displace them.
    assert selected == ("A05", "A04", "A03", "A02")
    assert len(selected) == 4


def test_instance_memory_thresholds_use_aggregate_budget_semantics():
    normal = InstanceMemorySnapshot(
        usage_mb=1200.0,
        limit_mb=2048.0,
        soft_mb=1433.6,
        start_block_mb=1587.2,
        terminate_mb=1689.6,
        source="test",
    )
    assert normal.soft_exceeded is False
    assert normal.start_blocked is False
    assert normal.terminate_required is False

    blocked = InstanceMemorySnapshot(
        usage_mb=1600.0,
        limit_mb=2048.0,
        soft_mb=1433.6,
        start_block_mb=1587.2,
        terminate_mb=1689.6,
        source="test",
    )
    assert blocked.soft_exceeded is True
    assert blocked.start_blocked is True
    assert blocked.terminate_required is False

    terminate = blocked.__class__(
        usage_mb=1700.0,
        limit_mb=2048.0,
        soft_mb=1433.6,
        start_block_mb=1587.2,
        terminate_mb=1689.6,
        source="test",
    )
    assert terminate.terminate_required is True


def test_permanent_portfolio_worker_does_not_construct_research_alpha_factory():
    source = inspect.getsource(lightweight_portfolio_worker)

    assert "ExpandedAlphaFactory" not in source
    assert "DisposableExpandedAlphaFactory" not in source
    assert "_DurableQualifiedStateHandle" in source


def test_render_supervisor_has_no_permanent_history_process():
    from inefficiency_engine import render_combined

    source = inspect.getsource(render_combined.child_commands)
    assert "active_volume_runtime" not in source
    assert "disposable_heavy_job" not in source

    heavy_source = inspect.getsource(render_combined.heavy_commands)
    assert "disposable_heavy_job" in heavy_source
    assert '"research"' in heavy_source
    assert '"history"' in heavy_source
