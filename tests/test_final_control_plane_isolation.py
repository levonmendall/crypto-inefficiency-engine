from __future__ import annotations

import inspect
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.durable_control_bridge import (
    DurableControlQualifiedOpportunityBridgePublisher,
)
from inefficiency_engine import (
    control_cycle_executor,
    permanent_control_worker,
    permanent_mechanism_worker,
)


@pytest.mark.asyncio
async def test_durable_control_bridge_never_calls_live_cex_dex_qualification():
    now = datetime.now(timezone.utc)
    live_called = False

    async def forbidden_live_cex_dex(*, total_capital_usd):
        nonlocal live_called
        live_called = True
        raise AssertionError("durable control plane must not perform live CEX-DEX qualification")

    class Ledger:
        def __init__(self):
            self.recorded = None

        def latest_active(self):
            return None

        def record(self, snapshot):
            self.recorded = snapshot

    ledger = Ledger()
    publisher = object.__new__(DurableControlQualifiedOpportunityBridgePublisher)
    publisher.core = SimpleNamespace(settings=SimpleNamespace())
    publisher.allocator = SimpleNamespace(
        alpha_factory=None,
        _cex_dex_family_candidates=forbidden_live_cex_dex,
    )
    publisher.ledger = ledger
    publisher._latest_scan = lambda: SimpleNamespace(
        scan_id="source-scan-1",
        completed_at=now,
        opportunities=[],
        executability=[],
    )

    result = await publisher.publish_latest(total_capital_usd=250_000.0)

    assert result is not None
    assert result.source_scan_id == "source-scan-1"
    assert result.candidates == []
    assert ledger.recorded is result
    assert live_called is False


def test_mechanism_worker_no_longer_owns_control_publication():
    source = inspect.getsource(permanent_mechanism_worker._run)

    assert "refresh_canonical_control_plane(" not in source
    assert "canonical_control_plane_refresh" not in source
    assert '"runtime_plane": "mechanism-forward"' in source
    assert '"canonical_control_owned_elsewhere": True' in source


def test_control_worker_is_durable_only_and_independent_of_mechanism_cycle():
    parent_source = inspect.getsource(permanent_control_worker._run)
    executor_source = inspect.getsource(control_cycle_executor.run_one_control_cycle)

    assert "supervisor.run_cycle" in parent_source
    assert "refresh_canonical_control_plane(" in executor_source
    assert "run_evidence_cycle" not in executor_source
    assert "refresh_l2_source_snapshot" not in executor_source
    assert "collect_live_evidence" not in executor_source
    assert '"provider_requests_allowed": False' in parent_source
    assert '"mechanism_forward_dependency": False' in parent_source
