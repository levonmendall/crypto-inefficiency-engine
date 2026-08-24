from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from pydantic import BaseModel

from inefficiency_engine.disposable_alpha_factory import DisposableExpandedAlphaFactoryService
from inefficiency_engine.durable_control_alpha import DurableControlAlphaFactoryService
from inefficiency_engine.durable_control_bridge import (
    DurableControlQualifiedOpportunityBridgePublisher,
)


class _Candidate(BaseModel):
    candidate_id: str = "candidate-1"
    strategy_id: str = "strategy-1"
    family: str = "directional_time_series"
    asset: str = "BTC"
    direction: str = "long"
    venue: str = "Coinbase"
    market_kind: str = "spot"
    symbol: str = "BTC-USD"
    value: int = 1


class _Decision(BaseModel):
    value: int


def _factory() -> DurableControlAlphaFactoryService:
    factory = object.__new__(DurableControlAlphaFactoryService)
    factory._durable_stage_reporter = None
    factory._reset_snapshot_promotion_cache()
    return factory


def test_control_alpha_discovery_is_computed_once_and_returned_as_deep_copies(monkeypatch):
    calls = {"count": 0}

    def parent_discover(self, snapshot, *, total_capital_usd):
        calls["count"] += 1
        return [_Candidate()]

    monkeypatch.setattr(
        DisposableExpandedAlphaFactoryService,
        "discover",
        parent_discover,
        raising=False,
    )
    factory = _factory()
    snapshot = SimpleNamespace(
        scan_id="scan-1",
        completed_at=datetime(2026, 8, 24, 16, 0, tzinfo=timezone.utc),
    )

    first = factory.discover(snapshot, total_capital_usd=250_000.0)
    first[0].value = 99
    second = factory.discover(snapshot, total_capital_usd=250_000.0)

    assert calls["count"] == 1
    assert second[0].value == 1
    diagnostics = factory.durable_promotion_diagnostics()
    assert diagnostics["snapshot_discovery_compute_count"] == 1
    assert diagnostics["snapshot_discovery_cache_hits"] == 1
    assert diagnostics["cached_results_returned_as_deep_copies"] is True


def test_control_alpha_qualification_and_health_reuse_exact_candidate_evidence(monkeypatch):
    calls = {"qualification": 0, "health": 0}

    def parent_qualification(self, candidate):
        calls["qualification"] += 1
        return _Decision(value=7)

    def parent_health(self, candidate):
        calls["health"] += 1
        return _Decision(value=11)

    monkeypatch.setattr(
        DisposableExpandedAlphaFactoryService,
        "qualification",
        parent_qualification,
        raising=False,
    )
    monkeypatch.setattr(
        DisposableExpandedAlphaFactoryService,
        "strategy_health",
        parent_health,
        raising=False,
    )
    factory = _factory()
    candidate = _Candidate()

    first_qualification = factory.qualification(candidate)
    first_qualification.value = 100
    second_qualification = factory.qualification(candidate.model_copy(deep=True))
    first_health = factory.strategy_health(candidate)
    first_health.value = 100
    second_health = factory.strategy_health(candidate.model_copy(deep=True))

    assert calls == {"qualification": 1, "health": 1}
    assert second_qualification.value == 7
    assert second_health.value == 11
    diagnostics = factory.durable_promotion_diagnostics()
    assert diagnostics["qualification_compute_count"] == 1
    assert diagnostics["qualification_cache_hits"] == 1
    assert diagnostics["strategy_health_compute_count"] == 1
    assert diagnostics["strategy_health_cache_hits"] == 1
    assert diagnostics["qualification_thresholds_unchanged"] is True
    assert diagnostics["paper_only"] is True


def test_control_alpha_cache_key_changes_across_source_snapshot_boundary(monkeypatch):
    calls = {"count": 0}

    def parent_discover(self, snapshot, *, total_capital_usd):
        calls["count"] += 1
        return [_Candidate(value=calls["count"])]

    monkeypatch.setattr(
        DisposableExpandedAlphaFactoryService,
        "discover",
        parent_discover,
        raising=False,
    )
    factory = _factory()
    first_snapshot = SimpleNamespace(
        scan_id="scan-1",
        completed_at=datetime(2026, 8, 24, 16, 0, tzinfo=timezone.utc),
    )
    second_snapshot = SimpleNamespace(
        scan_id="scan-2",
        completed_at=datetime(2026, 8, 24, 16, 1, tzinfo=timezone.utc),
    )

    first = factory.discover(first_snapshot, total_capital_usd=250_000.0)
    second = factory.discover(second_snapshot, total_capital_usd=250_000.0)

    assert calls["count"] == 2
    assert first[0].value == 1
    assert second[0].value == 2


def test_bridge_routes_alpha_substages_into_exact_control_stage_telemetry():
    observed: list[str] = []
    installed = {}

    class AlphaFactory:
        def set_control_stage_reporter(self, reporter):
            installed["reporter"] = reporter

    bridge = object.__new__(DurableControlQualifiedOpportunityBridgePublisher)
    bridge.allocator = SimpleNamespace(alpha_factory=AlphaFactory())
    bridge._control_stage_reporter = None
    bridge.set_control_stage_reporter(observed.append)

    installed["reporter"]("qualification_compute")

    assert observed == [
        "qualified_bridge:alpha_promotion:qualification_compute"
    ]
