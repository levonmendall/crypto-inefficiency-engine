from __future__ import annotations

import asyncio
import inspect
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from inefficiency_engine import option_capacity
from inefficiency_engine import permanent_source_worker_lane_repair
from inefficiency_engine import priority_source_collection as priority_sources
from inefficiency_engine import production_source_recovery_v2_runtime as recovery_v2
from inefficiency_engine import source_refresh_truth_repair as repair
from inefficiency_engine.priority_source_models import SourceProbeResult
from inefficiency_engine.provider_gap_collection import ProviderAdmissionObservation
from inefficiency_engine.provider_gap_resilience import ResilientProviderGapCollectionService


def _capacity_probe() -> SourceProbeResult:
    return SourceProbeResult(
        source_id="deribit-option-capacity",
        item_count=16,
        source_reference="https://www.deribit.com/api/v2/public/get_order_book",
        evidence_by_lane={"volatility": ["option_capacity"]},
        authoritative=True,
        commercial_use_permitted=True,
        point_in_time=True,
        economic_fields_complete=True,
        forward_testable_evidence=True,
        detail={
            "option_quote_greek_observation_count": 16,
            "visible_capacity_observation_count": 16,
            "paper_only": True,
            "allocation_authority": False,
        },
    )


class _HeartbeatStore:
    def __init__(self):
        self.heartbeats: list[dict[str, object]] = []

    def record_worker_heartbeat(self, *, worker_id, state, detail):
        self.heartbeats.append(
            {"worker_id": worker_id, "state": state, "detail": detail}
        )


def _direct_service(*, age_seconds: float, healthy: bool = True):
    observed_at = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
    row = SimpleNamespace(
        healthy=healthy,
        observed_at=observed_at,
        evidence_classes=["liquidation_events"],
    )
    store = _HeartbeatStore()
    coverage = SimpleNamespace(
        ledger=SimpleNamespace(
            latest=lambda: {("aave-liquidations", "liquidation_distress"): row}
        ),
        _freshness_seconds=lambda classes: 300.0,
    )
    return SimpleNamespace(store=store, source_coverage=coverage), store


def test_aave_refresh_failure_preserves_still_fresh_success(monkeypatch):
    service, store = _direct_service(age_seconds=180.0)
    original_calls: list[object] = []

    def original(*args, **kwargs):
        original_calls.append((args, kwargs))

    monkeypatch.setattr(repair, "_ORIGINAL_RECORD_FAILURE", original)

    repair._record_failure_preserving_fresh_truth(
        service,
        "aave-liquidations",
        ["liquidation_distress"],
        "https://ethereum-rpc.publicnode.com",
        TimeoutError("refresh missed"),
    )

    assert original_calls == []
    assert len(store.heartbeats) == 1
    heartbeat = store.heartbeats[0]
    assert heartbeat["state"] == "degraded"
    assert heartbeat["detail"]["preserved_previous_source_observation"] is True
    assert heartbeat["detail"]["evidence_freshness_ttl_seconds"] == 300.0
    assert heartbeat["detail"]["fail_closed_when_evidence_stales"] is True


def test_aave_refresh_failure_records_failure_after_evidence_stales(monkeypatch):
    service, store = _direct_service(age_seconds=301.0)
    original_calls: list[object] = []

    def original(*args, **kwargs):
        original_calls.append((args, kwargs))

    monkeypatch.setattr(repair, "_ORIGINAL_RECORD_FAILURE", original)

    repair._record_failure_preserving_fresh_truth(
        service,
        "aave-liquidations",
        ["liquidation_distress"],
        "https://ethereum-rpc.publicnode.com",
        TimeoutError("refresh missed"),
    )

    assert len(original_calls) == 1
    assert store.heartbeats == []


def test_deribit_shared_collection_singleflights_and_reuses_short_success(monkeypatch):
    attempts = 0
    store = object()
    repair._DERIBIT_INFLIGHT.clear()
    repair._DERIBIT_SUCCESS_CACHE.clear()

    async def collector(target_store):
        nonlocal attempts
        assert target_store is store
        attempts += 1
        await asyncio.sleep(0.02)
        return _capacity_probe()

    monkeypatch.setattr(repair, "collect_deribit_option_capacity_resilient", collector)

    async def exercise():
        first, second = await asyncio.gather(
            repair.collect_deribit_option_capacity_shared(store),
            repair.collect_deribit_option_capacity_shared(store),
        )
        third = await repair.collect_deribit_option_capacity_shared(store)
        return first, second, third

    first, second, third = asyncio.run(exercise())

    assert attempts == 1
    assert first.source_id == second.source_id == third.source_id == "deribit-option-capacity"
    assert any(
        probe.detail["singleflight_joined"] is True for probe in (first, second)
    )
    assert third.detail["shared_result_cache_hit"] is True
    assert third.detail["shared_result_ttl_seconds"] == repair.DERIBIT_SHARED_RESULT_TTL_SECONDS


def test_deribit_failed_admission_does_not_replace_fresh_success(monkeypatch):
    store = _HeartbeatStore()
    previous = SimpleNamespace(
        admission_id="good-admission",
        healthy=True,
        observed_at=datetime.now(timezone.utc) - timedelta(seconds=120),
    )
    ledger = SimpleNamespace(
        store=store,
        latest_by_provider=lambda mechanism_id: {
            "deribit:public-option-order-book": previous
        },
    )
    original_calls: list[object] = []

    def original(self, observation):
        original_calls.append(observation)
        return "new-admission"

    monkeypatch.setattr(repair, "_ORIGINAL_ADMISSION_RECORD", original)
    failed = ProviderAdmissionObservation(
        mechanism_id="volatility",
        provider="deribit:public-option-order-book",
        healthy=False,
        item_count=0,
        source_reference="https://www.deribit.com/api/v2/public/get_order_book",
        error_type="TimeoutError",
        detail={"message": "temporary timeout"},
    )

    result = repair._record_admission_preserving_fresh_deribit(ledger, failed)

    assert result == "good-admission"
    assert original_calls == []
    assert store.heartbeats[0]["state"] == "degraded"
    assert store.heartbeats[0]["detail"]["preserved_previous_provider_admission"] is True
    assert store.heartbeats[0]["detail"]["evidence_freshness_ttl_seconds"] == 900.0


def test_deribit_failed_admission_is_recorded_once_prior_success_is_stale(monkeypatch):
    previous = SimpleNamespace(
        admission_id="stale-admission",
        healthy=True,
        observed_at=datetime.now(timezone.utc) - timedelta(seconds=901),
    )
    ledger = SimpleNamespace(
        store=_HeartbeatStore(),
        latest_by_provider=lambda mechanism_id: {
            "deribit:public-option-order-book": previous
        },
    )
    original_calls: list[object] = []

    def original(self, observation):
        original_calls.append(observation)
        return "failed-admission"

    monkeypatch.setattr(repair, "_ORIGINAL_ADMISSION_RECORD", original)
    failed = ProviderAdmissionObservation(
        mechanism_id="volatility",
        provider="deribit:public-option-order-book",
        healthy=False,
        item_count=0,
        source_reference="https://www.deribit.com/api/v2/public/get_order_book",
        error_type="TimeoutError",
    )

    assert repair._record_admission_preserving_fresh_deribit(ledger, failed) == "failed-admission"
    assert len(original_calls) == 1


def test_install_routes_all_deribit_owners_to_shared_collector(monkeypatch):
    original_option_capacity = option_capacity.collect_deribit_option_capacity
    original_priority = priority_sources.collect_deribit_option_capacity
    original_critical = recovery_v2.collect_deribit_option_capacity
    original_provider_gap = ResilientProviderGapCollectionService._collect_deribit_options
    try:
        repair.install_source_refresh_truth_repair()
        assert option_capacity.collect_deribit_option_capacity is repair.collect_deribit_option_capacity_shared
        assert priority_sources.collect_deribit_option_capacity is repair.collect_deribit_option_capacity_shared
        assert recovery_v2.collect_deribit_option_capacity is repair.collect_deribit_option_capacity_shared
        assert (
            ResilientProviderGapCollectionService._collect_deribit_options
            is repair._collect_deribit_options_via_shared_capacity
        )
    finally:
        option_capacity.collect_deribit_option_capacity = original_option_capacity
        priority_sources.collect_deribit_option_capacity = original_priority
        recovery_v2.collect_deribit_option_capacity = original_critical
        ResilientProviderGapCollectionService._collect_deribit_options = original_provider_gap


def test_production_source_child_installs_truth_preservation_after_transport_repair():
    source = inspect.getsource(
        permanent_source_worker_lane_repair.install_remaining_source_lane_repairs
    )
    assert source.index("install_remaining_source_transport_repairs()") < source.index(
        "install_source_refresh_truth_repair()"
    )
    assert "source-validity windows" in source
