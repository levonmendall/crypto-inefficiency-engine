from __future__ import annotations

import asyncio
import inspect

from inefficiency_engine import permanent_source_worker_lane_repair
from inefficiency_engine import source_flap_repair as repair
from inefficiency_engine.coinbase_trade_flow import _persist_trade_events_bulk
from inefficiency_engine.evidence import ProviderStatus
from inefficiency_engine.priority_source_models import SourceProbeResult


def _probe(source_id: str = "test-source") -> SourceProbeResult:
    return SourceProbeResult(
        source_id=source_id,
        item_count=1,
        source_reference="https://example.test/source",
        evidence_by_lane={"microstructure": ["trade_flow"]},
        authoritative=True,
        commercial_use_permitted=True,
        point_in_time=True,
        economic_fields_complete=True,
        forward_testable_evidence=True,
        detail={},
    )


def test_generic_source_singleflight_joins_identical_inflight_acquisition():
    repair._INFLIGHT.clear()
    attempts = 0

    async def factory():
        nonlocal attempts
        attempts += 1
        await asyncio.sleep(0.02)
        return _probe()

    async def exercise():
        return await asyncio.gather(
            repair._singleflight_probe(("test-source", 1), factory),
            repair._singleflight_probe(("test-source", 1), factory),
        )

    first, second = asyncio.run(exercise())

    assert attempts == 1
    assert first.source_id == second.source_id == "test-source"
    assert {first.detail["singleflight_joined"], second.detail["singleflight_joined"]} == {
        False,
        True,
    }
    assert first.detail["source_transport_singleflight"] is True
    assert second.detail["source_transport_singleflight"] is True


def test_hot_provider_retry_uses_fresh_awaitable_after_connect_timeout():
    class Registry:
        def __init__(self):
            self.calls = 0

        async def _capture_list(self, provider, awaitable):
            self.calls += 1
            await awaitable
            if self.calls == 1:
                return [], ProviderStatus(
                    provider=provider,
                    ok=False,
                    item_count=0,
                    error_type="ConnectTimeout",
                )
            return [object()], ProviderStatus(
                provider=provider,
                ok=True,
                item_count=1,
                error_type=None,
            )

    registry = Registry()
    created = 0

    def factory():
        nonlocal created
        created += 1

        async def request():
            await asyncio.sleep(0)

        return request()

    rows, status = asyncio.run(
        repair._capture_surface_with_retries(
            registry,
            "coinbase-exchange:ticker",
            factory,
        )
    )

    assert registry.calls == repair.PROVIDER_SURFACE_ATTEMPTS == 2
    assert created == 2
    assert len(rows) == 1
    assert status.ok is True


def test_hot_provider_retry_does_not_retry_non_transport_failure():
    class Registry:
        def __init__(self):
            self.calls = 0

        async def _capture_list(self, provider, awaitable):
            self.calls += 1
            await awaitable
            return [], ProviderStatus(
                provider=provider,
                ok=False,
                item_count=0,
                error_type="ValueError",
            )

    registry = Registry()

    def factory():
        async def request():
            await asyncio.sleep(0)

        return request()

    _, status = asyncio.run(
        repair._capture_surface_with_retries(
            registry,
            "okx-v5:public:funding-rate",
            factory,
        )
    )

    assert registry.calls == 1
    assert status.error_type == "ValueError"


def test_trade_flow_persistence_uses_atomic_conflict_ignore():
    source = inspect.getsource(_persist_trade_events_bulk)
    assert "_conflict_safe_insert" in source
    assert "postgresql" in source
    assert "sqlite" in source


def test_production_installs_flap_repair_after_existing_source_repairs():
    source = inspect.getsource(
        permanent_source_worker_lane_repair.install_remaining_source_lane_repairs
    )
    assert source.index("install_remaining_source_transport_repairs()") < source.index(
        "install_source_refresh_truth_repair()"
    )
    assert source.index("install_source_refresh_truth_repair()") < source.index(
        "install_source_flap_repair()"
    )
    assert "source-validity" in source
    assert "qualification thresholds unchanged" in source
