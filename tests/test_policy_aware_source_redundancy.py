from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.priority_source_collection import (
    CRITICAL_REDUNDANCY_SOURCES_BY_LANE,
    PrioritySourceCollectionService,
)
from inefficiency_engine.source_coverage import SourceCoverageObservation, SourceCoveragePlane


def test_policy_disabled_bybit_never_counts_toward_source_redundancy(tmp_path, monkeypatch):
    monkeypatch.setenv("CIE_BYBIT_PUBLIC_ENABLED", "false")
    store = EvidenceStore(tmp_path / "coverage.sqlite")
    plane = SourceCoveragePlane(store)
    now = datetime.now(timezone.utc)

    plane.record(
        SourceCoverageObservation(
            source_id="bybit-catalog",
            lane_id="event_driven",
            observed_at=now,
            healthy=True,
            item_count=10,
            evidence_classes=["timestamped_events", "event_identity"],
        )
    )
    plane.record(
        SourceCoverageObservation(
            source_id="snapshot-governance",
            lane_id="event_driven",
            observed_at=now,
            healthy=True,
            item_count=10,
            evidence_classes=["timestamped_events", "event_identity"],
        )
    )

    lane = next(row for row in plane.snapshot(now=now).lanes if row.lane_id == "event_driven")
    bybit = next(row for row in lane.sources if row["source_id"] == "bybit-catalog")

    assert bybit["state"] == "not_applicable"
    assert bybit["admitted"] is False
    assert bybit["policy_disabled"] is True
    assert lane.admitted_authoritative_source_groups == ["snapshot"]
    assert lane.independent_authoritative_source_count == 1
    assert lane.missing_authoritative_source_count == 1
    assert "bybit-catalog" in lane.policy_disabled_source_ids
    assert lane.source_layer_sufficient is False


def test_base_provider_cache_uses_only_policy_permitted_fallbacks(monkeypatch):
    monkeypatch.setenv("CIE_BYBIT_PUBLIC_ENABLED", "false")
    service = object.__new__(PrioritySourceCollectionService)
    now = datetime.now(timezone.utc)

    class Admissions:
        def latest_by_provider(self, mechanism_id: str):
            providers = {
                "fundamental_onchain": service.ETHEREUM_PROVIDER,
                "event_driven": service.COINBASE_CATALOG_PROVIDER,
                "yield": service.LIDO_PROVIDER,
                "volatility": service.DERIBIT_PROVIDER,
                "liquidation_distress": service.HYPERLIQUID_DISTRESS_PROVIDER,
            }
            provider = providers[mechanism_id]
            return {
                provider: SimpleNamespace(
                    admitted=True,
                    observed_at=now,
                    item_count=7,
                    source_reference=f"test:{provider}",
                )
            }

    service.admissions = Admissions()
    summary = service._fresh_base_provider_summary()

    assert summary is not None
    assert summary["event_driven"]["provider"] == service.COINBASE_CATALOG_PROVIDER
    assert summary["event_driven"]["fallback_used"] is True
    assert (
        summary["liquidation_distress"]["provider"]
        == service.HYPERLIQUID_DISTRESS_PROVIDER
    )
    assert summary["liquidation_distress"]["fallback_used"] is True
    assert all(row["refresh_state"] == "fresh_cached" for row in summary.values())


def test_stale_base_provider_cache_forces_a_real_refresh(monkeypatch):
    monkeypatch.setenv("CIE_BYBIT_PUBLIC_ENABLED", "false")
    service = object.__new__(PrioritySourceCollectionService)
    old = datetime.now(timezone.utc) - timedelta(minutes=10)

    class Admissions:
        def latest_by_provider(self, mechanism_id: str):
            providers = {
                "fundamental_onchain": service.ETHEREUM_PROVIDER,
                "event_driven": service.COINBASE_CATALOG_PROVIDER,
                "yield": service.LIDO_PROVIDER,
                "volatility": service.DERIBIT_PROVIDER,
                "liquidation_distress": service.HYPERLIQUID_DISTRESS_PROVIDER,
            }
            provider = providers[mechanism_id]
            return {
                provider: SimpleNamespace(
                    admitted=True,
                    observed_at=old,
                    item_count=1,
                    source_reference=f"test:{provider}",
                )
            }

    service.admissions = Admissions()
    assert service._fresh_base_provider_summary() is None


def test_five_provider_gap_lanes_prioritize_existing_authoritative_partners():
    lanes = [
        SimpleNamespace(lane_id=lane_id, allocation_source_qualified=False)
        for lane_id in CRITICAL_REDUNDANCY_SOURCES_BY_LANE
    ]
    lanes.append(SimpleNamespace(lane_id="price_discrepancy", allocation_source_qualified=True))
    coverage = SimpleNamespace(lanes=lanes)

    source_ids = PrioritySourceCollectionService._critical_redundancy_source_ids(coverage)

    assert source_ids == {
        "aave-liquidations",
        "snapshot-governance",
        "morpho-markets",
        "okx-options",
        "deribit-option-capacity",
    }
