from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.models import MarketKind, MarketQuote
from inefficiency_engine.qualified_opportunity import (
    QualifiedOpportunityLedger,
    QualifiedOpportunitySnapshot,
)
from inefficiency_engine.qualified_opportunity_freshness import (
    FreshnessSeparatedQualifiedOpportunityAllocatorService,
    FreshnessSeparatedQualifiedOpportunityBridgePublisher,
)
from inefficiency_engine.unified_allocation import UnifiedPaperCandidate


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _alpha_candidate(at: datetime) -> UnifiedPaperCandidate:
    return UnifiedPaperCandidate(
        candidate_id="alpha:v372:btc",
        family="alpha",
        strategy="time_series_momentum_v1",
        asset="BTC",
        venues=["Coinbase"],
        capital_required_usd=25_000.0,
        notional_usd_per_leg=25_000.0,
        expected_profit_usd_per_deployment=250.0,
        expected_return_on_reserved_capital=0.01,
        modeled_holding_hours=6.0,
        source_return_metric="forward_ci_health_haircut_net_return",
        source_return_value=0.01,
        exposure_kind="directional_long",
        source_observed_at=at,
        instrument_symbol="BTC-USD",
        instrument_market_kind="spot",
        entry_reference_price=60_000.0,
        modeled_roundtrip_cost_return=0.001,
        conflict_keys=["venue-symbol:Coinbase:BTC-USD"],
    )


def _allocator(store: EvidenceStore, settings: Settings):
    return FreshnessSeparatedQualifiedOpportunityAllocatorService(
        SimpleNamespace(settings=settings),  # type: ignore[arg-type]
        None,
        SimpleNamespace(store=store),  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_stale_candidate_is_cash_not_family_degradation(tmp_path):
    now = _now()
    settings = Settings(max_quote_age_seconds=120.0)
    store = EvidenceStore(tmp_path / "stale-candidate.sqlite3")
    QualifiedOpportunityLedger(store).record(
        QualifiedOpportunitySnapshot(
            observed_at=now - timedelta(minutes=3),
            expires_at=now + timedelta(minutes=7),
            source_scan_id="bridge-control-active",
            total_capital_usd=250_000.0,
            candidates=[_alpha_candidate(now - timedelta(minutes=3))],
        )
    )

    plan = await _allocator(store, settings).allocate(total_capital_usd=250_000.0)

    assert plan.allocations == []
    assert plan.unused_cash_usd == pytest.approx(250_000.0)
    assert plan.family_failures == []
    assert len(plan.skipped) == 1
    assert plan.skipped[0]["candidate_id"] == "alpha:v372:btc"
    assert plan.skipped[0]["reason"] == "candidate evidence stale; awaiting fresh research qualification"


@pytest.mark.asyncio
async def test_fresh_candidate_remains_allocatable_inside_control_envelope(tmp_path):
    now = _now()
    settings = Settings(max_quote_age_seconds=120.0)
    store = EvidenceStore(tmp_path / "fresh-candidate.sqlite3")
    QualifiedOpportunityLedger(store).record(
        QualifiedOpportunitySnapshot(
            observed_at=now - timedelta(seconds=20),
            expires_at=now + timedelta(minutes=9),
            source_scan_id="bridge-control-active",
            total_capital_usd=250_000.0,
            candidates=[_alpha_candidate(now - timedelta(seconds=20))],
        )
    )

    plan = await _allocator(store, settings).allocate(total_capital_usd=250_000.0)

    assert len(plan.allocations) == 1
    assert plan.allocations[0].candidate_id == "alpha:v372:btc"
    assert plan.family_failures == []
    assert plan.allocated_capital_usd == pytest.approx(25_000.0)


@pytest.mark.asyncio
async def test_expired_control_envelope_still_fails_closed_as_real_degradation(tmp_path):
    now = _now()
    settings = Settings(max_quote_age_seconds=120.0)
    store = EvidenceStore(tmp_path / "expired-control.sqlite3")
    QualifiedOpportunityLedger(store).record(
        QualifiedOpportunitySnapshot(
            observed_at=now - timedelta(minutes=12),
            expires_at=now - timedelta(seconds=1),
            source_scan_id="bridge-control-expired",
            total_capital_usd=250_000.0,
            candidates=[],
        )
    )

    plan = await _allocator(store, settings).allocate(total_capital_usd=250_000.0)

    assert plan.allocations == []
    assert len(plan.family_failures) == 1
    assert plan.family_failures[0]["family"] == "qualified_opportunity_bridge"
    assert plan.family_failures[0]["error_type"] == "QualifiedOpportunitySnapshotUnavailableOrStale"


@pytest.mark.asyncio
async def test_publisher_control_ttl_outlives_candidate_market_ttl(tmp_path):
    now = _now()
    settings = Settings(max_quote_age_seconds=120.0)
    store = EvidenceStore(tmp_path / "publisher-control.sqlite3")
    store.record_scan(
        funding_quotes=[],
        market_quotes=[
            MarketQuote(
                venue="Coinbase",
                asset="BTC",
                market_kind=MarketKind.SPOT,
                symbol="BTC-USD",
                mid=60_000.0,
                observed_at=now,
                source="test",
            )
        ],
        opportunities=[],
        providers=[],
        started_at=now,
        completed_at=now,
        order_books=[],
        executability=[],
    )

    core = SimpleNamespace(settings=settings)
    publisher = FreshnessSeparatedQualifiedOpportunityBridgePublisher(
        core,  # type: ignore[arg-type]
        store,
        SimpleNamespace(alpha_factory=None),  # type: ignore[arg-type]
    )
    published = await publisher.publish_latest(total_capital_usd=250_000.0)

    assert published is not None
    control_ttl = (published.expires_at - _now()).total_seconds()
    assert control_ttl > settings.max_quote_age_seconds
    assert control_ttl >= 590.0
    assert published.candidates == []
