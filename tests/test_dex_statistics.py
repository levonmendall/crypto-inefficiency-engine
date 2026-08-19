from datetime import datetime, timedelta, timezone

from inefficiency_engine.cex_dex_evidence import CexDexCompositeEvidence
from inefficiency_engine.config import Settings
from inefficiency_engine.dex_frontier import DexRouteSizeFrontier, DexRouteSizePoint
from inefficiency_engine.dex_shadow import DexRouteShadowCycle, DexRouteShadowObservation
from inefficiency_engine.dex_statistics import (
    build_dex_statistical_qualification,
    qualify_cex_dex_research_evidence,
)


NOW = datetime(2026, 8, 19, 3, 0, tzinfo=timezone.utc)


def _observation(cycle_id: str, *, notional: float = 1000.0, deterioration: float = 5.0, survived: bool = True):
    return DexRouteShadowObservation(
        cycle_id=cycle_id,
        route_signature=f"Velora:1:ETH:buy_asset:{cycle_id}",
        asset="ETH",
        direction="buy_asset",
        source_amount_raw=str(int(notional * 1_000_000)),
        quote_notional_usd_proxy=notional,
        delay_seconds=5.0,
        started_at=NOW,
        verified_at=NOW + timedelta(seconds=5),
        initial_record_id=f"initial-{cycle_id}",
        verification_record_id=f"verify-{cycle_id}" if survived else None,
        survived=survived,
        initial_effective_asset_price=4000.0,
        verification_effective_asset_price=(4000.0 * (1 + deterioration / 10_000.0)) if survived else None,
        price_deterioration_bps=deterioration if survived else None,
        route_changed=False if survived else None,
    )


def _cycle(index: int, *, duplicates: int = 1, notional: float = 1000.0, deterioration: float = 5.0, survived: bool = True):
    cycle_id = f"cycle-{index}"
    return DexRouteShadowCycle(
        cycle_id=cycle_id,
        started_at=NOW,
        completed_at=NOW + timedelta(seconds=6),
        horizons_seconds=[5.0],
        initial_quote_count=1,
        observations=[
            _observation(cycle_id, notional=notional, deterioration=deterioration, survived=survived)
            for _ in range(duplicates)
        ],
    )


def _frontier(index: int, *, target: float = 1000.0, acceptable: bool = True):
    return DexRouteSizeFrontier(
        frontier_id=f"frontier-{index}",
        asset="ETH",
        direction="buy_asset",
        reference_price=4000.0,
        requested_notionals_usd=[target],
        deterioration_limit_bps=25.0,
        points=[
            DexRouteSizePoint(
                target_notional_usd=target,
                quoted=acceptable,
                within_deterioration_limit=acceptable,
                contiguous_acceptable=acceptable,
                failure_type=None if acceptable else "QuoteUnavailable",
            )
        ],
        largest_successful_tier_usd=target if acceptable else None,
        largest_contiguous_acceptable_tier_usd=target if acceptable else None,
        observed_at=NOW + timedelta(seconds=index),
    )


def _strong_evidence(*, deterioration: float = 5.0):
    cycles = [_cycle(i, deterioration=deterioration) for i in range(30)]
    frontiers = [_frontier(i) for i in range(30)]
    return cycles, frontiers


def test_strong_independent_evidence_passes_statistical_gate():
    cycles, frontiers = _strong_evidence()
    model = build_dex_statistical_qualification(
        cycles,
        frontiers,
        asset="ETH",
        direction="buy_asset",
        target_notional_usd=1000.0,
        settings=Settings(),
    )
    assert model.shadow_effective_sample_count == 30
    assert model.frontier_effective_sample_count == 30
    assert model.adverse_tail_sample_count == 30
    assert model.survival.probability == 1.0
    assert model.survival.ci_lower is not None and model.survival.ci_lower > 0.80
    assert model.frontier_acceptance.ci_lower is not None and model.frontier_acceptance.ci_lower > 0.80
    assert model.p95_adverse_deterioration_bps == 5.0
    assert model.statistically_qualified is True
    assert model.allocation_eligible is False
    assert model.executable_eligible is False


def test_duplicate_rows_inside_one_cycle_do_not_inflate_effective_sample_size():
    cycles = [_cycle(1, duplicates=50)]
    frontiers = [_frontier(1)]
    model = build_dex_statistical_qualification(
        cycles,
        frontiers,
        asset="ETH",
        direction="buy_asset",
        target_notional_usd=1000.0,
        settings=Settings(),
    )
    assert model.shadow_effective_sample_count == 1
    assert model.frontier_effective_sample_count == 1
    assert model.statistically_qualified is False
    assert any("shadow effective samples" in reason for reason in model.reasons)


def test_larger_tier_cannot_borrow_shadow_evidence_from_smaller_notional():
    cycles = [_cycle(i, notional=1000.0) for i in range(30)]
    frontiers = [_frontier(i, target=5000.0) for i in range(30)]
    model = build_dex_statistical_qualification(
        cycles,
        frontiers,
        asset="ETH",
        direction="buy_asset",
        target_notional_usd=5000.0,
        settings=Settings(),
    )
    assert model.shadow_effective_sample_count == 0
    assert model.frontier_effective_sample_count == 30
    assert model.statistically_qualified is False


def test_p95_adverse_deterioration_fails_closed_even_when_quotes_survive():
    cycles, frontiers = _strong_evidence(deterioration=40.0)
    model = build_dex_statistical_qualification(
        cycles,
        frontiers,
        asset="ETH",
        direction="buy_asset",
        target_notional_usd=1000.0,
        settings=Settings(),
    )
    assert model.survival.probability == 1.0
    assert model.p95_adverse_deterioration_bps == 40.0
    assert model.statistically_qualified is False
    assert any("p95 adverse route deterioration" in reason for reason in model.reasons)


def test_research_qualification_never_grants_allocation_or_execution_authority():
    cycles, frontiers = _strong_evidence()
    settings = Settings()
    model = build_dex_statistical_qualification(
        cycles,
        frontiers,
        asset="ETH",
        direction="buy_asset",
        target_notional_usd=1000.0,
        settings=settings,
    )
    evidence = CexDexCompositeEvidence(
        evidence_id="evidence-1",
        frontier_id="frontier-current",
        asset="ETH",
        route_direction="buy_asset",
        target_notional_usd=1000.0,
        route_contiguous_acceptable=True,
        cex_venue="Coinbase",
        cex_symbol="ETH-USD",
        cex_quote_currency="USD",
        cex_reference_price=4010.0,
        route_quote_currency="USDC",
        route_effective_asset_price=4000.0,
        route_quote_notional_usd_proxy=1000.0,
        conversion_risk_haircut_bps=2.0,
        cex_taker_fee_bps=5.0,
        gas_cost_bps=1.0,
        gross_edge_after_conversion_depth_bps=50.0,
        net_research_edge_bps=42.0,
        observed_at=NOW,
        evidence_complete=True,
        blocked_reason="research only",
    )
    qualified = qualify_cex_dex_research_evidence(evidence, model, settings)
    assert qualified.research_qualified is True
    assert qualified.capacity_claimed is False
    assert qualified.allocation_eligible is False
    assert qualified.executable_eligible is False
    assert any("inventory" in reason for reason in qualified.remaining_blockers)
    assert any("atomic hedge" in reason for reason in qualified.remaining_blockers)
