from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.cex_dex_canonical_runtime import (
    CEX_DEX_SETTLEMENT_METHOD,
    CexDexAwareAllocationForwardCertificationService,
    candidate_has_canonical_settlement,
    prepare_cex_dex_candidate,
)
from inefficiency_engine.models import MarketKind
from inefficiency_engine.unified_allocation import UnifiedPaperAllocation, UnifiedPaperCandidate


NOW = datetime(2026, 8, 20, 21, 0, tzinfo=timezone.utc)


def raw_candidate() -> UnifiedPaperCandidate:
    return UnifiedPaperCandidate(
        candidate_id="cex-dex:composite-key",
        family="cex_dex",
        strategy="cex_dex",
        asset="BTC",
        venues=["Coinbase", "DEX:ethereum"],
        capital_required_usd=20_000.0,
        notional_usd_per_leg=10_000.0,
        expected_profit_usd_per_deployment=12.0,
        expected_return_on_reserved_capital=12.0 / 20_000.0,
        source_return_metric="conservative_capture_edge_bps",
        source_return_value=12.0,
        exposure_kind="market_neutral",
        conflict_keys=[
            "cex:Coinbase:BTC-USD",
            "venue-symbol:Coinbase:BTC-USD",
            "dex:ethereum:BTC:sell_asset",
        ],
        evidence_id="evidence-1",
        allocation_eligible=True,
        executable_eligible=False,
        paper_only=True,
    )


def prepared_candidate() -> UnifiedPaperCandidate:
    return prepare_cex_dex_candidate(
        raw_candidate(),
        settings=SimpleNamespace(
            shadow_horizons_seconds=(60.0, 300.0),
            shadow_delay_seconds=60.0,
        ),
        observed_at=NOW,
    )


def allocation() -> UnifiedPaperAllocation:
    item = prepared_candidate()
    return UnifiedPaperAllocation(
        candidate_id=item.candidate_id,
        family=item.family,
        strategy=item.strategy,
        asset=item.asset,
        venues=item.venues,
        capital_required_usd=item.capital_required_usd,
        notional_usd_per_leg=item.notional_usd_per_leg,
        expected_profit_usd_per_deployment=item.expected_profit_usd_per_deployment,
        expected_return_on_reserved_capital=item.expected_return_on_reserved_capital,
        modeled_holding_hours=item.modeled_holding_hours,
        source_return_metric=item.source_return_metric,
        source_return_value=item.source_return_value,
        exposure_kind=item.exposure_kind,
        source_observed_at=item.source_observed_at,
        instrument_symbol=item.instrument_symbol,
        instrument_market_kind=item.instrument_market_kind,
        evidence_id=item.evidence_id,
        authorizes_execution=False,
        paper_only=True,
    )


def test_cex_dex_candidate_gets_exact_bounded_settlement_contract():
    item = prepared_candidate()
    assert item.source_observed_at == NOW
    assert item.instrument_symbol == "BTC-USD"
    assert item.instrument_market_kind == MarketKind.SPOT.value
    assert item.modeled_holding_hours == pytest.approx(60.0 / 3600.0)
    assert candidate_has_canonical_settlement(item) is True

    trial = CexDexAwareAllocationForwardCertificationService.trial_from_allocation(
        allocation(),
        plan_observed_at=NOW,
    )
    assert trial.settlement_supported is True
    assert trial.settlement_method == CEX_DEX_SETTLEMENT_METHOD
    assert trial.due_at == NOW + timedelta(seconds=60)
    assert trial.paper_only is True
    assert trial.live_execution_authority is False


def test_cex_dex_settlement_caps_upside_at_precommitted_conservative_edge():
    trial = CexDexAwareAllocationForwardCertificationService.trial_from_allocation(
        allocation(),
        plan_observed_at=NOW,
    )
    outcome = CexDexAwareAllocationForwardCertificationService._cex_dex_outcome(
        trial,
        matured_at=NOW + timedelta(seconds=75),
        verification_net_edge_bps=40.0,
        survived=True,
    )
    assert outcome.realized_profit_usd == pytest.approx(12.0)
    assert outcome.profit_capture_ratio == pytest.approx(1.0)
    assert outcome.realized_net_return == pytest.approx(12.0 / 20_000.0)
    assert outcome.paper_only is True
    assert outcome.live_execution_authority is False


def test_cex_dex_missing_route_realizes_zero_instead_of_invented_profit():
    trial = CexDexAwareAllocationForwardCertificationService.trial_from_allocation(
        allocation(),
        plan_observed_at=NOW,
    )
    outcome = CexDexAwareAllocationForwardCertificationService._cex_dex_outcome(
        trial,
        matured_at=NOW + timedelta(seconds=75),
        verification_net_edge_bps=None,
        survived=False,
    )
    assert outcome.realized_profit_usd == pytest.approx(0.0)
    assert outcome.profit_capture_ratio == pytest.approx(0.0)
    assert outcome.profitable is False


def test_cex_dex_adverse_verified_edge_can_realize_a_loss():
    trial = CexDexAwareAllocationForwardCertificationService.trial_from_allocation(
        allocation(),
        plan_observed_at=NOW,
    )
    outcome = CexDexAwareAllocationForwardCertificationService._cex_dex_outcome(
        trial,
        matured_at=NOW + timedelta(seconds=75),
        verification_net_edge_bps=-5.0,
        survived=True,
    )
    assert outcome.realized_profit_usd == pytest.approx(-5.0)
    assert outcome.profitable is False
