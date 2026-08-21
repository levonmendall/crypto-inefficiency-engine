from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from inefficiency_engine.config import Settings
from inefficiency_engine.evidence import EvidenceStore, ScanSnapshot
from inefficiency_engine.executable_lane_runtime import ExecutableMechanismExecutionService
from inefficiency_engine.lane_readiness import build_lane_executable_readiness
from inefficiency_engine.mechanism_execution import (
    CapitalTransferObservation,
    MechanismForwardOutcome,
    MechanismForwardTrial,
)
from inefficiency_engine.models import (
    MarketKind,
    MarketQuote,
    Opportunity,
    OpportunityLeg,
    Side,
    Strategy,
)
from inefficiency_engine.research_mechanisms import OptionQuoteObservation, YieldObservation


NOW = datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc)


def core():
    return SimpleNamespace(settings=Settings())


def quote(at: datetime, price: float, *, venue="Coinbase", asset="BTC", kind=MarketKind.SPOT, symbol=None):
    symbol = symbol or (f"{asset}-USD" if kind == MarketKind.SPOT else f"{asset}USDT")
    return MarketQuote(
        venue=venue,
        asset=asset,
        market_kind=kind,
        symbol=symbol,
        mid=price,
        bid=price * 0.9999,
        ask=price * 1.0001,
        observed_at=at,
        source="test",
    )


def record_quotes(store: EvidenceStore, rows: list[MarketQuote]):
    started = min(row.observed_at for row in rows)
    completed = max(row.observed_at for row in rows)
    store.record_scan(
        funding_quotes=[],
        market_quotes=rows,
        opportunities=[],
        providers=[],
        started_at=started,
        completed_at=completed,
    )


def test_incremental_mechanism_qualification_uses_same_three_to_thirty_ladder(tmp_path):
    store = EvidenceStore(tmp_path / "qualification.sqlite3")
    service = ExecutableMechanismExecutionService(core(), store)
    cohort = "yield|Morpho|USDC|lending"
    for index in range(3):
        service.ledger.record_outcome(MechanismForwardOutcome(
            trial_id=f"trial-{index}",
            mechanism_id="yield",
            cohort_key=cohort,
            asset="USDC",
            matured_at=NOW + timedelta(days=index + 1),
            due_at=NOW + timedelta(days=index + 1),
            predicted_net_return=0.002,
            realized_gross_return=0.002,
            realized_net_return=0.002,
            realized_profit_usd=2.0,
            profitable=True,
            settlement_method="test",
        ))
    q3 = service.qualification(cohort, "yield")
    assert q3.incremental_eligible is True
    assert q3.allocation_fraction == pytest.approx(0.10)

    for index in range(3, 30):
        service.ledger.record_outcome(MechanismForwardOutcome(
            trial_id=f"trial-{index}",
            mechanism_id="yield",
            cohort_key=cohort,
            asset="USDC",
            matured_at=NOW + timedelta(days=index + 1),
            due_at=NOW + timedelta(days=index + 1),
            predicted_net_return=0.002,
            realized_gross_return=0.002,
            realized_net_return=0.002,
            realized_profit_usd=2.0,
            profitable=True,
            settlement_method="test",
        ))
    q30 = service.qualification(cohort, "yield")
    assert q30.sample_count == 30
    assert q30.fully_statistically_qualified is True
    assert q30.allocation_fraction == pytest.approx(1.0)


def test_yield_native_forward_settlement_uses_future_rate_and_exit_capacity(tmp_path):
    store = EvidenceStore(tmp_path / "yield-settlement.sqlite3")
    service = ExecutableMechanismExecutionService(core(), store)
    entry = YieldObservation(
        provider="Morpho",
        protocol="Morpho",
        venue_or_chain="ethereum",
        asset="USDC",
        kind="lending",
        observed_at=NOW,
        as_of_at=NOW,
        gross_apy=0.12,
        capacity_usd=1_000_000,
        holding_hours=24,
        entry_exit_cost_bps=1,
        credit_or_protocol_risk_haircut_apy=0.01,
        authoritative=True,
        commercial_use_permitted=True,
    )
    exit_row = entry.model_copy(update={
        "observation_id": "yield-exit",
        "observed_at": NOW + timedelta(hours=25),
        "as_of_at": NOW + timedelta(hours=25),
        "gross_apy": 0.10,
        "capacity_usd": 900_000,
    })
    service.yield_service.record(entry)
    service.yield_service.record(exit_row)
    trial = MechanismForwardTrial(
        mechanism_id="yield",
        cohort_key="yield|Morpho|USDC|lending",
        asset="USDC",
        venues=["Morpho"],
        source_observed_at=NOW,
        due_at=NOW + timedelta(hours=24),
        capital_usd=1000,
        predicted_net_return=0.001,
        predicted_profit_usd=1.0,
        settlement_payload={
            "protocol": "Morpho",
            "asset": "USDC",
            "entry_net_apy": 0.10,
            "entry_exit_cost_bps": 1.0,
        },
    )
    settled = service.settle_trial(trial)
    assert settled is not None
    assert settled.settlement_method == "realized_yield_accrual_plus_exit_liquidity"
    assert settled.detail["exit_liquidity_sufficient"] is True


def test_volatility_native_forward_settlement_requires_option_and_underlying_evidence(tmp_path):
    store = EvidenceStore(tmp_path / "vol-settlement.sqlite3")
    service = ExecutableMechanismExecutionService(core(), store)
    expiry = NOW + timedelta(days=30)
    exit_option = OptionQuoteObservation(
        provider="Deribit",
        venue="Deribit",
        underlying="BTC",
        expiry=expiry,
        strike=60_000,
        option_type="call",
        bid=3050,
        ask=3070,
        implied_volatility=0.60,
        delta=0.50,
        observed_at=NOW + timedelta(hours=7),
        authoritative=True,
        commercial_use_permitted=True,
    )
    service.volatility_service.record(exit_option)
    record_quotes(store, [quote(NOW + timedelta(hours=7), 61_000)])
    trial = MechanismForwardTrial(
        mechanism_id="volatility",
        cohort_key="vol|Deribit|BTC|long_volatility",
        asset="BTC",
        venues=["Deribit"],
        source_observed_at=NOW,
        due_at=NOW + timedelta(hours=6),
        capital_usd=1000,
        predicted_net_return=0.01,
        predicted_profit_usd=10,
        settlement_payload={
            "venue": "Deribit",
            "underlying": "BTC",
            "expiry": expiry.isoformat(),
            "strike": 60_000.0,
            "option_type": "call",
            "entry_mid": 3000.0,
            "entry_delta": 0.50,
            "underlying_entry_price": 60_000.0,
            "direction": "long_volatility",
            "spread_fraction": 0.001,
            "hedge_cost_return": 0.001,
        },
    )
    settled = service.settle_trial(trial)
    assert settled is not None
    assert "option_mark_forward" in settled.settlement_method
    assert settled.detail["underlying_exit_venue"] == "Coinbase"


def test_liquidation_native_forward_settlement_uses_observed_recovery_mark(tmp_path):
    store = EvidenceStore(tmp_path / "liq-settlement.sqlite3")
    service = ExecutableMechanismExecutionService(core(), store)
    record_quotes(store, [
        quote(
            NOW + timedelta(hours=2),
            61_000,
            venue="Bybit",
            kind=MarketKind.PERPETUAL,
            symbol="BTCUSDT",
        )
    ])
    trial = MechanismForwardTrial(
        mechanism_id="liquidation_distress",
        cohort_key="liquidation|Bybit|BTC|long",
        asset="BTC",
        venues=["Bybit"],
        source_observed_at=NOW,
        due_at=NOW + timedelta(hours=1),
        capital_usd=1000,
        predicted_net_return=0.005,
        predicted_profit_usd=5,
        settlement_payload={
            "event_id": "liq-1",
            "venue": "Bybit",
            "asset": "BTC",
            "symbol": "BTCUSDT",
            "entry_price": 60_000.0,
            "direction": "long",
            "cost_return": 0.001,
        },
    )
    settled = service.settle_trial(trial)
    assert settled is not None
    assert settled.net_return > 0
    assert settled.detail["capture_assumed"] is False


def test_capital_location_native_forward_settlement_uses_future_incidence_and_transfer_telemetry(tmp_path):
    store = EvidenceStore(tmp_path / "location-settlement.sqlite3")
    service = ExecutableMechanismExecutionService(core(), store)
    service.ledger.record_transfer(CapitalTransferObservation(
        source="authoritative-transfer-test",
        route="Coinbase:BTC",
        venue="Coinbase",
        asset="BTC",
        observed_at=NOW,
        transfer_cost_usd=1.0,
        transfer_latency_seconds=30.0,
        notional_usd=1000.0,
    ))
    opportunity = Opportunity(
        id="future-basis",
        strategy=Strategy.SPOT_PERP_BASIS,
        asset="BTC",
        legs=[
            OpportunityLeg(venue="Coinbase", asset="BTC", market_kind=MarketKind.SPOT, side=Side.LONG),
            OpportunityLeg(venue="HlPerp", asset="BTC", market_kind=MarketKind.PERPETUAL, side=Side.SHORT),
        ],
        gross_edge_bps_per_hour=2.0,
        modeled_cost_bps=1.0,
        holding_hours=24.0,
        safety_buffer_bps_per_hour=0.0,
        net_edge_bps_per_hour=1.0,
        net_annualized_return=0.50,
        observed_at=NOW + timedelta(hours=12),
        expires_at=NOW + timedelta(hours=13),
    )
    store.record_scan(
        funding_quotes=[],
        market_quotes=[quote(NOW + timedelta(hours=25), 60_000)],
        opportunities=[opportunity],
        providers=[],
        started_at=NOW + timedelta(hours=12),
        completed_at=NOW + timedelta(hours=25),
    )
    trial = MechanismForwardTrial(
        mechanism_id="capital_location_settlement",
        cohort_key="location|Coinbase|BTC",
        asset="BTC",
        venues=["Coinbase"],
        source_observed_at=NOW,
        due_at=NOW + timedelta(hours=24),
        capital_usd=1000,
        predicted_net_return=0.001,
        predicted_profit_usd=1.0,
        settlement_payload={
            "venue": "Coinbase",
            "asset": "BTC",
            "transfer_cost_usd": 1.0,
            "transfer_latency_seconds": 30.0,
        },
    )
    settled = service.settle_trial(trial)
    assert settled is not None
    assert settled.detail["future_positive_opportunity_count"] == 1
    assert settled.settlement_method.startswith("forward_location_opportunity_incidence")


def test_all_thirteen_lanes_have_separate_architecture_and_current_qualification_flags(tmp_path):
    store = EvidenceStore(tmp_path / "readiness.sqlite3")
    snapshot = build_lane_executable_readiness(core(), store)
    assert snapshot.lane_count == 13
    assert snapshot.architecture_executable_count == 13
    assert snapshot.all_lanes_paper_execution_capable is True
    assert all(row.paper_execution_capable for row in snapshot.lanes)
    assert all(row.live_execution_capable is False for row in snapshot.lanes)
    assert snapshot.currently_qualified_count == 0
