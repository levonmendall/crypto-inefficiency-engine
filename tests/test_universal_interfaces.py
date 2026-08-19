from datetime import datetime, timedelta, timezone

from inefficiency_engine.universal import candidate_from_external_signal, evaluate_bridge_quote
from inefficiency_engine.universal_models import BridgeQuote, ExternalOpportunitySignal, UniversalFamily

NOW = datetime(2026,8,19,tzinfo=timezone.utc)

def test_bridge_capability_is_fail_closed_without_execution_authority():
    quote = BridgeQuote(provider="example",asset="USDC",origin_chain_id="ethereum",destination_chain_id="base",
        input_amount=1000,output_amount=998,fee_bps=10,expected_fill_seconds=30,settlement_risk_haircut_bps=5,
        observed_at=NOW,expires_at=NOW+timedelta(seconds=30),source="test",executable_eligible=False)
    candidate = evaluate_bridge_quote(quote)
    assert candidate.family == UniversalFamily.CROSS_CHAIN
    assert candidate.executable_eligible is False
    assert candidate.blocked_reason

def test_solver_and_liquidation_interface_requires_authoritative_capacity():
    signal = ExternalOpportunitySignal(family="solver",provider="solver-x",asset="ETH",gross_edge_bps=30,
        modeled_cost_bps=5,risk_haircut_bps=5,capacity_usd=50000,observed_at=NOW,
        expires_at=NOW+timedelta(seconds=5),source="test",authoritative_capacity=False,executable_eligible=True)
    candidate = candidate_from_external_signal(signal)
    assert candidate.family == UniversalFamily.SOLVER
    assert candidate.executable_eligible is False
