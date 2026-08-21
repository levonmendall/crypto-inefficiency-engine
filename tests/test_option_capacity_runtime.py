from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from inefficiency_engine.evidence import EvidenceStore
from inefficiency_engine.evidence_velocity import EVIDENCE_CLASS_FRESHNESS_SECONDS
from inefficiency_engine.governed_mechanism_execution import GovernedMechanismExecutionService
from inefficiency_engine.mechanism_execution import MechanismTrialSpec
from inefficiency_engine.option_capacity import OptionCapacityLedger, OptionCapacityObservation
from inefficiency_engine.source_coverage_catalog import LANES, SOURCES


NOW = datetime(2026, 8, 21, 20, 0, tzinfo=timezone.utc)
EXPIRY = datetime(2026, 8, 28, 8, 0, tzinfo=timezone.utc)


def _capacity(observation_id: str = "cap-1") -> OptionCapacityObservation:
    return OptionCapacityObservation(
        observation_id=observation_id,
        instrument_name="BTC-28AUG26-60000-C",
        underlying="BTC",
        expiry=EXPIRY,
        strike=60_000.0,
        option_type="call",
        observed_at=NOW,
        contract_size_underlying=1.0,
        underlying_price_usd=60_000.0,
        bid_visible_size_contracts=1.0,
        ask_visible_size_contracts=0.8,
        bid_capacity_usd=60_000.0,
        ask_capacity_usd=48_000.0,
        source_reference="deribit:test",
    )


def _spec() -> MechanismTrialSpec:
    return MechanismTrialSpec(
        mechanism_id="volatility",
        cohort_key="vol|Deribit|BTC|short_volatility",
        asset="BTC",
        venues=["Deribit"],
        source_observed_at=NOW,
        holding_hours=6.0,
        capital_usd=10_000.0,
        predicted_net_return=0.01,
        settlement_payload={
            "venue": "Deribit",
            "underlying": "BTC",
            "expiry": EXPIRY.isoformat(),
            "strike": 60_000.0,
            "option_type": "call",
            "direction": "short_volatility",
        },
        conflict_keys=["option:test"],
    )


def test_option_capacity_is_explicit_source_class_with_short_freshness():
    required = set(LANES["volatility"]["required"])
    capacity_sources = [
        source
        for source in SOURCES
        if "volatility" in source["lanes"] and "option_capacity" in source["classes"]
    ]

    assert "option_capacity" in required
    assert [source["id"] for source in capacity_sources] == ["deribit-option-capacity"]
    assert EVIDENCE_CLASS_FRESHNESS_SECONDS["option_capacity"] == 900.0


def test_volatility_trial_is_bounded_to_five_percent_visible_capacity(tmp_path):
    store = EvidenceStore(tmp_path / "option-capacity.sqlite3")
    ledger = OptionCapacityLedger(store)
    ledger.record(_capacity())

    service = GovernedMechanismExecutionService.__new__(GovernedMechanismExecutionService)
    service.option_capacity = ledger
    service.settings = SimpleNamespace(alpha_min_notional_usd=100.0)

    bounded = service._capacity_bounded_volatility_spec(
        _spec(),
        before=NOW + timedelta(minutes=1),
    )

    assert bounded is not None
    # Smallest visible side is $48k; only 5% may be used by the paper trial.
    assert bounded.capital_usd == 2_400.0
    assert bounded.settlement_payload["visible_option_capacity_usd"] == 48_000.0
    assert bounded.settlement_payload["option_capacity_fraction_used"] == 0.05
    assert bounded.settlement_payload["hidden_option_depth_assumed"] is False
    assert bounded.settlement_payload["option_capacity_evidence"][0]["observation_id"] == "cap-1"


def test_volatility_trial_fails_closed_without_exact_contract_capacity(tmp_path):
    store = EvidenceStore(tmp_path / "option-capacity-missing.sqlite3")
    service = GovernedMechanismExecutionService.__new__(GovernedMechanismExecutionService)
    service.option_capacity = OptionCapacityLedger(store)
    service.settings = SimpleNamespace(alpha_min_notional_usd=100.0)

    assert service._capacity_bounded_volatility_spec(
        _spec(),
        before=NOW + timedelta(minutes=1),
    ) is None
